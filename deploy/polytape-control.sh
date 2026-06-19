#!/usr/bin/env bash
# polytape control-plane helper — the SINGLE privileged choke point.
#
# Runs as ROOT, triggered by polytape-control.path when the (unprivileged)
# polytape-admin sidecar drops an allow-listed intent file into
# /run/polytape-admin/intent/. The admin NEVER runs systemctl or edits an env file
# itself; this helper re-validates EVERYTHING root-side and treats the intent dir as
# hostile. The action is ONLY ever the loop's hardcoded name — never derived from a
# filesystem entry — so a maliciously-named file can never be executed.
#
# Note: NOT `set -e`. One failing action must not abort the batch or skip the
# stray-file sweep at the end (a half-done run could otherwise strand intents that
# the edge-triggered .path then double-processes into an extra restart).
set -uo pipefail

RUN_DIR="/run/polytape-admin"
INTENT_DIR="$RUN_DIR/intent"
# heartbeat.env holds ONLY POLYTAPE_HEARTBEAT_URL — POLYTAPE_SALT lives in
# polytape.env (0600, owner polytape) and is NEVER read or written here, so this
# plane can neither leak nor clobber it.
HEARTBEAT_ENV="/etc/polytape/heartbeat.env"
LOCK="/run/polytape-control.lock"          # shared with polytape-refresh.sh
REFRESH="/opt/polytape/polytape-refresh.sh"

log() { logger -t polytape-control -- "$*"; }

exec 9>"$LOCK" || { log "cannot open lock"; exit 1; }
flock -w 30 9 || { log "lock timeout; aborting"; exit 1; }

arm_heartbeat() {
  local url tmp
  url="$(head -c 512 "$RUN_DIR/heartbeat.url" 2>/dev/null | tr -d '\r\n')"
  rm -f "$RUN_DIR/heartbeat.url"   # consume: no replay of a stale URL via a bare intent
  case "$url" in *[\'\"\`\$\\\ ]* ) log "arm-heartbeat REJECT: bad char in url"; return 1;; esac
  if [ -n "$url" ] && ! printf '%s' "$url" \
      | grep -Eq '^https://[A-Za-z0-9.-]+(:[0-9]{1,5})?(/[A-Za-z0-9._~/-]*)?(\?[A-Za-z0-9._~=&%-]*)?$'; then
    log "arm-heartbeat REJECT: url failed allow-list"; return 1
  fi
  tmp="$(mktemp /etc/polytape/.hb.XXXXXX)" || { log "arm-heartbeat: mktemp failed"; return 1; }
  [ -n "$url" ] && printf 'POLYTAPE_HEARTBEAT_URL=%s\n' "$url" > "$tmp"   # empty url => disarm
  chown root:polytape "$tmp"; chmod 0640 "$tmp"
  mv -f "$tmp" "$HEARTBEAT_ENV"
  log "arm-heartbeat: heartbeat.env updated (url fp $(printf '%s' "$url" | sha256sum | cut -c1-12)); restarting"
  systemctl restart polytape || log "arm-heartbeat: restart failed"
}

# Process only the allow-listed actions, in a fixed order. Consume each intent file
# BEFORE acting so a failure can't loop, and never let one failure abort the batch.
for action in restart refresh arm-heartbeat; do
  [ -e "$INTENT_DIR/$action" ] || continue
  rm -f "$INTENT_DIR/$action"
  case "$action" in
    restart) log "restart: systemctl restart polytape"; systemctl restart polytape || log "restart failed" ;;
    # refresh.sh would re-grab the SAME flock we already hold and self-deadlock; tell
    # it we hold the lock so it restarts directly instead of re-locking.
    refresh) log "refresh: exec $REFRESH"; POLYTAPE_HELD_LOCK=1 "$REFRESH" || log "refresh failed" ;;
    arm-heartbeat) arm_heartbeat || log "arm-heartbeat failed (rc=$?)" ;;
  esac
done
# Defensively drop anything that is not an allow-listed action.
find "$INTENT_DIR" -mindepth 1 -maxdepth 1 \
  ! -name restart ! -name refresh ! -name arm-heartbeat -exec rm -f {} + 2>/dev/null || true
