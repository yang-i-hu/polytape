#!/usr/bin/env bash
# polytape control-plane helper — the SINGLE privileged choke point.
#
# Runs as ROOT, triggered by polytape-control.path when the (unprivileged)
# polytape-admin sidecar drops an intent file into /run/polytape-admin/intent/.
# The admin NEVER runs systemctl or edits the env file itself; it only writes an
# allow-listed intent filename + (for arm-heartbeat) a staged URL. This helper
# re-validates EVERYTHING root-side — it must assume the intent dir is hostile.
#
# Install (root):
#   install -m 0755 deploy/polytape-control.sh /opt/polytape/polytape-control.sh
#   install -m 0644 deploy/polytape-control.service /etc/systemd/system/
#   install -m 0644 deploy/polytape-control.path    /etc/systemd/system/
#   systemctl daemon-reload && systemctl enable --now polytape-control.path
set -euo pipefail

RUN_DIR="/run/polytape-admin"
INTENT_DIR="$RUN_DIR/intent"
ENV_FILE="/etc/polytape/polytape.env"
LOCK="/run/polytape-control.lock"   # shared with polytape-refresh.sh
REFRESH="/opt/polytape/polytape-refresh.sh"

log() { logger -t polytape-control -- "$*"; }

# Serialize against polytape-refresh.sh's restart and any concurrent action.
exec 9>"$LOCK"
flock -w 30 9 || { log "lock timeout; aborting"; exit 1; }

arm_heartbeat() {
  local url
  url="$(head -c 512 "$RUN_DIR/heartbeat.url" 2>/dev/null | tr -d '\r\n')"
  # Reject anything that isn't a plain https URL or carries shell/env metacharacters.
  case "$url" in
    *[\'\"\`\$\\\ ]* ) log "arm-heartbeat REJECT: bad char in url"; return 1;;
  esac
  if ! printf '%s' "$url" \
      | grep -Eq '^https://[A-Za-z0-9.-]+(:[0-9]{1,5})?(/[A-Za-z0-9._~/-]*)?(\?[A-Za-z0-9._~=&%-]*)?$'; then
    log "arm-heartbeat REJECT: url failed allow-list"; return 1
  fi
  # Snapshot the keys we must NOT disturb, rewrite ONLY the heartbeat line,
  # then assert SALT/EVENT_ID are byte-identical before swapping the file in.
  local salt_before eid_before salt_after eid_after tmp
  salt_before="$(grep -E '^POLYTAPE_SALT=' "$ENV_FILE" || true)"
  eid_before="$(grep -E '^POLYTAPE_EVENT_ID=' "$ENV_FILE" || true)"
  tmp="$(mktemp /etc/polytape/.env.XXXXXX)"
  grep -vE '^POLYTAPE_HEARTBEAT_URL=' "$ENV_FILE" > "$tmp" || true
  [ -n "$url" ] && printf 'POLYTAPE_HEARTBEAT_URL=%s\n' "$url" >> "$tmp"
  salt_after="$(grep -E '^POLYTAPE_SALT=' "$tmp" || true)"
  eid_after="$(grep -E '^POLYTAPE_EVENT_ID=' "$tmp" || true)"
  if [ "$salt_before" != "$salt_after" ] || [ "$eid_before" != "$eid_after" ]; then
    rm -f "$tmp"; log "arm-heartbeat ABORT: SALT/EVENT_ID would change"; return 3
  fi
  chown polytape:polytape "$tmp"; chmod 0600 "$tmp"
  mv -f "$tmp" "$ENV_FILE"
  log "arm-heartbeat: env updated (url fp $(printf '%s' "$url" | sha256sum | cut -c1-12)); restarting"
  systemctl restart polytape
}

# Process only the allow-listed actions, in a fixed order. Consume each intent
# file BEFORE acting (so a failure can't loop), and sweep anything unexpected.
for action in restart refresh arm-heartbeat; do
  [ -e "$INTENT_DIR/$action" ] || continue
  rm -f "$INTENT_DIR/$action"
  case "$action" in
    restart) log "restart: systemctl restart polytape"; systemctl restart polytape ;;
    refresh) log "refresh: exec $REFRESH"; "$REFRESH" ;;
    arm-heartbeat) arm_heartbeat || log "arm-heartbeat failed (rc=$?)" ;;
  esac
done
# Defensively drop anything that is not an allow-listed action.
find "$INTENT_DIR" -mindepth 1 -maxdepth 1 \
  ! -name restart ! -name refresh ! -name arm-heartbeat -exec rm -f {} + 2>/dev/null || true
