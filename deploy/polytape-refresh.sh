#!/bin/bash
# polytape campaign event-set refresh (the roll-out / roll-in control plane).
#
# Re-discovers the campaign's current OPEN event set (sports main lines inside the
# lookahead window + the live/next hourly crypto strike ladders) from the campaign
# spec, and restarts the recorder ONLY if the set of (event, recorded markets)
# changed — rolling finished games / expired ladders OUT and new fixtures / the
# next hour's ladder IN, in one clean pass. The recording code is untouched; this
# is pure scheduling around it. Run by polytape-refresh.timer every ~10 min (root).
#
# What counts as a change: the canonical key printed by polytape-event-set.py —
# the event ids PLUS the ids of the markets to record inside each event. Comparing
# event ids alone would miss a main line moving to another market; diffing the
# whole file would restart on every price tick. See that script for the contract.
#
# Safety: a discovery failure (Gamma hiccup), an unreadable/empty result, or an
# unchanged set is a NO-OP — the live set is never wiped on a fluke and the recorder
# is never restarted for nothing. A discovery failure exits 1 (the unit shows as
# failed; the timer still re-fires) with the discovery's last stderr lines in the
# journal. The restart is serialized under a flock, and a restart that fails leaves
# a marker so the next run retries it.
#
# Restart economy: the installed file is passed back to the discovery (--previous) so
# main-line picks are sticky, and a change that only REMOVES events (finished games,
# expired ladders) is deferred — a finished market yields nothing, so leaving it
# subscribed costs nothing — until the next addition or until DEFER_MIN minutes have
# passed since the last install. Additions and changed market sets restart at once.
#
#   polytape-refresh.sh            # act (timer mode)
#   polytape-refresh.sh --check    # report what would happen; change nothing
set -uo pipefail
export LC_ALL=C   # comm/sort must agree with the keyer's code-point ordering

PY=${POLYTAPE_PY:-/opt/polytape/venv/bin/python}
SCRIPT=${POLYTAPE_DISCOVERY:-/opt/polytape/scripts/list_campaign_events.py}
KEYER=${POLYTAPE_EVENT_SET:-/opt/polytape/polytape-event-set.py}
SPEC=${POLYTAPE_CAMPAIGN_SPEC:-/etc/polytape/campaign.json}
CUR=${POLYTAPE_EVENTS_FILE:-/etc/polytape/campaign_events.json}
UNIT=${POLYTAPE_UNIT:-polytape}
LOCK=${POLYTAPE_LOCK:-/run/lock/polytape-refresh.lock}
OWNER=${POLYTAPE_OWNER:-polytape}
DEFER_MIN=${POLYTAPE_DEFER_REMOVALS_MIN:-60}   # pure roll-outs wait this long since the last install
PENDING="$CUR.restart-pending"   # exists iff a set was installed but the restart failed

CHECK=0
case "${1:-}" in
    "") ;;
    --check|--dry-run) CHECK=1 ;;
    -h|--help) sed -n '2,27p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "polytape-refresh: unknown argument: $1" >&2; exit 2 ;;
esac

log() {
    # Under systemd ($INVOCATION_ID set) stdout already lands in the journal under
    # SyslogIdentifier=polytape-refresh; logging via logger too would double every line.
    if [ -z "${INVOCATION_ID:-}" ] && command -v logger >/dev/null 2>&1; then
        logger -t polytape-refresh -- "$*"
    fi
    echo "polytape-refresh: $*"
}

NEW=$(mktemp "${TMPDIR:-/tmp}/campaign_events.XXXXXX") || { log "mktemp failed"; exit 1; }
ERR=$(mktemp "${TMPDIR:-/tmp}/campaign_discovery.XXXXXX") || { log "mktemp failed"; exit 1; }
trap 'rm -f "$NEW" "$ERR"' EXIT

PREVIOUS=()
[ -s "$CUR" ] && PREVIOUS=(--previous "$CUR")   # sticky main-line picks
if ! "$PY" "$SCRIPT" --spec "$SPEC" --out "$NEW" --open-only "${PREVIOUS[@]}" >/dev/null 2>"$ERR"; then
    log "discovery failed (Gamma error? bad spec?); leaving recorder unchanged: $(tail -n 3 "$ERR" | tr '\n' ' ')"
    exit 1
fi
tags=$(grep -m1 '^tags:' "$ERR" || true)   # per-tag open events / pages / truncation

NEW_KEY=$("$PY" "$KEYER" --open-only "$NEW" 2>/dev/null) || NEW_KEY=""
CUR_KEY=$("$PY" "$KEYER" --open-only "$CUR" 2>/dev/null) || CUR_KEY=""

# Refuse to act on an empty/garbage discovery (would blank the recorder).
if [ -z "$NEW_KEY" ]; then
    log "discovery returned no open events; leaving recorder unchanged${tags:+ ($tags)}"
    exit 0
fi

new_summary=$("$PY" "$KEYER" --open-only --summary "$NEW" 2>/dev/null)
if [ "$NEW_KEY" = "$CUR_KEY" ] && [ ! -e "$PENDING" ]; then
    log "no change ($new_summary)${tags:+ $tags}"
    exit 0
fi

# Describe the delta for the journal: events added / removed / with a changed market set.
ids_new=$(printf '%s\n' "$NEW_KEY" | cut -f1)
ids_cur=$(printf '%s\n' "$CUR_KEY" | cut -f1)
n_added=$(comm -13 <(printf '%s\n' "$ids_cur") <(printf '%s\n' "$ids_new") | grep -c .)
n_removed=$(comm -23 <(printf '%s\n' "$ids_cur") <(printf '%s\n' "$ids_new") | grep -c .)
n_common_ids=$(comm -12 <(printf '%s\n' "$ids_cur") <(printf '%s\n' "$ids_new") | grep -c .)
n_common_lines=$(comm -12 <(printf '%s\n' "$CUR_KEY") <(printf '%s\n' "$NEW_KEY") | grep -c .)
n_changed=$(( n_common_ids - n_common_lines ))
cur_summary=$("$PY" "$KEYER" --open-only --summary "$CUR" 2>/dev/null || echo "none installed")
delta="+$n_added -$n_removed ~$n_changed ($new_summary; was $cur_summary)"

# A pure roll-out (only removals, nothing added or changed, no restart pending): a
# finished market yields nothing, so leaving it subscribed costs nothing, whereas the
# restart costs a gap on EVERY market. Defer it until the next addition or until the
# installed file is DEFER_MIN minutes old (its mtime is the last install).
defer=0
if [ "$n_added" -eq 0 ] && [ "$n_changed" -eq 0 ] && [ "$n_removed" -gt 0 ] && [ ! -e "$PENDING" ]; then
    if [ -n "$(find "$CUR" -maxdepth 0 -mmin -"$DEFER_MIN" 2>/dev/null)" ]; then
        defer=1
    fi
fi

if [ "$CHECK" = 1 ]; then
    if [ "$NEW_KEY" = "$CUR_KEY" ]; then
        log "CHECK: set unchanged but a restart is pending from a failed one -> would restart $UNIT"
    elif [ "$defer" = 1 ]; then
        log "CHECK: only removals $delta -> would defer (last install <${DEFER_MIN}m ago)"
    else
        log "CHECK: event set changed $delta -> would install $CUR + restart $UNIT"
    fi
    exit 0
fi

if [ "$defer" = 1 ]; then
    log "deferring pure roll-out $delta: nothing to add; last install <${DEFER_MIN}m ago${tags:+ $tags}"
    exit 0
fi

if [ "$NEW_KEY" != "$CUR_KEY" ]; then
    if [ "$(id -u)" = 0 ]; then
        install -o "$OWNER" -g "$OWNER" -m 0644 "$NEW" "$CUR"
    else
        install -m 0644 "$NEW" "$CUR"
    fi || { log "installing $CUR failed; recorder unchanged"; exit 1; }
fi
: > "$PENDING"

# Serialize the restart under a lock (shared with any other restart-er, e.g. a control
# helper if one is ever deployed). When such a helper invoked us it ALREADY holds the
# lock (POLYTAPE_HELD_LOCK=1) — re-grabbing it would self-deadlock, so restart directly.
if [ -n "${POLYTAPE_HELD_LOCK:-}" ]; then
    systemctl restart "$UNIT"
else
    ( flock -w 30 9 && systemctl restart "$UNIT" ) 9>"$LOCK"
fi
rc=$?
if [ "$rc" -ne 0 ]; then
    log "event set changed $delta -> installed, but restart of $UNIT FAILED (rc=$rc); will retry next run"
    exit 1
fi
rm -f "$PENDING"
log "event set changed $delta -> installed + restarted $UNIT${tags:+ $tags}"
