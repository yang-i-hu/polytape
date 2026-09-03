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
# is never restarted for nothing. The restart is serialized under a flock, and a
# restart that fails leaves a marker so the next run retries it.
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
PENDING="$CUR.restart-pending"   # exists iff a set was installed but the restart failed

CHECK=0
case "${1:-}" in
    "") ;;
    --check|--dry-run) CHECK=1 ;;
    -h|--help) sed -n '2,21p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "polytape-refresh: unknown argument: $1" >&2; exit 2 ;;
esac

log() {
    command -v logger >/dev/null 2>&1 && logger -t polytape-refresh -- "$*"
    echo "polytape-refresh: $*"
}

NEW=$(mktemp "${TMPDIR:-/tmp}/campaign_events.XXXXXX") || { log "mktemp failed"; exit 1; }
trap 'rm -f "$NEW"' EXIT

if ! "$PY" "$SCRIPT" --spec "$SPEC" --out "$NEW" --open-only >/dev/null 2>&1; then
    log "discovery failed (Gamma error? bad spec?); leaving recorder unchanged"
    exit 0
fi

NEW_KEY=$("$PY" "$KEYER" --open-only "$NEW" 2>/dev/null) || NEW_KEY=""
CUR_KEY=$("$PY" "$KEYER" --open-only "$CUR" 2>/dev/null) || CUR_KEY=""

# Refuse to act on an empty/garbage discovery (would blank the recorder).
if [ -z "$NEW_KEY" ]; then
    log "discovery returned no open events; leaving recorder unchanged"
    exit 0
fi

new_summary=$("$PY" "$KEYER" --open-only --summary "$NEW" 2>/dev/null)
if [ "$NEW_KEY" = "$CUR_KEY" ] && [ ! -e "$PENDING" ]; then
    log "no change ($new_summary)"
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

if [ "$CHECK" = 1 ]; then
    if [ "$NEW_KEY" = "$CUR_KEY" ]; then
        log "CHECK: set unchanged but a restart is pending from a failed one -> would restart $UNIT"
    else
        log "CHECK: event set changed $delta -> would install $CUR + restart $UNIT"
    fi
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
log "event set changed $delta -> installed + restarted $UNIT"
