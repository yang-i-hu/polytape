#!/bin/bash
# polytape match-set refresh (the roll-out / roll-in control plane).
#
# Re-discovers the current set of OPEN World Cup match events and restarts the
# recorder ONLY if that set changed — which rolls resolved matches OUT and any
# newly-created fixtures (e.g. knockouts) IN, in one clean pass. The recording
# code itself is untouched; this is pure scheduling around it.
#
# Safety: a transient discovery failure (Gamma hiccup) or an empty/invalid result
# is a NO-OP — we never wipe the live set on a fluke. We only act on a genuine,
# non-empty, *changed* set. Run by polytape-refresh.timer (every ~10 min).
set -uo pipefail

SCRIPT=/opt/polytape/list_wc_matches.py
CUR=/etc/polytape/wc_matches.json
PY=/usr/bin/python3
NEW=$(mktemp /tmp/wc_new.XXXXXX.json)
trap 'rm -f "$NEW"' EXIT
log() { logger -t polytape-refresh "$*"; }

if ! "$PY" "$SCRIPT" --out "$NEW" --open-only >/dev/null 2>&1; then
    log "discovery failed (Gamma error?); leaving recorder unchanged"
    exit 0
fi

ids() { "$PY" -c "import json,sys;print(sorted(str(m['event_id']) for m in json.load(open(sys.argv[1]))))" "$1" 2>/dev/null; }
NEW_IDS=$(ids "$NEW")
CUR_IDS=$(ids "$CUR")

# Refuse to act on an empty/garbage discovery (would blank the recorder).
if [ -z "$NEW_IDS" ] || [ "$NEW_IDS" = "[]" ]; then
    log "discovery returned no matches; leaving recorder unchanged"
    exit 0
fi

if [ "$NEW_IDS" != "$CUR_IDS" ]; then
    n=$("$PY" -c "import json;print(len(json.load(open('$NEW'))))")
    install -o polytape -g polytape -m 0644 "$NEW" "$CUR"
    # Serialize the restart against the admin control helper (shared lock) so an
    # operator action and this timer can never restart the recorder simultaneously.
    ( flock -w 30 9 && systemctl restart polytape ) 9>/run/polytape-control.lock
    log "open match set changed -> regenerated ($n matches) + restarted polytape"
else
    log "no change ($(echo "$CUR_IDS" | tr ',' '\n' | grep -c .) matches)"
fi
