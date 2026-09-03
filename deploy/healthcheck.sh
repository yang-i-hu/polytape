#!/bin/bash
# polytape maker-campaign health check — read-only; run by hand over SSH (or from cron).
#
# Prints: recorder unit state, freshness from meta.json (age of last_record_at,
# cumulative counts, open events/tokens, gaps), segment and per-match file sizes,
# the polytape-* timers, and disk usage. Exit status: 0 healthy; 1 STALE (no record
# for more than --stale-after seconds, default 300) or meta.json missing/unreadable.
#
#   healthcheck.sh [--run-dir /data/run-maker] [--stale-after 300]
# Env: POLYTAPE_RUN_DIR, POLYTAPE_STALE_AFTER, POLYTAPE_UNIT, POLYTAPE_PY.
set -uo pipefail

RUN_DIR=${POLYTAPE_RUN_DIR:-/data/run-maker}
STALE_AFTER=${POLYTAPE_STALE_AFTER:-300}
UNIT=${POLYTAPE_UNIT:-polytape}
PY=${POLYTAPE_PY:-}

while [ $# -gt 0 ]; do
    case "$1" in
        --run-dir) RUN_DIR=$2; shift 2 ;;
        --stale-after) STALE_AFTER=$2; shift 2 ;;
        -h|--help) sed -n '2,10p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "healthcheck: unknown argument: $1" >&2; exit 2 ;;
    esac
done
if [ -z "$PY" ]; then
    for c in /opt/polytape/venv/bin/python python3 python; do
        if command -v "$c" >/dev/null 2>&1; then PY=$c; break; fi
    done
fi
[ -n "$PY" ] || { echo "healthcheck: no python interpreter found" >&2; exit 2; }

rc=0

echo "== recorder ($UNIT)"
if command -v systemctl >/dev/null 2>&1; then
    printf '  unit: %s (active since %s)\n' \
        "$(systemctl is-active "$UNIT" 2>/dev/null)" \
        "$(systemctl show -p ActiveEnterTimestamp --value "$UNIT" 2>/dev/null)"
else
    echo "  (systemctl not available here)"
fi

echo "== freshness ($RUN_DIR/meta.json; stale after ${STALE_AFTER}s)"
"$PY" - "$RUN_DIR/meta.json" "$STALE_AFTER" <<'PY' || rc=1
import json
import sys
from datetime import datetime, timezone

path, stale_after = sys.argv[1], float(sys.argv[2])
try:
    with open(path, encoding="utf-8") as fh:
        meta = json.load(fh)
except (OSError, ValueError) as exc:
    print(f"  MISSING: cannot read {path}: {exc}")
    sys.exit(1)


def parse(ts):
    try:
        return datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except ValueError:
        return None


now = datetime.now(timezone.utc)
last = meta.get("last_record_at")
last_dt = parse(last) if last else None
age = (now - last_dt).total_seconds() if last_dt else None
counts = meta.get("counts") or {}
events = meta.get("events") or []
tokens = meta.get("clob_token_ids") or []
gaps = meta.get("gaps") or []
print(f"  started_at:     {meta.get('started_at')}   stopped_at: {meta.get('stopped_at')}")
print(f"  last_record_at: {last}   age: {'%.0fs' % age if age is not None else 'n/a'}")
print(f"  counts:         {counts}   open events: {len(events)}   tokens: {len(tokens)}")
if gaps:
    g = gaps[-1]
    print(
        f"  gaps (this process): {len(gaps)}   last: {g.get('disconnected_at')} "
        f"down {g.get('downtime_seconds')}s ({g.get('note')})"
    )
else:
    print("  gaps (this process): 0")
if meta.get("stopped_at"):
    print("  NOTE: stopped_at is set -> the recorder finalized this run (restarting, or down)")
if age is None:
    print("  STALE: no last_record_at in meta.json (nothing recorded yet?)")
    sys.exit(1)
if age > stale_after:
    print(f"  STALE: last record {age:.0f}s ago (> {stale_after:.0f}s)")
    sys.exit(1)
print("  ok: fresh")
PY

echo "== files ($RUN_DIR)"
if [ -d "$RUN_DIR" ]; then
    found=0
    for f in "$RUN_DIR"/book*.jsonl "$RUN_DIR"/book*.jsonl.*; do
        [ -e "$f" ] || continue
        found=1
        printf '  %8s  %s\n' "$(du -h "$f" 2>/dev/null | cut -f1)" "${f#"$RUN_DIR"/}"
    done
    [ "$found" = 1 ] || echo "  (no book files yet)"
    if [ -d "$RUN_DIR/segments" ]; then
        n_seg=$(find "$RUN_DIR/segments" -mindepth 1 -maxdepth 1 -type f 2>/dev/null | wc -l)
        echo "  segments/: $n_seg file(s), $(du -sh "$RUN_DIR/segments" 2>/dev/null | cut -f1); newest:"
        ls -t "$RUN_DIR/segments" 2>/dev/null | head -5 | while IFS= read -r s; do
            printf '  %8s  segments/%s\n' "$(du -h "$RUN_DIR/segments/$s" 2>/dev/null | cut -f1)" "$s"
        done
    fi
    n_native=$(find "$RUN_DIR/matches" -mindepth 1 -maxdepth 1 -type d -name 'event-*' 2>/dev/null | wc -l)
    n_offloaded=$(find "$RUN_DIR/matches" -mindepth 1 -maxdepth 1 -name 'event-*.offloaded.json' 2>/dev/null | wc -l)
    echo "  per-match: $n_native native dir(s), $n_offloaded offloaded marker(s)"
    echo "  run total: $(du -sh "$RUN_DIR" 2>/dev/null | cut -f1)"
else
    echo "  (run dir missing)"
fi

echo "== timers"
if command -v systemctl >/dev/null 2>&1; then
    systemctl list-timers --all --no-pager 'polytape-*' 2>/dev/null | sed 's/^/  /'
else
    echo "  (systemctl not available here)"
fi

echo "== disk"
df -h "$RUN_DIR" 2>/dev/null | sed 's/^/  /' || df -h / | sed 's/^/  /'

if [ "$rc" -ne 0 ]; then
    echo "RESULT: UNHEALTHY (see freshness above)"
else
    echo "RESULT: healthy"
fi
exit "$rc"
