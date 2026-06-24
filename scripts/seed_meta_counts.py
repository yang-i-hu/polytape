#!/usr/bin/env python3
"""One-time: seed accurate cumulative counts into an existing run's ``meta.json``.

The metadata-driven admin dashboard reads per-match message counts from ``meta.json``.
A run recorded BEFORE the recorder began maintaining cumulative counts has stale/partial
counts there (the old recorder reset them to 0 on every restart). This scans the run's
append-only JSONL files ONCE and writes accurate totals + per-event counts + last-seen
timestamps into ``meta.json``, so the upgraded recorder — which seeds its counters from
``meta.json`` on open() — continues from a correct baseline.

Run it ONCE per run during the upgrade. To avoid a recording gap, do the slow scan while
the recorder is LIVE (writing a side file) and only the fast merge in the stop->start
window (meta.json is owned by the running recorder, so the merge must not race it):

    # while the recorder is LIVE — the scan causes no recording gap:
    .../python scripts/seed_meta_counts.py --out /tmp/seed.json
    # then the brief upgrade window:
    systemctl stop polytape
    .../python scripts/seed_meta_counts.py --apply /tmp/seed.json   # fast, no scan
    systemctl start polytape   # upgraded recorder seeds the accurate counts

(Or, if a recording gap is acceptable, run with no flags between stop and start to scan +
merge in one shot.) Reads stream line by line, so memory stays tiny regardless of file
size. ``--dry-run`` prints the computed counts without writing.

Attribution mirrors the recorder/reader: book records by top-level ``market`` (condition
id -> event, via the registry + the meta open set); comments by ``parentEntityID``.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Callable
from pathlib import Path


def _cond_to_event(run_dir: Path, registry_file: Path) -> dict[str, str]:
    """conditionId -> event_id, from the cumulative registry plus the meta open set.

    The registry covers FINISHED matches (their conditionIds left meta.events on roll-out);
    the meta open set is authoritative for currently-open markets and wins on overlap.
    """
    cond2event: dict[str, str] = {}
    try:
        rdata = json.loads(registry_file.read_text(encoding="utf-8"))
        revents = rdata.get("events") if isinstance(rdata, dict) else rdata
        for e in revents or []:
            if not isinstance(e, dict):
                continue
            eid = str(e.get("event_id") or e.get("id") or "")
            for m in e.get("markets") or []:
                cond = m.get("conditionId")
                if cond and str(cond) not in cond2event:
                    cond2event[str(cond)] = eid
    except (OSError, json.JSONDecodeError, TypeError):
        pass
    try:
        meta = json.loads((run_dir / "meta.json").read_text(encoding="utf-8"))
        for e in meta.get("events") or []:
            eid = str(e.get("id"))
            for m in e.get("markets") or []:
                cond = m.get("conditionId")
                if cond:
                    cond2event[str(cond)] = eid  # meta wins (current markets)
    except (OSError, json.JSONDecodeError):
        pass
    return cond2event


def _comment_event(raw: dict) -> str | None:
    core = raw.get("payload") if isinstance(raw.get("payload"), dict) else raw
    parent = core.get("parentEntityID") if isinstance(core, dict) else None
    return str(parent) if parent is not None else None


def _scan(
    path: Path,
    stream: str,
    attribute: Callable[[dict], str | None],
    counts: dict[str, int],
    by_event: dict[str, dict[str, int]],
    last_ts: dict[str, str],
) -> str | None:
    """Stream one JSONL file, tallying total + per-event counts and per-event last ts.
    Returns the overall newest ts seen in this file (or None)."""
    n = 0
    overall: str | None = None
    try:
        handle = open(path, "rb")
    except OSError:
        counts[stream] = counts.get(stream, 0)
        return None
    with handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            n += 1
            raw = rec.get("raw") or {}
            ts = rec.get("ts_recv")
            ev = attribute(raw)
            if ev:
                bucket = by_event.setdefault(ev, {})
                bucket[stream] = bucket.get(stream, 0) + 1
                if ts and (ev not in last_ts or ts > last_ts[ev]):
                    last_ts[ev] = ts
            if ts and (overall is None or ts > overall):
                overall = ts
    counts[stream] = n
    return overall


def compute(run_dir: Path, registry_file: Path) -> dict:
    cond2event = _cond_to_event(run_dir, registry_file)
    counts: dict[str, int] = {}
    by_event: dict[str, dict[str, int]] = {}
    last_ts: dict[str, str] = {}
    book_last = _scan(
        run_dir / "book.jsonl",
        "book",
        lambda raw: cond2event.get(str(raw.get("market"))),
        counts,
        by_event,
        last_ts,
    )
    comments_last = _scan(
        run_dir / "comments.jsonl", "comments", _comment_event, counts, by_event, last_ts
    )
    last_record_at = max([t for t in (book_last, comments_last) if t], default=None)
    return {
        "counts": counts,
        "counts_by_event": by_event,
        "last_ts_by_event": last_ts,
        "last_record_at": last_record_at,
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--run-dir", default=os.environ.get("POLYTAPE_RUN_DIR", "/data/run-wc"))
    ap.add_argument(
        "--registry-file",
        default=os.environ.get("POLYTAPE_REGISTRY_FILE", "/var/log/polytape-admin/registry.json"),
    )
    ap.add_argument("--dry-run", action="store_true", help="print computed counts; do not write")
    ap.add_argument(
        "--out",
        metavar="FILE",
        help="Scan and write the computed seed to FILE (JSON), WITHOUT touching meta.json. "
        "Run this while the recorder is LIVE (the slow scan causes no recording gap), then "
        "apply it in the brief stop->start window with --apply.",
    )
    ap.add_argument(
        "--apply",
        metavar="FILE",
        help="Merge a seed FILE written by --out into meta.json (fast, no scan). Run in the "
        "recorder's stop->start window so a live writer can't race the merge.",
    )
    args = ap.parse_args(argv)

    run_dir = Path(args.run_dir)
    if args.apply:  # fast path: merge a pre-computed seed, no scan
        seed = json.loads(Path(args.apply).read_text(encoding="utf-8"))
    else:
        seed = compute(run_dir, Path(args.registry_file))
    print(
        f"book={seed['counts'].get('book', 0)} comments={seed['counts'].get('comments', 0)} "
        f"events={len(seed['counts_by_event'])} last_record_at={seed['last_record_at']}",
        file=sys.stderr,
    )
    if args.dry_run:
        print(json.dumps(seed, ensure_ascii=False, indent=2))
        return 0
    if args.out:  # write the side file; meta.json is left untouched (applied later)
        Path(args.out).write_text(
            json.dumps(seed, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        print(f"wrote seed {args.out}", file=sys.stderr)
        return 0

    meta_path = run_dir / "meta.json"
    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        if not isinstance(meta, dict):
            meta = {}
    except (OSError, json.JSONDecodeError):
        meta = {}
    meta.update(seed)
    tmp = meta_path.with_name("meta.json.seed.tmp")
    tmp.write_text(json.dumps(meta, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp, meta_path)
    print(f"seeded {meta_path}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
