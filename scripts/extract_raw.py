"""Unzip polytape archives from data/zipped/ into data/raw/event-<id>/ (one folder per event).

Each archive's per-event files (book.jsonl, meta.json, comments.jsonl, ...) are written flat to
data/raw/event-<id>/<file>, regardless of any intermediate directory inside the archive -- so
there is never a folder between data/raw/ and the event folder.

Idempotent + incremental: every run extracts into the SAME data/raw/, accumulating events across
archives/batches. The first time a run touches an event it wipes that event's existing folder, so
re-extracting a re-recording cleanly REPLACES the event (no stale files linger) while events not in
this run's archives are left untouched. Run the 2-match zip then the 20-match zip into one data/raw/
and you get the union, with the overlapping events replaced by whichever archive wrote them last.

Usage:
    uv run python scripts/extract_raw.py                       # all archives in data/zipped
    uv run python scripts/extract_raw.py data/zipped/foo.tar.gz   # a specific archive
    uv run python scripts/extract_raw.py --raw-dir data/raw --zipped-dir data/zipped
"""

from __future__ import annotations

import argparse
import shutil
import sys
import tarfile
from pathlib import Path


def _event_id(member_name: str) -> str | None:
    """Find the event-<id> component anywhere in a member path and return <id>."""
    for part in member_name.replace("\\", "/").split("/"):
        if part.startswith("event-") and len(part) > len("event-"):
            return part[len("event-") :]
    return None


def extract_archive(archive: Path, raw_dir: Path, cleared: set[str]) -> dict[str, int]:
    """Extract one archive; return {event_id: files_written}.

    `cleared` tracks events already wiped during this run: the first file seen for an event
    clears that event's existing folder (clean replace), so re-running over the same event
    discards stale files, while two archives that share an event in one run merge into it.
    """
    written: dict[str, int] = {}
    with tarfile.open(archive, "r:*") as tar:
        for member in tar:
            if not member.isfile():
                continue
            eid = _event_id(member.name)
            if eid is None:
                continue
            fh = tar.extractfile(member)
            if fh is None:
                continue
            dest_dir = raw_dir / f"event-{eid}"
            if eid not in cleared:  # first touch this run -> replace the whole event folder
                if dest_dir.exists():
                    shutil.rmtree(dest_dir)
                cleared.add(eid)
            dest_dir.mkdir(parents=True, exist_ok=True)
            dest = dest_dir / Path(member.name).name
            with open(dest, "wb") as out:
                shutil.copyfileobj(fh, out)
            written[eid] = written.get(eid, 0) + 1
    return written


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Unzip polytape archives into data/raw/event-<id>/.")
    ap.add_argument("archives", nargs="*", help="archive paths (default: all under --zipped-dir)")
    ap.add_argument(
        "--zipped-dir", default="data/zipped", help="where archives live (default data/zipped)"
    )
    ap.add_argument("--raw-dir", default="data/raw", help="output root (default data/raw)")
    args = ap.parse_args(argv)

    raw_dir = Path(args.raw_dir)
    if args.archives:
        archives = [Path(a) for a in args.archives]
    else:
        zd = Path(args.zipped_dir)
        archives = sorted(p for p in zd.glob("*") if p.name.endswith((".tar.gz", ".tgz", ".tar")))
    if not archives:
        print(f"no archives found (looked in {args.zipped_dir})", file=sys.stderr)
        return 1

    total = 0
    cleared: set[str] = set()  # events wiped this run, so each is replaced at most once
    for arc in archives:
        if not arc.exists():
            print(f"  [skip] missing {arc}", file=sys.stderr)
            continue
        print(f"== {arc} ==", file=sys.stderr, flush=True)
        written = extract_archive(arc, raw_dir, cleared)
        for eid, n in sorted(written.items()):
            print(f"   event-{eid}: {n} file(s) -> {raw_dir / ('event-' + eid)}", file=sys.stderr)
        total += len(written)
    print(f"\nExtracted {total} event folder(s) into {raw_dir}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
