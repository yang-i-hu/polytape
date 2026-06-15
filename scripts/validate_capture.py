"""Validate a polytape capture: well-formed envelopes carrying both timestamps.

Usage:
    python scripts/validate_capture.py data/event-<id>
    python scripts/validate_capture.py data        # scans for event-* subdirs

Exit code is 0 when every JSONL line is a valid envelope and each enabled
stream produced at least one line; non-zero otherwise. Intended for the manual
smoke test (see the README), but useful for spot-checking any capture.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

_ENVELOPE_KEYS = {"stream", "id", "ts_recv", "ts_server", "raw"}


def validate_event_dir(event_dir: Path) -> bool:
    """Validate one ``event-<id>`` directory. Returns True if it looks healthy."""
    ok = True
    meta_path = event_dir / "meta.json"
    if meta_path.exists():
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        print(
            f"  meta.json: event={meta.get('event_id')} streams={meta.get('streams')} "
            f"counts={meta.get('counts')} gaps={len(meta.get('gaps', []))} "
            f"hashing={meta.get('hashing', {}).get('enabled')}"
        )
    else:
        print("  WARNING: no meta.json")
        ok = False

    jsonl_files = sorted(event_dir.glob("*.jsonl"))
    if not jsonl_files:
        print("  WARNING: no .jsonl files")
        return False

    for path in jsonl_files:
        valid = with_server = malformed = 0
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                malformed += 1
                continue
            if set(rec) != _ENVELOPE_KEYS or not rec.get("ts_recv"):
                malformed += 1
                continue
            valid += 1
            if rec.get("ts_server"):
                with_server += 1
        status = "OK" if valid and not malformed else "PROBLEM"
        print(
            f"  {path.name}: {valid} valid, {with_server} with ts_server, "
            f"{malformed} malformed [{status}]"
        )
        if not valid or malformed:
            ok = False
    return ok


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate a polytape capture directory.")
    parser.add_argument("path", help="An event-<id> directory, or a data root containing one.")
    args = parser.parse_args(argv)

    root = Path(args.path)
    if (root / "meta.json").exists() or list(root.glob("*.jsonl")):
        targets = [root]
    else:
        targets = sorted(root.glob("event-*"))
    if not targets:
        print(f"no capture found under {root}")
        return 1

    ok = True
    for event_dir in targets:
        print(f"== {event_dir} ==")
        ok = validate_event_dir(event_dir) and ok
    print("RESULT:", "OK" if ok else "PROBLEMS FOUND")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
