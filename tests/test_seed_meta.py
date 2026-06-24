"""Tests for the one-time meta-counts baseline script (scripts/seed_meta_counts.py)."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

_SPEC = importlib.util.spec_from_file_location(
    "seed_meta_counts",
    Path(__file__).resolve().parents[1] / "scripts" / "seed_meta_counts.py",
)
seed = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(seed)


def _rec(stream, rid, raw, ts="2026-06-20T00:00:00.000000Z"):
    return {"stream": stream, "id": rid, "ts_recv": ts, "ts_server": ts, "raw": raw}


def _setup(tmp_path):
    # Open match 1001 (cond 0xA1) in meta; finished match 0900 (cond 0xZ) only in registry.
    (tmp_path / "meta.json").write_text(
        json.dumps(
            {
                "started_at": "2026-06-19T00:00:00Z",
                "events": [{"id": "1001", "title": "A vs B", "markets": [{"conditionId": "0xA1"}]}],
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "registry.json").write_text(
        json.dumps(
            {
                "events": [
                    {"event_id": "0900", "markets": [{"conditionId": "0xZ"}]},
                    {"event_id": "1001", "markets": [{"conditionId": "0xA1"}]},
                ]
            }
        ),
        encoding="utf-8",
    )
    book = [
        _rec("book", "b1", {"event_type": "book", "market": "0xA1"}, ts="2026-06-20T00:00:01Z"),
        _rec("book", "b2", {"event_type": "book", "market": "0xZ"}, ts="2026-06-20T00:00:02Z"),
        _rec("book", "b3", {"event_type": "book", "market": "0xZ"}, ts="2026-06-20T00:00:09Z"),
        _rec("book", "b4", {"event_type": "book", "market": "0xUNKNOWN"}),  # no event -> total only
    ]
    (tmp_path / "book.jsonl").write_text(
        "".join(json.dumps(r) + "\n" for r in book), encoding="utf-8"
    )
    (tmp_path / "comments.jsonl").write_text(
        json.dumps(_rec("comments", "c1", {"payload": {"parentEntityID": 11433}})) + "\n",
        encoding="utf-8",
    )


def test_compute_counts_and_attribution(tmp_path):
    _setup(tmp_path)
    out = seed.compute(tmp_path, tmp_path / "registry.json")
    assert out["counts"] == {"book": 4, "comments": 1}  # totals incl. the unattributed record
    assert out["counts_by_event"]["1001"]["book"] == 1  # via meta open set
    assert out["counts_by_event"]["0900"]["book"] == 2  # finished match via the registry
    assert out["counts_by_event"]["11433"]["comments"] == 1  # comment parent (series id)
    assert out["last_record_at"] == "2026-06-20T00:00:09Z"  # newest ts overall


def test_main_writes_meta_preserving_other_fields(tmp_path):
    _setup(tmp_path)
    rc = seed.main(["--run-dir", str(tmp_path), "--registry-file", str(tmp_path / "registry.json")])
    assert rc == 0
    meta = json.loads((tmp_path / "meta.json").read_text(encoding="utf-8"))
    assert meta["counts"] == {"book": 4, "comments": 1}
    assert meta["counts_by_event"]["0900"]["book"] == 2
    assert meta["started_at"] == "2026-06-19T00:00:00Z"  # untouched
    assert meta["events"][0]["id"] == "1001"  # untouched
    assert not (tmp_path / "meta.json.seed.tmp").exists()  # atomic write cleans up


def test_dry_run_does_not_write(tmp_path):
    _setup(tmp_path)
    before = (tmp_path / "meta.json").read_text(encoding="utf-8")
    seed.main(
        [
            "--run-dir",
            str(tmp_path),
            "--registry-file",
            str(tmp_path / "registry.json"),
            "--dry-run",
        ]
    )
    assert (tmp_path / "meta.json").read_text(encoding="utf-8") == before  # unchanged
