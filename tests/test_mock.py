"""Tests for the offline dry-run mock pipeline."""

from __future__ import annotations

import json

from polytape.mock import run_dry


async def test_dry_run_full_pipeline(make_config):
    cfg = make_config(event_id="demo", dry_run=True)
    assert await run_dry(cfg) == 0

    edir = cfg.event_dir
    book = (edir / "book.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(book) == 5  # duplicates deduped

    for line in book:
        rec = json.loads(line)
        assert set(rec) == {"stream", "id", "ts_recv", "ts_server", "raw"}
        assert rec["stream"] == "book" and rec["ts_recv"].endswith("Z")

    meta = json.loads((edir / "meta.json").read_text(encoding="utf-8"))
    assert meta["counts"] == {"book": 5}
    assert meta["stopped_at"]
