"""Tests for the offline dry-run mock pipeline."""

from __future__ import annotations

import json

from polytape.mock import run_dry


async def test_dry_run_full_pipeline(make_config):
    cfg = make_config(event_id="demo", dry_run=True)
    assert await run_dry(cfg) == 0

    edir = cfg.event_dir
    comments = (edir / "comments.jsonl").read_text(encoding="utf-8").splitlines()
    book = (edir / "book.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(comments) == 5 and len(book) == 5  # duplicates deduped

    for label, lines in (("comments", comments), ("book", book)):
        for line in lines:
            rec = json.loads(line)
            assert set(rec) == {"stream", "id", "ts_recv", "ts_server", "raw"}
            assert rec["stream"] == label and rec["ts_recv"].endswith("Z")

    meta = json.loads((edir / "meta.json").read_text(encoding="utf-8"))
    assert meta["counts"] == {"comments": 5, "book": 5}
    assert meta["hashing"]["enabled"] is True
    assert len(meta["gaps"]) == 1 and meta["gaps"][0]["backfilled"] == 1
    assert meta["stopped_at"]

    # hashing applied to comment identifiers
    first = json.loads(comments[0])
    assert first["raw"]["payload"]["userAddress"] != "0xUSER0"
    assert first["raw"]["payload"]["profile"]["name"] != "user0"


async def test_dry_run_comments_only(make_config):
    cfg = make_config(event_id="demo", dry_run=True, book=False)
    assert await run_dry(cfg) == 0
    assert (cfg.event_dir / "comments.jsonl").exists()
    assert not (cfg.event_dir / "book.jsonl").exists()


async def test_dry_run_no_hash_keeps_identifiers(make_config):
    cfg = make_config(event_id="demo", dry_run=True, book=False, hash_usernames=False)
    await run_dry(cfg)
    first = json.loads(
        (cfg.event_dir / "comments.jsonl").read_text(encoding="utf-8").splitlines()[0]
    )
    assert first["raw"]["payload"]["userAddress"] == "0xUSER0"
