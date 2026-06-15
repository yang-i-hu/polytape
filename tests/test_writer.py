"""Tests for the JSONL writer: dedup, JSONL output, meta.json, gap log."""

from __future__ import annotations

import json

import pytest

from polytape.envelope import Hasher
from polytape.writer import CaptureWriter, _downtime_seconds


def _read(path):
    return path.read_text(encoding="utf-8").splitlines()


def test_write_dedup_and_counts(make_config):
    cfg = make_config(book=False)
    with CaptureWriter(cfg) as w:
        assert w.write("comments", {"payload": {"id": "a"}}) is True
        assert w.write("comments", {"payload": {"id": "a"}}) is False  # dup
        assert w.write("comments", {"payload": {"id": "b"}}) is True
        assert w.counts == {"comments": 2}
        assert w.seen_count("comments") == 2


def test_jsonl_well_formed_dual_timestamps(make_config):
    cfg = make_config(book=False)
    with CaptureWriter(cfg) as w:
        w.write("comments", {"payload": {"id": "a", "createdAt": "2025-01-01T00:00:00Z"}})
    lines = _read(cfg.event_dir / "comments.jsonl")
    rec = json.loads(lines[0])
    assert set(rec) == {"stream", "id", "ts_recv", "ts_server", "raw"}
    assert rec["ts_recv"].endswith("Z") and rec["ts_server"].endswith("Z")


def test_meta_json_contents(make_config, sample_event):
    cfg = make_config()
    with CaptureWriter(cfg, event_info=sample_event, hasher=Hasher(salt="s")) as w:
        w.write("comments", {"payload": {"id": "a"}})
        w.write("book", {"event_type": "book", "hash": "0xH"})
    meta = json.loads((cfg.event_dir / "meta.json").read_text(encoding="utf-8"))
    assert meta["event_id"] == "20200"
    assert meta["market_ids"] == ["0xc1"]
    assert meta["clob_token_ids"] == ["t1", "t2"]
    assert meta["streams"] == ["comments", "book"]
    assert meta["hashing"]["enabled"] is True and len(meta["hashing"]["salt_fingerprint"]) == 8
    assert meta["counts"] == {"comments": 1, "book": 1}
    assert meta["started_at"] and meta["stopped_at"]
    assert meta["event"]["markets"][0]["clobTokenIds"] == ["t1", "t2"]
    assert not (cfg.event_dir / "meta.json.tmp").exists()  # atomic write cleans up


def test_no_hash_records_verbatim(make_config):
    cfg = make_config(book=False)
    with CaptureWriter(cfg, hasher=None) as w:  # hashing disabled
        w.write("comments", {"payload": {"id": "a", "userAddress": "0xWALLET"}})
    rec = json.loads(_read(cfg.event_dir / "comments.jsonl")[0])
    assert rec["raw"]["payload"]["userAddress"] == "0xWALLET"


def test_record_gap_downtime(make_config):
    cfg = make_config(book=False)
    with CaptureWriter(cfg) as w:
        gap = w.record_gap(
            "comments",
            "2026-06-15T19:31:02.000000Z",
            "2026-06-15T19:31:07.500000Z",
            backfilled=4,
            note="reconnect",
        )
    assert gap["downtime_seconds"] == 5.5 and gap["backfilled"] == 4


def test_downtime_seconds_helper():
    assert _downtime_seconds("2026-01-01T00:00:00Z", "2026-01-01T00:00:02Z") == 2.0
    assert _downtime_seconds("bad", "2026-01-01T00:00:02Z") is None


def test_append_mode_preserves_prior_lines(make_config):
    cfg = make_config(book=False)
    with CaptureWriter(cfg) as w:
        w.write("comments", {"payload": {"id": "a"}})
    with CaptureWriter(cfg) as w:  # new run, same dir -> append
        w.write("comments", {"payload": {"id": "b"}})
    assert len(_read(cfg.event_dir / "comments.jsonl")) == 2


def test_write_before_open_raises(make_config):
    w = CaptureWriter(make_config(book=False))
    with pytest.raises(RuntimeError, match="not open"):
        w.write("comments", {"payload": {"id": "a"}})
