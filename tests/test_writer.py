"""Tests for the JSONL writer: dedup, JSONL output, meta.json, gap log."""

from __future__ import annotations

import json

import pytest

from polytape.envelope import Hasher
from polytape.writer import CaptureWriter, FatalRecorderError, _downtime_seconds


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


class _FullDisk:
    """A file stub that fails every write/flush as if the disk were full (ENOSPC)."""

    def write(self, *_args):
        raise OSError(28, "No space left on device")

    def flush(self):
        raise OSError(28, "No space left on device")

    def close(self):
        pass


def test_write_full_disk_is_fatal(make_config):
    cfg = make_config(book=False)
    with CaptureWriter(cfg) as w:
        w._files["comments"].close()
        w._files["comments"] = _FullDisk()  # simulate ENOSPC on the data file
        with pytest.raises(FatalRecorderError):
            w.write("comments", {"payload": {"id": "a"}})


def test_meta_write_full_disk_is_fatal(make_config, monkeypatch):
    cfg = make_config(book=False)
    with CaptureWriter(cfg) as w:

        def _boom(*_a, **_k):
            raise OSError(28, "No space left on device")

        monkeypatch.setattr("polytape.writer.os.replace", _boom)
        with pytest.raises(FatalRecorderError):
            w.record_gap("comments", "2026-06-15T19:31:02Z", "2026-06-15T19:31:07Z")


def test_seen_set_is_bounded(make_config, monkeypatch):
    monkeypatch.setattr("polytape.writer._SEEN_CAP", 3)
    cfg = make_config(book=False)
    with CaptureWriter(cfg) as w:
        for i in range(5):
            assert w.write("comments", {"payload": {"id": f"c{i}"}}) is True
        assert w.seen_count("comments") <= 3
        # the oldest ids were evicted -> writable again (no longer "seen")
        assert w.write("comments", {"payload": {"id": "c0"}}) is True
        # a still-recent id is correctly rejected as a duplicate
        assert w.write("comments", {"payload": {"id": "c4"}}) is False


def test_multi_event_counts_and_envelope_shape(make_config):
    cfg = make_config()
    with CaptureWriter(cfg) as w:
        assert w.write("comments", {"payload": {"id": "a"}}, event_id="1001") is True
        assert w.write("comments", {"payload": {"id": "b"}}, event_id="1002") is True
        assert w.write("book", {"event_type": "book", "hash": "0xH"}, event_id="1001") is True
    rec = json.loads(_read(cfg.event_dir / "comments.jsonl")[0])
    # the documented 5-key envelope contract survives the multi-event path
    assert set(rec) == {"stream", "id", "ts_recv", "ts_server", "raw"}
    meta = json.loads((cfg.event_dir / "meta.json").read_text(encoding="utf-8"))
    assert meta["counts"] == {"comments": 2, "book": 1}
    assert meta["counts_by_event"] == {
        "1001": {"comments": 1, "book": 1},
        "1002": {"comments": 1},
    }


def _meta(cfg):
    return json.loads((cfg.event_dir / "meta.json").read_text(encoding="utf-8"))


def test_counts_cumulative_across_restart(make_config):
    # The recorder appends to the same files across restarts (refresh roll-over), so
    # meta counts must CONTINUE from the prior process, not reset to 0 and undercount.
    cfg = make_config(book=False)
    with CaptureWriter(cfg) as w:
        w.write("comments", {"payload": {"id": "a"}}, event_id="7")
        w.write("comments", {"payload": {"id": "b"}}, event_id="7")
    assert _meta(cfg)["counts"]["comments"] == 2
    with CaptureWriter(cfg) as w:  # restart, same dir
        assert w.counts["comments"] == 2  # seeded from meta on open(), not reset
        w.write("comments", {"payload": {"id": "c"}}, event_id="7")
    m = _meta(cfg)
    assert m["counts"]["comments"] == 3  # cumulative across the restart
    assert m["counts_by_event"]["7"]["comments"] == 3


def test_meta_has_freshness_fields(make_config):
    cfg = make_config(book=False)
    with CaptureWriter(cfg) as w:
        w.write("comments", {"payload": {"id": "a"}}, event_id="7")
    m = _meta(cfg)
    assert m["last_record_at"] is not None
    assert m["last_ts_by_event"]["7"] == m["last_record_at"]  # last per-event == overall


def test_flush_meta_persists_fresh_counts_without_close(make_config):
    # The periodic flusher keeps meta.json current mid-run so the admin needs no scan.
    cfg = make_config(book=False)
    with CaptureWriter(cfg) as w:
        w.write("comments", {"payload": {"id": "a"}})
        w.write("comments", {"payload": {"id": "b"}})
        assert w.flush_meta() is True
        assert _meta(cfg)["counts"]["comments"] == 2  # on disk before close()


def test_seed_ignores_corrupt_meta(make_config):
    # A garbage meta.json must not crash open() or poison the counters — start fresh.
    cfg = make_config(book=False)
    cfg.event_dir.mkdir(parents=True, exist_ok=True)
    (cfg.event_dir / "meta.json").write_text("{ not valid json", encoding="utf-8")
    with CaptureWriter(cfg) as w:
        assert w.counts.get("comments", 0) == 0
        w.write("comments", {"payload": {"id": "a"}})
        assert w.counts["comments"] == 1
