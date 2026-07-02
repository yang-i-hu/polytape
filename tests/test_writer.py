"""Tests for the JSONL writer: dedup, JSONL output, meta.json, gap log."""

from __future__ import annotations

import json

import pytest

from polytape.writer import CaptureWriter, FatalRecorderError, _downtime_seconds


def _read(path):
    return path.read_text(encoding="utf-8").splitlines()


def _book(h: str) -> dict:
    return {"event_type": "book", "hash": h}


def test_write_dedup_and_counts(make_config):
    cfg = make_config()
    with CaptureWriter(cfg) as w:
        assert w.write("book", _book("a")) is True
        assert w.write("book", _book("a")) is False  # dup
        assert w.write("book", _book("b")) is True
        assert w.counts == {"book": 2}
        assert w.seen_count("book") == 2


def test_jsonl_well_formed_dual_timestamps(make_config):
    cfg = make_config()
    with CaptureWriter(cfg) as w:
        w.write("book", {"event_type": "book", "hash": "a", "timestamp": "1700000000000"})
    lines = _read(cfg.event_dir / "book.jsonl")
    rec = json.loads(lines[0])
    assert set(rec) == {"stream", "id", "ts_recv", "ts_server", "raw"}
    assert rec["ts_recv"].endswith("Z") and rec["ts_server"].endswith("Z")


def test_meta_json_contents(make_config, sample_event):
    cfg = make_config()
    with CaptureWriter(cfg, event_info=sample_event) as w:
        w.write("book", _book("0xH"))
    meta = json.loads((cfg.event_dir / "meta.json").read_text(encoding="utf-8"))
    assert meta["event_id"] == "20200"
    assert meta["market_ids"] == ["0xc1"]
    assert meta["clob_token_ids"] == ["t1", "t2"]
    assert meta["streams"] == ["book"]
    assert meta["counts"] == {"book": 1}
    assert meta["started_at"] and meta["stopped_at"]
    assert meta["event"]["markets"][0]["clobTokenIds"] == ["t1", "t2"]
    assert not (cfg.event_dir / "meta.json.tmp").exists()  # atomic write cleans up


def test_record_gap_downtime(make_config):
    cfg = make_config()
    with CaptureWriter(cfg) as w:
        gap = w.record_gap(
            "book",
            "2026-06-15T19:31:02.000000Z",
            "2026-06-15T19:31:07.500000Z",
            note="reconnect",
        )
    assert gap["downtime_seconds"] == 5.5 and gap["note"] == "reconnect"


def test_downtime_seconds_helper():
    assert _downtime_seconds("2026-01-01T00:00:00Z", "2026-01-01T00:00:02Z") == 2.0
    assert _downtime_seconds("bad", "2026-01-01T00:00:02Z") is None


def test_append_mode_preserves_prior_lines(make_config):
    cfg = make_config()
    with CaptureWriter(cfg) as w:
        w.write("book", _book("a"))
    with CaptureWriter(cfg) as w:  # new run, same dir -> append
        w.write("book", _book("b"))
    assert len(_read(cfg.event_dir / "book.jsonl")) == 2


def test_write_before_open_raises(make_config):
    w = CaptureWriter(make_config())
    with pytest.raises(RuntimeError, match="not open"):
        w.write("book", _book("a"))


class _FullDisk:
    """A file stub that fails every write/flush as if the disk were full (ENOSPC)."""

    def write(self, *_args):
        raise OSError(28, "No space left on device")

    def flush(self):
        raise OSError(28, "No space left on device")

    def close(self):
        pass


def test_write_full_disk_is_fatal(make_config):
    cfg = make_config()
    with CaptureWriter(cfg) as w:
        w._files["book"].close()
        w._files["book"] = _FullDisk()  # simulate ENOSPC on the data file
        with pytest.raises(FatalRecorderError):
            w.write("book", _book("a"))


def test_meta_write_full_disk_is_fatal(make_config, monkeypatch):
    cfg = make_config()
    with CaptureWriter(cfg) as w:

        def _boom(*_a, **_k):
            raise OSError(28, "No space left on device")

        monkeypatch.setattr("polytape.writer.os.replace", _boom)
        with pytest.raises(FatalRecorderError):
            w.record_gap("book", "2026-06-15T19:31:02Z", "2026-06-15T19:31:07Z")


def test_seen_set_is_bounded(make_config, monkeypatch):
    monkeypatch.setattr("polytape.writer._SEEN_CAP", 3)
    cfg = make_config()
    with CaptureWriter(cfg) as w:
        for i in range(5):
            assert w.write("book", _book(f"c{i}")) is True
        assert w.seen_count("book") <= 3
        # the oldest ids were evicted -> writable again (no longer "seen")
        assert w.write("book", _book("c0")) is True
        # a still-recent id is correctly rejected as a duplicate
        assert w.write("book", _book("c4")) is False


def test_multi_event_counts_and_envelope_shape(make_config):
    cfg = make_config()
    with CaptureWriter(cfg) as w:
        assert w.write("book", _book("a"), event_id="1001") is True
        assert w.write("book", _book("b"), event_id="1002") is True
        assert w.write("book", _book("c"), event_id="1001") is True
    rec = json.loads(_read(cfg.event_dir / "book.jsonl")[0])
    # the documented 5-key envelope contract survives the multi-event path
    assert set(rec) == {"stream", "id", "ts_recv", "ts_server", "raw"}
    meta = json.loads((cfg.event_dir / "meta.json").read_text(encoding="utf-8"))
    assert meta["counts"] == {"book": 3}
    assert meta["counts_by_event"] == {
        "1001": {"book": 2},
        "1002": {"book": 1},
    }


def _meta(cfg):
    return json.loads((cfg.event_dir / "meta.json").read_text(encoding="utf-8"))


def test_counts_cumulative_across_restart(make_config):
    # The recorder appends to the same files across restarts (refresh roll-over), so
    # meta counts must CONTINUE from the prior process, not reset to 0 and undercount.
    cfg = make_config()
    with CaptureWriter(cfg) as w:
        w.write("book", _book("a"), event_id="7")
        w.write("book", _book("b"), event_id="7")
    assert _meta(cfg)["counts"]["book"] == 2
    with CaptureWriter(cfg) as w:  # restart, same dir
        assert w.counts["book"] == 2  # seeded from meta on open(), not reset
        w.write("book", _book("c"), event_id="7")
    m = _meta(cfg)
    assert m["counts"]["book"] == 3  # cumulative across the restart
    assert m["counts_by_event"]["7"]["book"] == 3


def test_meta_has_freshness_fields(make_config):
    cfg = make_config()
    with CaptureWriter(cfg) as w:
        w.write("book", _book("a"), event_id="7")
    m = _meta(cfg)
    assert m["last_record_at"] is not None
    assert m["last_ts_by_event"]["7"] == m["last_record_at"]  # last per-event == overall


def test_flush_meta_persists_fresh_counts_without_close(make_config):
    # The periodic flusher keeps meta.json current mid-run so the admin needs no scan.
    cfg = make_config()
    with CaptureWriter(cfg) as w:
        w.write("book", _book("a"))
        w.write("book", _book("b"))
        assert w.flush_meta() is True
        assert _meta(cfg)["counts"]["book"] == 2  # on disk before close()


def test_seed_ignores_corrupt_meta(make_config):
    # A garbage meta.json must not crash open() or poison the counters — start fresh.
    cfg = make_config()
    cfg.event_dir.mkdir(parents=True, exist_ok=True)
    (cfg.event_dir / "meta.json").write_text("{ not valid json", encoding="utf-8")
    with CaptureWriter(cfg) as w:
        assert w.counts.get("book", 0) == 0
        w.write("book", _book("a"))
        assert w.counts["book"] == 1


# -- per-match dual write (primary) + monolith (backup) --------------------- #


def _pm(cfg, eid, stream="book"):
    return cfg.event_dir / "matches" / f"event-{eid}" / f"{stream}.jsonl"


def test_per_match_dual_write_matches_monolith(make_config):
    # Every event-tagged record lands in BOTH the monolith (all records) and its
    # per-match file (just that event), byte-identical — nothing lost or misrouted.
    cfg = make_config()
    with CaptureWriter(cfg) as w:
        assert w.write("book", _book("h1"), event_id="1001") is True
        assert w.write("book", _book("h2"), event_id="1002") is True
        assert w.write("book", _book("h3"), event_id="1001") is True
    mono = _read(cfg.event_dir / "book.jsonl")
    e1, e2 = _read(_pm(cfg, "1001")), _read(_pm(cfg, "1002"))
    assert len(mono) == 3  # backup has everything
    assert len(e1) == 2 and len(e2) == 1  # primary split by event
    assert set(e1) | set(e2) == set(mono)  # identical lines, fully partitioned


def test_per_match_dedup_is_shared_with_monolith(make_config):
    # A duplicate is rejected once and written to NEITHER file (shared seen set).
    cfg = make_config()
    with CaptureWriter(cfg) as w:
        assert w.write("book", _book("dup"), event_id="1001") is True
        assert w.write("book", _book("dup"), event_id="1001") is False
    assert len(_read(cfg.event_dir / "book.jsonl")) == 1
    assert len(_read(_pm(cfg, "1001"))) == 1


def test_per_match_disabled_writes_only_monolith(make_config):
    cfg = make_config(per_match=False)
    with CaptureWriter(cfg) as w:
        w.write("book", _book("h1"), event_id="1001")
    assert len(_read(cfg.event_dir / "book.jsonl")) == 1
    assert not (cfg.event_dir / "matches").exists()


def test_per_match_untagged_record_skips_per_match(make_config):
    # A record with no event_id only goes to the monolith (no per-match dir to pick).
    cfg = make_config()
    with CaptureWriter(cfg) as w:
        w.write("book", _book("a"))  # no event_id
    assert len(_read(cfg.event_dir / "book.jsonl")) == 1
    assert not (cfg.event_dir / "matches").exists()


def test_per_match_meta_is_self_contained(make_config, sample_event):
    cfg = make_config()
    with CaptureWriter(cfg, event_info=sample_event) as w:
        w.write("book", _book("h1"), event_id="20200")
    meta = json.loads((cfg.event_dir / "matches" / "event-20200" / "meta.json").read_text("utf-8"))
    assert meta["event_id"] == "20200"
    assert meta["counts"] == {"book": 1}
    assert meta["event"]["markets"][0]["clobTokenIds"] == ["t1", "t2"]
    assert meta["started_at"] and meta["stopped_at"]
    assert not (cfg.event_dir / "matches" / "event-20200" / "meta.json.tmp").exists()


def test_per_match_write_full_disk_is_fatal_but_backup_kept(make_config):
    # If the per-match write fails (ENOSPC), it is fatal — but the monolith backup has
    # already captured the record (source of truth), so nothing is silently dropped.
    cfg = make_config()
    with CaptureWriter(cfg) as w:
        assert w.write("book", _book("h1"), event_id="1001") is True
        w._event_files[("book", "1001")].close()
        w._event_files[("book", "1001")] = _FullDisk()  # ENOSPC on the per-match file
        with pytest.raises(FatalRecorderError):
            w.write("book", _book("h2"), event_id="1001")
    assert len(_read(cfg.event_dir / "book.jsonl")) == 2  # backup kept both records
