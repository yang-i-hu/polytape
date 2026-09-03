"""Tests for the JSONL writer: dedup, JSONL output, meta.json, gap log, daily segments."""

from __future__ import annotations

import json

import pytest

from polytape.writer import (
    CaptureWriter,
    FatalRecorderError,
    _downtime_seconds,
    parse_segment_name,
    segment_name,
)


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


# -- daily monolith segments (multi-event runs rotate; single-event does not) -- #


class _Clock:
    """An injectable ``now`` that replays the given stamps in order, then repeats the last.

    The writer reads it once in ``open()`` (started_at), once per ``write()`` (ts_recv)
    and once in ``close()`` (stopped_at) — nowhere else.
    """

    def __init__(self, *stamps: str) -> None:
        self._stamps = list(stamps)

    def __call__(self) -> str:
        if len(self._stamps) > 1:
            return self._stamps.pop(0)
        return self._stamps[0]


def _seg(cfg, day):
    return cfg.event_dir / segment_name("book", day)


def _ids(lines):
    return [json.loads(line)["id"] for line in lines]


def test_segment_name_helpers():
    assert segment_name("book", "2026-09-03") == "book.2026-09-03.jsonl"
    assert parse_segment_name("book.2026-09-03.jsonl") == ("book", "2026-09-03")
    assert parse_segment_name("book.jsonl") is None  # the legacy monolith
    assert parse_segment_name("comments.jsonl") is None
    assert parse_segment_name("book.2026-09-03.jsonl.zst") is None  # offload scratch
    assert parse_segment_name("book.2026-09-03.offloaded.json") is None  # marker
    assert parse_segment_name("book.notaday.jsonl") is None


def test_multi_event_run_rotates_monolith_at_utc_day_boundary(make_config):
    # An open-ended run's monolith is one segment per UTC day of ts_recv: the first
    # record of a new day rolls the file; counts stay cumulative; per-match natives
    # are untouched by rotation; meta lists the open + seen segments.
    cfg = make_config(run_name="camp")  # run_name -> multi-event layout -> rotation
    clock = _Clock(
        "2026-09-02T23:59:58.000000Z",  # open(): started_at
        "2026-09-02T23:59:59.500000Z",  # a -> still 09-02
        "2026-09-03T00:00:00.100000Z",  # b -> rolls to 09-03
        "2026-09-03T00:00:01.000000Z",  # c -> 09-03
        "2026-09-03T00:00:02.000000Z",  # close(): stopped_at
    )
    with CaptureWriter(cfg, now=clock) as w:
        assert w.write("book", _book("a"), event_id="7") is True
        assert w.segments == {
            "book": {"current": "book.2026-09-02.jsonl", "seen": ["book.2026-09-02.jsonl"]}
        }
        assert w.write("book", _book("b"), event_id="7") is True
        assert w.segments["book"]["current"] == "book.2026-09-03.jsonl"
        # the roll-over itself refreshes meta.json so #segments is current at once
        assert _meta(cfg)["segments"]["book"]["current"] == "book.2026-09-03.jsonl"
        assert w.write("book", _book("c"), event_id="8") is True
        assert w.counts == {"book": 3}

    assert _ids(_read(_seg(cfg, "2026-09-02"))) == ["a"]
    assert _ids(_read(_seg(cfg, "2026-09-03"))) == ["b", "c"]
    assert not (cfg.event_dir / "book.jsonl").exists()  # never a legacy monolith
    # per-match PRIMARY files are unaffected by the monolith's rotation
    assert _ids(_read(_pm(cfg, "7"))) == ["a", "b"]
    assert _ids(_read(_pm(cfg, "8"))) == ["c"]
    all_segment_lines = _read(_seg(cfg, "2026-09-02")) + _read(_seg(cfg, "2026-09-03"))
    assert set(all_segment_lines) == set(_read(_pm(cfg, "7")) + _read(_pm(cfg, "8")))
    m = _meta(cfg)
    assert m["counts"] == {"book": 3}  # cumulative across segments
    assert m["counts_by_event"] == {"7": {"book": 2}, "8": {"book": 1}}
    assert m["segments"] == {
        "book": {
            "current": "book.2026-09-03.jsonl",
            "seen": ["book.2026-09-02.jsonl", "book.2026-09-03.jsonl"],
        }
    }


def test_restart_appends_to_the_days_segment_and_counts_stay_cumulative(make_config):
    # The refresh timer restarts the recorder routinely: the new process opens the
    # same day's segment in append mode and continues the tally seeded from meta.
    cfg = make_config(run_name="camp")
    with CaptureWriter(cfg, now=_Clock("2026-09-03T10:00:00.000000Z")) as w:
        w.write("book", _book("a"), event_id="7")
    with CaptureWriter(cfg, now=_Clock("2026-09-03T10:10:00.000000Z")) as w:
        assert w.counts["book"] == 1  # seeded, not reset
        w.write("book", _book("b"), event_id="7")
    assert _ids(_read(_seg(cfg, "2026-09-03"))) == ["a", "b"]
    m = _meta(cfg)
    assert m["counts"] == {"book": 2}
    # "seen" is per PROCESS (this one only opened today's segment)
    assert m["segments"]["book"]["seen"] == ["book.2026-09-03.jsonl"]


def test_legacy_monolith_is_left_alone_by_a_rotating_run(make_config):
    # A run upgraded in place still has the pre-rotation book.jsonl: it is never
    # appended to again — new records go to the day's segment.
    cfg = make_config(run_name="wc")
    cfg.event_dir.mkdir(parents=True)
    legacy = cfg.event_dir / "book.jsonl"
    legacy.write_text('{"legacy": true}\n', encoding="utf-8", newline="")
    with CaptureWriter(cfg, now=_Clock("2026-09-03T10:00:00.000000Z")) as w:
        w.write("book", _book("a"))
    assert legacy.read_text(encoding="utf-8") == '{"legacy": true}\n'
    assert _ids(_read(_seg(cfg, "2026-09-03"))) == ["a"]


def test_single_event_capture_keeps_legacy_monolith_layout(make_config):
    # Single-event captures (monitor/viewer tools) keep event-<id>/book.jsonl.
    cfg = make_config()
    with CaptureWriter(cfg, now=_Clock("2026-09-03T10:00:00.000000Z")) as w:
        w.write("book", _book("a"))
        assert w.segments == {}
    assert _ids(_read(cfg.event_dir / "book.jsonl")) == ["a"]
    assert list(cfg.event_dir.glob("book.*.jsonl")) == []
    assert _meta(cfg)["segments"] == {}


def test_segment_roll_over_failure_is_fatal_and_keeps_prior_records(make_config):
    cfg = make_config(run_name="camp")
    clock = _Clock(
        "2026-09-02T23:59:59.000000Z",
        "2026-09-02T23:59:59.500000Z",
        "2026-09-03T00:00:00.000000Z",
    )
    with CaptureWriter(cfg, now=clock) as w:
        w.write("book", _book("a"))
        _seg(cfg, "2026-09-03").mkdir()  # the next segment's path cannot be opened
        with pytest.raises(FatalRecorderError, match="roll-over"):
            w.write("book", _book("b"))
        assert w.counts == {"book": 1}  # the failed record was not counted
    assert _ids(_read(_seg(cfg, "2026-09-02"))) == ["a"]  # closed cleanly on shutdown


def test_write_envelope_with_unparseable_ts_stays_in_open_segment(make_config):
    # A hand-built envelope without a usable ts_recv never rolls (or crashes) — it
    # lands in whatever segment is open.
    cfg = make_config(run_name="camp")
    with CaptureWriter(cfg, now=_Clock("2026-09-03T10:00:00.000000Z")) as w:
        for rid, ts in (("x", None), ("y", "garbage"), ("z", 1234)):
            env = {"stream": "book", "id": rid, "ts_recv": ts, "ts_server": None, "raw": {}}
            assert w.write_envelope(env) is True
    assert _ids(_read(_seg(cfg, "2026-09-03"))) == ["x", "y", "z"]
    assert sorted(cfg.event_dir.glob("book.*.jsonl")) == [_seg(cfg, "2026-09-03")]
