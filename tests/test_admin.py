"""Tests for the read-only admin RunReader (offline; no fastapi required)."""

from __future__ import annotations

import json

import pytest

from polytape.admin.reader import RunReader
from polytape.envelope import utc_now_iso


def _meta() -> dict:
    def mk(c):
        return {"id": "m", "conditionId": c, "clobTokenIds": ["x", "y"]}

    return {
        "started_at": "2026-06-19T16:20:00.000000Z",
        "events": [
            {
                "id": "1001",
                "title": "A vs. B",
                "slug": "fifwc-a-b-2026-06-19",
                "markets": [mk("0xA1"), mk("0xA2"), mk("0xA3")],
            },
            {
                "id": "1002",
                "title": "C vs. D",
                "slug": "fifwc-c-d-2026-06-20",
                "markets": [mk("0xB1"), mk("0xB2"), mk("0xB3")],
            },
        ],
    }


def _book(market: str) -> dict:
    ts = utc_now_iso()
    return {
        "stream": "book",
        "id": f"{market}-{ts}",
        "ts_recv": ts,
        "ts_server": ts,
        "raw": {"event_type": "book", "market": market},
    }


def _comment(parent: int, cid: str) -> dict:
    ts = utc_now_iso()
    return {
        "stream": "comments",
        "id": cid,
        "ts_recv": ts,
        "ts_server": ts,
        "raw": {"type": "comment_created", "payload": {"id": cid, "parentEntityID": parent}},
    }


def _write_jsonl(path, records):
    path.write_text("".join(json.dumps(r) + "\n" for r in records), encoding="utf-8")


def _reader(tmp_path):
    (tmp_path / "meta.json").write_text(json.dumps(_meta()), encoding="utf-8")
    return RunReader(tmp_path, env_file=tmp_path / "missing.env")


def test_status_counts_and_coverage(tmp_path):
    _write_jsonl(tmp_path / "book.jsonl", [_book("0xA1"), _book("0xA1"), _book("0xB1")])
    _write_jsonl(tmp_path / "comments.jsonl", [_comment(1001, "c1")])
    r = _reader(tmp_path)
    r.update()
    st = r.status()
    assert st["records"] == {"comments": 1, "book": 3}
    assert st["open_matches"] == 2
    assert st["coverage"] == {"seen": 2, "total": 6}  # 0xA1, 0xB1 of 6 condition ids
    assert st["last_record_age_s"] is not None and st["last_record_age_s"] < 60


def test_matches_per_event_recency_and_status(tmp_path):
    _write_jsonl(tmp_path / "book.jsonl", [_book("0xA1"), _book("0xA2")])
    _write_jsonl(tmp_path / "comments.jsonl", [_comment(1001, "c1")])
    r = _reader(tmp_path)
    r.update()
    by_id = {m["event_id"]: m for m in r.matches()}
    assert by_id["1001"]["counts"] == {"book": 2, "comments": 1}
    assert by_id["1001"]["title"] == "A vs. B" and by_id["1001"]["date"] == "2026-06-19"
    assert by_id["1001"]["status"] == "live"
    assert by_id["1002"]["status"] == "pending" and by_id["1002"]["counts"] == {}
    # rows are in schedule-date order now (1001's date precedes 1002's), not by recency
    assert r.matches()[0]["event_id"] == "1001"


def test_incremental_reads_only_new_bytes(tmp_path):
    book = tmp_path / "book.jsonl"
    _write_jsonl(book, [_book("0xA1")])
    (tmp_path / "comments.jsonl").write_text("", encoding="utf-8")
    r = _reader(tmp_path)
    r.update()
    assert r.status()["records"]["book"] == 1
    off1 = r._offsets["book"]
    with open(book, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(_book("0xB1")) + "\n")
    r.update()
    assert r.status()["records"]["book"] == 2
    assert r._offsets["book"] > off1


def test_partial_trailing_line_not_consumed(tmp_path):
    book = tmp_path / "book.jsonl"
    rec1, rec2 = json.dumps(_book("0xA1")), json.dumps(_book("0xB1"))
    book.write_text(rec1 + "\n" + rec2, encoding="utf-8")  # rec2 has no trailing newline
    (tmp_path / "comments.jsonl").write_text("", encoding="utf-8")
    r = _reader(tmp_path)
    r.update()
    assert r.status()["records"]["book"] == 1  # half-written line is not consumed
    with open(book, "a", encoding="utf-8") as fh:
        fh.write("\n")
    r.update()
    assert r.status()["records"]["book"] == 2


def test_heartbeat_armed_from_env_file(tmp_path):
    (tmp_path / "meta.json").write_text(json.dumps(_meta()), encoding="utf-8")
    env = tmp_path / "p.env"
    env.write_text(
        "POLYTAPE_SALT=s\nPOLYTAPE_HEARTBEAT_URL=https://hc-ping.com/x\n", encoding="utf-8"
    )
    assert RunReader(tmp_path, env_file=env).status()["heartbeat_armed"] is True
    env.write_text("POLYTAPE_SALT=s\n", encoding="utf-8")
    assert RunReader(tmp_path, env_file=env).status()["heartbeat_armed"] is False


def test_match_view_reconstructs_book(tmp_path):
    meta = {
        "started_at": "2026-06-19T16:20:00.000000Z",
        "events": [
            {
                "id": "1001",
                "title": "A vs. B",
                "slug": "fifwc-a-b-2026-06-19",
                "markets": [{"id": "m", "conditionId": "0xA1", "clobTokenIds": ["YT", "NT"]}],
            }
        ],
    }
    (tmp_path / "meta.json").write_text(json.dumps(meta), encoding="utf-8")

    def rec(i, raw):
        ts = utc_now_iso()
        return {"stream": "book", "id": str(i), "ts_recv": ts, "ts_server": ts, "raw": raw}

    recs = [
        rec(
            0,
            {
                "event_type": "book",
                "market": "0xA1",
                "asset_id": "YT",
                "bids": [{"price": "0.88", "size": "100"}, {"price": "0.87", "size": "50"}],
                "asks": [{"price": "0.92", "size": "80"}],
            },
        ),
        rec(
            1,
            {
                "event_type": "price_change",
                "market": "0xA1",
                "price_changes": [{"asset_id": "YT", "price": "0.89", "size": "40", "side": "BUY"}],
            },
        ),
        rec(
            2,
            {
                "event_type": "price_change",
                "market": "0xA1",
                "price_changes": [{"asset_id": "YT", "price": "0.87", "size": "0", "side": "BUY"}],
            },
        ),
        rec(
            3,
            {
                "event_type": "last_trade_price",
                "market": "0xA1",
                "asset_id": "YT",
                "price": "0.905",
                "size": "10",
                "side": "BUY",
            },
        ),
    ]
    _write_jsonl(tmp_path / "book.jsonl", recs)
    (tmp_path / "comments.jsonl").write_text("", encoding="utf-8")
    r = RunReader(tmp_path, env_file=tmp_path / "x.env", matches_file=tmp_path / "x.json")
    r.update()
    view = r.match_view("1001")
    assert view["title"] == "A vs. B"
    mk = view["markets"][0]
    assert mk["best_bid"] == 0.89 and mk["best_ask"] == 0.92 and mk["mid"] == 0.905
    prices = [b["price"] for b in mk["bids"]]
    assert (
        0.89 in prices and 0.88 in prices and 0.87 not in prices
    )  # delta added 0.89, removed 0.87
    assert mk["asks"][0]["price"] == 0.92
    assert mk["last_trade"]["price"] == "0.905"
    assert len(mk["price_hist"]) == 1 and mk["price_hist"][0]["p"] == 0.905


def test_gaps_surfaced_in_status_and_live(tmp_path):
    meta = _meta()
    meta["gaps"] = [
        {
            "stream": "book",
            "disconnected_at": "2026-06-19T16:00:00Z",
            "reconnected_at": "2026-06-19T16:00:03Z",
            "downtime_seconds": 3.0,
            "backfilled": 0,
            "note": "reconnect",
        },
        {
            "stream": "comments",
            "disconnected_at": "2026-06-19T16:10:00Z",
            "reconnected_at": "2026-06-19T16:10:01Z",
            "downtime_seconds": 1.0,
            "backfilled": 2,
            "note": "reconnect",
        },
    ]
    (tmp_path / "meta.json").write_text(json.dumps(meta), encoding="utf-8")
    _write_jsonl(tmp_path / "book.jsonl", [_book("0xA1")])
    (tmp_path / "comments.jsonl").write_text("", encoding="utf-8")
    r = RunReader(tmp_path, env_file=tmp_path / "missing.env")
    r.update()
    assert r.status()["gaps"] == 2
    gaps = r.live()["gaps"]
    assert len(gaps) == 2 and gaps[0]["stream"] == "book" and gaps[1]["backfilled"] == 2


def test_rates_computed_from_count_deltas(tmp_path):
    clock = [100.0]
    (tmp_path / "meta.json").write_text(json.dumps(_meta()), encoding="utf-8")
    book = tmp_path / "book.jsonl"
    _write_jsonl(book, [_book("0xA1"), _book("0xA1")])
    (tmp_path / "comments.jsonl").write_text("", encoding="utf-8")
    r = RunReader(tmp_path, env_file=tmp_path / "missing.env", mono=lambda: clock[0])
    r.update()  # first tick sets the baseline; no rate yet
    assert r.live()["rates"] == {}
    with open(book, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(_book("0xA1")) + "\n")  # event 1001
        fh.write(json.dumps(_book("0xB1")) + "\n")  # event 1002
    clock[0] = 102.0  # 2s later
    r.update()
    rates = r.live()["rates"]
    assert rates["window_s"] == 2.0
    assert rates["by_stream"]["book"] == 1.0  # 2 new book records / 2s
    assert rates["by_event"]["1001"]["book"] == 0.5 and rates["by_event"]["1002"]["book"] == 0.5


def test_peek_ring_holds_recent_records(tmp_path):
    (tmp_path / "meta.json").write_text(json.dumps(_meta()), encoding="utf-8")
    _write_jsonl(tmp_path / "book.jsonl", [_book("0xA1"), _book("0xB1")])
    _write_jsonl(tmp_path / "comments.jsonl", [_comment(1001, "c1")])
    r = RunReader(tmp_path, env_file=tmp_path / "missing.env", peek=10)
    r.update()
    recent = r.live()["recent"]
    assert len(recent) == 3  # 2 book (consumed first) + 1 comment
    book_rec = next(x for x in recent if x["stream"] == "book")
    assert book_rec["kind"] == "book" and book_rec["eid"] in ("1001", "1002")
    comment_rec = next(x for x in recent if x["stream"] == "comments")
    assert comment_rec["kind"] == "comment_created"
    assert comment_rec["eid"] == "1001" and comment_rec["title"] == "A vs. B"


def test_peek_ring_capped(tmp_path):
    (tmp_path / "meta.json").write_text(json.dumps(_meta()), encoding="utf-8")
    _write_jsonl(tmp_path / "book.jsonl", [_book("0xA1"), _book("0xA1"), _book("0xA1")])
    (tmp_path / "comments.jsonl").write_text("", encoding="utf-8")
    r = RunReader(tmp_path, env_file=tmp_path / "missing.env", peek=2)
    r.update()
    assert len(r.live()["recent"]) == 2  # oldest evicted (FIFO)


def test_systemctl_cached_once_per_tick(tmp_path):
    (tmp_path / "meta.json").write_text(json.dumps(_meta()), encoding="utf-8")
    _write_jsonl(tmp_path / "book.jsonl", [_book("0xA1")])
    (tmp_path / "comments.jsonl").write_text("", encoding="utf-8")
    r = RunReader(tmp_path, env_file=tmp_path / "missing.env")
    calls = [0]

    def fake():
        calls[0] += 1
        return {"active": "active", "restarts": 0, "since": None}

    r._systemctl = fake  # type: ignore[method-assign]
    r.update()
    assert calls[0] == 1  # one subprocess per update() tick
    r.status()
    r.status()
    r.status()
    assert calls[0] == 1  # status() reads the cache, never re-spawns
    assert r.status()["recorder"]["active"] == "active"


def test_consume_reads_in_bounded_chunks(tmp_path):
    # A small read cap forces multi-tick catch-up: the first scan must NOT load the
    # whole file at once (the OOM bug), but the backlog still drains fully over ticks.
    (tmp_path / "meta.json").write_text(json.dumps(_meta()), encoding="utf-8")
    _write_jsonl(tmp_path / "book.jsonl", [_book("0xA1") for _ in range(20)])
    (tmp_path / "comments.jsonl").write_text("", encoding="utf-8")
    size = (tmp_path / "book.jsonl").stat().st_size
    cap = max(size // 6, 250)  # ~6 chunks, but always larger than one record line
    r = RunReader(tmp_path, env_file=tmp_path / "x.env", max_read_bytes=cap)
    r.update()
    first = r.status()["records"]["book"]
    assert 0 < first < 20  # bounded — not all 20 consumed in one read
    for _ in range(20):  # plenty of ticks to drain the backlog
        r.update()
    assert r.status()["records"]["book"] == 20  # fully caught up
    assert r._offsets["book"] == size  # offset reached EOF


# --------------------------------------------------------------------------- #
# Scan checkpoint: resume forward on restart instead of re-draining from byte 0
# --------------------------------------------------------------------------- #


def _reader_ck(tmp_path, ckpt, **kw):
    """A reader (with meta.json written) that persists its scan checkpoint to ``ckpt``."""
    (tmp_path / "meta.json").write_text(json.dumps(_meta()), encoding="utf-8")
    return RunReader(tmp_path, env_file=tmp_path / "missing.env", checkpoint_file=ckpt, **kw)


def _seed_checkpoint(tmp_path, ckpt):
    """Write two book + one comment record and persist a valid checkpoint over them."""
    _write_jsonl(tmp_path / "book.jsonl", [_book("0xA1"), _book("0xB1")])
    _write_jsonl(tmp_path / "comments.jsonl", [_comment(1001, "c1")])
    r = _reader_ck(tmp_path, ckpt)
    r.update()
    assert r.save_checkpoint() is True
    return r


def test_checkpoint_resume_equals_full_drain(tmp_path):
    # Golden test: state rebuilt by resuming from a checkpoint (then reading only the
    # appended tail) must EXACTLY equal state built by a full drain of the final log.
    book = tmp_path / "book.jsonl"
    comments = tmp_path / "comments.jsonl"
    _write_jsonl(book, [_book("0xA1"), _book("0xA1"), _book("0xB1")])
    _write_jsonl(comments, [_comment(1001, "c1"), _comment(1002, "c2")])
    ckpt = tmp_path / "reader-checkpoint.json"

    warm = _reader_ck(tmp_path, ckpt)
    warm.update()
    assert warm.save_checkpoint() is True
    saved_book_off = json.loads(ckpt.read_text(encoding="utf-8"))["offsets"]["book"]

    # Append more records to BOTH streams after the checkpoint was taken.
    with open(book, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(_book("0xA2")) + "\n")
        fh.write(json.dumps(_book("0xB1")) + "\n")
    with open(comments, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(_comment(1001, "c3")) + "\n")

    resumed = _reader_ck(tmp_path, ckpt)
    assert resumed.load_checkpoint() is True
    assert resumed._offsets["book"] == saved_book_off  # resumed AT the boundary, not 0
    resumed.update()

    ref = _reader(tmp_path)  # fresh full drain over the same final files
    ref.update()

    assert resumed.status()["records"] == ref.status()["records"] == {"book": 5, "comments": 3}
    assert {m["event_id"]: m["counts"] for m in resumed.matches()} == {
        m["event_id"]: m["counts"] for m in ref.matches()
    }
    assert resumed._markets_seen == ref._markets_seen
    assert resumed._last_ts == ref._last_ts
    assert resumed._offsets == ref._offsets  # both reached the same EOF, no double-count


def test_checkpoint_warm_start_reads_only_tail(tmp_path):
    # A warm start catches up to the live edge in ONE bounded tick (it reads only the
    # small tail), where a cold start would need many ticks to re-drain the big prefix.
    book = tmp_path / "book.jsonl"
    _write_jsonl(book, [_book("0xA1") for _ in range(40)])
    (tmp_path / "comments.jsonl").write_text("", encoding="utf-8")
    prefix_size = book.stat().st_size
    cap = max(prefix_size // 8, 250)  # ~8 bounded ticks to cold-drain the prefix
    ckpt = tmp_path / "reader-checkpoint.json"

    warm = _reader_ck(tmp_path, ckpt, max_read_bytes=cap)
    for _ in range(40):  # drain the whole prefix, then checkpoint at EOF
        warm.update()
    assert warm._offsets["book"] == prefix_size
    assert warm.save_checkpoint() is True

    with open(book, "a", encoding="utf-8") as fh:  # a small fresh tail (< cap)
        fh.write(json.dumps(_book("0xB1")) + "\n")
        fh.write(json.dumps(_book("0xB1")) + "\n")
    full_size = book.stat().st_size

    resumed = _reader_ck(tmp_path, ckpt, max_read_bytes=cap)
    assert resumed.load_checkpoint() is True
    resumed.update()  # a SINGLE tick
    assert resumed._offsets["book"] == full_size  # already at the live edge
    assert resumed.status()["records"]["book"] == 42

    # Contrast: a cold reader is still mid-drain after the same single tick.
    cold = RunReader(tmp_path, env_file=tmp_path / "missing.env", max_read_bytes=cap)
    cold.update()
    assert cold.status()["records"]["book"] < 42


def test_checkpoint_rejected_when_corrupt(tmp_path):
    ckpt = tmp_path / "reader-checkpoint.json"
    _write_jsonl(tmp_path / "book.jsonl", [_book("0xA1"), _book("0xB1")])
    (tmp_path / "comments.jsonl").write_text("", encoding="utf-8")
    ckpt.write_text("{ this is not valid json", encoding="utf-8")
    r = _reader_ck(tmp_path, ckpt)
    assert r.load_checkpoint() is False
    assert r._offsets == {}  # nothing adopted
    r.update()
    assert r.status()["records"]["book"] == 2  # full re-drain from byte 0


def test_checkpoint_rejected_on_schema_change(tmp_path):
    ckpt = tmp_path / "reader-checkpoint.json"
    _seed_checkpoint(tmp_path, ckpt)
    data = json.loads(ckpt.read_text(encoding="utf-8"))
    data["schema"] = 999  # a payload shape this build does not understand
    ckpt.write_text(json.dumps(data), encoding="utf-8")
    r = _reader_ck(tmp_path, ckpt)
    assert r.load_checkpoint() is False
    assert r._offsets == {}


def test_checkpoint_resumes_across_recorder_restart(tmp_path):
    # A recorder restart APPENDS to the same book.jsonl (stable inode, file only grows)
    # but rewrites meta.json with a fresh started_at. The checkpoint MUST still resume —
    # the prefix we counted is byte-for-byte unchanged. started_at is advisory, not a gate;
    # gating on it would re-drain on every VM reboot / co-restart (the regression we fixed).
    ckpt = tmp_path / "reader-checkpoint.json"
    book = tmp_path / "book.jsonl"
    _seed_checkpoint(tmp_path, ckpt)  # 2 book + 1 comment, checkpoint started_at = default
    saved_off = json.loads(ckpt.read_text(encoding="utf-8"))["offsets"]["book"]
    with open(book, "a", encoding="utf-8") as fh:  # recorder appended after its restart
        fh.write(json.dumps(_book("0xA2")) + "\n")
    meta = _meta()
    meta["started_at"] = "2026-06-25T10:00:00.000000Z"  # fresh started_at from the restart
    (tmp_path / "meta.json").write_text(json.dumps(meta), encoding="utf-8")

    r = RunReader(tmp_path, env_file=tmp_path / "missing.env", checkpoint_file=ckpt)
    assert r.load_checkpoint() is True  # adopted despite the new started_at (same inode)
    assert r._offsets["book"] == saved_off
    r.update()
    assert r.status()["records"]["book"] == 3  # 2 resumed + 1 appended; no re-drain, no double


def test_checkpoint_rejected_when_counts_without_offset(tmp_path):
    # Defense-in-depth: a checkpoint that claims book counts but a zero/absent book offset
    # would re-read book.jsonl from byte 0 on the next tick and DOUBLE-count on top of the
    # adopted total. _checkpoint_is_current must reject it (offset<->count consistency).
    ckpt = tmp_path / "reader-checkpoint.json"
    _seed_checkpoint(tmp_path, ckpt)  # offsets.book > 0, counts.book == 2
    data = json.loads(ckpt.read_text(encoding="utf-8"))
    data["offsets"]["book"] = 0  # counts.book stays 2 -> inconsistent (adopting would double)
    ckpt.write_text(json.dumps(data), encoding="utf-8")
    r = _reader_ck(tmp_path, ckpt)
    assert r.load_checkpoint() is False
    assert r._offsets == {}
    r.update()
    assert r.status()["records"]["book"] == 2  # full re-drain — not doubled

    # Same hole if the stream is dropped from offsets entirely.
    data = json.loads(ckpt.read_text(encoding="utf-8"))
    del data["offsets"]["book"]
    ckpt.write_text(json.dumps(data), encoding="utf-8")
    r2 = _reader_ck(tmp_path, ckpt)
    assert r2.load_checkpoint() is False


def test_checkpoint_rejected_when_offset_past_eof(tmp_path):
    ckpt = tmp_path / "reader-checkpoint.json"
    _seed_checkpoint(tmp_path, ckpt)
    saved_off = json.loads(ckpt.read_text(encoding="utf-8"))["offsets"]["book"]
    assert saved_off > 0
    # Truncate book.jsonl so the saved offset now points past EOF (rotation/truncation).
    (tmp_path / "book.jsonl").write_text(json.dumps(_book("0xA1")) + "\n", encoding="utf-8")
    assert (tmp_path / "book.jsonl").stat().st_size < saved_off
    r = _reader_ck(tmp_path, ckpt)
    assert r.load_checkpoint() is False
    assert r._offsets == {}


def test_checkpoint_rejected_on_file_replace(tmp_path):
    ckpt = tmp_path / "reader-checkpoint.json"
    _seed_checkpoint(tmp_path, ckpt)
    data = json.loads(ckpt.read_text(encoding="utf-8"))
    # Same run + size still covers the offset, but a DIFFERENT inode: the file was
    # rotated/replaced under us, so the prefix we counted is no longer trustworthy.
    data["files"]["book"]["ino"] = int(data["files"]["book"]["ino"]) + 1
    ckpt.write_text(json.dumps(data), encoding="utf-8")
    r = _reader_ck(tmp_path, ckpt)
    assert r.load_checkpoint() is False
    assert r._offsets == {}


def test_checkpoint_disabled_is_noop(tmp_path):
    _write_jsonl(tmp_path / "book.jsonl", [_book("0xA1")])
    (tmp_path / "comments.jsonl").write_text("", encoding="utf-8")
    r = _reader(tmp_path)  # no checkpoint_file configured
    assert r.save_checkpoint() is False
    assert r.load_checkpoint() is False
    assert not list(tmp_path.glob("*checkpoint*"))  # nothing written to disk


def test_checkpoint_save_skipped_without_run_identity(tmp_path):
    # No meta.json -> no started_at -> no run identity to validate a resume against, so
    # there is nothing safe to persist yet: save is a no-op and writes no file.
    ckpt = tmp_path / "reader-checkpoint.json"
    _write_jsonl(tmp_path / "book.jsonl", [_book("0xA1")])
    (tmp_path / "comments.jsonl").write_text("", encoding="utf-8")
    r = RunReader(tmp_path, env_file=tmp_path / "missing.env", checkpoint_file=ckpt)
    r.update()
    assert r.save_checkpoint() is False
    assert not ckpt.exists()


def test_app_lifespan_saves_and_resumes_checkpoint(tmp_path):
    # End-to-end through the real app lifespan: a clean shutdown writes the checkpoint,
    # and a fresh app's STARTUP loads it — reaching the live edge after a single bounded
    # read, which a cold byte-0 re-drain with this small cap provably cannot do.
    pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient

    from polytape.admin.app import create_app

    (tmp_path / "meta.json").write_text(json.dumps(_meta()), encoding="utf-8")
    book = tmp_path / "book.jsonl"
    _write_jsonl(book, [_book("0xA1") for _ in range(40)])
    (tmp_path / "comments.jsonl").write_text("", encoding="utf-8")
    prefix_size = book.stat().st_size
    cap = max(prefix_size // 8, 250)  # cold start needs ~8 bounded ticks to reach EOF
    ckpt = tmp_path / "reader-checkpoint.json"

    r1 = RunReader(
        tmp_path, env_file=tmp_path / "missing.env", checkpoint_file=ckpt, max_read_bytes=cap
    )
    for _ in range(40):  # drain the prefix deterministically, not via the poll loop
        r1.update()
    app1 = create_app(r1, poll_interval=3600, registry_refresh_s=0, extract_refresh_s=0)
    assert not ckpt.exists()
    with TestClient(app1):  # enter + exit the lifespan -> final save on shutdown
        pass
    assert ckpt.exists()

    with open(book, "a", encoding="utf-8") as fh:  # a small fresh tail (< cap)
        fh.write(json.dumps(_book("0xB1")) + "\n")
    full_size = book.stat().st_size

    r2 = RunReader(
        tmp_path, env_file=tmp_path / "missing.env", checkpoint_file=ckpt, max_read_bytes=cap
    )
    app2 = create_app(r2, poll_interval=3600, registry_refresh_s=0, extract_refresh_s=0)
    with TestClient(app2) as c:
        st = c.get("/api/status").json()
    assert st["records"]["book"] == 41  # resumed: prefix (40) + the tail (1)
    assert r2._offsets["book"] == full_size  # at the live edge after startup, not mid-drain
