"""Tests for the read-only admin RunReader (offline; no fastapi required)."""

from __future__ import annotations

import json

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
    # live (recent) sorts ahead of pending (never seen)
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
