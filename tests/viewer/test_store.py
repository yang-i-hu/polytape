"""Tests for CaptureStore: keyframe replay == naive replay, series/trades, SSE fan-out."""

from __future__ import annotations

import json
import queue
import time
from datetime import timedelta

from polytape.envelope import iso_to_datetime
from polytape.viewer.book import OrderBook
from polytape.viewer.reconstruct import apply_change_to_book, is_book_change, normalize_book_event
from polytape.viewer.store import CaptureStore


def _started(event_dir, **kw):
    store = CaptureStore(event_dir, poll_interval=0.03, **kw)
    store.start()
    return store


def _naive_book(event_dir, asset, at_iso):
    """Full replay from line 0 up to ``at_iso`` (the reference the keyframe path must match)."""
    at_dt = iso_to_datetime(at_iso)
    book = OrderBook()
    for line in (event_dir / "book.jsonl").read_text(encoding="utf-8").splitlines():
        env = json.loads(line)
        if iso_to_datetime(env["ts_recv"]) > at_dt:
            break
        for change in normalize_book_event(env["raw"]):
            if change["asset_id"] == asset and is_book_change(change):
                apply_change_to_book(book, change, ts=env["ts_recv"])
    return book


def test_assets_and_latest_book(make_book_capture):
    store = _started(make_book_capture())
    try:
        assets = {a["asset_id"]: a for a in store.assets()}
        assert assets["100"]["outcome"] == "YES" and assets["100"]["present"]
        assert assets["200"]["outcome"] == "NO"
        latest = store.book_as_of("100", None)["book"]
        assert latest.bids == {0.45: 100.0} and latest.asks == {0.47: 100.0}  # post-gap snapshot
    finally:
        store.stop()


def test_keyframe_replay_matches_naive(make_book_capture):
    event_dir = make_book_capture()
    store = _started(event_dir, keyframe_every=1)  # force keyframe usage every change
    try:
        timestamps = [
            json.loads(line)["ts_recv"]
            for line in (event_dir / "book.jsonl").read_text(encoding="utf-8").splitlines()
        ]
        for at in timestamps:
            got = store.book_as_of("100", at)["book"]
            ref = _naive_book(event_dir, "100", at)
            assert got.bids == ref.bids and got.asks == ref.asks, at
    finally:
        store.stop()


def test_empty_asset_and_unknown(make_book_capture):
    store = _started(make_book_capture())
    try:
        empty = store.book_as_of("200", None)
        assert empty["book"].is_empty()
        assert store.book_as_of("does-not-exist", None) is None
    finally:
        store.stop()


def test_stale_after_gap_inside_window(make_book_capture):
    event_dir = make_book_capture()
    store = _started(event_dir)
    try:
        meta = store.meta()
        gap = [g for g in meta["gaps"] if g.get("stream") == "book"][0]
        disc = iso_to_datetime(gap["disconnected_at"])
        recon = iso_to_datetime(gap["reconnected_at"])
        mid = disc + (recon - disc) / 2
        in_gap = mid.strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"
        assert store.book_as_of("100", in_gap)["stale_after_gap"] is True
        # strictly before the gap: not stale
        before = (disc - timedelta(seconds=1)).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"
        assert store.book_as_of("100", before)["stale_after_gap"] is False
    finally:
        store.stop()


def test_series_and_trades(make_book_capture):
    store = _started(make_book_capture())
    try:
        points, gaps = store.series("100", None, None, 1000)
        assert points and set(points[0]) == {"ts", "mid", "bid", "ask", "spread", "micro"}
        assert len(gaps) == 1
        trades = store.trades("100", None, 10)
        assert len(trades) == 1 and trades[0]["side"] == "BUY" and trades[0]["tx"] == "0xT"
    finally:
        store.stop()


def test_sse_fanout_on_append(make_book_capture):
    event_dir = make_book_capture()
    store = _started(event_dir)
    try:
        sub = store.subscribe("100")
        line = (
            '{"stream":"book","id":"live1","ts_recv":"2026-01-01T02:00:00.000000Z","ts_server":null,'
            '"raw":{"event_type":"book","asset_id":"100","hash":"hL","timestamp":"99",'
            '"bids":[{"price":"0.50","size":"10"}],"asks":[{"price":"0.52","size":"10"}]}}'
        )
        with open(event_dir / "book.jsonl", "a", encoding="utf-8") as fh:
            fh.write(line + "\n")
        # the tailer should ingest and dispatch a state frame.
        deadline = time.time() + 3
        frame = None
        while time.time() < deadline:
            try:
                frame = sub.queue.get(timeout=0.2)
                break
            except queue.Empty:
                continue
        assert frame is not None
        assert frame["event"] in ("snapshot", "price_change")
        assert frame["data"]["asset_id"] == "100"
        assert frame["data"]["metrics"]["mid"] == 0.51
    finally:
        store.stop()
