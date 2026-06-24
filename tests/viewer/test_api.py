"""Tests for the API response builders (no socket)."""

from __future__ import annotations

import time

from polytape.viewer import api
from polytape.viewer.store import CaptureStore


def _started(event_dir):
    store = CaptureStore(event_dir, poll_interval=0.03)
    store.start()
    time.sleep(0.15)
    return store


def test_meta_response_shape(make_book_capture):
    store = _started(make_book_capture())
    try:
        meta = api.build_meta_response(store)
        assert meta["event_id"] == "20200"
        assert meta["live"] is False
        outcomes = {a["outcome"] for a in meta["assets"]}
        assert {"YES", "NO"} <= outcomes
        assert len(meta["gaps"]) == 1 and meta["gaps"][0]["stream"] == "book"
        assert meta["time_range"]["start"] and meta["time_range"]["end"]
    finally:
        store.stop()


def test_book_response_shape_and_metrics(make_book_capture):
    store = _started(make_book_capture())
    try:
        book = api.build_book_response(store, "100", None, 25)
        assert set(book) >= {
            "asset_id",
            "label",
            "as_of",
            "seq",
            "ladder",
            "metrics",
            "stale_after_gap",
            "book_unseeded",
        }
        assert book["label"] == "YES"
        bids = book["ladder"]["bids"]
        assert set(bids[0]) == {"price", "size", "cum"}
        m = book["metrics"]
        assert m["best_bid"] == 0.45 and m["best_ask"] == 0.47 and m["mid"] == 0.46
    finally:
        store.stop()


def test_book_response_empty_asset_null_metrics(make_book_capture):
    store = _started(make_book_capture())
    try:
        book = api.build_book_response(store, "200", None, 25)
        assert book["ladder"]["bids"] == [] and book["ladder"]["asks"] == []
        assert book["metrics"]["best_bid"] is None and book["metrics"]["mid"] is None
    finally:
        store.stop()


def test_book_response_unknown_asset_is_none(make_book_capture):
    store = _started(make_book_capture())
    try:
        assert api.build_book_response(store, "404", None, 25) is None
    finally:
        store.stop()


def test_series_and_trades_responses(make_book_capture):
    store = _started(make_book_capture())
    try:
        series = api.build_series_response(store, "100", None, None, 1500)
        assert series["asset_id"] == "100" and isinstance(series["points"], list)
        trades = api.build_trades_response(store, "100", None, 100)
        assert trades["trades"][0]["side"] == "BUY"
    finally:
        store.stop()


def test_event_summary_shape():
    summary = api.event_summary(
        "80505", {"stopped_at": None, "counts": {"book": 3}, "event": {"title": "T", "slug": "s"}}
    )
    assert summary == {
        "event_id": "80505",
        "dir": "event-80505",
        "title": "T",
        "slug": "s",
        "live": True,
        "counts": {"book": 3},
    }
    assert api.event_summary("x", None)["live"] is False
