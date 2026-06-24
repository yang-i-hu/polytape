"""Tests for normalize_book_event + Reconstructor, incl. the SHARED mock vector.

Folding ``polytape.mock.synthetic_book_frames`` keeps the viewer's understanding
of the wire format locked to the recorder's own test vector.
"""

from __future__ import annotations

import json

from polytape.envelope import extract_id
from polytape.mock import synthetic_book_frames
from polytape.viewer.book import OrderBook
from polytape.viewer.reconstruct import (
    Reconstructor,
    apply_change_to_book,
    is_book_change,
    normalize_book_event,
)


def _flatten(frames):
    """The mock emits JSON strings; some are arrays of messages."""
    out = []
    for frame in frames:
        data = json.loads(frame)
        out.extend(data if isinstance(data, list) else [data])
    return out


def test_normalize_snapshot():
    changes = normalize_book_event(
        {
            "event_type": "book",
            "asset_id": "100",
            "bids": [{"price": "0.5", "size": "100"}],
            "asks": [{"price": "0.6", "size": "80"}],
        }
    )
    assert changes == [
        {"kind": "snapshot", "asset_id": "100", "bids": [(0.5, 100.0)], "asks": [(0.6, 80.0)]}
    ]


def test_normalize_price_change_reads_asset_per_element():
    # NO top-level asset_id; one line fans out to two assets.
    changes = normalize_book_event(
        {
            "event_type": "price_change",
            "market": "0xC",
            "timestamp": "1",
            "price_changes": [
                {"asset_id": "100", "price": "0.5", "size": "200", "side": "BUY", "hash": "a"},
                {"asset_id": "200", "price": "0.5", "size": "0", "side": "SELL", "hash": "b"},
            ],
        }
    )
    assert [c["asset_id"] for c in changes] == ["100", "200"]
    assert changes[0] == {
        "kind": "delta",
        "asset_id": "100",
        "side": "BUY",
        "price": 0.5,
        "size": 200.0,
    }
    assert changes[1]["size"] == 0.0 and changes[1]["side"] == "SELL"


def test_normalize_trade_and_tick():
    trade = normalize_book_event(
        {
            "event_type": "last_trade_price",
            "asset_id": "100",
            "price": "0.5",
            "size": "10",
            "side": "BUY",
            "transaction_hash": "0xT",
        }
    )[0]
    assert trade["kind"] == "trade" and trade["tx"] == "0xT" and not is_book_change(trade)
    tick = normalize_book_event(
        {"event_type": "tick_size_change", "asset_id": "100", "new_tick_size": "0.001"}
    )[0]
    assert tick == {"kind": "tick", "asset_id": "100", "tick_size": 0.001}


def test_unknown_event_type_ignored():
    assert normalize_book_event({"event_type": "best_bid_ask"}) == []
    assert normalize_book_event({"event_type": "book"}) == []  # no asset_id


def test_apply_change_size_zero_deletes_level():
    book = OrderBook()
    apply_change_to_book(book, {"kind": "delta", "side": "SELL", "price": 0.42, "size": 80.0})
    assert book.asks == {0.42: 80.0}
    apply_change_to_book(book, {"kind": "delta", "side": "SELL", "price": 0.42, "size": 0.0})
    assert book.asks == {}


def test_reconstructor_folds_shared_mock_vector():
    recon = Reconstructor()
    seen = set()  # dedup by id, exactly as the recorder does before writing
    for raw in _flatten(synthetic_book_frames()):
        rid = extract_id("book", raw)
        if rid in seen:  # the vector ends with a duplicate snapshot
            continue
        seen.add(rid)
        for change in normalize_book_event(raw):
            recon.apply(change, ts="2026-01-01T00:00:00Z")

    # asset 100: snapshot (0.5/100 bid, 0.6/80 ask) then price_change BUY 0.55 size 40.
    b100 = recon.books["100"]
    assert b100.bids == {0.5: 100.0, 0.55: 40.0}
    assert b100.asks == {0.6: 80.0}
    # the last_trade_price did NOT add a level; asks unchanged.
    assert b100.best_ask() == 0.6
    # asset 200: empty snapshot only.
    assert recon.books["200"].is_empty()


def test_snapshot_resets_only_that_asset():
    recon = Reconstructor()
    recon.apply({"kind": "snapshot", "asset_id": "100", "bids": [(0.4, 10)], "asks": []}, ts="t1")
    recon.apply({"kind": "snapshot", "asset_id": "200", "bids": [(0.6, 5)], "asks": []}, ts="t2")
    # fresh snapshot for 100 clears only 100
    recon.apply({"kind": "snapshot", "asset_id": "100", "bids": [(0.45, 99)], "asks": []}, ts="t3")
    assert recon.books["100"].bids == {0.45: 99}
    assert recon.books["200"].bids == {0.6: 5}


def test_non_finite_sizes_rejected():
    # inf / overflow / nan sizes must never enter the book (would poison metrics,
    # emit invalid JSON, or be misread as a delete).
    snap = normalize_book_event(
        {
            "event_type": "book",
            "asset_id": "100",
            "bids": [
                {"price": "0.5", "size": "inf"},
                {"price": "0.49", "size": "1e400"},
                {"price": "0.48", "size": "100"},
            ],
            "asks": [{"price": "0.6", "size": "nan"}],
        }
    )[0]
    assert snap["bids"] == [(0.48, 100.0)]  # inf and overflow dropped
    assert snap["asks"] == []  # nan dropped
    # a nan-size delta must not be emitted at all (so it can't masquerade as a delete)
    assert (
        normalize_book_event(
            {
                "event_type": "price_change",
                "price_changes": [
                    {"asset_id": "100", "price": "0.5", "size": "nan", "side": "BUY", "hash": "h"}
                ],
            }
        )
        == []
    )


def test_delta_before_snapshot_is_unseeded():
    recon = Reconstructor()
    recon.apply(
        {"kind": "delta", "asset_id": "100", "side": "BUY", "price": 0.4, "size": 10}, ts="t"
    )
    assert "100" in recon.unseeded
    recon.apply({"kind": "snapshot", "asset_id": "100", "bids": [(0.4, 10)], "asks": []}, ts="t2")
    assert "100" not in recon.unseeded
