"""Unit tests for the pure OrderBook model."""

from __future__ import annotations

from polytape.viewer.book import OrderBook


def test_reset_seeds_and_drops_nonpositive():
    b = OrderBook()
    b.reset_from_snapshot(
        [(0.40, 100), (0.39, 0)], [(0.42, 80), (0.43, -5)], ts="2026-01-01T00:00:00Z"
    )
    assert b.bids == {0.40: 100}
    assert b.asks == {0.42: 80}
    assert b.reset_ts == "2026-01-01T00:00:00Z"


def test_level_replace_set_update_delete():
    b = OrderBook()
    b.apply_level_replace("BUY", 0.40, 100)
    assert b.bids[0.40] == 100
    b.apply_level_replace("BUY", 0.40, 250)  # aggregate replace, not increment
    assert b.bids[0.40] == 250
    b.apply_level_replace("BUY", 0.40, 0)  # delete
    assert 0.40 not in b.bids
    b.apply_level_replace("SELL", 0.42, 80)
    assert b.asks[0.42] == 80


def test_best_bid_ask_and_totals():
    b = OrderBook()
    b.reset_from_snapshot([(0.40, 100), (0.39, 50)], [(0.42, 80), (0.43, 60)])
    assert b.best_bid() == 0.40
    assert b.best_ask() == 0.42
    assert b.total_bid() == 150
    assert b.total_ask() == 140
    assert not b.is_empty()


def test_empty_book_accessors():
    b = OrderBook()
    assert b.best_bid() is None and b.best_ask() is None
    assert b.is_empty()
    bids, asks, max_cum = b.levels()
    assert bids == [] and asks == [] and max_cum == 0.0


def test_levels_ordering_and_cumulative():
    b = OrderBook()
    b.reset_from_snapshot([(0.39, 50), (0.40, 100), (0.38, 25)], [(0.43, 60), (0.42, 80)])
    bids, asks, max_cum = b.levels(depth=10)
    assert [p for p, _, _ in bids] == [0.40, 0.39, 0.38]  # high -> low
    assert [c for _, _, c in bids] == [100, 150, 175]  # cumulative from best
    assert [p for p, _, _ in asks] == [0.42, 0.43]  # low -> high
    assert [c for _, _, c in asks] == [80, 140]
    assert max_cum == 175


def test_levels_depth_cap():
    b = OrderBook()
    b.reset_from_snapshot([(0.40 - i / 100, 10) for i in range(5)], [])
    bids, _, _ = b.levels(depth=2)
    assert len(bids) == 2


def test_copy_is_independent():
    b = OrderBook(tick_size=0.01, reset_ts="t")
    b.reset_from_snapshot([(0.40, 100)], [(0.42, 80)], ts="t")
    c = b.copy()
    c.apply_level_replace("BUY", 0.40, 5)
    assert b.bids[0.40] == 100  # original untouched
    assert c.bids[0.40] == 5
    assert c.tick_size == 0.01 and c.reset_ts == "t"
