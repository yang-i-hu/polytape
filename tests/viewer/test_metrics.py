"""Unit tests for the pure top-of-book metrics."""

from __future__ import annotations

import math

from polytape.viewer import metrics
from polytape.viewer.book import OrderBook


def _book(bids, asks):
    b = OrderBook()
    b.reset_from_snapshot(bids, asks)
    return b


def test_spread_mid_bps():
    b = _book([(0.40, 100)], [(0.42, 80)])
    assert math.isclose(metrics.spread(b), 0.02, rel_tol=1e-9)
    assert math.isclose(metrics.mid(b), 0.41, rel_tol=1e-9)
    assert math.isclose(metrics.spread_bps(b), (0.02 / 0.41) * 10_000, rel_tol=1e-9)


def test_microprice_weighting():
    # more size on the ask -> microprice pulled toward the bid.
    b = _book([(0.40, 100)], [(0.42, 300)])
    mp = metrics.microprice(b)
    assert 0.40 < mp < metrics.mid(b)
    # exact: (0.40*300 + 0.42*100) / 400
    assert math.isclose(mp, (0.40 * 300 + 0.42 * 100) / 400, rel_tol=1e-9)


def test_imbalance_range_and_sign():
    assert metrics.imbalance(_book([(0.4, 150)], [(0.42, 50)])) > 0  # bid-heavy
    assert metrics.imbalance(_book([(0.4, 50)], [(0.42, 150)])) < 0  # ask-heavy
    assert -1 <= metrics.imbalance(_book([(0.4, 1)], [(0.42, 1_000)])) <= 1


def test_one_sided_and_empty_return_none():
    only_bids = _book([(0.40, 100)], [])
    assert metrics.spread(only_bids) is None
    assert metrics.mid(only_bids) is None
    assert metrics.spread_bps(only_bids) is None
    assert metrics.microprice(only_bids) is None
    assert metrics.imbalance(only_bids) is not None  # one side still defines imbalance

    empty = _book([], [])
    assert metrics.imbalance(empty) is None
    summary = metrics.summary(empty)
    assert summary["best_bid"] is None and summary["total_bid"] is None
    assert summary["levels_bid"] == 0


def test_summary_keys():
    b = _book([(0.40, 100)], [(0.42, 80)])
    s = metrics.summary(b)
    assert set(s) == {
        "best_bid",
        "best_ask",
        "spread",
        "spread_bps",
        "mid",
        "microprice",
        "imbalance",
        "total_bid",
        "total_ask",
        "levels_bid",
        "levels_ask",
    }
