"""Pure top-of-book metrics over an :class:`~polytape.viewer.book.OrderBook`.

Every metric returns ``None`` when it is undefined for the current book (e.g. a
one-sided or empty book), so the API can serialize ``null`` and the UI can show
an em-dash rather than a fabricated number. No I/O; trivially unit tested.
"""

from __future__ import annotations

from polytape.viewer.book import OrderBook


def spread(book: OrderBook) -> float | None:
    bb, ba = book.best_bid(), book.best_ask()
    if bb is None or ba is None:
        return None
    return ba - bb


def mid(book: OrderBook) -> float | None:
    bb, ba = book.best_bid(), book.best_ask()
    if bb is None or ba is None:
        return None
    return (bb + ba) / 2


def spread_bps(book: OrderBook) -> float | None:
    """Spread as basis points of the mid. ``None`` if either side is empty."""
    s, m = spread(book), mid(book)
    if s is None or not m:
        return None
    return (s / m) * 10_000


def microprice(book: OrderBook) -> float | None:
    """Size-weighted fair value within the spread.

    ``(best_bid * ask_size + best_ask * bid_size) / (bid_size + ask_size)`` — the
    price is pulled toward the side with *less* size. Falls back to the mid if the
    touch sizes sum to zero.
    """
    bb, ba = book.best_bid(), book.best_ask()
    if bb is None or ba is None:
        return None
    bid_sz = book.bids[bb]
    ask_sz = book.asks[ba]
    denom = bid_sz + ask_sz
    if denom <= 0:
        return mid(book)
    return (bb * ask_sz + ba * bid_sz) / denom


def imbalance(book: OrderBook) -> float | None:
    """Order-book imbalance ``(bid_depth - ask_depth) / (bid_depth + ask_depth)``.

    In ``[-1, 1]``: positive = bid-heavy. ``None`` if the whole book is empty.
    """
    tb = book.total_bid()
    ta = book.total_ask()
    denom = tb + ta
    if denom <= 0:
        return None
    return (tb - ta) / denom


def summary(book: OrderBook) -> dict[str, float | int | None]:
    """All top-of-book metrics as raw (unrounded) values for the API layer."""
    bb, ba = book.best_bid(), book.best_ask()
    return {
        "best_bid": bb,
        "best_ask": ba,
        "spread": spread(book),
        "spread_bps": spread_bps(book),
        "mid": mid(book),
        "microprice": microprice(book),
        "imbalance": imbalance(book),
        "total_bid": book.total_bid() if book.bids else None,
        "total_ask": book.total_ask() if book.asks else None,
        "levels_bid": len(book.bids),
        "levels_ask": len(book.asks),
    }
