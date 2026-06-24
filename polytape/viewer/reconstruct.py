"""Canonical order-book reconstruction from recorded ``book.jsonl`` envelopes.

This is the single source of truth for the snapshot/level-replace/delete/trade
rules (see ``PROTOCOL.md`` §2). It is pure (no I/O): :func:`normalize_book_event`
turns one ``raw`` feed dict into typed per-asset *changes*, and a
:class:`Reconstructor` folds changes into per-asset :class:`OrderBook` state. The
store layer drives this forward over a tailed file; the same change list is
replayed from a keyframe to answer "book as of T", so live and history can never
disagree.

Ordering is always by file/line order (== ``ts_recv`` order, since the recorder
appends in receive order); ``ts_recv`` is used only for time labels and the
scrubber, never ``ts_server`` (which may be ``null``).
"""

from __future__ import annotations

import math
from typing import Any

from polytape.viewer.book import OrderBook

# A normalized change is a small dict tagged by ``kind``:
#   snapshot: {kind, asset_id, bids:[(p,s)], asks:[(p,s)]}
#   delta:    {kind, asset_id, side:'BUY'|'SELL', price:float, size:float}
#   trade:    {kind, asset_id, price, size, side, tx}
#   tick:     {kind, asset_id, tick_size:float}
Change = dict[str, Any]


def _to_float(value: Any) -> float | None:
    """Parse a feed number; reject non-finite values (inf/nan/overflow).

    A non-finite size would poison metrics, emit ``Infinity``/``NaN`` JSON the
    browser can't parse, and (for ``nan``) be misread as a level delete. Returning
    ``None`` makes such a level get dropped upstream instead.
    """
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _levels(raw_levels: Any) -> list[tuple[float, float]]:
    out: list[tuple[float, float]] = []
    if not isinstance(raw_levels, list):
        return out
    for lvl in raw_levels:
        if not isinstance(lvl, dict):
            continue
        p = _to_float(lvl.get("price"))
        s = _to_float(lvl.get("size"))
        if p is not None and s is not None:
            out.append((p, s))
    return out


def normalize_book_event(raw: Any) -> list[Change]:
    """Turn one ``raw`` book-stream message into zero or more typed changes.

    Handles the four ``event_type`` values. Crucially, ``price_change`` carries no
    top-level ``asset_id`` — each ``price_changes[]`` element has its own, and one
    wire line may touch several assets, so we fan out per element.
    """
    if not isinstance(raw, dict):
        return []
    event_type = raw.get("event_type")

    if event_type == "book":
        asset_id = raw.get("asset_id")
        if asset_id is None:
            return []
        return [
            {
                "kind": "snapshot",
                "asset_id": str(asset_id),
                "bids": _levels(raw.get("bids")),
                "asks": _levels(raw.get("asks")),
            }
        ]

    if event_type == "price_change":
        changes: list[Change] = []
        elements = raw.get("price_changes")
        if not isinstance(elements, list):
            return []
        for el in elements:
            if not isinstance(el, dict):
                continue
            asset_id = el.get("asset_id")
            price = _to_float(el.get("price"))
            size = _to_float(el.get("size"))
            if asset_id is None or price is None or size is None:
                continue
            changes.append(
                {
                    "kind": "delta",
                    "asset_id": str(asset_id),
                    "side": str(el.get("side") or "").upper(),
                    "price": price,
                    "size": size,
                }
            )
        return changes

    if event_type == "last_trade_price":
        asset_id = raw.get("asset_id")
        if asset_id is None:
            return []
        return [
            {
                "kind": "trade",
                "asset_id": str(asset_id),
                "price": _to_float(raw.get("price")),
                "size": _to_float(raw.get("size")),
                "side": str(raw.get("side") or "").upper(),
                "tx": raw.get("transaction_hash"),
            }
        ]

    if event_type == "tick_size_change":
        asset_id = raw.get("asset_id")
        if asset_id is None:
            return []
        return [
            {
                "kind": "tick",
                "asset_id": str(asset_id),
                "tick_size": _to_float(raw.get("new_tick_size")),
            }
        ]

    return []


def is_book_change(change: Change) -> bool:
    """True for changes that mutate the ladder (snapshot or delta)."""
    return change["kind"] in ("snapshot", "delta")


def apply_change_to_book(book: OrderBook, change: Change, *, ts: str | None = None) -> None:
    """Apply one ladder-affecting change to ``book`` in place.

    Snapshots reset the book; deltas replace/delete a level. Trade and tick
    changes do not touch the ladder and are ignored here.
    """
    kind = change["kind"]
    if kind == "snapshot":
        book.reset_from_snapshot(change["bids"], change["asks"], ts=ts)
    elif kind == "delta":
        book.apply_level_replace(change["side"], change["price"], change["size"])


class Reconstructor:
    """Folds normalized changes into per-asset :class:`OrderBook` state.

    Used for the forward "warm" pass during ingest. ``unseeded`` tracks assets for
    which a delta arrived before any snapshot (top-of-book may be partial until the
    next ``book``).
    """

    def __init__(self) -> None:
        self.books: dict[str, OrderBook] = {}
        self.unseeded: set[str] = set()

    def book(self, asset_id: str) -> OrderBook:
        book = self.books.get(asset_id)
        if book is None:
            book = OrderBook()
            self.books[asset_id] = book
        return book

    def apply(self, change: Change, *, ts: str | None = None) -> None:
        asset_id = change["asset_id"]
        book = self.book(asset_id)
        kind = change["kind"]
        if kind == "snapshot":
            apply_change_to_book(book, change, ts=ts)
            self.unseeded.discard(asset_id)
        elif kind == "delta":
            if book.reset_ts is None:
                self.unseeded.add(asset_id)
            apply_change_to_book(book, change, ts=ts)
        elif kind == "tick":
            if change.get("tick_size"):
                book.tick_size = change["tick_size"]
        # trades do not mutate the book
