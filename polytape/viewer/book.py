"""Pure order-book model for the viewer (no I/O, no feed semantics).

An :class:`OrderBook` holds one asset's resting liquidity as two ``price -> size``
maps. It knows only the mutations the CLOB feed implies — reset from a snapshot,
replace a level's aggregate size, delete a level — plus read accessors (best
bid/ask, depth-limited levels with cumulative size, deep copy). Timestamps,
parsing and event routing live in :mod:`polytape.viewer.reconstruct`; keeping
this model dumb makes it trivial to unit test in isolation.
"""

from __future__ import annotations

from collections.abc import Iterable

# Price keys are rounded to this many decimals so string-parsed prices like
# "0.55" and "0.550" collapse onto one level and float arithmetic can never spawn
# phantom adjacent levels. Polymarket ticks are >= 0.001, so 6 dp is ample.
_PRICE_DP = 6

# A ladder level as returned to callers: (price, size, cumulative_size_from_best).
Level = tuple[float, float, float]


def price_key(price: float) -> float:
    """Round a price to the canonical level key."""
    return round(price, _PRICE_DP)


class OrderBook:
    """One asset's order book: ``price -> size`` maps for bids and asks."""

    __slots__ = ("bids", "asks", "tick_size", "reset_ts")

    def __init__(self, tick_size: float | None = None, reset_ts: str | None = None) -> None:
        self.bids: dict[float, float] = {}
        self.asks: dict[float, float] = {}
        self.tick_size = tick_size
        self.reset_ts = reset_ts

    # -- mutations --------------------------------------------------------- #

    def reset_from_snapshot(
        self,
        bids: Iterable[tuple[float, float]],
        asks: Iterable[tuple[float, float]],
        *,
        ts: str | None = None,
    ) -> None:
        """Clear and re-seed both sides from a full ``book`` snapshot.

        Levels with size <= 0 are dropped. ``ts`` (the snapshot's ``ts_recv``) is
        remembered as :attr:`reset_ts` so the viewer can tell how recently this
        asset's book was re-anchored (used for post-gap staleness).
        """
        self.bids = {price_key(p): s for p, s in bids if s > 0}
        self.asks = {price_key(p): s for p, s in asks if s > 0}
        if ts is not None:
            self.reset_ts = ts

    def apply_level_replace(self, side: str, price: float, size: float) -> None:
        """Apply one ``price_change`` level: an aggregate REPLACE, ``size<=0`` deletes.

        ``side`` is the wire side: ``BUY`` touches the bid book, ``SELL`` the ask.
        """
        book = self.bids if side == "BUY" else self.asks
        key = price_key(price)
        if size > 0:
            book[key] = size
        else:
            book.pop(key, None)

    # -- accessors --------------------------------------------------------- #

    def best_bid(self) -> float | None:
        return max(self.bids) if self.bids else None

    def best_ask(self) -> float | None:
        return min(self.asks) if self.asks else None

    def total_bid(self) -> float:
        return sum(self.bids.values())

    def total_ask(self) -> float:
        return sum(self.asks.values())

    def is_empty(self) -> bool:
        return not self.bids and not self.asks

    def levels(self, depth: int = 25) -> tuple[list[Level], list[Level], float]:
        """Return ``(bids, asks, max_cum)`` for rendering a depth ladder.

        Bids are ordered best-first (high to low), asks best-first (low to high);
        each level carries the running cumulative size outward from the touch.
        ``max_cum`` is the largest visible cumulative on either side (for bar
        normalization). ``depth`` caps the number of levels per side.
        """
        bid_items = sorted(self.bids.items(), reverse=True)[:depth]
        ask_items = sorted(self.asks.items())[:depth]
        bids: list[Level] = []
        cum = 0.0
        for p, s in bid_items:
            cum += s
            bids.append((p, s, cum))
        asks: list[Level] = []
        cum = 0.0
        for p, s in ask_items:
            cum += s
            asks.append((p, s, cum))
        max_cum = max(bids[-1][2] if bids else 0.0, asks[-1][2] if asks else 0.0)
        return bids, asks, max_cum

    def copy(self) -> OrderBook:
        """A deep-enough copy: independent level maps, shared immutable scalars."""
        ob = OrderBook(self.tick_size, self.reset_ts)
        ob.bids = dict(self.bids)
        ob.asks = dict(self.asks)
        return ob
