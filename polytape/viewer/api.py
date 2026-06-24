"""API response composition — the JSON schema owner (no sockets).

These functions turn :class:`~polytape.viewer.store.CaptureStore` outputs into the
exact contracted JSON dicts served by the HTTP/SSE layer. :func:`book_state` is the
ONE state-object shape shared by ``GET /book`` and every live SSE ``snapshot`` /
``price_change`` frame, so the browser has a single render path. Everything here
returns plain dicts and is unit-testable without a running server.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from polytape.viewer import metrics
from polytape.viewer.book import OrderBook

if TYPE_CHECKING:
    from polytape.viewer.store import CaptureStore


def _round(value: Any, ndigits: int) -> Any:
    return round(value, ndigits) if isinstance(value, (int, float)) else value


def _round_metrics(raw: dict[str, Any]) -> dict[str, Any]:
    precision = {
        "best_bid": 6,
        "best_ask": 6,
        "mid": 6,
        "microprice": 6,
        "spread": 6,
        "spread_bps": 3,
        "imbalance": 4,
        "total_bid": 4,
        "total_ask": 4,
    }
    return {
        key: (_round(val, precision[key]) if key in precision else val) for key, val in raw.items()
    }


def book_state(
    *,
    asset_id: str,
    label: str,
    book: OrderBook,
    as_of: str | None,
    seq: int,
    stale_after_gap: bool,
    book_unseeded: bool,
    depth: int = 25,
) -> dict[str, Any]:
    """The shared reconstructed-book state object (``/book`` and SSE state frames)."""
    bids, asks, max_cum = book.levels(depth)
    return {
        "asset_id": asset_id,
        "label": label,
        "as_of": as_of,
        "seq": seq,
        "tick_size": book.tick_size,
        "reset_since": book.reset_ts,
        "stale_after_gap": stale_after_gap,
        "book_unseeded": book_unseeded,
        "ladder": {
            "bids": [
                {"price": _round(p, 6), "size": _round(s, 4), "cum": _round(c, 4)}
                for p, s, c in bids
            ],
            "asks": [
                {"price": _round(p, 6), "size": _round(s, 4), "cum": _round(c, 4)}
                for p, s, c in asks
            ],
            "max_cum": _round(max_cum, 4),
        },
        "metrics": _round_metrics(metrics.summary(book)),
    }


def event_summary(event_id: str, meta: dict[str, Any] | None) -> dict[str, Any]:
    """One row for the multi-capture picker (``GET /events``)."""
    event = (meta or {}).get("event") or {}
    return {
        "event_id": event_id,
        "dir": f"event-{event_id}",
        "title": event.get("title"),
        "slug": event.get("slug"),
        "live": (meta.get("stopped_at") is None) if meta else False,
        "counts": (meta or {}).get("counts", {}),
    }


def build_events_response(summaries: list[dict[str, Any]]) -> dict[str, Any]:
    return {"events": list(summaries)}


def build_meta_response(store: CaptureStore) -> dict[str, Any] | None:
    meta = store.meta()
    if meta is None:
        return None
    start, end = store.time_range()
    return {
        "event_id": meta.get("event_id"),
        "title": (meta.get("event") or {}).get("title"),
        "slug": (meta.get("event") or {}).get("slug"),
        "live": store.is_live(),
        "started_at": meta.get("started_at"),
        "stopped_at": meta.get("stopped_at"),
        "counts": meta.get("counts", {}),
        "gaps": [g for g in meta.get("gaps", []) if g.get("stream") in (None, "book")],
        "assets": store.assets(),
        "time_range": {"start": start, "end": end},
    }


def build_book_response(
    store: CaptureStore, asset_id: str, at: str | None, depth: int
) -> dict[str, Any] | None:
    result = store.book_as_of(asset_id, at)
    if result is None:
        return None
    return book_state(
        asset_id=asset_id,
        label=store.label_for(asset_id),
        book=result["book"],
        as_of=result["as_of"],
        seq=result["seq"],
        stale_after_gap=result["stale_after_gap"],
        book_unseeded=result["book_unseeded"],
        depth=depth,
    )


def build_series_response(
    store: CaptureStore, asset_id: str, frm: str | None, to: str | None, max_points: int
) -> dict[str, Any] | None:
    result = store.series(asset_id, frm, to, max_points)
    if result is None:
        return None
    points, gaps = result
    return {"asset_id": asset_id, "points": points, "gaps": gaps}


def build_trades_response(
    store: CaptureStore, asset_id: str, before: str | None, limit: int
) -> dict[str, Any] | None:
    trades = store.trades(asset_id, before, limit)
    if trades is None:
        return None
    return {"asset_id": asset_id, "trades": trades}
