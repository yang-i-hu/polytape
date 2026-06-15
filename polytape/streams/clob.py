"""CLOB order-book stream consumer.

Connects to Polymarket's CLOB market channel and records order-book activity for
a set of CLOB token ids. See ``PROTOCOL.md`` §2 for the verified subscribe frame,
keepalive, and message shapes.

Message types delivered on this channel (all recorded verbatim, type preserved
inside ``raw.event_type``):

* ``book`` — a full **snapshot**, sent on (re)subscribe and after trades.
* ``price_change`` — an incremental **delta** (per-level aggregate sizes).
* ``last_trade_price`` — a single executed trade.
* ``tick_size_change`` — a tick-size change notification.

So the feed delivers **both snapshots and deltas**: state is seeded by ``book``
and maintained by ``price_change``. On reconnect a fresh ``book`` snapshot
re-establishes state, which is why this stream needs no REST backfill.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from polytape.config import STREAM_BOOK
from polytape.streams.base import WebSocketStream

logger = logging.getLogger("polytape.stream.clob")

CLOB_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

# Application-level keepalive: the CLOB market channel expects the literal
# uppercase text "PING"; the server drops idle connections after ~10s, so 5s is
# comfortably within the window (PROTOCOL.md §2.3).
_PING_TEXT = "PING"
_PING_INTERVAL = 5.0


def book_subscribe_frame(token_ids: list[str] | tuple[str, ...]) -> str:
    """Build the CLOB market-channel subscribe frame for the given token ids."""
    return json.dumps({"assets_ids": list(token_ids), "type": "market"})


class BookStream(WebSocketStream):
    """Records the CLOB market channel for a set of CLOB token ids."""

    stream = STREAM_BOOK

    def __init__(
        self,
        *,
        token_ids: list[str] | tuple[str, ...],
        writer: Any,
        connect: Any = None,
    ) -> None:
        kwargs: dict[str, Any] = {}
        if connect is not None:
            kwargs["connect"] = connect
        super().__init__(
            url=CLOB_URL,
            writer=writer,
            ping_text=_PING_TEXT,
            ping_interval=_PING_INTERVAL,
            **kwargs,
        )
        self.token_ids = tuple(token_ids)

    def subscribe_frames(self) -> list[str]:
        if not self.token_ids:
            logger.warning("book stream has no token ids; nothing to subscribe to")
            return []
        return [book_subscribe_frame(self.token_ids)]
