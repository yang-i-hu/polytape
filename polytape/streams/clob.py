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
from collections.abc import Sequence
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

# Read deadline: the CLOB book channel is busy and replies to our PING with a text
# PONG every ~5s, so a 20s deadline never false-fires on a healthy link but catches
# a true freeze (e.g. a GCP live-migration network blackout) within seconds.
_READ_TIMEOUT = 20.0


def book_subscribe_frame(token_ids: list[str] | tuple[str, ...]) -> str:
    """Build the CLOB market-channel subscribe frame for the given token ids."""
    return json.dumps({"assets_ids": list(token_ids), "type": "market"})


# Per-connection token cap: stay well under the ~200 proven-safe limit (and far
# below the ~250 silent-freeze zone and the 500 snapshot-loss cliff). Whole event
# token groups are packed into buckets without splitting any event across sockets.
_SHARD_CAP = 180


def shard_tokens(
    token_groups: Sequence[Sequence[str]], cap: int = _SHARD_CAP
) -> list[tuple[str, ...]]:
    """Pack whole per-event token groups into CLOB subscription buckets of ``<= cap``.

    An event's tokens are never split across buckets, so a reconnect snapshot for
    an event always arrives on a single socket. Raises if one group exceeds ``cap``.
    """
    buckets: list[list[str]] = []
    current: list[str] = []
    for group in token_groups:
        toks = [str(t) for t in group if str(t)]
        if not toks:
            continue
        if len(toks) > cap:
            raise ValueError(f"event token group of {len(toks)} exceeds shard cap {cap}")
        if current and len(current) + len(toks) > cap:
            buckets.append(current)
            current = []
        current.extend(toks)
    if current:
        buckets.append(current)
    return [tuple(b) for b in buckets]


class BookStream(WebSocketStream):
    """Records the CLOB market channel for a set of CLOB token ids."""

    stream = STREAM_BOOK

    def __init__(
        self,
        *,
        token_ids: list[str] | tuple[str, ...],
        writer: Any,
        connect: Any = None,
        on_activity: Any = None,
        cond_to_event: dict[str, str] | None = None,
    ) -> None:
        kwargs: dict[str, Any] = {}
        if connect is not None:
            kwargs["connect"] = connect
        if on_activity is not None:
            kwargs["on_activity"] = on_activity
        super().__init__(
            url=CLOB_URL,
            writer=writer,
            ping_text=_PING_TEXT,
            ping_interval=_PING_INTERVAL,
            read_timeout=_READ_TIMEOUT,
            **kwargs,
        )
        self.token_ids = tuple(token_ids)
        # condition_id -> event_id, for routing each message to its event. Empty
        # in single-event mode (then we accept and tag nothing — back-compat).
        self._cond_to_event = dict(cond_to_event or {})

    def subscribe_frames(self) -> list[str]:
        if not self.token_ids:
            logger.warning("book stream has no token ids; nothing to subscribe to")
            return []
        return [book_subscribe_frame(self.token_ids)]

    def resolve_event_id(self, raw: dict[str, Any]) -> str | None:
        """Route a CLOB message to its event by top-level ``market`` (condition id).

        Used for *all* message types — including ``price_change``, which carries no
        top-level ``asset_id`` — so routing never depends on a per-token field.
        """
        return self._cond_to_event.get(str(raw.get("market")))

    def should_record(self, raw: dict[str, Any]) -> bool:
        """Keep messages for this run's events; never drop on a missing ``market``.

        With no routing map (single-event mode) accept everything (back-compat). With
        a map, drop only a message whose ``market`` is present but unknown; a message
        with no ``market`` key is recorded (untagged) rather than silently dropped.
        """
        if not self._cond_to_event:
            return True
        market = raw.get("market")
        if market is None:
            return True
        return str(market) in self._cond_to_event
