"""RTDS comment stream consumer.

Connects to Polymarket's Real-Time Data Service, subscribes to the ``comments``
topic filtered to one Event, and records every comment/reaction. See
``PROTOCOL.md`` §1 for the verified subscribe frame and keepalive.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from polytape.config import STREAM_COMMENTS
from polytape.streams.base import WebSocketStream

logger = logging.getLogger("polytape.stream.rtds")

RTDS_URL = "wss://ws-live-data.polymarket.com"

# Application-level keepalive: the RTDS reference client sends the literal
# lowercase text "ping" every 5s (PROTOCOL.md §1.4).
_PING_TEXT = "ping"
_PING_INTERVAL = 5.0


def comment_subscribe_frame(event_id: str | int) -> str:
    """Build the RTDS subscribe frame for one event's comments.

    ``filters`` is a *stringified* JSON object (RTDS quirk), and
    ``parentEntityID`` must be the numeric event id. A non-numeric id (only
    possible under ``--dry-run``) is passed through as-is.
    """
    entity_id: int | str
    try:
        entity_id = int(event_id)
    except (TypeError, ValueError):
        entity_id = str(event_id)
    inner = json.dumps({"parentEntityID": entity_id, "parentEntityType": "Event"})
    frame = {
        "action": "subscribe",
        "subscriptions": [{"topic": "comments", "type": "*", "filters": inner}],
    }
    return json.dumps(frame)


class CommentStream(WebSocketStream):
    """Records the RTDS ``comments`` topic for a single event.

    Tracks :attr:`last_comment_id` (the latest *comment* id, ignoring reactions)
    so the supervisor can resume comment backfill after a disconnect.
    """

    stream = STREAM_COMMENTS

    def __init__(self, *, event_id: str | int, writer: Any, connect: Any = None) -> None:
        kwargs: dict[str, Any] = {}
        if connect is not None:
            kwargs["connect"] = connect
        super().__init__(
            url=RTDS_URL,
            writer=writer,
            ping_text=_PING_TEXT,
            ping_interval=_PING_INTERVAL,
            **kwargs,
        )
        self.event_id = str(event_id)
        self.last_comment_id: str | None = None

    def subscribe_frames(self) -> list[str]:
        return [comment_subscribe_frame(self.event_id)]

    def on_written(self, raw: dict[str, Any]) -> None:
        """Advance the comment-backfill cursor for comment (not reaction) messages."""
        msg_type = raw.get("type", "")
        if msg_type and not msg_type.startswith("comment"):
            return  # reactions have their own ids; backfill keys on comment ids
        payload = raw.get("payload")
        core = payload if isinstance(payload, dict) else raw
        cid = core.get("id")
        if cid is not None:
            self.last_comment_id = str(cid)
