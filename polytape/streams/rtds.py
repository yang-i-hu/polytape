"""RTDS comment stream consumer.

Connects to Polymarket's Real-Time Data Service ``comments`` topic and records
the comments/reactions for one Event.

**Filtering is client-side.** Live testing (2026-06-15) showed the server-side
per-event ``filters`` field returns *zero* messages for every format tried
(stringified int/string and object), while the unfiltered firehose delivers the
event's comments normally. So polytape subscribes to the firehose and keeps only
messages belonging to its event: comments by ``parentEntityID``, and reactions
(which carry no ``parentEntityID``) by ``commentID`` against the comments seen
this session. See ``PROTOCOL.md`` §1 and Open Question #3.
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

# Read deadline: the comments firehose is global (all events) and thus rarely idle,
# so a generous 120s deadline avoids any false trip on a quiet stretch while still
# bounding a true silent freeze. (RTDS may answer keepalive with a protocol-level
# pong that never surfaces here, so we rely on firehose traffic, not PONGs.)
_READ_TIMEOUT = 120.0


def comment_subscribe_frame() -> str:
    """Build the RTDS subscribe frame for the (unfiltered) comments firehose.

    No ``filters`` field: the server-side filter was found to drop all messages,
    so filtering happens client-side in :meth:`CommentStream.should_record`.
    """
    frame = {
        "action": "subscribe",
        "subscriptions": [{"topic": "comments", "type": "*"}],
    }
    return json.dumps(frame)


class CommentStream(WebSocketStream):
    """Records the RTDS ``comments`` topic for a single event (client-filtered).

    Tracks :attr:`last_comment_id` (the latest *comment* id, ignoring reactions)
    so the supervisor can resume comment backfill after a disconnect, and the set
    of comment ids seen this session so reactions can be attributed.
    """

    stream = STREAM_COMMENTS

    def __init__(
        self,
        *,
        event_id: str | int | None = None,
        event_ids: set[str] | None = None,
        writer: Any,
        connect: Any = None,
        on_activity: Any = None,
    ) -> None:
        kwargs: dict[str, Any] = {}
        if connect is not None:
            kwargs["connect"] = connect
        if on_activity is not None:
            kwargs["on_activity"] = on_activity
        super().__init__(
            url=RTDS_URL,
            writer=writer,
            ping_text=_PING_TEXT,
            ping_interval=_PING_INTERVAL,
            read_timeout=_READ_TIMEOUT,
            **kwargs,
        )
        ids = (
            event_ids if event_ids is not None else ({event_id} if event_id is not None else set())
        )
        self.event_ids: set[str] = {str(e) for e in ids}
        # Primary id (single-event convenience / back-compat); None for multi-event.
        self.event_id: str | None = next(iter(self.event_ids)) if len(self.event_ids) == 1 else None
        # Per-event backfill cursor: latest comment id seen for each event.
        self._last_comment_id: dict[str, str] = {}
        # commentID -> event id, so reactions (which carry no parentEntityID) route.
        self._comment_to_event: dict[str, str] = {}

    @property
    def last_comment_id(self) -> str | None:
        """Backfill cursor for a single-event stream (``None`` for multi-event)."""
        if len(self.event_ids) == 1:
            return self._last_comment_id.get(next(iter(self.event_ids)))
        return None

    def last_comment_id_for(self, event_id: str) -> str | None:
        """Backfill cursor for a specific event (used by the multi-event backfill)."""
        return self._last_comment_id.get(str(event_id))

    def subscribe_frames(self) -> list[str]:
        return [comment_subscribe_frame()]

    @staticmethod
    def _core(raw: dict[str, Any]) -> dict[str, Any]:
        payload = raw.get("payload")
        return payload if isinstance(payload, dict) else raw

    def should_record(self, raw: dict[str, Any]) -> bool:
        """Keep only this run's events' comments/reactions from the firehose.

        Comments are matched by ``parentEntityID`` against the SET of recorded
        events. Reactions carry no ``parentEntityID``, so they are matched by
        ``commentID`` against comments already seen this session (reactions to
        comments we never saw are not attributable and are dropped).
        """
        core = self._core(raw)
        parent = core.get("parentEntityID")
        if parent is not None:
            return str(parent) in self.event_ids
        comment_id = core.get("commentID")
        if comment_id is not None:
            return str(comment_id) in self._comment_to_event
        return False

    def resolve_event_id(self, raw: dict[str, Any]) -> str | None:
        """Event id for a message: ``parentEntityID`` (comment); a reaction routes
        via the ``commentID`` -> event map seeded by comments seen this session."""
        core = self._core(raw)
        parent = core.get("parentEntityID")
        if parent is not None:
            return str(parent)
        comment_id = core.get("commentID")
        if comment_id is not None:
            return self._comment_to_event.get(str(comment_id))
        return None

    def on_written(self, raw: dict[str, Any]) -> None:
        """Advance the per-event cursor and seed reaction attribution for a comment."""
        if raw.get("type", "").startswith("reaction"):
            return  # reactions don't move the cursor or seed attribution
        core = self._core(raw)
        cid = core.get("id")
        parent = core.get("parentEntityID")
        if cid is None or parent is None:
            return
        event_id = str(parent)
        self._last_comment_id[event_id] = str(cid)
        self._comment_to_event[str(cid)] = event_id
