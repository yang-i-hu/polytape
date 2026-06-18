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

    Keeps comments whose ``parentEntityID`` is the event id — or, when
    ``series_ids`` are given (opt-in ``--include-series-comments``), the event's
    parent series (e.g. a sports league/tournament, where the chat lives at the
    series level rather than the individual match). Tracks a per-parent latest
    comment id so the supervisor can resume backfill for each parent after a
    disconnect, plus the set of comment ids seen this session so reactions can be
    attributed.
    """

    stream = STREAM_COMMENTS

    def __init__(
        self,
        *,
        event_id: str | int,
        writer: Any,
        connect: Any = None,
        series_ids: tuple[str, ...] = (),
        entity_type: str = "Event",
    ) -> None:
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
        # The parent entity type of the primary id. Usually "Event"; "Series" when
        # the capture *is* a parent-series chat (e.g. a sports league recorded by
        # its series id). Series ids are always "Series".
        self.entity_type = str(entity_type) or "Event"
        self.series_ids = tuple(str(s) for s in series_ids)
        # The (parentEntityType, parentEntityID) pairs this capture accepts.
        # Matching the TYPE as well as the id matters because the numeric id
        # spaces collide: e.g. id 11433 is BOTH an Event ("...SAB 121...") and a
        # Series ("FIFA World Cup"). Filtering on id alone would bleed a foreign
        # chat into the capture; the pair is exact.
        self._want: set[tuple[str, str]] = {(self.entity_type, self.event_id)}
        self._want |= {("Series", sid) for sid in self.series_ids}
        self._cursors: dict[str, str] = {}  # parentEntityID -> latest comment id
        self._known_comment_ids: set[str] = set()

    @property
    def last_comment_id(self) -> str | None:
        """The event's latest seen comment id (backfill cursor for the event)."""
        return self._cursors.get(self.event_id)

    def subscribe_frames(self) -> list[str]:
        return [comment_subscribe_frame()]

    def backfill_targets(self) -> list[tuple[str, str]]:
        """(parent_entity_type, parent_id) pairs to backfill on reconnect.

        Uses the primary id's actual :attr:`entity_type` (not a hard-coded
        ``"Event"``), so a series-chat capture backfills against
        ``/comments?parent_entity_type=Series`` and actually recovers its missed
        comments on reconnect.
        """
        targets = [(self.entity_type, self.event_id)]
        targets += [("Series", sid) for sid in self.series_ids]
        return targets

    def cursor_for(self, parent_id: str) -> str | None:
        """Latest comment id recorded for ``parent_id`` (its backfill cursor)."""
        return self._cursors.get(str(parent_id))

    @staticmethod
    def _core(raw: dict[str, Any]) -> dict[str, Any]:
        payload = raw.get("payload")
        return payload if isinstance(payload, dict) else raw

    def should_record(self, raw: dict[str, Any]) -> bool:
        """Keep only this event's (and opted-in series') comments/reactions.

        Comments are matched on the ``(parentEntityType, parentEntityID)`` pair so
        an id that exists in both the Event and Series namespaces cannot leak a
        foreign chat in. If a comment omits ``parentEntityType`` (the live
        firehose always sets it; this guards synthetic/legacy frames), it falls
        back to an id-only match. Reactions carry no ``parentEntityID``, so they
        are matched by ``commentID`` against the comments already seen this
        session (reactions to comments we never saw are not attributable).
        """
        core = self._core(raw)
        parent = core.get("parentEntityID")
        if parent is not None:
            ptype = core.get("parentEntityType")
            if ptype is not None:
                return (str(ptype), str(parent)) in self._want
            return any(str(parent) == pid for _t, pid in self._want)
        comment_id = core.get("commentID")
        if comment_id is not None:
            return str(comment_id) in self._known_comment_ids
        return False

    def on_written(self, raw: dict[str, Any]) -> None:
        """Track each parent's latest comment id (backfill cursor) and seen ids."""
        if raw.get("type", "").startswith("reaction"):
            return  # reactions don't move the cursor or seed attribution
        core = self._core(raw)
        cid = core.get("id")
        if cid is None:
            return
        self._known_comment_ids.add(str(cid))
        parent = core.get("parentEntityID")
        if parent is not None:
            self._cursors[str(parent)] = str(cid)
