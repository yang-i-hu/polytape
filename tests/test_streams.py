"""Tests for the websocket stream consumers (offline, fake connection)."""

from __future__ import annotations

import json

from polytape.streams.base import WebSocketStream
from polytape.streams.clob import BookStream, book_subscribe_frame
from polytape.streams.rtds import CommentStream, comment_subscribe_frame
from polytape.writer import CaptureWriter


def test_comment_subscribe_frame_is_unfiltered_firehose():
    # Server-side filtering returns nothing live, so we subscribe to the firehose.
    frame = json.loads(comment_subscribe_frame())
    assert frame["action"] == "subscribe"
    sub = frame["subscriptions"][0]
    assert sub["topic"] == "comments" and sub["type"] == "*"
    assert "filters" not in sub


def test_comment_should_record_filters_by_event():
    cs = CommentStream(event_id="20200", writer=None)
    assert cs.should_record(
        {"type": "comment_created", "payload": {"id": "x", "parentEntityID": 20200}}
    )
    assert not cs.should_record(
        {"type": "comment_created", "payload": {"id": "y", "parentEntityID": 999}}
    )
    # reaction only attributed once its parent comment has been seen
    reaction = {"type": "reaction_created", "payload": {"id": "r", "commentID": "x"}}
    assert not cs.should_record(reaction)
    cs._known_comment_ids.add("x")
    assert cs.should_record(reaction)


def test_comment_should_record_includes_series_when_enabled():
    cs = CommentStream(event_id="351729", writer=None, series_ids=("11433",))
    # event-level comment
    assert cs.should_record({"type": "comment_created", "payload": {"parentEntityID": "351729"}})
    # parent-series comment (where sports chat lives)
    assert cs.should_record(
        {
            "type": "comment_created",
            "payload": {"parentEntityID": 11433, "parentEntityType": "Series"},
        }
    )
    # a different series is still dropped
    assert not cs.should_record({"type": "comment_created", "payload": {"parentEntityID": "99999"}})


def test_comment_series_off_by_default():
    cs = CommentStream(event_id="351729", writer=None)  # no series_ids
    assert not cs.should_record({"type": "comment_created", "payload": {"parentEntityID": "11433"}})
    assert cs.should_record({"type": "comment_created", "payload": {"parentEntityID": "351729"}})


def _comment(parent_id, parent_type):
    return {
        "type": "comment_created",
        "payload": {"parentEntityID": parent_id, "parentEntityType": parent_type},
    }


def test_comment_event_capture_rejects_colliding_series_chat():
    # id 11433 exists in BOTH namespaces (an Event AND the "FIFA World Cup" Series).
    # Recording the EVENT must not absorb the SERIES chat when the type is present.
    cs = CommentStream(event_id="11433", writer=None)  # entity_type defaults to "Event"
    assert cs.should_record(_comment(11433, "Event"))
    assert not cs.should_record(_comment(11433, "Series"))


def test_comment_series_capture_records_series_and_backfills_as_series():
    # Recording a parent-series chat directly by its series id (the dashboard's
    # "Live chat now" Record on a Series row).
    cs = CommentStream(event_id="11433", writer=None, entity_type="Series")
    assert cs.should_record(_comment(11433, "Series"))
    # the same id in the Event namespace is a different entity -> dropped
    assert not cs.should_record(_comment(11433, "Event"))
    # backfill must query /comments?parent_entity_type=Series, not Event
    assert cs.backfill_targets() == [("Series", "11433")]


def test_comment_per_parent_cursor_and_backfill_targets():
    cs = CommentStream(event_id="351729", writer=None, series_ids=("11433",))
    assert cs.backfill_targets() == [("Event", "351729"), ("Series", "11433")]
    cs.on_written({"type": "comment_created", "payload": {"id": "e1", "parentEntityID": "351729"}})
    cs.on_written({"type": "comment_created", "payload": {"id": "s1", "parentEntityID": "11433"}})
    assert cs.cursor_for("351729") == "e1"
    assert cs.cursor_for("11433") == "s1"
    assert cs.last_comment_id == "e1"  # the event's cursor


def test_book_subscribe_frame():
    assert json.loads(book_subscribe_frame(["t1", "t2"])) == {
        "assets_ids": ["t1", "t2"],
        "type": "market",
    }


def test_decode_variants():
    s = WebSocketStream(url="x", writer=None, ping_text="ping")
    assert s.decode('{"a":1}') == [{"a": 1}]
    assert s.decode('[{"a":1},{"b":2}]') == [{"a": 1}, {"b": 2}]
    assert s.decode("[1,2,3]") == []  # non-dict items dropped
    assert s.decode("pong") == []  # non-JSON keepalive reply
    assert s.decode(b'{"a":1}') == [{"a": 1}]  # bytes
    assert s.decode("42") == []  # scalar


async def test_comment_stream_dedup_and_cursor(make_config, make_connect):
    cfg = make_config(book=False)

    def c(cid):
        return {
            "type": "comment_created",
            "payload": {"id": cid, "parentEntityID": 20200, "userAddress": "0xa"},
        }

    # reaction to c1 (seen before it arrives) plus one to an unseen comment (dropped)
    react = {"type": "reaction_created", "payload": {"id": "r1", "commentID": "c1"}}
    other_event = {"type": "comment_created", "payload": {"id": "z1", "parentEntityID": 999}}
    frames = [
        json.dumps(c("c1")),
        json.dumps(react),
        json.dumps(other_event),  # different event -> filtered out client-side
        json.dumps([c("c2"), c("c3")]),
        json.dumps(c("c1")),  # dup
    ]
    connect = make_connect(frames)
    with CaptureWriter(cfg) as w:
        cs = CommentStream(event_id="20200", writer=w, connect=connect)
        await cs.run_once()
        assert w.counts["comments"] == 4  # c1, r1, c2, c3 (other-event + dup c1 skipped)
        assert cs.last_comment_id == "c3"  # reaction ignored for the cursor
    assert connect.ws.sent == [comment_subscribe_frame()]


async def test_book_stream_ids(make_config, make_connect):
    cfg = make_config(comments=False)
    book = {"event_type": "book", "asset_id": "t1", "hash": "0xH", "timestamp": "1700000000000"}
    pc = {
        "event_type": "price_change",
        "timestamp": "1700000000001",
        "price_changes": [{"hash": "h"}],
    }
    frames = [json.dumps(book), json.dumps(pc), json.dumps(book)]  # last dup
    connect = make_connect(frames)
    with CaptureWriter(cfg) as w:
        bs = BookStream(token_ids=["t1"], writer=w, connect=connect)
        await bs.run_once()
        assert w.counts["book"] == 2  # book + price_change (dup book skipped)


def test_book_stream_empty_tokens_no_frame():
    assert BookStream(token_ids=[], writer=None).subscribe_frames() == []
