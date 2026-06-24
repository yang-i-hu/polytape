"""Tests for the websocket stream consumers (offline, fake connection)."""

from __future__ import annotations

import asyncio
import json

import pytest

from polytape.gamma import EventInfo, Market, cond_to_event
from polytape.streams.base import StreamInactivityError, WebSocketStream
from polytape.streams.clob import BookStream, book_subscribe_frame, shard_tokens
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
    cs._comment_to_event["x"] = "20200"  # parent comment now seen this session
    assert cs.should_record(reaction)


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


async def test_watchdog_raises_on_inactivity(make_config, make_connect):
    # Socket open but no frame ever arrives (a silent freeze / migration blackout):
    # the read deadline must fire so the supervisor reconnects and records a gap.
    cfg = make_config(book=False)
    connect = make_connect([], blocking=True)
    with CaptureWriter(cfg) as w:
        cs = CommentStream(event_id="20200", writer=w, connect=connect)
        cs.read_timeout = 0.05
        with pytest.raises(StreamInactivityError):
            await asyncio.wait_for(cs.run_once(), timeout=2.0)


async def test_watchdog_does_not_trip_while_data_flows(make_config, make_connect):
    # A frame within the deadline must not trip the watchdog; a clean close returns.
    cfg = make_config(book=False)
    c = {"type": "comment_created", "payload": {"id": "c1", "parentEntityID": 20200}}
    connect = make_connect([json.dumps(c)])
    with CaptureWriter(cfg) as w:
        cs = CommentStream(event_id="20200", writer=w, connect=connect)
        cs.read_timeout = 0.5
        await asyncio.wait_for(cs.run_once(), timeout=2.0)
        assert w.counts["comments"] == 1


# -- multi-event (Phase 3) ------------------------------------------------- #


def test_shard_tokens_packs_whole_groups():
    groups = [tuple(f"e{e}t{i}" for i in range(6)) for e in range(44)]  # 44 events x 6 tokens
    shards = shard_tokens(groups, cap=180)
    flat = [t for s in shards for t in s]
    assert len(flat) == 264 and len(set(flat)) == 264  # complete union, no token shared
    assert all(len(s) <= 180 for s in shards)
    assert all(len(s) % 6 == 0 for s in shards)  # an event's 6 tokens never split
    assert len(shards) == 2  # 264 tokens at cap 180


def test_shard_tokens_single_and_oversized():
    assert shard_tokens([("a", "b")], cap=180) == [("a", "b")]
    assert shard_tokens([(), ("a",)], cap=180) == [("a",)]  # empty groups skipped
    with pytest.raises(ValueError):
        shard_tokens([tuple(range(200))], cap=180)


def test_book_stream_demux_routes_by_market():
    routing = {"0xA": "1001", "0xB": "1002"}
    bs = BookStream(token_ids=["t"], writer=None, cond_to_event=routing)
    # all message types route by top-level market; price_change has NO top-level asset_id
    assert bs.resolve_event_id({"event_type": "book", "market": "0xA", "asset_id": "t"}) == "1001"
    assert (
        bs.resolve_event_id(
            {"event_type": "price_change", "market": "0xB", "price_changes": [{"asset_id": "z"}]}
        )
        == "1002"
    )
    assert bs.resolve_event_id({"event_type": "last_trade_price", "market": "0xA"}) == "1001"
    assert bs.resolve_event_id({"market": "0xUNKNOWN"}) is None
    # known market kept, unknown dropped, missing market recorded (never silently lost)
    assert bs.should_record({"market": "0xA"}) is True
    assert bs.should_record({"market": "0xZZZ"}) is False
    assert bs.should_record({"event_type": "book"}) is True


def test_book_stream_single_event_accepts_all():
    bs = BookStream(token_ids=["t"], writer=None)  # no routing map (single-event back-compat)
    assert bs.should_record({"market": "anything"}) is True
    assert bs.resolve_event_id({"market": "anything"}) is None


def test_comment_stream_multi_event_routing():
    cs = CommentStream(event_ids={"1001", "1002"}, writer=None)
    a = {"type": "comment_created", "payload": {"id": "ca", "parentEntityID": 1001}}
    b = {"type": "comment_created", "payload": {"id": "cb", "parentEntityID": 1002}}
    other = {"type": "comment_created", "payload": {"id": "cx", "parentEntityID": 9999}}
    assert cs.should_record(a) and cs.should_record(b) and not cs.should_record(other)
    assert cs.resolve_event_id(a) == "1001" and cs.resolve_event_id(b) == "1002"
    cs.on_written(a)
    cs.on_written(b)
    assert cs.last_comment_id_for("1001") == "ca" and cs.last_comment_id_for("1002") == "cb"
    assert cs.last_comment_id is None  # ambiguous for a multi-event stream
    react = {"type": "reaction_created", "payload": {"id": "r", "commentID": "ca"}}
    assert cs.should_record(react) and cs.resolve_event_id(react) == "1001"


def test_comment_stream_records_series_parented_comments():
    # World Cup case: every match comment is parented to the Series (11433), not the
    # event. The firehose filter must accept the series id, or 100% of comments drop.
    cs = CommentStream(event_ids={"351771", "351765"}, series_ids={"11433"}, writer=None)
    series_comment = {"type": "comment_created", "payload": {"id": "s1", "parentEntityID": 11433}}
    event_comment = {"type": "comment_created", "payload": {"id": "e1", "parentEntityID": 351771}}
    foreign = {"type": "comment_created", "payload": {"id": "x1", "parentEntityID": 99999}}
    assert cs.should_record(series_comment)  # series-parented -> kept
    assert cs.should_record(event_comment)  # event-parented -> kept
    assert not cs.should_record(foreign)  # unrelated parent -> dropped
    assert cs.resolve_event_id(series_comment) == "11433"
    # The series cursor advances and seeds reaction attribution to the series.
    cs.on_written(series_comment)
    assert cs.last_comment_id_for("11433") == "s1"
    react = {"type": "reaction_created", "payload": {"id": "r1", "commentID": "s1"}}
    assert cs.should_record(react) and cs.resolve_event_id(react) == "11433"


def test_cond_to_event_maps_condition_ids():
    e1 = EventInfo(
        event_id="1001",
        title=None,
        slug=None,
        markets=(
            Market(id="m1", condition_id="0xA", token_ids=("t1", "t2")),
            Market(id="m2", condition_id="0xB", token_ids=("t3", "t4")),
        ),
        raw={},
    )
    e2 = EventInfo(
        event_id="1002",
        title=None,
        slug=None,
        markets=(Market(id="m3", condition_id="0xC", token_ids=("t5", "t6")),),
        raw={},
    )
    assert cond_to_event([e1, e2]) == {"0xA": "1001", "0xB": "1001", "0xC": "1002"}
