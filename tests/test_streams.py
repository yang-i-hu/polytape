"""Tests for the websocket stream consumers (offline, fake connection)."""

from __future__ import annotations

import json

from polytape.streams.base import WebSocketStream
from polytape.streams.clob import BookStream, book_subscribe_frame
from polytape.streams.rtds import CommentStream, comment_subscribe_frame
from polytape.writer import CaptureWriter


def test_comment_subscribe_frame():
    frame = json.loads(comment_subscribe_frame("20200"))
    assert frame["action"] == "subscribe"
    sub = frame["subscriptions"][0]
    assert sub["topic"] == "comments" and sub["type"] == "*"
    assert json.loads(sub["filters"]) == {"parentEntityID": 20200, "parentEntityType": "Event"}


def test_comment_subscribe_frame_nonnumeric_passthrough():
    inner = json.loads(json.loads(comment_subscribe_frame("demo"))["subscriptions"][0]["filters"])
    assert inner["parentEntityID"] == "demo"


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
        return {"type": "comment_created", "payload": {"id": cid, "userAddress": "0xa"}}

    react = {"type": "reaction_created", "payload": {"id": "r1", "userAddress": "0xb"}}
    frames = [
        json.dumps(c("c1")),
        json.dumps(react),
        json.dumps([c("c2"), c("c3")]),
        json.dumps(c("c1")),
    ]
    connect = make_connect(frames)
    with CaptureWriter(cfg) as w:
        cs = CommentStream(event_id="20200", writer=w, connect=connect)
        await cs.run_once()
        assert w.counts["comments"] == 4  # c1, r1, c2, c3 (dup c1 skipped)
        assert cs.last_comment_id == "c3"  # reaction ignored for the cursor
    assert connect.ws.sent == [comment_subscribe_frame("20200")]


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
