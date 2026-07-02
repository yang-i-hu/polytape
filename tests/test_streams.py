"""Tests for the websocket stream consumers (offline, fake connection)."""

from __future__ import annotations

import asyncio
import json

import pytest

from polytape.gamma import EventInfo, Market, cond_to_event
from polytape.streams.base import StreamInactivityError, WebSocketStream
from polytape.streams.clob import BookStream, book_subscribe_frame, shard_tokens
from polytape.writer import CaptureWriter


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


async def test_book_stream_ids(make_config, make_connect):
    cfg = make_config()
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
    cfg = make_config()
    connect = make_connect([], blocking=True)
    with CaptureWriter(cfg) as w:
        bs = BookStream(token_ids=["t1"], writer=w, connect=connect)
        bs.read_timeout = 0.05
        with pytest.raises(StreamInactivityError):
            await asyncio.wait_for(bs.run_once(), timeout=2.0)


async def test_watchdog_does_not_trip_while_data_flows(make_config, make_connect):
    # A frame within the deadline must not trip the watchdog; a clean close returns.
    cfg = make_config()
    book = {"event_type": "book", "asset_id": "t1", "hash": "0xH", "timestamp": "1700000000000"}
    connect = make_connect([json.dumps(book)])
    with CaptureWriter(cfg) as w:
        bs = BookStream(token_ids=["t1"], writer=w, connect=connect)
        bs.read_timeout = 0.5
        await asyncio.wait_for(bs.run_once(), timeout=2.0)
        assert w.counts["book"] == 1


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
