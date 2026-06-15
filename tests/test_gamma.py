"""Tests for the Gamma client: pure parsing and async paths via MockTransport."""

from __future__ import annotations

import json

import httpx
import pytest

from polytape.gamma import (
    GammaClient,
    GammaError,
    _filter_markets,
    _parse_event,
    _parse_token_ids,
)

EVENT_OBJ = {
    "id": "12345",
    "title": "Big Game",
    "slug": "big-game",
    "markets": [
        {"id": "239826", "conditionId": "0xcond1", "clobTokenIds": json.dumps(["7142", "9823"])},
        {"id": "239827", "conditionId": "0xcond2", "clobTokenIds": json.dumps(["111", "222"])},
    ],
}


# -- pure parsing -------------------------------------------------------- #


def test_parse_event_object_and_list_forms():
    info = _parse_event(EVENT_OBJ, "12345")
    assert info.event_id == "12345" and info.title == "Big Game"
    assert info.gamma_market_ids == ("239826", "239827")
    assert info.condition_ids == ("0xcond1", "0xcond2")
    assert info.clob_token_ids == ("7142", "9823", "111", "222")
    assert _parse_event([EVENT_OBJ], "12345").event_id == "12345"  # ?id= array form


def test_parse_token_ids_stringified():
    assert _parse_token_ids({"clobTokenIds": '["a","b"]'}) == ("a", "b")
    assert _parse_token_ids({}) == ()
    with pytest.raises(GammaError):
        _parse_token_ids({"clobTokenIds": "not json"})


def test_parse_event_empty_list_raises():
    with pytest.raises(GammaError):
        _parse_event([], "12345")


def test_filter_markets_by_gamma_or_condition_id():
    info = _parse_event(EVENT_OBJ, "12345")
    assert len(_filter_markets(info.markets, ("239826",))) == 1
    assert len(_filter_markets(info.markets, ("0xcond2",))) == 1
    with pytest.raises(GammaError):
        _filter_markets(info.markets, ("nope",))


# -- async network paths (MockTransport, no real network) ---------------- #


def _client(handler) -> GammaClient:
    transport = httpx.MockTransport(handler)
    http = httpx.AsyncClient(transport=transport, base_url="https://gamma-api.polymarket.com")
    return GammaClient(client=http, backoff_base=0.001, page_delay=0.0)


async def test_resolve_event_and_filter():
    def handler(req):
        return httpx.Response(200, json=EVENT_OBJ)

    g = _client(handler)
    ev = await g.resolve_event("12345")
    assert ev.clob_token_ids == ("7142", "9823", "111", "222")
    ev2 = await g.resolve_event("12345", market_ids=("0xcond1",))
    assert ev2.clob_token_ids == ("7142", "9823")
    await g.aclose()


async def test_resolve_event_404():
    g = _client(lambda req: httpx.Response(404, json={"error": "nf"}))
    with pytest.raises(GammaError, match="404"):
        await g.resolve_event("12345")
    await g.aclose()


async def test_get_retries_on_5xx():
    state = {"n": 0}

    def handler(req):
        state["n"] += 1
        if state["n"] == 1:
            return httpx.Response(503, text="later")
        return httpx.Response(200, json=EVENT_OBJ)

    g = _client(handler)
    ev = await g.resolve_event("12345")
    assert ev.event_id == "12345" and state["n"] == 2
    await g.aclose()


async def test_backfill_since_stops_at_last_seen():
    newest_first = [{"id": f"c{i}", "createdAt": f"t{i}"} for i in (5, 4, 3, 2, 1)]

    def handler(req):
        q = req.url.params
        assert q["parent_entity_type"] == "Event"
        assert q["parent_entity_id"] == "12345"
        off, lim = int(q["offset"]), int(q["limit"])
        return httpx.Response(200, json=newest_first[off : off + lim])

    g = _client(handler)
    bf = await g.backfill_since("12345", last_seen_id="c2", page_size=2, max_pages=10)
    assert [c["id"] for c in bf] == ["c3", "c4", "c5"]  # chronological, exclusive of c2
    full = await g.backfill_since("12345", last_seen_id="missing", page_size=2, max_pages=10)
    assert [c["id"] for c in full] == ["c1", "c2", "c3", "c4", "c5"]
    await g.aclose()
