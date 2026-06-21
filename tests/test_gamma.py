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
    parse_event_ref,
    related_events,
    resolve_event_id,
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


def test_parse_event_series_ids():
    obj = {**EVENT_OBJ, "series": [{"id": 11433, "slug": "soccer-fifwc"}, {"id": "999"}]}
    assert _parse_event(obj, "12345").series_ids == ("11433", "999")
    assert _parse_event(EVENT_OBJ, "12345").series_ids == ()  # no series -> empty


def test_parse_token_ids_stringified():
    assert _parse_token_ids({"clobTokenIds": '["a","b"]'}) == ("a", "b")
    assert _parse_token_ids({}) == ()
    with pytest.raises(GammaError):
        _parse_token_ids({"clobTokenIds": "not json"})


def test_parse_event_empty_list_raises():
    with pytest.raises(GammaError):
        _parse_event([], "12345")


def test_parse_event_ref_id_slug_and_url():
    assert parse_event_ref("351729") == ("id", "351729")
    assert parse_event_ref("  351729  ") == ("id", "351729")
    assert parse_event_ref("fifwc-ksa-ury-2026-06-15") == ("slug", "fifwc-ksa-ury-2026-06-15")
    assert parse_event_ref("https://polymarket.com/sports/world-cup/fifwc-ksa-ury-2026-06-15") == (
        "slug",
        "fifwc-ksa-ury-2026-06-15",
    )
    # trailing slash + query string are tolerated
    assert parse_event_ref("https://polymarket.com/event/big-game/?tid=1") == ("slug", "big-game")
    with pytest.raises(GammaError):
        parse_event_ref("   ")


def test_resolve_event_id_numeric_needs_no_network():
    # A numeric ref returns immediately; passing client=None would try the network,
    # so reaching the return proves no request was made.
    assert resolve_event_id("351729", client=None) == "351729"


def test_resolve_event_id_slug_via_gamma():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        return httpx.Response(200, json=[{"id": "351729", "title": "Saudi Arabia vs. Uruguay"}])

    http = httpx.Client(
        transport=httpx.MockTransport(handler), base_url="https://gamma-api.polymarket.com"
    )
    assert resolve_event_id("fifwc-ksa-ury-2026-06-15", client=http) == "351729"
    assert "slug=fifwc-ksa-ury-2026-06-15" in seen["url"]


def test_resolve_event_id_unknown_slug_raises():
    http = httpx.Client(
        transport=httpx.MockTransport(lambda r: httpx.Response(200, json=[])),
        base_url="https://gamma-api.polymarket.com",
    )
    with pytest.raises(GammaError):
        resolve_event_id("no-such-event", client=http)


def test_related_events_lists_series_matches():
    def handler(req):
        p = req.url.params
        if "slug" in p:  # resolve the source event
            return httpx.Response(
                200,
                json=[
                    {
                        "id": "351730",
                        "slug": "fifwc-irn-nzl",
                        "series": [{"id": 11433, "title": "FIFA World Cup"}],
                    }
                ],
            )
        if "series_id" in p:  # list the series' open events
            assert p["series_id"] == "11433" and p["closed"] == "false"
            return httpx.Response(
                200,
                json=[
                    {"id": "351731", "slug": "a", "title": "France vs. Senegal", "closed": False},
                    {
                        "id": "351730",
                        "slug": "fifwc-irn-nzl",
                        "title": "Iran vs. NZ",
                        "closed": False,
                    },
                ],
            )
        return httpx.Response(404, json={})

    http = httpx.Client(
        transport=httpx.MockTransport(handler), base_url="https://gamma-api.polymarket.com"
    )
    out = related_events("https://polymarket.com/sports/world-cup/fifwc-irn-nzl", client=http)
    assert out["source_event_id"] == "351730"
    assert out["series_id"] == "11433"
    assert out["series_title"] == "FIFA World Cup"
    assert [e["event_id"] for e in out["events"]] == ["351731", "351730"]
    assert out["events"][0]["title"] == "France vs. Senegal"


def test_related_events_no_series_returns_self():
    http = httpx.Client(
        transport=httpx.MockTransport(
            lambda r: httpx.Response(200, json=[{"id": "999", "slug": "solo", "title": "Solo"}])
        ),
        base_url="https://gamma-api.polymarket.com",
    )
    out = related_events("solo", client=http)
    assert out["series_id"] is None
    assert [e["event_id"] for e in out["events"]] == ["999"]


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


async def test_fetch_comments_series_parent():
    seen: dict[str, str] = {}

    def handler(req):
        seen.update(dict(req.url.params))
        return httpx.Response(200, json=[{"id": "s1"}])

    g = _client(handler)
    out = await g.fetch_comments("11433", parent_entity_type="Series")
    assert out == [{"id": "s1"}]
    assert seen["parent_entity_type"] == "Series" and seen["parent_entity_id"] == "11433"
    await g.aclose()


async def test_fetch_comments_requests_holdings_by_default():
    seen: dict[str, str] = {}

    def handler(req):
        seen.clear()
        seen.update(dict(req.url.params))
        return httpx.Response(200, json=[])

    g = _client(handler)
    await g.fetch_comments("11433", parent_entity_type="Series")
    assert seen.get("get_positions") == "true"  # holdings requested by default
    await g.fetch_comments("11433", parent_entity_type="Series", get_positions=False)
    assert "get_positions" not in seen  # opt-out drops the param
    await g.aclose()


async def test_backfill_threads_get_positions():
    seen: dict[str, str] = {}

    def handler(req):
        seen.update(dict(req.url.params))
        return httpx.Response(200, json=[])

    g = _client(handler)
    await g.backfill_since("11433", parent_entity_type="Series", max_pages=1)
    assert seen.get("get_positions") == "true"
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
