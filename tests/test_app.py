"""Integration tests for orchestration + graceful shutdown (offline)."""

from __future__ import annotations

import asyncio
import json

import pytest

from polytape import app
from polytape.gamma import EventInfo, GammaError


class RaisingGamma:
    """Gamma whose event resolution always fails (e.g. a series id, or an outage)."""

    def __init__(self) -> None:
        self.closed = False

    async def resolve_event(self, event_id, market_ids=()):
        raise GammaError(f"event {event_id} not found (HTTP 404)")

    async def backfill_since(self, *_a, **_k):
        return []

    async def aclose(self):
        self.closed = True


class FakeGamma:
    def __init__(self, event: EventInfo) -> None:
        self.event = event
        self.closed = False

    async def resolve_event(self, event_id, market_ids=()):
        return self.event

    async def backfill_since(self, event_id, last):
        return []

    async def aclose(self):
        self.closed = True


async def test_run_records_both_streams_and_finalizes(make_config, sample_event, make_connect):
    cfg = make_config()
    cframe = {
        "topic": "comments",
        "type": "comment_created",
        "payload": {
            "id": "c1",
            "parentEntityID": 20200,
            "userAddress": "0xW",
            "createdAt": "2025-01-01T00:00:00Z",
        },
    }
    bframe = {"event_type": "book", "asset_id": "t1", "hash": "0xH", "timestamp": "1700000000000"}
    connect = make_connect(
        by_url={"live-data": [json.dumps(cframe)], "clob": [json.dumps(bframe)]}, blocking=True
    )
    gamma = FakeGamma(sample_event)

    task = asyncio.create_task(app.run(cfg, gamma=gamma, connect=connect))
    await asyncio.sleep(0.2)  # let both connect and consume
    task.cancel()  # simulate Ctrl-C
    assert await task == 0
    assert gamma.closed is False  # injected gamma is not owned -> not closed

    meta = json.loads((cfg.event_dir / "meta.json").read_text(encoding="utf-8"))
    assert meta["counts"] == {"comments": 1, "book": 1}
    assert meta["stopped_at"]
    crec = json.loads(
        (cfg.event_dir / "comments.jsonl").read_text(encoding="utf-8").splitlines()[0]
    )
    assert crec["raw"]["payload"]["userAddress"] != "0xW"  # hashed by default


async def test_run_book_only_without_tokens_returns_1(make_config, make_connect):
    cfg = make_config(comments=False)
    empty_event = EventInfo(event_id="20200", title=None, slug=None, markets=(), raw={})
    rc = await app.run(cfg, gamma=FakeGamma(empty_event), connect=make_connect([]))
    assert rc == 1


async def test_run_degrades_to_comments_when_event_unresolvable(make_config, make_connect):
    """A chat capture proceeds even if Gamma can't resolve the id (e.g. a series id).

    The comment stream only needs the numeric id it filters ``parentEntityID`` by,
    so an unresolvable id falls back to a stub event (no markets/book) rather than
    aborting the whole capture.
    """
    cfg = make_config(book=False)  # comments only
    cframe = {
        "topic": "comments",
        "type": "comment_created",
        "payload": {"id": "c1", "parentEntityID": 20200, "createdAt": "2025-01-01T00:00:00Z"},
    }
    connect = make_connect(by_url={"live-data": [json.dumps(cframe)]}, blocking=True)

    task = asyncio.create_task(app.run(cfg, gamma=RaisingGamma(), connect=connect))
    await asyncio.sleep(0.2)
    task.cancel()
    assert await task == 0

    meta = json.loads((cfg.event_dir / "meta.json").read_text(encoding="utf-8"))
    assert meta["counts"]["comments"] == 1  # the chat was captured despite no resolution
    assert meta["event"]["id"] == "20200"
    assert meta["event"]["title"] is None  # stub event -> no title/markets
    assert meta["event"]["markets"] == []


async def test_run_book_only_unresolvable_event_raises(make_config, make_connect):
    """A book capture still needs real markets, so its resolution failure is fatal."""
    cfg = make_config(comments=False)  # book only
    with pytest.raises(GammaError):
        await app.run(cfg, gamma=RaisingGamma(), connect=make_connect([]))


async def test_run_series_capture_skips_resolution_and_records(make_config, make_connect):
    """A Series chat (entity_type='Series') is recorded by id with NO event resolution."""

    class BoomGamma:
        async def resolve_event(self, *_a, **_k):
            raise AssertionError("a Series id must not be resolved via /events/")

        async def backfill_since(self, *_a, **_k):
            return []

        async def aclose(self):
            pass

    cfg = make_config(book=False, entity_type="Series")  # comments-only series chat
    cframe = {
        "topic": "comments",
        "type": "comment_created",
        "payload": {
            "id": "s1",
            "parentEntityID": 20200,
            "parentEntityType": "Series",
            "createdAt": "2025-01-01T00:00:00Z",
        },
    }
    connect = make_connect(by_url={"live-data": [json.dumps(cframe)]}, blocking=True)

    task = asyncio.create_task(app.run(cfg, gamma=BoomGamma(), connect=connect))
    await asyncio.sleep(0.2)
    task.cancel()
    assert await task == 0

    meta = json.loads((cfg.event_dir / "meta.json").read_text(encoding="utf-8"))
    assert meta["counts"]["comments"] == 1  # the series chat was captured by id
