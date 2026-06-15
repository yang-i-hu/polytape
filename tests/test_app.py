"""Integration tests for orchestration + graceful shutdown (offline)."""

from __future__ import annotations

import asyncio
import json

from polytape import app
from polytape.gamma import EventInfo


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
