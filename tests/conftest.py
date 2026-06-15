"""Shared pytest fixtures and offline fakes (no network in any test)."""

from __future__ import annotations

import asyncio

import pytest

from polytape.config import Config
from polytape.gamma import EventInfo, Market


class FakeWS:
    """A fake websocket that yields a fixed list of frames, then ends."""

    def __init__(self, frames: list[str]) -> None:
        self._frames = list(frames)
        self.sent: list[str] = []
        self._block = asyncio.Event()
        self.blocking = False

    async def send(self, data: str) -> None:
        self.sent.append(data)

    def __aiter__(self) -> FakeWS:
        return self

    async def __anext__(self) -> str:
        if self._frames:
            await asyncio.sleep(0)
            return self._frames.pop(0)
        if self.blocking:
            await self._block.wait()  # stay open until cancelled
        raise StopAsyncIteration


class FakeCM:
    """Async context manager yielding a :class:`FakeWS`."""

    def __init__(self, ws: FakeWS) -> None:
        self._ws = ws

    async def __aenter__(self) -> FakeWS:
        return self._ws

    async def __aexit__(self, *_exc: object) -> bool:
        return False


@pytest.fixture
def make_connect():
    """Factory for an injectable websocket ``connect`` callable.

    Single ws: ``make_connect(frames)``.
    Routed by url substring: ``make_connect(by_url={"live-data": [...], "clob": [...]})``.
    Pass ``blocking=True`` to keep the connection open after the frames.
    """

    def _make(frames=None, *, blocking=False, by_url=None):
        if by_url is not None:
            wss = {}
            for sub, frs in by_url.items():
                ws = FakeWS(frs)
                ws.blocking = blocking
                wss[sub] = ws

            def factory(url):
                for sub, ws in wss.items():
                    if sub in url:
                        return FakeCM(ws)
                raise AssertionError(f"no fake ws registered for {url}")

            factory.wss = wss
            return factory

        ws = FakeWS(frames or [])
        ws.blocking = blocking

        def factory(_url):
            return FakeCM(ws)

        factory.ws = ws
        return factory

    return _make


@pytest.fixture
def sample_event() -> EventInfo:
    return EventInfo(
        event_id="20200",
        title="Test Event",
        slug="test-event",
        markets=(Market(id="m1", condition_id="0xc1", token_ids=("t1", "t2")),),
        raw={"id": "20200"},
    )


@pytest.fixture
def make_config(tmp_path):
    """Factory for a :class:`Config` rooted in a temp dir (event_id 20200)."""

    def _make(**kwargs) -> Config:
        kwargs.setdefault("event_id", "20200")
        kwargs.setdefault("out_dir", tmp_path)
        return Config(**kwargs)

    return _make
