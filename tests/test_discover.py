"""Unit tests for active-chat discovery (offline; the firehose is faked)."""

from __future__ import annotations

import asyncio
import json

from polytape.streams import discover


class _WS:
    """Fake RTDS socket: yields queued frames from ``recv()``, then 'times out'."""

    def __init__(self, frames: list[str]) -> None:
        self._frames = list(frames)
        self.sent: list[str] = []

    async def send(self, data: str) -> None:
        self.sent.append(data)

    async def recv(self) -> str:
        if self._frames:
            return self._frames.pop(0)
        raise asyncio.TimeoutError  # caught by _sample -> ends the window


class _CM:
    def __init__(self, ws: _WS) -> None:
        self._ws = ws

    async def __aenter__(self) -> _WS:
        return self._ws

    async def __aexit__(self, *_exc: object) -> bool:
        return False


def _comment(pid, body, ptype="Event", name=None):
    payload = {"parentEntityID": pid, "parentEntityType": ptype, "body": body}
    if name:
        payload["profile"] = {"name": name}
    return json.dumps({"topic": "comments", "type": "comment_created", "payload": payload})


def test_ranks_events_by_comment_volume(monkeypatch):
    frames = [
        _comment(100, "hello", name="alice"),
        _comment(200, "single"),
        _comment(100, "again"),
        _comment(100, "third"),
        # a reaction carries no parentEntityID -> ignored for ranking
        json.dumps({"topic": "comments", "type": "reaction_created",
                    "payload": {"commentID": 9, "reactionType": "HEART"}}),
    ]
    ws = _WS(frames)
    monkeypatch.setattr(discover.websockets, "connect", lambda *a, **k: _CM(ws))

    out = discover.active_chat_events(2.0, resolve_titles=False)

    assert out["total_comments"] == 4
    assert out["total_events"] == 2
    assert out["events"][0]["event_id"] == "100"  # busiest first
    assert out["events"][0]["comments"] == 3
    assert out["events"][0]["sample"] == "hello"
    assert out["events"][0]["last_author"] == "alice"
    # the subscribe frame was actually sent
    assert any("subscribe" in s for s in ws.sent)


def test_clamps_sample_window(monkeypatch):
    monkeypatch.setattr(discover.websockets, "connect", lambda *a, **k: _CM(_WS([])))
    out = discover.active_chat_events(0.1, resolve_titles=False)  # below the floor
    assert out["sampled_seconds"] == 2.0
    assert out["events"] == []


def test_max_events_caps_results(monkeypatch):
    frames = [_comment(i, f"msg{i}") for i in range(20)]
    monkeypatch.setattr(discover.websockets, "connect", lambda *a, **k: _CM(_WS(frames)))
    out = discover.active_chat_events(2.0, max_events=5, resolve_titles=False)
    assert len(out["events"]) == 5
    assert out["total_events"] == 20


def test_nan_seconds_falls_back_to_default(monkeypatch):
    # NaN slips past a plain min/max clamp (all comparisons are False); guard it.
    monkeypatch.setattr(discover.websockets, "connect", lambda *a, **k: _CM(_WS([])))
    out = discover.active_chat_events(float("nan"), resolve_titles=False)
    assert out["sampled_seconds"] == 8.0


class _Resp:
    def __init__(self, obj):
        self._obj = obj

    def raise_for_status(self):
        pass

    def json(self):
        return self._obj


def test_resolve_titles_routes_events_vs_series(monkeypatch):
    frames = [_comment(100, "a", "Event"), _comment(200, "b", "Series")]
    monkeypatch.setattr(discover.websockets, "connect", lambda *a, **k: _CM(_WS(frames)))

    calls = []

    class _FakeClient:
        def __init__(self, *_a, **_k):
            pass

        def get(self, path):
            calls.append(path)
            if path.startswith("/series/"):
                return _Resp({"title": "The Series", "slug": "srs"})
            return _Resp([{"title": "The Event", "slug": "evt"}])  # /events/ returns a list

        def close(self):
            pass

    monkeypatch.setattr(discover.httpx, "Client", _FakeClient)
    out = discover.active_chat_events(2.0, resolve_titles=True)
    by = {e["event_id"]: e for e in out["events"]}
    assert by["100"]["title"] == "The Event"  # Event -> /events/{id} (list unwrapped)
    assert by["200"]["title"] == "The Series"  # Series -> /series/{id}
    assert any(p == "/events/100" for p in calls)
    assert any(p == "/series/200" for p in calls)
