"""Discover which events have *active chat right now*.

The comment volume on any one event is entirely event-driven: a marquee live
match can hit several messages a second while most events sit idle. Picking a
quiet event is the usual reason a capture looks empty. This module samples the
RTDS ``comments`` firehose for a few seconds and ranks events by how much live
chat they are producing, so the dashboard can point a recording at an event that
is actually busy.

Sampling is read-only and unauthenticated (same firehose the recorder uses); it
opens one short-lived websocket, counts messages per ``parentEntityID``, then
(best-effort) resolves event titles via Gamma.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
from typing import Any

import httpx
import websockets

from polytape.gamma import _USER_AGENT, GAMMA_BASE_URL
from polytape.streams.rtds import RTDS_URL, comment_subscribe_frame

logger = logging.getLogger("polytape.discover")

# Keep a hard ceiling so a misconfigured caller can't hold a worker thread open.
_MAX_SAMPLE_SECONDS = 30.0
_WS_MAX_SIZE = 4 * 1024 * 1024


def _core(obj: dict[str, Any]) -> dict[str, Any]:
    payload = obj.get("payload")
    return payload if isinstance(payload, dict) else obj


async def _sample(seconds: float) -> dict[str, dict[str, Any]]:
    """Sample the comments firehose for ``seconds``; tally activity per event."""
    by_event: dict[str, dict[str, Any]] = {}
    loop = asyncio.get_event_loop()
    deadline = loop.time() + seconds
    async with websockets.connect(
        RTDS_URL, ping_interval=None, max_size=_WS_MAX_SIZE, open_timeout=15
    ) as ws:
        await ws.send(comment_subscribe_frame())
        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                break
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=remaining)
            except (asyncio.TimeoutError, websockets.ConnectionClosed):
                break
            try:
                obj = json.loads(msg)
            except (json.JSONDecodeError, TypeError):
                continue
            if not isinstance(obj, dict):
                continue
            core = _core(obj)
            pid = core.get("parentEntityID")
            if pid is None:
                # Reactions carry no parentEntityID, so they can't be attributed
                # to an event from the firehose — skip them for ranking entirely.
                continue
            key = str(pid)
            ent = by_event.setdefault(
                key,
                {
                    "event_id": key,
                    "parent_entity_type": core.get("parentEntityType") or "Event",
                    "comments": 0,
                    "sample": None,
                    "last_author": None,
                },
            )
            ent["comments"] += 1  # every parentEntityID-bearing frame is a comment event
            body = core.get("body")
            if body and ent["sample"] is None:
                ent["sample"] = str(body)[:160]
                prof = core.get("profile") or {}
                ent["last_author"] = prof.get("name") or prof.get("pseudonym")
    return by_event


def _resolve_titles(events: list[dict[str, Any]], timeout: float) -> None:
    """Best-effort: fill in each event's title/slug from Gamma (mutates in place)."""
    client = httpx.Client(
        base_url=GAMMA_BASE_URL,
        timeout=timeout,
        headers={"User-Agent": _USER_AGENT, "Accept": "application/json"},
    )
    try:
        for ev in events:
            # Sports chat often lives on the parent Series (league/tournament),
            # so resolve titles from the matching Gamma collection.
            path = "/series/" if (ev.get("parent_entity_type") == "Series") else "/events/"
            try:
                resp = client.get(f"{path}{ev['event_id']}")
                resp.raise_for_status()
                obj = resp.json()
                if isinstance(obj, list):
                    obj = obj[0] if obj else {}
                if isinstance(obj, dict):
                    ev["title"] = obj.get("title")
                    ev["slug"] = obj.get("slug")
            except (httpx.HTTPError, ValueError, IndexError):
                continue
    finally:
        client.close()


def active_chat_events(
    seconds: float = 8.0,
    *,
    max_events: int = 12,
    resolve_titles: bool = True,
    title_timeout: float = 8.0,
) -> dict[str, Any]:
    """Sample the comments firehose and return events ranked by live chat volume.

    Args:
        seconds: How long to sample (clamped to ``[2, 30]``).
        max_events: How many of the busiest events to return.
        resolve_titles: Look up each event's title/slug via Gamma (best-effort).
        title_timeout: HTTP timeout for the whole title-resolution pass.

    Returns a dict with the sample window, total counts, and a ranked ``events``
    list (busiest first), each entry carrying ``event_id``, ``parent_entity_type``,
    ``comments``, a ``sample`` message, and (if resolved) ``title``/``slug``.
    """
    try:
        seconds = float(seconds)
    except (TypeError, ValueError):
        seconds = 8.0
    if not math.isfinite(seconds):  # NaN/inf slip past a plain min/max clamp
        seconds = 8.0
    seconds = max(2.0, min(_MAX_SAMPLE_SECONDS, seconds))
    by_event = asyncio.run(_sample(seconds))
    ranked = sorted(by_event.values(), key=lambda e: e["comments"], reverse=True)
    top = ranked[: max(1, int(max_events))]
    if resolve_titles and top:
        try:
            _resolve_titles(top, title_timeout)
        except Exception:  # noqa: BLE001 — titles are a nicety, never fatal
            logger.debug("title resolution failed", exc_info=True)
    return {
        "sampled_seconds": round(seconds, 1),
        "total_events": len(by_event),
        "total_comments": sum(e["comments"] for e in by_event.values()),
        "events": top,
    }
