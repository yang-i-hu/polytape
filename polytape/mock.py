"""Offline ``--dry-run``: exercise the whole capture path with no network.

Synthetic comment and book frames are pushed through the *real* stream consumers
(via an in-process fake connection) and the real writer, so envelope
construction, hashing, dedup, JSONL output, and ``meta.json`` are all exercised
exactly as in a live capture. A disconnect + comment backfill is simulated too.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from polytape.config import STREAM_COMMENTS, Config
from polytape.envelope import Hasher, utc_now_iso
from polytape.gamma import EventInfo, Market
from polytape.streams.clob import BookStream
from polytape.streams.rtds import CommentStream
from polytape.writer import CaptureWriter

logger = logging.getLogger("polytape.mock")


# --------------------------------------------------------------------------- #
# Synthetic data
# --------------------------------------------------------------------------- #


def synthetic_event(config: Config) -> EventInfo:
    """A fake resolved event for the dry run."""
    return EventInfo(
        event_id=str(config.event_id),
        title="DRY RUN",
        slug="dry-run",
        markets=(Market(id="m-dry", condition_id="0xDRY", token_ids=("100", "200")),),
        raw={"dry_run": True},
    )


def synthetic_comment_frames(event_id: str) -> list[str]:
    """Comment-stream frames: comments, a reaction, and a duplicate (for dedup).

    Comments carry ``parentEntityID`` and the reaction a ``commentID`` so they
    pass the client-side event filter, exactly like live frames.
    """
    frames: list[str] = []
    for i in range(3):
        frames.append(
            json.dumps(
                {
                    "topic": "comments",
                    "type": "comment_created",
                    "timestamp": 1700000000000 + i,
                    "payload": {
                        "id": f"dry-c{i}",
                        "parentEntityID": event_id,
                        "parentEntityType": "Event",
                        "createdAt": f"2026-01-01T00:00:0{i}Z",
                        "userAddress": f"0xUSER{i}",
                        "body": f"synthetic comment {i}",
                        "profile": {
                            "name": f"user{i}",
                            "pseudonym": f"pseud{i}",
                            "proxyWallet": f"0xUSER{i}",
                        },
                    },
                }
            )
        )
    frames.append(
        json.dumps(
            {
                "topic": "comments",
                "type": "reaction_created",
                "timestamp": 1700000000100,
                "payload": {
                    "id": "dry-r0",
                    "commentID": "dry-c0",
                    "reactionType": "like",
                    "userAddress": "0xUSER0",
                },
            }
        )
    )
    frames.append(frames[0])  # duplicate -> deduped
    return frames


def synthetic_backfill_comments(event_id: str) -> list[dict[str, Any]]:
    """Flat (Gamma-shaped) comments simulating a reconnect backfill.

    Includes a duplicate of a live comment id to demonstrate overlap protection.
    """
    return [
        {
            "id": "dry-bf0",
            "parentEntityID": event_id,
            "createdAt": "2026-01-01T00:01:00Z",
            "userAddress": "0xUSER9",
            "body": "missed during the gap",
            "profile": {"name": "latecomer", "proxyWallet": "0xUSER9"},
        },
        {  # already seen live -> deduped, counts as not-newly-written
            "id": "dry-c0",
            "parentEntityID": event_id,
            "createdAt": "2026-01-01T00:00:00Z",
            "userAddress": "0xUSER0",
            "body": "synthetic comment 0",
        },
    ]


def synthetic_book_frames() -> list[str]:
    """Book-stream frames: snapshot, delta, trade, an array, and a duplicate."""
    return [
        json.dumps(
            {
                "event_type": "book",
                "asset_id": "100",
                "market": "0xDRY",
                "hash": "0xBOOK1",
                "timestamp": "1700000000000",
                "bids": [{"price": "0.5", "size": "100"}],
                "asks": [{"price": "0.6", "size": "80"}],
            }
        ),
        json.dumps(
            {
                "event_type": "price_change",
                "market": "0xDRY",
                "timestamp": "1700000000500",
                "price_changes": [
                    {"asset_id": "100", "price": "0.55", "size": "40", "side": "BUY", "hash": "pc1"}
                ],
            }
        ),
        json.dumps(
            {
                "event_type": "last_trade_price",
                "asset_id": "100",
                "market": "0xDRY",
                "price": "0.55",
                "size": "10",
                "side": "BUY",
                "timestamp": "1700000000600",
                "transaction_hash": "0xTX1",
            }
        ),
        json.dumps(
            [
                {
                    "event_type": "tick_size_change",
                    "asset_id": "100",
                    "market": "0xDRY",
                    "old_tick_size": "0.01",
                    "new_tick_size": "0.001",
                    "timestamp": "1700000000700",
                },
                {
                    "event_type": "book",
                    "asset_id": "200",
                    "market": "0xDRY",
                    "hash": "0xBOOK2",
                    "timestamp": "1700000000800",
                    "bids": [],
                    "asks": [],
                },
            ]
        ),
        json.dumps(  # duplicate snapshot -> deduped
            {
                "event_type": "book",
                "asset_id": "100",
                "market": "0xDRY",
                "hash": "0xBOOK1",
                "timestamp": "1700000000000",
                "bids": [{"price": "0.5", "size": "100"}],
                "asks": [{"price": "0.6", "size": "80"}],
            }
        ),
    ]


# --------------------------------------------------------------------------- #
# Fake in-process connection
# --------------------------------------------------------------------------- #


class _MockWS:
    def __init__(self, frames: list[str]) -> None:
        self._frames = list(frames)
        self.sent: list[str] = []

    async def send(self, data: str) -> None:
        self.sent.append(data)

    def __aiter__(self) -> _MockWS:
        return self

    async def __anext__(self) -> str:
        if self._frames:
            await asyncio.sleep(0)  # cooperative yield
            return self._frames.pop(0)
        raise StopAsyncIteration


class _MockCM:
    def __init__(self, ws: _MockWS) -> None:
        self._ws = ws

    async def __aenter__(self) -> _MockWS:
        return self._ws

    async def __aexit__(self, *_exc: object) -> bool:
        return False


def _mock_connect(frames: list[str]):
    ws = _MockWS(frames)

    def factory(_url: str) -> _MockCM:
        return _MockCM(ws)

    return factory


# --------------------------------------------------------------------------- #
# Runner
# --------------------------------------------------------------------------- #


async def run_dry(config: Config) -> int:
    """Async dry-run: drive synthetic frames through the real pipeline."""
    hasher = Hasher() if config.hash_usernames else None
    event = synthetic_event(config)
    logger.info("DRY RUN: feeding synthetic messages through the pipeline (no network)")
    with CaptureWriter(config, event_info=event, hasher=hasher) as writer:
        if config.comments:
            comments = CommentStream(
                event_id=event.event_id,
                writer=writer,
                connect=_mock_connect(synthetic_comment_frames(event.event_id)),
            )
            await comments.run_once()
            # Simulate a disconnect + comment backfill recovery.
            disconnected_at = utc_now_iso()
            recovered = 0
            for comment in synthetic_backfill_comments(event.event_id):
                if writer.write(STREAM_COMMENTS, comment):
                    recovered += 1
            writer.record_gap(
                STREAM_COMMENTS,
                disconnected_at,
                utc_now_iso(),
                backfilled=recovered,
                note="dry-run simulated gap",
            )
        if config.book:
            book = BookStream(
                token_ids=event.clob_token_ids,
                writer=writer,
                connect=_mock_connect(synthetic_book_frames()),
            )
            await book.run_once()
        counts = writer.counts
    logger.info("DRY RUN complete: counts=%s -> %s", counts, event.event_id)
    return 0


def run_dry_run(config: Config) -> int:
    """Synchronous entry point for ``--dry-run`` (wraps :func:`run_dry`)."""
    return asyncio.run(run_dry(config))
