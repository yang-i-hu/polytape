"""Reconnect supervisor: keep a stream alive across disconnects, with backfill.

A :class:`StreamSupervisor` runs a single :class:`~polytape.streams.base.WebSocketStream`
in a loop, reconnecting with exponential backoff (plus jitter) whenever the
session ends. Each disconnect/recovery is recorded in ``meta.json`` via the
writer's gap log, and — for the comment stream — missed messages are backfilled
from Gamma on reconnect.

Losing the burst of activity around a key moment is the worst-case failure for a
recorder, so the reconnect/backfill path is the most important code here.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from polytape.envelope import utc_now_iso
from polytape.writer import FatalRecorderError

if TYPE_CHECKING:
    from polytape.gamma import GammaClient
    from polytape.streams.base import WebSocketStream
    from polytape.streams.rtds import CommentStream
    from polytape.writer import CaptureWriter

logger = logging.getLogger("polytape.supervisor")

BackfillCallback = Callable[[], Awaitable[int]]


class StreamSupervisor:
    """Supervises one websocket stream: reconnect with backoff, gap log, backfill."""

    def __init__(
        self,
        stream: WebSocketStream,
        *,
        writer: CaptureWriter,
        backfill: BackfillCallback | None = None,
        base_delay: float = 1.0,
        max_delay: float = 30.0,
        reset_after: float = 15.0,
        jitter: float = 0.3,
        clock: Callable[[], str] = utc_now_iso,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._stream = stream
        self._writer = writer
        self._backfill = backfill
        self._base_delay = base_delay
        self._max_delay = max_delay
        self._reset_after = reset_after
        self._jitter = jitter
        self._clock = clock
        self._monotonic = monotonic
        self._name = stream.stream
        self._stop = asyncio.Event()

    @property
    def name(self) -> str:
        """The supervised stream's name."""
        return self._name

    def stop(self) -> None:
        """Request a graceful stop; the current backoff wait is interrupted."""
        self._stop.set()

    def _backoff(self, attempt: int) -> float:
        delay = min(self._max_delay, self._base_delay * (2**attempt))
        if self._jitter:
            delay *= 1 + random.uniform(0, self._jitter)
        return delay

    async def _sleep_or_stop(self, delay: float) -> None:
        """Sleep up to ``delay`` seconds, returning early if a stop is requested."""
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=delay)
        except asyncio.TimeoutError:
            pass

    async def _handle_reconnect(self, disconnected_at: str) -> None:
        """Record a gap (and backfill, for comments) after a successful reconnect."""
        reconnected_at = self._clock()
        backfilled = 0
        if self._backfill is not None:
            try:
                backfilled = await self._backfill()
            except asyncio.CancelledError:
                raise
            except FatalRecorderError:
                raise  # disk full while backfilling: surface it, don't swallow
            except Exception:
                logger.exception("%s: backfill failed on reconnect", self._name)
        self._writer.record_gap(
            self._name,
            disconnected_at,
            reconnected_at,
            backfilled=backfilled,
            note="reconnect",
        )

    async def run(self) -> None:
        """Run the supervised stream until :meth:`stop` is called.

        Exits cleanly (without reconnecting) once a stop has been requested.
        """
        attempt = 0
        pending_disconnect: str | None = None

        async def _on_connect() -> None:
            nonlocal pending_disconnect
            if pending_disconnect is not None:
                await self._handle_reconnect(pending_disconnect)
                pending_disconnect = None

        while not self._stop.is_set():
            started = self._monotonic()
            try:
                await self._stream.run_once(on_connect=_on_connect)
            except asyncio.CancelledError:
                raise
            except FatalRecorderError:
                raise  # disk full / unrecoverable: do NOT reconnect, stop the process
            except Exception as exc:
                logger.warning("%s: connection error: %s", self._name, exc)
            else:
                logger.info("%s: session ended", self._name)

            # Latch the first drop time and hold it across failed retries so the
            # recorded gap reflects true end-to-end downtime.
            if pending_disconnect is None:
                pending_disconnect = self._clock()

            if self._stop.is_set():
                break

            uptime = self._monotonic() - started
            attempt = 0 if uptime >= self._reset_after else attempt + 1
            delay = self._backoff(attempt)
            logger.info("%s: reconnecting in %.1fs", self._name, delay)
            await self._sleep_or_stop(delay)

        logger.info("%s: supervisor stopped", self._name)


def make_comment_backfill(
    stream: CommentStream,
    gamma: GammaClient,
    writer: CaptureWriter,
) -> BackfillCallback:
    """Build a backfill callback that recovers comments missed during a gap.

    Pages Gamma from each parent's last-seen comment id and writes recovered
    comments through the writer (dedup prevents overlap with the live stream).
    Each parent is paged with its correct ``parent_entity_type`` — Event ids as
    ``"Event"``, series ids as ``"Series"`` — because the comment feed for some
    products (e.g. the World Cup) hangs off the parent Series, where an Event-typed
    query returns nothing. Returns the number of *newly written* comments.
    """
    # (parent_id, parent_entity_type) for every parent this run records.
    targets: list[tuple[str, str]] = [(eid, "Event") for eid in stream.event_ids]
    targets += [(sid, "Series") for sid in stream.series_ids]

    async def _backfill() -> int:
        count = 0
        # One firehose, N parents: page each from its own cursor and tag the
        # recovered comments with that parent id (dedup guards against overlap).
        for parent_id, parent_type in targets:
            missed = await gamma.backfill_since(
                parent_id,
                stream.last_comment_id_for(parent_id),
                parent_entity_type=parent_type,
            )
            for comment in missed:
                if writer.write(stream.stream, comment, event_id=parent_id):
                    count += 1
        if count:
            logger.info("%s: backfilled %d missed comment(s)", stream.stream, count)
        return count

    return _backfill
