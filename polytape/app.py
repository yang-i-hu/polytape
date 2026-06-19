"""Orchestration and graceful shutdown for a polytape capture.

:func:`run` resolves the event, opens the writer, and runs a supervisor per
enabled stream concurrently until interrupted; :func:`run_live` is the
synchronous wrapper the CLI calls. On ``Ctrl-C``/SIGTERM the supervisors are
stopped and cancelled, the writer is flushed and closed (finalizing
``meta.json``), and the process exits without corrupting output.

Because every JSON line is flushed on write and ``meta.json`` is written
atomically, output is crash-safe even on an abrupt kill; graceful shutdown adds
the stop time and final counts.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal
from collections.abc import Awaitable, Callable
from typing import Any

import httpx

from polytape.config import Config
from polytape.envelope import Hasher
from polytape.gamma import EventInfo, GammaClient, GammaError
from polytape.streams.base import ConnectFactory
from polytape.streams.clob import BookStream
from polytape.streams.rtds import CommentStream
from polytape.supervisor import StreamSupervisor, make_comment_backfill
from polytape.writer import CaptureWriter, FatalRecorderError

logger = logging.getLogger("polytape.app")

#: Env var holding the external dead-man's-switch URL (e.g. a healthchecks.io ping
#: URL). Optional; if unset, no heartbeat is sent.
HEARTBEAT_ENV = "POLYTAPE_HEARTBEAT_URL"


async def _heartbeat(
    url: str,
    last_activity: list[float],
    *,
    period: float = 45.0,
    stale_after: float = 120.0,
    ping: Callable[[str], Awaitable[Any]] | None = None,
) -> None:
    """Ping ``url`` every ``period`` seconds while data is flowing.

    ``last_activity[0]`` is refreshed (to ``loop.time()``) on every received frame.
    If the event loop stalls, this task never wakes and the pings stop — so an
    external monitor (healthchecks.io) alerts on a frozen or dead process. A ping is
    skipped (not the task killed) when no frame has arrived within ``stale_after``,
    so a genuine multi-stream stall also surfaces as a missed ping.
    """
    loop = asyncio.get_running_loop()
    async with contextlib.AsyncExitStack() as stack:
        if ping is None:
            client = await stack.enter_async_context(httpx.AsyncClient(timeout=10.0))
            ping = client.get
        while True:
            await asyncio.sleep(period)
            if loop.time() - last_activity[0] <= stale_after:
                with contextlib.suppress(Exception):
                    await ping(url)


def _build_supervisors(
    config: Config,
    event: EventInfo,
    writer: CaptureWriter,
    gamma: GammaClient,
    connect: ConnectFactory | None,
    on_activity: Callable[[], None] | None = None,
) -> list[StreamSupervisor]:
    """Construct a supervisor per enabled stream (skipping book if no token ids)."""
    supervisors: list[StreamSupervisor] = []
    if config.comments:
        comments = CommentStream(
            event_id=event.event_id, writer=writer, connect=connect, on_activity=on_activity
        )
        supervisors.append(
            StreamSupervisor(
                comments,
                writer=writer,
                backfill=make_comment_backfill(comments, gamma, writer),
            )
        )
    if config.book:
        if event.clob_token_ids:
            book = BookStream(
                token_ids=event.clob_token_ids,
                writer=writer,
                connect=connect,
                on_activity=on_activity,
            )
            supervisors.append(StreamSupervisor(book, writer=writer))
        else:
            logger.warning("book stream requested but event has no CLOB token ids; skipping book")
    return supervisors


async def run(
    config: Config,
    *,
    gamma: GammaClient | None = None,
    connect: ConnectFactory | None = None,
) -> int:
    """Resolve the event and record all enabled streams until interrupted.

    Args:
        config: Validated run configuration.
        gamma: Optional pre-built Gamma client (for testing); otherwise one is
            created and owned by this call.
        connect: Optional websocket connect factory (for testing); otherwise the
            real connection is used.

    Returns:
        A process exit code.
    """
    hasher = Hasher() if config.hash_usernames else None
    own_gamma = gamma is None
    if gamma is None:
        gamma = GammaClient()

    writer: CaptureWriter | None = None
    supervisors: list[StreamSupervisor] = []
    tasks: list[asyncio.Task[None]] = []
    loop = asyncio.get_running_loop()
    installed: list[signal.Signals] = []
    fatal = False

    # Liveness for the external heartbeat: refreshed on every received frame so a
    # stalled event loop stops pinging (see _heartbeat).
    last_activity = [loop.time()]

    def _mark_activity() -> None:
        last_activity[0] = loop.time()

    try:
        event = await gamma.resolve_event(config.event_id, config.market_ids)
        writer = CaptureWriter(config, event_info=event, hasher=hasher)
        writer.open()

        supervisors = _build_supervisors(
            config, event, writer, gamma, connect, on_activity=_mark_activity
        )
        if not supervisors:
            logger.error("nothing to record (book requested but the event has no CLOB token ids)")
            return 1
        tasks = [asyncio.create_task(s.run(), name=f"polytape.{s.name}") for s in supervisors]

        hb_url = os.environ.get(HEARTBEAT_ENV)
        if hb_url:
            tasks.append(
                asyncio.create_task(_heartbeat(hb_url, last_activity), name="polytape.heartbeat")
            )
            logger.info("external heartbeat enabled")

        def _request_stop() -> None:
            for supervisor in supervisors:
                supervisor.stop()
            for task in tasks:
                task.cancel()

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _request_stop)
                installed.append(sig)
            except (NotImplementedError, AttributeError, ValueError):
                # Not supported on this platform (e.g. Windows); rely on the
                # KeyboardInterrupt path in run_live() instead.
                pass

        logger.info("recording %d stream(s); press Ctrl-C to stop", len(supervisors))
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        logger.info("shutdown requested")
    except FatalRecorderError as exc:
        logger.error("fatal, stopping: %s", exc)
        fatal = True
    finally:
        for sig in installed:
            with contextlib.suppress(Exception):
                loop.remove_signal_handler(sig)
        for supervisor in supervisors:
            supervisor.stop()
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if writer is not None:
            writer.close()
        if own_gamma:
            await gamma.aclose()
    return 2 if fatal else 0


def run_live(config: Config) -> int:
    """Synchronous entry point for a live capture (wraps :func:`run`)."""
    try:
        return asyncio.run(run(config))
    except KeyboardInterrupt:
        # POSIX without signal handlers, or Windows: output is already finalized
        # by run()'s finally block during interpreter shutdown of the loop.
        logger.info("interrupted; output finalized")
        return 130
    except GammaError as exc:
        logger.error("could not start capture: %s", exc)
        return 1
