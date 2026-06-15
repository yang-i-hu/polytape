"""Shared base for polytape's websocket stream consumers.

A :class:`WebSocketStream` models a *single* connection session: connect,
subscribe, keep the socket alive with an application-level text ping, and pipe
every received message into the writer. Reconnection and backfill are layered on
top by the supervisor (see ``polytape/supervisor.py``); a stream object here only
knows how to run one session via :meth:`run_once`.

The ``connect`` factory is injectable so the consume loop can be exercised
offline with a fake connection (no network).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

import websockets

if TYPE_CHECKING:
    from polytape.writer import CaptureWriter

logger = logging.getLogger("polytape.stream")

# Largest websocket message we accept. Book snapshots can be sizeable; a recorder
# should not silently drop a large frame.
_MAX_MESSAGE_SIZE = 16 * 1024 * 1024

ConnectFactory = Callable[[str], Any]  # url -> async context manager yielding a ws


def default_connect(url: str) -> Any:
    """Open a websocket with library keepalive disabled (we send our own ping)."""
    return websockets.connect(
        url,
        ping_interval=None,
        max_size=_MAX_MESSAGE_SIZE,
        open_timeout=20,
        close_timeout=5,
    )


class WebSocketStream:
    """Base class for a single-session websocket consumer.

    Subclasses set :attr:`stream` and implement :meth:`subscribe_frames`.
    """

    #: Stream name (also the output file name and envelope ``stream`` value).
    stream: str = ""

    def __init__(
        self,
        *,
        url: str,
        writer: CaptureWriter,
        ping_text: str,
        ping_interval: float = 5.0,
        connect: ConnectFactory = default_connect,
    ) -> None:
        self.url = url
        self.writer = writer
        self.ping_text = ping_text
        self.ping_interval = ping_interval
        self._connect = connect

    # -- to override ------------------------------------------------------- #

    def subscribe_frames(self) -> list[str]:
        """Return the text frames to send immediately after connecting."""
        raise NotImplementedError

    def should_record(self, raw: dict[str, Any]) -> bool:
        """Whether a decoded message belongs to this capture.

        Default accepts everything; the comment stream overrides this to filter
        the firehose down to its event (server-side filtering is unavailable).
        """
        return True

    def on_written(self, raw: dict[str, Any]) -> None:
        """Hook invoked after a *new* (non-duplicate) message is written."""

    # -- message decoding -------------------------------------------------- #

    def decode(self, message: str | bytes) -> list[dict[str, Any]]:
        """Decode a raw frame into zero or more message dicts.

        Returns an empty list for non-JSON frames (e.g. ``pong``/``PONG``
        keepalive replies) and normalizes a JSON array of messages into a list.
        """
        if isinstance(message, (bytes, bytearray)):
            try:
                message = message.decode("utf-8")
            except UnicodeDecodeError:
                return []
        try:
            data = json.loads(message)
        except (json.JSONDecodeError, TypeError):
            return []
        if isinstance(data, list):
            return [m for m in data if isinstance(m, dict)]
        if isinstance(data, dict):
            return [data]
        return []

    # -- keepalive --------------------------------------------------------- #

    async def _keepalive(self, ws: Any) -> None:
        """Send the application-level ping every ``ping_interval`` seconds."""
        try:
            while True:
                await asyncio.sleep(self.ping_interval)
                await ws.send(self.ping_text)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.debug("%s: keepalive stopped (connection closing)", self.stream)

    # -- single session ---------------------------------------------------- #

    async def run_once(self, *, on_connect: Callable[[], Awaitable[None]] | None = None) -> None:
        """Run one connection session until the socket closes.

        Args:
            on_connect: Optional async callback run right after subscribing (the
                keepalive is already active). The supervisor uses it to reset
                backoff and to backfill missed comments on reconnect.

        Raises:
            Propagates connection errors (e.g. ``websockets.ConnectionClosed``)
            so the supervisor can reconnect. A clean close returns normally.
        """
        frames = self.subscribe_frames()
        async with self._connect(self.url) as ws:
            for frame in frames:
                await ws.send(frame)
            logger.info("%s: connected and subscribed (%d frame(s))", self.stream, len(frames))
            keepalive = asyncio.create_task(self._keepalive(ws))
            try:
                if on_connect is not None:
                    await on_connect()
                async for message in ws:
                    for raw in self.decode(message):
                        if not self.should_record(raw):
                            continue
                        if self.writer.write(self.stream, raw):
                            self.on_written(raw)
            finally:
                keepalive.cancel()
                with contextlib.suppress(BaseException):
                    await keepalive
        logger.info("%s: connection closed", self.stream)
