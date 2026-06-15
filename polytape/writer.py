"""JSONL capture writer: per-stream append-only files, dedup, and ``meta.json``.

The :class:`CaptureWriter` is the single sink for the whole pipeline. Streams,
backfill, and the dry-run mock all hand it raw messages; it envelopes them,
de-duplicates by id, appends one flushed JSON line per stream file, and keeps
``meta.json`` current (start/stop times, counts, and a gap audit log).

The writer is synchronous (fast, append + flush per line) and intended to be
called from the async stream tasks.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any, TextIO

from polytape import __version__
from polytape.config import Config
from polytape.envelope import Hasher, build_envelope, iso_to_datetime, utc_now_iso

if TYPE_CHECKING:
    from polytape.gamma import EventInfo

logger = logging.getLogger("polytape.writer")


def _downtime_seconds(start_iso: str, end_iso: str) -> float | None:
    """Seconds between two ISO timestamps, or ``None`` if either is unparseable."""
    start, end = iso_to_datetime(start_iso), iso_to_datetime(end_iso)
    if start is None or end is None:
        return None
    return round((end - start).total_seconds(), 3)


class CaptureWriter:
    """Writes enveloped messages to per-stream JSONL files and maintains meta.json.

    Use as a context manager::

        with CaptureWriter(config, event_info=ev, hasher=hasher) as w:
            w.write("comments", raw_msg)
    """

    def __init__(
        self,
        config: Config,
        *,
        event_info: EventInfo | None = None,
        hasher: Hasher | None = None,
        now: Any = utc_now_iso,
    ) -> None:
        self._config = config
        self._event_info = event_info
        self._hasher = hasher
        self._now = now
        self._dir: Path = config.event_dir
        self._files: dict[str, TextIO] = {}
        self._seen: dict[str, set[str]] = {}
        self._counts: dict[str, int] = {}
        self._gaps: list[dict[str, Any]] = []
        self._started_at: str | None = None
        self._stopped_at: str | None = None
        self._open = False

    # -- lifecycle ---------------------------------------------------------- #

    def __enter__(self) -> CaptureWriter:
        self.open()
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def open(self) -> None:
        """Create the event directory, open per-stream files, write initial meta."""
        if self._open:
            return
        self._dir.mkdir(parents=True, exist_ok=True)
        self._started_at = self._now()
        for stream in self._config.enabled_streams:
            path = self._dir / f"{stream}.jsonl"
            # Append-only so an existing capture is never clobbered; dedup is
            # per-run (in-memory), per the spec.
            self._files[stream] = open(path, "a", encoding="utf-8", newline="\n")
            self._seen[stream] = set()
            self._counts.setdefault(stream, 0)
        self._open = True
        self._write_meta()
        logger.info(
            "writing to %s (streams: %s)", self._dir, ", ".join(self._config.enabled_streams)
        )

    def close(self) -> None:
        """Flush and close all files; finalize ``meta.json`` with stop time/counts."""
        if not self._open:
            return
        self._stopped_at = self._now()
        for stream, handle in self._files.items():
            try:
                handle.flush()
                handle.close()
            except OSError:
                logger.exception("error closing %s file", stream)
        self._open = False
        self._write_meta()
        logger.info("capture stopped; counts: %s", dict(self._counts))

    # -- writing ------------------------------------------------------------ #

    def write(self, stream: str, raw: dict[str, Any]) -> bool:
        """Envelope and write a raw message. Returns ``False`` if it was a duplicate."""
        envelope = build_envelope(stream, raw, hasher=self._hasher, ts_recv=self._now())
        return self.write_envelope(envelope)

    def write_envelope(self, envelope: dict[str, Any]) -> bool:
        """Write a pre-built envelope, de-duplicating by id within the stream."""
        if not self._open:
            raise RuntimeError("writer is not open")
        stream = envelope["stream"]
        handle = self._files.get(stream)
        if handle is None:
            raise ValueError(f"stream {stream!r} is not open for writing")
        message_id = envelope["id"]
        seen = self._seen[stream]
        if message_id in seen:
            return False
        seen.add(message_id)
        handle.write(json.dumps(envelope, ensure_ascii=False) + "\n")
        handle.flush()
        self._counts[stream] += 1
        return True

    def record_gap(
        self,
        stream: str,
        disconnected_at: str,
        reconnected_at: str,
        *,
        backfilled: int = 0,
        note: str = "",
    ) -> dict[str, Any]:
        """Append a disconnect/recovery entry to the gap log and persist meta."""
        gap = {
            "stream": stream,
            "disconnected_at": disconnected_at,
            "reconnected_at": reconnected_at,
            "downtime_seconds": _downtime_seconds(disconnected_at, reconnected_at),
            "backfilled": backfilled,
            "note": note,
        }
        self._gaps.append(gap)
        self._write_meta()
        logger.info(
            "recorded gap on %s: down %ss, backfilled %d",
            stream,
            gap["downtime_seconds"],
            backfilled,
        )
        return gap

    # -- introspection ------------------------------------------------------ #

    @property
    def counts(self) -> dict[str, int]:
        """A snapshot of per-stream written-message counts."""
        return dict(self._counts)

    def seen_count(self, stream: str) -> int:
        """Number of distinct message ids seen on a stream (for tests/diagnostics)."""
        return len(self._seen.get(stream, ()))

    # -- meta.json ---------------------------------------------------------- #

    def _event_snapshot(self) -> dict[str, Any] | None:
        event = self._event_info
        if event is None:
            return None
        return {
            "id": event.event_id,
            "title": event.title,
            "slug": event.slug,
            "markets": [
                {"id": m.id, "conditionId": m.condition_id, "clobTokenIds": list(m.token_ids)}
                for m in event.markets
            ],
        }

    def _meta(self) -> dict[str, Any]:
        event = self._event_info
        return {
            "polytape_version": __version__,
            "event_id": self._config.event_id,
            "market_ids": list(event.condition_ids) if event else [],
            "clob_token_ids": list(event.clob_token_ids) if event else [],
            "streams": list(self._config.enabled_streams),
            "out_dir": self._dir.as_posix(),
            "hashing": {
                "enabled": self._hasher is not None,
                "salt_fingerprint": self._hasher.fingerprint if self._hasher else None,
            },
            "started_at": self._started_at,
            "stopped_at": self._stopped_at,
            "counts": dict(self._counts),
            "event": self._event_snapshot(),
            "gaps": list(self._gaps),
        }

    def _write_meta(self) -> None:
        """Atomically (temp file + replace) write ``meta.json``."""
        self._dir.mkdir(parents=True, exist_ok=True)
        path = self._dir / "meta.json"
        tmp = self._dir / "meta.json.tmp"
        with open(tmp, "w", encoding="utf-8", newline="\n") as handle:
            json.dump(self._meta(), handle, ensure_ascii=False, indent=2)
            handle.write("\n")
        os.replace(tmp, path)
