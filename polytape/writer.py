"""JSONL capture writer: per-stream append-only files, dedup, and ``meta.json``.

The :class:`CaptureWriter` is the single sink for the whole pipeline. Streams
and the dry-run mock hand it raw messages; it envelopes them, de-duplicates by
id, appends one flushed JSON line per stream file, and keeps ``meta.json``
current (start/stop times, counts, and a gap audit log).

The writer is synchronous (fast, append + flush per line) and intended to be
called from the async stream tasks.

On-disk layout
--------------

A capture directory (``event-<id>/`` for a single-event capture, ``run-<name>/``
for a multi-event run) holds::

    <dir>/
    ├── meta.json                       rewritten atomically (start/stop, counts, gaps, segments)
    ├── book.jsonl                      LEGACY monolith: single-event captures, and runs recorded
    │                                   before daily rotation. A rotating run never appends to it.
    ├── book.<YYYY-MM-DD>.jsonl         DAILY SEGMENTS of the monolith (multi-event runs): the
    │                                   complete append-only backup, one file per UTC day of
    │                                   ``ts_recv``; rolled over on the first write of a new day.
    ├── segments/
    │   └── book.<YYYY-MM-DD>.offloaded.json   a segment moved to GCS (admin/offload.py)
    └── matches/
        ├── event-<id>/{book.jsonl,meta.json}  per-match PRIMARY natives (never rotated)
        └── event-<id>.offloaded.json          a finished match moved to GCS (admin/offload.py)

**Daily rotation** applies to multi-event runs only (``Config.is_multi`` — the
open-ended campaign recorder), so single-event captures keep the documented
``event-<id>/book.jsonl`` that the monitor and viewer tail. The roll-over is a
plain string compare of the envelope's ``ts_recv`` day (``YYYY-MM-DD``) against the
open segment's day — no extra clock reads on the hot path, and the roll only ever
goes FORWARD (a ``ts_recv`` earlier than the open segment's day — a clock step back
across midnight — stays in the open segment rather than re-creating, and possibly
resurrecting an already offloaded, earlier segment). Segments are opened in append
mode, so the 10-minute refresh restart simply continues the day's file; before any
append-open (segment or per-match file) a torn trailing line left by a crash
mid-write is terminated with a newline, so the next record is never glued onto it.
``meta.json#segments`` names the open segment and every segment this process has
opened; the cumulative ``counts`` are unaffected by rotation.
"""

from __future__ import annotations

import json
import logging
import os
import re
from collections import OrderedDict
from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any, TextIO

from polytape import __version__
from polytape.config import Config
from polytape.envelope import build_envelope, iso_to_datetime, utc_now_iso

if TYPE_CHECKING:
    from polytape.gamma import EventInfo

logger = logging.getLogger("polytape.writer")

# Cap the per-stream in-memory dedup set so a long high-volume capture cannot grow
# it without bound (a 12h busy book feed could otherwise reach multiple GB and OOM
# a small VM). Real duplicates are recency-bounded — the CLOB resends the book
# snapshot only on reconnect — so an oldest-first eviction at this size never
# drops an id that could still recur.
_SEEN_CAP = 500_000

# A daily monolith segment: ``<stream>.<YYYY-MM-DD>.jsonl``. The single source of truth
# for the naming scheme — the admin's scan/offload paths parse names with this too.
_SEGMENT_RE = re.compile(r"\A(?P<stream>[a-z][a-z0-9_]*)\.(?P<day>\d{4}-\d{2}-\d{2})\.jsonl\Z")


def segment_name(stream: str, day: str) -> str:
    """File name of the daily monolith segment of ``stream`` for UTC ``day`` (``YYYY-MM-DD``)."""
    return f"{stream}.{day}.jsonl"


def parse_segment_name(name: str) -> tuple[str, str] | None:
    """``(stream, day)`` for a daily-segment file name, or ``None`` for anything else
    (the legacy ``<stream>.jsonl``, ``comments.jsonl``, markers, ...)."""
    m = _SEGMENT_RE.match(name)
    return (m.group("stream"), m.group("day")) if m else None


def _day_of(ts: Any) -> str | None:
    """UTC calendar day (``YYYY-MM-DD``) of a canonical ISO ``ts_recv``, or ``None``.

    A pure slice + two character checks: cheap enough for every record on the hot
    path, and lenient about anything that is not a well-formed timestamp (a hand-built
    envelope) — which then simply stays in the currently open segment.
    """
    if isinstance(ts, str) and len(ts) >= 10 and ts[4] == "-" and ts[7] == "-":
        return ts[:10]
    return None


def _repair_tail(path: Path) -> bool:
    """Terminate a torn trailing line in ``path`` before it is opened for append.

    A crash mid-write (ENOSPC, SIGKILL, a hard reset) can leave a partial last line
    on disk; appending the next record straight after it would glue the two into one
    unparseable line, costing every reader TWO records. A lone ``\\n`` turns the torn
    fragment into one unparseable line that readers already skip. Returns True if a
    repair was made. A missing file is fine (nothing to repair); other ``OSError``s
    propagate to the caller exactly like the open that follows would.
    """
    try:
        with open(path, "rb+") as fh:
            fh.seek(0, os.SEEK_END)
            if fh.tell() == 0:
                return False
            fh.seek(-1, os.SEEK_END)
            if fh.read(1) == b"\n":
                return False
            fh.write(b"\n")
    except FileNotFoundError:
        return False
    logger.warning(
        "%s ended in a torn line (crash mid-write?); terminated it before appending", path
    )
    return True


class FatalRecorderError(Exception):
    """An unrecoverable I/O error (e.g. disk full) — stop the process, do not reconnect.

    Distinct from a transient connection error: the supervisor re-raises this past
    its reconnect loop so the process exits non-zero, surfacing via systemd's
    restart and the dead external heartbeat instead of looping silently while data
    is dropped.
    """


def _downtime_seconds(start_iso: str, end_iso: str) -> float | None:
    """Seconds between two ISO timestamps, or ``None`` if either is unparseable."""
    start, end = iso_to_datetime(start_iso), iso_to_datetime(end_iso)
    if start is None or end is None:
        return None
    return round((end - start).total_seconds(), 3)


class CaptureWriter:
    """Writes enveloped messages to per-stream JSONL files and maintains meta.json.

    Use as a context manager::

        with CaptureWriter(config, event_info=ev) as w:
            w.write("book", raw_msg)
    """

    def __init__(
        self,
        config: Config,
        *,
        event_info: EventInfo | None = None,
        event_infos: Sequence[EventInfo] | None = None,
        now: Any = utc_now_iso,
    ) -> None:
        self._config = config
        if event_infos is not None:
            self._event_infos = tuple(event_infos)
        elif event_info is not None:
            self._event_infos = (event_info,)
        else:
            self._event_infos = ()
        # Primary event kept for back-compat single-event meta fields.
        self._event_info = self._event_infos[0] if self._event_infos else None
        self._now = now
        self._dir: Path = config.event_dir
        self._files: dict[str, TextIO] = {}
        # Daily rotation of the monolith (see the module docstring): multi-event runs are
        # open-ended, so their backup log is split into one segment per UTC day. A
        # single-event capture keeps the legacy ``<stream>.jsonl``.
        self._rotate: bool = bool(getattr(config, "is_multi", False))
        self._segment_day: dict[str, str] = {}  # stream -> UTC day of its OPEN segment
        self._segments_seen: list[str] = []  # segment file names opened by THIS process
        # Per-match (PRIMARY) output: one append handle per (stream, event_id), opened
        # lazily on the first record for that event. The monolithic self._files stays the
        # complete append-only backup; these are the ready-to-use per-match files that let
        # a finished match be consumed/offloaded without scanning the whole run. See
        # write_envelope for the lossless dual-write.
        self._per_match: bool = bool(getattr(config, "per_match", True))
        self._event_files: dict[tuple[str, str], TextIO] = {}
        # Events whose per-match meta.json is behind their counts: the periodic flush
        # rewrites only these (hundreds of open events x every 5 s would otherwise be a
        # steady stream of small synchronous writes on the hot loop).
        self._event_meta_dirty: set[str] = set()
        self._event_snapshots: dict[str, dict[str, Any]] = {
            e.event_id: self._snapshot(e) for e in self._event_infos
        }
        self._seen: dict[str, OrderedDict[str, None]] = {}
        self._counts: dict[str, int] = {}
        # Per-event, per-stream written counts (multi-event runs). Records still go
        # to the shared per-stream files; this is just accounting for meta.json.
        self._counts_by_event: dict[str, dict[str, int]] = {}
        # Freshness for the admin: last record ts per event + overall. Lets the dashboard
        # show live/quiet and recorder liveness straight from meta.json — no log scan.
        self._last_ts_by_event: dict[str, str] = {}
        self._last_record_at: str | None = None
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
        # Continue the CUMULATIVE tally across restarts: the recorder appends to the same
        # JSONL files, so counts must pick up where the prior process left off (the refresh
        # roll-over restarts this process routinely). Without this, meta.json would reset to
        # 0 on every restart and undercount the append-only log.
        self._seed_counts_from_meta()
        # The first segment is the day the process started (the stamp is already taken
        # for started_at — no extra clock read); the first record of a later day rolls.
        day = _day_of(self._started_at)
        for stream in self._config.enabled_streams:
            # Append-only so an existing capture is never clobbered; dedup is
            # per-run (in-memory), per the spec.
            self._files[stream] = self._open_stream_file(stream, day)
            self._seen[stream] = OrderedDict()
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
        for (stream, eid), handle in self._event_files.items():
            try:
                handle.flush()
                handle.close()
            except OSError:
                logger.exception("error closing per-match %s file for event %s", stream, eid)
        self._open = False
        # Best-effort on shutdown: if the disk is full we cannot finalize meta.json,
        # but that must not mask the original cause or crash the cleanup path.
        try:
            self._write_meta(all_events=True)  # every open match gets its stopped_at
        except FatalRecorderError:
            logger.exception("could not finalize meta.json on close")
        logger.info("capture stopped; counts: %s", dict(self._counts))

    # -- monolith segments -------------------------------------------------- #

    def _stream_path(self, stream: str, day: str | None) -> Path:
        """Where ``stream``'s monolith lines go: the daily segment on a rotating run
        (``<stream>.<day>.jsonl``), else the legacy ``<stream>.jsonl``."""
        if self._rotate and day is not None:
            return self._dir / segment_name(stream, day)
        return self._dir / f"{stream}.jsonl"

    def _open_stream_file(self, stream: str, day: str | None) -> TextIO:
        """Open (append) the monolith file for ``stream`` on ``day`` and track the segment.

        Raises ``OSError`` to the caller (``open()`` lets it propagate at startup; the
        roll-over path turns it into a :class:`FatalRecorderError`).
        """
        path = self._stream_path(stream, day)
        _repair_tail(path)  # a torn last line from a crash must not swallow the next record
        handle = open(path, "a", encoding="utf-8", newline="\n")
        if self._rotate and day is not None:
            self._segment_day[stream] = day
            if path.name not in self._segments_seen:
                self._segments_seen.append(path.name)
        return handle

    def _roll_segment(self, stream: str, day: str) -> TextIO:
        """Close ``stream``'s open segment and open the one for ``day`` (a day boundary
        crossed in ``ts_recv``). Returns the new handle.

        Synchronous (open + close, no ``await``), so a record can never straddle two
        segments. The new segment is opened BEFORE the old one is closed, so a failed
        open leaves the writer on its still-valid handle for an orderly shutdown. Any
        ``OSError`` is fatal: the record could not be written to the backup, and the
        process must restart rather than continue on a dead handle.
        """
        try:
            handle = self._open_stream_file(stream, day)
        except OSError as exc:
            raise FatalRecorderError(f"segment roll-over for {stream!r} failed: {exc}") from exc
        old = self._files[stream]
        self._files[stream] = handle
        try:
            old.flush()
            old.close()
        except OSError as exc:
            raise FatalRecorderError(f"closing {stream!r} segment failed: {exc}") from exc
        logger.info("rolled %s monolith to segment %s", stream, segment_name(stream, day))
        # Once a day: make meta.json#segments current right away rather than at the next
        # periodic flush. Best-effort (flush_meta never escalates).
        self.flush_meta()
        return handle

    # -- writing ------------------------------------------------------------ #

    def write(self, stream: str, raw: dict[str, Any], *, event_id: str | None = None) -> bool:
        """Envelope and write a raw message. Returns ``False`` if it was a duplicate.

        ``event_id`` (keyword-only, optional) attributes the message to an event for
        per-event counts in ``meta.json``. It is **not** stored in the envelope — the
        record stays the documented 5-key shape; the event is recoverable from
        ``raw`` (``market`` for book).
        """
        envelope = build_envelope(stream, raw, ts_recv=self._now())
        wrote = self.write_envelope(envelope, event_id=event_id)
        if wrote:
            ts = envelope["ts_recv"]
            self._last_record_at = ts
            if event_id is not None:
                eid = str(event_id)
                per_event = self._counts_by_event.setdefault(eid, {})
                per_event[stream] = per_event.get(stream, 0) + 1
                self._last_ts_by_event[eid] = ts
        return wrote

    def write_envelope(self, envelope: dict[str, Any], *, event_id: str | None = None) -> bool:
        """Write a pre-built envelope, de-duplicating by id within the stream.

        Writes the SAME line to two places, both synchronously and flushed before
        returning. The recorder's hot loop has no ``await`` between receiving a frame
        and this returning (see ``streams/base.py``), so the pair is atomic w.r.t. every
        other write — a record can never be split across, or lost at, a per-match
        boundary:

        1. the monolithic ``<stream>.jsonl`` — the complete append-only BACKUP (on a
           rotating run, its daily segment ``<stream>.<YYYY-MM-DD>.jsonl``, chosen by
           the envelope's ``ts_recv`` day), and
        2. (when ``event_id`` is set and ``per_match`` is on) the per-match PRIMARY file
           ``matches/event-<id>/<stream>.jsonl``.

        The monolith is written FIRST and is the source of truth: if the per-match write
        fails (e.g. ENOSPC), the record is still safely in the backup — and per-match
        files are rebuildable from it — while the failure still surfaces as a fatal disk
        error so the process restarts instead of silently dropping data.
        """
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
        seen[message_id] = None
        if len(seen) > _SEEN_CAP:
            seen.popitem(last=False)  # evict oldest; the dedup window stays recency-bounded
        if self._rotate:
            # Day boundary in ts_recv -> roll the monolith to that day's segment. A cheap
            # string compare per record; a malformed ts stays in the open segment. Only
            # a LATER day rolls: an earlier ts_recv (clock stepped back across midnight)
            # stays in the open segment instead of re-creating yesterday's file, which
            # may already be offloaded.
            day = _day_of(envelope.get("ts_recv"))
            if day is not None and day > (self._segment_day.get(stream) or ""):
                handle = self._roll_segment(stream, day)
        line = json.dumps(envelope, ensure_ascii=False) + "\n"
        try:
            handle.write(line)  # monolithic backup (source of truth) — written first
            handle.flush()
            if event_id is not None and self._per_match:
                per = self._event_handle(stream, str(event_id))  # per-match PRIMARY
                per.write(line)
                per.flush()
                self._event_meta_dirty.add(str(event_id))
        except OSError as exc:
            raise FatalRecorderError(f"write to {stream!r} failed: {exc}") from exc
        self._counts[stream] += 1
        return True

    def _event_handle(self, stream: str, event_id: str) -> TextIO:
        """Lazily open + cache the per-match append handle for one ``(stream, event)``.

        Synchronous (mkdir + open, no ``await``) so the dual write stays atomic with the
        rest of the hot loop. Any failure raises ``OSError`` to the caller, which turns
        it into a :class:`FatalRecorderError` exactly like the monolithic path.
        """
        key = (stream, event_id)
        handle = self._event_files.get(key)
        if handle is None:
            event_dir = self._dir / "matches" / f"event-{event_id}"
            event_dir.mkdir(parents=True, exist_ok=True)
            path = event_dir / f"{stream}.jsonl"
            _repair_tail(path)  # same torn-line guard as the monolith segment
            handle = open(path, "a", encoding="utf-8", newline="\n")
            self._event_files[key] = handle
        return handle

    def record_gap(
        self,
        stream: str,
        disconnected_at: str,
        reconnected_at: str,
        *,
        note: str = "",
    ) -> dict[str, Any]:
        """Append a disconnect/recovery entry to the gap log and persist meta."""
        gap = {
            "stream": stream,
            "disconnected_at": disconnected_at,
            "reconnected_at": reconnected_at,
            "downtime_seconds": _downtime_seconds(disconnected_at, reconnected_at),
            "note": note,
        }
        self._gaps.append(gap)
        self._write_meta()
        logger.info("recorded gap on %s: down %ss", stream, gap["downtime_seconds"])
        return gap

    # -- introspection ------------------------------------------------------ #

    @property
    def counts(self) -> dict[str, int]:
        """A snapshot of per-stream written-message counts."""
        return dict(self._counts)

    def seen_count(self, stream: str) -> int:
        """Number of distinct message ids seen on a stream (for tests/diagnostics)."""
        return len(self._seen.get(stream, ()))

    @property
    def segments(self) -> dict[str, dict[str, Any]]:
        """Per-stream monolith segment state (empty for a non-rotating capture)::

            {"book": {"current": "book.2026-09-03.jsonl",
                      "seen": ["book.2026-09-02.jsonl", "book.2026-09-03.jsonl"]}}

        ``current`` is the segment open for appends; ``seen`` lists every segment THIS
        process has opened, in order (earlier segments belong to earlier processes and
        are found on disk / in the offload markers, not here).
        """
        return {
            stream: {
                "current": segment_name(stream, day),
                "seen": [n for n in self._segments_seen if n.startswith(f"{stream}.")],
            }
            for stream, day in self._segment_day.items()
        }

    # -- meta.json ---------------------------------------------------------- #

    @staticmethod
    def _snapshot(event: EventInfo) -> dict[str, Any]:
        return {
            "id": event.event_id,
            "title": event.title,
            "slug": event.slug,
            "markets": [
                {"id": m.id, "conditionId": m.condition_id, "clobTokenIds": list(m.token_ids)}
                for m in event.markets
            ],
        }

    def _event_snapshot(self) -> dict[str, Any] | None:
        return self._snapshot(self._event_info) if self._event_info else None

    def flush_meta(self) -> bool:
        """Persist ``meta.json`` best-effort, for the periodic flusher.

        Unlike the gap/close paths, a transient flush failure here must NOT escalate to
        :class:`FatalRecorderError` and kill a healthy recording — the stream-write path
        is the real disk-full detector. Called on the event loop (serialized with
        :meth:`write`), so the count snapshot is consistent. Returns True on success.
        """
        if not self._open:
            return False
        try:
            self._write_meta()
            return True
        except FatalRecorderError:
            logger.warning("periodic meta.json flush failed (continuing)", exc_info=True)
            return False

    def _seed_counts_from_meta(self) -> None:
        """Seed cumulative counts + freshness from an existing ``meta.json`` so a restart
        CONTINUES the tally over the append-only files instead of restarting from zero.

        Best-effort: a missing/corrupt meta leaves everything at zero (a fresh run). Only
        well-typed, non-negative values are adopted, so a hand-mangled meta can't poison
        the counters.
        """
        try:
            meta = json.loads((self._dir / "meta.json").read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, UnicodeDecodeError):
            return
        if not isinstance(meta, dict):
            return
        counts = meta.get("counts")
        if isinstance(counts, dict):
            for stream, n in counts.items():
                if isinstance(n, int) and n >= 0:
                    self._counts[str(stream)] = n
        by_event = meta.get("counts_by_event")
        if isinstance(by_event, dict):
            for eid, per in by_event.items():
                if isinstance(per, dict):
                    self._counts_by_event[str(eid)] = {
                        str(s): n for s, n in per.items() if isinstance(n, int) and n >= 0
                    }
        last_ts = meta.get("last_ts_by_event")
        if isinstance(last_ts, dict):
            self._last_ts_by_event = {str(e): t for e, t in last_ts.items() if isinstance(t, str)}
        last_at = meta.get("last_record_at")
        if isinstance(last_at, str):
            self._last_record_at = last_at

    def _meta(self) -> dict[str, Any]:
        return {
            "polytape_version": __version__,
            "event_id": self._config.event_id,
            "event_ids": list(self._config.event_ids),
            "run_name": self._config.run_name,
            "market_ids": [c for e in self._event_infos for c in e.condition_ids],
            "clob_token_ids": [t for e in self._event_infos for t in e.clob_token_ids],
            "streams": list(self._config.enabled_streams),
            "out_dir": self._dir.as_posix(),
            "started_at": self._started_at,
            "stopped_at": self._stopped_at,
            "counts": dict(self._counts),
            "counts_by_event": {k: dict(v) for k, v in self._counts_by_event.items()},
            # Freshness, for the admin to show live/quiet + recorder liveness from meta alone.
            "last_ts_by_event": dict(self._last_ts_by_event),
            "last_record_at": self._last_record_at,
            # Daily monolith segments (rotating runs): the open one + those this process
            # opened. Empty for a single-event capture (legacy <stream>.jsonl).
            "segments": self.segments,
            "event": self._event_snapshot(),
            "events": [self._snapshot(e) for e in self._event_infos],
            "gaps": list(self._gaps),
        }

    def _write_meta(self, *, all_events: bool = False) -> None:
        """Atomically (temp file + replace) write ``meta.json``, then the per-match metas
        of events with new records since their last write (every open event's when
        ``all_events`` — the close path, which stamps ``stopped_at``).

        Raises :class:`FatalRecorderError` on an I/O error (e.g. disk full) so a
        failure while recording a gap stops the process rather than being swallowed
        into a silent reconnect loop. :meth:`close` guards its own call so shutdown
        stays best-effort.
        """
        try:
            self._dir.mkdir(parents=True, exist_ok=True)
            path = self._dir / "meta.json"
            tmp = self._dir / "meta.json.tmp"
            with open(tmp, "w", encoding="utf-8", newline="\n") as handle:
                json.dump(self._meta(), handle, ensure_ascii=False, indent=2)
                handle.write("\n")
            os.replace(tmp, path)
        except OSError as exc:
            raise FatalRecorderError(f"writing meta.json failed: {exc}") from exc
        self._write_event_metas(all_open=all_events)

    def _event_meta(self, event_id: str) -> dict[str, Any]:
        """A self-contained per-match meta dict (event snapshot + that event's counts)."""
        return {
            "polytape_version": __version__,
            "event_id": event_id,
            "run_name": self._config.run_name,
            "streams": list(self._config.enabled_streams),
            "started_at": self._started_at,
            "stopped_at": self._stopped_at,
            "counts": dict(self._counts_by_event.get(event_id, {})),
            "last_record_at": self._last_ts_by_event.get(event_id),
            "event": self._event_snapshots.get(event_id),
        }

    def _write_event_metas(self, *, all_open: bool = False) -> None:
        """Persist a small ``meta.json`` next to each per-match file's directory.

        Makes ``matches/event-<id>/`` a ready-to-use archive (data + meta), mirroring the
        per-match download layout. Best-effort: a per-match meta failure is logged but
        NEVER escalates to fatal — the ``book.jsonl`` data is what matters and the meta is
        derivable from it (the monolith stays the source of truth). Only events that have
        an open per-match handle are written (so finished, rolled-out matches keep their
        last-written meta), and — unless ``all_open`` — only those with new records since
        their meta was last written: with hundreds of open events, rewriting every one
        on each 5-second flush would be a steady synchronous I/O load on the hot loop
        for no new information. A failed write stays dirty and is retried next flush.
        """
        if not self._per_match:
            return
        open_ids = {eid for (_stream, eid) in self._event_files}
        targets = open_ids if all_open else (self._event_meta_dirty & open_ids)
        for event_id in targets:
            event_dir = self._dir / "matches" / f"event-{event_id}"
            try:
                tmp = event_dir / "meta.json.tmp"
                with open(tmp, "w", encoding="utf-8", newline="\n") as handle:
                    json.dump(self._event_meta(event_id), handle, ensure_ascii=False, indent=2)
                    handle.write("\n")
                os.replace(tmp, event_dir / "meta.json")
            except OSError:
                logger.warning(
                    "could not write per-match meta for event %s", event_id, exc_info=True
                )
            else:
                self._event_meta_dirty.discard(event_id)
