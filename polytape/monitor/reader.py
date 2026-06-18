"""Read-only tailing reader: turn a capture's files into live dashboard stats.

This module never touches the recorder. It is a passive observer that reads the
exact artifacts a capture already writes — the append-only ``*.jsonl`` stream
files and the atomically-rewritten ``meta.json`` — and derives live health
stats: per-stream counts, throughput, message-type mix, the server→recv delay
distribution, staleness, and the disconnect/gap log. Running in a separate
process, it adds no work to the recorder's hot path.

Design notes
------------
- **Incremental tail.** Each stream file is scanned once on attach — newlines
  are counted in binary chunks (cheap, exact total without parsing every line)
  and the offset is parked on the last newline boundary. Subsequent polls parse
  only the bytes appended since, holding back a partial trailing line until its
  newline arrives, so no line is parsed twice or half-parsed.
- **Exact totals, live windows.** ``count`` is the exact number of records on
  disk from the first poll — a cheap newline count, since the writer flushes
  exactly one valid envelope per line. The type mix, delay percentiles, rate,
  and the ``malformed`` counter describe data seen *since the monitor attached*
  — the natural framing for a live monitor, and what keeps attach cheap on a
  large existing file.
- **meta.json is re-read only when it changes** (by mtime+size). This saves work
  and shrinks the window in which the reader holds the file open, so it stays
  out of the way of the writer's atomic ``os.replace`` finalization.
- **No payload content leaves this process.** Only aggregate counters and
  non-identifying metadata (stream, message type, timestamps, delay) are
  surfaced — never comment bodies or (under ``--no-hash``) raw usernames.

The reader is not thread-safe; the server serializes calls with a lock.
"""

from __future__ import annotations

import json
import time
from collections import Counter, deque
from collections.abc import Callable
from pathlib import Path
from typing import Any

from polytape.envelope import iso_to_datetime, utc_now_iso

# Stream files end every flushed record with ``\n``; we tail on that boundary.
_NEWLINE = b"\n"
_CHUNK = 1 << 20  # 1 MiB scan chunk for the cheap on-attach newline count.
_SEED_TAIL = 1 << 16  # bytes read from the end on attach to locate the last line.

#: Live-window sizes. The delay sample and per-poll throughput history are
#: bounded so memory stays flat no matter how long the monitor runs.
_DELAY_WINDOW = 512
_SPARK_WINDOW = 90
_RECENT_WINDOW = 40

#: Samples above this (ms) are excluded from the live receive-delay percentiles.
#: For comments ``ts_server`` is the content's ``createdAt``, so REST backfill of
#: historical comments after a disconnect yields delays of minutes-to-hours that
#: are *content age*, not live receive latency — letting them into the window
#: would pin p95/max on stale data. (Genuine recorder lag surfaces instead via
#: staleness / the "idle" status.) The per-message ticker still shows raw values.
_MAX_LIVE_DELAY_MS = 5 * 60 * 1000

#: A stream with no new line for this long (and not stopped) is reported "idle"
#: rather than "live" — low comment volume is normal, so this is informational.
DEFAULT_IDLE_THRESHOLD = 20.0


def _percentile(values: list[float], pct: float) -> float | None:
    """Nearest-rank percentile of a list (``pct`` in 0..100); ``None`` if empty."""
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return round(ordered[0], 1)
    rank = max(0, min(len(ordered) - 1, round((pct / 100.0) * (len(ordered) - 1))))
    return round(ordered[rank], 1)


def _delay_ms(server_iso: Any, recv_iso: Any) -> float | None:
    """Milliseconds between the server timestamp and local receive time."""
    server = iso_to_datetime(server_iso) if isinstance(server_iso, str) else None
    recv = iso_to_datetime(recv_iso) if isinstance(recv_iso, str) else None
    if server is None or recv is None:
        return None
    return round((recv - server).total_seconds() * 1000.0, 1)


def _message_type(stream: str, raw: Any) -> str:
    """Non-identifying message type label (book ``event_type`` / comment ``type``)."""
    if isinstance(raw, dict):
        kind = raw.get("event_type") or raw.get("type")
        if isinstance(kind, str) and kind:
            return kind
    return "backfill" if stream == "comments" else "other"


class _StreamTail:
    """Incremental tail + live stats for one ``<stream>.jsonl`` file."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.name = path.stem
        self.exists = False
        self.offset = 0
        self.count = 0
        self.malformed = 0
        self.with_server_ts = 0
        self.types: Counter[str] = Counter()
        self.last_ts: str | None = None
        self.last_server_ts: str | None = None
        self._delays: deque[float] = deque(maxlen=_DELAY_WINDOW)
        self._buf = b""
        self._attached = False
        # Smoothed rate (EMA of per-poll msgs/sec) + a short history for the spark.
        self._last_mono: float | None = None
        self._last_count = 0
        self.rate = 0.0
        self.spark: deque[float] = deque([0.0] * _SPARK_WINDOW, maxlen=_SPARK_WINDOW)

    # -- attach / tail ------------------------------------------------------ #

    def _count_newlines(self, size: int) -> int:
        """Count newlines in exactly the first ``size`` bytes (no JSON parsing).

        Bounding to ``size`` is load-bearing, not cosmetic: ``_attach`` parks the
        offset relative to this same ``size``, so the count and the offset must
        describe the identical byte prefix. If we instead read to the live EOF,
        a record appended after the caller's ``stat()`` would be counted here yet
        re-read (and re-counted) on the next poll — double-counting on an actively
        written file. The file only grows (append-only), so ``[0, size)`` is stable.
        """
        total = 0
        remaining = size
        with open(self.path, "rb") as fh:
            while remaining > 0:
                chunk = fh.read(min(_CHUNK, remaining))
                if not chunk:
                    break
                total += chunk.count(_NEWLINE)
                remaining -= len(chunk)
        return total

    def _attach(self, size: int) -> None:
        """First sight of the file: set the exact count and park on a line boundary."""
        self.exists = True
        self.count = self._count_newlines(size)
        self._last_count = self.count
        # Park the offset on the last newline so a trailing partial line (a record
        # mid-flush) is re-read intact on the next poll rather than split.
        tail_len = min(size, _SEED_TAIL)
        with open(self.path, "rb") as fh:
            fh.seek(size - tail_len)
            tail = fh.read(tail_len)
        nl = tail.rfind(_NEWLINE)
        self.offset = size if nl == -1 else size - (len(tail) - (nl + 1))
        # Seed "last seen" from the final complete line so the first view isn't blank.
        if nl != -1:
            self._seed_last(tail[:nl])
        self._attached = True

    def _seed_last(self, blob: bytes) -> None:
        """Populate last_ts/type/delay from the final complete line (display only)."""
        line = blob.rsplit(_NEWLINE, 1)[-1].strip()
        if not line:
            return
        try:
            rec = json.loads(line)
        except (json.JSONDecodeError, UnicodeDecodeError):
            return
        if not isinstance(rec, dict):
            return
        self.last_ts = rec.get("ts_recv") or self.last_ts
        self.last_server_ts = rec.get("ts_server") or self.last_server_ts

    def poll(self, mono: float) -> list[dict[str, Any]]:
        """Consume new bytes; return lightweight (PII-free) descriptors of new records."""
        try:
            size = self.path.stat().st_size
        except (FileNotFoundError, NotADirectoryError):
            self._update_rate(mono, 0)
            return []
        if not self._attached:
            self._attach(size)
            self._update_rate(mono, 0)
            return []
        if size < self.offset:  # truncated or replaced under us — resync from start.
            self._reset()
            self._attach(size)
            self._update_rate(mono, 0)
            return []
        if size == self.offset:
            self._update_rate(mono, 0)
            return []

        with open(self.path, "rb") as fh:
            fh.seek(self.offset)
            data = fh.read(size - self.offset)
        self.offset = size
        self._buf += data
        *complete, self._buf = self._buf.split(_NEWLINE)

        new: list[dict[str, Any]] = []
        for raw_line in complete:
            desc = self._consume(raw_line)
            if desc is not None:
                new.append(desc)
        self._update_rate(mono, len(new))
        return new

    def _consume(self, raw_line: bytes) -> dict[str, Any] | None:
        """Parse one line, update counters, and return a PII-free descriptor."""
        line = raw_line.strip()
        if not line:
            return None
        try:
            rec = json.loads(line)
        except (json.JSONDecodeError, UnicodeDecodeError):
            self.malformed += 1
            return None
        if not isinstance(rec, dict) or "stream" not in rec or "ts_recv" not in rec:
            self.malformed += 1
            return None
        self.count += 1
        ts_recv = rec.get("ts_recv")
        if isinstance(ts_recv, str):
            self.last_ts = ts_recv
        kind = _message_type(self.name, rec.get("raw"))
        self.types[kind] += 1
        ts_server = rec.get("ts_server")
        delay = None
        if isinstance(ts_server, str) and ts_server:
            self.with_server_ts += 1
            self.last_server_ts = ts_server
            delay = _delay_ms(ts_server, ts_recv)
            # Keep historical replay (backfill) out of the live percentiles, but
            # still report the raw per-message value in the ticker.
            if delay is not None and delay <= _MAX_LIVE_DELAY_MS:
                self._delays.append(delay)
        return {"stream": self.name, "type": kind, "ts": ts_recv, "delay_ms": delay}

    def _reset(self) -> None:
        self.offset = 0
        self.count = 0
        self.malformed = 0
        self.with_server_ts = 0
        self.types.clear()
        self.last_ts = None
        self.last_server_ts = None
        self._delays.clear()
        self._buf = b""
        self._attached = False
        self._last_count = 0

    def _update_rate(self, mono: float, new_records: int) -> None:
        """EMA of per-poll messages/sec, plus a value pushed to the sparkline."""
        if self._last_mono is None:
            self._last_mono = mono
            return
        dt = mono - self._last_mono
        self._last_mono = mono
        if dt <= 0:
            return
        inst = new_records / dt
        self.rate = 0.4 * inst + 0.6 * self.rate
        self.spark.append(round(inst, 3))

    # -- snapshot ----------------------------------------------------------- #

    def seconds_since_last(self, now_iso: str) -> float | None:
        last = iso_to_datetime(self.last_ts) if self.last_ts else None
        now = iso_to_datetime(now_iso)
        if last is None or now is None:
            return None
        return round(max(0.0, (now - last).total_seconds()), 1)

    def snapshot(self, now_iso: str) -> dict[str, Any]:
        delays = list(self._delays)
        return {
            "name": self.name,
            "exists": self.exists,
            "count": self.count,
            "malformed": self.malformed,
            "rate_per_sec": round(self.rate, 2),
            "last_ts": self.last_ts,
            "seconds_since_last": self.seconds_since_last(now_iso),
            "with_server_ts": self.with_server_ts,
            "types": dict(self.types.most_common()),
            "delay_ms": {
                "p50": _percentile(delays, 50),
                "p95": _percentile(delays, 95),
                "max": round(max(delays), 1) if delays else None,
                "n": len(delays),
            },
            "spark": list(self.spark),
        }


class _EventState:
    """Tail state + cached meta for a single ``event-<id>`` capture directory."""

    def __init__(self, directory: Path) -> None:
        self.dir = directory
        name = directory.name
        self.event_id = name[len("event-") :] if name.startswith("event-") else name
        self.streams: dict[str, _StreamTail] = {}
        self.recent: deque[dict[str, Any]] = deque(maxlen=_RECENT_WINDOW)
        self._meta: dict[str, Any] | None = None
        self._meta_sig: tuple[int, int] | None = None

    def _refresh_meta(self) -> None:
        # Non-interference, not just caching: the writer finalizes meta.json with
        # an atomic os.replace, and on Windows a rename onto a file another process
        # holds *open for read* can fail. ``stat()`` opens with full sharing and
        # never collides; we only actually open meta.json for reading on the rare
        # poll where it changed, so our read handle is essentially never live at
        # the instant the writer replaces. Do NOT change this to an unconditional
        # read — that reintroduces the collision window.
        path = self.dir / "meta.json"
        try:
            stat = path.stat()
        except (FileNotFoundError, NotADirectoryError):
            self._meta, self._meta_sig = None, None
            return
        sig = (stat.st_mtime_ns, stat.st_size)
        if sig == self._meta_sig:
            return
        try:
            self._meta = json.loads(path.read_bytes().decode("utf-8"))
            self._meta_sig = sig
        except (json.JSONDecodeError, UnicodeDecodeError, OSError):
            return  # keep the last good meta; a mid-write read is rare and transient.

    def _stream_names(self) -> list[str]:
        if self._meta and isinstance(self._meta.get("streams"), list):
            return [s for s in self._meta["streams"] if isinstance(s, str)]
        return sorted(p.stem for p in self.dir.glob("*.jsonl"))

    def _ensure_streams(self) -> None:
        for name in self._stream_names():
            if name not in self.streams:
                self.streams[name] = _StreamTail(self.dir / f"{name}.jsonl")

    def last_activity(self) -> float:
        """Most recent mtime across meta/stream files (for picking the active event)."""
        latest = 0.0
        for candidate in (self.dir / "meta.json", *(self.dir.glob("*.jsonl"))):
            try:
                latest = max(latest, candidate.stat().st_mtime)
            except OSError:
                continue
        return latest

    def poll(self, mono: float) -> None:
        self._refresh_meta()
        self._ensure_streams()
        for tail in self.streams.values():
            for desc in tail.poll(mono):
                self.recent.append(desc)

    def _status(self, now_iso: str, idle_threshold: float) -> str:
        meta = self._meta or {}
        any_data = any(t.count for t in self.streams.values())
        if meta.get("stopped_at"):
            return "stopped"
        if not self.streams or (not any_data and not meta.get("started_at")):
            return "no-data"
        gaps = [
            t.seconds_since_last(now_iso) for t in self.streams.values() if t.last_ts is not None
        ]
        if not gaps:
            return "starting" if meta.get("started_at") else "no-data"
        return "live" if min(gaps) <= idle_threshold else "idle"

    def light_status(self, now_ts: float, idle_threshold: float) -> str:
        """Cheap status for the events list — meta + file mtime, no tailing."""
        meta = self._meta or {}
        if meta.get("stopped_at"):
            return "stopped"
        activity = self.last_activity()
        if activity and (now_ts - activity) <= idle_threshold:
            return "live"
        if meta.get("started_at"):
            return "idle"
        return "no-data"

    def snapshot(self, now_iso: str, idle_threshold: float) -> dict[str, Any]:
        meta = self._meta or {}
        started = iso_to_datetime(meta.get("started_at")) if meta.get("started_at") else None
        stopped = iso_to_datetime(meta.get("stopped_at")) if meta.get("stopped_at") else None
        now = iso_to_datetime(now_iso)
        uptime = None
        if started and now:
            end = stopped or now
            uptime = round(max(0.0, (end - started).total_seconds()), 1)
        streams = {name: tail.snapshot(now_iso) for name, tail in self.streams.items()}
        return {
            "event_id": self.event_id,
            "dir": self.dir.as_posix(),
            "status": self._status(now_iso, idle_threshold),
            "uptime_seconds": uptime,
            "meta": {
                "polytape_version": meta.get("polytape_version"),
                "event_id": meta.get("event_id"),
                "title": (meta.get("event") or {}).get("title") if meta.get("event") else None,
                "slug": (meta.get("event") or {}).get("slug") if meta.get("event") else None,
                "streams": meta.get("streams") or list(self.streams),
                "market_ids": meta.get("market_ids") or [],
                "clob_token_ids": meta.get("clob_token_ids") or [],
                "hashing": meta.get("hashing") or {},
                "started_at": meta.get("started_at"),
                "stopped_at": meta.get("stopped_at"),
                "out_dir": meta.get("out_dir"),
            },
            "streams": streams,
            "totals": {
                "count": sum(s["count"] for s in streams.values()),
                "rate_per_sec": round(sum(s["rate_per_sec"] for s in streams.values()), 2),
                "malformed": sum(s["malformed"] for s in streams.values()),
            },
            "gaps": meta.get("gaps") or [],
            "recent": list(reversed(self.recent)),
        }


class CaptureMonitor:
    """Discovers capture directories under a root and serves live snapshots.

    A single instance backs the dashboard. It polls the currently-selected event
    in full and refreshes only the (cached, mtime-keyed) meta of the others, so
    cost stays proportional to one active capture regardless of how many old
    captures sit under the root.
    """

    def __init__(
        self,
        out_dir: str | Path,
        *,
        idle_threshold: float = DEFAULT_IDLE_THRESHOLD,
        monotonic: Callable[[], float] = time.monotonic,
        now_iso: Callable[[], str] = utc_now_iso,
    ) -> None:
        self.out_dir = Path(out_dir)
        self.idle_threshold = idle_threshold
        self._monotonic = monotonic
        self._now_iso = now_iso
        self._events: dict[str, _EventState] = {}

    def discover(self) -> list[Path]:
        """Capture directories under the root (or the root itself if it is one)."""
        root = self.out_dir
        if (root / "meta.json").exists() or any(root.glob("*.jsonl")):
            return [root]
        try:
            return sorted(d for d in root.glob("event-*") if d.is_dir())
        except OSError:
            return []

    def _state_for(self, directory: Path) -> _EventState:
        key = directory.as_posix()
        state = self._events.get(key)
        if state is None:
            state = _EventState(directory)
            self._events[key] = state
        return state

    def _select(self, states: list[_EventState], event_id: str | None) -> _EventState | None:
        if not states:
            return None
        if event_id:
            for state in states:
                if state.event_id == event_id:
                    return state
        return max(states, key=lambda s: s.last_activity())

    def snapshot(self, event_id: str | None = None) -> dict[str, Any]:
        """Poll the selected event and return a full dashboard snapshot."""
        now_iso = self._now_iso()
        mono = self._monotonic()
        dirs = self.discover()
        states = [self._state_for(d) for d in dirs]
        for state in states:
            state._refresh_meta()  # cheap (mtime-cached); keeps the picker current.

        selected = self._select(states, event_id)
        now_dt = iso_to_datetime(now_iso)
        now_ts = now_dt.timestamp() if now_dt else 0.0
        events = [
            {
                "event_id": s.event_id,
                "dir": s.dir.as_posix(),
                "title": (
                    (s._meta or {}).get("event", {}).get("title")
                    if (s._meta or {}).get("event")
                    else None
                ),
                "status": s.light_status(now_ts, self.idle_threshold),
                "selected": s is selected,
            }
            for s in states
        ]

        result: dict[str, Any] = {
            "now": now_iso,
            "out_dir": self.out_dir.as_posix(),
            "idle_threshold": self.idle_threshold,
            "events": events,
            "selected_event": selected.event_id if selected else None,
        }
        if selected is None:
            result["status"] = "no-data"
            result["streams"] = {}
            result["totals"] = {"count": 0, "rate_per_sec": 0.0, "malformed": 0}
            result["gaps"] = []
            result["recent"] = []
            result["meta"] = {}
            result["uptime_seconds"] = None
            return result

        selected.poll(mono)
        result.update(selected.snapshot(now_iso, self.idle_threshold))
        return result
