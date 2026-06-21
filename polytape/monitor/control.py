"""Optional control plane: launch and stop capture processes from the dashboard.

This is the *only* part of the monitor that is not purely read-only. A
:class:`RecorderManager` spawns ``python -m polytape`` (a live capture) or the
synthetic demo feeder as managed child processes, tracks them, and stops them
gracefully. Monitoring itself remains a passive file tail; a *launched* recorder
is an ordinary ``polytape`` process — identical to one started by hand — so the
recorder's behavior is unchanged whether or not the dashboard launched it.

Safety is enforced at the HTTP layer (loopback-only, CSRF header — see
:mod:`polytape.monitor.server`); this module additionally never uses a shell,
builds argv as a validated list, and refuses obviously bad input. There is no
"pause": a websocket capture cannot pause without disconnecting and missing the
very burst it exists to record, so only start/stop are offered.

Process groups & graceful stop:

* POSIX — each child gets its own session (``start_new_session``); stop sends
  ``SIGINT`` (polytape finalizes ``meta.json`` and exits cleanly), escalating to
  ``terminate``/``kill`` if it does not exit in time.
* Windows — each child gets ``CREATE_NEW_PROCESS_GROUP``; stop sends
  ``CTRL_BREAK_EVENT``, escalating to ``terminate`` if needed. Captured data is
  safe either way (every JSONL line is flushed on write); only ``meta.json``
  finalization can be skipped on a hard fallback, exactly like any abrupt kill.
"""

from __future__ import annotations

import json
import logging
import signal
import subprocess
import sys
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from polytape.envelope import utc_now_iso
from polytape.gamma import GammaError, resolve_event_id

logger = logging.getLogger("polytape.monitor.control")

_GRACEFUL_TIMEOUT = 8.0
_TERMINATE_TIMEOUT = 3.0
#: An event dir whose meta has no ``stopped_at`` and whose JSONL was written
#: within this many seconds is treated as actively recorded — refuse to launch a
#: second recorder into it (concurrent appenders corrupt the file).
_ACTIVE_CAPTURE_STALENESS = 20.0


class ControlError(Exception):
    """A control action was rejected (bad input or invalid state)."""


def _valid_event_id(event_id: str, *, numeric: bool) -> str:
    event_id = str(event_id).strip()
    if not event_id:
        raise ControlError("event id is required")
    if numeric and not event_id.isdigit():
        raise ControlError("event id must be numeric for a live recording")
    # Used to build a directory name; keep it filesystem- and shell-safe.
    if not all(c.isalnum() or c in "-_" for c in event_id):
        raise ControlError("event id may contain only letters, digits, '-' and '_'")
    return event_id


@dataclass
class _Recording:
    event_id: str
    kind: str  # "record" | "demo"
    argv: list[str]
    proc: subprocess.Popen[bytes]
    log_path: Path
    started_at: str

    def status(self) -> dict[str, Any]:
        returncode = self.proc.poll()
        return {
            "event_id": self.event_id,
            "kind": self.kind,
            "pid": self.proc.pid,
            "running": returncode is None,
            "returncode": returncode,
            "started_at": self.started_at,
            "log": self.log_path.as_posix(),
        }


class RecorderManager:
    """Spawns and stops capture subprocesses, all rooted at one output dir."""

    def __init__(
        self,
        out_dir: str | Path,
        *,
        python: str = sys.executable,
        now_iso: Callable[[], str] = utc_now_iso,
        resolver: Callable[[str], str] = resolve_event_id,
    ) -> None:
        self.out_dir = Path(out_dir)
        self._python = python
        self._now_iso = now_iso
        self._resolver = resolver
        self._recordings: dict[str, _Recording] = {}
        self._lock = threading.Lock()

    # -- start -------------------------------------------------------------- #

    def start_recording(
        self,
        event_id: str,
        *,
        comments: bool = True,
        book: bool = True,
        hash_usernames: bool = True,
        include_series_comments: bool = False,
        entity_type: str = "Event",
        log_level: str = "INFO",
    ) -> dict[str, Any]:
        """Launch a live ``polytape`` capture.

        ``event_id`` may be a numeric id, a slug, or a Polymarket URL — it is
        resolved to a numeric event id first (slugs/URLs need one Gamma lookup).
        ``entity_type="Series"`` records a parent-series chat directly by its
        series id (comments only).
        """
        if entity_type not in ("Event", "Series"):
            raise ControlError("entity_type must be 'Event' or 'Series'")
        # A series id is not an /events/ slug; resolving it would 404. (Numeric
        # ids pass through the resolver unchanged either way.)
        if entity_type == "Event":
            try:
                event_id = self._resolver(event_id)
            except GammaError as exc:
                raise ControlError(str(exc)) from exc
        event_id = _valid_event_id(event_id, numeric=True)
        if not comments and not book:
            raise ControlError("enable at least one of comments / book")
        if entity_type == "Series" and not comments:
            raise ControlError("a Series capture records comments only; enable comments")
        argv = [self._python, "-m", "polytape", "--event-id", event_id, "--out", str(self.out_dir)]
        if not comments:
            argv.append("--no-comments")
        if not book:
            argv.append("--no-book")
        if not hash_usernames:
            argv.append("--no-hash")
        if include_series_comments:
            argv.append("--include-series-comments")
        if entity_type != "Event":
            argv += ["--entity-type", entity_type]
        argv += ["--log-level", _valid_log_level(log_level)]
        return self._spawn(event_id, "record", argv)

    def start_demo(self, event_id: str = "demo", *, rate: float = 8.0) -> dict[str, Any]:
        """Launch the synthetic demo feeder (no network) for a quick try."""
        event_id = _valid_event_id(event_id, numeric=False)
        try:
            rate_val = float(rate)
        except (TypeError, ValueError):
            raise ControlError("rate must be a number") from None
        if not 0 < rate_val <= 1000:
            raise ControlError("rate must be between 0 and 1000")
        argv = [
            self._python,
            "-m",
            "polytape.monitor.demo",
            "--out",
            str(self.out_dir),
            "--event-id",
            event_id,
            "--rate",
            str(rate_val),
        ]
        return self._spawn(event_id, "demo", argv)

    def _active_capture_exists(self, event_dir: Path) -> bool:
        """True if ``event_dir`` looks like a capture some process is writing now.

        Heuristic (no recorder change): the meta has no ``stopped_at`` *and* a
        stream file was modified within ``_ACTIVE_CAPTURE_STALENESS``. Catches the
        common, high-risk case — an event being actively recorded by *another*
        process (a second monitor, or the CLI) — without a lock file. A quiet feed
        can slip past it; concurrent appenders are still the user's hazard to avoid.
        """
        if not event_dir.exists():
            return False
        meta = event_dir / "meta.json"
        try:
            if meta.exists() and json.loads(meta.read_bytes()).get("stopped_at"):
                return False  # cleanly finalized -> not active
        except (OSError, json.JSONDecodeError, UnicodeDecodeError):
            pass
        now = time.time()
        for stream_file in event_dir.glob("*.jsonl"):
            try:
                if now - stream_file.stat().st_mtime < _ACTIVE_CAPTURE_STALENESS:
                    return True
            except OSError:
                continue
        return False

    def _spawn(self, event_id: str, kind: str, argv: list[str]) -> dict[str, Any]:
        with self._lock:
            existing = self._recordings.get(event_id)
            if existing is not None and existing.proc.poll() is None:
                raise ControlError(f"a recording for event {event_id} is already running")
            event_dir = self.out_dir / f"event-{event_id}"
            if self._active_capture_exists(event_dir):
                raise ControlError(
                    f"event {event_id} appears to be recording already (a stream file in "
                    f"{event_dir.as_posix()} was just updated); refusing to start a second "
                    "recorder into the same directory, which would corrupt the capture"
                )
            event_dir.mkdir(parents=True, exist_ok=True)
            log_path = event_dir / "recorder.log"
            kwargs: dict[str, Any] = {}
            if sys.platform == "win32":
                kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
            else:
                kwargs["start_new_session"] = True
            try:
                log = open(log_path, "a", encoding="utf-8")
            except OSError as exc:
                raise ControlError(f"could not open log file: {exc}") from exc
            # The child inherits its own handle to the log; the parent closes ours
            # in the finally either way (on success the child keeps writing).
            try:
                log.write(f"\n=== {self._now_iso()} launch: {' '.join(argv)} ===\n")
                log.flush()
                proc = subprocess.Popen(  # noqa: S603 — argv is a validated list, never a shell
                    argv,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    stdin=subprocess.DEVNULL,
                    **kwargs,
                )
            except OSError as exc:
                raise ControlError(f"failed to launch: {exc}") from exc
            finally:
                log.close()
            rec = _Recording(event_id, kind, argv, proc, log_path, self._now_iso())
            self._recordings[event_id] = rec
            logger.info("started %s capture for event %s (pid %s)", kind, event_id, proc.pid)
            return rec.status()

    # -- stop --------------------------------------------------------------- #

    def stop(self, event_id: str, *, graceful_timeout: float = _GRACEFUL_TIMEOUT) -> dict[str, Any]:
        """Gracefully stop a managed recording, escalating only if it hangs."""
        with self._lock:
            rec = self._recordings.get(str(event_id).strip())
        if rec is None:
            raise ControlError(f"no recording managed here for event {event_id}")
        proc = rec.proc
        if proc.poll() is not None:
            return rec.status()  # already exited
        try:
            if sys.platform == "win32":
                proc.send_signal(signal.CTRL_BREAK_EVENT)  # type: ignore[attr-defined]
            else:
                proc.send_signal(signal.SIGINT)
        except (OSError, ValueError):
            pass
        try:
            proc.wait(timeout=graceful_timeout)
        except subprocess.TimeoutExpired:
            logger.warning("event %s did not stop gracefully; terminating", rec.event_id)
            proc.terminate()
            try:
                proc.wait(timeout=_TERMINATE_TIMEOUT)
            except subprocess.TimeoutExpired:
                logger.warning("event %s ignored terminate; killing", rec.event_id)
                proc.kill()
                # Reap so status() reflects the real exit (and no zombie lingers).
                try:
                    proc.wait(timeout=_TERMINATE_TIMEOUT)
                except subprocess.TimeoutExpired:
                    pass
        return rec.status()

    # -- introspection ------------------------------------------------------ #

    def statuses(self) -> list[dict[str, Any]]:
        """Status of every recording this manager has launched (running or exited)."""
        with self._lock:
            return [rec.status() for rec in self._recordings.values()]

    def running_count(self) -> int:
        with self._lock:
            return sum(1 for rec in self._recordings.values() if rec.proc.poll() is None)


def _valid_log_level(level: str) -> str:
    level = str(level).upper()
    if level not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
        raise ControlError(f"invalid log level: {level!r}")
    return level
