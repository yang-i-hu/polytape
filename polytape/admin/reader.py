"""Read-only, metadata-driven view of a polytape run for the admin dashboard.

The recorder keeps ``meta.json`` current — per-stream and per-event counts, per-event
last-seen timestamps, the open match set, and the gap log — and flushes it every few
seconds. So the dashboard reads EVERYTHING it shows from ``meta.json`` plus the persisted
run registry: a few KB of memory, instant load, no scan of the multi-GB JSONL log and no
catch-up after a restart. The append-only logs are touched only by the SEPARATE on-demand
download/extractor path, never here.

Event attribution and counting live in the recorder (it tags every record by event as it
writes); this module just surfaces the result.
"""

from __future__ import annotations

import json
import logging
import shutil
import subprocess
import threading
from collections.abc import Callable
from pathlib import Path
from typing import Any

from polytape.admin import registry as _reg
from polytape.envelope import iso_to_datetime, utc_now_iso

logger = logging.getLogger("polytape.admin.reader")


def _slug_date(slug: str | None) -> str | None:
    """Trailing ``YYYY-MM-DD`` in a ``fifwc-...-2026-06-19`` slug, if present."""
    if not slug:
        return None
    tail = slug.rsplit("-", 3)[-3:]
    if len(tail) == 3 and tail[0].isdigit() and len(tail[0]) == 4:
        return "-".join(tail)
    return None


class RunReader:
    """Metadata-driven, read-only view of one run directory (e.g. ``/data/run-wc``)."""

    def __init__(
        self,
        run_dir: str | Path,
        *,
        unit: str = "polytape",
        env_file: str | Path = "/etc/polytape/polytape.env",
        registry_file: str | Path = "/var/log/polytape-admin/registry.json",
        live_window_s: float = 60.0,
        now: Callable[[], str] = utc_now_iso,
    ) -> None:
        self._dir = Path(run_dir)
        self._unit = unit
        self._env_file = Path(env_file)
        self._registry_file = Path(registry_file)
        self._live_window = live_window_s
        self._now = now
        # update() runs in a worker thread (off the event loop); this guards every
        # read/write of the reader's mutable state. _systemctl() (a subprocess) is kept
        # OFF this lock so a slow systemctl never makes a request wait.
        self._lock = threading.RLock()
        # All of the following are refreshed from meta.json on each update() — meta is the
        # authoritative, cumulative source the recorder maintains.
        self._counts: dict[str, int] = {"comments": 0, "book": 0}
        self._by_event: dict[str, dict[str, int]] = {}
        self._last_ts: dict[str, str] = {}  # event id -> last record ts_recv
        self._last_record_at: str | None = None  # overall last record ts_recv
        self._gaps: list[dict[str, Any]] = []
        self._started_at: str | None = None
        self._event_ids: list[str] = []  # current OPEN set
        self._event_title: dict[str, str] = {}
        self._event_date: dict[str, str | None] = {}
        self._event_conds: dict[str, list[str]] = {}
        self._markets_total: set[str] = set()  # open-set condition ids (coverage denominator)
        self._markets_seen: set[str] = set()  # open-set condition ids with recorded book data
        # _systemctl() spawns a subprocess; cache it per update() tick so status() never
        # blocks the event loop with a subprocess on every request.
        self._recorder_cache: dict[str, Any] | None = None
        # Cumulative run registry (all matches, finished + open) — recovered from Gamma and
        # persisted; EXTENDS the meta-derived maps so finished matches (rolled out of
        # meta.events) are still listed, counted, and downloadable.
        self._registry: _reg.Registry = _reg.Registry()
        self._registry_sig: tuple[int, int] | None = None

    @property
    def run_dir(self) -> Path:
        """The run directory this reader observes (used by the download endpoint)."""
        return self._dir

    # -- metadata + registry ------------------------------------------------- #

    def _load_meta(self) -> None:
        """Refresh everything the dashboard shows from ``meta.json`` (counts, freshness,
        open set, gaps). Best-effort: a missing/garbage meta leaves the prior snapshot."""
        try:
            meta = json.loads((self._dir / "meta.json").read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return
        if not isinstance(meta, dict):
            return
        self._started_at = meta.get("started_at")
        self._gaps = list(meta.get("gaps") or [])
        self._last_record_at = (
            meta.get("last_record_at") if isinstance(meta.get("last_record_at"), str) else None
        )
        # Counts straight from the recorder's cumulative accounting.
        counts = meta.get("counts")
        merged = {"comments": 0, "book": 0}
        if isinstance(counts, dict):
            merged.update({str(s): n for s, n in counts.items() if isinstance(n, int) and n >= 0})
        self._counts = merged
        by_event = meta.get("counts_by_event")
        self._by_event = (
            {
                str(eid): {str(s): n for s, n in per.items() if isinstance(n, int) and n >= 0}
                for eid, per in by_event.items()
                if isinstance(per, dict)
            }
            if isinstance(by_event, dict)
            else {}
        )
        last_ts = meta.get("last_ts_by_event")
        self._last_ts = (
            {str(e): t for e, t in last_ts.items() if isinstance(t, str)}
            if isinstance(last_ts, dict)
            else {}
        )
        # Event identity maps for the CURRENT open set (from meta.events).
        title: dict[str, str] = {}
        date: dict[str, str | None] = {}
        ids: list[str] = []
        markets: set[str] = set()
        event_conds: dict[str, list[str]] = {}
        for event in meta.get("events") or []:
            eid = str(event.get("id"))
            ids.append(eid)
            title[eid] = (event.get("title") or "").strip()
            date[eid] = _slug_date(event.get("slug"))
            conds: list[str] = []
            for market in event.get("markets") or []:
                cond = market.get("conditionId")
                if not cond:
                    continue
                markets.add(cond)
                conds.append(cond)
            event_conds[eid] = conds
        self._event_title, self._event_date, self._event_ids = title, date, ids
        self._markets_total, self._event_conds = markets, event_conds
        # Coverage: open-set condition ids belonging to events that have recorded book data
        # (per-match granularity is all meta carries; intersected with the open set below).
        seen: set[str] = set()
        for eid, per in self._by_event.items():
            if per.get("book"):
                seen.update(event_conds.get(eid, []))
        self._markets_seen = seen

    def _load_registry(self) -> None:
        """Load the persisted run registry (mtime-cached). Best-effort: a missing/garbage
        file leaves an empty registry, so the admin degrades to meta-only."""
        try:
            stat = self._registry_file.stat()
        except OSError:
            self._registry, self._registry_sig = _reg.Registry(), None
            return
        sig = (stat.st_mtime_ns, stat.st_size)
        if sig == self._registry_sig:
            return
        self._registry = _reg.load_registry(self._registry_file)
        self._registry_sig = sig

    def update(self) -> None:
        """Refresh the meta + registry snapshots. Cheap (reads two small JSON files), so it
        runs every poll with ~KB of memory — no scan of the append-only logs."""
        recorder = self._systemctl()  # subprocess — never under the lock
        with self._lock:
            self._load_meta()
            self._load_registry()
            self._recorder_cache = recorder

    def extractable_event_ids(self) -> list[str]:
        """Finished matches (rolled out of the open set) that have recorded data — the
        immutable, downloadable matches the background extractor should pre-build."""
        with self._lock:
            open_set = set(self._event_ids)
            out: list[str] = []
            for eid in self._registry.order:
                if eid in open_set:
                    continue
                counts = self._by_event.get(eid, {})
                if counts.get("book") or counts.get("comments"):
                    out.append(eid)
            return out

    # -- environment probes (best-effort; degrade gracefully) --------------- #

    def _systemctl(self) -> dict[str, Any]:
        try:
            out = subprocess.run(
                [
                    "systemctl",
                    "show",
                    self._unit,
                    "-p",
                    "ActiveState",
                    "-p",
                    "NRestarts",
                    "-p",
                    "ActiveEnterTimestamp",
                ],
                capture_output=True,
                text=True,
                timeout=5,
            ).stdout
        except (OSError, subprocess.SubprocessError):
            return {"active": "unknown", "restarts": None, "since": None}
        kv = dict(line.split("=", 1) for line in out.splitlines() if "=" in line)
        restarts = kv.get("NRestarts")
        return {
            "active": kv.get("ActiveState", "unknown"),
            "restarts": int(restarts) if restarts and restarts.isdigit() else None,
            "since": kv.get("ActiveEnterTimestamp") or None,
        }

    def _disk_percent(self) -> int | None:
        try:
            usage = shutil.disk_usage(self._dir if self._dir.exists() else self._dir.parent)
        except OSError:
            return None
        return round(usage.used / usage.total * 100) if usage.total else None

    def _heartbeat_armed(self) -> bool:
        try:
            for line in self._env_file.read_text(encoding="utf-8").splitlines():
                if line.startswith("POLYTAPE_HEARTBEAT_URL=") and line.split("=", 1)[1].strip():
                    return True
        except OSError:
            pass
        return False

    def _age_s(self, ts: str | None) -> float | None:
        if not ts:
            return None
        then, now = iso_to_datetime(ts), iso_to_datetime(self._now())
        if then is None or now is None:
            return None
        return max(0.0, (now - then).total_seconds())

    # -- public views ------------------------------------------------------- #

    def status(self) -> dict[str, Any]:
        # I/O that doesn't touch reader state stays OFF the lock: the cached recorder
        # state (or a rare cold-start subprocess fallback), disk usage, env-file probe.
        recorder = self._recorder_cache if self._recorder_cache is not None else self._systemctl()
        disk = self._disk_percent()
        heartbeat = self._heartbeat_armed()
        with self._lock:
            # Overall freshness from the recorder's last_record_at, falling back to the
            # newest per-event ts if an older meta has no overall field.
            freshest = self._last_record_at or max(self._last_ts.values(), default=None)
            seen = len(self._markets_seen & self._markets_total)
            return {
                "recorder": recorder,
                "started_at": self._started_at,
                "last_record_age_s": self._age_s(freshest),
                "records": dict(self._counts),
                "open_matches": len(self._event_ids),
                "coverage": {"seen": seen, "total": len(self._markets_total)},
                "disk_percent": disk,
                "heartbeat_armed": heartbeat,
                "gaps": len(self._gaps),
                "as_of": self._now(),
            }

    def matches(self) -> list[dict[str, Any]]:
        """Every match in the run — finished + open — in STABLE schedule order.

        Lists the registry (all matches ever recorded) unioned with the current open set,
        so finished/rolled-out matches stay selectable for download. Ordered by scheduled
        ``date`` (then ``event_id``), NOT recency — so rows never jump as counts tick.
        Open-set membership decides live/quiet/pending; everything else is ``finished``.
        Counts + freshness come straight from meta.json (no log scan).
        """
        with self._lock:
            open_set = set(self._event_ids)
            ids = list(self._registry.order)
            ids += [eid for eid in self._event_ids if eid not in set(ids)]  # brand-new fixtures

            rows: list[dict[str, Any]] = []
            for eid in ids:
                age = self._age_s(self._last_ts.get(eid))
                if eid in open_set:  # recording — recency decides live/quiet/pending
                    status = (
                        "pending"
                        if age is None
                        else ("live" if age <= self._live_window else "quiet")
                    )
                else:  # rolled out of the open set — a finished/past match
                    status = "finished"
                counts = dict(self._by_event.get(eid, {}))  # copy: row outlives the lock
                rows.append(
                    {
                        "event_id": eid,
                        "title": self._registry.title.get(eid) or self._event_title.get(eid, eid),
                        "date": self._registry.date.get(eid) or self._event_date.get(eid),
                        "counts": counts,
                        "last_seen_age_s": age,
                        "status": status,
                        "open": eid in open_set,
                        # On disk and selectable — data was actually recorded for it.
                        "downloadable": bool(counts.get("book") or counts.get("comments")),
                    }
                )
            # STABLE schedule order: by date (undated last), then event_id. No recency.
            rows.sort(key=lambda r: (r["date"] is None, r["date"] or "", r["event_id"]))
            return rows

    def download_registry(self, meta: dict[str, Any] | None = None) -> dict[str, dict[str, Any]]:
        """The merged ``{event_id: entry}`` the download path uses — the in-memory
        registry (all matches, finished + open) unioned with the CURRENT open set.

        Prefers a freshly-read ``meta`` for the open set (the route already loads it),
        falling back to the reader's cached ``meta`` snapshot. Registry entries win on
        overlap (richer identity); a brand-new fixture present only in meta is still added.
        Each entry carries ``markets[{conditionId, clobTokenIds}]`` so the download can
        resolve a finished match's conditionIds for filtering.
        """
        with self._lock:  # snapshot reader state; the merge below builds off these copies
            merged: dict[str, dict[str, Any]] = {
                eid: dict(e) for eid, e in self._registry.events.items()
            }
            open_ids = list(self._event_ids)
            ev_title = dict(self._event_title)
            ev_date = dict(self._event_date)
            ev_conds = {k: list(v) for k, v in self._event_conds.items()}
        meta_events = (meta.get("events") if meta else None) or []
        if meta_events:
            for ev in meta_events:
                eid = str(ev.get("id"))
                if not eid or eid == "None":
                    continue
                merged.setdefault(
                    eid,
                    {
                        "event_id": eid,
                        "title": (ev.get("title") or "").strip() or eid,
                        "slug": ev.get("slug"),
                        "date": _slug_date(ev.get("slug")),
                        "closed": False,
                        "markets": [
                            {
                                "conditionId": m.get("conditionId"),
                                "clobTokenIds": m.get("clobTokenIds") or [],
                            }
                            for m in (ev.get("markets") or [])
                        ],
                    },
                )
        else:  # no fresh meta -> fall back to the reader's cached open set
            for eid in open_ids:
                merged.setdefault(
                    eid,
                    {
                        "event_id": eid,
                        "title": ev_title.get(eid, eid),
                        "date": ev_date.get(eid),
                        "closed": False,
                        "markets": [
                            {"conditionId": c, "clobTokenIds": []} for c in ev_conds.get(eid, [])
                        ],
                    },
                )
        return merged
