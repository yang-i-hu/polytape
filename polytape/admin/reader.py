"""Read-only aggregation over a polytape run for the admin dashboard.

Turns the recorder's on-disk outputs into the ``/status`` and ``/matches`` views.
Incremental by design: the first :meth:`update` scans each JSONL file in full,
then later calls read only the bytes appended since — so it stays fast as
``book.jsonl`` grows. Everything here is read-only; it never touches the recorder.

Event attribution mirrors the recorder: book records route by top-level ``market``
(condition id, via the meta event map); comments by ``parentEntityID``.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import shutil
import subprocess
import threading
import time
from collections import deque
from collections.abc import Callable
from pathlib import Path
from typing import Any

from polytape.admin import registry as _reg
from polytape.envelope import iso_to_datetime, utc_now_iso

logger = logging.getLogger("polytape.admin.reader")

# Bound the bytes read per _consume call. Without this, the FIRST scan of a
# multi-GB book.jsonl does handle.read() of the whole file — ~1.5 GB of bytes + a
# decoded str + a million-element line list at once — which OOM-kills the sidecar on
# a small VM. With the cap, a large backlog simply catches up over several update()
# ticks (the recorder appends far slower than a 64 MiB/tick read drains it).
_MAX_READ_BYTES = 64 * 1024 * 1024  # 64 MiB

# Reader-checkpoint format version. Persisted alongside the offsets + aggregates so an
# old checkpoint left by a previous build is safely REBUILT (a full re-drain from 0)
# rather than mis-read after the payload shape changes — bump on any shape change.
_CHECKPOINT_SCHEMA = 1


def _slug_date(slug: str | None) -> str | None:
    """Trailing ``YYYY-MM-DD`` in a ``fifwc-...-2026-06-19`` slug, if present."""
    if not slug:
        return None
    tail = slug.rsplit("-", 3)[-3:]
    if len(tail) == 3 and tail[0].isdigit() and len(tail[0]) == 4:
        return "-".join(tail)
    return None


def _comment_event(raw: dict[str, Any]) -> str | None:
    """Event id of a comment/reaction record (its ``parentEntityID``)."""
    core = raw.get("payload") if isinstance(raw.get("payload"), dict) else raw
    parent = core.get("parentEntityID")
    return str(parent) if parent is not None else None


class RunReader:
    """Incremental, read-only view of one run directory (e.g. ``/data/run-wc``)."""

    def __init__(
        self,
        run_dir: str | Path,
        *,
        unit: str = "polytape",
        env_file: str | Path = "/etc/polytape/polytape.env",
        matches_file: str | Path = "/etc/polytape/wc_matches.json",
        registry_file: str | Path = "/var/log/polytape-admin/registry.json",
        live_window_s: float = 60.0,
        price_history: int = 120,
        peek: int = 60,
        now: Callable[[], str] = utc_now_iso,
        mono: Callable[[], float] = time.monotonic,
        max_read_bytes: int = _MAX_READ_BYTES,
        checkpoint_file: str | Path | None = None,
    ) -> None:
        self._dir = Path(run_dir)
        self._unit = unit
        self._env_file = Path(env_file)
        self._matches_file = Path(matches_file)
        self._registry_file = Path(registry_file)
        self._live_window = live_window_s
        self._price_history = price_history
        self._now = now
        self._mono = mono
        self._max_read = max_read_bytes
        # Where to persist the scan checkpoint (offsets + aggregates). None disables it;
        # a warm start then re-drains from 0 exactly as before. See save/load_checkpoint.
        self._checkpoint_file = Path(checkpoint_file) if checkpoint_file else None
        # update() runs in a worker thread (off the event loop); this guards every
        # read/write of the reader's mutable state. Reentrant so a read method can call
        # helpers freely. _systemctl() (a subprocess) is deliberately kept OFF this lock.
        self._lock = threading.RLock()
        self._offsets: dict[str, int] = {}
        self._counts: dict[str, int] = {"comments": 0, "book": 0}
        self._by_event: dict[str, dict[str, int]] = {}
        self._last_ts: dict[str, str] = {}
        self._markets_seen: set[str] = set()
        # Live view (poll-based, no SSE): a bounded ring of the most recent records,
        # records/sec computed from per-tick count snapshots, and the meta gap list.
        # All derived from the SAME per-tick parse the reader already does — no extra I/O.
        self._peek: deque = deque(maxlen=peek)
        self._gaps: list[dict[str, Any]] = []
        self._rates: dict[str, Any] = {}
        self._rate_prev: tuple[float, dict[str, int], dict[str, dict[str, int]]] | None = None
        # _systemctl() spawns a subprocess; cache it per update() tick so status()
        # never blocks the event loop with a subprocess on every request.
        self._recorder_cache: dict[str, Any] | None = None
        # Derived from meta.json (refreshed each update; the open set changes on roll-out/in).
        self._cond2event: dict[str, str] = {}
        self._event_title: dict[str, str] = {}
        self._event_date: dict[str, str | None] = {}
        self._event_ids: list[str] = []
        self._markets_total: set[str] = set()
        self._started_at: str | None = None
        # Per-market metadata: condition id -> yes token; token -> condition id;
        # event -> its condition ids; condition id -> outcome label (from labels file).
        self._market_yes: dict[str, str] = {}
        self._asset_cond: dict[str, str] = {}
        self._event_conds: dict[str, list[str]] = {}
        self._market_label: dict[str, str] = {}
        # Cumulative run registry (all matches, finished + open) — recovered from Gamma
        # discovery and persisted; EXTENDS the meta-derived maps so finished matches
        # (rolled out of meta.events) are still listed, counted, and downloadable.
        self._registry: _reg.Registry = _reg.Registry()
        self._registry_sig: tuple[int, int] | None = None
        # Reconstructed L2 book + last trade + price history, keyed by token (asset_id).
        self._book: dict[str, dict[str, dict[str, str]]] = {}
        self._last_trade: dict[str, dict[str, Any]] = {}
        self._price_hist: dict[str, deque] = {}

    # -- file helpers ------------------------------------------------------- #

    @property
    def run_dir(self) -> Path:
        """The run directory this reader observes (used by the download endpoint)."""
        return self._dir

    def _file(self, stream: str) -> Path:
        return self._dir / f"{stream}.jsonl"

    def _load_meta(self) -> None:
        try:
            meta = json.loads((self._dir / "meta.json").read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return
        self._started_at = meta.get("started_at")
        self._gaps = list(meta.get("gaps") or [])
        cond2event: dict[str, str] = {}
        title: dict[str, str] = {}
        date: dict[str, str | None] = {}
        ids: list[str] = []
        markets: set[str] = set()
        yes: dict[str, str] = {}
        asset_cond: dict[str, str] = {}
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
                cond2event[cond] = eid
                markets.add(cond)
                conds.append(cond)
                tokens = [str(t) for t in (market.get("clobTokenIds") or [])]
                if tokens:
                    yes[cond] = tokens[0]  # [0] = YES token (probability of the outcome)
                for tok in tokens:
                    asset_cond[tok] = cond
            event_conds[eid] = conds
        self._cond2event, self._event_title, self._event_date = cond2event, title, date
        self._event_ids, self._markets_total = ids, markets
        self._market_yes, self._asset_cond, self._event_conds = yes, asset_cond, event_conds

    def _load_labels(self) -> None:
        """Outcome labels (e.g. 'Brazil', 'Draw') per condition id, from the matches file.

        Best-effort: meta.json carries the markets but not the human label, so we pull
        ``groupItemTitle`` from the discovery file. Missing file -> labels fall back to
        a short condition id.
        """
        try:
            data = json.loads(self._matches_file.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return
        labels: dict[str, str] = {}
        for match in data:
            for market in match.get("moneyline_markets") or []:
                cond = market.get("conditionId")
                if cond:
                    labels[cond] = market.get("groupItemTitle") or cond
        self._market_label = labels

    def _load_registry(self) -> None:
        """Load the persisted run registry (mtime-cached), and fold in its labels.

        Best-effort: a missing/garbage file leaves an empty registry, so the admin
        degrades to meta-only (exactly today's behaviour). The registry supersedes
        ``_load_labels`` when present (its ``groupItemTitle`` covers finished matches),
        but ``_load_labels`` stays as the fallback when the file is absent.
        """
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
        if self._registry.labels:
            # Registry labels win (cover finished matches); keep any meta-only labels.
            self._market_label = {**self._market_label, **self._registry.labels}

    def _book_event(self, raw: dict[str, Any]) -> str | None:
        """Attribute a book record to its event: current meta first, then the registry.

        The registry fallback is what credits FINISHED matches' records (their
        conditionIds left ``meta.events`` when they rolled out) during the same book
        scan the reader already performs — no extra pass.
        """
        market = str(raw.get("market"))
        return self._cond2event.get(market) or self._registry.cond2event.get(market)

    def _consume(self, stream: str, event_of: Callable[[dict[str, Any]], str | None]) -> None:
        path = self._file(stream)
        try:
            size = path.stat().st_size
        except OSError:
            return
        off = self._offsets.get(stream, 0)
        if off > size:  # file rotated/truncated — restart this stream's tally
            off = 0
            self._counts[stream] = 0
            for ev in self._by_event.values():
                ev.pop(stream, None)
        try:
            with open(path, "rb") as handle:
                handle.seek(off)
                data = handle.read(self._max_read)  # bounded; a big backlog drains over ticks
        except OSError:
            return
        nl = data.rfind(b"\n")
        if nl == -1:
            return  # no complete new line in this chunk yet (records are << the cap)
        self._offsets[stream] = off + nl + 1
        for line in data[: nl + 1].decode("utf-8", "replace").splitlines():
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            raw = rec.get("raw") or {}
            self._counts[stream] = self._counts.get(stream, 0) + 1
            ev = event_of(raw)
            self._peek.append(
                {
                    "ts": rec.get("ts_recv"),
                    "stream": stream,
                    "kind": raw.get("event_type") if stream == "book" else raw.get("type"),
                    "eid": ev,
                    "title": self._event_title.get(ev) if ev else None,
                }
            )
            if ev:
                bucket = self._by_event.setdefault(ev, {})
                bucket[stream] = bucket.get(stream, 0) + 1
                ts = rec.get("ts_recv")
                if ts and (ev not in self._last_ts or ts > self._last_ts[ev]):
                    self._last_ts[ev] = ts
            if stream == "book":
                market = raw.get("market")
                if market:
                    self._markets_seen.add(str(market))
                self._apply_book(raw, rec.get("ts_recv"))

    def _apply_book(self, raw: dict[str, Any], ts: str | None) -> None:
        """Reconstruct the per-token L2 book / last trade / price series from a record."""
        et = raw.get("event_type")
        if et == "book":  # full snapshot replaces the token's book
            asset = str(raw.get("asset_id"))
            self._book[asset] = {
                "bids": {
                    str(lvl.get("price")): str(lvl.get("size"))
                    for lvl in raw.get("bids") or []
                    if str(lvl.get("size")) != "0"
                },
                "asks": {
                    str(lvl.get("price")): str(lvl.get("size"))
                    for lvl in raw.get("asks") or []
                    if str(lvl.get("size")) != "0"
                },
            }
        elif et == "price_change":  # per-level delta (size 0 removes the level)
            for change in raw.get("price_changes") or []:
                asset = str(change.get("asset_id"))
                side = "bids" if change.get("side") == "BUY" else "asks"
                book = self._book.setdefault(asset, {"bids": {}, "asks": {}})
                price, size = str(change.get("price")), str(change.get("size"))
                if size == "0":
                    book[side].pop(price, None)
                else:
                    book[side][price] = size
        elif et == "last_trade_price":
            asset = str(raw.get("asset_id"))
            self._last_trade[asset] = {
                "price": raw.get("price"),
                "size": raw.get("size"),
                "side": raw.get("side"),
                "ts": ts,
            }
            try:
                price = float(raw.get("price"))
            except (TypeError, ValueError):
                return
            self._price_hist.setdefault(asset, deque(maxlen=self._price_history)).append(
                (ts, price)
            )

    def update(self) -> None:
        """Refresh meta + labels, ingest newly-appended records, refresh live snapshots.

        Runs in a worker thread (``asyncio.to_thread``), so all state mutation is held
        under ``self._lock`` — except ``_systemctl()`` (a subprocess up to 5 s), which
        is fetched first, OUTSIDE the lock, so it can never make a request wait on it.
        """
        recorder = self._systemctl()  # subprocess — never under the lock
        with self._lock:
            self._load_meta()
            self._load_labels()
            self._load_registry()
            self._consume("book", self._book_event)
            self._consume("comments", _comment_event)
            self._recorder_cache = recorder
            self._compute_rates()

    def _compute_rates(self) -> None:
        """records/sec per stream and per active event, over the gap since the last tick."""
        now_t = self._mono()
        if self._rate_prev is not None:
            prev_t, prev_counts, prev_by_event = self._rate_prev
            dt = now_t - prev_t
            if dt > 0:
                by_stream = {
                    s: round(max(0.0, (self._counts.get(s, 0) - prev_counts.get(s, 0)) / dt), 3)
                    for s in self._counts
                }
                by_event: dict[str, dict[str, float]] = {}
                for eid, counts in self._by_event.items():
                    prev = prev_by_event.get(eid, {})
                    ev_rates = {
                        s: round(max(0.0, (counts.get(s, 0) - prev.get(s, 0)) / dt), 3)
                        for s in counts
                    }
                    if any(v > 0 for v in ev_rates.values()):  # only events ticking now
                        by_event[eid] = ev_rates
                self._rates = {
                    "by_stream": by_stream,
                    "by_event": by_event,
                    "window_s": round(dt, 2),
                }
        self._rate_prev = (
            now_t,
            dict(self._counts),
            {eid: dict(counts) for eid, counts in self._by_event.items()},
        )

    # -- scan checkpoint (resume forward instead of re-draining the log) ----- #
    #
    # The cumulative scan state (offsets + counts + per-event counts + last-seen
    # timestamps + markets seen) lives only in RAM, so a restart rebuilds it from byte
    # 0 — minutes of re-drain over a multi-GB book.jsonl, during which last_record_age_s
    # tracks the READ position and the dashboard wrongly shows every match stale.
    # Persisting that state lets a restart RESUME reading forward from the saved offsets.
    # The on-demand L2 book reconstruction (_book/_last_trade/_price_hist) is deliberately
    # NOT persisted — it self-heals from the next full ``book`` snapshot after a resume.

    def _file_identity(self, stream: str) -> dict[str, int] | None:
        """``{dev, ino, size}`` for a stream's file, or None if it can't be stat'd.

        dev/ino pin the checkpoint to the SAME underlying file (a rotate/replace gets a
        new inode -> the checkpoint is discarded); size lets load reject an offset past
        EOF (a truncation).
        """
        try:
            st = self._file(stream).stat()
        except OSError:
            return None
        return {"dev": st.st_dev, "ino": st.st_ino, "size": st.st_size}

    def _checkpoint_snapshot(self) -> dict[str, Any]:
        """Build the on-disk checkpoint payload. MUST run under ``self._lock`` so the
        offsets and the aggregates are a consistent snapshot of the SAME scan position.

        Offsets are always at a complete-line boundary (``_consume`` never advances past
        the last newline), so a resume can never split or double-count a record.
        """
        return {
            "schema": _CHECKPOINT_SCHEMA,
            "saved_at": self._now(),
            # Advisory only (diagnostics) — NOT an adoption gate. The recorder appends to
            # the SAME book.jsonl across its own restarts while rewriting started_at, so a
            # mismatch does not mean the prefix changed; file identity below is the real
            # integrity guard. Gating resume on started_at would re-drain on every co-restart.
            "run": {"started_at": self._started_at},
            "files": {s: ident for s in self._offsets if (ident := self._file_identity(s))},
            "offsets": dict(self._offsets),
            "counts": dict(self._counts),
            "by_event": {eid: dict(c) for eid, c in self._by_event.items()},
            "last_ts": dict(self._last_ts),
            "markets_seen": sorted(self._markets_seen),
        }

    def save_checkpoint(self) -> bool:
        """Atomically persist the scan state so a restart resumes forward. Returns True
        iff a checkpoint was written.

        A no-op when checkpointing is disabled or before the first meta load (no run
        identity yet to validate a resume against). The consistent snapshot is taken
        under the lock; the disk write (temp + ``os.replace``, 0600) happens OFF the lock
        so a slow disk never stalls ``update()`` or a read endpoint. Best-effort: a write
        failure is logged, never raised — losing a checkpoint just means the next start
        re-drains, exactly today's behaviour.
        """
        if self._checkpoint_file is None:
            return False
        with self._lock:
            if self._started_at is None:
                return False
            payload = self._checkpoint_snapshot()
        # Per-pid temp name so a second writer can never O_TRUNC ours mid-write; the
        # os.replace then publishes atomically (a reader never sees a torn file).
        tmp = self._checkpoint_file.with_name(f"{self._checkpoint_file.name}.{os.getpid()}.tmp")
        try:
            self._checkpoint_file.parent.mkdir(parents=True, exist_ok=True)
            data = json.dumps(payload).encode("utf-8")
            fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            try:
                view = memoryview(data)
                while view:  # POSIX permits a short write — loop until all bytes land
                    view = view[os.write(fd, view) :]
                os.fsync(fd)  # durable before publish, so a hard reboot can't expose a torn file
            finally:
                os.close(fd)
            os.replace(tmp, self._checkpoint_file)
            self._fsync_dir(self._checkpoint_file.parent)  # make the rename itself durable
        except OSError:
            # Never leave the temp behind (e.g. ENOSPC mid-write); best-effort cleanup.
            with contextlib.suppress(OSError):
                os.unlink(tmp)
            logger.warning(
                "reader checkpoint persist failed (%s)", self._checkpoint_file, exc_info=True
            )
            return False
        return True

    @staticmethod
    def _fsync_dir(path: Path) -> None:
        """fsync a directory so a rename within it is durable. Best-effort: a directory
        fd can't be opened/fsync'd on Windows, so this is a harmless no-op there."""
        try:
            dfd = os.open(path, os.O_RDONLY)
        except OSError:
            return
        try:
            os.fsync(dfd)
        except OSError:
            pass
        finally:
            os.close(dfd)

    def _checkpoint_is_current(self, data: dict[str, Any]) -> bool:
        """Whether a parsed checkpoint may be trusted for THIS run's CURRENT files.

        Two integrity gates (caller holds the lock):

        - FILE IDENTITY: every stream with a saved offset > 0 must still resolve to the
          SAME underlying file (st_dev/st_ino) whose size still covers the offset — so the
          append-only prefix we already counted is byte-for-byte unchanged. A rotate/
          replace (new inode) or truncation (size < offset) -> reject. ``started_at`` is
          deliberately NOT a gate: the recorder appends to the same book.jsonl across its
          OWN restarts while rewriting meta.json's started_at, and that grown prefix is
          still valid to resume — gating on it would re-drain on every VM reboot / co-restart.

        - OFFSET<->COUNT CONSISTENCY: any stream carrying adopted aggregate state (a
          nonzero stream count, a per-event bucket, or markets seen) MUST sit on such a
          validated positive offset. Otherwise resume would re-read that stream from byte 0
          and double-count on top of the adopted totals -> reject.

        An offset of 0 with no state resumes a stream from scratch, which is always safe.
        """
        files = data.get("files") if isinstance(data.get("files"), dict) else {}
        offsets = data.get("offsets") if isinstance(data.get("offsets"), dict) else {}
        # Streams whose (dev, ino, size) authorize resuming from a positive saved offset.
        validated: set[str] = set()
        for stream, raw_off in offsets.items():
            try:
                off = int(raw_off)
            except (TypeError, ValueError):
                return False
            if off <= 0:
                continue
            ident = self._file_identity(stream)
            if ident is None or ident["size"] < off:
                return False  # file gone, rotated to empty, truncated, or offset past EOF
            saved = files.get(stream)
            if not isinstance(saved, dict) or (saved.get("dev"), saved.get("ino")) != (
                ident["dev"],
                ident["ino"],
            ):
                return False  # different underlying file (rotated/replaced)
            validated.add(str(stream))
        # Every stream carrying adopted state must rest on a validated positive offset.
        stateful: set[str] = set()
        try:
            for stream, count in (data.get("counts") or {}).items():
                if int(count) > 0:
                    stateful.add(str(stream))
            for bucket in (data.get("by_event") or {}).values():
                for stream, count in (bucket or {}).items():
                    if int(count) > 0:
                        stateful.add(str(stream))
        except (TypeError, ValueError, AttributeError):
            return False  # malformed counts -> safe re-drain
        if data.get("markets_seen"):
            stateful.add("book")  # markets are only ever seen while scanning the book stream
        return stateful <= validated

    def load_checkpoint(self) -> bool:
        """Resume the scan from a persisted checkpoint instead of re-draining from 0.

        Validates schema + per-stream file identity/size + offset<->count consistency
        BEFORE adopting anything (see :meth:`_checkpoint_is_current`); ANY problem (corrupt,
        stale schema, rotated/truncated/replaced file, offset past EOF, or counts without a
        matching offset) discards the checkpoint and leaves the reader at offset 0 for a
        full, safe re-drain. Returns True iff adopted.

        Restores the offsets and the aggregates computed up to them. The rate baseline is
        reset (the next tick re-establishes it and emits no rate, like a cold start — a
        persisted *monotonic* deadline is meaningless after a restart). The L2 book
        reconstruction is intentionally left empty; it re-heals from new ``book`` records.
        """
        if self._checkpoint_file is None:
            return False
        try:
            data = json.loads(self._checkpoint_file.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, UnicodeDecodeError):
            return False
        if not isinstance(data, dict) or data.get("schema") != _CHECKPOINT_SCHEMA:
            return False
        with self._lock:
            if not self._checkpoint_is_current(data):
                return False
            try:
                offsets = {str(s): int(o) for s, o in (data.get("offsets") or {}).items()}
                counts = {str(s): int(c) for s, c in (data.get("counts") or {}).items()}
                by_event = {
                    str(eid): {str(s): int(n) for s, n in (c or {}).items()}
                    for eid, c in (data.get("by_event") or {}).items()
                }
                last_ts = {str(eid): str(ts) for eid, ts in (data.get("last_ts") or {}).items()}
                markets_seen = {str(m) for m in (data.get("markets_seen") or [])}
            except (TypeError, ValueError, AttributeError):
                return False  # malformed payload -> safe full re-drain
            self._offsets = offsets
            self._counts = {"comments": 0, "book": 0, **counts}
            self._by_event = by_event
            self._last_ts = last_ts
            self._markets_seen = markets_seen
            self._rate_prev = None  # fresh baseline; first tick emits no rate (cold-start-like)
        return True

    def match_view(self, event_id: str) -> dict[str, Any]:
        """Reconstructed preview for one match: each outcome's book, last trade, price line."""
        event_id = str(event_id)
        with self._lock:
            markets: list[dict[str, Any]] = []
            # Registry fallback so a FINISHED match (gone from meta.events) still previews.
            conds = self._event_conds.get(event_id) or self._registry.event_conds.get(event_id, [])
            for cond in conds:
                token = self._market_yes.get(cond) or self._registry.market_yes.get(cond, "")
                book = self._book.get(token, {"bids": {}, "asks": {}})
                bids = sorted(
                    ((float(p), float(s)) for p, s in book["bids"].items()), reverse=True
                )[:8]
                asks = sorted((float(p), float(s)) for p, s in book["asks"].items())[:8]
                best_bid = bids[0][0] if bids else None
                best_ask = asks[0][0] if asks else None
                if best_bid is not None and best_ask is not None:
                    mid: float | None = round((best_bid + best_ask) / 2, 4)
                else:
                    mid = best_bid if best_bid is not None else best_ask
                hist = list(self._price_hist.get(token, ()))
                if len(hist) > 60:  # downsample for the sparkline
                    step = len(hist) // 60 + 1
                    hist = hist[::step]
                markets.append(
                    {
                        "conditionId": cond,
                        "label": self._market_label.get(cond, cond[:10]),
                        "mid": mid,
                        "best_bid": best_bid,
                        "best_ask": best_ask,
                        "bids": [{"price": round(p, 4), "size": round(s, 2)} for p, s in bids],
                        "asks": [{"price": round(p, 4), "size": round(s, 2)} for p, s in asks],
                        "last_trade": self._last_trade.get(token),
                        "price_hist": [{"t": t, "p": round(p, 4)} for t, p in hist],
                    }
                )
            return {
                "event_id": event_id,
                "title": self._registry.title.get(event_id)
                or self._event_title.get(event_id, event_id),
                "date": self._registry.date.get(event_id) or self._event_date.get(event_id),
                # A finished match's reconstructed book is frozen at its last record, not live.
                "historical": event_id not in set(self._event_ids),
                "markets": markets,
            }

    def extractable_event_ids(self) -> list[str]:
        """Finished matches (rolled out of the open set) that have recorded data.

        These are the immutable, downloadable matches the background extractor should
        pre-build (a finished match never gets new records, so its slice is stable).
        """
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

    def caught_up(self) -> bool:
        """True once the book scan is within one chunk of EOF (post-restart drain done).

        The extractor waits for this so its full-file scan never piles CPU onto the
        bounded catch-up re-scan after a restart.
        """
        try:
            size = self._file("book").stat().st_size
        except OSError:
            return True
        with self._lock:
            off = self._offsets.get("book", 0)
        return (size - off) <= self._max_read

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
            freshest = max(self._last_ts.values(), default=None)
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

    def live(self) -> dict[str, Any]:
        """Poll-based live view: records/sec, the most-recent records, and recent gaps.

        Everything here is read straight from state the :meth:`update` loop already
        maintains — no file I/O, so it is cheap to poll every couple of seconds.
        """
        with self._lock:
            freshest = max(self._last_ts.values(), default=None)
            return {
                "rates": self._rates,
                "recent": list(self._peek),
                "gaps": self._gaps[-25:],
                "freshest_age_s": self._age_s(freshest),
                "as_of": self._now(),
            }

    def matches(self) -> list[dict[str, Any]]:
        """Every match in the run — finished + open — in STABLE schedule order.

        Lists the registry (all matches ever recorded) unioned with the current open
        set, so finished/rolled-out matches stay selectable for download. Ordered by
        scheduled ``date`` (then ``event_id``), NOT recency — so rows never jump as
        live counts tick. ``status`` adds ``finished``; open-set membership overrides
        the registry's ``closed`` flag (a reopened match never shows finished).
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
        falling back to the reader's cached ``meta`` snapshot — so the gate works even
        before the first poll. Registry entries win on overlap (richer identity); a
        brand-new fixture present only in meta is still added. Each entry carries
        ``markets[{conditionId, clobTokenIds}]`` so the download can resolve a finished
        match's conditionIds for filtering.
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
