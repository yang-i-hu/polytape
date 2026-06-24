"""Stateful capture store: one background tailer that owns reconstructed state.

A :class:`CaptureStore` wraps a :class:`~polytape.viewer.reader.CaptureReader` and
runs a single background thread that incrementally tails ``book.jsonl``, folds new
envelopes through the canonical :class:`~polytape.viewer.reconstruct.Reconstructor`,
and maintains the derived structures the API serves:

* per-asset "warm" :class:`OrderBook` (the latest state),
* a per-asset list of book-affecting changes plus a sparse **keyframe index**
  (deep-copied books) so ``book_as_of(T)`` replays only a bounded tail,
* a downsampled mid/spread time-series and a recent-trades ring,
* SSE subscriber queues that new events are fanned out to.

All shared mutable state is guarded by one lock; request threads call the
thread-safe query methods (``meta``/``assets``/``book_as_of``/``series``/``trades``)
which copy small results out under the lock. The SSE socket writes happen in the
server threads, draining per-subscriber queues outside the lock.
"""

from __future__ import annotations

import bisect
import logging
import queue
import threading
from datetime import datetime
from pathlib import Path
from typing import Any

from polytape.envelope import iso_to_datetime
from polytape.viewer import api, metrics
from polytape.viewer.book import OrderBook
from polytape.viewer.reader import CaptureReader
from polytape.viewer.reconstruct import (
    Reconstructor,
    apply_change_to_book,
    is_book_change,
    normalize_book_event,
)

logger = logging.getLogger("polytape.viewer.store")

FRAME_DEPTH = 50  # ladder depth carried in live SSE state frames (covers the UI's max)
_MAX_TRADES = 5000  # per-asset recent-trades ring cap (bounds live-capture memory)


class Subscriber:
    """An SSE client's frame queue, optionally filtered to one asset."""

    __slots__ = ("asset_id", "queue")

    def __init__(self, asset_id: str | None, maxsize: int) -> None:
        self.asset_id = asset_id
        self.queue: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=maxsize)


class _Asset:
    """Per-asset accumulated state."""

    __slots__ = ("book", "changes", "keyframes", "kf_dts", "trades", "series", "kf_since")

    def __init__(self, book: OrderBook) -> None:
        self.book = book
        # book-affecting changes, in file order: (seq, ts, change)
        self.changes: list[tuple[int, str, dict[str, Any]]] = []
        # sparse keyframe index: (change_index, seq, ts, OrderBook copy)
        self.keyframes: list[tuple[int, int, str, OrderBook]] = []
        self.kf_dts: list[datetime] = []  # parallel parsed ts for bisect
        self.trades: list[tuple[int, str, float | None, float | None, str, Any]] = []
        # downsampled-on-read series points: (ts, mid, bid, ask, spread, micro)
        self.series: list[tuple[str, Any, Any, Any, Any, Any]] = []
        self.kf_since = 0  # changes appended since the last keyframe


class CaptureStore:
    """Owns the tailer thread and the reconstructed state for one capture dir."""

    def __init__(
        self,
        event_dir: Path,
        *,
        keyframe_every: int = 250,
        poll_interval: float = 0.25,
        sse_queue_size: int = 2000,
    ) -> None:
        self.event_dir = Path(event_dir)
        self.event_id = (
            self.event_dir.name[len("event-") :]
            if self.event_dir.name.startswith("event-")
            else self.event_dir.name
        )
        self._reader = CaptureReader(self.event_dir)
        self._keyframe_every = max(1, keyframe_every)
        self._poll_interval = poll_interval
        self._sse_queue_size = sse_queue_size

        self._lock = threading.RLock()
        self._assets: dict[str, _Asset] = {}
        self._recon = Reconstructor()
        self._meta: dict[str, Any] | None = None
        self._labels: dict[str, dict[str, Any]] = {}
        self._subscribers: set[Subscriber] = set()
        self._offset = 0
        self._seq = 0
        self._first_ts: str | None = None
        self._last_ts: str | None = None
        self._live = True
        self._eof_sent = False

        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    # -- lifecycle ---------------------------------------------------------- #

    def start(self) -> None:
        if self._thread is not None:
            return
        # Prime meta + a first read synchronously so the very first request sees data.
        self._poll_once()
        self._thread = threading.Thread(
            target=self._run, name=f"polytape-tailer-{self.event_id}", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self._poll_once()
            except Exception:  # never let the tailer die silently
                logger.exception("viewer tailer poll failed for %s", self.event_id)
            self._stop.wait(self._poll_interval)

    # -- ingest ------------------------------------------------------------- #

    def _poll_once(self) -> None:
        meta = self._reader.read_meta()
        envelopes, new_offset, reset = self._reader.tail(self._offset)
        frames: list[dict[str, Any]] = []
        with self._lock:
            if meta is not None and meta is not self._meta:
                self._meta = meta
                self._rebuild_labels()
            self._live = (self._meta.get("stopped_at") is None) if self._meta else True
            if reset:
                self._reset_state()
            self._offset = new_offset
            for env in envelopes:
                self._seq += 1
                frames.extend(self._ingest(self._seq, env))
            if self._meta and self._meta.get("stopped_at") is not None and not self._eof_sent:
                self._eof_sent = True
                frames.append({"event": "eof", "id": self._seq, "data": {"live": False}})
        for frame in frames:
            self._dispatch(frame)

    def _reset_state(self) -> None:
        self._assets.clear()
        self._recon = Reconstructor()
        self._seq = 0
        self._first_ts = self._last_ts = None
        self._eof_sent = False

    def _asset(self, asset_id: str) -> _Asset:
        a = self._assets.get(asset_id)
        if a is None:
            a = _Asset(self._recon.book(asset_id))
            self._assets[asset_id] = a
        return a

    def _ingest(self, seq: int, env: dict[str, Any]) -> list[dict[str, Any]]:
        ts = env.get("ts_recv")
        raw = env.get("raw")
        if not isinstance(ts, str):
            return []
        if self._first_ts is None:
            self._first_ts = ts
        self._last_ts = ts

        frames: list[dict[str, Any]] = []
        affected: dict[str, str] = {}  # asset_id -> "snapshot" | "price_change"
        for change in normalize_book_event(raw):
            asset_id = change["asset_id"]
            a = self._asset(asset_id)
            kind = change["kind"]
            if is_book_change(change):
                self._recon.apply(change, ts=ts)
                a.changes.append((seq, ts, change))
                self._record_keyframe(a, seq, ts)
                self._record_series(a, ts)
                if kind == "snapshot" or affected.get(asset_id) == "snapshot":
                    affected[asset_id] = "snapshot"
                else:
                    affected[asset_id] = "price_change"
            elif kind == "trade":
                a.trades.append(
                    (seq, ts, change["price"], change["size"], change["side"], change["tx"])
                )
                if len(a.trades) > _MAX_TRADES:
                    del a.trades[: len(a.trades) - _MAX_TRADES]
                frames.append(
                    {
                        "event": "last_trade_price",
                        "id": seq,
                        "data": {
                            "frame": "trade",
                            "asset_id": asset_id,
                            "trade": _trade_dict(ts, change),
                        },
                    }
                )
            elif kind == "tick":
                frames.append(
                    {
                        "event": "tick_size_change",
                        "id": seq,
                        "data": {
                            "frame": "tick",
                            "asset_id": asset_id,
                            "tick_size": change.get("tick_size"),
                        },
                    }
                )

        for asset_id, kind in affected.items():
            state = self._state_frame(asset_id, ts, seq)
            frames.append(
                {
                    "event": "snapshot" if kind == "snapshot" else "price_change",
                    "id": seq,
                    "data": state,
                }
            )
        return frames

    def _record_keyframe(self, a: _Asset, seq: int, ts: str) -> None:
        is_snapshot = a.changes[-1][2]["kind"] == "snapshot"
        a.kf_since += 1
        if is_snapshot or a.kf_since >= self._keyframe_every:
            dt = iso_to_datetime(ts)
            if dt is None:
                return
            a.keyframes.append((len(a.changes) - 1, seq, ts, a.book.copy()))
            a.kf_dts.append(dt)
            a.kf_since = 0

    def _record_series(self, a: _Asset, ts: str) -> None:
        book = a.book
        a.series.append(
            (
                ts,
                metrics.mid(book),
                book.best_bid(),
                book.best_ask(),
                metrics.spread(book),
                metrics.microprice(book),
            )
        )

    def _state_frame(self, asset_id: str, ts: str, seq: int) -> dict[str, Any]:
        book = self._asset(asset_id).book
        return api.book_state(
            asset_id=asset_id,
            label=self.label_for(asset_id),
            book=book,
            as_of=ts,
            seq=seq,
            stale_after_gap=self._stale_after_gap(book, ts),
            book_unseeded=book.reset_ts is None and not book.is_empty(),
            depth=FRAME_DEPTH,
        )

    def _dispatch(self, frame: dict[str, Any]) -> None:
        data = frame.get("data")
        asset = data.get("asset_id") if isinstance(data, dict) else None
        with self._lock:
            subs = list(self._subscribers)
        for sub in subs:
            if sub.asset_id is None or asset is None or sub.asset_id == asset:
                try:
                    sub.queue.put_nowait(frame)
                except queue.Full:
                    pass  # slow client; it reconciles via /api/book on reconnect

    # -- labels / assets ---------------------------------------------------- #

    def _rebuild_labels(self) -> None:
        labels: dict[str, dict[str, Any]] = {}
        meta = self._meta or {}
        markets = (meta.get("event") or {}).get("markets") or []
        for market in markets:
            tokens = [str(t) for t in (market.get("clobTokenIds") or [])]
            for i, token in enumerate(tokens):
                labels[token] = _label_entry(token, i, tokens, market.get("conditionId"))
        if not labels:
            tokens = [str(t) for t in (meta.get("clob_token_ids") or [])]
            for i, token in enumerate(tokens):
                labels[token] = _label_entry(token, i, tokens, None)
        self._labels = labels

    def label_for(self, asset_id: str) -> str:
        entry = self._labels.get(asset_id)
        if entry and entry.get("label"):
            return entry["label"]
        return asset_id[:8] + "…" if len(asset_id) > 9 else asset_id

    def assets(self) -> list[dict[str, Any]]:
        with self._lock:
            seen = set(self._assets)
            order = list(self._labels)
            for token in seen:
                if token not in self._labels:
                    order.append(token)
            out = []
            for token in order:
                entry = self._labels.get(token) or _label_entry(token, -1, [], None)
                row = dict(entry)
                row["present"] = token in seen
                out.append(row)
            return out

    # -- thread-safe queries ------------------------------------------------ #

    def meta(self) -> dict[str, Any] | None:
        with self._lock:
            return self._meta

    def is_live(self) -> bool:
        with self._lock:
            return self._live

    def last_seq(self) -> int:
        with self._lock:
            return self._seq

    def time_range(self) -> tuple[str | None, str | None]:
        with self._lock:
            return self._first_ts, self._last_ts

    def _known(self, asset_id: str) -> bool:
        return asset_id in self._assets or asset_id in self._labels

    def _book_gaps(self) -> list[dict[str, Any]]:
        meta = self._meta or {}
        return [g for g in meta.get("gaps", []) if g.get("stream") in (None, "book")]

    def _stale_after_gap(self, book: OrderBook, as_of: str | None) -> bool:
        if as_of is None:
            return False
        at_dt = iso_to_datetime(as_of)
        if at_dt is None:
            return False
        reset_dt = iso_to_datetime(book.reset_ts) if book.reset_ts else None
        for gap in self._book_gaps():
            dis = iso_to_datetime(gap.get("disconnected_at"))
            if dis is not None and dis <= at_dt and (reset_dt is None or reset_dt <= dis):
                return True
        return False

    def book_as_of(self, asset_id: str, at: str | None) -> dict[str, Any] | None:
        with self._lock:
            a = self._assets.get(asset_id)
            if a is None:
                if not self._known(asset_id):
                    return None
                empty = OrderBook()
                return {
                    "book": empty,
                    "seq": 0,
                    "as_of": at,
                    "stale_after_gap": False,
                    "book_unseeded": False,
                }
            at_dt = iso_to_datetime(at) if at else None
            if at_dt is None:
                book = a.book.copy()
                seq, as_of = self._seq if not a.changes else a.changes[-1][0], self._last_ts
            else:
                book, seq, as_of = self._replay_as_of(a, at, at_dt)
            # Staleness is judged against the requested *view* time, not the resolved
            # event time: no events occur during a gap, so the last applied event is
            # always just before it — scrubbing into the gap window must still flag.
            view_ts = at if at_dt is not None else as_of
            return {
                "book": book,
                "seq": seq,
                "as_of": as_of,
                "stale_after_gap": self._stale_after_gap(book, view_ts),
                "book_unseeded": book.reset_ts is None and not book.is_empty(),
            }

    def _replay_as_of(
        self, a: _Asset, at: str, at_dt: datetime
    ) -> tuple[OrderBook, int, str | None]:
        idx = bisect.bisect_right(a.kf_dts, at_dt) - 1
        if idx >= 0:
            change_index, seq, ts, kf_book = a.keyframes[idx]
            book = kf_book.copy()
            start = change_index + 1
            result_seq, result_ts = seq, ts
        else:
            book = OrderBook()
            start = 0
            result_seq, result_ts = 0, None
        for entry_seq, entry_ts, change in a.changes[start:]:
            entry_dt = iso_to_datetime(entry_ts)
            if entry_dt is not None and entry_dt > at_dt:
                break
            # An unparseable mid-stream ts is "no time signal", not a stop: apply it
            # anyway so as-of replay matches the forward warm fold (which is
            # unconditional). Otherwise live and history would silently diverge.
            apply_change_to_book(book, change, ts=entry_ts)
            result_seq, result_ts = entry_seq, entry_ts
        return book, result_seq, result_ts if result_ts is not None else at

    def series(
        self, asset_id: str, frm: str | None, to: str | None, max_points: int
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]] | None:
        with self._lock:
            a = self._assets.get(asset_id)
            gaps = [
                {"from": g.get("disconnected_at"), "to": g.get("reconnected_at")}
                for g in self._book_gaps()
            ]
            if a is None:
                return ([], gaps) if self._known(asset_id) else None
            frm_dt = iso_to_datetime(frm) if frm else None
            to_dt = iso_to_datetime(to) if to else None
            selected = []
            for point in a.series:
                pt_dt = iso_to_datetime(point[0])
                if frm_dt and pt_dt and pt_dt < frm_dt:
                    continue
                if to_dt and pt_dt and pt_dt > to_dt:
                    continue
                selected.append(point)
            points = _downsample(selected, max(1, max_points))
            return [_series_point(p) for p in points], gaps

    def trades(self, asset_id: str, before: str | None, limit: int) -> list[dict[str, Any]] | None:
        with self._lock:
            a = self._assets.get(asset_id)
            if a is None:
                return [] if self._known(asset_id) else None
            before_dt = iso_to_datetime(before) if before else None
            rows = a.trades
            if before_dt is not None:
                rows = [r for r in rows if (iso_to_datetime(r[1]) or before_dt) <= before_dt]
            rows = rows[-max(1, limit) :]
            return [
                {"ts": ts, "price": _r(price, 6), "size": _r(size, 4), "side": side, "tx": tx}
                for _seq, ts, price, size, side, tx in reversed(rows)
            ]

    # -- SSE subscription --------------------------------------------------- #

    def subscribe(self, asset_id: str | None = None) -> Subscriber:
        sub = Subscriber(asset_id, self._sse_queue_size)
        with self._lock:
            self._subscribers.add(sub)
        return sub

    def unsubscribe(self, sub: Subscriber) -> None:
        with self._lock:
            self._subscribers.discard(sub)


def _label_entry(token: str, index: int, tokens: list[str], market: str | None) -> dict[str, Any]:
    outcome = {0: "YES", 1: "NO"}.get(index)
    if outcome is None and index >= 0:
        outcome = f"OUTCOME{index}"
    complement = tokens[1 - index] if index in (0, 1) and len(tokens) == 2 else None
    return {
        "asset_id": token,
        "market": market,
        "outcome": outcome,
        "label": outcome or (token[:8] + "…" if len(token) > 9 else token),
        "complement_asset_id": complement,
    }


def _trade_dict(ts: str, change: dict[str, Any]) -> dict[str, Any]:
    return {
        "ts": ts,
        "price": _r(change.get("price"), 6),
        "size": _r(change.get("size"), 4),
        "side": change.get("side"),
        "tx": change.get("tx"),
    }


def _series_point(point: tuple[str, Any, Any, Any, Any, Any]) -> dict[str, Any]:
    ts, mid, bid, ask, spread, micro = point
    return {
        "ts": ts,
        "mid": _r(mid, 6),
        "bid": _r(bid, 6),
        "ask": _r(ask, 6),
        "spread": _r(spread, 6),
        "micro": _r(micro, 6),
    }


def _downsample(points: list, max_points: int) -> list:
    if len(points) <= max_points:
        return points
    stride = (len(points) + max_points - 1) // max_points
    sampled = points[::stride]
    if sampled and sampled[-1] is not points[-1]:
        sampled.append(points[-1])
    return sampled


def _r(value: Any, ndigits: int) -> Any:
    return round(value, ndigits) if isinstance(value, (int, float)) else value
