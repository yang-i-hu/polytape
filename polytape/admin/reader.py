"""Read-only aggregation over a polytape run for the admin dashboard.

Turns the recorder's on-disk outputs into the ``/status`` and ``/matches`` views.
Incremental by design: the first :meth:`update` scans each JSONL file in full,
then later calls read only the bytes appended since — so it stays fast as
``book.jsonl`` grows. Everything here is read-only; it never touches the recorder.

Event attribution mirrors the recorder: book records route by top-level ``market``
(condition id, via the meta event map); comments by ``parentEntityID``.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from collections import deque
from collections.abc import Callable
from pathlib import Path
from typing import Any

from polytape.envelope import iso_to_datetime, utc_now_iso


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
        live_window_s: float = 60.0,
        price_history: int = 120,
        now: Callable[[], str] = utc_now_iso,
    ) -> None:
        self._dir = Path(run_dir)
        self._unit = unit
        self._env_file = Path(env_file)
        self._matches_file = Path(matches_file)
        self._live_window = live_window_s
        self._price_history = price_history
        self._now = now
        self._offsets: dict[str, int] = {}
        self._counts: dict[str, int] = {"comments": 0, "book": 0}
        self._by_event: dict[str, dict[str, int]] = {}
        self._last_ts: dict[str, str] = {}
        self._markets_seen: set[str] = set()
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
        # Reconstructed L2 book + last trade + price history, keyed by token (asset_id).
        self._book: dict[str, dict[str, dict[str, str]]] = {}
        self._last_trade: dict[str, dict[str, Any]] = {}
        self._price_hist: dict[str, deque] = {}

    # -- file helpers ------------------------------------------------------- #

    def _file(self, stream: str) -> Path:
        return self._dir / f"{stream}.jsonl"

    def _load_meta(self) -> None:
        try:
            meta = json.loads((self._dir / "meta.json").read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return
        self._started_at = meta.get("started_at")
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
                data = handle.read()
        except OSError:
            return
        nl = data.rfind(b"\n")
        if nl == -1:
            return  # no complete new line yet
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
        """Refresh meta + labels, then ingest any newly-appended JSONL records."""
        self._load_meta()
        self._load_labels()
        self._consume("book", lambda raw: self._cond2event.get(str(raw.get("market"))))
        self._consume("comments", _comment_event)

    def match_view(self, event_id: str) -> dict[str, Any]:
        """Reconstructed preview for one match: each outcome's book, last trade, price line."""
        event_id = str(event_id)
        markets: list[dict[str, Any]] = []
        for cond in self._event_conds.get(event_id, []):
            token = self._market_yes.get(cond, "")
            book = self._book.get(token, {"bids": {}, "asks": {}})
            bids = sorted(((float(p), float(s)) for p, s in book["bids"].items()), reverse=True)[:8]
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
            "title": self._event_title.get(event_id, event_id),
            "date": self._event_date.get(event_id),
            "markets": markets,
        }

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
        freshest = max(self._last_ts.values(), default=None)
        seen = len(self._markets_seen & self._markets_total)
        return {
            "recorder": self._systemctl(),
            "started_at": self._started_at,
            "last_record_age_s": self._age_s(freshest),
            "records": dict(self._counts),
            "open_matches": len(self._event_ids),
            "coverage": {"seen": seen, "total": len(self._markets_total)},
            "disk_percent": self._disk_percent(),
            "heartbeat_armed": self._heartbeat_armed(),
            "as_of": self._now(),
        }

    def matches(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for eid in self._event_ids:
            age = self._age_s(self._last_ts.get(eid))
            if age is None:
                state = "pending"
            elif age <= self._live_window:
                state = "live"
            else:
                state = "quiet"
            rows.append(
                {
                    "event_id": eid,
                    "title": self._event_title.get(eid, eid),
                    "date": self._event_date.get(eid),
                    "counts": self._by_event.get(eid, {}),
                    "last_seen_age_s": age,
                    "status": state,
                }
            )
        # Freshest first; never-seen (pending) sink to the bottom.
        rows.sort(key=lambda r: (r["last_seen_age_s"] is None, r["last_seen_age_s"] or 0.0))
        return rows
