"""Cumulative run registry: every World Cup match (finished + open) of the run.

The recorder overwrites ``meta.json`` to the CURRENT open set on each restart, so
a finished match's *identity* (id / title / conditionIds) is lost from
``meta.events`` once the refresh rolls it out — yet its *records* remain in the
append-only ``book.jsonl`` / ``comments.jsonl``. This module recovers the full set
from Gamma discovery (open AND closed events) and persists it, so the admin can
**list** finished matches in schedule order, **count** them (the reader credits
their conditionIds during the book scan it already does), and **download** them
(their conditionIds still resolve to an event).

Two deliberately separated halves so the network never touches request handling:

* :func:`fetch_registry` — synchronous Gamma discovery (``urllib``). Called ONLY
  inside ``asyncio.to_thread`` on the admin's slow timer; its result is persisted.
* :func:`load_registry` / :class:`Registry` — a pure parse of that file into the
  lookup maps the reader and the download path consume. No network. Best-effort: a
  missing/garbage file yields an empty registry, so the admin degrades to
  meta-only (exactly today's behaviour).
"""

from __future__ import annotations

import json
import logging
import os
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger("polytape.admin.registry")

_GAMMA = "https://gamma-api.polymarket.com"
_TAG_SLUG = "fifa-world-cup"  # the 2026 World Cup tag (mirrors scripts/list_wc_matches.py)
_USER_AGENT = "polytape-admin-registry (read-only)"
_PAGE = 100
_PAGE_DELAY = 0.25
_TIMEOUT = 40.0
REGISTRY_SCHEMA = 1


# --------------------------------------------------------------------------- #
# Gamma discovery (network — off the event loop only)
# --------------------------------------------------------------------------- #


def _get(path: str, params: dict[str, Any]) -> Any:
    url = f"{_GAMMA}{path}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(
        url, headers={"User-Agent": _USER_AGENT, "Accept": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=_TIMEOUT) as resp:  # noqa: S310 - https, fixed host
        return json.load(resp)


def _as_list(payload: Any) -> list[dict]:
    if isinstance(payload, list):
        return [e for e in payload if isinstance(e, dict)]
    if isinstance(payload, dict):
        data = payload.get("data")
        return [e for e in data if isinstance(e, dict)] if isinstance(data, list) else []
    return []


def _parse_json_array(raw: Any) -> list:
    """Gamma encodes ``clobTokenIds`` etc. as a JSON-encoded string."""
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            return []
    return raw if isinstance(raw, list) else []


def _slug_date(slug: str | None) -> str | None:
    """Trailing ``YYYY-MM-DD`` of a ``fifwc-...-2026-06-19`` slug, if present."""
    if not slug:
        return None
    tail = slug.rsplit("-", 3)[-3:]
    if len(tail) == 3 and tail[0].isdigit() and len(tail[0]) == 4:
        return "-".join(tail)
    return None


def _is_match_event(ev: dict) -> bool:
    return any(m.get("sportsMarketType") == "moneyline" for m in (ev.get("markets") or []))


def _event_to_entry(ev: dict) -> dict:
    moneyline = [m for m in (ev.get("markets") or []) if m.get("sportsMarketType") == "moneyline"]
    markets = [
        {
            "conditionId": m.get("conditionId"),
            "clobTokenIds": _parse_json_array(m.get("clobTokenIds")),
            "groupItemTitle": m.get("groupItemTitle"),
        }
        for m in moneyline
    ]
    slug = ev.get("slug")
    return {
        "event_id": str(ev.get("id")),
        "title": (ev.get("title") or "").strip(),
        "slug": slug,
        "date": _slug_date(slug),
        "closed": bool(ev.get("closed")),
        "markets": markets,
    }


def _fetch_state(closed: bool) -> list[dict]:
    out: list[dict] = []
    offset = 0
    while True:
        batch = _as_list(
            _get(
                "/events",
                {
                    "tag_slug": _TAG_SLUG,
                    "closed": str(closed).lower(),
                    "limit": _PAGE,
                    "offset": offset,
                },
            )
        )
        if not batch:
            break
        out.extend(batch)
        if len(batch) < _PAGE:
            break
        offset += _PAGE
        time.sleep(_PAGE_DELAY)  # be polite between pages
    return out


def fetch_registry() -> list[dict]:
    """Discover every run match (open + closed) from Gamma — synchronous; off-loop only.

    Returns a list of registry entries. Raises on a hard network error; the caller
    suppresses it and keeps the last good persisted registry.
    """
    raw = _fetch_state(closed=False) + _fetch_state(closed=True)
    by_id: dict[str, dict] = {}
    for ev in raw:
        if _is_match_event(ev):
            by_id[str(ev.get("id"))] = ev
    return [_event_to_entry(ev) for ev in by_id.values()]


def write_registry_atomic(path: str | Path, events: list[dict], *, now_iso: str) -> None:
    """Persist the registry (temp + ``os.replace``). Never call with an empty fetch."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    payload = {"schema": REGISTRY_SCHEMA, "updated_at": now_iso, "events": events}
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp, path)


# --------------------------------------------------------------------------- #
# Pure registry (no network) — the lookup maps reader/download consume
# --------------------------------------------------------------------------- #


def _entry_sort_key(entry: dict) -> tuple:
    return (entry.get("date") is None, entry.get("date") or "", str(entry.get("event_id") or ""))


@dataclass
class Registry:
    """Derived lookup maps over the run's full match set (finished + open)."""

    events: dict[str, dict] = field(default_factory=dict)  # event_id -> entry
    cond2event: dict[str, str] = field(default_factory=dict)  # conditionId -> event_id
    event_conds: dict[str, list[str]] = field(default_factory=dict)
    market_yes: dict[str, str] = field(default_factory=dict)  # conditionId -> YES token
    labels: dict[str, str] = field(default_factory=dict)  # conditionId -> outcome label
    title: dict[str, str] = field(default_factory=dict)
    date: dict[str, str | None] = field(default_factory=dict)
    closed: dict[str, bool] = field(default_factory=dict)
    order: list[str] = field(default_factory=list)  # event_ids, schedule order
    collisions: int = 0

    def add(self, entry: dict) -> None:
        """Index one event entry; a conditionId is kept for its FIRST (earliest) claimant."""
        eid = str(entry.get("event_id") or "")
        if not eid or eid == "None":
            return
        self.events[eid] = entry
        self.title[eid] = entry.get("title") or eid
        self.date[eid] = entry.get("date")
        self.closed[eid] = bool(entry.get("closed"))
        conds: list[str] = self.event_conds.setdefault(eid, [])
        for market in entry.get("markets") or []:
            cond = market.get("conditionId")
            if not cond:
                continue
            cond = str(cond)
            owner = self.cond2event.get(cond)
            if owner is not None and owner != eid:
                # A conditionId claimed by another event: keep the first (we add in
                # schedule order, so that's the earlier-scheduled match). Never last-wins.
                self.collisions += 1
                continue
            self.cond2event[cond] = eid
            if cond not in conds:
                conds.append(cond)
            tokens = [str(t) for t in (market.get("clobTokenIds") or [])]
            if tokens:
                self.market_yes[cond] = tokens[0]  # [0] = YES token
            label = market.get("groupItemTitle")
            if label:
                self.labels[cond] = str(label)


def build_registry(events: list[dict]) -> Registry:
    """Build a :class:`Registry` from entries, indexing in schedule order."""
    reg = Registry()
    ordered = sorted((e for e in events if isinstance(e, dict)), key=_entry_sort_key)
    for entry in ordered:
        reg.add(entry)
    reg.order = [str(e.get("event_id")) for e in ordered if e.get("event_id") is not None]
    return reg


def load_registry(path: str | Path) -> Registry:
    """Parse the persisted registry into a :class:`Registry`; empty on any problem.

    Accepts both ``{"events": [...]}`` and a bare list (so a raw
    ``list_wc_matches.py`` ``extract_match`` dump loads too — its ``match_date`` key
    is normalised to ``date``).
    """
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return Registry()
    events = data.get("events") if isinstance(data, dict) else data
    if not isinstance(events, list):
        return Registry()
    normalised = []
    for e in events:
        if not isinstance(e, dict):
            continue
        if "date" not in e and "match_date" in e:  # tolerate the raw discovery shape
            e = {**e, "date": e.get("match_date")}
        normalised.append(e)
    return build_registry(normalised)


def known_ids(reg_events: dict[str, dict]) -> list[str]:
    """Event ids of a merged registry-events map (used by the download gate)."""
    return list(reg_events.keys())


def cond_to_event(reg_events: dict[str, dict]) -> dict[str, str]:
    """conditionId -> event_id over a merged registry-events map (first claim, by schedule)."""
    out: dict[str, str] = {}
    for entry in sorted(reg_events.values(), key=_entry_sort_key):
        eid = str(entry.get("event_id") or "")
        if not eid:
            continue
        for market in entry.get("markets") or []:
            cond = market.get("conditionId")
            if cond and str(cond) not in out:
                out[str(cond)] = eid
    return out
