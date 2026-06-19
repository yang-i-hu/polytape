#!/usr/bin/env python3
"""List every FIFA World Cup *match* (win/tie/lose) event from Polymarket's Gamma API.

A World Cup match is an event titled ``"Team A vs. Team B"`` (slug ``fifwc-...``)
whose markets are the 3-way moneyline — three binary Yes/No markets:

* ``Will <Team A> win ...?``      -> Team A
* ``Will <Team B> win ...?``      -> Team B
* ``Will <A> vs. <B> end in a draw?`` -> Draw

Tournament/group/outright events (``World Cup Winner``, ``Group A Winner``, ...)
are *not* matches — their markets carry no ``sportsMarketType``. We keep only
events that have at least one ``moneyline`` market and emit just those markets.

Read-only and unauthenticated. Pages the whole ``fifa-world-cup`` tag (open and,
by default, closed too) with a polite delay, de-dupes by event id, and writes a
JSON file plus a printed summary.

Usage::

    python scripts/list_wc_matches.py [--out wc_matches.json] [--open-only]
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

GAMMA = "https://gamma-api.polymarket.com"
TAG_SLUG = "fifa-world-cup"  # tag id 102232 (the 2026 World Cup)
USER_AGENT = "polytape-wc-discovery (read-only)"
PAGE = 100
PAGE_DELAY = 0.25


def _get(path: str, params: dict[str, object]) -> object:
    url = f"{GAMMA}{path}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(
        url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=40) as resp:
        return json.load(resp)


def _as_list(payload: object) -> list[dict]:
    if isinstance(payload, list):
        return [e for e in payload if isinstance(e, dict)]
    if isinstance(payload, dict):
        data = payload.get("data")
        return [e for e in data if isinstance(e, dict)] if isinstance(data, list) else []
    return []


def _parse_json_array(raw: object) -> list:
    """Gamma encodes ``outcomes``/``clobTokenIds``/``outcomePrices`` as JSON strings."""
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            return []
    return raw if isinstance(raw, list) else []


def fetch_all_events(closed: bool) -> list[dict]:
    """Page every event under the FIFA World Cup tag for the given closed state."""
    out: list[dict] = []
    offset = 0
    while True:
        batch = _as_list(
            _get(
                "/events",
                {
                    "tag_slug": TAG_SLUG,
                    "closed": str(closed).lower(),
                    "limit": PAGE,
                    "offset": offset,
                },
            )
        )
        if not batch:
            break
        out.extend(batch)
        if len(batch) < PAGE:
            break
        offset += PAGE
        time.sleep(PAGE_DELAY)
    return out


def is_match_event(ev: dict) -> bool:
    return any(m.get("sportsMarketType") == "moneyline" for m in (ev.get("markets") or []))


def _slug_date(slug: str | None) -> str | None:
    """Trailing ``YYYY-MM-DD`` in a ``fifwc-...-2026-06-19`` slug, if present."""
    if not slug:
        return None
    tail = slug.rsplit("-", 3)[-3:]
    if len(tail) == 3 and tail[0].isdigit() and len(tail[0]) == 4:
        return "-".join(tail)
    return None


def extract_match(ev: dict) -> dict:
    moneyline = [m for m in (ev.get("markets") or []) if m.get("sportsMarketType") == "moneyline"]
    markets = [
        {
            "question": m.get("question"),
            "groupItemTitle": m.get("groupItemTitle"),
            "conditionId": m.get("conditionId"),
            "outcomes": _parse_json_array(m.get("outcomes")),
            "outcomePrices": _parse_json_array(m.get("outcomePrices")),
            "clobTokenIds": _parse_json_array(m.get("clobTokenIds")),
            "closed": m.get("closed"),
        }
        for m in moneyline
    ]
    return {
        "event_id": str(ev.get("id")),
        "title": (ev.get("title") or "").strip(),
        "slug": ev.get("slug"),
        "match_date": _slug_date(ev.get("slug")),
        "gameStartTime": ev.get("gameStartTime"),
        "startDate": ev.get("startDate"),
        "endDate": ev.get("endDate"),
        "closed": bool(ev.get("closed")),
        "active": bool(ev.get("active")),
        "moneyline_markets": markets,
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="List World Cup match (win/tie/lose) events.")
    ap.add_argument(
        "--out", default="wc_matches.json", help="Output JSON path (default: wc_matches.json)"
    )
    ap.add_argument(
        "--open-only", action="store_true", help="Only open events (skip closed/resolved)"
    )
    args = ap.parse_args(argv)

    try:
        raw = fetch_all_events(closed=False)
        if not args.open_only:
            raw += fetch_all_events(closed=True)
    except urllib.error.URLError as exc:
        print(f"error fetching from Gamma: {exc}", file=sys.stderr)
        return 1

    by_id: dict[str, dict] = {}
    for ev in raw:
        if is_match_event(ev):
            by_id[str(ev.get("id"))] = ev
    matches = [extract_match(ev) for ev in by_id.values()]
    matches.sort(key=lambda m: (m["match_date"] or "9999", m["slug"] or ""))

    with open(args.out, "w", encoding="utf-8", newline="\n") as fh:
        json.dump(matches, fh, ensure_ascii=False, indent=2)
        fh.write("\n")

    n_open = sum(1 for m in matches if not m["closed"])
    n_closed = len(matches) - n_open
    n_markets = sum(len(m["moneyline_markets"]) for m in matches)
    print(f"FIFA World Cup match events (win/tie/lose): {len(matches)}")
    print(f"  open/upcoming: {n_open}   closed/resolved: {n_closed}")
    print(f"  moneyline markets total: {n_markets}   -> {args.out}")
    print()
    print(f"{'date':<12} {'status':<8} {'mkts':>4}  match")
    print("-" * 64)
    for m in matches:
        status = "closed" if m["closed"] else ("active" if m["active"] else "open")
        n_mkt = len(m["moneyline_markets"])
        print(f"{(m['match_date'] or '?'):<12} {status:<8} {n_mkt:>4}  {m['title']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
