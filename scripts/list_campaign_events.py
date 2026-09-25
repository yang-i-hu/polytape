#!/usr/bin/env python3
"""List the events and markets a multi-family recorder campaign should record.

Discovery for the open-ended campaign (``deploy/campaign.json``): per league tag the
game markets to keep (moneyline / spreads / totals; every line or only the main line
per type) for games in a lookahead/lookback window, plus the hourly crypto strike
ladders for the current and next few ET hours. All the selection rules are pure and
live in :mod:`polytape.campaign`; this script is the network wrapper + CLI.

Read-only and unauthenticated (public Gamma ``/events``). Pages each tag's OPEN events
fully (the listing is not ordered by game time) with a polite delay, stops at Gamma's
offset cap (HTTP 422) gracefully, looks the ladder events up by slug, de-dupes by event
id, writes a JSON file in the matches-file contract (plus ``family`` and
``record_markets`` — see :mod:`polytape.campaign`) and prints a per-family summary
(every family of the spec, ``0`` rows included). One ``tags: ...`` diagnostic line per
run goes to stderr — open events and pages fetched per tag, and ``TRUNCATED`` when a
listing hit the offset cap — so a typo'd tag, a truncated listing and an off-season
league can be told apart in the journal.

``--previous <installed events file>`` makes the main-line picks sticky (a pick is kept
while it is still an open, quoted candidate — see :func:`polytape.campaign.pick_main_line`);
without it every run re-picks from the live quotes and adjacent lines trading places as
odds move would change the recorded set — and restart the recorder — every few minutes.

Usage::

    python scripts/list_campaign_events.py --spec deploy/campaign.json \\
        --out campaign_events.json [--previous /etc/polytape/campaign_events.json] \\
        [--now 2026-09-03T20:00:00Z] [--no-open-only]
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path

from polytape.campaign import (
    CampaignSpec,
    entry_tokens,
    format_summary,
    ladder_entry,
    ladder_slugs,
    load_spec,
    parse_gamma_time,
    previous_picks,
    sort_entries,
    spec_families,
    sports_entries,
    summarize,
)

GAMMA = "https://gamma-api.polymarket.com"
USER_AGENT = "polytape-campaign-discovery (read-only)"
PAGE = 100
PAGE_DELAY = 0.25
#: Gamma rejects deep offsets (HTTP 422 "offset too large" at 3000); stop before that.
MAX_OFFSET = 2000

Getter = Callable[[str, dict[str, object]], object]


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


def fetch_tag_events(
    tag: str, *, get: Getter | None = None, stats: dict[str, object] | None = None
) -> list[dict]:
    """Page every OPEN event under ``tag`` (newest-listed first).

    Stops on a short page, an empty page, the local offset cap, or Gamma's own cap
    (HTTP 422) — the last two keep whatever was fetched so far rather than failing,
    and are reported as ``truncated`` in ``stats`` (``{events, pages, truncated}``) so
    the caller can say so: the listing is newest-listed first, so a truncation drops
    the OLDEST-listed events — game events listed weeks ahead — first.
    """
    get = get or _get
    out: list[dict] = []
    offset = 0
    pages = 0
    truncated: str | None = None
    while True:
        if offset >= MAX_OFFSET:
            truncated = f"local cap at offset {offset}"
            break
        try:
            batch = _as_list(
                get(
                    "/events",
                    {
                        "tag_slug": tag,
                        "closed": "false",
                        "limit": PAGE,
                        "offset": offset,
                        "order": "startDate",
                        "ascending": "false",
                    },
                )
            )
        except urllib.error.HTTPError as exc:
            if exc.code == 422:  # "offset too large" — Gamma's pagination cap
                truncated = f"Gamma 422 at offset {offset}"
                break
            raise
        pages += 1
        if not batch:
            break
        out.extend(batch)
        if len(batch) < PAGE:
            break
        offset += PAGE
        time.sleep(PAGE_DELAY)
    if stats is not None:
        stats.update({"events": len(out), "pages": pages, "truncated": truncated})
    return out


def fetch_event_by_slug(slug: str, *, get: Getter | None = None) -> dict | None:
    """``GET /events?slug=<slug>`` -> the event object, or ``None`` if Gamma has none."""
    get = get or _get
    try:
        events = _as_list(get("/events", {"slug": slug}))
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None
        raise
    return next((e for e in events if e.get("id")), None)


def format_tag_stats(stats: dict[str, dict[str, object]]) -> str:
    """One journal-friendly line: ``tags: mlb=343/4p nfl=528/6p ... TRUNCATED: cfb (...)``."""
    cells = [f"{tag}={s.get('events', 0)}/{s.get('pages', 0)}p" for tag, s in stats.items()]
    truncated = [f"{tag} ({s['truncated']})" for tag, s in stats.items() if s.get("truncated")]
    line = "tags: " + (" ".join(cells) or "(none)")
    if truncated:
        line += "  TRUNCATED: " + ", ".join(truncated)
    return line


def discover(
    spec: CampaignSpec,
    now: datetime,
    *,
    open_only: bool = True,
    get: Getter | None = None,
    previous: dict[str, dict[str, str]] | None = None,
    tag_stats: dict[str, dict[str, object]] | None = None,
) -> list[dict]:
    """Run the whole discovery for ``spec`` at ``now``; returns unsorted entries.

    ``previous`` (see :func:`polytape.campaign.previous_picks`) keeps last time's
    main-line picks while they are still quoted candidates; ``tag_stats`` collects the
    per-tag paging statistics (``tag -> {events, pages, truncated}``).
    """
    events_by_tag: dict[str, list[dict]] = {}
    for sport in spec.sports:
        if sport.tag not in events_by_tag:
            stats: dict[str, object] = {}
            events_by_tag[sport.tag] = fetch_tag_events(sport.tag, get=get, stats=stats)
            if tag_stats is not None:
                tag_stats[sport.tag] = stats
            time.sleep(PAGE_DELAY)
    entries = sports_entries(events_by_tag, spec, now, open_only=open_only, previous=previous)
    seen = {e["event_id"] for e in entries}
    if spec.ladders is not None:
        for asset, slug in ladder_slugs(spec.ladders, now):
            entry = ladder_entry(
                fetch_event_by_slug(slug, get=get), asset, open_only=open_only, now=now
            )
            if entry is not None and entry["event_id"] not in seen:
                seen.add(entry["event_id"])
                entries.append(entry)
            time.sleep(PAGE_DELAY)
    return entries


def parse_now(text: str) -> datetime:
    """``--now`` for tests: an ISO-8601 instant (``Z`` or offset; naive = UTC)."""
    parsed = parse_gamma_time(text)
    if parsed is None:
        raise ValueError(f"--now must be an ISO-8601 timestamp, got {text!r}")
    return parsed


def load_previous(path: str | None) -> dict[str, dict[str, str]]:
    """The previous discovery's main-line picks from ``path`` (an events file), for
    stickiness. A missing / unreadable / malformed file means no stickiness — it is
    reported on stderr, never fatal (the first run has no previous file)."""
    if not path:
        return {}
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError, UnicodeDecodeError) as exc:
        print(f"previous events file unusable ({exc}); picking main lines afresh", file=sys.stderr)
        return {}
    if not isinstance(data, list):
        print(f"previous events file {path} is not a JSON list; picking afresh", file=sys.stderr)
        return {}
    return previous_picks(data)


def _print_listing(entries: list[dict]) -> None:
    print()
    print(f"{'game start (UTC)':<17} {'family':<18} {'status':<7} {'mkts':>4} {'toks':>5}  event")
    print("-" * 92)
    for entry in entries:
        status = "closed" if entry["closed"] else ("active" if entry["active"] else "open")
        start = (entry.get("gameStartTime") or "?")[:16].replace("T", " ")
        print(
            f"{start:<17} {entry['family']:<18} {status:<7} "
            f"{len(entry['record_markets']):>4} {len(entry_tokens(entry)):>5}  {entry['title']}"
        )


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="List the events + markets a recorder campaign should record (Gamma)."
    )
    ap.add_argument("--spec", required=True, help="Campaign spec JSON (e.g. deploy/campaign.json)")
    ap.add_argument(
        "--out",
        default="campaign_events.json",
        help="Output JSON path (default: campaign_events.json)",
    )
    ap.add_argument(
        "--now",
        default=None,
        metavar="ISO",
        help="Pretend the current time is this ISO-8601 instant (for tests/replays)",
    )
    ap.add_argument(
        "--open-only",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Emit only open events/markets (default); --no-open-only keeps closed ones too",
    )
    ap.add_argument(
        "--previous",
        default=None,
        metavar="EVENTS_JSON",
        help="The currently installed events file: keep its main-line picks while they "
        "are still open, quoted candidates (stickiness); a missing file is fine",
    )
    args = ap.parse_args(argv)

    try:
        spec = load_spec(json.loads(Path(args.spec).read_text(encoding="utf-8")))
        now = parse_now(args.now) if args.now else datetime.now(timezone.utc)
    except (OSError, ValueError) as exc:
        print(f"bad campaign spec / arguments: {exc}", file=sys.stderr)
        return 2
    previous = load_previous(args.previous)

    tag_stats: dict[str, dict[str, object]] = {}
    try:
        entries = sort_entries(
            discover(spec, now, open_only=args.open_only, previous=previous, tag_stats=tag_stats)
        )
    except urllib.error.URLError as exc:
        print(f"error fetching from Gamma: {exc}", file=sys.stderr)
        return 1
    print(format_tag_stats(tag_stats), file=sys.stderr)

    with open(args.out, "w", encoding="utf-8", newline="\n") as fh:
        json.dump(entries, fh, ensure_ascii=False, indent=2)
        fh.write("\n")

    rows = summarize(entries, families=spec_families(spec))
    print(
        f"campaign '{spec.run_name}' @ {now.strftime('%Y-%m-%dT%H:%M:%SZ')}: "
        f"{len(entries)} event(s) -> {args.out}"
    )
    print(format_summary(rows))
    _print_listing(entries)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
