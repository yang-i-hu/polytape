#!/usr/bin/env python3
"""Canonical (event, recorded-market-set) key of a campaign events file.

``polytape-refresh.sh`` decides whether to restart the recorder by comparing the
output of this script for the freshly discovered file against the installed one.
Restarting costs a capture gap, so the comparison must be exactly as sensitive as
the recorder's subscription set and no more:

* it CHANGES when an event enters/leaves the open set, or when the set of markets
  to record inside an event changes (e.g. the main-line spread moved to another
  market, or a strike-ladder market was added);
* it does NOT change on cosmetic churn (titles, outcome prices, timestamps, list
  order) — an event-id-only comparison also ignored those, but it missed the
  market-level changes; a whole-file diff would restart on every price tick.

Input contract (a JSON list of event objects, as written by
``scripts/list_campaign_events.py``; the recorder reads the same file):

* ``event_id`` (fallback ``id``) — the Polymarket event id;
* the markets to record — the first list present among ``record_markets``,
  ``markets``, ``moneyline_markets`` (override the key with ``--markets-key`` or
  ``$POLYTAPE_MARKETS_KEY``); each element is a market object carrying
  ``conditionId`` / ``condition_id`` / ``id``, or a bare id string;
* ``closed`` (optional) — with ``--open-only`` closed events are skipped, mirroring
  the recorder's own ``--open-only``.

Output: one line per event, ``<event_id><TAB><market ids sorted, comma-joined>``,
lines sorted by event id — deterministic and order-invariant, so two files with
the same recorded set compare byte-equal. ``--summary`` prints
``events=N markets=M`` instead. An unreadable / malformed file exits 1 with nothing
on stdout, which the caller treats as "no valid discovery" (a no-op).

Standard library only: it runs from the deploy tree, not from the package.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

MARKET_LIST_KEYS = ("record_markets", "markets", "moneyline_markets")
MARKET_ID_KEYS = ("conditionId", "condition_id", "id")


def market_ids(entry: dict[str, Any], list_key: str | None = None) -> list[str]:
    """Sorted, de-duplicated ids of the markets ``entry`` says to record.

    Uses ``list_key`` if given, else the first of :data:`MARKET_LIST_KEYS` that is
    present. Elements may be market objects (id taken from the first of
    :data:`MARKET_ID_KEYS` that is set) or bare id strings.
    """
    keys = (list_key,) if list_key else MARKET_LIST_KEYS
    for key in keys:
        value = entry.get(key)
        if not isinstance(value, list):
            continue
        ids: set[str] = set()
        for market in value:
            if isinstance(market, dict):
                for id_key in MARKET_ID_KEYS:
                    mid = market.get(id_key)
                    if mid not in (None, ""):
                        ids.add(str(mid))
                        break
            elif isinstance(market, (str, int)) and str(market):
                ids.add(str(market))
        return sorted(ids)
    return []


def event_set(
    events: list[Any], *, open_only: bool = False, list_key: str | None = None
) -> list[tuple[str, list[str]]]:
    """``[(event_id, [market ids...]), ...]`` sorted by event id.

    Non-object entries and entries without an id are ignored. A duplicated event id
    contributes the union of its market sets (a well-formed file never repeats one).
    """
    out: dict[str, set[str]] = {}
    for entry in events:
        if not isinstance(entry, dict):
            continue
        if open_only and entry.get("closed"):
            continue
        raw_id = entry.get("event_id", entry.get("id"))
        if raw_id in (None, ""):
            continue
        eid = str(raw_id).strip()
        if not eid:
            continue
        out.setdefault(eid, set()).update(market_ids(entry, list_key))
    return [(eid, sorted(mids)) for eid, mids in sorted(out.items())]


def format_lines(rows: list[tuple[str, list[str]]]) -> str:
    return "".join(f"{eid}\t{','.join(mids)}\n" for eid, mids in rows)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="polytape-event-set.py",
        description="Print the canonical (event, recorded-market-set) key of an events file.",
    )
    ap.add_argument("path", help="campaign events JSON file (a list of event objects)")
    ap.add_argument(
        "--open-only", action="store_true", help="skip events whose 'closed' flag is set"
    )
    ap.add_argument(
        "--summary", action="store_true", help="print 'events=N markets=M' instead of the key"
    )
    ap.add_argument(
        "--markets-key",
        default=os.environ.get("POLYTAPE_MARKETS_KEY") or None,
        help="field holding the markets to record (default: first present of "
        f"{', '.join(MARKET_LIST_KEYS)}; env POLYTAPE_MARKETS_KEY)",
    )
    args = ap.parse_args(argv)

    try:
        data = json.loads(Path(args.path).read_text(encoding="utf-8"))
    except (OSError, ValueError, UnicodeDecodeError) as exc:
        print(f"polytape-event-set: cannot read {args.path}: {exc}", file=sys.stderr)
        return 1
    if not isinstance(data, list):
        print(f"polytape-event-set: {args.path} is not a JSON list", file=sys.stderr)
        return 1

    rows = event_set(data, open_only=args.open_only, list_key=args.markets_key)
    if args.summary:
        print(f"events={len(rows)} markets={sum(len(m) for _, m in rows)}")
    else:
        sys.stdout.write(format_lines(rows))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
