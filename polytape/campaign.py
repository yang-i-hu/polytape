"""Campaign discovery — the PURE selection rules for a multi-family recorder campaign.

A *campaign spec* (``deploy/campaign.json``) names the market families to record:

* ``sports`` — per league tag (``mlb``, ``nfl``, ``epl``, ...): which game-market types
  to keep (``moneyline`` / ``spreads`` / ``totals``) and whether to keep **every** line
  (``"lines": "all"``) or only the **main** line per type (``"lines": "main"`` — the market
  of that type whose ``outcomePrices`` are closest to 0.50/0.50; ties go to the lowest
  market id, i.e. the line that was listed first). The moneyline is always kept in full
  (soccer's 3-way moneyline is three Yes/No markets).
* ``ladders`` — the hourly crypto strike ladders, one Gamma event per asset and ET hour
  (``bitcoin-above-on-september-3-2026-4pm-et``, ~20 strike markets, live ~80 minutes).

Everything in this module is pure: no network and no clock. Gamma payloads and ``now``
are passed in, so every rule is unit-testable against fixture payloads. The network
wrapper (paging Gamma, slug lookups, the CLI) lives in ``scripts/list_campaign_events.py``.

Output contract — one entry per event to record (a superset of the ``wc_matches.json``
matches-file contract the recorder already consumes via ``--matches-file``)::

    {
      "event_id": "930715", "title": "...", "slug": "mlb-nyy-sd-2026-09-04",
      "match_date": "2026-09-04", "closed": false, "active": true,
      "startDate": "...", "endDate": "...",
      "gameStartTime": "2026-09-05T01:40:00Z",      # UTC ISO-8601, normalized
      "family": "sports:mlb",                        # or "ladder:bitcoin"
      "record_markets": [                            # ONLY the markets to record
        {"id": "3973269", "conditionId": "0x...", "clobTokenIds": ["...", "..."],
         "sportsMarketType": "moneyline", "line": null, "question": "...",
         "outcomes": ["New York Yankees", "San Diego Padres"],
         "outcomePrices": ["0.545", "0.455"], "groupItemTitle": null, "closed": false}
      ]
    }
"""

from __future__ import annotations

import json
import math
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone, tzinfo
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from polytape.admin.registry import slug_date

#: Max CLOB token ids per websocket shard. Keep in sync with
#: ``polytape.streams.clob._SHARD_CAP`` (pinned by a test).
SHARD_CAP = 180
MONEYLINE = "moneyline"
LINE_MODES = ("all", "main")
MONTHS = (
    "january",
    "february",
    "march",
    "april",
    "may",
    "june",
    "july",
    "august",
    "september",
    "october",
    "november",
    "december",
)


class _USEastern(tzinfo):
    """US Eastern time with the post-2007 DST rule, used ONLY when the host has no
    IANA tz database (bare Windows without ``tzdata``): DST runs from 02:00 EST on the
    second Sunday of March (07:00Z) to 02:00 EDT on the first Sunday of November (06:00Z)."""

    _EST = timedelta(hours=-5)
    _EDT = timedelta(hours=-4)

    @staticmethod
    def _bounds_utc(year: int) -> tuple[datetime, datetime]:
        march = datetime(year, 3, 1)
        start = march + timedelta(days=(6 - march.weekday()) % 7 + 7, hours=7)
        november = datetime(year, 11, 1)
        end = november + timedelta(days=(6 - november.weekday()) % 7, hours=6)
        return start, end

    def _is_dst_local(self, dt: datetime | None) -> bool:
        if dt is None:
            return False
        start_utc, end_utc = self._bounds_utc(dt.year)
        local = dt.replace(tzinfo=None)
        start_local, end_local = start_utc + self._EST, end_utc + self._EDT
        if end_local - timedelta(hours=1) <= local < end_local:  # the repeated 1am hour
            return dt.fold == 0
        return start_local <= local < end_local

    def utcoffset(self, dt: datetime | None) -> timedelta:
        return self._EDT if self._is_dst_local(dt) else self._EST

    def dst(self, dt: datetime | None) -> timedelta:
        return timedelta(hours=1) if self._is_dst_local(dt) else timedelta(0)

    def tzname(self, dt: datetime | None) -> str:
        return "EDT" if self._is_dst_local(dt) else "EST"

    def fromutc(self, dt: datetime) -> datetime:
        start_utc, end_utc = self._bounds_utc(dt.year)
        naive = dt.replace(tzinfo=None)
        if start_utc <= naive < end_utc:
            return dt + self._EDT
        local = dt + self._EST
        return local.replace(fold=1) if end_utc <= naive < end_utc + timedelta(hours=1) else local


def eastern() -> tzinfo:
    """``America/New_York`` from the tz database, else the built-in rule fallback."""
    try:
        return ZoneInfo("America/New_York")
    except ZoneInfoNotFoundError:  # pragma: no cover - depends on the host's tz database
        return _USEastern()


# --------------------------------------------------------------------------- #
# Spec
# --------------------------------------------------------------------------- #


@dataclass(frozen=True, slots=True)
class SportsSpec:
    """One league family: the Gamma tag, the market types to keep, and the line mode."""

    tag: str
    types: tuple[str, ...]
    lines: str = "main"


@dataclass(frozen=True, slots=True)
class LadderSpec:
    """The hourly crypto strike ladders: assets and how many hours ahead to include."""

    assets: tuple[str, ...]
    hours_ahead: int = 2


@dataclass(frozen=True, slots=True)
class CampaignSpec:
    """A validated campaign spec (see :func:`load_spec`)."""

    run_name: str = "campaign"
    lookahead_h: float = 72.0
    lookback_h: float = 8.0
    sports: tuple[SportsSpec, ...] = ()
    ladders: LadderSpec | None = None


def _str_list(raw: Any, what: str) -> tuple[str, ...]:
    if not isinstance(raw, list) or not all(isinstance(x, str) and x.strip() for x in raw):
        raise ValueError(f"{what} must be a list of non-empty strings, got {raw!r}")
    return tuple(dict.fromkeys(x.strip() for x in raw))


def _hours(raw: Any, what: str, default: float) -> float:
    if raw is None:
        return default
    if isinstance(raw, bool) or not isinstance(raw, int | float) or raw < 0:
        raise ValueError(f"{what} must be a non-negative number of hours, got {raw!r}")
    return float(raw)


def load_spec(obj: Any) -> CampaignSpec:
    """Validate a campaign spec object (parsed JSON) into a :class:`CampaignSpec`.

    The moneyline is always added to each sport's ``types`` (it is recorded in full
    regardless of the line mode). Raises :class:`ValueError` on anything malformed.
    """
    if not isinstance(obj, dict):
        raise ValueError("campaign spec must be a JSON object")
    run_name = obj.get("run_name", "campaign")
    if not isinstance(run_name, str) or not run_name.strip():
        raise ValueError(f"run_name must be a non-empty string, got {run_name!r}")
    sports: list[SportsSpec] = []
    raw_sports = obj.get("sports") or []
    if not isinstance(raw_sports, list):
        raise ValueError("sports must be a list")
    for item in raw_sports:
        if not isinstance(item, dict):
            raise ValueError(f"each sports entry must be an object, got {item!r}")
        tag = item.get("tag")
        if not isinstance(tag, str) or not tag.strip():
            raise ValueError(f"sports entry needs a non-empty 'tag': {item!r}")
        types = _str_list(item.get("types") or [MONEYLINE], f"sports[{tag}].types")
        types = (MONEYLINE, *(t for t in types if t != MONEYLINE))
        lines = item.get("lines", "main")
        if lines not in LINE_MODES:
            raise ValueError(f"sports[{tag}].lines must be one of {LINE_MODES}, got {lines!r}")
        sports.append(SportsSpec(tag=tag.strip(), types=types, lines=lines))
    ladders: LadderSpec | None = None
    raw_ladders = obj.get("ladders")
    if raw_ladders is not None:
        if not isinstance(raw_ladders, dict):
            raise ValueError("ladders must be an object")
        assets = _str_list(raw_ladders.get("assets") or [], "ladders.assets")
        hours_ahead = raw_ladders.get("hours_ahead", 2)
        if isinstance(hours_ahead, bool) or not isinstance(hours_ahead, int) or hours_ahead < 0:
            raise ValueError(f"ladders.hours_ahead must be a non-negative int, got {hours_ahead!r}")
        if assets:
            ladders = LadderSpec(assets=assets, hours_ahead=hours_ahead)
    if not sports and ladders is None:
        raise ValueError("campaign spec names no families (empty 'sports' and no 'ladders')")
    return CampaignSpec(
        run_name=run_name.strip(),
        lookahead_h=_hours(obj.get("lookahead_h"), "lookahead_h", 72.0),
        lookback_h=_hours(obj.get("lookback_h"), "lookback_h", 8.0),
        sports=tuple(sports),
        ladders=ladders,
    )


# --------------------------------------------------------------------------- #
# Gamma payload helpers
# --------------------------------------------------------------------------- #

_TZ_SHORT = re.compile(r"([+-]\d{2})$")
_FRACTION = re.compile(r"\.(\d+)")


def parse_gamma_time(value: Any) -> datetime | None:
    """Parse the timestamp formats Gamma emits into an aware UTC datetime.

    Seen on the wire: ``"2026-09-05 01:40:00+00"`` (market ``gameStartTime``),
    ``"2026-09-03T18:40:15Z"``, ``"2026-01-21T20:45:03.607Z"`` and
    ``"...T03:08:09.276545Z"``. Naive values are taken as UTC. Returns ``None`` for
    anything missing or unparseable (never raises) — Python 3.10's ``fromisoformat`` is
    strict, so the string is normalized first.
    """
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    if " " in text and "T" not in text:
        text = text.replace(" ", "T", 1)
    if text[-1] in "Zz":
        text = text[:-1] + "+00:00"
    if "T" in text:
        text = _TZ_SHORT.sub(r"\1:00", text)  # "+00" -> "+00:00"
    frac = _FRACTION.search(text)
    if frac:
        digits = (frac.group(1) + "000000")[:6]  # 3.10 accepts only 3 or 6 digits
        text = f"{text[: frac.start()]}.{digits}{text[frac.end() :]}"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def iso_utc(when: datetime | None) -> str | None:
    """``YYYY-MM-DDTHH:MM:SSZ`` for an aware datetime (``None`` passes through)."""
    if when is None:
        return None
    return when.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_json_array(raw: Any) -> list:
    """Gamma encodes ``outcomes`` / ``outcomePrices`` / ``clobTokenIds`` as JSON strings."""
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            return []
    return raw if isinstance(raw, list) else []


def token_ids(market: Mapping[str, Any]) -> list[str]:
    """The market's CLOB token ids (empty if it has none — nothing to subscribe to)."""
    return [str(t) for t in parse_json_array(market.get("clobTokenIds")) if str(t)]


def market_start(event: Mapping[str, Any], market: Mapping[str, Any]) -> datetime | None:
    """A market's game start: its ``gameStartTime``, else the event's, else ``startDate``."""
    return (
        parse_gamma_time(market.get("gameStartTime"))
        or parse_gamma_time(event.get("gameStartTime"))
        or parse_gamma_time(event.get("startDate"))
    )


def in_window(when: datetime | None, now: datetime, lookback_h: float, lookahead_h: float) -> bool:
    """Whether ``when`` lies in ``[now - lookback_h, now + lookahead_h]`` (inclusive)."""
    if when is None:
        return False
    return now - timedelta(hours=lookback_h) <= when <= now + timedelta(hours=lookahead_h)


def market_id_key(market: Mapping[str, Any]) -> tuple[int, int | str]:
    """Sort key for "lowest market id": numeric ids compare as numbers, others after."""
    raw = str(market.get("id") or "")
    return (0, int(raw)) if raw.isdigit() else (1, raw)


def price_distance(market: Mapping[str, Any]) -> float:
    """How far the market's ``outcomePrices`` sit from 0.50/0.50 (``inf`` if unpriced)."""
    prices: list[float] = []
    for raw in parse_json_array(market.get("outcomePrices")):
        try:
            prices.append(float(raw))
        except (TypeError, ValueError):
            return math.inf
    if not prices:
        return math.inf
    return max(abs(p - 0.5) for p in prices)


def pick_main_line(markets: Sequence[Mapping[str, Any]]) -> Mapping[str, Any] | None:
    """The MAIN line among markets of one type: closest to 0.50/0.50, ties -> lowest id."""
    if not markets:
        return None
    return min(markets, key=lambda m: (price_distance(m), market_id_key(m)))


# --------------------------------------------------------------------------- #
# Sports selection
# --------------------------------------------------------------------------- #


def select_sports_markets(
    event: Mapping[str, Any],
    sport: SportsSpec,
    *,
    now: datetime,
    lookback_h: float,
    lookahead_h: float,
    open_only: bool = True,
) -> list[dict[str, Any]]:
    """The raw Gamma markets of ``event`` to record under ``sport``.

    Candidates are the event's markets whose ``sportsMarketType`` is in ``sport.types``,
    that carry CLOB token ids, are open (unless ``open_only`` is off) and whose game
    start lies in the campaign window. ``lines == "all"`` keeps every candidate;
    ``"main"`` keeps every moneyline market plus one main line per other type.
    The result is ordered by the spec's type order, then market id.
    """
    candidates: list[dict[str, Any]] = []
    for market in event.get("markets") or []:
        if not isinstance(market, dict) or market.get("sportsMarketType") not in sport.types:
            continue
        if open_only and market.get("closed"):
            continue
        if not token_ids(market):
            continue
        if not in_window(market_start(event, market), now, lookback_h, lookahead_h):
            continue
        candidates.append(market)
    if sport.lines == "all":
        chosen = list(candidates)
    else:
        chosen = [m for m in candidates if m.get("sportsMarketType") == MONEYLINE]
        for mtype in sport.types:
            if mtype == MONEYLINE:
                continue
            main = pick_main_line([m for m in candidates if m.get("sportsMarketType") == mtype])
            if main is not None:
                chosen.append(dict(main))
    order = {t: i for i, t in enumerate(sport.types)}
    chosen.sort(key=lambda m: (order.get(m.get("sportsMarketType"), len(order)), market_id_key(m)))
    return chosen


def market_record(market: Mapping[str, Any]) -> dict[str, Any]:
    """The compact ``record_markets`` entry for one Gamma market."""
    return {
        "id": str(market.get("id") or ""),
        "conditionId": market.get("conditionId"),
        "clobTokenIds": token_ids(market),
        "sportsMarketType": market.get("sportsMarketType"),
        "line": market.get("line"),
        "question": market.get("question"),
        "outcomes": parse_json_array(market.get("outcomes")),
        "outcomePrices": parse_json_array(market.get("outcomePrices")),
        "groupItemTitle": market.get("groupItemTitle"),
        "closed": bool(market.get("closed")),
    }


def _slug_day(slug: str | None) -> str | None:
    """The shared ``slug_date`` heuristic, accepted only when it is a real calendar date
    (a ladder slug's ``...-2026-4pm-et`` tail would otherwise pass as ``2026-4pm-et``)."""
    raw = slug_date(slug)
    if raw is None:
        return None
    try:
        date.fromisoformat(raw)
    except ValueError:
        return None
    return raw


def event_entry(
    event: Mapping[str, Any],
    family: str,
    markets: Sequence[Mapping[str, Any]],
    *,
    game_start: datetime | None,
) -> dict[str, Any]:
    """One output entry (matches-file contract + ``family`` + ``record_markets``)."""
    slug = event.get("slug")
    return {
        "event_id": str(event.get("id") or ""),
        "title": (event.get("title") or "").strip(),
        "slug": slug,
        "match_date": _slug_day(slug) or (game_start.date().isoformat() if game_start else None),
        "closed": bool(event.get("closed")),
        "active": bool(event.get("active")),
        "startDate": event.get("startDate"),
        "endDate": event.get("endDate"),
        "gameStartTime": iso_utc(game_start),
        "family": family,
        "record_markets": [market_record(m) for m in markets],
    }


def sports_entry(
    event: Mapping[str, Any],
    sport: SportsSpec,
    *,
    now: datetime,
    lookback_h: float,
    lookahead_h: float,
    open_only: bool = True,
) -> dict[str, Any] | None:
    """The output entry for a game event under ``sport``, or ``None`` if nothing to record."""
    markets = select_sports_markets(
        event, sport, now=now, lookback_h=lookback_h, lookahead_h=lookahead_h, open_only=open_only
    )
    if not markets:
        return None
    starts = [s for s in (market_start(event, m) for m in markets) if s is not None]
    game_start = min(starts) if starts else None
    return event_entry(event, f"sports:{sport.tag}", markets, game_start=game_start)


def sports_entries(
    events_by_tag: Mapping[str, Sequence[Mapping[str, Any]]],
    spec: CampaignSpec,
    now: datetime,
    *,
    open_only: bool = True,
) -> list[dict[str, Any]]:
    """Entries for every sports family in ``spec``, de-duplicated by event id.

    An event listed under two tags is emitted once, under the first tag (in spec order)
    that selects at least one market from it.
    """
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for sport in spec.sports:
        for event in events_by_tag.get(sport.tag, ()):
            if not isinstance(event, dict):
                continue
            event_id = str(event.get("id") or "")
            if not event_id or event_id in seen:
                continue
            if open_only and event.get("closed"):
                continue
            entry = sports_entry(
                event,
                sport,
                now=now,
                lookback_h=spec.lookback_h,
                lookahead_h=spec.lookahead_h,
                open_only=open_only,
            )
            if entry is not None:
                seen.add(event_id)
                out.append(entry)
    return out


# --------------------------------------------------------------------------- #
# Crypto strike ladders
# --------------------------------------------------------------------------- #


def ladder_hours(now: datetime, hours_ahead: int) -> list[datetime]:
    """The resolution hours to look up: ``now-1h .. now+hours_ahead`` (floored, UTC)."""
    base = now.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
    return [base + timedelta(hours=h) for h in range(-1, hours_ahead + 1)]


def ladder_slug(asset: str, when: datetime) -> str:
    """``<asset>-above-on-<month>-<day>-<year>-<H>(am|pm)-et`` for the ET hour of ``when``."""
    local = when.astimezone(eastern())
    hour12 = local.hour % 12 or 12
    ampm = "am" if local.hour < 12 else "pm"
    return f"{asset}-above-on-{MONTHS[local.month - 1]}-{local.day}-{local.year}-{hour12}{ampm}-et"


def ladder_slugs(spec: LadderSpec, now: datetime) -> list[tuple[str, str]]:
    """``(asset, slug)`` pairs to look up, unique and in asset-then-time order."""
    out: dict[tuple[str, str], None] = {}
    for asset in spec.assets:
        for when in ladder_hours(now, spec.hours_ahead):
            out[(asset, ladder_slug(asset, when))] = None
    return list(out)


def ladder_entry(
    event: Mapping[str, Any] | None, asset: str, *, open_only: bool = True
) -> dict[str, Any] | None:
    """The output entry for a ladder event (all its strike markets), or ``None``."""
    if not isinstance(event, dict) or not event.get("id"):
        return None
    if open_only and event.get("closed"):
        return None
    markets = [
        m
        for m in event.get("markets") or []
        if isinstance(m, dict) and token_ids(m) and not (open_only and m.get("closed"))
    ]
    if not markets:
        return None
    markets.sort(key=market_id_key)
    return event_entry(
        event, f"ladder:{asset}", markets, game_start=parse_gamma_time(event.get("startDate"))
    )


# --------------------------------------------------------------------------- #
# Output ordering + summary
# --------------------------------------------------------------------------- #


def entry_tokens(entry: Mapping[str, Any]) -> list[str]:
    """All CLOB token ids of an entry's ``record_markets``, order-preserving, de-duped."""
    ordered: dict[str, None] = {}
    for market in entry.get("record_markets") or []:
        for token in market.get("clobTokenIds") or []:
            ordered[str(token)] = None
    return list(ordered)


def sort_entries(entries: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Schedule order: game start (unknown last), then family, slug, event id."""
    return sorted(
        (dict(e) for e in entries),
        key=lambda e: (
            e.get("gameStartTime") is None,
            e.get("gameStartTime") or "",
            e.get("family") or "",
            e.get("slug") or "",
            e.get("event_id") or "",
        ),
    )


def count_shards(token_groups: Sequence[Sequence[str]], cap: int = SHARD_CAP) -> int:
    """How many CLOB websocket shards the recorder would open for these per-event groups.

    Mirrors ``polytape.streams.clob.shard_tokens``: whole groups are packed greedily
    into buckets of ``<= cap`` tokens, in order, never splitting a group. Raises if one
    group alone exceeds ``cap`` (the recorder would refuse it too).
    """
    shards = 0
    current = 0
    for group in token_groups:
        size = len([t for t in group if str(t)])
        if not size:
            continue
        if size > cap:
            raise ValueError(f"event token group of {size} exceeds shard cap {cap}")
        if current and current + size > cap:
            shards += 1
            current = 0
        current += size
    return shards + (1 if current else 0)


def summarize(entries: Sequence[Mapping[str, Any]], cap: int = SHARD_CAP) -> list[dict[str, Any]]:
    """Per-family rows (events, markets, tokens, shards) plus a ``TOTAL`` row.

    The total's shard count packs the entries in the given order — the order the
    recorder subscribes them in — so it is what the recorder will actually open.
    """
    families: dict[str, list[Mapping[str, Any]]] = {}
    for entry in entries:
        families.setdefault(str(entry.get("family") or "?"), []).append(entry)
    rows: list[dict[str, Any]] = []
    for family in sorted(families):
        members = families[family]
        rows.append(
            {
                "family": family,
                "events": len(members),
                "markets": sum(len(e.get("record_markets") or []) for e in members),
                "tokens": sum(len(entry_tokens(e)) for e in members),
                "shards": count_shards([entry_tokens(e) for e in members], cap),
            }
        )
    rows.append(
        {
            "family": "TOTAL",
            "events": len(entries),
            "markets": sum(len(e.get("record_markets") or []) for e in entries),
            "tokens": sum(len(entry_tokens(e)) for e in entries),
            "shards": count_shards([entry_tokens(e) for e in entries], cap),
        }
    )
    return rows


def format_summary(rows: Sequence[Mapping[str, Any]], cap: int = SHARD_CAP) -> str:
    """Render :func:`summarize` rows as a fixed-width table."""
    width = max([len("family"), *(len(str(r["family"])) for r in rows)])
    shards = f"shards@{cap}"
    lines = [f"{'family':<{width}} {'events':>7} {'markets':>8} {'tokens':>7} {shards:>11}"]
    lines.append("-" * len(lines[0]))
    for row in rows:
        if row["family"] == "TOTAL":
            lines.append("-" * len(lines[0]))
        lines.append(
            f"{row['family']:<{width}} {row['events']:>7} {row['markets']:>8} "
            f"{row['tokens']:>7} {row['shards']:>11}"
        )
    return "\n".join(lines)
