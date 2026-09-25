"""Command-line interface for polytape: argument parsing, validation, logging.

The actual capture pipeline (Gamma resolution, websockets, writer) is wired into
:func:`main` in later build steps; this module is responsible only for turning
``argv`` into a validated :class:`~polytape.config.Config` and configuring logging.
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from collections.abc import Mapping
from pathlib import Path
from typing import NamedTuple

from polytape import __version__
from polytape.config import Config

_LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")

logger = logging.getLogger("polytape")


class MatchSelection(NamedTuple):
    """What a matches file selects: the events to record, plus per-event allow-lists.

    ``event_markets`` holds only the events that carried a non-empty
    ``record_markets`` list (``event_id -> market ids / conditionIds``); every
    other event in ``event_ids`` records all of its markets.
    """

    event_ids: tuple[str, ...]
    event_markets: dict[str, tuple[str, ...]]


def _market_key(value: object) -> str | None:
    """Normalize one id-ish value (``id`` / ``conditionId``) to a string, or ``None``."""
    if isinstance(value, bool):  # bool is an int subclass; never an id
        return None
    if isinstance(value, int):
        return str(value)
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _record_markets(match: dict, event_id: str) -> tuple[str, ...] | None:
    """Read a match's ``record_markets`` allow-list (the campaign contract).

    Entries are objects like ``{"id": "...", "conditionId": "0x...", "clobTokenIds": [...]}``
    — either key suffices; both are kept so resolution can match on whichever it
    finds — or bare id strings. Returns ``None`` when the key is absent or the list
    is empty (record every market, i.e. the ``wc_matches.json`` behaviour), else the
    order-preserving, de-duplicated tuple of ids (possibly empty if no entry was
    usable — the caller decides what that means). A non-list value is a malformed
    file and raises :class:`ValueError`.
    """
    raw = match.get("record_markets")
    if raw is None:
        return None
    if not isinstance(raw, list):
        raise ValueError(
            f"matches file: event {event_id}: record_markets must be a list, "
            f"got {type(raw).__name__}"
        )
    if not raw:
        return None
    ids: dict[str, None] = {}
    for entry in raw:
        keys: list[str | None]
        if isinstance(entry, dict):
            keys = [_market_key(entry.get("id")), _market_key(entry.get("conditionId"))]
        else:
            keys = [_market_key(entry)]
        usable = [k for k in keys if k]
        if not usable:
            logger.warning(
                "matches file: event %s: record_markets entry without id/conditionId ignored: %r",
                event_id,
                entry,
            )
        for key in usable:
            ids[key] = None
    return tuple(ids)


def build_parser() -> argparse.ArgumentParser:
    """Construct the argument parser for the ``polytape`` command."""
    parser = argparse.ArgumentParser(
        prog="polytape",
        description=(
            "Record Polymarket's public real-time order-book (CLOB) feed for a "
            "live event to timestamped JSONL. Read-only; never authenticates and "
            "never trades."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--event-id",
        action="append",
        dest="event_id",
        metavar="ID",
        help="Polymarket Event ID to record (numeric; repeatable for several events).",
    )
    parser.add_argument(
        "--matches-file",
        dest="matches_file",
        metavar="PATH",
        help="JSON file of matches (e.g. wc_matches.json) to record instead of --event-id.",
    )
    parser.add_argument(
        "--open-only",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="With --matches-file, record only events that are not yet closed.",
    )
    parser.add_argument(
        "--run-name",
        dest="run_name",
        metavar="NAME",
        help="Label for a multi-event run; output goes to OUT/run-<name>/.",
    )
    parser.add_argument(
        "--out",
        default="./data",
        metavar="DIR",
        help="Output root directory; data is written to DIR/event-<id>/ (or DIR/run-<name>/).",
    )
    parser.add_argument(
        "--per-match",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Also write per-match files (matches/event-<id>/<stream>.jsonl) alongside "
        "the monolithic backup log (use --no-per-match to write only the monolith).",
    )
    parser.add_argument(
        "--market-id",
        action="append",
        metavar="ID",
        dest="market_id",
        help="Record only this market id (repeatable) instead of all event markets.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Feed synthetic messages through the full pipeline with no network.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=_LOG_LEVELS,
        type=str.upper,
        help="Logging verbosity.",
    )
    parser.add_argument(
        "-V",
        "--version",
        action="version",
        version=f"polytape {__version__}",
    )
    return parser


def load_matches(path: str, open_only: bool = True) -> MatchSelection:
    """Read the events (and per-event market allow-lists) from a matches JSON file.

    The file is a list of objects with ``event_id`` and ``closed`` fields (as
    produced by ``scripts/list_wc_matches.py`` / ``scripts/list_campaign_events.py``).
    With ``open_only`` (default), closed/resolved events are skipped. Event ids are
    order-preserving and de-duplicated.

    A match may carry ``record_markets`` — a list of ``{"id", "conditionId", ...}``
    objects naming the ONLY markets to record for that event (see
    :func:`_record_markets`). Absent or empty means "record every market", which
    keeps ``wc_matches.json`` (no such key) behaving exactly as before. A duplicate
    event entry unions its allow-list with the earlier one; an entry whose
    ``record_markets`` yields no usable id is skipped with a warning rather than
    silently widened to "everything".

    Raises:
        ValueError: if the file is not a JSON list of objects, or a
            ``record_markets`` value is not a list.
    """
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError(f"matches file {path}: expected a JSON list of matches")
    ids: dict[str, None] = {}
    markets: dict[str, tuple[str, ...]] = {}
    for match in data:
        if not isinstance(match, dict):
            raise ValueError(f"matches file {path}: expected match objects, got {match!r}")
        if open_only and match.get("closed"):
            continue
        event_id = match.get("event_id")
        if not event_id:
            continue
        event_id = str(event_id).strip()
        allow = _record_markets(match, event_id)
        if allow is not None and not allow:
            logger.warning(
                "matches file: event %s has record_markets but no usable id; skipping event",
                event_id,
            )
            continue
        ids[event_id] = None
        if allow:
            markets[event_id] = tuple(dict.fromkeys(markets.get(event_id, ()) + allow))
    return MatchSelection(tuple(ids), markets)


def config_from_args(args: argparse.Namespace) -> Config:
    """Build a validated :class:`Config` from parsed arguments.

    Raises:
        ValueError: if the argument combination is invalid (propagated from
            :class:`Config` validation, or for a missing/empty event source).
    """
    event_markets: Mapping[str, tuple[str, ...]] = {}
    if args.event_id:
        event_ids = tuple(str(e).strip() for e in args.event_id)
    elif args.matches_file:
        event_ids, event_markets = load_matches(args.matches_file, args.open_only)
        if not event_ids:
            raise ValueError(f"no matching events found in {args.matches_file}")
    else:
        raise ValueError("one of --event-id or --matches-file is required")
    return Config(
        event_ids=event_ids,
        run_name=args.run_name,
        out_dir=Path(args.out),
        market_ids=tuple(args.market_id or ()),
        event_markets=event_markets,
        per_match=args.per_match,
        dry_run=args.dry_run,
        log_level=args.log_level,
    )


def parse_args(argv: list[str] | None = None) -> Config:
    """Parse ``argv`` into a :class:`Config`.

    On a bad argument or invalid combination this calls ``parser.error``, which
    prints usage to stderr and raises :class:`SystemExit` with code 2.
    """
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return config_from_args(args)
    except ValueError as exc:
        parser.error(str(exc))  # prints usage and raises SystemExit(2)


def setup_logging(level: str) -> None:
    """Configure root logging to stderr with UTC timestamps.

    Idempotent: :func:`logging.basicConfig` is a no-op once handlers exist.
    """
    logging.Formatter.converter = time.gmtime
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )


def main(argv: list[str] | None = None) -> int:
    """Console entry point.

    Args:
        argv: Arguments excluding the program name; defaults to ``sys.argv[1:]``.

    Returns:
        A process exit code (0 on success).
    """
    config = parse_args(argv)
    setup_logging(config.log_level)
    logger.info(
        "polytape %s | events=%d (primary=%s, %d with a market allow-list) streams=%s "
        "out=%s dry_run=%s",
        __version__,
        len(config.event_ids),
        config.event_id,
        len(config.event_markets),
        ",".join(config.enabled_streams),
        config.event_dir,
        config.dry_run,
    )
    if config.dry_run:
        from polytape.mock import run_dry_run

        return run_dry_run(config)
    from polytape.app import run_live

    return run_live(config)


if __name__ == "__main__":
    raise SystemExit(main())
