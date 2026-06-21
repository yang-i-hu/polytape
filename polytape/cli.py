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
from pathlib import Path

from polytape import __version__
from polytape.config import Config

_LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")

logger = logging.getLogger("polytape")


def build_parser() -> argparse.ArgumentParser:
    """Construct the argument parser for the ``polytape`` command."""
    parser = argparse.ArgumentParser(
        prog="polytape",
        description=(
            "Record Polymarket's public real-time comment (RTDS) and order-book "
            "(CLOB) feeds for a live event to timestamped JSONL. Read-only; never "
            "authenticates and never trades."
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
        "--comments",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Record the RTDS comment stream (use --no-comments to skip).",
    )
    parser.add_argument(
        "--book",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Record the CLOB order-book stream (use --no-book to skip).",
    )
    parser.add_argument(
        "--no-hash",
        action="store_true",
        help="Write usernames/identifiers verbatim instead of salted-hashing them.",
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


def load_matches(path: str, open_only: bool = True) -> tuple[str, ...]:
    """Read event ids from a matches JSON file (e.g. ``wc_matches.json``).

    The file is a list of objects with ``event_id`` and ``closed`` fields (as
    produced by ``scripts/list_wc_matches.py``). With ``open_only`` (default),
    closed/resolved events are skipped. Order-preserving and de-duplicated.
    """
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    ids: list[str] = []
    for match in data:
        if open_only and match.get("closed"):
            continue
        event_id = match.get("event_id")
        if event_id:
            ids.append(str(event_id).strip())
    return tuple(dict.fromkeys(ids))


def config_from_args(args: argparse.Namespace) -> Config:
    """Build a validated :class:`Config` from parsed arguments.

    Raises:
        ValueError: if the argument combination is invalid (propagated from
            :class:`Config` validation, or for a missing/empty event source).
    """
    if args.event_id:
        event_ids = tuple(str(e).strip() for e in args.event_id)
    elif args.matches_file:
        event_ids = load_matches(args.matches_file, args.open_only)
        if not event_ids:
            raise ValueError(f"no matching events found in {args.matches_file}")
    else:
        raise ValueError("one of --event-id or --matches-file is required")
    return Config(
        event_ids=event_ids,
        run_name=args.run_name,
        out_dir=Path(args.out),
        comments=args.comments,
        book=args.book,
        hash_usernames=not args.no_hash,
        market_ids=tuple(args.market_id or ()),
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
        "polytape %s | events=%d (primary=%s) streams=%s out=%s hash=%s dry_run=%s",
        __version__,
        len(config.event_ids),
        config.event_id,
        ",".join(config.enabled_streams),
        config.event_dir,
        config.hash_usernames,
        config.dry_run,
    )
    if config.dry_run:
        from polytape.mock import run_dry_run

        return run_dry_run(config)
    from polytape.app import run_live

    return run_live(config)


if __name__ == "__main__":
    raise SystemExit(main())
