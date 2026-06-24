"""Command-line interface for ``polytape-view`` (separate from the recorder CLI).

Resolves a launch mode into a validated :class:`ViewerConfig`, starts the
:mod:`~polytape.viewer.server`, prints the URL, optionally opens a browser, and
serves until interrupted. The recorder CLI (``polytape``) is untouched.
"""

from __future__ import annotations

import argparse
import logging
import webbrowser
from pathlib import Path

from polytape import __version__
from polytape.cli import setup_logging
from polytape.viewer.config import ViewerConfig
from polytape.viewer.server import create_server

_LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")

logger = logging.getLogger("polytape.viewer")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="polytape-view",
        description=(
            "Replay and live-follow a recorded Polymarket order book in the browser. "
            "Reads a polytape capture directory; never connects to Polymarket."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    source = parser.add_argument_group("capture source (choose one)")
    source.add_argument(
        "--event-dir", metavar="DIR", help="A single capture directory, e.g. ./data/event-80505."
    )
    source.add_argument(
        "--out",
        default="./data",
        metavar="DIR",
        help="Recorder output root (with --event-id), or the root to scan in picker mode.",
    )
    source.add_argument(
        "--event-id", metavar="ID", help="Event id under --out to view (mirrors the recorder)."
    )
    source.add_argument(
        "--data", metavar="DIR", help="A data root to scan for all captures (multi-capture picker)."
    )

    parser.add_argument("--host", default="127.0.0.1", help="Bind address (localhost by default).")
    parser.add_argument("--port", default=8770, type=int, help="TCP port.")
    parser.add_argument(
        "--poll-interval", default=0.25, type=float, metavar="SEC", help="File-tail cadence."
    )
    parser.add_argument(
        "--keyframe-every",
        default=250,
        type=int,
        metavar="N",
        help="Book snapshot cached every N events for fast scrubbing.",
    )
    parser.add_argument("--no-open", action="store_true", help="Do not open a browser on start.")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=_LOG_LEVELS,
        type=str.upper,
        help="Logging verbosity.",
    )
    parser.add_argument("-V", "--version", action="version", version=f"polytape {__version__}")
    return parser


def config_from_args(args: argparse.Namespace) -> ViewerConfig:
    common = dict(
        host=args.host,
        port=args.port,
        poll_interval=args.poll_interval,
        keyframe_every=args.keyframe_every,
        open_browser=not args.no_open,
        log_level=args.log_level,
    )
    if args.event_dir:
        event_dir = Path(args.event_dir).expanduser()
        name = event_dir.name
        event_id = name[len("event-") :] if name.startswith("event-") else name
        return ViewerConfig(
            data_root=event_dir.parent, event_id=event_id, event_dir_override=event_dir, **common
        )
    if args.event_id:
        return ViewerConfig(
            data_root=Path(args.out).expanduser(), event_id=str(args.event_id), **common
        )
    root = Path(args.data or args.out).expanduser()
    return ViewerConfig(data_root=root, event_id=None, **common)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        config = config_from_args(args)
    except ValueError as exc:
        parser.error(str(exc))

    setup_logging(config.log_level)
    httpd, app = create_server(config)
    url = f"http://{config.host}:{config.port}/"
    if config.single_event:
        logger.info("polytape-view %s | viewing %s at %s", __version__, config.event_dir, url)
    else:
        logger.info(
            "polytape-view %s | scanning %s at %s (pick a capture in the UI)",
            __version__,
            config.data_root,
            url,
        )
    if config.open_browser:
        try:
            webbrowser.open(url)
        except Exception:  # headless / no browser — not fatal
            logger.debug("could not open a browser")

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        logger.info("interrupted; shutting down")
    finally:
        httpd.shutdown()
        httpd.server_close()
        app.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
