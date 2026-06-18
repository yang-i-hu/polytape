"""``python -m polytape.monitor`` — launch the dashboard.

Points the monitor at a capture root (default ``./data``, matching the
recorder's default ``--out``), starts a localhost web server, and serves the
dashboard until interrupted.

Monitoring is always read-only. A control plane (start/stop captures from the
UI) is enabled by default *only on a loopback bind* — it spawns processes, so it
stays off when bound to the network unless you pass ``--allow-control``, and can
be turned off entirely with ``--read-only``.
"""

from __future__ import annotations

import argparse
import logging
import sys
import webbrowser

from polytape.monitor.control import RecorderManager
from polytape.monitor.reader import DEFAULT_IDLE_THRESHOLD, CaptureMonitor
from polytape.monitor.server import LOOPBACK_HOSTS, make_server

logger = logging.getLogger("polytape.monitor")


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m polytape.monitor",
        description="Live dashboard for a polytape capture (read-only; optional control).",
    )
    parser.add_argument(
        "--out",
        default="./data",
        help="Capture root to watch (the recorder's --out, or a single event-<id> dir). "
        "Default: ./data",
    )
    parser.add_argument("--host", default="127.0.0.1", help="Bind host. Default: 127.0.0.1")
    parser.add_argument("--port", type=int, default=8787, help="Bind port. Default: 8787")
    parser.add_argument(
        "--idle-threshold",
        type=float,
        default=DEFAULT_IDLE_THRESHOLD,
        help="Seconds without a new message before a running capture is shown as 'idle'.",
    )
    parser.add_argument(
        "--read-only",
        action="store_true",
        help="Disable the start/stop control plane entirely (pure observer).",
    )
    parser.add_argument(
        "--allow-control",
        action="store_true",
        help="Allow control on a non-loopback bind (spawns processes — use with care).",
    )
    parser.add_argument("--open", action="store_true", help="Open the dashboard in a browser.")
    parser.add_argument("--log-level", default="INFO", help="Python logging level. Default: INFO")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    monitor = CaptureMonitor(args.out, idle_threshold=args.idle_threshold)
    manager = RecorderManager(args.out)

    loopback = args.host in LOOPBACK_HOSTS
    control_enabled = not args.read_only and (loopback or args.allow_control)
    # Pin the Host header on a loopback bind (blocks DNS-rebinding); a deliberate
    # non-loopback bind is reached by many hostnames, so don't pin there.
    host_allowlist = LOOPBACK_HOSTS if loopback else None

    try:
        server = make_server(
            monitor,
            host=args.host,
            port=args.port,
            manager=manager,
            control_enabled=control_enabled,
            host_allowlist=host_allowlist,
        )
    except OSError as exc:
        logger.error("could not bind %s:%d (%s)", args.host, args.port, exc)
        return 1

    host, port = server.server_address[0], server.server_address[1]
    url = f"http://{'localhost' if host in ('127.0.0.1', '0.0.0.0') else host}:{port}/"
    if not loopback:
        logger.warning("binding to %s exposes capture volume/timing to the network", args.host)
        if not control_enabled and not args.read_only:
            logger.warning(
                "control disabled on a non-loopback bind; pass --allow-control to enable"
            )
    logger.info("monitoring %s", monitor.out_dir)
    logger.info("control %s", "ENABLED (start/stop captures)" if control_enabled else "disabled")
    logger.info("dashboard at %s  (Ctrl-C to stop)", url)
    if args.open:
        webbrowser.open(url)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("shutting down")
    finally:
        server.shutdown()
        server.server_close()
        running = manager.running_count()
        if running:
            logger.info(
                "%d capture(s) launched here are still recording (the monitor does not stop "
                "them on exit); stop them from the dashboard or their own process",
                running,
            )
    return 0


if __name__ == "__main__":
    sys.exit(main())
