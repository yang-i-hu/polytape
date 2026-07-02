"""``python -m polytape.monitor.demo`` — a live synthetic capture to watch.

Drives realistic, randomized book messages through the **real**
:class:`~polytape.writer.CaptureWriter` (the same sink a live capture uses), so
you can point the dashboard at it and watch throughput, latency, the type mix,
and the occasional simulated disconnect move in real time — with no network and
no live event. Stop it with Ctrl-C; the capture is finalized just like a real one.

This is a convenience for trying the monitor, not part of the recorder.
"""

from __future__ import annotations

import argparse
import logging
import random
import signal
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from polytape.config import STREAM_BOOK, Config
from polytape.mock import synthetic_event
from polytape.writer import CaptureWriter

logger = logging.getLogger("polytape.monitor.demo")

_BOOK_TYPES = (
    ("price_change", 0.78),
    ("book", 0.12),
    ("last_trade_price", 0.08),
    ("tick_size_change", 0.02),
)


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


def _weighted_book_type() -> str:
    r = random.random()
    cumulative = 0.0
    for kind, weight in _BOOK_TYPES:
        cumulative += weight
        if r <= cumulative:
            return kind
    return "price_change"


def _book_message(seq: int) -> dict:
    """A synthetic CLOB message with a server timestamp a little in the past."""
    kind = _weighted_book_type()
    server = datetime.now(timezone.utc) - timedelta(milliseconds=random.randint(40, 600))
    ts = str(int(server.timestamp() * 1000))
    asset = random.choice(["100", "200"])
    price = round(random.uniform(0.05, 0.95), 3)
    if kind == "book":
        return {
            "event_type": "book",
            "asset_id": asset,
            "market": "0xDEMO",
            "hash": f"0xB{seq}",
            "timestamp": ts,
            "bids": [{"price": str(price), "size": "100"}],
            "asks": [{"price": str(round(price + 0.01, 3)), "size": "80"}],
        }
    if kind == "last_trade_price":
        return {
            "event_type": "last_trade_price",
            "asset_id": asset,
            "market": "0xDEMO",
            "price": str(price),
            "size": str(random.randint(1, 200)),
            "side": random.choice(["BUY", "SELL"]),
            "timestamp": ts,
            "transaction_hash": f"0xTX{seq}",
        }
    if kind == "tick_size_change":
        return {
            "event_type": "tick_size_change",
            "asset_id": asset,
            "market": "0xDEMO",
            "old_tick_size": "0.01",
            "new_tick_size": "0.001",
            "timestamp": ts,
        }
    return {
        "event_type": "price_change",
        "market": "0xDEMO",
        "timestamp": ts,
        "price_changes": [
            {
                "asset_id": asset,
                "price": str(price),
                "size": str(random.randint(1, 500)),
                "side": random.choice(["BUY", "SELL"]),
                "hash": f"pc{seq}",
            }
        ],
    }


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="python -m polytape.monitor.demo",
        description="Write a live synthetic capture (no network) so the dashboard has data.",
    )
    p.add_argument(
        "--out", default="./data", help="Output root (point the monitor here). Default: ./data"
    )
    p.add_argument("--event-id", default="demo", help="Synthetic event id. Default: demo")
    p.add_argument("--rate", type=float, default=8.0, help="Approx book messages/sec. Default: 8")
    p.add_argument("--duration", type=float, default=0.0, help="Seconds to run (0 = until Ctrl-C).")
    p.add_argument("--seed", type=int, default=None, help="RNG seed for reproducible output.")
    return p.parse_args(argv)


def _raise_keyboard_interrupt(signum: int, frame: object) -> None:
    raise KeyboardInterrupt


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    # On Windows the monitor's "Stop" sends CTRL_BREAK_EVENT (-> SIGBREAK); map it
    # to KeyboardInterrupt so the capture is finalized just like Ctrl-C.
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, _raise_keyboard_interrupt)
    if args.seed is not None:
        random.seed(args.seed)

    config = Config(
        event_id=str(args.event_id),
        out_dir=Path(args.out),
        dry_run=True,
    )
    event = synthetic_event(config)

    base_interval = 1.0 / max(0.1, args.rate)
    deadline = time.monotonic() + args.duration if args.duration > 0 else None
    seq = 0
    next_gap = time.monotonic() + random.uniform(20, 45)

    logger.info("demo capture -> %s  (rate ~%.0f/s, Ctrl-C to stop)", config.event_dir, args.rate)
    with CaptureWriter(config, event_info=event) as writer:
        try:
            while deadline is None or time.monotonic() < deadline:
                seq += 1
                writer.write(STREAM_BOOK, _book_message(seq))

                now = time.monotonic()
                if now >= next_gap:  # simulate a disconnect + recovery
                    down = _iso(
                        datetime.now(timezone.utc) - timedelta(seconds=random.uniform(2, 8))
                    )
                    writer.record_gap(
                        STREAM_BOOK,
                        down,
                        _iso(datetime.now(timezone.utc)),
                        note="demo simulated gap",
                    )
                    next_gap = now + random.uniform(20, 45)

                time.sleep(base_interval * random.uniform(0.4, 1.6))
        except KeyboardInterrupt:
            logger.info("stopping demo capture")
    logger.info("demo capture finalized: counts=%s", writer.counts)
    return 0


if __name__ == "__main__":
    sys.exit(main())
