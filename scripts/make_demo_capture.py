"""Generate a realistic synthetic polytape *book* capture for viewer demos/tests.

Produces a ``<out>/event-<id>/`` directory with a ``book.jsonl`` + ``meta.json``
shaped exactly like a live capture (same envelope, same ``raw`` event types), but
dense and *internally consistent*: each outcome is modelled as a real moving order
book seeded by one ``book`` snapshot and then evolved purely with ``price_change``
deltas (level adds/resizes/removals that never cross), with periodic trades, a
mid-capture reconnect gap (fresh snapshot), and complementary YES/NO prices.

It drives the *real* :class:`~polytape.writer.CaptureWriter`, so envelopes, ids,
dual timestamps and ``meta.json`` come from the production code path.

Usage:
    python scripts/make_demo_capture.py --out ./_vtmp --event-id 80505
    # then append ~10s of live updates (stopped_at stays null while running):
    python scripts/make_demo_capture.py --out ./_vtmp --event-id 80505 --live 10
"""

from __future__ import annotations

import argparse
import random
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from polytape.config import STREAM_BOOK, Config
from polytape.gamma import EventInfo, Market
from polytape.writer import CaptureWriter

YES_TOKEN = "71321045679252212594626385532706912750332728571942532289631379312455583992563"
NO_TOKEN = "21742633143463906290569050155826241533067272736897614950488156847949938836455"
CONDITION_ID = "0x5f65177b394277fd294cd75650044e32ba009a95022d88a0c1d565897d72f8f1"

TICK = 0.01
LEVELS = 10
_LOW_IDX = LEVELS + 1
_HIGH_IDX = 99 - 3 - (LEVELS - 1)


def _fmt(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


def _event(event_id: str) -> EventInfo:
    return EventInfo(
        event_id=event_id,
        title="Demo: will it resolve YES?",
        slug="demo-will-it-resolve-yes",
        markets=(Market(id="239826", condition_id=CONDITION_ID, token_ids=(YES_TOKEN, NO_TOKEN)),),
        raw={"demo": True},
    )


class _BookGen:
    """Models one asset's order book as tick-index -> size, evolved by deltas."""

    def __init__(self, asset_id: str, rng: random.Random, hashes) -> None:
        self.asset_id = asset_id
        self.rng = rng
        self._hash = hashes
        self.bids: dict[int, float] = {}
        self.asks: dict[int, float] = {}
        self.bid_top = 0
        self.spread = 2

    def price(self, idx: int) -> float:
        return round(idx * TICK, 6)

    def _size(self, i: int = 0) -> float:
        return round(self.rng.uniform(20, 400) / (i * 0.4 + 1), 2)

    def snapshot(self, bid_top: int, spread: int, ts_ms: int) -> dict:
        """Re-seed a clean ladder and return a ``book`` raw message."""
        self.bid_top, self.spread = bid_top, spread
        ask_top = bid_top + spread
        self.bids = {bid_top - i: self._size(i) for i in range(LEVELS)}
        self.asks = {ask_top + i: self._size(i) for i in range(LEVELS)}
        return {
            "event_type": "book",
            "asset_id": self.asset_id,
            "market": CONDITION_ID,
            "hash": self._hash(),
            "timestamp": str(ts_ms),
            "bids": [
                {"price": f"{self.price(i):.3f}", "size": f"{s:.2f}"}
                for i, s in sorted(self.bids.items(), reverse=True)
            ],
            "asks": [
                {"price": f"{self.price(i):.3f}", "size": f"{s:.2f}"}
                for i, s in sorted(self.asks.items())
            ],
        }

    def _retarget(self, bid_top: int, spread: int) -> list[tuple[str, int, float]]:
        """Move the book to a new touch; emit only boundary level changes (uncrossed)."""
        self.bid_top, self.spread = bid_top, spread
        ask_top = bid_top + spread
        want_b = {bid_top - i for i in range(LEVELS)}
        want_a = {ask_top + i for i in range(LEVELS)}
        changes: list[tuple[str, int, float]] = []
        for idx in list(self.bids):  # removals first, so a price never sits on both sides
            if idx not in want_b:
                del self.bids[idx]
                changes.append(("BUY", idx, 0.0))
        for idx in list(self.asks):
            if idx not in want_a:
                del self.asks[idx]
                changes.append(("SELL", idx, 0.0))
        for i, idx in enumerate(sorted(want_b, reverse=True)):
            if idx not in self.bids:
                self.bids[idx] = self._size(i)
                changes.append(("BUY", idx, self.bids[idx]))
        for i, idx in enumerate(sorted(want_a)):
            if idx not in self.asks:
                self.asks[idx] = self._size(i)
                changes.append(("SELL", idx, self.asks[idx]))
        return changes

    def _jitter(self) -> list[tuple[str, int, float]]:
        changes: list[tuple[str, int, float]] = []
        for book, side in ((self.bids, "BUY"), (self.asks, "SELL")):
            if not book:
                continue
            for idx in self.rng.sample(list(book), k=min(len(book), self.rng.randint(1, 2))):
                if self.rng.random() < 0.1:
                    del book[idx]
                    changes.append((side, idx, 0.0))
                else:
                    book[idx] = self._size()
                    changes.append((side, idx, book[idx]))
        return changes

    def step(self, bid_top: int, spread: int, ts_ms: int) -> dict | None:
        """Walk to a new touch + jitter; return a ``price_change`` raw message."""
        changes = self._retarget(bid_top, spread) + self._jitter()
        if not changes:
            return None
        return {
            "event_type": "price_change",
            "market": CONDITION_ID,
            "timestamp": str(ts_ms),
            "price_changes": [
                {
                    "asset_id": self.asset_id,
                    "price": f"{self.price(idx):.3f}",
                    "size": f"{size:.2f}",
                    "side": side,
                    "hash": self._hash(),
                }
                for side, idx, size in changes
            ],
        }

    def trade(self, side: str, ts_ms: int, i: int) -> dict:
        idx = self.bid_top if side == "SELL" else self.bid_top + self.spread
        return {
            "event_type": "last_trade_price",
            "asset_id": self.asset_id,
            "market": CONDITION_ID,
            "price": f"{self.price(idx):.3f}",
            "size": f"{self.rng.uniform(5, 120):.2f}",
            "side": side,
            "timestamp": str(ts_ms),
            "transaction_hash": f"0xtx{i:05d}",
        }


def _clamp(idx: int) -> int:
    return max(_LOW_IDX, min(_HIGH_IDX, idx))


def _no_top(yes_bid_top: int, yes_spread: int, no_spread: int) -> int:
    """A NO touch whose mid is ~ (1 - YES mid), so prices look complementary."""
    yes_mid_idx = yes_bid_top + yes_spread / 2
    return _clamp(round((100 - yes_mid_idx) - no_spread / 2))


def _hash_seq():
    n = [0]

    def nxt() -> str:
        n[0] += 1
        return f"0xhash{n[0]:06d}"

    return nxt


def generate(out_dir: Path, event_id: str, updates: int = 260, seed: int = 7) -> Path:
    rng = random.Random(seed)
    config = Config(event_id=event_id, out_dir=out_dir, comments=False, book=True, dry_run=True)
    hashes = _hash_seq()
    clock = {"t": datetime(2026, 6, 14, 18, 0, 0, tzinfo=timezone.utc)}
    base_ms = 1_750_000_000_000

    def now() -> str:
        return _fmt(clock["t"])

    yes = _BookGen(YES_TOKEN, rng, hashes)
    no = _BookGen(NO_TOKEN, rng, hashes)
    yes_top, yes_spread = 41, 2

    with CaptureWriter(config, event_info=_event(event_id), hasher=None, now=now) as w:

        def emit(raw: dict | None) -> None:
            if raw is None:
                return
            clock["t"] += timedelta(milliseconds=rng.randint(300, 1400))
            w.write(STREAM_BOOK, raw)

        emit(yes.snapshot(yes_top, yes_spread, base_ms))
        emit(no.snapshot(_no_top(yes_top, yes_spread, 2), 2, base_ms + 1))

        for i in range(updates):
            ts_ms = base_ms + (i + 2) * 1000
            if i == updates // 2:  # reconnect gap → fresh snapshots re-seed both books
                w.record_gap(
                    STREAM_BOOK,
                    _fmt(clock["t"]),
                    _fmt(clock["t"] + timedelta(seconds=4)),
                    note="demo simulated reconnect",
                )
                clock["t"] += timedelta(seconds=4)
                emit(yes.snapshot(yes_top, yes_spread, ts_ms))
                emit(no.snapshot(_no_top(yes_top, yes_spread, no.spread), no.spread, ts_ms))
                continue

            yes_top = _clamp(yes_top + rng.choice([-1, -1, 0, 0, 1, 1]))
            if rng.random() < 0.1:
                yes_spread = rng.randint(1, 3)
            emit(yes.step(yes_top, yes_spread, ts_ms))
            no_spread = no.spread if rng.random() > 0.1 else rng.randint(1, 3)
            emit(no.step(_no_top(yes_top, yes_spread, no_spread), no_spread, ts_ms))

            if rng.random() < 0.35:
                emit(yes.trade(rng.choice(("BUY", "SELL")), ts_ms, i))

    return config.event_dir


def append_live(out_dir: Path, event_id: str, seconds: float, interval: float, seed: int) -> int:
    """Append live updates with real sleeps; ``stopped_at`` stays null until done."""
    rng = random.Random(seed)
    config = Config(event_id=event_id, out_dir=out_dir, comments=False, book=True, dry_run=True)
    hashes = _hash_seq()
    yes = _BookGen(YES_TOKEN, rng, hashes)
    no = _BookGen(NO_TOKEN, rng, hashes)
    yes_top, yes_spread = 41, 2
    written = 0
    base_ms = int(time.time() * 1000)
    with CaptureWriter(config, event_info=_event(event_id), hasher=None) as w:

        def emit(raw: dict | None) -> None:
            nonlocal written
            if raw is not None and w.write(STREAM_BOOK, raw):
                written += 1

        emit(yes.snapshot(yes_top, yes_spread, base_ms))
        emit(no.snapshot(_no_top(yes_top, yes_spread, 2), 2, base_ms))
        deadline = time.monotonic() + seconds
        i = 0
        while time.monotonic() < deadline:
            ts_ms = int(time.time() * 1000)
            yes_top = _clamp(yes_top + rng.choice([-1, 0, 1]))
            emit(yes.step(yes_top, yes_spread, ts_ms))
            emit(no.step(_no_top(yes_top, yes_spread, no.spread), no.spread, ts_ms))
            if rng.random() < 0.4:
                emit(yes.trade(rng.choice(("BUY", "SELL")), ts_ms, i))
            i += 1
            time.sleep(interval)
    return written


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--out", default="./_vtmp", type=Path)
    p.add_argument("--event-id", default="80505")
    p.add_argument("--updates", type=int, default=260)
    p.add_argument("--seed", type=int, default=7)
    p.add_argument(
        "--live",
        type=float,
        default=0.0,
        metavar="SEC",
        help="Append live updates to an existing capture for SEC seconds.",
    )
    p.add_argument("--interval", type=float, default=0.5, help="Live append cadence (with --live).")
    args = p.parse_args(argv)

    if args.live > 0:
        n = append_live(args.out, args.event_id, args.live, args.interval, args.seed + 1)
        print(f"appended {n} live lines to {Path(args.out) / f'event-{args.event_id}'}")
        return 0

    event_dir = generate(args.out, args.event_id, updates=args.updates, seed=args.seed)
    lines = (event_dir / "book.jsonl").read_text(encoding="utf-8").splitlines()
    print(f"wrote {event_dir} ({len(lines)} book lines)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
