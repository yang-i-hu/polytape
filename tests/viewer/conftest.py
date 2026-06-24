"""Fixtures for viewer tests: build real captures with the production writer."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from polytape.config import Config
from polytape.gamma import EventInfo, Market
from polytape.writer import CaptureWriter


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


@pytest.fixture
def make_book_capture(tmp_path):
    """Write a small but representative book capture; return the event dir.

    Contents (asset "100" = YES, "200" = NO):
      * snapshot 100 (two levels each side) and an EMPTY snapshot 200,
      * a price_change touching 100 only (improve bid, delete best ask),
      * a trade on 100 (must not move the book),
      * a reconnect gap, then a fresh snapshot 100 (re-seed).
    ``ts_recv`` advances one second per written line (deterministic ordering).
    """

    def _make(event_id: str = "20200") -> object:
        cfg = Config(event_id=event_id, out_dir=tmp_path, comments=False, book=True, dry_run=True)
        event = EventInfo(
            event_id=event_id,
            title="Cap Event",
            slug="cap-event",
            markets=(Market(id="m1", condition_id="0xC", token_ids=("100", "200")),),
            raw={},
        )
        clock = [datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)]

        def now() -> str:
            clock[0] += timedelta(seconds=1)
            return _iso(clock[0])

        with CaptureWriter(cfg, event_info=event, hasher=None, now=now) as w:
            w.write(
                "book",
                {
                    "event_type": "book",
                    "asset_id": "100",
                    "market": "0xC",
                    "hash": "h1",
                    "timestamp": "1",
                    "bids": [{"price": "0.40", "size": "100"}, {"price": "0.39", "size": "50"}],
                    "asks": [{"price": "0.42", "size": "80"}, {"price": "0.43", "size": "60"}],
                },
            )
            w.write(
                "book",
                {
                    "event_type": "book",
                    "asset_id": "200",
                    "market": "0xC",
                    "hash": "h2",
                    "timestamp": "2",
                    "bids": [],
                    "asks": [],
                },
            )
            w.write(
                "book",
                {
                    "event_type": "price_change",
                    "market": "0xC",
                    "timestamp": "3",
                    "price_changes": [
                        {
                            "asset_id": "100",
                            "price": "0.41",
                            "size": "30",
                            "side": "BUY",
                            "hash": "p1",
                        },
                        {
                            "asset_id": "100",
                            "price": "0.42",
                            "size": "0",
                            "side": "SELL",
                            "hash": "p2",
                        },
                    ],
                },
            )
            w.write(
                "book",
                {
                    "event_type": "last_trade_price",
                    "asset_id": "100",
                    "market": "0xC",
                    "price": "0.41",
                    "size": "12",
                    "side": "BUY",
                    "timestamp": "4",
                    "transaction_hash": "0xT",
                },
            )
            disconnected = now()
            reconnected = now()
            w.record_gap("book", disconnected, reconnected, note="test gap")
            w.write(
                "book",
                {
                    "event_type": "book",
                    "asset_id": "100",
                    "market": "0xC",
                    "hash": "h3",
                    "timestamp": "9",
                    "bids": [{"price": "0.45", "size": "100"}],
                    "asks": [{"price": "0.47", "size": "100"}],
                },
            )
        return cfg.event_dir

    return _make
