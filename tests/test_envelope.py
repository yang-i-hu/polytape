"""Tests for envelope construction, timestamps, and dedup ids."""

from __future__ import annotations

from polytape.envelope import (
    _iso_from_epoch,
    build_envelope,
    content_id,
    extract_id,
    iso_to_datetime,
    parse_server_ts,
    utc_now_iso,
)

# -- timestamps ---------------------------------------------------------- #


def test_utc_now_iso_format():
    now = utc_now_iso()
    assert now.endswith("Z") and "T" in now
    assert iso_to_datetime(now) is not None


def test_iso_to_datetime_variants():
    assert iso_to_datetime("2025-11-16T19:05:08Z") is not None
    assert iso_to_datetime("2025-11-16T19:05:08+00:00") is not None
    # 5-digit and 7-digit fractional seconds both normalize (3.10-safe)
    assert iso_to_datetime("2025-11-16T19:05:08.13357Z") is not None
    assert iso_to_datetime("2025-11-16T19:05:08.1234567Z") is not None
    assert iso_to_datetime("not a date") is None
    assert iso_to_datetime(12345) is None  # type: ignore[arg-type]


def test_iso_from_epoch_ms_and_seconds_agree():
    assert _iso_from_epoch(1718380800)[:10] == _iso_from_epoch(1718380800000)[:10]
    assert _iso_from_epoch("not-a-number") is None


def test_parse_server_ts_book_uses_timestamp():
    assert parse_server_ts("book", {"timestamp": "1757908892351"}).endswith("Z")
    assert parse_server_ts("book", {}) is None


def test_parse_server_ts_unknown_stream_is_none():
    assert parse_server_ts("other", {"timestamp": "1757908892351"}) is None


# -- dedup ids ----------------------------------------------------------- #


def test_extract_id_book_variants():
    assert extract_id("book", {"event_type": "book", "hash": "0xH"}) == "0xH"
    assert (
        extract_id("book", {"event_type": "last_trade_price", "transaction_hash": "0xT"}) == "0xT"
    )
    pc = {"event_type": "price_change", "price_changes": [{"hash": "h"}]}
    assert extract_id("book", pc).startswith("sha256:")
    assert extract_id("book", {"event_type": "book"}).startswith("sha256:")  # no hash -> content


def test_content_id_is_order_independent():
    assert content_id({"a": 1, "b": 2}) == content_id({"b": 2, "a": 1})


# -- envelope ------------------------------------------------------------ #


def test_build_envelope_book():
    raw = {"event_type": "book", "hash": "0xH", "asset_id": "t1", "timestamp": "1700000000000"}
    env = build_envelope("book", raw)
    assert env["stream"] == "book" and env["id"] == "0xH"
    assert env["raw"] == raw
    assert env["ts_recv"].endswith("Z")
    assert env["ts_server"].endswith("Z")


def test_build_envelope_ts_recv_override():
    env = build_envelope(
        "book", {"event_type": "book", "hash": "h"}, ts_recv="2020-01-01T00:00:00.0Z"
    )
    assert env["ts_recv"] == "2020-01-01T00:00:00.0Z"
