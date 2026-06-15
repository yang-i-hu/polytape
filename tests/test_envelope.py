"""Tests for envelope construction, timestamps, dedup ids, and hashing."""

from __future__ import annotations

from polytape.envelope import (
    Hasher,
    _iso_from_epoch,
    build_envelope,
    content_id,
    extract_id,
    iso_to_datetime,
    parse_server_ts,
    redact,
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


def test_parse_server_ts_comments_prefers_createdat():
    raw = {"timestamp": 1700000000000, "payload": {"createdAt": "2025-01-02T03:04:05Z"}}
    assert parse_server_ts("comments", raw).startswith("2025-01-02T03:04:05")


def test_parse_server_ts_comments_falls_back_to_timestamp():
    raw = {"timestamp": 1718380800000, "payload": {"id": "x"}}
    assert parse_server_ts("comments", raw).endswith("Z")


def test_parse_server_ts_book_uses_timestamp():
    assert parse_server_ts("book", {"timestamp": "1757908892351"}).endswith("Z")
    assert parse_server_ts("book", {}) is None


# -- dedup ids ----------------------------------------------------------- #


def test_extract_id_comment_live_and_flat_match():
    live = {"type": "comment_created", "payload": {"id": "c1"}}
    flat = {"id": "c1"}
    assert extract_id("comments", live) == "c1" == extract_id("comments", flat)


def test_extract_id_reaction_uses_its_own_id():
    assert extract_id("comments", {"type": "reaction_created", "payload": {"id": "r1"}}) == "r1"


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


# -- hashing ------------------------------------------------------------- #


def test_hasher_deterministic_and_salt_sensitive():
    h1, h2 = Hasher(salt="s"), Hasher(salt="other")
    assert h1.hash_value("x") == h1.hash_value("x")
    assert h1.hash_value("x") != h2.hash_value("x")
    assert len(h1.fingerprint) == 8
    assert Hasher(salt="s").hash_value("x") == Hasher(salt=b"s").hash_value("x")


def test_hasher_reads_env_salt(monkeypatch):
    monkeypatch.setenv("POLYTAPE_SALT", "envsalt")
    assert Hasher().hash_value("x") == Hasher(salt="envsalt").hash_value("x")


def test_redact_recursive_and_typed():
    h = Hasher(salt="s")
    obj = {
        "userAddress": "0xA",
        "keep": "0xA",
        "count": 5,
        "empty": "",
        "profile": {"name": "bob", "proxyWallet": "0xA"},
        "list": [{"pseudonym": "p"}],
    }
    fields = frozenset({"userAddress", "name", "pseudonym", "proxyWallet"})
    redact(obj, fields, h)
    assert obj["userAddress"] == h.hash_value("0xA")
    assert obj["keep"] == "0xA"  # not in fields -> untouched
    assert obj["count"] == 5  # non-string -> untouched
    assert obj["empty"] == ""  # empty string -> untouched
    assert obj["profile"]["name"] == h.hash_value("bob")
    assert obj["profile"]["proxyWallet"] == h.hash_value("0xA")
    assert obj["list"][0]["pseudonym"] == h.hash_value("p")


# -- envelope ------------------------------------------------------------ #


def test_build_envelope_comment_hashes_in_place():
    raw = {"type": "comment_created", "payload": {"id": "c1", "userAddress": "0xW", "body": "hi"}}
    env = build_envelope("comments", raw, hasher=Hasher(salt="s"))
    assert env["stream"] == "comments" and env["id"] == "c1"
    assert env["raw"]["payload"]["userAddress"] != "0xW"  # hashed
    assert env["raw"]["payload"]["body"] == "hi"  # intact
    assert env["ts_recv"].endswith("Z")


def test_build_envelope_book_not_hashed():
    raw = {"event_type": "book", "hash": "0xH", "asset_id": "t1", "timestamp": "1700000000000"}
    env = build_envelope("book", raw, hasher=Hasher(salt="s"))
    assert env["raw"] == raw  # no PII keys on book -> unchanged


def test_build_envelope_ts_recv_override():
    env = build_envelope(
        "book", {"event_type": "book", "hash": "h"}, ts_recv="2020-01-01T00:00:00.0Z"
    )
    assert env["ts_recv"] == "2020-01-01T00:00:00.0Z"
