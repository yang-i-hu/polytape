"""Tests for Config validation and CLI argument parsing."""

from __future__ import annotations

import pytest

from polytape.cli import parse_args
from polytape.config import Config


def test_defaults(tmp_path):
    c = Config(event_id="123", out_dir=tmp_path)
    assert c.comments and c.book and c.hash_usernames
    assert c.enabled_streams == ("comments", "book")
    assert c.event_dir == tmp_path / "event-123"


def test_book_only_streams():
    assert Config(event_id="1", comments=False).enabled_streams == ("book",)


def test_both_streams_off_raises():
    with pytest.raises(ValueError, match="at least one stream"):
        Config(event_id="1", comments=False, book=False)


def test_nonnumeric_live_raises():
    with pytest.raises(ValueError, match="numeric"):
        Config(event_id="demo")


def test_nonnumeric_dry_run_ok():
    assert Config(event_id="demo", dry_run=True).event_id == "demo"


def test_empty_event_id_raises():
    with pytest.raises(ValueError, match="at least one event id"):
        Config(event_id="   ")


def test_bad_log_level_raises():
    with pytest.raises(ValueError, match="log level"):
        Config(event_id="1", log_level="LOUD")


# -- CLI ----------------------------------------------------------------- #


def test_cli_basic():
    cfg = parse_args(["--event-id", "123"])
    assert cfg.event_id == "123" and cfg.comments and cfg.book and cfg.hash_usernames


def test_cli_flags():
    cfg = parse_args(
        ["--event-id", "123", "--no-hash", "--no-book", "--market-id", "a", "--market-id", "b"]
    )
    assert cfg.hash_usernames is False
    assert cfg.book is False
    assert cfg.market_ids == ("a", "b")


def test_cli_log_level_uppercased():
    assert parse_args(["--event-id", "1", "--log-level", "debug"]).log_level == "DEBUG"


def test_cli_missing_event_id_exits():
    with pytest.raises(SystemExit):
        parse_args([])


def test_cli_both_streams_off_exits():
    with pytest.raises(SystemExit):
        parse_args(["--event-id", "1", "--no-comments", "--no-book"])


# -- multi-event (Phase 3) ------------------------------------------------- #


def _write_matches(tmp_path):
    import json

    p = tmp_path / "m.json"
    p.write_text(
        json.dumps(
            [
                {"event_id": "1001", "closed": False},
                {"event_id": "1002", "closed": True},
                {"event_id": "1003", "closed": False},
                {"event_id": "1001", "closed": False},  # duplicate
            ]
        ),
        encoding="utf-8",
    )
    return p


def test_load_matches_open_only(tmp_path):
    from polytape.cli import load_matches

    p = _write_matches(tmp_path)
    assert load_matches(str(p), open_only=True) == ("1001", "1003")  # closed skipped, deduped
    assert load_matches(str(p), open_only=False) == ("1001", "1002", "1003")


def test_cli_matches_file_is_multi(tmp_path):
    p = _write_matches(tmp_path)
    cfg = parse_args(["--matches-file", str(p), "--run-name", "wc", "--out", str(tmp_path)])
    assert cfg.event_ids == ("1001", "1003")
    assert cfg.is_multi and cfg.event_dir == tmp_path / "run-wc"


def test_cli_multiple_event_ids(tmp_path):
    cfg = parse_args(
        ["--event-id", "1001", "--event-id", "1002", "--run-name", "wc", "--out", str(tmp_path)]
    )
    assert cfg.event_ids == ("1001", "1002")
    assert cfg.event_dir == tmp_path / "run-wc"
