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
    with pytest.raises(ValueError, match="non-empty"):
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


def test_cli_include_series_comments_flag():
    assert parse_args(["--event-id", "1"]).include_series_comments is False
    assert (
        parse_args(["--event-id", "1", "--include-series-comments"]).include_series_comments is True
    )


def test_cli_missing_event_id_exits():
    with pytest.raises(SystemExit):
        parse_args([])


def test_cli_both_streams_off_exits():
    with pytest.raises(SystemExit):
        parse_args(["--event-id", "1", "--no-comments", "--no-book"])
