"""Tests for the read-only monitor: incremental tail, stats, discovery, server.

All offline. Captures are produced with the real :class:`CaptureWriter` so the
reader is exercised against exactly the bytes a live recorder writes.
"""

from __future__ import annotations

import json
import threading
import time
import urllib.request
from importlib import resources

import pytest

from polytape.envelope import utc_now_iso
from polytape.monitor.reader import CaptureMonitor, _delay_ms, _percentile, _StreamTail
from polytape.monitor.server import make_server
from polytape.writer import CaptureWriter

# --------------------------------------------------------------------------- #
# Pure helpers
# --------------------------------------------------------------------------- #


def test_percentile_basic():
    assert _percentile([], 50) is None
    assert _percentile([5.0], 95) == 5.0
    assert _percentile([1, 2, 3, 4, 5], 50) == 3.0
    assert _percentile([1, 2, 3, 4, 5], 100) == 5.0


def test_delay_ms():
    assert _delay_ms("2026-01-01T00:00:00.000000Z", "2026-01-01T00:00:00.250000Z") == 250.0
    assert _delay_ms(None, "2026-01-01T00:00:00Z") is None
    assert _delay_ms("bad", "also-bad") is None


# --------------------------------------------------------------------------- #
# Tailing + stats
# --------------------------------------------------------------------------- #


def _book_line(i: int) -> str:
    """A well-formed book envelope as a JSON string (no trailing newline)."""
    return json.dumps(
        {
            "stream": "book",
            "id": str(i),
            "ts_recv": "2026-01-01T00:00:00Z",
            "ts_server": None,
            "raw": {"event_type": "book"},
        }
    )


def test_exact_count_on_attach_and_incremental_tail(make_config):
    cfg = make_config()  # comments + book under tmp_path/event-20200
    w = CaptureWriter(cfg)
    w.open()
    try:
        w.write("book", {"event_type": "book", "hash": "h1", "timestamp": "1700000000000"})
        w.write("book", {"event_type": "price_change", "timestamp": "1700000000500"})
        w.write("comments", {"payload": {"id": "c1", "createdAt": "2026-01-01T00:00:00Z"}})

        mon = CaptureMonitor(cfg.out_dir)
        snap = mon.snapshot()  # first poll: attach counts existing lines exactly
        assert snap["selected_event"] == "20200"
        assert snap["streams"]["book"]["count"] == 2
        assert snap["streams"]["comments"]["count"] == 1
        assert snap["totals"]["count"] == 3
        assert snap["recent"] == []  # attach does not replay history

        # Append after attach -> shows up as counted + in the recent ticker.
        w.write(
            "book",
            {
                "event_type": "last_trade_price",
                "transaction_hash": "tx1",
                "timestamp": "1700000001000",
            },
        )
        snap = mon.snapshot()
        assert snap["streams"]["book"]["count"] == 3
        assert snap["recent"][0]["stream"] == "book"
        assert snap["recent"][0]["type"] == "last_trade_price"
        assert snap["streams"]["book"]["types"]["last_trade_price"] == 1
    finally:
        w.close()


def test_type_mix_and_delay(make_config):
    cfg = make_config(comments=False)
    w = CaptureWriter(cfg)
    w.open()
    try:
        mon = CaptureMonitor(cfg.out_dir)
        mon.snapshot()  # attach
        ts = str(int(time.time() * 1000))  # recent server time -> live (un-clamped) delay
        for i in range(4):  # distinct payloads (identical ones would be deduped)
            w.write(
                "book",
                {
                    "event_type": "price_change",
                    "timestamp": ts,
                    "price_changes": [{"hash": f"pc{i}", "price": str(i)}],
                },
            )
        w.write("book", {"event_type": "book", "hash": "hx", "timestamp": ts})
        snap = mon.snapshot()
        book = snap["streams"]["book"]
        assert book["types"] == {"price_change": 4, "book": 1}
        assert book["with_server_ts"] == 5
        assert book["delay_ms"]["n"] == 5  # all carried a (recent) server timestamp
    finally:
        w.close()


def test_malformed_lines_tolerated_on_live_tail(tmp_path):
    """A bad line appended while tailing is counted separately, not fatal."""
    event_dir = tmp_path / "event-9"
    event_dir.mkdir()
    path = event_dir / "book.jsonl"
    path.write_text(_book_line(1) + "\n" + _book_line(2) + "\n", encoding="utf-8")  # healthy file
    mon = CaptureMonitor(tmp_path)
    snap = mon.snapshot()  # attach: exact count of well-formed records
    assert snap["streams"]["book"]["count"] == 2
    assert snap["streams"]["book"]["malformed"] == 0

    with open(path, "a", encoding="utf-8") as fh:  # corruption arrives on the wire
        fh.write("{not json\n" + _book_line(3) + "\n")
    snap = mon.snapshot()
    assert snap["streams"]["book"]["count"] == 3  # only the valid line counts
    assert snap["streams"]["book"]["malformed"] == 1


def test_partial_trailing_line_held_until_complete(tmp_path):
    event_dir = tmp_path / "event-7"
    event_dir.mkdir()
    path = event_dir / "book.jsonl"
    # Two complete lines + a partial third (no trailing newline yet).
    path.write_text(_book_line(1) + "\n" + _book_line(2) + "\n" + _book_line(3), encoding="utf-8")
    mon = CaptureMonitor(tmp_path)
    snap = mon.snapshot()
    assert snap["streams"]["book"]["count"] == 2  # partial not counted

    with open(path, "a", encoding="utf-8") as fh:  # complete the line
        fh.write("\n")
    snap = mon.snapshot()
    assert snap["streams"]["book"]["count"] == 3


def test_truncation_resyncs(tmp_path):
    event_dir = tmp_path / "event-5"
    event_dir.mkdir()
    path = event_dir / "book.jsonl"
    rec = json.dumps(
        {
            "stream": "book",
            "id": "1",
            "ts_recv": "2026-01-01T00:00:00Z",
            "ts_server": None,
            "raw": {"event_type": "book"},
        }
    )
    path.write_text(rec + "\n" + rec + "\n", encoding="utf-8")
    mon = CaptureMonitor(tmp_path)
    assert mon.snapshot()["streams"]["book"]["count"] == 2
    path.write_text(rec + "\n", encoding="utf-8")  # shrink (rotation/rewrite)
    assert mon.snapshot()["streams"]["book"]["count"] == 1


def test_count_newlines_is_bounded_to_size(tmp_path):
    path = tmp_path / "x.jsonl"
    path.write_bytes(b"a\nb\nc\n")  # bytes: match the writer's newline="\n" exactly
    tail = _StreamTail(path)
    assert tail._count_newlines(4) == 2  # only "a\nb\n"
    assert tail._count_newlines(6) == 3


def test_attach_does_not_double_count_lines_appended_during_scan(tmp_path):
    """Regression: attach must count and park-offset against the SAME byte bound.

    Simulates the live race where the recorder appends a 4th line between poll()'s
    stat() and the on-attach scan. Before the fix, that line was counted by the
    scan AND re-read on the next poll (count -> 5 for a 4-line file).
    """
    event_dir = tmp_path / "event-race"
    event_dir.mkdir()
    path = event_dir / "book.jsonl"
    lines = [_book_line(i) for i in range(1, 5)]
    # newline="\n" so the on-disk bytes match our offset math (the real writer does
    # the same; Path.write_text would translate \n->\r\n on Windows and skew offsets).
    path.write_text("".join(s + "\n" for s in lines), encoding="utf-8", newline="\n")
    size_of_first_three = len("".join(s + "\n" for s in lines[:3]).encode("utf-8"))

    tail = _StreamTail(path)
    tail._attach(size_of_first_three)  # stat() saw only 3 lines; 4th already on disk
    assert tail.count == 3  # bounded to the size we were given
    assert tail.offset == size_of_first_three
    tail.poll(1.0)  # now sees the real EOF
    assert tail.count == 4  # 4th line counted exactly once (was 5 before the fix)


def test_historical_backfill_delay_excluded_from_percentiles(make_config):
    """Backfilled comments (old createdAt) must not pollute the live delay window."""
    cfg = make_config(book=False)
    w = CaptureWriter(cfg)
    w.open()
    try:
        mon = CaptureMonitor(cfg.out_dir)
        mon.snapshot()  # attach
        # A live comment (createdAt ~now) and a historical/backfilled one (year 2000).
        w.write(
            "comments",
            {"type": "comment_created", "payload": {"id": "live", "createdAt": utc_now_iso()}},
        )
        w.write(
            "comments",
            {
                "type": "comment_created",
                "payload": {"id": "old", "createdAt": "2000-01-01T00:00:00Z"},
            },
        )
        c = mon.snapshot()["streams"]["comments"]
        assert c["count"] == 2  # both recorded
        assert c["with_server_ts"] == 2  # both had a server timestamp
        assert c["delay_ms"]["n"] == 1  # but only the live one is in the percentile window
    finally:
        w.close()


def test_dashboard_html_escapes_stream_name():
    """The only feed/filesystem-derived value in the page must be HTML-escaped."""
    html = resources.files("polytape.monitor").joinpath("index.html").read_text(encoding="utf-8")
    assert "${esc(name)}" in html
    assert "</span>${name}</h2>" not in html  # unescaped text-node sink
    assert 'data-stream="${name}"' not in html  # unescaped attribute sink


# --------------------------------------------------------------------------- #
# Status + discovery
# --------------------------------------------------------------------------- #


def test_status_no_data_when_empty(tmp_path):
    mon = CaptureMonitor(tmp_path)
    snap = mon.snapshot()
    assert snap["status"] == "no-data"
    assert snap["events"] == []


def test_status_live_then_stopped(make_config):
    cfg = make_config(comments=False)
    w = CaptureWriter(cfg)
    w.open()
    w.write("book", {"event_type": "book", "hash": "h", "timestamp": "1700000000000"})
    mon = CaptureMonitor(cfg.out_dir)
    snap = mon.snapshot()  # meta has started_at, no stopped_at, fresh ts_recv -> live
    assert snap["status"] == "live"
    assert snap["uptime_seconds"] is not None
    w.close()  # finalizes meta with stopped_at
    assert mon.snapshot()["status"] == "stopped"


def test_idle_when_last_message_is_stale(make_config):
    cfg = make_config(comments=False)
    w = CaptureWriter(cfg)
    w.open()
    w.write("book", {"event_type": "book", "hash": "h", "timestamp": "1700000000000"})
    try:
        # now far in the future -> last message looks stale -> idle (still running)
        mon = CaptureMonitor(cfg.out_dir, now_iso=lambda: "2099-01-01T00:00:00.000000Z")
        assert mon.snapshot()["status"] == "idle"
    finally:
        w.close()


def test_discovery_multiple_events_and_picker(make_config, tmp_path):
    for eid in ("1", "2"):
        cfg = make_config(event_id=eid, comments=False)
        with CaptureWriter(cfg) as w:
            w.write("book", {"event_type": "book", "hash": "h", "timestamp": "1700000000000"})
    mon = CaptureMonitor(tmp_path)
    snap = mon.snapshot()
    ids = {e["event_id"] for e in snap["events"]}
    assert ids == {"1", "2"}
    # Explicit selection is honored.
    assert mon.snapshot(event_id="1")["selected_event"] == "1"


def test_single_event_dir_as_root(make_config):
    cfg = make_config(comments=False)
    with CaptureWriter(cfg) as w:
        w.write("book", {"event_type": "book", "hash": "h", "timestamp": "1700000000000"})
    mon = CaptureMonitor(cfg.event_dir)  # point directly at event-<id>
    snap = mon.snapshot()
    assert snap["selected_event"] == "20200"
    assert snap["streams"]["book"]["count"] == 1


def test_no_pii_in_snapshot(make_config):
    """Recent ticker and stats must never carry payload content / identifiers."""
    cfg = make_config(book=False, hash_usernames=False)
    w = CaptureWriter(cfg, hasher=None)
    w.open()
    mon = CaptureMonitor(cfg.out_dir)
    mon.snapshot()
    w.write(
        "comments",
        {
            "type": "comment_created",
            "payload": {
                "id": "c1",
                "createdAt": "2026-01-01T00:00:00Z",
                "userAddress": "0xSECRET",
                "body": "secret text",
            },
        },
    )
    snap = mon.snapshot()
    blob = json.dumps(snap)
    assert "0xSECRET" not in blob and "secret text" not in blob
    assert snap["recent"][0]["type"] == "comment_created"
    w.close()


# --------------------------------------------------------------------------- #
# HTTP server
# --------------------------------------------------------------------------- #


@pytest.fixture
def running_server(make_config):
    cfg = make_config(comments=False)
    with CaptureWriter(cfg) as w:
        w.write("book", {"event_type": "book", "hash": "h", "timestamp": "1700000000000"})
    mon = CaptureMonitor(cfg.out_dir)
    server = make_server(mon, host="127.0.0.1", port=0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address[0], server.server_address[1]
    try:
        yield f"http://{host}:{port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_server_serves_dashboard_and_stats(running_server):
    with urllib.request.urlopen(running_server + "/", timeout=5) as resp:
        assert resp.status == 200
        body = resp.read().decode("utf-8")
        assert "polytape" in body and "<canvas" in body

    with urllib.request.urlopen(running_server + "/api/stats", timeout=5) as resp:
        assert resp.status == 200
        data = json.loads(resp.read())
        assert data["selected_event"] == "20200"
        assert data["streams"]["book"]["count"] == 1

    with urllib.request.urlopen(running_server + "/healthz", timeout=5) as resp:
        assert json.loads(resp.read())["ok"] is True


def test_server_404(running_server):
    try:
        urllib.request.urlopen(running_server + "/nope", timeout=5)
        raise AssertionError("expected 404")
    except urllib.error.HTTPError as exc:
        assert exc.code == 404
