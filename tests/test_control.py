"""Tests for the optional control plane: start/stop capture subprocesses + gating.

These spawn the real demo feeder as a child process (offline, no network) to
exercise the full start -> running -> stop lifecycle and the HTTP guards.
"""

from __future__ import annotations

import json
import os
import threading
import time
import urllib.error
import urllib.request

import pytest

from polytape.gamma import GammaError, parse_event_ref
from polytape.monitor.control import ControlError, RecorderManager
from polytape.monitor.reader import CaptureMonitor
from polytape.monitor.server import LOOPBACK_HOSTS, make_server

_CTRL_HEADERS = {"Content-Type": "application/json", "X-Polytape-Control": "1"}

# Known slugs map to ids without network; unknown slugs raise (offline resolver).
_FAKE_EVENTS = {"fifwc-ksa-ury-2026-06-15": "351729"}


def _fake_resolver(ref: str) -> str:
    kind, value = parse_event_ref(ref)
    if kind == "id":
        return value
    if value in _FAKE_EVENTS:
        return _FAKE_EVENTS[value]
    raise GammaError(f"no event found for slug {value!r}")


def _wait(predicate, timeout=8.0, interval=0.1):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


@pytest.fixture
def manager(tmp_path):
    mgr = RecorderManager(tmp_path, resolver=_fake_resolver)  # offline event resolution
    yield mgr
    for status in mgr.statuses():  # never leave a child process running
        if status["running"]:
            try:
                mgr.stop(status["event_id"], graceful_timeout=4.0)
            except Exception:  # noqa: BLE001
                pass


# --------------------------------------------------------------------------- #
# Input validation (no process is spawned)
# --------------------------------------------------------------------------- #


def test_live_recording_rejects_unresolvable_ref(manager):
    with pytest.raises(ControlError):
        manager.start_recording("no-such-event-slug")


def test_demo_rejects_unsafe_event_id(manager):
    with pytest.raises(ControlError):
        manager.start_demo("../escape")
    with pytest.raises(ControlError):
        manager.start_demo("bad id!")


def test_demo_rejects_out_of_range_rate(manager):
    with pytest.raises(ControlError):
        manager.start_demo("demo", rate=0)
    with pytest.raises(ControlError):
        manager.start_demo("demo", rate=99999)


def test_stop_unknown_event_raises(manager):
    with pytest.raises(ControlError):
        manager.stop("nope")


def test_active_capture_detection(manager, tmp_path):
    event_dir = tmp_path / "event-1"
    event_dir.mkdir()
    assert manager._active_capture_exists(event_dir) is False  # empty dir

    book = event_dir / "book.jsonl"
    book.write_text('{"stream":"book"}\n', encoding="utf-8")
    assert manager._active_capture_exists(event_dir) is True  # fresh file, no meta

    (event_dir / "meta.json").write_text(
        json.dumps({"stopped_at": "2026-01-01T00:00:00Z"}), encoding="utf-8"
    )
    assert manager._active_capture_exists(event_dir) is False  # cleanly finalized

    (event_dir / "meta.json").write_text(json.dumps({"stopped_at": None}), encoding="utf-8")
    old = time.time() - 3600
    os.utime(book, (old, old))
    assert manager._active_capture_exists(event_dir) is False  # not finalized but stale


class _FakeProc:
    def __init__(self, pid):
        self.pid = pid

    def poll(self):
        return None


def _capture_spawned_argv(monkeypatch) -> list[list[str]]:
    """Mock ``subprocess.Popen`` so no process spawns; return the argv log."""
    seen: list[list[str]] = []
    monkeypatch.setattr(
        "polytape.monitor.control.subprocess.Popen",
        lambda argv, **kw: (seen.append(argv), _FakeProc(len(seen)))[1],
    )
    return seen


def test_start_recording_resolves_slug_into_argv(manager, monkeypatch):
    """A slug ref is resolved to its numeric event id before the spawn."""
    seen = _capture_spawned_argv(monkeypatch)
    manager.start_recording("fifwc-ksa-ury-2026-06-15")
    assert "--event-id" in seen[0]
    assert seen[0][seen[0].index("--event-id") + 1] == "351729"


def test_start_recording_argv_parses_with_real_recorder_cli(manager, monkeypatch):
    """Every argv the manager builds must be accepted by the real ``polytape`` CLI.

    Regression test: the manager once appended flags the recorder never defined
    (``--include-series-comments``, ``--entity-type``), so those captures died
    instantly with argparse ``SystemExit(2)`` while the dashboard reported them
    as running. Round-tripping the built argv through the real parser catches
    any such drift.
    """
    from polytape.cli import build_parser

    seen = _capture_spawned_argv(monkeypatch)
    manager.start_recording("111")  # all defaults
    manager.start_recording("222", log_level="DEBUG")

    parser = build_parser()
    for argv in seen:
        recorder_argv = argv[argv.index("polytape") + 1 :]  # after `<python> -m polytape`
        # SystemExit(2) here means the manager passed a flag the recorder lacks.
        parser.parse_args(recorder_argv)


def test_refuses_second_recorder_for_active_capture(manager, tmp_path):
    # An event dir that looks actively recorded (fresh JSONL, no stopped_at).
    event_dir = tmp_path / "event-777"
    event_dir.mkdir()
    (event_dir / "book.jsonl").write_text('{"stream":"book"}\n', encoding="utf-8")
    (event_dir / "meta.json").write_text(
        json.dumps({"started_at": "2026-01-01T00:00:00Z", "stopped_at": None}), encoding="utf-8"
    )
    with pytest.raises(ControlError, match="already"):
        manager.start_recording("777")  # numeric -> no network; refused before any spawn


# --------------------------------------------------------------------------- #
# Start -> running -> stop lifecycle (spawns the demo feeder)
# --------------------------------------------------------------------------- #


def test_demo_start_stop_lifecycle(manager, tmp_path):
    status = manager.start_demo("demo", rate=50)
    assert status["running"] is True
    assert status["kind"] == "demo"
    assert manager.running_count() == 1

    event_dir = tmp_path / "event-demo"
    assert _wait(lambda: (event_dir / "recorder.log").exists())
    assert _wait(lambda: (event_dir / "meta.json").exists())

    # A second start for the same event while it is running is refused.
    with pytest.raises(ControlError):
        manager.start_demo("demo", rate=10)

    stopped = manager.stop("demo", graceful_timeout=6.0)
    assert stopped["running"] is False
    assert stopped["returncode"] is not None
    assert manager.running_count() == 0


# --------------------------------------------------------------------------- #
# HTTP gating
# --------------------------------------------------------------------------- #


def _http(url, *, method="GET", body=None, headers=None):
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=8) as resp:
            return resp.status, json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read() or b"{}")


@pytest.fixture
def control_server(manager):
    server = make_server(
        CaptureMonitor(manager.out_dir),
        host="127.0.0.1",
        port=0,
        manager=manager,
        control_enabled=True,
        host_allowlist=LOOPBACK_HOSTS,  # Host-pinned, as on a real loopback bind
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}", manager
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_stats_includes_control_block(control_server):
    base, _ = control_server
    status, data = _http(base + "/api/stats")
    assert status == 200
    assert data["control"]["enabled"] is True
    assert data["control"]["recordings"] == []


def test_control_requires_csrf_header(control_server):
    base, _ = control_server
    status, data = _http(
        base + "/api/recordings/start", method="POST", body={"mode": "demo", "event_id": "demo"}
    )  # deliberately no X-Polytape-Control header
    assert status == 403
    assert "header" in data["error"].lower()


def test_start_and_stop_demo_over_http(control_server):
    base, _ = control_server
    status, data = _http(
        base + "/api/recordings/start",
        method="POST",
        body={"mode": "demo", "event_id": "demo", "rate": 40},
        headers=_CTRL_HEADERS,
    )
    assert status == 200 and data["ok"] is True
    assert data["recording"]["running"] is True

    _, stats = _http(base + "/api/stats")
    assert any(r["event_id"] == "demo" for r in stats["control"]["recordings"])

    status, data = _http(
        base + "/api/recordings/stop",
        method="POST",
        body={"event_id": "demo"},
        headers=_CTRL_HEADERS,
    )
    assert status == 200 and data["recording"]["running"] is False


def test_unresolvable_event_ref_over_http_returns_400(control_server):
    base, _ = control_server
    status, data = _http(
        base + "/api/recordings/start",
        method="POST",
        body={"mode": "live", "event_id": "no-such-event-slug"},
        headers=_CTRL_HEADERS,
    )
    assert status == 400
    assert "no-such-event-slug" in data["error"]


def test_legacy_stream_toggles_are_accepted_and_ignored(control_server, monkeypatch):
    """Old clients may still send stream/hash toggles; the recorder has a single
    (book) stream, so the keys are silently ignored and never forwarded."""
    base, manager = control_server
    seen: dict[str, object] = {}

    def fake_start(event_id, **kwargs):
        seen["event_id"] = event_id
        seen["kwargs"] = kwargs
        return {"event_id": event_id, "running": True}

    monkeypatch.setattr(manager, "start_recording", fake_start)
    status, data = _http(
        base + "/api/recordings/start",
        method="POST",
        body={
            "mode": "live",
            "event_id": "123",
            "series_comments": True,
            "comments": False,
            "book": True,
            "hash": False,
        },
        headers=_CTRL_HEADERS,
    )
    assert status == 200 and data["ok"] is True
    assert seen["event_id"] == "123"
    assert seen["kwargs"] == {}


def test_malformed_rate_returns_400_not_500(control_server):
    base, _ = control_server
    # Non-numeric rate must be a validated 400, not a bare 500.
    status, data = _http(
        base + "/api/recordings/start",
        method="POST",
        body={"mode": "demo", "event_id": "demo", "rate": "abc"},
        headers=_CTRL_HEADERS,
    )
    assert status == 400
    assert "rate" in data["error"].lower()
    # rate 0 is out of range (and must not be silently coerced to the default).
    status, _ = _http(
        base + "/api/recordings/start",
        method="POST",
        body={"mode": "demo", "event_id": "demo", "rate": 0},
        headers=_CTRL_HEADERS,
    )
    assert status == 400


def test_related_events_over_http(control_server, monkeypatch):
    base, _ = control_server
    monkeypatch.setattr(
        "polytape.monitor.server.related_events",
        lambda ref, **kw: {
            "source_event_id": "351730",
            "series_id": "11433",
            "series_title": "FIFA World Cup",
            "events": [{"event_id": "351731", "title": "France vs. Senegal", "closed": False}],
        },
    )
    status, data = _http(
        base + "/api/related",
        method="POST",
        body={"ref": "https://polymarket.com/sports/world-cup/fifwc-irn-nzl"},
        headers=_CTRL_HEADERS,
    )
    assert status == 200 and data["ok"] is True
    assert data["series_title"] == "FIFA World Cup"
    assert data["events"][0]["event_id"] == "351731"


def test_related_requires_ref(control_server):
    base, _ = control_server
    status, data = _http(base + "/api/related", method="POST", body={}, headers=_CTRL_HEADERS)
    assert status == 400


def test_control_rejects_foreign_host(control_server):
    """DNS-rebinding guard: a non-loopback Host header is rejected."""
    base, _ = control_server
    status, data = _http(
        base + "/api/recordings/start",
        method="POST",
        body={"mode": "demo", "event_id": "demo"},
        headers={**_CTRL_HEADERS, "Host": "evil.example"},
    )
    assert status == 403
    assert "host" in data["error"].lower()


def test_read_only_server_rejects_control(tmp_path):
    manager = RecorderManager(tmp_path)
    server = make_server(
        CaptureMonitor(tmp_path),
        host="127.0.0.1",
        port=0,
        manager=manager,
        control_enabled=False,
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        status, data = _http(
            base + "/api/recordings/start",
            method="POST",
            body={"mode": "demo"},
            headers=_CTRL_HEADERS,
        )
        assert status == 403
        assert "disabled" in data["error"].lower() or "read-only" in data["error"].lower()
        _, stats = _http(base + "/api/stats")
        assert stats["control"]["enabled"] is False
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
