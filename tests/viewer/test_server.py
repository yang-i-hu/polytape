"""Integration tests: a real ThreadingHTTPServer on an ephemeral port (offline)."""

from __future__ import annotations

import http.client
import json
import threading
import urllib.error
import urllib.request

import pytest

from polytape.viewer.config import ViewerConfig
from polytape.viewer.server import AppContext, ViewerHTTPServer


@pytest.fixture
def server(make_book_capture):
    event_dir = make_book_capture("20200")
    cfg = ViewerConfig(
        data_root=event_dir.parent,
        event_id="20200",
        event_dir_override=event_dir,
        port=8770,
        poll_interval=0.05,
        open_browser=False,
    )
    app = AppContext(cfg)
    httpd = ViewerHTTPServer(("127.0.0.1", 0), app)
    port = httpd.server_address[1]
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        yield port
    finally:
        app.close()
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=2)


def _get(port, path):
    with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=5) as resp:
        body = resp.read().decode("utf-8")
        ctype = resp.headers.get("Content-Type", "")
        return resp.status, ctype, body


def _get_json(port, path):
    status, _ctype, body = _get(port, path)
    return status, json.loads(body)


def test_index_and_static(server):
    status, ctype, body = _get(server, "/")
    assert status == 200 and "polytape" in body
    status, ctype, _ = _get(server, "/static/js/main.js")
    assert status == 200 and "javascript" in ctype


def test_path_traversal_blocked(server):
    with pytest.raises(urllib.error.HTTPError) as exc:
        _get(server, "/static/../server.py")
    assert exc.value.code == 404


def test_events_and_meta(server):
    _, events = _get_json(server, "/api/v1/events")
    assert events["events"][0]["event_id"] == "20200"
    _, meta = _get_json(server, "/api/v1/events/20200/meta")
    assert {a["outcome"] for a in meta["assets"]} >= {"YES", "NO"}
    assert len(meta["gaps"]) == 1


def test_book_endpoints(server):
    _, yes = _get_json(server, "/api/v1/events/20200/book?asset=100&depth=5")
    assert yes["metrics"]["best_bid"] == 0.45 and yes["ladder"]["asks"][0]["price"] == 0.47
    _, empty = _get_json(server, "/api/v1/events/20200/book?asset=200")
    assert empty["metrics"]["mid"] is None and empty["ladder"]["bids"] == []


def test_book_requires_asset(server):
    with pytest.raises(urllib.error.HTTPError) as exc:
        _get(server, "/api/v1/events/20200/book")
    assert exc.value.code == 400


def test_series_and_trades_endpoints(server):
    _, series = _get_json(server, "/api/v1/events/20200/series?asset=100&max=100")
    assert isinstance(series["points"], list)
    _, trades = _get_json(server, "/api/v1/events/20200/trades?asset=100")
    assert trades["trades"][0]["side"] == "BUY"


def test_unknown_event_404(server):
    with pytest.raises(urllib.error.HTTPError) as exc:
        _get(server, "/api/v1/events/99999/meta")
    assert exc.value.code == 404


def test_event_id_traversal_blocked(server):
    # raw backslash in the id must not escape data_root (path-traversal guard).
    conn = http.client.HTTPConnection("127.0.0.1", server, timeout=5)
    conn.request("GET", "/api/v1/events/x\\..\\..\\..\\Windows/meta")
    resp = conn.getresponse()
    resp.read()
    conn.close()
    assert resp.status == 404


def test_book_bad_at_returns_400(server):
    with pytest.raises(urllib.error.HTTPError) as exc:
        _get(server, "/api/v1/events/20200/book?asset=100&at=not-a-timestamp")
    assert exc.value.code == 400


def test_sse_sends_hello_and_snapshot(server):
    conn = http.client.HTTPConnection("127.0.0.1", server, timeout=5)
    conn.request("GET", "/api/v1/events/20200/stream?asset=100")
    resp = conn.getresponse()
    try:
        assert resp.status == 200
        assert "text/event-stream" in resp.getheader("Content-Type")
        data = b""
        # ended capture: server sends hello + snapshot + eof, then closes.
        while b"event: snapshot" not in data:
            chunk = resp.read(64)
            if not chunk:
                break
            data += chunk
        assert b"event: hello" in data
        assert b"event: snapshot" in data
        assert b'"asset_id": "100"' in data
    finally:
        conn.close()
