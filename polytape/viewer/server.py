"""Stdlib HTTP + Server-Sent-Events server for the viewer (thin I/O, no domain logic).

A :class:`ThreadingHTTPServer` serves a tiny versioned JSON API plus an SSE stream
over :class:`~polytape.viewer.store.CaptureStore` state, and the static SPA assets.
Threading (not plain ``HTTPServer``) is required because an SSE connection is
long-lived and would otherwise block every other request. All interpretation lives
in the store/api/reconstruct layers; this module only routes, serializes, and
streams.
"""

from __future__ import annotations

import json
import logging
import mimetypes
import queue
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from polytape.envelope import iso_to_datetime
from polytape.viewer import api
from polytape.viewer.config import ViewerConfig
from polytape.viewer.reader import CaptureReader
from polytape.viewer.store import FRAME_DEPTH, CaptureStore

logger = logging.getLogger("polytape.viewer.server")

STATIC_DIR = Path(__file__).parent / "static"
_API_PREFIX = "/api/v1/events"
_HEARTBEAT_SECONDS = 15.0
_CONTENT_TYPES = {
    ".html": "text/html; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".js": "text/javascript; charset=utf-8",
    ".mjs": "text/javascript; charset=utf-8",
    ".svg": "image/svg+xml",
    ".json": "application/json",
    ".ico": "image/x-icon",
}


class AppContext:
    """Owns the per-event store registry and resolves event directories."""

    def __init__(self, config: ViewerConfig) -> None:
        self.config = config
        self._stores: dict[str, CaptureStore] = {}
        self._lock = threading.Lock()
        self.stopping = threading.Event()

    def _event_dir(self, event_id: str) -> Path | None:
        cfg = self.config
        if cfg.single_event and event_id == cfg.event_id:
            return cfg.event_dir
        if (
            cfg.event_dir_override is not None
            and event_id == cfg.event_dir_override.name[len("event-") :]
        ):
            return cfg.event_dir_override
        # Untrusted id from the URL: it must be a single path component and the
        # resolved dir must stay inside data_root (block ../ and \..\ traversal).
        if not _is_safe_event_id(event_id):
            return None
        candidate = cfg.data_root / f"event-{event_id}"
        try:
            if not candidate.resolve().is_relative_to(cfg.data_root.resolve()):
                return None
        except OSError:
            return None
        return candidate

    def get_store(self, event_id: str) -> CaptureStore | None:
        with self._lock:
            store = self._stores.get(event_id)
            if store is not None:
                return store
            event_dir = self._event_dir(event_id)
            if event_dir is None or not event_dir.is_dir():
                return None
            store = CaptureStore(
                event_dir,
                keyframe_every=self.config.keyframe_every,
                poll_interval=self.config.poll_interval,
            )
            store.start()
            self._stores[event_id] = store
            return store

    def event_summaries(self) -> list[dict[str, Any]]:
        cfg = self.config
        if cfg.single_event and cfg.event_dir is not None:
            reader = CaptureReader(cfg.event_dir)
            event_id = cfg.event_id or cfg.event_dir.name[len("event-") :]
            return [api.event_summary(event_id, reader.read_meta())]
        summaries = []
        for entry in CaptureReader.list_events(cfg.data_root):
            reader = CaptureReader(cfg.data_root / entry["dir"])
            summaries.append(api.event_summary(entry["event_id"], reader.read_meta()))
        return summaries

    def close(self) -> None:
        self.stopping.set()
        with self._lock:
            for store in self._stores.values():
                store.stop()
            self._stores.clear()


class ViewerHTTPServer(ThreadingHTTPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, addr: tuple[str, int], app: AppContext) -> None:
        super().__init__(addr, ViewerRequestHandler)
        self.app = app


class ViewerRequestHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "polytape-view"

    @property
    def app(self) -> AppContext:
        return self.server.app  # type: ignore[attr-defined]

    def log_message(self, fmt: str, *args: Any) -> None:  # quiet by default
        logger.debug("%s - %s", self.address_string(), fmt % args)

    def handle_one_request(self) -> None:
        # Browsers and EventSource routinely drop connections; treat that as a
        # normal close rather than letting socketserver print a traceback.
        try:
            super().handle_one_request()
        except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError):
            self.close_connection = True

    # -- routing ------------------------------------------------------------ #

    def do_GET(self) -> None:  # noqa: N802 (stdlib API)
        try:
            parsed = urlparse(self.path)
            path = parsed.path
            params = parse_qs(parsed.query)
            if path in ("/", "/index.html"):
                self._send_static("index.html")
            elif path.startswith("/static/"):
                self._send_static(path[len("/static/") :])
            elif path == _API_PREFIX:
                self._send_json(api.build_events_response(self.app.event_summaries()))
            elif path.startswith(_API_PREFIX + "/"):
                self._route_event_api(path, params)
            else:
                self._send_error_json(404, "not found")
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            pass
        except Exception:
            logger.exception("error handling %s", self.path)
            try:
                self._send_error_json(500, "internal error")
            except OSError:
                pass

    def _route_event_api(self, path: str, params: dict[str, list[str]]) -> None:
        rest = path[len(_API_PREFIX + "/") :]
        segments = rest.split("/")
        if len(segments) < 2:
            self._send_error_json(404, "not found")
            return
        event_id, resource = segments[0], segments[1]
        store = self.app.get_store(event_id)
        if store is None:
            self._send_error_json(404, f"no capture for event {event_id}")
            return

        if resource == "meta":
            payload = api.build_meta_response(store)
            self._send_json(payload) if payload else self._send_error_json(404, "no meta yet")
        elif resource == "book":
            at = _one(params, "at")
            if at is not None and iso_to_datetime(at) is None:
                self._send_error_json(400, "invalid 'at' timestamp")
                return
            self._require_asset(
                params,
                lambda asset: self._send_or_404(
                    api.build_book_response(store, asset, at, _int(params, "depth", 25))
                ),
            )
        elif resource == "series":
            self._require_asset(
                params,
                lambda asset: self._send_or_404(
                    api.build_series_response(
                        store,
                        asset,
                        _one(params, "from"),
                        _one(params, "to"),
                        _int(params, "max", 1500),
                    )
                ),
            )
        elif resource == "trades":
            self._require_asset(
                params,
                lambda asset: self._send_or_404(
                    api.build_trades_response(
                        store, asset, _one(params, "before"), _int(params, "limit", 100)
                    )
                ),
            )
        elif resource == "stream":
            self._stream(store, _one(params, "asset"))
        else:
            self._send_error_json(404, "not found")

    def _require_asset(self, params: dict[str, list[str]], fn: Any) -> None:
        asset = _one(params, "asset")
        if not asset:
            self._send_error_json(400, "asset query parameter is required")
            return
        fn(asset)

    def _send_or_404(self, payload: dict[str, Any] | None) -> None:
        if payload is None:
            self._send_error_json(404, "unknown asset")
        else:
            self._send_json(payload)

    # -- responses ---------------------------------------------------------- #

    def _send_json(self, payload: dict[str, Any], status: int = 200) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _send_error_json(self, status: int, message: str) -> None:
        self._send_json({"error": message}, status=status)

    def _send_static(self, rel: str) -> None:
        base = STATIC_DIR.resolve()
        target = (base / rel).resolve()
        if not target.is_relative_to(base) or not target.is_file():
            self._send_error_json(404, "not found")
            return
        body = target.read_bytes()
        ctype = _CONTENT_TYPES.get(target.suffix.lower()) or (
            mimetypes.guess_type(target.name)[0] or "application/octet-stream"
        )
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(body)

    # -- SSE ---------------------------------------------------------------- #

    def _stream(self, store: CaptureStore, asset: str | None) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Connection", "close")
        self.send_header("X-Accel-Buffering", "no")
        self.end_headers()
        self.close_connection = True

        sub = store.subscribe(asset)
        try:
            self._write_frame(
                "hello", None, {"live": store.is_live(), "last_seq": store.last_seq()}
            )
            for entry in store.assets():
                if asset and entry["asset_id"] != asset:
                    continue
                if not entry.get("present"):
                    continue
                state = api.build_book_response(store, entry["asset_id"], None, FRAME_DEPTH)
                if state is not None:
                    self._write_frame("snapshot", state.get("seq"), state)
            self._pump(store, sub)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            pass
        finally:
            store.unsubscribe(sub)

    def _pump(self, store: CaptureStore, sub: Any) -> None:
        idle = 0.0
        while not self.app.stopping.is_set():
            try:
                frame = sub.queue.get(timeout=1.0)
            except queue.Empty:
                idle += 1.0
                if idle >= _HEARTBEAT_SECONDS:  # keep the connection warm
                    self._write_comment("keepalive")
                    idle = 0.0
                continue
            idle = 0.0
            self._write_frame(frame.get("event"), frame.get("id"), frame.get("data"))
            if frame.get("event") == "eof":
                return

    def _write_frame(self, event: str | None, frame_id: Any, data: Any) -> None:
        chunk = []
        if frame_id is not None:
            chunk.append(f"id: {frame_id}\n")
        if event:
            chunk.append(f"event: {event}\n")
        chunk.append(f"data: {json.dumps(data)}\n\n")
        self.wfile.write("".join(chunk).encode("utf-8"))
        self.wfile.flush()

    def _write_comment(self, text: str) -> None:
        self.wfile.write(f": {text}\n\n".encode())
        self.wfile.flush()


def _is_safe_event_id(event_id: str) -> bool:
    """An event id must be a single path component — no separators or traversal."""
    return (
        bool(event_id)
        and event_id not in (".", "..")
        and "/" not in event_id
        and "\\" not in event_id
        and event_id == Path(event_id).name
    )


def _one(params: dict[str, list[str]], key: str) -> str | None:
    values = params.get(key)
    return values[0] if values else None


def _int(params: dict[str, list[str]], key: str, default: int) -> int:
    raw = _one(params, key)
    try:
        return int(raw) if raw is not None else default
    except ValueError:
        return default


def create_server(config: ViewerConfig) -> tuple[ViewerHTTPServer, AppContext]:
    """Build (but do not serve) the viewer server for ``config``."""
    if not STATIC_DIR.is_dir():
        raise RuntimeError(f"static assets directory missing: {STATIC_DIR}")
    app = AppContext(config)
    httpd = ViewerHTTPServer((config.host, config.port), app)
    return httpd, app
