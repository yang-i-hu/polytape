"""Stdlib HTTP layer for the monitor dashboard.

A :class:`http.server.ThreadingHTTPServer` serves the dashboard, a JSON snapshot,
and — only when control is enabled — start/stop actions for capture processes:

* ``GET  /``                     — the self-contained dashboard page.
* ``GET  /api/stats``            — JSON snapshot (``?event=<id>`` to pin one).
* ``GET  /healthz``              — liveness probe.
* ``POST /api/recordings/start`` — launch a live capture or demo feed.
* ``POST /api/recordings/stop``  — gracefully stop a managed capture.

Read snapshots mutate the reader's tail state, so each is serialized with a lock
(cheap — a couple of small file reads). Control actions go through the manager's
own lock and are deliberately *not* taken under the read lock, so a graceful stop
(which may wait several seconds) never blocks the dashboard's polling.

Control is gated three ways:

* **Loopback only.** ``control_enabled`` is set by the launcher and is false when
  bound to a non-loopback host (unless explicitly overridden). The dashboard
  surfaces no payload content, but spawning processes is a real capability.
* **CSRF / drive-by guard.** Control requests must carry ``X-Polytape-Control: 1``.
  A browser cannot set a custom header on a *cross-origin* request without a CORS
  preflight, which this server never approves — so a foreign page cannot POST.
* **Host pinning (anti DNS-rebinding).** On a loopback bind, every request whose
  ``Host`` header is not a loopback name is rejected. Rebinding makes a foreign
  page same-origin (defeating the header guard alone), so pinning ``Host`` is
  what actually closes that hole.

No external dependencies — only the Python standard library.
"""

from __future__ import annotations

import json
import logging
import threading
from functools import partial
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib import resources
from urllib.parse import parse_qs, urlparse

from polytape.gamma import GammaError, related_events
from polytape.monitor.control import ControlError, RecorderManager
from polytape.monitor.reader import CaptureMonitor
from polytape.streams.discover import active_chat_events

logger = logging.getLogger("polytape.monitor")

_NO_STORE = {"Cache-Control": "no-store"}
_MAX_BODY = 64 * 1024  # control payloads are tiny; cap to avoid abuse.
_CONTROL_HEADER = "X-Polytape-Control"
LOOPBACK_HOSTS = frozenset({"127.0.0.1", "localhost", "::1"})


def _load_index() -> bytes:
    """Read the bundled dashboard HTML (works from source tree and installed wheel)."""
    return resources.files("polytape.monitor").joinpath("index.html").read_bytes()


def _host_in_allowlist(host_header: str, allowlist: frozenset[str]) -> bool:
    """True if the request ``Host`` header's hostname is in ``allowlist``."""
    host = (host_header or "").strip()
    if not host:
        return False
    if host.startswith("["):  # bracketed IPv6, e.g. [::1]:8787
        host = host[1:].split("]", 1)[0]
    elif ":" in host:
        host = host.rsplit(":", 1)[0]
    return host.lower() in allowlist


class _Handler(BaseHTTPRequestHandler):
    """Routes the dashboard, the stats API, and (optional) control actions."""

    protocol_version = "HTTP/1.1"
    server_version = "polytape-monitor"

    def __init__(
        self,
        *args: object,
        monitor: CaptureMonitor,
        manager: RecorderManager | None,
        control_enabled: bool,
        host_allowlist: frozenset[str] | None,
        lock: threading.Lock,
        active_chat_lock: threading.Lock,
        **kwargs: object,
    ) -> None:
        self._monitor = monitor
        self._manager = manager
        self._control_enabled = control_enabled and manager is not None
        self._host_allowlist = host_allowlist
        self._lock = lock
        self._active_chat_lock = active_chat_lock
        super().__init__(*args, **kwargs)  # type: ignore[arg-type]

    def _reject_foreign_host(self) -> bool:
        """Reject (and respond 403) a request whose Host is off the allowlist."""
        if self._host_allowlist is None:
            return False
        if _host_in_allowlist(self.headers.get("Host", ""), self._host_allowlist):
            return False
        self._send_json({"error": "host not allowed"}, status=403)
        return True

    # Quiet by default: a dashboard polling once a second would flood the console.
    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        logger.debug("%s - %s", self.address_string(), format % args)

    # -- response helpers --------------------------------------------------- #

    def _send(self, status: int, body: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        for key, value in _NO_STORE.items():
            self.send_header(key, value)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def _send_json(self, payload: object, status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
        self._send(status, body, "application/json; charset=utf-8")

    # -- GET ---------------------------------------------------------------- #

    def do_HEAD(self) -> None:  # noqa: N802
        self.do_GET()

    def do_GET(self) -> None:  # noqa: N802
        route = urlparse(self.path)
        path = route.path
        try:
            if self._reject_foreign_host():
                return
            if path in ("/", "/index.html"):
                self._send(200, _load_index(), "text/html; charset=utf-8")
            elif path == "/healthz":
                self._send_json({"ok": True})
            elif path == "/api/stats":
                event = (parse_qs(route.query).get("event") or [None])[0]
                with self._lock:
                    snapshot = self._monitor.snapshot(event_id=event)
                snapshot["control"] = {
                    "enabled": self._control_enabled,
                    "recordings": self._manager.statuses() if self._manager else [],
                }
                self._send_json(snapshot)
            else:
                self._send_json({"error": "not found", "path": path}, status=404)
        except BrokenPipeError:
            pass  # client navigated away mid-response; nothing to do.
        except Exception:  # noqa: BLE001 — one bad request must not kill the thread.
            logger.exception("error handling GET %s", self.path)
            self._safe_500()

    # -- POST (control) ----------------------------------------------------- #

    def do_POST(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        try:
            # Drain the request body first, always — responding while bytes remain
            # unread desyncs the keep-alive connection (and aborts it on Windows).
            raw = self._drain_body()
            if self._reject_foreign_host():  # anti DNS-rebinding (after draining)
                return
            if not self._control_enabled:
                self._send_json({"error": "control is disabled (dashboard is read-only)"}, 403)
                return
            if self.headers.get(_CONTROL_HEADER) != "1":
                self._send_json({"error": "missing control header"}, 403)
                return
            body = self._parse_json(raw)
            if path == "/api/recordings/start":
                self._send_json({"ok": True, "recording": self._do_start(body)})
            elif path == "/api/recordings/stop":
                event_id = body.get("event_id")
                if not event_id:
                    raise ControlError("event_id is required")
                self._send_json({"ok": True, "recording": self._manager.stop(event_id)})
            elif path == "/api/related":
                ref = str(body.get("ref") or "").strip()
                if not ref:
                    raise ControlError("ref is required")
                try:
                    result = related_events(ref)
                except GammaError as exc:
                    raise ControlError(str(exc)) from exc
                self._send_json({"ok": True, **result})
            elif path == "/api/active-chat":
                self._send_json({"ok": True, **self._do_active_chat(body)})
            else:
                self._send_json({"error": "not found", "path": path}, status=404)
        except ControlError as exc:
            self._send_json({"error": str(exc)}, status=400)
        except BrokenPipeError:
            pass
        except Exception:  # noqa: BLE001
            logger.exception("error handling POST %s", self.path)
            self._safe_500()

    def _drain_body(self) -> bytes:
        """Read (consume) the request body so the connection stays aligned."""
        try:
            length = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            self.close_connection = True
            raise ControlError("invalid Content-Length") from None
        if length <= 0:
            return b""
        if length > _MAX_BODY:
            self.close_connection = True  # don't try to drain an oversized body
            raise ControlError("request body too large")
        return self.rfile.read(length)

    def _parse_json(self, raw: bytes) -> dict[str, object]:
        if not raw:
            return {}
        try:
            parsed = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError):
            raise ControlError("invalid JSON body") from None
        if not isinstance(parsed, dict):
            raise ControlError("body must be a JSON object")
        return parsed

    def _do_start(self, body: dict[str, object]) -> dict[str, object]:
        assert self._manager is not None  # guarded by _control_enabled
        mode = str(body.get("mode") or "live")
        event_id = str(body.get("event_id") or "").strip()
        if mode == "demo":
            # Pass the raw value through; start_demo validates type and range and
            # raises ControlError -> 400 (don't pre-convert and risk a bare 500).
            rate = body.get("rate")
            return self._manager.start_demo(event_id or "demo", rate=8.0 if rate is None else rate)
        if mode == "live":
            return self._manager.start_recording(
                event_id,
                comments=bool(body.get("comments", True)),
                book=bool(body.get("book", True)),
                hash_usernames=bool(body.get("hash", True)),
                include_series_comments=bool(body.get("series_comments", False)),
                entity_type=str(body.get("entity_type") or "Event"),
            )
        raise ControlError(f"unknown start mode {mode!r}")

    def _do_active_chat(self, body: dict[str, object]) -> dict[str, object]:
        """Sample the live comments firehose and rank events by chat volume.

        Read-only (same public firehose the recorder uses); held behind the
        control gate because it briefly ties up a worker thread on an outbound
        connection and naturally pairs with starting a recording. Serialized with
        a non-blocking lock so repeated/concurrent clicks can't pile up parallel
        firehose connections (the client disables its button, but the server must
        not rely on that).
        """
        try:
            seconds = float(body.get("seconds", 8.0))
        except (TypeError, ValueError):
            raise ControlError("seconds must be a number") from None
        if not self._active_chat_lock.acquire(blocking=False):
            raise ControlError("a chat scan is already in progress; try again in a moment")
        try:
            return active_chat_events(seconds)
        finally:
            self._active_chat_lock.release()

    def _safe_500(self) -> None:
        try:
            self._send_json({"error": "internal error"}, status=500)
        except Exception:  # noqa: BLE001
            pass


def make_server(
    monitor: CaptureMonitor,
    *,
    host: str = "127.0.0.1",
    port: int = 8787,
    manager: RecorderManager | None = None,
    control_enabled: bool = False,
    host_allowlist: frozenset[str] | None = None,
) -> ThreadingHTTPServer:
    """Build (but do not start) a threaded HTTP server bound to ``host:port``.

    ``host_allowlist`` (when set) pins the request ``Host`` header to those
    hostnames — pass it on a loopback bind to block DNS-rebinding; leave it
    ``None`` to skip the check (e.g. a deliberate non-loopback bind).
    """
    lock = threading.Lock()
    handler = partial(
        _Handler,
        monitor=monitor,
        manager=manager,
        control_enabled=control_enabled,
        host_allowlist=host_allowlist,
        lock=lock,
        active_chat_lock=threading.Lock(),
    )
    server = ThreadingHTTPServer((host, port), handler)
    server.daemon_threads = True
    return server
