"""FastAPI sidecar serving the read-only polytape admin dashboard.

Read-only endpoints (always present):
- ``GET /``                    -> the dashboard page
- ``GET /api/status``          -> recorder health, freshness, coverage, disk, counts, gaps
- ``GET /api/matches``         -> per-match counts, recency, and live/quiet status
- ``GET /api/matches/{id}``    -> one match's reconstructed L2 book / mid / last trade
- ``GET /api/live``            -> records/sec, most-recent records, recent gaps (poll)
- ``GET /api/session``         -> whether controls are enabled and the caller is logged in

Guarded control plane (only when ``POLYTAPE_ADMIN_TOKEN`` is set):
- ``POST /api/login``          -> exchange the shared secret for a SameSite session cookie
- ``POST /api/logout``         -> drop the session
- ``POST /api/control/{action}`` -> restart | refresh | arm-heartbeat (typed-confirm,
  rate-limited, audited; never runs systemctl itself — see :mod:`polytape.admin.control`)

Binds to localhost by default — reach it through an SSH tunnel
(``gcloud compute ssh polytape-rec -- -L 8080:localhost:8080``), so there is no
public port and no new firewall hole. fastapi/uvicorn are an optional extra
(``pip install 'polytape[admin]'``); importing this module does not require them.
"""

import argparse
import asyncio
import contextlib
import os

from polytape.admin.page import PAGE
from polytape.admin.reader import RunReader


def create_app(
    reader: RunReader,
    *,
    poll_interval: float = 2.0,
    admin_token: str | None = None,
    broker=None,
    audit=None,
    sessions=None,
    rate_limiter=None,
):
    """Build the FastAPI app over a :class:`RunReader` (fastapi imported lazily).

    The read-only endpoints are always present. The guarded control plane (login +
    the mutating actions) is enabled ONLY when ``admin_token`` is set — a mandatory
    shared secret. With it unset the control endpoints return 503 and the recorder
    cannot be touched from the dashboard at all. ``broker``/``audit``/``sessions``/
    ``rate_limiter`` are injectable so the control plane is tested with no real
    ``systemctl`` or privileged filesystem.
    """
    from contextlib import asynccontextmanager

    from fastapi import FastAPI, Request
    from fastapi.responses import HTMLResponse, JSONResponse

    from polytape.admin import control

    controls_on = bool(admin_token)
    if controls_on:
        sessions = sessions or control.Sessions()
        rate_limiter = rate_limiter or control.RateLimiter()
        login_throttle = control.LoginThrottle()
        if audit is None:
            audit = control.AuditLog(
                os.environ.get("POLYTAPE_AUDIT_DIR", "/var/log/polytape-admin/audit.jsonl")
            )
        if broker is None:
            broker = control.IntentBroker(
                os.environ.get("POLYTAPE_INTENT_DIR", "/run/polytape-admin/intent")
            )
    inflight = asyncio.Lock()  # at most one mutation in flight (anti-fat-finger)

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        async def _loop() -> None:
            # Single-threaded: update() runs in the event loop between requests,
            # so there is no race with the (sync) status()/matches() reads.
            while True:
                with contextlib.suppress(Exception):
                    reader.update()
                await asyncio.sleep(poll_interval)

        task = asyncio.create_task(_loop())
        try:
            yield
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    app = FastAPI(title="polytape-admin", docs_url=None, redoc_url=None, lifespan=lifespan)

    @app.get("/", response_class=HTMLResponse)
    async def index() -> str:
        return PAGE

    @app.get("/api/status")
    async def status() -> JSONResponse:
        return JSONResponse(reader.status())

    @app.get("/api/matches")
    async def matches() -> JSONResponse:
        return JSONResponse(reader.matches())

    @app.get("/api/matches/{event_id}")
    async def match(event_id: str) -> JSONResponse:
        return JSONResponse(reader.match_view(event_id))

    @app.get("/api/live")
    async def live() -> JSONResponse:
        # Poll-based live view (rates + recent records + gaps). Cheap: it reads
        # only state the update() loop already maintains, so no file I/O per request.
        return JSONResponse(reader.live())

    # -- guarded control plane (present only when a secret is configured) ---- #

    def _source(request: Request) -> str:
        return request.client.host if request.client else "?"

    def _json_request(request: Request) -> bool:
        # CSRF defense: a cross-site HTML form cannot send application/json without a
        # CORS preflight we never grant, so requiring it (plus the SameSite cookie)
        # stops a forged cross-origin POST from riding the session cookie.
        ct = request.headers.get("content-type", "").split(";")[0].strip().lower()
        return ct == "application/json"

    @app.get("/api/session")
    async def session(request: Request) -> JSONResponse:
        authed = controls_on and sessions.valid(request.cookies.get("polytape_admin"))
        return JSONResponse(
            {
                "controls_enabled": controls_on,
                "authed": bool(authed),
                "actions": sorted(control.ACTIONS) if authed else [],
            }
        )

    @app.post("/api/login")
    async def login(request: Request) -> JSONResponse:
        if not controls_on:
            return JSONResponse({"error": "controls disabled"}, status_code=503)
        if not _json_request(request):
            return JSONResponse({"error": "application/json required"}, status_code=415)
        if login_throttle.locked():
            audit.write(action="login", result="lockedout", source=_source(request))
            return JSONResponse({"error": "too many attempts; wait a minute"}, status_code=429)
        try:
            body = await request.json()
        except Exception:
            body = {}
        if not isinstance(body, dict):
            body = {}
        if not control.token_ok(body.get("token"), admin_token):
            login_throttle.record_failure()
            await asyncio.sleep(0.25)  # fixed per-failure delay blunts brute force
            audit.write(action="login", result="denied", source=_source(request))
            return JSONResponse({"error": "invalid token"}, status_code=403)
        login_throttle.record_success()
        sid = sessions.mint()
        audit.write(
            action="login",
            result="ok",
            source=_source(request),
            session_fp=control.fingerprint(sid),
        )
        resp = JSONResponse({"ok": True})
        resp.set_cookie(
            "polytape_admin", sid, max_age=1800, httponly=True, samesite="strict", path="/"
        )
        return resp

    @app.post("/api/logout")
    async def logout(request: Request) -> JSONResponse:
        if controls_on:
            sid = request.cookies.get("polytape_admin")
            if sessions.valid(sid):
                audit.write(
                    action="logout",
                    result="ok",
                    source=_source(request),
                    session_fp=control.fingerprint(sid),
                )
            sessions.drop(sid)
        resp = JSONResponse({"ok": True})
        resp.delete_cookie("polytape_admin", path="/")
        return resp

    @app.post("/api/control/{action}")
    async def do_control(action: str, request: Request) -> JSONResponse:
        if not controls_on:
            return JSONResponse({"error": "controls disabled"}, status_code=503)
        if not _json_request(request):
            return JSONResponse({"error": "application/json required"}, status_code=415)
        src = _source(request)
        sid = request.cookies.get("polytape_admin")
        if not sessions.valid(sid):
            audit.write(action=action, result="unauthorized", source=src)
            return JSONResponse({"error": "not authenticated"}, status_code=403)
        fp = control.fingerprint(sid)
        policy = control.ACTIONS.get(action)
        if policy is None:
            audit.write(action=action, result="unknown", source=src, session_fp=fp)
            return JSONResponse({"error": "unknown action"}, status_code=404)
        try:
            body = await request.json()
        except Exception:
            body = {}
        if not isinstance(body, dict):
            body = {}
        if body.get("confirm") != policy["confirm"]:  # re-verified server-side
            audit.write(action=action, result="bad-confirm", source=src, session_fp=fp)
            return JSONResponse(
                {"error": f"type '{policy['confirm']}' to confirm"}, status_code=400
            )
        url = None
        if policy["needs_url"]:
            url = body.get("url")
            if not control.validate_heartbeat_url(url):
                audit.write(action=action, result="bad-url", source=src, session_fp=fp)
                return JSONResponse({"error": "invalid https url"}, status_code=400)
        # No await between the locked() check and acquiring the lock below, so the
        # in-flight guard + rate-limit decision are race-free on the single loop.
        if inflight.locked():
            audit.write(action=action, result="inflight", source=src, session_fp=fp)
            return JSONResponse({"error": "another action is in flight"}, status_code=409)
        if not rate_limiter.allow(action, policy["min_interval_s"]):
            audit.write(action=action, result="ratelimited", source=src, session_fp=fp)
            return JSONResponse({"error": "rate limited; wait and retry"}, status_code=429)
        fields = {"action": action, "source": src, "session_fp": fp}
        if url:
            fields["url_fp"] = control.fingerprint(url)  # never log the raw URL
        async with inflight:
            try:
                await asyncio.to_thread(broker.dispatch, action, url=url)
            except Exception as exc:  # noqa: BLE001 - audit + generic error, never leak
                audit.write(result="error", error=type(exc).__name__, **fields)
                return JSONResponse({"error": "dispatch failed"}, status_code=500)
        audit.write(result="ok", **fields)
        return JSONResponse({"ok": True, "action": action})

    return app


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="polytape-admin",
        description="Read-only admin dashboard sidecar for a polytape run (localhost).",
    )
    parser.add_argument("--run-dir", default=os.environ.get("POLYTAPE_RUN_DIR", "/data/run-wc"))
    parser.add_argument("--unit", default=os.environ.get("POLYTAPE_UNIT", "polytape"))
    parser.add_argument(
        "--env-file", default=os.environ.get("POLYTAPE_ENV_FILE", "/etc/polytape/polytape.env")
    )
    parser.add_argument(
        "--matches-file",
        default=os.environ.get("POLYTAPE_MATCHES_FILE", "/etc/polytape/wc_matches.json"),
    )
    parser.add_argument("--host", default=os.environ.get("POLYTAPE_ADMIN_HOST", "127.0.0.1"))
    parser.add_argument(
        "--port", type=int, default=int(os.environ.get("POLYTAPE_ADMIN_PORT", "8080"))
    )
    args = parser.parse_args(argv)

    reader = RunReader(
        args.run_dir, unit=args.unit, env_file=args.env_file, matches_file=args.matches_file
    )
    with contextlib.suppress(Exception):
        reader.update()  # warm once so the first request has data

    # Controls are OFF unless a shared secret is configured (defense in depth: the
    # localhost bind + SSH tunnel are necessary but not sufficient on their own).
    admin_token = os.environ.get("POLYTAPE_ADMIN_TOKEN") or None

    import uvicorn

    uvicorn.run(
        create_app(reader, admin_token=admin_token),
        host=args.host,
        port=args.port,
        log_level="warning",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
