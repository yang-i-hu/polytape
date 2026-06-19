"""FastAPI sidecar serving the read-only polytape admin dashboard.

Endpoints (all read-only in this phase):
- ``GET /``            -> the dashboard page
- ``GET /api/status``  -> recorder health, freshness, coverage, disk, counts
- ``GET /api/matches`` -> per-match counts, recency, and live/quiet status

Binds to localhost by default — reach it through an SSH tunnel
(``gcloud compute ssh polytape-rec -- -L 8080:localhost:8080``), so there is no
public port and no new firewall hole. fastapi/uvicorn are an optional extra
(``pip install 'polytape[admin]'``); importing this module does not require them.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import os

from polytape.admin.page import PAGE
from polytape.admin.reader import RunReader


def create_app(reader: RunReader, *, poll_interval: float = 2.0):
    """Build the FastAPI app over a :class:`RunReader` (fastapi imported lazily)."""
    from contextlib import asynccontextmanager

    from fastapi import FastAPI
    from fastapi.responses import HTMLResponse, JSONResponse

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
    parser.add_argument("--host", default=os.environ.get("POLYTAPE_ADMIN_HOST", "127.0.0.1"))
    parser.add_argument(
        "--port", type=int, default=int(os.environ.get("POLYTAPE_ADMIN_PORT", "8080"))
    )
    args = parser.parse_args(argv)

    reader = RunReader(args.run_dir, unit=args.unit, env_file=args.env_file)
    with contextlib.suppress(Exception):
        reader.update()  # warm once so the first request has data

    import uvicorn

    uvicorn.run(create_app(reader), host=args.host, port=args.port, log_level="warning")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
