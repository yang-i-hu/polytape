# polytape

Passive recorder for Polymarket's public real-time CLOB order-book feed, written
to timestamped JSONL. **This repo is purely for getting data** — it never trades
and never authenticates (public read-only endpoints only).

All quantitative **research** (market microstructure / ML
studies, the `polytape_mm` package, notebooks, the raw→parquet→tensor pipeline)
lives in the sibling **PolyQuant** repo. Do not add analysis code here.

# Commands
- Test: `pytest -q` (fully offline — no test touches the network)
- Lint: `ruff check .`
- Format: `ruff format .` (CI runs `ruff format --check --diff` — keep it clean; ruff replaces black)
- Install: `pip install -e ".[dev]"` (add `.[admin]` for the FastAPI admin server)
- Run recorder: `python -m polytape --event-id <ID> --out <DIR>` (long-running; launch deliberately)
- Console entry points: `polytape`, `polytape-admin`, `polytape-monitor`, `polytape-view`

# Architecture
- `polytape/` — recorder core (`app`, `cli`, `streams`, `supervisor`, `writer`, `envelope`, `gamma`)
- `polytape/admin/`, `polytape/monitor/`, `polytape/viewer/` — dashboards/UIs over recorded captures
- `polytape/admin/offload.py` — moves FINISHED matches' per-match native files to a Coldline
  GCS bucket and deletes them locally (the monolith stays the backstop); the admin serves
  offloaded matches via a signed-URL 302. `google-cloud-storage` is in the `[admin]` extra
  (imported lazily). Run: `python -m polytape.admin.offload --list` / (no flag) to offload.
- `deploy/` — systemd units for the production recorder VM (GCP), incl. `polytape-offload.{service,timer}`
- `scripts/` — operational helpers (capture validation, demo capture, WC match listing, meta seeding)

# Conventions
- Recorder runtime deps are intentionally minimal: only `websockets` + `httpx`.
- `ruff` version is pinned exactly in `pyproject.toml` so CI lint/format stays deterministic.
- CI gate is the single `ci-success` check (lint + pytest across Python 3.10–3.14).

# Gotchas
- `data/` is **live**: it's the recorder's default `--out` and may hold in-progress captures.
  Never `rm -rf data/`, and never run two recorders into the same `event-<id>/` dir (concurrent
  appends corrupt the JSONL). Use an isolated temp `--out` when smoke-testing.
- The recorder resolves an Event ID → markets/token IDs via the public Gamma API.
- Old captures on disk may still hold a `comments.jsonl` from before comment recording
  was removed (2026-07); readers ignore it and downloads no longer ship it.
- A `matches/event-<id>.offloaded.json` marker means that match's native files were moved
  to GCS (local dir deleted). The admin serves it via a signed URL; a multi-select download
  still works via the monolith scan. Never delete a marker without also removing its GCS object.
