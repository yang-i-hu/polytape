# polytape

Passive recorder for two of Polymarket's public real-time feeds (RTDS comments +
CLOB order book), written to timestamped JSONL. **This repo is purely for getting
data** — it never trades and never authenticates (public read-only endpoints only).

All quantitative **research** (backtests, microstructure / ML / market-making
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
- `deploy/` — systemd units for the production recorder VM (GCP)
- `scripts/` — operational helpers (capture validation, demo capture, WC match listing, meta seeding)

# Conventions
- Recorder runtime deps are intentionally minimal: only `websockets` + `httpx`.
- `ruff` version is pinned exactly in `pyproject.toml` so CI lint/format stays deterministic.
- CI gate is the single `ci-success` check (lint + pytest across Python 3.10–3.14).

# Gotchas
- `data/` is **live**: it's the recorder's default `--out` and may hold in-progress captures.
  Never `rm -rf data/`, and never run two recorders into the same `event-<id>/` dir (concurrent
  appends corrupt the JSONL). Use an isolated temp `--out` when smoke-testing.
- The recorder resolves an Event ID → markets/token IDs via the public Gamma API; comment
  filtering is **client-side** (the server-side `filters` field delivers zero messages).
