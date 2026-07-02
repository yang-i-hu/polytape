# polytape

[![CI](https://github.com/yang-i-hu/polytape/actions/workflows/ci.yml/badge.svg)](https://github.com/yang-i-hu/polytape/actions/workflows/ci.yml)

Record Polymarket's public, real-time **order book** (CLOB websocket) during a
live event to timestamped JSONL for later research.

`polytape` is a passive recorder. It connects to a public, read-only feed, wraps
every message it receives in a small envelope with dual timestamps, and appends
it to disk. It is built to survive websocket drops (reconnect + fresh snapshot) so
that the burst of activity around a key moment — a goal, a resolution, a price
swing — is captured rather than lost.

> **This tool never trades and never authenticates.** It uses only public,
> unauthenticated endpoints. There is no wallet, no API key, and no code path
> that places an order or touches an account.

---

## Responsible use

- **Public data only.** All endpoints used are public and read-only.
- **Polite to the API.** REST calls are rate-limited with backoff; the recorder
  is read-only and low-volume.
- **Keepalive.** The websocket is kept alive with the application-level text
  keepalive the feed expects — the CLOB market channel wants uppercase `PING`
  every 5 seconds (safely within its ~10 s idle timeout).

You are responsible for complying with Polymarket's terms of service and with any
applicable laws when recording and using this data.

---

## Requirements

- Python 3.10+
- Runtime dependencies: [`websockets`](https://pypi.org/project/websockets/),
  [`httpx`](https://pypi.org/project/httpx/)
- Dev dependencies: `pytest`, `ruff`

## Install

```bash
# from the repo root
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\Activate.ps1
pip install -e ".[dev]"
```

---

## Usage

```bash
python -m polytape --event-id <EVENT_ID> [options]
```

`polytape` resolves the Event ID to its market(s) and CLOB token IDs via the
public Gamma API, then records the order-book stream until you stop it with
`Ctrl-C` (SIGINT) or SIGTERM.

### Options

One of `--event-id` / `--matches-file` is required.

| Flag | Default | Description |
| --- | --- | --- |
| `--event-id ID` | — | Polymarket **Event** ID to record (numeric for a live capture; any string under `--dry-run`). Repeatable for a multi-event run. |
| `--matches-file PATH` | — | JSON file of matches (as produced by `scripts/list_wc_matches.py`) to record instead of `--event-id`. |
| `--open-only` / `--no-open-only` | on | With `--matches-file`, record only events that are not yet closed. |
| `--run-name NAME` | — | Label for a multi-event run; output goes to `DIR/run-<name>/`. |
| `--out DIR` | `./data` | Output root directory. Data is written to `DIR/event-<id>/` (or `DIR/run-<name>/`). |
| `--market-id ID` | *(auto)* | Override the market(s) to record instead of every market in the event. May be repeated. |
| `--per-match` / `--no-per-match` | on | Also write per-match files (`matches/event-<id>/book.jsonl`) alongside the monolithic backup log. |
| `--dry-run` | off | Feed synthetic book messages through the full pipeline with **no network**. For testing the capture path offline. |
| `--log-level LEVEL` | `INFO` | Python logging level (`DEBUG`, `INFO`, `WARNING`, ...). |

### Examples

```bash
# Record an event
python -m polytape --event-id 12345

# Record a single market into a custom directory
python -m polytape --event-id 12345 --market-id 0xabc... --out ./captures

# Exercise the whole pipeline offline
python -m polytape --event-id demo --dry-run
```

---

## Output

Everything for one capture lives under a single directory:

```
data/
└── event-<id>/
    ├── book.jsonl       # one JSON object per line, append-only
    └── meta.json        # capture metadata, rewritten at start/stop and on each gap
```

### Record envelope

Every recorded message is wrapped in the same envelope and written as one line
of JSON (JSONL), flushed immediately:

```json
{
  "stream": "book",
  "id": "0x...book-hash",
  "ts_recv": "2026-06-14T19:03:21.481123Z",
  "ts_server": "2026-06-14T19:03:21.402000Z",
  "raw": { "...": "the original message payload" }
}
```

| Field | Type | Meaning |
| --- | --- | --- |
| `stream` | string | `"book"`. |
| `id` | string | Stable unique ID for the message, used for de-duplication. Derived from the payload's own id field where one exists (e.g. a book `hash` or a trade's `transaction_hash`); otherwise a deterministic content hash of the payload. |
| `ts_recv` | string | UTC time the message was received locally, ISO-8601 with microseconds and a `Z` suffix. Always present. |
| `ts_server` | string \| null | Server-side timestamp parsed from the payload and normalized to the same UTC ISO-8601 format. `null` if the payload carries no usable server timestamp. |
| `raw` | object | The message payload as received, byte-for-byte. |

### `meta.json`

Written when capture starts, updated on every disconnect/reconnect, and finalized
on shutdown:

```json
{
  "polytape_version": "0.1.0",
  "event_id": "12345",
  "market_ids": ["0x...condition_id..."],
  "clob_token_ids": ["7142...", "9823..."],
  "streams": ["book"],
  "out_dir": "data/event-12345",
  "started_at": "2026-06-14T19:00:00.000000Z",
  "stopped_at": "2026-06-14T20:00:00.000000Z",
  "counts": { "book": 90213 },
  "event": { "id": "12345", "title": "...", "slug": "...", "...": "resolved event snapshot" },
  "gaps": [
    {
      "stream": "book",
      "disconnected_at": "2026-06-14T19:31:02.111000Z",
      "reconnected_at": "2026-06-14T19:31:07.840000Z",
      "downtime_seconds": 5.73,
      "note": "reconnect"
    }
  ]
}
```

`gaps` is the audit trail of every disconnect: when it happened and how long the
stream was down. The CLOB feed re-sends a full book snapshot on (re)subscribe, so
book recovery relies on that snapshot rather than REST backfill.

---

## Live monitor (dashboard)

The polytape monitor (run with `python -m polytape.monitor`, or the installed
`polytape-monitor` command) is a small **read-only** web dashboard for watching a
capture happen — message counts, throughput, the server→receive delay, the
message-type mix, staleness, and the disconnect log, refreshed about once a
second.

**Monitoring is read-only.** It is a separate process that only ever **reads**
the files a capture already writes (the append-only `*.jsonl` and the
atomically-rewritten `meta.json`). It never imports the recorder's network or
writer path and adds **no work to the capture's hot path** — start it, stop it,
or restart it freely at any time, even mid-recording, with zero effect on what's
being recorded. (An optional, loopback-only control plane can additionally
*launch* and *stop* captures — see [Controls](#controls-start--stop-a-capture-from-the-dashboard)
below — but a launched recorder is just a normal `polytape` process.)

```bash
# In one terminal: record (or already recording) into ./data
python -m polytape --event-id 12345

# In another: watch it live (defaults to ./data, http://localhost:8787)
python -m polytape.monitor --open
```

Try it with no live event by pointing the monitor at a synthetic feed:

```bash
python -m polytape.monitor.demo            # writes a live synthetic capture to ./data
python -m polytape.monitor --open          # then watch it move
```

### Controls (start / stop a capture from the dashboard)

The dashboard has a **Recorder** panel that can launch and stop captures for you:

- **Start** a *Live event* — enter a numeric event id, a slug, or just **paste the
  Polymarket URL** (e.g. `https://polymarket.com/sports/.../fifwc-ksa-ury-2026-06-15`);
  the slug is resolved to its event id for you. Or start a *Demo feed* (synthetic,
  no network).
- **Find related** — paste any event's URL/slug and click *Find related* to list
  the **other events in its series** (e.g. every match in the tournament). Click a
  row to drop it into the input, or hit **Record ▶** to capture it directly — each
  in its own session. Handy for recording several matches without hunting URLs.
- **Stop** any capture the dashboard launched — a graceful shutdown that
  finalizes `meta.json` (`SIGINT` on POSIX, `CTRL_BREAK` on Windows).
- **Pause / Resume** the live view — this freezes the *dashboard's* refresh so
  you can inspect; it never affects what's being recorded. (There is no "pause
  recording": a live websocket feed can't pause without disconnecting and
  missing data — the very thing the recorder exists to capture.)

You can record **several events at once**. The **Events & sessions** list shows
every capture under the root (recording, idle, or stopped); click a row to view
it here, hit **Open ↗** to open it in its own browser tab, or **Stop** a running
one. Starting a new recording opens it in its own tab by default — so each
event/match gets its own live session.

A launched capture is an ordinary `polytape` process, identical to one started by
hand, so it keeps running if you close the dashboard (the monitor does not stop
recordings on exit). Captures started in another terminal are still shown and
monitored, but only ones this dashboard launched get a **Stop** button.

> **One recorder per event.** Two recorders writing the same `event-<id>` folder
> interleave their appends and corrupt the JSONL. The dashboard refuses to start a
> capture for an event that already looks actively recorded (its files were just
> written) — but it can't detect a *quiet* feed already being recorded by another
> process, so don't point a second recorder at an event you're already capturing.

**Safety.** Control spawns processes, so it is **enabled only on a loopback bind**
and is guarded against cross-site requests (a custom header browsers can't forge
cross-origin). Disable it entirely with `--read-only`; to allow it on a
non-loopback bind (use with care) pass `--allow-control`.

### Options

| Flag | Default | Description |
| --- | --- | --- |
| `--out DIR` | `./data` | Capture root to watch (the recorder's `--out`), or a single `event-<id>` dir. New captures started from the UI are written here. |
| `--host HOST` | `127.0.0.1` | Bind host. Loopback by default; binding elsewhere exposes capture volume/timing to the network. |
| `--port PORT` | `8787` | Bind port. |
| `--idle-threshold S` | `20` | Seconds without a new message before a running capture is shown as **idle** (a quiet market is normal). |
| `--read-only` | off | Disable the start/stop control plane entirely (pure observer). |
| `--allow-control` | off | Allow control on a non-loopback bind (spawns processes — use with care). |
| `--open` | off | Open the dashboard in a browser on start. |

### Notes

- **Zero new dependencies.** The dashboard is the Python standard library
  (`http.server`) plus one self-contained HTML page (vanilla JS, canvas
  sparklines) — no framework, no build step, no CDN; it works fully offline.
- **No payload content is exposed.** The dashboard surfaces only aggregate
  counters and non-identifying metadata (stream, message type, timestamps,
  delay) — never record payloads.
- **Exact totals, live windows.** Message *counts* are exact from the first
  refresh; the *rate*, *delay percentiles*, and *type mix* describe traffic seen
  since the monitor attached (so attaching to a long-running capture stays cheap).
- If the recorder root holds several `event-*` captures, the dashboard shows a
  picker and defaults to the most recently active one.

---

## Viewing a capture

`polytape-view` opens a local web UI that **replays a capture's history and follows a
live one** — a depth ladder, a cumulative-depth chart, a mid-price time-series with a
draggable scrubber, microprice/imbalance gauges, top-of-book metrics, and a trades tape.
It only **reads** a capture directory (it never connects to Polymarket), so you can point
it at a finished capture or at one that is still recording.

```bash
# View a single capture (opens http://127.0.0.1:8770 in your browser)
polytape-view --event-dir ./data/event-12345

# Same capture, mirroring the recorder's flags
polytape-view --out ./data --event-id 12345

# Scan a data root and pick from all captures
polytape-view --data ./data
```

Useful flags: `--host`/`--port` (binds `127.0.0.1` by default so captures aren't exposed),
`--poll-interval` (file-tail cadence for live follow), `--keyframe-every` (book snapshots
cached for fast scrubbing), `--no-open` (don't launch a browser). It adds **no runtime
dependencies** — the backend is pure standard library (HTTP + Server-Sent Events) and the
frontend is dependency-free vanilla JS.

**Live vs history.** When the capture is still recording (`stopped_at` is `null`), the viewer
follows new book updates in real time over SSE; the Live/History toggle (or dragging the
scrubber) switches to replaying any past moment, with reconnect gaps shaded and a
"book uncertain" badge while inside a gap. Order-book reconstruction is done server-side and
is the single source of truth.

**Try it with synthetic data** (writes to a temp dir — never `./data`):

```bash
# Generate a dense demo capture (snapshot + deltas + trades + a reconnect gap, YES/NO)
python scripts/make_demo_capture.py --out ./_vtmp --event-id 80505
polytape-view --event-dir ./_vtmp/event-80505

# ...or stream ~10s of live updates into it to exercise the live/SSE path
python scripts/make_demo_capture.py --out ./_vtmp --event-id 80505 --live 10
```

---

## Behavior

### Stream

- **Order book (CLOB).** Connects to
  `wss://ws-subscriptions-clob.polymarket.com/ws/market`, subscribes to the
  event's CLOB token IDs (`{"assets_ids":[...],"type":"market"}`), keeps the
  socket alive with uppercase `PING`, and records book messages. The CLOB market
  channel delivers a full **snapshot** (`book`) on subscribe and incremental
  **deltas** (`price_change`) thereafter — plus `last_trade_price` and
  `tick_size_change`. All are recorded verbatim with the message type preserved
  inside `raw`.

### Reconnect

The stream runs under a supervisor that reconnects with exponential backoff on
any disconnect. The fresh subscribe yields a new full snapshot, re-establishing
state. Every disconnect and its recovery are appended to `meta.json#gaps`.

### Graceful shutdown

On `Ctrl-C` / SIGTERM, the stream tasks are cancelled cleanly: all buffers are
flushed, files are closed, `stopped_at` and final `counts` are written to
`meta.json`, and the process exits without truncating or corrupting any line.

### Dry-run

`--dry-run` runs the entire capture pipeline — envelope construction, dedup,
JSONL writing, `meta.json` — against an in-process generator of synthetic book
messages. No sockets are opened and no REST calls are made, so the capture path
can be exercised and tested with zero network access.

---

## Development

```bash
pip install -e ".[dev]"
pytest                 # unit tests, fully offline (no network required)
ruff check .           # lint
ruff format --check .  # style (drop --check to auto-format)
```

These are exactly the checks CI runs on every push and pull request to `main`
(across Python 3.10–3.14). `ruff` is pinned in the `dev` extra so local results
match CI; bump it deliberately and reformat in the same commit.

### Smoke test (manual, requires network + a live event)

```bash
# 1. Record a currently-live event for ~60 seconds, then press Ctrl-C:
python -m polytape --event-id <LIVE_EVENT_ID> --out ./smoke

# 2. Validate the capture (well-formed envelopes carrying both timestamps):
python scripts/validate_capture.py ./smoke/event-<LIVE_EVENT_ID>
```

The validator reports, per stream file, how many lines are valid envelopes and
how many carry a server timestamp, and exits non-zero if anything is malformed or
a stream produced no lines. A healthy run shows a non-empty `book.jsonl`, each
line with a `ts_recv` and (where the feed provides one) a `ts_server`.

For a network-free check that the whole capture path works, use the dry run:

```bash
python -m polytape --event-id demo --dry-run --out ./smoke
python scripts/validate_capture.py ./smoke/event-demo
```

---

## Endpoints used (all public, no auth)

| Purpose | Endpoint |
| --- | --- |
| Order-book stream | `wss://ws-subscriptions-clob.polymarket.com/ws/market` |
| Resolve event → markets / token IDs | `https://gamma-api.polymarket.com` (`/events`) |

Exact subscribe frames and the book subscription/payload shapes were verified
against the CLOB websocket docs before the network layer was written. The full,
source-cited findings — including message-by-message field maps — are in
[PROTOCOL.md](PROTOCOL.md).

## License

MIT — see [LICENSE](LICENSE).
