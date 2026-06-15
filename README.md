# polytape

Record two of Polymarket's public, real-time feeds during a live event — the
**comment stream** (RTDS websocket) and the **order book** (CLOB websocket) — to
timestamped JSONL for later research.

`polytape` is a passive recorder. It connects to public, read-only feeds, wraps
every message it receives in a small envelope with dual timestamps, and appends
it to disk. It is built to survive websocket drops (reconnect + REST backfill) so
that the burst of activity around a key moment — a goal, a resolution, a price
swing — is captured rather than lost.

> **This tool never trades and never authenticates.** It uses only public,
> unauthenticated endpoints. There is no wallet, no API key, and no code path
> that places an order or touches an account.

---

## Responsible use

- **Public data only.** All endpoints used are public and read-only.
- **Privacy by default.** Usernames and other personal identifiers in comment
  payloads are replaced with a salted SHA-256 hash before anything is written to
  disk. Pass `--no-hash` to disable this (e.g. for a private capture you control).
- **Polite to the API.** REST backfill calls are rate-limited with backoff; the
  recorder is read-only and low-volume.
- **Keepalive.** Each websocket is kept alive with the application-level text
  keepalive that feed expects, sent every 5 seconds — RTDS wants lowercase
  `ping`, the CLOB market channel wants uppercase `PING`. (5 s is safely within
  the CLOB channel's ~10 s idle timeout.)

You are responsible for complying with Polymarket's terms of service and with any
applicable laws when recording and using this data.

---

## Requirements

- Python 3.10+
- Runtime dependencies: [`websockets`](https://pypi.org/project/websockets/),
  [`httpx`](https://pypi.org/project/httpx/)
- Dev dependencies: `pytest`, `ruff`, `black`

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
public Gamma API, then records the comment and order-book streams concurrently
until you stop it with `Ctrl-C` (SIGINT) or SIGTERM.

### Options

| Flag | Default | Description |
| --- | --- | --- |
| `--event-id ID` | *(required)* | Polymarket **Event** ID to record (numeric for a live capture; any string under `--dry-run`). |
| `--out DIR` | `./data` | Output root directory. Data is written to `DIR/event-<id>/`. |
| `--comments` / `--no-comments` | on | Record (or skip) the RTDS comment stream. |
| `--book` / `--no-book` | on | Record (or skip) the CLOB order-book stream. |
| `--market-id ID` | *(auto)* | Override the market(s) to record instead of every market in the event. May be repeated. |
| `--no-hash` | off | Write usernames/identifiers verbatim instead of hashing them. |
| `--dry-run` | off | Feed synthetic comment and book messages through the full pipeline with **no network**. For testing the capture path offline. |
| `--log-level LEVEL` | `INFO` | Python logging level (`DEBUG`, `INFO`, `WARNING`, ...). |

At least one of `--comments` / `--book` must be enabled.

### Examples

```bash
# Record everything for an event
python -m polytape --event-id 12345

# Comments only, into a custom directory
python -m polytape --event-id 12345 --no-book --out ./captures

# Record raw usernames (no hashing) for a single market
python -m polytape --event-id 12345 --no-hash --market-id 0xabc...

# Exercise the whole pipeline offline
python -m polytape --event-id demo --dry-run
```

---

## Output

Everything for one capture lives under a single directory:

```
data/
└── event-<id>/
    ├── comments.jsonl   # one JSON object per line, append-only
    ├── book.jsonl       # one JSON object per line, append-only
    └── meta.json        # capture metadata, rewritten at start/stop and on each gap
```

A stream's file is only created if that stream is enabled.

### Record envelope

Every recorded message — comment or book update — is wrapped in the same
envelope and written as one line of JSON (JSONL), flushed immediately:

```json
{
  "stream": "comments",
  "id": "0c0f6b2e-...-message-id",
  "ts_recv": "2026-06-14T19:03:21.481123Z",
  "ts_server": "2026-06-14T19:03:21.402000Z",
  "raw": { "...": "the original message payload" }
}
```

| Field | Type | Meaning |
| --- | --- | --- |
| `stream` | string | `"comments"` or `"book"`. |
| `id` | string | Stable unique ID for the message, used for de-duplication. Derived from the payload's own id field where one exists (e.g. a comment id, or a book `hash`); otherwise a deterministic content hash of the payload. |
| `ts_recv` | string | UTC time the message was received locally, ISO-8601 with microseconds and a `Z` suffix. Always present. |
| `ts_server` | string \| null | Server-side timestamp parsed from the payload and normalized to the same UTC ISO-8601 format. `null` if the payload carries no usable server timestamp. |
| `raw` | object | The message payload as received. **With hashing on (the default), identifier fields inside `raw` are replaced in place by their salted hash** (see below). With `--no-hash`, `raw` is byte-for-byte the original payload. |

> **Design note — `raw` and hashing.** The privacy default (hash usernames) and a
> verbatim `raw` are in tension: writing the original payload unchanged would put
> plaintext usernames on disk. `polytape` resolves this in favor of privacy:
> when hashing is on, identifier fields *within* `raw` are replaced by their hash,
> so no plaintext identifier is ever persisted. `raw` is therefore "the original
> payload, minus PII" by default, and truly verbatim only under `--no-hash`.

### Username hashing

When hashing is enabled (default), each configured identifier field is replaced
with:

```
sha256(salt + "\x1f" + value).hexdigest()
```

- **Salt.** Taken from the `POLYTAPE_SALT` environment variable if set (use this
  to keep hashes stable/correlatable across runs), otherwise a random salt is
  generated for the run. The salt itself is **never** written to disk. A short,
  non-reversible *fingerprint* of the salt (`sha256(salt)` truncated) is recorded
  in `meta.json` so you can tell whether two captures share a salt without
  leaking it.
- **Fields hashed (defaults).** Applied to the **comments** stream only — book
  payloads carry no personal data (just prices, sizes, asset IDs). These keys are
  replaced wherever they appear in the payload: `userAddress`, `replyAddress`,
  `proxyWallet`, `baseAddress` (wallet addresses) and `name`, `pseudonym`
  (display handles). The set is defined in `polytape/envelope.py`.

### `meta.json`

Written when capture starts, updated on every disconnect/reconnect, and finalized
on shutdown:

```json
{
  "polytape_version": "0.1.0",
  "event_id": "12345",
  "market_ids": ["0x...condition_id..."],
  "clob_token_ids": ["7142...", "9823..."],
  "streams": ["comments", "book"],
  "out_dir": "data/event-12345",
  "hashing": { "enabled": true, "salt_fingerprint": "a1b2c3d4" },
  "started_at": "2026-06-14T19:00:00.000000Z",
  "stopped_at": "2026-06-14T20:00:00.000000Z",
  "counts": { "comments": 1284, "book": 90213 },
  "event": { "id": "12345", "title": "...", "slug": "...", "...": "resolved event snapshot" },
  "gaps": [
    {
      "stream": "comments",
      "disconnected_at": "2026-06-14T19:31:02.111000Z",
      "reconnected_at": "2026-06-14T19:31:07.840000Z",
      "downtime_seconds": 5.73,
      "backfilled": 12,
      "note": "reconnected; backfilled via gamma /comments since last-seen id"
    }
  ]
}
```

`gaps` is the audit trail of every disconnect: when it happened, how long the
stream was down, and how many messages backfill recovered. `book` gaps are logged
too, but the CLOB feed re-sends a full book snapshot on (re)subscribe, so book
recovery relies on that snapshot rather than REST backfill.

---

## Behavior

### Streams

- **Comments (RTDS).** Connects to `wss://ws-live-data.polymarket.com`, subscribes
  to the `comments` topic filtered to the Event (`filters` is a stringified
  `{"parentEntityID":<event id>,"parentEntityType":"Event"}`), keeps the socket
  alive with lowercase `ping`, and records each comment/reaction.
- **Order book (CLOB).** Connects to
  `wss://ws-subscriptions-clob.polymarket.com/ws/market`, subscribes to the
  event's CLOB token IDs (`{"assets_ids":[...],"type":"market"}`), keeps the
  socket alive with uppercase `PING`, and records book messages. The CLOB market
  channel delivers a full **snapshot** (`book`) on subscribe and incremental
  **deltas** (`price_change`) thereafter — plus `last_trade_price` and
  `tick_size_change`. All are recorded verbatim with the message type preserved
  inside `raw`.

### Reconnect + backfill

Each stream runs under a supervisor that reconnects with exponential backoff on
any disconnect. On reconnect:

- **Comments:** missed comments are backfilled from the Gamma `/comments` endpoint
  starting after the last-seen comment id. The dedup set guarantees no duplicates
  even if backfill overlaps with live messages.
- **Book:** the fresh subscribe yields a new full snapshot, re-establishing state.

Every disconnect and its recovery are appended to `meta.json#gaps`.

### Graceful shutdown

On `Ctrl-C` / SIGTERM, both stream tasks are cancelled cleanly: all buffers are
flushed, files are closed, `stopped_at` and final `counts` are written to
`meta.json`, and the process exits without truncating or corrupting any line.

### Dry-run

`--dry-run` runs the entire capture pipeline — envelope construction, hashing,
dedup, JSONL writing, `meta.json` — against an in-process generator of synthetic
comment and book messages. No sockets are opened and no REST calls are made, so
the capture path can be exercised and tested with zero network access.

---

## Development

```bash
pip install -e ".[dev]"
pytest            # unit tests, fully offline (no network required)
ruff check .
black .
```

### Smoke test (manual, requires network + a live event)

```bash
# 1. Record a currently-live event for ~60 seconds, then press Ctrl-C:
python -m polytape --event-id <LIVE_EVENT_ID> --out ./smoke

# 2. Validate the capture (well-formed envelopes carrying both timestamps):
python scripts/validate_capture.py ./smoke/event-<LIVE_EVENT_ID>
```

The validator reports, per stream file, how many lines are valid envelopes and
how many carry a server timestamp, and exits non-zero if anything is malformed or
a stream produced no lines. A healthy run shows non-empty `comments.jsonl` and
`book.jsonl`, each line with a `ts_recv` and (where the feed provides one) a
`ts_server`.

For a network-free check that the whole capture path works, use the dry run:

```bash
python -m polytape --event-id demo --dry-run --out ./smoke
python scripts/validate_capture.py ./smoke/event-demo
```

---

## Endpoints used (all public, no auth)

| Purpose | Endpoint |
| --- | --- |
| Comment stream | `wss://ws-live-data.polymarket.com` (RTDS, topic `comments`) |
| Order-book stream | `wss://ws-subscriptions-clob.polymarket.com/ws/market` |
| Resolve event → markets / token IDs; comment backfill | `https://gamma-api.polymarket.com` (`/events`, `/comments`) |

Exact subscribe frames, the comment-by-event filter format, and the book
subscription/payload shapes were verified against Polymarket's RTDS docs, the
official [`real-time-data-client`](https://github.com/Polymarket/real-time-data-client),
and the CLOB websocket docs before the network layer was written. The full,
source-cited findings — including message-by-message field maps and a list of
items to confirm against a live capture — are in [PROTOCOL.md](PROTOCOL.md).

## License

MIT — see [LICENSE](LICENSE).
