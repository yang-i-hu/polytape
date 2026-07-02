# polytape — Polymarket Feed Recorder Protocol Spec

Authoritative, implementation-ready spec for recording a single Polymarket Event's
order-book stream (CLOB market channel) and resolution data (Gamma REST).

Ground-truth precedence: **official client SOURCE CODE > prose docs.** Disagreements are
flagged inline with **[CROSS-CHECK]**. Items not confirmed from a primary source are in
**OPEN QUESTIONS** at the bottom.

---

## 0. The two data planes at a glance

| Plane | Transport | Endpoint | Keyed by | Auth |
|-------|-----------|----------|----------|------|
| Order book (CLOB) | WebSocket, JSON + plain-text heartbeat | `wss://ws-subscriptions-clob.polymarket.com/ws/market` | CLOB **token ids** (`assets_ids`) | none (public market channel) |
| Resolution (Gamma) | HTTP GET (REST) | `https://gamma-api.polymarket.com` | event id / token id | none |

The CLOB token ids (obtained from Gamma `markets[].clobTokenIds`) are the join key into the
CLOB stream. **Start with Gamma to resolve everything, then open the websocket.**

---

## 1. CLOB book stream

### 1.1 Connection

- **URL:** `wss://ws-subscriptions-clob.polymarket.com/ws/market`

### 1.2 Subscribe frame (exact wire format)

Keyed by **CLOB token ids** (the huge decimal-string per-outcome ERC-1155 ids). In the
SUBSCRIBE frame the field is **plural `assets_ids`** (array of token-id strings). The `type`
is the const `"market"`.

```json
{"assets_ids":["21742633143463906290569050155826241533067272736897614950488156847949938836455","48331043336612883890938759509493159234755048973500640148014422747788308965732"],"type":"market"}
```

To record one Event/market: put **all** that market's outcome token ids (typically the
2-element `[yesToken, noToken]` from Gamma `clobTokenIds`) into `assets_ids`. For a
multi-market event, include every market's token ids.

Optional subscribe fields (from AsyncAPI): `initial_dump` (bool, default true — send `book`
snapshot on subscribe), `level` (int 1/2/3, default 2), `custom_feature_enabled` (bool,
default false — enables `best_bid_ask` / `new_market` / `market_resolved`).

**Dynamic add/remove on a live connection (no reconnect):** use `operation` instead of `type`:

```json
{"assets_ids":["<NEW_TOKEN_ID>"],"operation":"subscribe"}
{"assets_ids":["<OLD_TOKEN_ID>"],"operation":"unsubscribe"}
```

**[CROSS-CHECK — docs/AsyncAPI vs Python client]** The plural `assets_ids` is confirmed by the
docs, the AsyncAPI spec (`SubscriptionRequest` requires `assets_ids` + `type` const `market`),
and the agent-skills repo. The `py-clob-client` dataclasses only model REST shapes and use
**singular** `asset_id`/`token_id`; they have no WS-subscribe dataclass. So the plural WS key
is confirmed from docs/AsyncAPI, not from a Python constant. **Use `assets_ids` (plural) on the
wire** — sending the singular key has historically caused a silent freeze (py-clob-client
issue #292). This is a naming inconsistency to watch, not a true contradiction.

### 1.3 Keepalive / ping

- Client MUST send the literal **uppercase** text frame `PING` (plain text, NOT JSON) every
  **~10 s** (polytape sends it every 5 s, safely inside the limit). Server replies with
  literal text `PONG` (ignore it).
- Miss it and the server drops the connection after ~10 s.

> (The CLOB *sports* channel inverts this — server pings, client replies `pong` — but that is a
> different endpoint, out of scope.)

### 1.4 Incoming message types

All timestamps are **strings of Unix epoch MILLISECONDS** (e.g. `"1757908892351"`), confirmed
in AsyncAPI + market.md. There is no client/recv timestamp in the payload — **the recorder
must add its own receive timestamp.**

#### 1.4.1 `book` — full snapshot

Sent on subscribe (when `initial_dump`) and again when a trade affects the book.

```
event_type  "book"
asset_id    string   (the token id — per-outcome key)
market      string   (0x condition id)
bids[]      {price, size}
asks[]      {price, size}
timestamp   string   (epoch ms)
hash        string   <-- DEDUP / change-detect key (hash of book content)
```

```json
{"event_type":"book","asset_id":"65818619657568813474341868652308942079804919287380422192892211131408793125422","market":"0xbd31dc8a20211944f6b70f31557f1001557b59905b7738480ca09bd4532f84af","bids":[{"price":"0.48","size":"30"},{"price":"0.49","size":"20"}],"asks":[{"price":"0.52","size":"25"},{"price":"0.53","size":"60"}],"timestamp":"1757908892351","hash":"0xabc123..."}
```

#### 1.4.2 `price_change` — delta

Emitted on order placement/cancellation. **Watch the shape difference vs `book`:** there is
**NO top-level `asset_id`**; the per-token id and the hash live **inside each `price_changes[]`
element**. Filter by top-level `market` and/or `price_changes[].asset_id`.

```
event_type      "price_change"
market          string   (0x condition id)
timestamp       string   (epoch ms, top-level)
price_changes[] each element:
    asset_id    string   (per-token key)
    price       string
    size        string   <-- NEW AGGREGATE size at that level ("0" = level REMOVED), not a diff
    side        "BUY" | "SELL"
    hash        string   <-- per-element dedup key
    best_bid    string   (optional)
    best_ask    string   (optional)
```

```json
{"event_type":"price_change","market":"0x5f65177b394277fd294cd75650044e32ba009a95022d88a0c1d565897d72f8f1","price_changes":[{"asset_id":"71321045679252212594626385532706912750332728571942532289631379312455583992563","price":"0.5","size":"200","side":"BUY","hash":"56621a121a47ed9333273e21c83b660cff37ae50","best_bid":"0.5","best_ask":"1"}],"timestamp":"1757908892351"}
```

> Book reconstruction: `price_change.size` is a **level replace**, not an increment. Set level
> `price` to `size`; if `size == "0"`, delete the level. Seed from the latest `book` snapshot.

#### 1.4.3 `last_trade_price` — single trade

```
event_type        "last_trade_price"
asset_id          string
market            string   (0x condition id)
price             string
size              string
side              "BUY"|"SELL"  (taker perspective)
fee_rate_bps      string   (optional)
timestamp         string   (epoch ms)
transaction_hash  string   (optional) <-- closest unique id (on-chain tx)
```

```json
{"event_type":"last_trade_price","asset_id":"114122071509644379678018727908709560226618148003371446110114509806601493071694","market":"0x6a67b9d828d53862160e470329ffea5246f338ecfffdf2cab45211ec578b0347","price":"0.456","size":"219.217767","fee_rate_bps":"0","side":"BUY","timestamp":"1750428146322","transaction_hash":"0xeeefffggghhh"}
```

#### 1.4.4 `tick_size_change` — event notification

```
event_type     "tick_size_change"
asset_id       string
market         string   (0x condition id)
old_tick_size  string
new_tick_size  string
timestamp      string   (epoch ms)
```

No dedicated id/hash field; key on `asset_id` + `timestamp`.

#### 1.4.5 `best_bid_ask`, `new_market`, `market_resolved` — only if `custom_feature_enabled:true`

Semi-confirmed (only "key fields" documented; full schemas/timestamps unconfirmed — see OPEN
QUESTIONS). polytape can leave `custom_feature_enabled` off by default and reconstruct
top-of-book from `book` + `price_change`.

### 1.5 Recorder field map for CLOB

| Message | id / dedup | token key | condition id | server ts |
|---------|-----------|-----------|--------------|-----------|
| `book` | `hash` (top-level) | `asset_id` (top-level) | `market` | `timestamp` (ms str) |
| `price_change` | `price_changes[].hash` | `price_changes[].asset_id` | `market` (top-level) | `timestamp` (top-level, ms str) |
| `last_trade_price` | `transaction_hash` (optional) | `asset_id` | `market` | `timestamp` (ms str) |
| `tick_size_change` | none (use `asset_id`+`timestamp`) | `asset_id` | `market` | `timestamp` (ms str) |

Optional REST seed/reconcile: `POST https://clob.polymarket.com/book` body
`{"token_id":"<CLOB_TOKEN_ID>"}` → `OrderBookSummary {market, asset_id, timestamp, bids, asks, hash}`.

---

## 2. Gamma REST — resolution

Base: `https://gamma-api.polymarket.com`. Unauthenticated GET only. No keepalive.

### 2.1 Resolve Event ID → markets → CLOB token ids (confirmed live + against `agents/.../gamma.py`)

**Call (path form — returns a single OBJECT, starts with `{`):**

```
GET https://gamma-api.polymarket.com/events/2890
```

**Or query form — returns an ARRAY of one (starts with `[`); also supports slug:**

```
GET https://gamma-api.polymarket.com/events?id=2890
GET https://gamma-api.polymarket.com/events?slug=<event-slug>
```

> **Shape gotcha:** `/events/{id}` → object; `/events?id=` / `?slug=` / bare `/events` →
> array. polytape must handle both: if the response is a list, take `[0]`.

**Resolution path:** `event` → `event.markets[*]` → for each market read:

- `market.id` — string, e.g. `"239826"`.
- `market.conditionId` — string, 0x CTF condition id. **Note camelCase `conditionId`** on the
  wire, NOT `condition_id`. (Matches the CLOB `market` field.)
- `market.clobTokenIds` — **a JSON-ENCODED STRING that must be `json.loads`'d**. On the wire:
  `"[\"2818...0833\", \"4704...6290\"]"`. After parsing → 2-element array of decimal-string
  token ids: **`[0]` = YES token, `[1]` = NO token** (aligned with `market.outcomes`, itself a
  stringified `"[\"Yes\",\"No\"]"`). `market.outcomePrices` is likewise a stringified array.

The official client confirms the parse:
`market_object['clobTokenIds'] = json.loads(market_object['clobTokenIds'])` (same for
`outcomePrices`). **[CROSS-CHECK]** docs and live API and official client all agree — no conflict.

The parsed token ids are exactly the `assets_ids` for the CLOB subscribe frame (§1.2).

**Minimal Event JSON (abbreviated):**

```json
{"id":"2890","slug":"...","title":"...","markets":[{"id":"239826","conditionId":"0x064d33e3...ede1609a","outcomes":"[\"Yes\", \"No\"]","outcomePrices":"[\"0.0000004\", \"0.9999995\"]","clobTokenIds":"[\"28182404005967940652495463228537840901055649726248190462854914416579180110833\", \"47044845753450022047436429968808601130811164131571549682541703866165095016290\"]","closedTime":"2021-12-05 20:37:01+00"}]}
```

---

## 3. End-to-end recorder flow

1. **Gamma:** `GET /events/{eventId}` → parse `markets[]`. For each market collect
   `conditionId` and `json.loads(clobTokenIds)` → token ids.
2. **CLOB WS:** connect to `wss://ws-subscriptions-clob.polymarket.com/ws/market`, send
   `{"assets_ids":[...all token ids...],"type":"market"}`, then send text `PING` every 10 s.
   Record `book` (seed), apply `price_change`, log `last_trade_price` / `tick_size_change`.
3. Stamp every recorded message with a local receive timestamp (websocket payload timestamps
   are server-side).

---

## 4. OPEN QUESTIONS / must-verify-live

These are NOT confirmed from a primary source and must be validated against a live capture
before relying on them:

1. **CLOB `best_bid_ask` / `new_market` / `market_resolved` full schemas** (only under
   `custom_feature_enabled:true`). Only "key fields" are documented; whether they carry
   `asset_id`/`market`/`timestamp` is unconfirmed. Avoid depending on them; reconstruct
   top-of-book from `book` + `price_change` instead.
2. **CLOB `book` re-snapshot triggers.** Docs say "on subscribe" and "when a trade affects the
   book," but the exact conditions for a fresh full snapshot vs a `price_change` delta are not
   exhaustively specified — validate book-reconstruction against live data.
3. **CLOB `book.hash` stability for change detection.** Documented as "hash of the orderbook
   content"; whether it is stable/comparable across messages for the same state is implied,
   not guaranteed.
4. **CLOB `price_change.size` = new aggregate (level replace), `"0"` = remove.** Stated in
   AsyncAPI/agent-skills; confirm against live data when building book reconstruction.
5. **Gamma rate limits.** Not documented for these public GET endpoints; cadence is up to the
   client. Be conservative when paging.
6. **Gamma `/events?id=` multi-id batching.** `id` is array-typed in docs but only verified
   live with a single id.

> Historical note: earlier revisions of this document also specified the RTDS `comments`
> stream and its Gamma `/comments` backfill. Comment recording was removed from polytape
> (2026-07); see git history for the full RTDS findings (including the live finding that
> server-side `filters` suppresses all delivery, and that sports chat is parented to the
> Series rather than the Event).
