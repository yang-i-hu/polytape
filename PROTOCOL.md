# polytape — Polymarket Feed Recorder Protocol Spec

Authoritative, implementation-ready spec for recording a single Polymarket Event's
comment stream (RTDS), order-book stream (CLOB market channel), and backfill/resolution
data (Gamma REST).

Ground-truth precedence: **official client SOURCE CODE > prose docs.** Disagreements are
flagged inline with **[CROSS-CHECK]**. Items not confirmed from a primary source are in
**OPEN QUESTIONS** at the bottom.

---

## 0. The three data planes at a glance

| Plane | Transport | Endpoint | Keyed by | Auth |
|-------|-----------|----------|----------|------|
| Comments (RTDS) | WebSocket, JSON text frames | `wss://ws-live-data.polymarket.com` | numeric Gamma **event id** (`parentEntityID`) | none |
| Order book (CLOB) | WebSocket, JSON + plain-text heartbeat | `wss://ws-subscriptions-clob.polymarket.com/ws/market` | CLOB **token ids** (`assets_ids`) | none (public market channel) |
| Resolution + backfill (Gamma) | HTTP GET (REST) | `https://gamma-api.polymarket.com` | event id / token id / comment id | none |

The numeric event id is the join key between Gamma and RTDS. The CLOB token ids (obtained
from Gamma `markets[].clobTokenIds`) are the join key into the CLOB stream. **Start with
Gamma to resolve everything, then open the two websockets.**

---

## 1. RTDS comment stream

### 1.1 Connection

- **URL:** `wss://ws-live-data.polymarket.com`
  (`DEFAULT_HOST`, `src/client.ts`). Plain WebSocket, JSON text frames. No path, no query
  params, no auth for the public `comments` topic.

### 1.2 Subscribe frame (exact wire format)

The client builds the frame as `JSON.stringify({ action: "subscribe", ...msg })` where `msg`
is `{ subscriptions: [ ... ] }`. The outer frame has exactly two keys: `action` and
`subscriptions`. There is **no top-level `topic`/`type`/auth** — those live per-subscription.

Per-subscription object keys (`src/model.ts`): `topic` (req), `type` (req), `filters`
(optional **string**), `clob_auth` (optional), `gamma_auth` (optional).

**The frame polytape actually sends** (the unfiltered firehose — see the LIVE FINDING in
§1.3; server-side `filters` suppresses all delivery, so polytape filters client-side):

```json
{"action":"subscribe","subscriptions":[{"topic":"comments","type":"*"}]}
```

**What the reference client documents but does NOT work live** (a per-event `filters` string —
kept here only to explain why it is *absent* above):

```json
{"action":"subscribe","subscriptions":[{"topic":"comments","type":"*","filters":"{\"parentEntityID\":20200,\"parentEntityType\":\"Event\"}"}]}
```

**Unsubscribe (identical but `action`):**

```json
{"action":"unsubscribe","subscriptions":[{"topic":"comments","type":"*"}]}
```

### 1.3 Event filter format (critical detail)

- `filters` is a **STRING whose content is itself JSON** (TS type `filters?: string`). It is
  NOT a nested object. You must `JSON.stringify` the inner filter and place it as a string.
- Inner filter shape for comments:
  `{"parentEntityID": <number>, "parentEntityType": "Event"}`
- `parentEntityID` is a **NUMBER** — the numeric Gamma **event id** (not a slug, not a
  condition id, not a CLOB token id). `parentEntityType` is `"Event"` (or `"Series"`).
- `type` selects the subtype: `"*"` for all four, or one of
  `comment_created` / `comment_removed` / `reaction_created` / `reaction_removed`.
- Empty `filters` (`""`) or omitting it = **no filtering** (all comments platform-wide).
  This is what polytape relies on: per the LIVE FINDING below, setting `filters` suppresses
  all delivery, so polytape deliberately records the unfiltered firehose and filters
  client-side (by `(parentEntityType, parentEntityID)`).

**[CROSS-CHECK — docs vs source]** Polymarket's prose docs page at one point implied comments
**cannot** be filtered to a single event. The official client repo contradicts this: the
README messages-hierarchy table and `examples/quick-connection.ts` show
`filters: {"parentEntityID":20200,"parentEntityType":"Event"}`. **Source/repo wins — per-event
filtering is supported.** (Still flagged in OPEN QUESTIONS to verify live, since it is the load-bearing assumption for the whole recorder.)

> **⚠️ LIVE FINDING (2026-06-15) — server-side filtering does NOT work.** A controlled
> same-window test (one unfiltered firehose vs. three filtered connections: `filters` as a
> stringified int, a stringified string, and a raw object) found the firehose received the
> event's comments while **all three filtered connections received zero messages**. Setting
> any `filters` field appears to suppress all delivery. **polytape therefore subscribes to the
> unfiltered comment firehose and filters client-side** by `parentEntityID` (reactions, which
> carry no `parentEntityID`, are matched by `commentID` against comments seen in the session).
> Global comment volume is low, so the firehose overhead is negligible.

### 1.4 Keepalive / ping (confirmed from source)

- Application-level **text** keepalive — NOT a WebSocket-protocol ping opcode, NOT a JSON
  object. The client calls `this.ws.send("ping", ...)` (`src/client.ts:151`) — the literal
  4-byte lowercase string `ping`.
- Cadence: ping sent immediately `onOpen`; then pong-driven — `onPong` does
  `delay(this.pingInterval).then(() => this.ping())`. `DEFAULT_PING_INTERVAL = 5000` ms.
- **Recorder strategy:** send the text `ping` on connect, then every ~5 s on a fixed timer.
  Do not rely on a server pong to re-arm (simpler and robust).

**[CROSS-CHECK — note]** The client wires `this.ws.pong = this.onPong` (non-standard). It is
ambiguous whether the server replies with a literal text `pong` frame or a WS-protocol pong
control frame. Be prepared to ignore either. (OPEN QUESTION.)

### 1.5 Incoming message envelope (confirmed from `src/model.ts`)

Every inbound message has this envelope:

```
topic         string
type          string
timestamp     number   (push time; units UNCONFIRMED — see OPEN QUESTIONS, treat as epoch ms)
payload       object   (the Comment or Reaction, below)
connection_id string   (per-connection, not per-message)
```

There is **no envelope-level id**. Dedup on `payload.id`.

**[CROSS-CHECK — note]** The client only forwards frames whose raw text contains the substring
`"payload"` (`src/client.ts:164`) and parses with `JSON.parse`. Any ack/error/control frames
without that substring are dropped by the reference client and their shape is unknown. A
recorder writing its own client should log everything regardless.

### 1.6 Comment payload (`comment_created` / `comment_removed`)

Fields (from README schema table; `model.ts` types `payload` as generic `object`, so extra
wire fields may appear — see OPEN QUESTIONS):

```
id               string   <-- DEDUP KEY (unique comment id)
body             string
parentEntityType string   "Event" | "Series"
parentEntityID   number
parentCommentID  string   ("" for top-level)
userAddress      string   <-- HASH THIS (commenter wallet, 0x hex)
replyAddress     string
createdAt        string   (ISO-8601-like; format not pinned by source)
updatedAt        string
```

Literal example:

```json
{"topic":"comments","type":"comment_created","timestamp":1718380800000,"connection_id":"abc123","payload":{"id":"123456","body":"hello","parentEntityType":"Event","parentEntityID":20200,"parentCommentID":"","userAddress":"0xabc...","replyAddress":"","createdAt":"2026-06-14T12:00:00Z","updatedAt":"2026-06-14T12:00:00Z"}}
```

### 1.7 Reaction payload (`reaction_created` / `reaction_removed`)

```
id           string   <-- DEDUP KEY (unique reaction id)
commentID    number   (FK to the parent comment; reactions carry NO parentEntityID)
reactionType string
icon         string
userAddress  string   <-- HASH THIS
createdAt    string
```

```json
{"topic":"comments","type":"reaction_created","timestamp":1718380800000,"connection_id":"abc123","payload":{"id":"789","commentID":123456,"reactionType":"like","icon":"👍","userAddress":"0xabc...","createdAt":"2026-06-14T12:00:00Z"}}
```

> A Reaction does **not** include `parentEntityID`. To attribute it to the event, either
> rely on the subscription filter (it only delivers this event's reactions) or join on
> `commentID`.

### 1.8 Recorder field map for comments

- **Dedup id:** `payload.id` (string), for both comments and reactions.
- **Server timestamp:** envelope `timestamp` (number, push time) AND `payload.createdAt`
  (string, content time). Prefer `createdAt` for ordering/replay; keep `timestamp` too.
- **Username/identifier to hash:** `payload.userAddress` (the wallet). Hash this for privacy.
- **Always add your own client receive timestamp** — the envelope timestamp unit is unconfirmed.

---

## 2. CLOB book stream

### 2.1 Connection

- **URL:** `wss://ws-subscriptions-clob.polymarket.com/ws/market`

### 2.2 Subscribe frame (exact wire format)

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

### 2.3 Keepalive / ping (CLOB-specific — different from RTDS!)

- Client MUST send the literal **uppercase** text frame `PING` (plain text, NOT JSON) every
  **~10 s**. Server replies with literal text `PONG` (ignore it).
- Miss it and the server drops the connection after ~10 s.

> **Do not reuse the RTDS keepalive code here.** RTDS = lowercase `ping` every 5 s;
> CLOB market = uppercase `PING` every 10 s. Different casing, different cadence.
> (The CLOB *sports* channel inverts this — server pings, client replies `pong` — but that is a
> different endpoint, out of scope.)

### 2.4 Incoming message types

All timestamps are **strings of Unix epoch MILLISECONDS** (e.g. `"1757908892351"`), confirmed
in AsyncAPI + market.md. There is no client/recv timestamp in the payload — **the recorder
must add its own receive timestamp.**

#### 2.4.1 `book` — full snapshot

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

#### 2.4.2 `price_change` — delta

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

#### 2.4.3 `last_trade_price` — single trade

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

#### 2.4.4 `tick_size_change` — event notification

```
event_type     "tick_size_change"
asset_id       string
market         string   (0x condition id)
old_tick_size  string
new_tick_size  string
timestamp      string   (epoch ms)
```

No dedicated id/hash field; key on `asset_id` + `timestamp`.

#### 2.4.5 `best_bid_ask`, `new_market`, `market_resolved` — only if `custom_feature_enabled:true`

Semi-confirmed (only "key fields" documented; full schemas/timestamps unconfirmed — see OPEN
QUESTIONS). polytape can leave `custom_feature_enabled` off by default and reconstruct
top-of-book from `book` + `price_change`.

### 2.5 Recorder field map for CLOB

| Message | id / dedup | token key | condition id | server ts |
|---------|-----------|-----------|--------------|-----------|
| `book` | `hash` (top-level) | `asset_id` (top-level) | `market` | `timestamp` (ms str) |
| `price_change` | `price_changes[].hash` | `price_changes[].asset_id` | `market` (top-level) | `timestamp` (top-level, ms str) |
| `last_trade_price` | `transaction_hash` (optional) | `asset_id` | `market` | `timestamp` (ms str) |
| `tick_size_change` | none (use `asset_id`+`timestamp`) | `asset_id` | `market` | `timestamp` (ms str) |

Optional REST seed/reconcile: `POST https://clob.polymarket.com/book` body
`{"token_id":"<CLOB_TOKEN_ID>"}` → `OrderBookSummary {market, asset_id, timestamp, bids, asks, hash}`.

---

## 3. Gamma REST — resolution + backfill

Base: `https://gamma-api.polymarket.com`. Unauthenticated GET only. No keepalive.

### 3.1 Resolve Event ID → markets → CLOB token ids (confirmed live + against `agents/.../gamma.py`)

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

The parsed token ids are exactly the `assets_ids` for the CLOB subscribe frame (§2.2). The
numeric `event.id` is exactly the `parentEntityID` for the RTDS comment filter (§1.3) and the
`parent_entity_id` for the comments backfill (§3.2).

**Minimal Event JSON (abbreviated):**

```json
{"id":"2890","slug":"...","title":"...","markets":[{"id":"239826","conditionId":"0x064d33e3...ede1609a","outcomes":"[\"Yes\", \"No\"]","outcomePrices":"[\"0.0000004\", \"0.9999995\"]","clobTokenIds":"[\"28182404005967940652495463228537840901055649726248190462854914416579180110833\", \"47044845753450022047436429968808601130811164131571549682541703866165095016290\"]","closedTime":"2021-12-05 20:37:01+00"}]}
```

### 3.2 Backfill /comments for an event since a last-seen comment id (confirmed live)

**Call (deterministic oldest-first paging):**

```
GET https://gamma-api.polymarket.com/comments?parent_entity_type=Event&parent_entity_id=80505&limit=100&offset=0&order=createdAt&ascending=true
```

Params:

- `parent_entity_type` — `Event` | `Series` | `market`. **Required in practice** (omitting
  parent params returns HTTP **422**). Use `Event` (confirmed working live; matches the
  `parentEntityType` returned in data).
- `parent_entity_id` — integer, the numeric event id (same value as RTDS `parentEntityID`).
  **Required in practice.**
- `limit` (int), `offset` (int) — paging.
- `order` (comma-separated field names, e.g. `createdAt`), `ascending` (bool). Use
  `order=createdAt&ascending=true` for oldest→newest. (`order` only works once parent params
  are present; bare `order` also 422s.)
- Optional: `get_positions` (bool), `holders_only` (bool).

**Resume-since-last-seen:** there is **NO `after=<commentId>` or `since=<ts>` cursor**. Paging
is `limit`/`offset` + `order`/`ascending` only. Implement resume client-side: sort
`ascending` by `createdAt`, walk `offset` forward, and for each comment compare its `id` /
`createdAt` against the last recorded one; stop ingesting once you reach already-seen ids.
Dedup on `id`.

**Comment object fields (live):**

```
id               string   <-- DEDUP KEY (e.g. "2064395")
body             string
parentEntityType string   "Event"
parentEntityID   number   (80505)
parentCommentID  string   (present on replies)
userAddress      string   <-- HASH THIS (lowercase 0x wallet)
createdAt        string   RFC3339/ISO-8601 UTC, e.g. "2025-11-16T19:05:08.13357Z"  <-- order key
updatedAt        string
reportCount      number
reactionCount    number
profile          object   { name, pseudonym, displayUsernamePublic, proxyWallet, baseAddress, profileImage }
reactions        array    (often absent/empty in samples)
```

```json
[{"id":"2064395","body":"fire patel","parentEntityType":"Event","parentEntityID":80505,"userAddress":"0xdbdb3f890ded7641f6a64d2a953fedfbe5ae95a2","createdAt":"2025-11-16T19:05:08.13357Z","updatedAt":"2025-11-16T19:05:21.516223Z","profile":{"name":"feralnectar81","pseudonym":"Vain-Ethics","displayUsernamePublic":true,"proxyWallet":"0xdbdb3f890ded7641f6a64d2a953fedfbe5ae95a2","baseAddress":"0xdbdb3f890ded7641f6a64d2a953fedfbe5ae95a2"},"reportCount":0,"reactionCount":0}]
```

### 3.3 Username/identifier to hash (privacy)

The field that **uniquely identifies the user** and should be hashed is the **wallet**:
`userAddress` (== `profile.proxyWallet` == `profile.baseAddress`, all the same lowercase 0x
address). The human handle is `profile.name` (fallback `profile.pseudonym`);
`profile.displayUsernamePublic` indicates whether `name` is public. Hash `userAddress`.

> Consistency win: the RTDS live stream (§1.6) and the Gamma backfill (§3.2) both expose the
> same `userAddress` wallet and the same comment `id`, so dedup-by-`id` and hash-by-`userAddress`
> work uniformly across both the live and backfill paths.

---

## 4. End-to-end recorder flow

1. **Gamma:** `GET /events/{eventId}` → parse `markets[]`. For each market collect
   `conditionId` and `json.loads(clobTokenIds)` → token ids. Keep the numeric `event.id`.
2. **CLOB WS:** connect to `wss://ws-subscriptions-clob.polymarket.com/ws/market`, send
   `{"assets_ids":[...all token ids...],"type":"market"}`, then send text `PING` every 10 s.
   Record `book` (seed), apply `price_change`, log `last_trade_price` / `tick_size_change`.
3. **RTDS WS:** connect to `wss://ws-live-data.polymarket.com`, send the **unfiltered**
   subscribe (server-side `filters` suppresses all delivery — see §1.3 LIVE FINDING):
   `{"action":"subscribe","subscriptions":[{"topic":"comments","type":"*"}]}`,
   then send text `ping` every 5 s. Record `comment_*` / `reaction_*` payloads, dedup on
   `payload.id`, and **keep only this capture's `(parentEntityType, parentEntityID)`**
   client-side (`Event`+event id, or `Series`+series id for a parent-league chat).
4. **Gamma backfill:** on startup / after gaps, page
   `GET /comments?parent_entity_type=Event&parent_entity_id=<event.id>&order=createdAt&ascending=true&limit=100&offset=N`
   until you reach the last recorded comment id. Dedup on `id` against the live stream.
5. Stamp every recorded message with a local receive timestamp (websocket payload timestamps
   are server-side and, for RTDS, of unconfirmed units).

---

## 5. OPEN QUESTIONS / must-verify-live

These are NOT confirmed from a primary source and must be validated against a live capture
before relying on them:

1. **RTDS envelope `timestamp` units.** `model.ts` only says "Timestamp of when the message
   was sent" (number); the unit is not stated. Other RTDS topics document ms, so epoch-ms is
   the strong assumption — verify on a live frame.
2. **RTDS `createdAt`/`updatedAt` string format.** Typed as `string` "Creation timestamp" in
   the README; prose docs once said "ISO 8601" but source does not pin it. Gamma's REST
   comments return RFC3339 UTC with fractional seconds (`...19:05:08.13357Z`) — the WS stream
   likely matches but is unconfirmed.
3. **RTDS per-event comment filtering actually works on the wire.** Repo says yes (filter by
   `parentEntityID`/`parentEntityType`); an older prose docs page implied no. This is the
   load-bearing assumption for the whole comment recorder — confirm with a live subscribe.
4. **RTDS extra/undocumented payload fields.** `model.ts` types `payload` as a generic
   `object`; the field list comes from the README schema table. Live frames may carry extra
   fields (profile name/pseudonym/image, like counts, reportCount). Capture and inspect.
5. **RTDS server reply to keepalive.** Client assigns `ws.pong = onPong` (non-standard); unclear
   whether the server sends a literal text `pong` or a WS-protocol pong control frame, and
   what the server-side idle-timeout actually is (5 s is the client cadence, not a documented
   server limit).
6. **RTDS comments snapshot-on-subscribe.** Only `crypto_prices`/`equity_prices` document an
   initial dump; comments appear delta-only (inferred from absence of a snapshot handler).
   Confirm there is no historical backfill on subscribe — this is **why the Gamma /comments
   backfill in §3.2 is required.**
7. **RTDS control/ack/error frame shapes.** The reference client drops any frame lacking the
   substring `"payload"`; the shape of acks/errors is unknown. A custom recorder should log
   raw frames to discover them.
8. **CLOB `best_bid_ask` / `new_market` / `market_resolved` full schemas** (only under
   `custom_feature_enabled:true`). Only "key fields" are documented; whether they carry
   `asset_id`/`market`/`timestamp` is unconfirmed. Avoid depending on them; reconstruct
   top-of-book from `book` + `price_change` instead.
9. **CLOB `book` re-snapshot triggers.** Docs say "on subscribe" and "when a trade affects the
   book," but the exact conditions for a fresh full snapshot vs a `price_change` delta are not
   exhaustively specified — validate book-reconstruction against live data.
10. **CLOB `book.hash` stability for change detection.** Documented as "hash of the orderbook
    content"; whether it is stable/comparable across messages for the same state is implied,
    not guaranteed.
11. **CLOB `price_change.size` = new aggregate (level replace), `"0"` = remove.** Stated in
    AsyncAPI/agent-skills; confirm against live data when building book reconstruction.
12. **Gamma rate limits.** Not documented for these public GET endpoints; cadence is up to the
    client. Be conservative when paging backfill.
13. **`parent_entity_type` casing.** Docs list `Event`/`Series`/`market` (note lowercase
    `market`). `Event` confirmed working live; the lowercase `market` value is untested here.
14. **Gamma `/events?id=` multi-id batching.** `id` is array-typed in docs but only verified
    live with a single id.

---

## 6. US mobile-app "live chat" — what it is, and the boundary of that claim

The Polymarket **US mobile app**'s live chat (typed messages, bursty — several per second on a
marquee live event) is the **RTDS `comments` topic documented in §1** — the same public,
anonymous feed polytape records. The dashboard's "⚡ Live chat now" button samples this firehose
(`polytape/streams/discover.py`) to find which events are actively chatting, so a capture is
pointed at a busy event rather than a dead one.

**Verified live (2026-06-16):** `wss://ws-live-data.polymarket.com`, subscribe
`{"action":"subscribe","subscriptions":[{"topic":"comments","type":"*"}]}`, anonymous —
delivers real typed comments (`body` + `parentEntityID` + `parentEntityType` + `profile` +
`userAddress`) and reactions, captured end-to-end through polytape's production pipeline. This
matches the envelope the US app uses internally (a `ClientWSLiveDataSubscriptionMessage`:
`{action, subscriptions:[{topic, type, filters}], channelKey}`), decoded by static analysis of
the app's native lib `libPolymarketUI.so`.

**Negative results (recorded so the boundary is auditable):**
- The app's **private** gateway `wss://gateway-ws-markets.polymarket.us/v1/ws/subscriptions`
  rejected every reconstructed subscribe envelope with `{"error":"invalid_message"}`; its
  `subscriptionType` enum is market-data only (`marketData`/`marketDataLite`/`trade`/`order`/
  `position`/`accountBalance`) — **no comment type**.
- `wss://sports-api.polymarket.com/ws` (subscribe `{"channel":"comments"|"prices"|...}`)
  carries **live game scores only**, not chat.
- Running the app in an emulator to capture its real traffic was **impossible on the dev host**
  (x86_64 Windows): Android Emulator v36 refuses arm64 AVDs ("not supported by QEMU2 on x86_64
  host") and the x86 image can't install the ARM-only app (`INSTALL_FAILED_NO_MATCHING_ABIS`).

**NOT proven:** that the installed US app's *private* socket is byte-for-byte identical to the
public `.com` feed (it was never observed on the wire — the emulator was unavailable). The
grounded, reproducible claim is narrow: the **public** Polymarket RTDS `comments` topic carries
the typed chat, and Gamma `/comments` backfill agrees on `id`/`userAddress`. polytape records
that public feed; for shared events that is the same comment corpus the app surfaces.
