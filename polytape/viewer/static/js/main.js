// Composition root: wires api.js + live.js into the store and the store into all
// render modules. No reconstruction or parsing — that is the server's job.

import { api } from "./api.js";
import { openStream } from "./live.js";
import { createStore, INITIAL } from "./store.js";
import { renderLadder } from "./ladder.js";
import { renderMetrics } from "./metrics.js";
import { renderGauges } from "./gauges.js";
import { renderDepth } from "./depthchart.js";
import { renderTape } from "./tape.js";
import { createTimeseries } from "./timeseries.js";
import { fmtClock, el } from "./util.js";

const $ = (id) => document.getElementById(id);
const store = createStore(INITIAL);
const MAX_SERIES = 6000;
const TRADE_LIMIT = 80;

let stream = null;
let loadToken = 0; // monotonic; bumped by every load-initiating action
let depth = 25;

const newLoad = () => ++loadToken;
// A fetched result is stale if a newer load started, or the selected asset changed.
const stale = (token, assetId) => token !== loadToken || store.get().asset !== assetId;

// ---- DOM refs ---- //
const dom = {
  app: $("app"),
  boot: $("boot"),
  picker: $("picker"),
  pickerList: $("picker-list"),
  title: $("event-title"),
  slug: $("event-slug"),
  chips: $("asset-chips"),
  modeLive: $("mode-live"),
  modeHistory: $("mode-history"),
  connDot: $("conn-dot"),
  connText: $("conn-text"),
  gapBadge: $("gap-badge"),
  banner: $("banner"),
  metrics: $("metrics"),
  ladder: $("ladder"),
  depth: $("depth"),
  depthchart: $("depthchart"),
  gauges: $("gauges"),
  tape: $("tape"),
  tapeRate: $("tape-rate"),
  asof: $("asof"),
  seriesReadout: $("series-readout"),
  tsCanvas: $("ts-canvas"),
  tsTooltip: $("ts-tooltip"),
};

const timeseries = createTimeseries(dom.tsCanvas, dom.tsTooltip, {
  onSeek: (iso) => seek(iso, false),
  onSeekEnd: (iso) => seek(iso, true),
});

// ---- boot ---- //
async function boot() {
  let events = [];
  try {
    events = (await api.events()).events || [];
  } catch (e) {
    dom.boot.textContent = "could not reach the viewer server";
    return;
  }
  const wanted = new URLSearchParams(location.search).get("event");
  if (wanted && events.some((e) => e.event_id === wanted)) return loadEvent(wanted);
  if (events.length === 1) return loadEvent(events[0].event_id);
  if (events.length === 0) {
    dom.boot.textContent = "no captures found";
    return;
  }
  renderPicker(events);
}

function renderPicker(events) {
  dom.boot.hidden = true;
  dom.picker.hidden = false;
  dom.pickerList.innerHTML = "";
  for (const ev of events) {
    // Build with text nodes (NOT innerHTML): ev.title is third-party event
    // metadata and must never be interpreted as HTML.
    const title = el("div", { class: "p-title", text: ev.title || ev.event_id });
    const meta = el("div", { class: "p-meta" });
    meta.append(`${ev.dir} · ${ev.counts.book || 0} book msgs · `);
    meta.append(ev.live ? el("span", { class: "p-live", text: "● live" }) : "ended");
    const btn = el("button", {}, [title, meta]);
    btn.onclick = () => {
      dom.picker.hidden = true;
      loadEvent(ev.event_id);
    };
    dom.pickerList.append(el("li", {}, [btn]));
  }
}

async function loadEvent(eventId) {
  dom.boot.hidden = false;
  dom.boot.textContent = "loading capture…";
  const meta = await api.meta(eventId);
  const assets = meta.assets || [];
  const present = assets.find((a) => a.present) || assets[0];
  store.set({
    eventId,
    meta,
    assets,
    captureLive: !!meta.live,
    gaps: meta.gaps ? meta.gaps.map((g) => ({ from: g.disconnected_at, to: g.reconnected_at })) : [],
    timeRange: meta.time_range || { start: null, end: null },
    mode: meta.live ? "live" : "history",
    asset: present ? present.asset_id : null,
    label: present ? present.label : "",
  });
  renderHeader(store.get());
  dom.app.hidden = false;
  dom.boot.hidden = true;
  if (present) await selectAsset(present.asset_id);
}

async function selectAsset(assetId) {
  const s = store.get();
  const entry = s.assets.find((a) => a.asset_id === assetId);
  store.set({ asset: assetId, label: entry ? entry.label : assetId, scrubTs: null });
  if (stream) {
    stream.close();
    stream = null;
  }
  const token = newLoad();
  await Promise.all([
    loadSeries(assetId, token),
    loadBook(assetId, null, token),
    loadTrades(assetId, null, token),
  ]);
  if (store.get().mode === "live") connectLive();
}

// ---- data loads ---- //
async function loadSeries(assetId, token) {
  try {
    const res = await api.series(store.get().eventId, assetId, { max: 1500 });
    if (stale(token, assetId)) return;
    store.set({ series: res.points || [], gaps: res.gaps || store.get().gaps });
  } catch {
    /* keep prior */
  }
}

async function loadBook(assetId, at, token) {
  try {
    const book = await api.book(store.get().eventId, assetId, { at, depth });
    if (stale(token, assetId)) return;
    store.set({ book });
  } catch {
    /* keep prior */
  }
}

async function loadTrades(assetId, before, token) {
  try {
    const res = await api.trades(store.get().eventId, assetId, { before, limit: TRADE_LIMIT });
    if (stale(token, assetId)) return;
    store.set({ trades: res.trades || [] });
  } catch {
    /* keep prior */
  }
}

// ---- live ---- //
function connectLive() {
  const s = store.get();
  if (stream) stream.close();
  store.set({ connection: "reconnecting" });
  stream = openStream(s.eventId, s.asset, {
    onStatus: (status) => store.set({ connection: status }),
    onHello: (d) => store.set({ connection: "connected", captureLive: !!d.live }),
    onState: (state) => {
      if (store.get().mode !== "live" || state.asset_id !== store.get().asset) return;
      appendSeriesPoint(state);
      store.set({ book: state });
    },
    onTrade: (d) => {
      if (store.get().mode !== "live" || d.asset_id !== store.get().asset) return;
      const trades = [d.trade, ...store.get().trades].slice(0, TRADE_LIMIT);
      store.set({ trades });
    },
    onEnd: () => {
      // Capture ended: the server closes the stream after eof, so stop following
      // (otherwise EventSource auto-reconnects and clobbers the 'ended' status).
      if (stream) {
        stream.close();
        stream = null;
      }
      store.set({ captureLive: false, mode: "history", connection: "ended" });
    },
  });
}

function appendSeriesPoint(state) {
  const m = state.metrics || {};
  const pt = { ts: state.as_of, mid: m.mid, bid: m.best_bid, ask: m.best_ask, spread: m.spread, micro: m.microprice };
  const prev = store.get().series;
  const series = prev.length && prev[prev.length - 1].ts === pt.ts ? prev : [...prev, pt].slice(-MAX_SERIES);
  const tr = store.get().timeRange;
  store.set({ series, timeRange: { start: tr.start, end: state.as_of } });
}

// ---- modes + scrubbing ---- //
function setMode(mode) {
  const s = store.get();
  if (mode === "live" && !s.captureLive) return;
  if (mode === s.mode) return;
  store.set({ mode, scrubTs: null });
  if (mode === "live") {
    const token = newLoad();
    loadBook(s.asset, null, token);
    loadTrades(s.asset, null, token);
    connectLive();
  } else if (stream) {
    stream.close();
    stream = null;
    store.set({ connection: "ended" });
  }
}

async function seek(iso, isEnd) {
  const s = store.get();
  if (s.mode === "live") {
    if (stream) {
      stream.close();
      stream = null;
    }
    store.set({ mode: "history" });
  }
  store.set({ scrubTs: iso });
  const token = newLoad();
  const asset = s.asset;
  const [book, trades] = await Promise.all([
    api.book(s.eventId, asset, { at: iso, depth }).catch(() => null),
    api.trades(s.eventId, asset, { before: iso, limit: TRADE_LIMIT }).catch(() => null),
  ]);
  if (stale(token, asset)) return; // a newer load (seek or asset switch) superseded this
  const patch = {};
  if (book) patch.book = book;
  if (trades) patch.trades = trades.trades || [];
  store.set(patch);
}

// ---- header / banner ---- //
function renderHeader(s) {
  const meta = s.meta || {};
  dom.title.textContent = meta.title || s.eventId || "—";
  dom.slug.textContent = meta.slug || "";

  dom.chips.innerHTML = "";
  for (const a of s.assets) {
    const chip = document.createElement("button");
    chip.className = "chip" + (a.asset_id === s.asset ? " active" : "");
    if (a.outcome) chip.dataset.outcome = a.outcome;
    chip.textContent = a.label;
    chip.disabled = !a.present;
    chip.title = a.asset_id;
    chip.onclick = () => selectAsset(a.asset_id);
    dom.chips.append(chip);
  }

  dom.modeLive.classList.toggle("active", s.mode === "live");
  dom.modeHistory.classList.toggle("active", s.mode === "history");
  dom.modeLive.disabled = !s.captureLive;

  const dotClass =
    s.connection === "connected" ? "dot-green" : s.connection === "reconnecting" ? "dot-amber" : "dot-grey";
  dom.connDot.className = "dot " + dotClass;
  dom.connText.textContent =
    s.mode === "history" ? "history" : s.connection === "connected" ? "live" : s.connection;

  if (s.gaps && s.gaps.length) {
    dom.gapBadge.hidden = false;
    dom.gapBadge.textContent = `⚠ ${s.gaps.length} gap${s.gaps.length > 1 ? "s" : ""}`;
    dom.gapBadge.onclick = () => s.gaps[0] && seek(s.gaps[0].from, true);
  } else {
    dom.gapBadge.hidden = true;
  }
}

function renderBanner(s) {
  const b = s.book;
  let msg = "";
  if (b && b.stale_after_gap) msg = "⚠ Inside a reconnect gap — book state is unknown until the next snapshot.";
  else if (b && b.book_unseeded) msg = "⚠ Partial book — deltas seen before any snapshot.";
  dom.banner.hidden = !msg;
  dom.banner.textContent = msg;
}

// ---- render subscription ---- //
store.subscribe((s) => {
  renderHeader(s);
  renderBanner(s);
  if (s.book) {
    renderMetrics(s.book, dom.metrics);
    renderLadder(s.book, dom.ladder, depth);
    renderDepth(s.book, dom.depthchart);
    renderGauges(s.book, dom.gauges);
    dom.asof.textContent = s.book.as_of ? (s.mode === "live" ? "live · " : "as of ") + fmtClock(s.book.as_of) : "";
  }
  renderTape(s.trades, dom.tape, dom.tapeRate);
  timeseries.update(s);
  const head = s.scrubTs || (s.timeRange && s.timeRange.end);
  dom.seriesReadout.textContent = head ? fmtClock(head) : "";
});

// ---- controls ---- //
dom.modeLive.onclick = () => setMode("live");
dom.modeHistory.onclick = () => setMode("history");
dom.depth.onchange = () => {
  depth = parseInt(dom.depth.value, 10) || 25;
  const s = store.get();
  if (s.mode === "history" && s.asset) loadBook(s.asset, s.scrubTs, newLoad());
  else if (s.book) renderLadder(s.book, dom.ladder, depth);
};
window.addEventListener("resize", () => {
  const s = store.get();
  if (s.book) renderDepth(s.book, dom.depthchart);
});

boot();
