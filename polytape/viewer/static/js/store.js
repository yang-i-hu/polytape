// The single ViewState and the seam between data and view. No fetching, no
// rendering — main.js feeds it from api.js/live.js and subscribes render modules.

export function createStore(initial) {
  let state = { ...initial };
  const subscribers = new Set();

  const get = () => state;

  const set = (patch) => {
    state = { ...state, ...(typeof patch === "function" ? patch(state) : patch) };
    for (const fn of subscribers) fn(state);
  };

  const subscribe = (fn) => {
    subscribers.add(fn);
    return () => subscribers.delete(fn);
  };

  return { get, set, subscribe };
}

export const INITIAL = {
  eventId: null,
  meta: null,
  assets: [],
  asset: null, // selected asset_id
  label: "",
  mode: "history", // "live" | "history"
  captureLive: false, // capture still recording (meta.stopped_at == null)
  connection: "offline", // "connected" | "reconnecting" | "offline"
  book: null, // shared state object
  series: [], // [{ts, mid, bid, ask, spread, micro}]
  gaps: [], // [{from,to}]
  trades: [], // newest-first
  scrubTs: null, // null => latest
  timeRange: { start: null, end: null },
};
