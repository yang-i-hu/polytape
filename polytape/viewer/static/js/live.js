// Data layer: SSE wrapper. EventSource auto-reconnects (and the server re-sends a
// fresh snapshot on connect), so on reconnect the client re-seeds rather than
// drifting. No DOM access — callbacks hand parsed data to the store.

export function openStream(eventId, asset, handlers) {
  const q = asset ? `?asset=${encodeURIComponent(asset)}` : "";
  const es = new EventSource(`/api/v1/events/${eventId}/stream${q}`);

  const on = (name, fn) =>
    es.addEventListener(name, (e) => {
      try {
        fn(JSON.parse(e.data));
      } catch {
        /* ignore malformed frame */
      }
    });

  es.addEventListener("open", () => handlers.onStatus && handlers.onStatus("connected"));
  es.addEventListener("error", () => handlers.onStatus && handlers.onStatus("reconnecting"));
  on("hello", (d) => handlers.onHello && handlers.onHello(d));
  on("snapshot", (d) => handlers.onState && handlers.onState(d, "snapshot"));
  on("price_change", (d) => handlers.onState && handlers.onState(d, "price_change"));
  on("last_trade_price", (d) => handlers.onTrade && handlers.onTrade(d));
  on("tick_size_change", (d) => handlers.onTick && handlers.onTick(d));
  on("eof", (d) => handlers.onEnd && handlers.onEnd(d));

  return { close: () => es.close() };
}
