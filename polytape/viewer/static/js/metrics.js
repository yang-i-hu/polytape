// RENDER: the top-of-book metric tiles. Pure render(book, el).

import { fmtPrice, fmtSize, fmtBps, fmtSigned } from "./util.js";

function tile(label, value, cls = "", sub = "") {
  return (
    `<div class="tile" title="${label}">` +
    `<div class="label">${label}</div>` +
    `<div class="value ${cls}">${value}</div>` +
    (sub ? `<div class="sub">${sub}</div>` : "") +
    `</div>`
  );
}

export function renderMetrics(book, el) {
  const m = (book && book.metrics) || {};
  const crossed = m.spread != null && m.spread < 0;
  el.innerHTML =
    tile("Best bid", fmtPrice(m.best_bid), "bid") +
    tile("Best ask", fmtPrice(m.best_ask), "ask") +
    tile(
      "Spread",
      m.spread == null ? "—" : fmtPrice(m.spread),
      crossed ? "warn" : "",
      m.spread_bps == null ? "" : `${fmtBps(m.spread_bps)} bps`
    ) +
    tile("Mid", fmtPrice(m.mid)) +
    tile("Microprice", fmtPrice(m.microprice)) +
    tile(
      "Imbalance",
      fmtSigned(m.imbalance),
      m.imbalance == null ? "" : m.imbalance > 0 ? "bid" : "ask"
    ) +
    tile("Bid depth", fmtSize(m.total_bid), "bid", `${m.levels_bid ?? 0} lvls`) +
    tile("Ask depth", fmtSize(m.total_ask), "ask", `${m.levels_ask ?? 0} lvls`);
}
