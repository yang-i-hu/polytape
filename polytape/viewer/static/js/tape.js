// RENDER: recent-trades tape. Pure render(trades, el); newest on top, BUY green /
// SELL red, freshest row flashes. `trades` is newest-first (as the API returns).

import { fmtPrice, fmtSize, fmtClock } from "./util.js";

let lastTopTx = null;

export function renderTape(trades, el, rateEl) {
  if (!trades || !trades.length) {
    el.innerHTML = `<div class="tape-empty">no trades</div>`;
    if (rateEl) rateEl.textContent = "";
    lastTopTx = null;
    return;
  }
  const topTx = trades[0].tx || trades[0].ts;
  const isNewTop = topTx !== lastTopTx;
  lastTopTx = topTx;

  el.innerHTML = trades
    .map((t, i) => {
      const side = (t.side || "").toLowerCase() === "sell" ? "sell" : "buy";
      const fresh = i === 0 && isNewTop ? " fresh" : "";
      return (
        `<div class="trade-row ${side}${fresh}">` +
        `<span class="t-time">${fmtClock(t.ts)}</span>` +
        `<span class="t-price">${fmtPrice(t.price)}</span>` +
        `<span class="t-size">${fmtSize(t.size)}</span>` +
        `</div>`
      );
    })
    .join("");

  if (rateEl) rateEl.textContent = `${trades.length} shown`;
}
