// RENDER: the depth ladder. asks descending above a mid/spread divider, bids
// descending below; each row shows price | size | cumulative with a cumulative-
// depth bar. Pure render(book, el, depth); flashes levels whose size changed.

import { fmtPrice, fmtSize } from "./util.js";

let prev = { asset: null, sizes: new Map() };

function rowHtml(side, lvl, maxCum, isBest, flash) {
  const w = maxCum > 0 ? (lvl.cum / maxCum) * 100 : 0;
  return (
    `<div class="ladder-row row-${side}${isBest ? " row-best" : ""}${flash ? " " + flash : ""}">` +
    `<div class="bar" style="width:${w.toFixed(1)}%"></div>` +
    `<div class="cell price">${fmtPrice(lvl.price)}</div>` +
    `<div class="cell size">${fmtSize(lvl.size)}</div>` +
    `<div class="cell cum">${fmtSize(lvl.cum)}</div>` +
    `</div>`
  );
}

export function renderLadder(book, el, depth = 25) {
  if (!book || !book.ladder) {
    el.innerHTML = `<div class="ladder-empty">no data</div>`;
    return;
  }
  const { bids, asks, max_cum } = book.ladder;
  if (book.asset_id !== prev.asset) prev = { asset: book.asset_id, sizes: new Map() };

  const nextSizes = new Map();
  const flashFor = (lvl) => {
    const key = lvl.price.toFixed(4);
    nextSizes.set(key, lvl.size);
    const old = prev.sizes.get(key);
    if (old === undefined || old === lvl.size) return "";
    return lvl.size > old ? "flash-up" : "flash-dn";
  };

  const showAsks = asks.slice(0, depth);
  const showBids = bids.slice(0, depth);

  let html = "";
  if (!showAsks.length && !showBids.length) {
    el.innerHTML = book.book_unseeded
      ? `<div class="ladder-empty">awaiting first snapshot…</div>`
      : `<div class="ladder-empty">empty book</div>`;
    prev.sizes = nextSizes;
    return;
  }

  // asks: render highest price at top, best (lowest) just above the divider
  for (let i = showAsks.length - 1; i >= 0; i--) {
    html += rowHtml("ask", showAsks[i], max_cum, i === 0, flashFor(showAsks[i]));
  }
  const m = book.metrics || {};
  const crossed = m.spread != null && m.spread < 0;
  html +=
    `<div class="ladder-mid">` +
    `<span>spread <b class="spread-val${crossed ? " warn" : ""}">${m.spread == null ? "—" : fmtPrice(m.spread)}</b></span>` +
    `<span class="mid-val">${m.mid == null ? "—" : fmtPrice(m.mid)}</span>` +
    `</div>`;
  for (let i = 0; i < showBids.length; i++) {
    html += rowHtml("bid", showBids[i], max_cum, i === 0, flashFor(showBids[i]));
  }

  el.innerHTML = html;
  prev.sizes = nextSizes;
}
