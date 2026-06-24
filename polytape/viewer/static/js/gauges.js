// RENDER: order-book imbalance bar + microprice marker within the spread.
// Pure render(book, el) drawing inline SVG.

import { fmtPrice, fmtSigned, clamp } from "./util.js";

function imbalanceSvg(imb) {
  // center needle; green fills right for bid-heavy, red left for ask-heavy.
  const pct = imb == null ? 0 : clamp(imb, -1, 1);
  const half = 50;
  const w = Math.abs(pct) * half;
  const x = pct >= 0 ? half : half - w;
  const color = pct >= 0 ? "var(--bid)" : "var(--ask)";
  return (
    `<svg viewBox="0 0 100 22" preserveAspectRatio="none">` +
    `<rect x="0" y="6" width="100" height="10" rx="3" fill="var(--panel-2)"/>` +
    (imb == null ? "" : `<rect x="${x}" y="6" width="${w}" height="10" rx="2" fill="${color}"/>`) +
    `<line x1="50" y1="2" x2="50" y2="20" stroke="var(--text-faint)" stroke-width="1"/>` +
    `</svg>`
  );
}

function micropriceSvg(book) {
  const m = book.metrics || {};
  const { best_bid: bb, best_ask: ba, microprice: mp } = m;
  if (bb == null || ba == null || ba <= bb || mp == null) {
    return `<svg viewBox="0 0 100 22"><rect x="0" y="9" width="100" height="4" rx="2" fill="var(--panel-2)"/></svg>`;
  }
  const t = clamp((mp - bb) / (ba - bb), 0, 1) * 100;
  return (
    `<svg viewBox="0 0 100 22" preserveAspectRatio="none">` +
    `<rect x="0" y="9" width="100" height="4" rx="2" fill="var(--panel-2)"/>` +
    `<circle cx="0" cy="11" r="3" fill="var(--bid)"/>` +
    `<circle cx="100" cy="11" r="3" fill="var(--ask)"/>` +
    `<line x1="${t}" y1="3" x2="${t}" y2="19" stroke="var(--accent)" stroke-width="2"/>` +
    `<circle cx="${t}" cy="11" r="3.5" fill="var(--accent)"/>` +
    `</svg>`
  );
}

export function renderGauges(book, el) {
  const m = (book && book.metrics) || {};
  el.innerHTML =
    `<div class="gauge-row" title="(bid_depth − ask_depth) / total depth">` +
    `<span class="glabel">Imbalance</span>${imbalanceSvg(m.imbalance)}` +
    `<span class="gval">${fmtSigned(m.imbalance)}</span></div>` +
    `<div class="gauge-row" title="size-weighted fair value within the spread">` +
    `<span class="glabel">Microprice</span>${micropriceSvg(book || {})}` +
    `<span class="gval">${fmtPrice(m.microprice)}</span></div>`;
}
