// RENDER: cumulative-depth (staircase) chart as inline SVG. Bid cumulative fills
// left of the touch (green), ask cumulative fills right (red), forming the depth
// valley. Pure render(book, el); sizes itself to the container.

import { fmtPrice, fmtSize } from "./util.js";

const PAD = { l: 8, r: 8, t: 14, b: 18 };

export function renderDepth(book, el) {
  const W = el.clientWidth || 600;
  const H = el.clientHeight || 240;
  const ladder = book && book.ladder;
  const bids = (ladder && ladder.bids) || [];
  const asks = (ladder && ladder.asks) || [];
  if (!bids.length && !asks.length) {
    el.innerHTML = `<svg width="${W}" height="${H}"><text x="${W / 2}" y="${H / 2}" fill="var(--text-faint)" text-anchor="middle" font-size="12">no book data</text></svg>`;
    return;
  }

  const prices = [...bids, ...asks].map((l) => l.price);
  const xMin = Math.min(...prices);
  const xMax = Math.max(...prices);
  const yMax = Math.max(ladder.max_cum || 0, 1);
  const span = xMax - xMin || 1;
  const X = (p) => PAD.l + ((p - xMin) / span) * (W - PAD.l - PAD.r);
  const Y = (c) => H - PAD.b - (c / yMax) * (H - PAD.t - PAD.b);
  const baseY = H - PAD.b;

  const area = (levels) => {
    if (!levels.length) return "";
    const pts = levels.map((l) => `${X(l.price).toFixed(1)},${Y(l.cum).toFixed(1)}`);
    const first = X(levels[0].price).toFixed(1);
    const last = X(levels[levels.length - 1].price).toFixed(1);
    return `M ${first},${baseY} L ${pts.join(" L ")} L ${last},${baseY} Z`;
  };

  const gridY = [0.25, 0.5, 0.75, 1].map((f) => {
    const y = Y(yMax * f).toFixed(1);
    return (
      `<line x1="${PAD.l}" y1="${y}" x2="${W - PAD.r}" y2="${y}" stroke="var(--grid-line)" stroke-width="1"/>` +
      `<text x="${PAD.l + 2}" y="${y - 2}" fill="var(--text-faint)" font-size="9">${fmtSize(yMax * f)}</text>`
    );
  });

  const mid = book.metrics && book.metrics.mid;
  const midLine =
    mid != null && mid >= xMin && mid <= xMax
      ? `<line x1="${X(mid).toFixed(1)}" y1="${PAD.t}" x2="${X(mid).toFixed(1)}" y2="${baseY}" stroke="var(--text-faint)" stroke-dasharray="3 3" stroke-width="1"/>`
      : "";

  el.innerHTML =
    `<svg width="${W}" height="${H}">` +
    gridY.join("") +
    `<path d="${area(bids)}" fill="var(--bid-soft)" stroke="var(--bid)" stroke-width="1.5"/>` +
    `<path d="${area(asks)}" fill="var(--ask-soft)" stroke="var(--ask)" stroke-width="1.5"/>` +
    midLine +
    `<text x="${PAD.l}" y="${H - 5}" fill="var(--text-faint)" font-size="10">${fmtPrice(xMin)}</text>` +
    `<text x="${W - PAD.r}" y="${H - 5}" fill="var(--text-faint)" font-size="10" text-anchor="end">${fmtPrice(xMax)}</text>` +
    `</svg>`;
}
