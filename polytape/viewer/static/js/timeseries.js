// RENDER + interaction: the mid-price time-series on a <canvas> with a draggable
// scrubber. Drawing is pure (fed by state); the scrubber emits seek(tsIso) which
// main.js turns into a History-mode /book fetch. In live mode the playhead tracks
// the latest point. Factory returns { update(state) }.

import { tsMs, msToIso, clamp, fmtPrice, fmtClock } from "./util.js";

const PAD = { l: 46, r: 12, t: 10, b: 18 };

export function createTimeseries(canvas, tooltipEl, { onSeek, onSeekEnd } = {}) {
  const ctx = canvas.getContext("2d");
  let last = null; // last state, for resize redraws
  let dragging = false;
  let domain = null; // { x0, x1, plotL, plotR }
  let lastSeek = 0;

  function cssSize() {
    return { w: canvas.clientWidth || 600, h: canvas.clientHeight || 180 };
  }

  function xToMs(px) {
    if (!domain) return NaN;
    const { x0, x1, plotL, plotR } = domain;
    return x0 + ((px - plotL) / Math.max(1, plotR - plotL)) * (x1 - x0);
  }

  function draw(state) {
    last = state;
    const { w, h } = cssSize();
    const dpr = window.devicePixelRatio || 1;
    canvas.width = Math.round(w * dpr);
    canvas.height = Math.round(h * dpr);
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, w, h);

    const series = state.series || [];
    const plotL = PAD.l;
    const plotR = w - PAD.r;
    const plotT = PAD.t;
    const plotB = h - PAD.b;

    if (series.length < 2) {
      ctx.fillStyle = "#5a6678";
      ctx.font = "12px ui-monospace, monospace";
      ctx.fillText("waiting for data…", plotL, (plotT + plotB) / 2);
      domain = null;
      return;
    }

    let x0 = tsMs(state.timeRange && state.timeRange.start) || tsMs(series[0].ts);
    let x1 = tsMs(state.timeRange && state.timeRange.end) || tsMs(series[series.length - 1].ts);
    const lastTs = tsMs(series[series.length - 1].ts);
    if (lastTs > x1) x1 = lastTs;
    if (x1 <= x0) x1 = x0 + 1;
    domain = { x0, x1, plotL, plotR };

    const lows = [];
    const highs = [];
    for (const p of series) {
      const lo = p.bid != null ? p.bid : p.mid;
      const hi = p.ask != null ? p.ask : p.mid;
      if (lo != null) lows.push(lo);
      if (hi != null) highs.push(hi);
    }
    if (!lows.length || !highs.length) {
      // >=2 points but none priced (empty/one-sided book stretch): avoid NaN axes.
      ctx.fillStyle = "#5a6678";
      ctx.font = "12px ui-monospace, monospace";
      ctx.fillText("no priced levels in view", plotL, (plotT + plotB) / 2);
      return; // domain (x-axis) is set above, so scrubbing still works
    }
    let yMin = Math.min(...lows);
    let yMax = Math.max(...highs);
    const padY = (yMax - yMin || 0.02) * 0.12;
    yMin -= padY;
    yMax += padY;
    const X = (ms) => plotL + ((ms - x0) / (x1 - x0)) * (plotR - plotL);
    const Y = (v) => plotB - ((v - yMin) / (yMax - yMin || 1)) * (plotB - plotT);

    // y gridlines + labels
    ctx.font = "10px ui-monospace, monospace";
    ctx.fillStyle = "#5a6678";
    ctx.strokeStyle = "#222b3a";
    ctx.lineWidth = 1;
    for (let i = 0; i <= 4; i++) {
      const v = yMin + ((yMax - yMin) * i) / 4;
      const y = Y(v);
      ctx.beginPath();
      ctx.moveTo(plotL, y);
      ctx.lineTo(plotR, y);
      ctx.stroke();
      ctx.fillText(v.toFixed(3), 6, y + 3);
    }

    // gap shading
    for (const g of state.gaps || []) {
      const gx0 = X(tsMs(g.from));
      const gx1 = X(tsMs(g.to));
      if (Number.isNaN(gx0) || Number.isNaN(gx1)) continue;
      ctx.fillStyle = "rgba(232,179,57,0.10)";
      ctx.fillRect(gx0, plotT, Math.max(2, gx1 - gx0), plotB - plotT);
    }

    // bid/ask band
    ctx.beginPath();
    series.forEach((p, i) => {
      const x = X(tsMs(p.ts));
      const y = Y(p.ask != null ? p.ask : p.mid);
      i ? ctx.lineTo(x, y) : ctx.moveTo(x, y);
    });
    for (let i = series.length - 1; i >= 0; i--) {
      const p = series[i];
      ctx.lineTo(X(tsMs(p.ts)), Y(p.bid != null ? p.bid : p.mid));
    }
    ctx.closePath();
    ctx.fillStyle = "rgba(76,141,255,0.10)";
    ctx.fill();

    // mid line
    ctx.beginPath();
    series.forEach((p, i) => {
      if (p.mid == null) return;
      const x = X(tsMs(p.ts));
      const y = Y(p.mid);
      i ? ctx.lineTo(x, y) : ctx.moveTo(x, y);
    });
    ctx.strokeStyle = "#4c8dff";
    ctx.lineWidth = 1.5;
    ctx.stroke();

    // playhead
    const headMs = state.scrubTs ? tsMs(state.scrubTs) : x1;
    const hx = X(clamp(headMs, x0, x1));
    ctx.strokeStyle = state.mode === "live" ? "#1fb574" : "#e8b339";
    ctx.lineWidth = 1.5;
    ctx.beginPath();
    ctx.moveTo(hx, plotT);
    ctx.lineTo(hx, plotB);
    ctx.stroke();
    ctx.fillStyle = ctx.strokeStyle;
    ctx.beginPath();
    ctx.arc(hx, plotT, 3, 0, Math.PI * 2);
    ctx.fill();
  }

  function seekAt(clientX, end) {
    if (!domain) return;
    const rect = canvas.getBoundingClientRect();
    const ms = clamp(xToMs(clientX - rect.left), domain.x0, domain.x1);
    const now = performance.now();
    if (!end && now - lastSeek < 60) return;
    lastSeek = now;
    const iso = msToIso(ms);
    if (end) onSeekEnd && onSeekEnd(iso);
    else onSeek && onSeek(iso);
  }

  function showTooltip(clientX) {
    if (!domain || !last || !(last.series || []).length) return;
    const rect = canvas.getBoundingClientRect();
    const ms = xToMs(clientX - rect.left);
    const series = last.series;
    let best = series[0];
    let bestD = Infinity;
    for (const p of series) {
      const d = Math.abs(tsMs(p.ts) - ms);
      if (d < bestD) {
        bestD = d;
        best = p;
      }
    }
    tooltipEl.hidden = false;
    tooltipEl.style.left = clientX - rect.left + "px";
    tooltipEl.style.top = "8px";
    tooltipEl.innerHTML = `${fmtClock(best.ts)} · mid ${fmtPrice(best.mid)} · ${fmtPrice(best.bid)}/${fmtPrice(best.ask)}`;
  }

  canvas.addEventListener("pointerdown", (e) => {
    dragging = true;
    canvas.setPointerCapture(e.pointerId);
    seekAt(e.clientX, false);
  });
  canvas.addEventListener("pointermove", (e) => {
    showTooltip(e.clientX);
    if (dragging) seekAt(e.clientX, false);
  });
  canvas.addEventListener("pointerup", (e) => {
    if (dragging) {
      dragging = false;
      seekAt(e.clientX, true);
    }
  });
  canvas.addEventListener("pointerleave", () => {
    tooltipEl.hidden = true;
  });

  if (typeof ResizeObserver !== "undefined") {
    new ResizeObserver(() => last && draw(last)).observe(canvas);
  }

  return { update: draw };
}
