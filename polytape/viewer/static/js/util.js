// Small formatting + math helpers shared by the render modules. No DOM, no fetch.

export const fmtPrice = (v) => (v == null ? "—" : Number(v).toFixed(3));
export const fmtSize = (v) =>
  v == null ? "—" : v >= 1000 ? (v / 1000).toFixed(1) + "k" : Number(v).toFixed(1);
export const fmtBps = (v) => (v == null ? "—" : Math.round(v).toLocaleString());
export const fmtPct = (v) => (v == null ? "—" : (v * 100).toFixed(1) + "%");
export const fmtSigned = (v) => (v == null ? "—" : (v > 0 ? "+" : "") + (v * 100).toFixed(1) + "%");

export const tsMs = (iso) => (iso ? Date.parse(iso) : NaN);
export const clamp = (v, a, b) => Math.max(a, Math.min(b, v));

export function fmtClock(iso) {
  if (!iso) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "—";
  const p = (n) => String(n).padStart(2, "0");
  return `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
}

export function msToIso(ms) {
  return new Date(ms).toISOString();
}

export function el(tag, attrs = {}, children = []) {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (k === "class") node.className = v;
    else if (k === "text") node.textContent = v;
    else if (v != null) node.setAttribute(k, v);
  }
  for (const c of [].concat(children)) {
    if (c != null) node.append(c);
  }
  return node;
}
