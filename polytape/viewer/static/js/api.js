// Data layer: thin fetch wrappers. The ONLY module (besides live.js) that knows
// endpoint paths. No DOM access.

const BASE = "/api/v1/events";

async function getJSON(url) {
  const res = await fetch(url, { headers: { Accept: "application/json" } });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText} for ${url}`);
  return res.json();
}

const qs = (params) =>
  Object.entries(params)
    .filter(([, v]) => v != null && v !== "")
    .map(([k, v]) => `${k}=${encodeURIComponent(v)}`)
    .join("&");

export const api = {
  events: () => getJSON(BASE),
  meta: (id) => getJSON(`${BASE}/${id}/meta`),
  book: (id, asset, { at, depth } = {}) => getJSON(`${BASE}/${id}/book?${qs({ asset, at, depth })}`),
  series: (id, asset, { from, to, max } = {}) =>
    getJSON(`${BASE}/${id}/series?${qs({ asset, from, to, max })}`),
  trades: (id, asset, { before, limit } = {}) =>
    getJSON(`${BASE}/${id}/trades?${qs({ asset, before, limit })}`),
};
