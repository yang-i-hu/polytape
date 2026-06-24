"""Build a dense, fixed-shape ML tensor from the lean per-event streams.

Reads data/dataset/event-<id>/{quotes,trades}.parquet + meta.json and produces a regular-grid
tensor X of shape [T, n_legs, F] (float32) per event -- the modeling-friendly companion to the
event log. Saved as data/tensor/event-<id>.npz (X, t_ms, optional label Y) plus a .meta.json
describing the axes. Memory-mappable, no nulls, no nested columns: load and train directly.

Features per (time, leg):
    mid, spread, best_bid_size, best_ask_size, microprice, queue_imbalance,
    signed_flow (sum trade size*side in bin), trade_count, staleness_ms

Incremental: every run writes into the SAME data/tensor/. Each event's .npz/.meta.json is
overwritten by id, and _tensor_manifest.json is MERGED by event_id (prior events kept, this run's
events replace their old entries) -- build new batches into one tensor dir over time. Each manifest
entry carries its own grid_ms, so a dir holding tensors built at different grids stays unambiguous.

Usage:
    uv run python scripts/build_tensor.py                       # all events, 1s grid
    uv run python scripts/build_tensor.py --grid-ms 100 --label-horizon 10
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import numpy as np
import pandas as pd

FEATURES = [
    "mid",
    "spread",
    "best_bid_size",
    "best_ask_size",
    "microprice",
    "queue_imbalance",
    "signed_flow",
    "trade_count",
    "staleness_ms",
]


def build_event(ev_dir: Path, out_dir: Path, grid_ms: int, label_h: int) -> dict:
    meta = json.loads((ev_dir / "meta.json").read_text(encoding="utf-8"))
    eid = meta["event_id"]
    scale = meta.get("price_scale", 1000)
    legs = sorted(meta["legs"], key=lambda x: x["leg"])
    n = len(legs)

    q = pd.read_parquet(ev_dir / "quotes.parquet")
    t = pd.read_parquet(ev_dir / "trades.parquet")
    q["dt"] = pd.to_datetime(q["ts_us"], unit="us", utc=True)
    if len(t):
        t["dt"] = pd.to_datetime(t["ts_us"], unit="us", utc=True)
    # decode int16 (-1 sentinel) -> probability float
    for c in ("best_bid", "best_ask"):
        q[c] = np.where(q[c].values < 0, np.nan, q[c].values / scale)

    freq = f"{grid_ms}ms"
    start = q["dt"].min().floor(freq)
    end = q["dt"].max().ceil(freq)
    grid = pd.date_range(start, end, freq=freq)
    X = np.full((len(grid), n, len(FEATURES)), np.nan, dtype=np.float32)

    for lg in range(n):
        ql = q[q.leg == lg].set_index("dt").sort_index()
        if ql.empty:
            continue
        r = ql.resample(freq).last()
        bb = r["best_bid"].ffill().reindex(grid, method="ffill")
        ba = r["best_ask"].ffill().reindex(grid, method="ffill")
        bs = r["best_bid_size"].ffill().reindex(grid, method="ffill")
        as_ = r["best_ask_size"].ffill().reindex(grid, method="ffill")
        mid = (bb + ba) / 2
        denom = (bs + as_).replace(0, np.nan)
        micro = (bb * as_ + ba * bs) / denom  # size-weighted microprice
        micro = micro.where(denom.notna(), mid)
        qimb = (bs - as_) / denom
        # staleness: ms since the leg's last real quote at each grid point
        last_ts = ql.index.to_series().resample(freq).last().reindex(grid, method="ffill")
        stale = (grid.to_series().values - last_ts.values) / np.timedelta64(1, "ms")

        # trade flow per bin for this leg
        if len(t):
            tl = t[t.leg == lg].set_index("dt")
            flow = (tl["side"] * tl["size"]).resample(freq).sum().reindex(grid).fillna(0.0)
            cnt = tl["size"].resample(freq).count().reindex(grid).fillna(0.0)
        else:
            flow = pd.Series(0.0, index=grid)
            cnt = pd.Series(0.0, index=grid)

        cols = [
            mid.values,
            (ba - bb).values,
            bs.values,
            as_.values,
            micro.values,
            qimb.values,
            flow.values,
            cnt.values,
            stale.astype(np.float32),
        ]
        X[:, lg, :] = np.array(cols, dtype=np.float32).T

    t_ms = ((grid - grid[0]) / np.timedelta64(1, "ms")).astype(np.int32)
    out_dir.mkdir(parents=True, exist_ok=True)
    payload = {"X": X, "t_ms": t_ms}
    Y = None
    if label_h > 0:
        mid_idx = FEATURES.index("mid")
        mids = X[:, :, mid_idx]
        Y = np.full_like(mids, np.nan)
        Y[:-label_h] = mids[label_h:] - mids[:-label_h]  # forward mid change, h bins ahead
        payload["Y"] = Y.astype(np.float32)
    np.savez(out_dir / f"event-{eid}.npz", **payload)
    sidecar = {
        "event_id": eid,
        "title": meta.get("title"),
        "shape": list(X.shape),
        "dims": ["time", "leg", "feature"],
        "features": FEATURES,
        "grid_ms": grid_ms,
        "label": (f"forward mid change, {label_h} bins ahead" if label_h > 0 else None),
        "legs": [
            {"leg": leg_["leg"], "group_title": leg_["group_title"], "tick_size": leg_["tick_size"]}
            for leg_ in legs
        ],
        "start_ts_us": int(q["ts_us"].min()),
        "taker_fee_bps": meta.get("taker_fee_bps"),
    }
    (out_dir / f"event-{eid}.meta.json").write_text(json.dumps(sidecar, indent=2), encoding="utf-8")
    nbytes = X.nbytes + (Y.nbytes if Y is not None else 0)
    return {
        "event_id": eid,
        "shape": list(X.shape),
        "mb": round(nbytes / 1e6, 2),
        "grid_ms": grid_ms,
    }


def _event_sort_key(event_id) -> tuple[int, object]:
    """Numeric event ids sort numerically; anything else sorts after, lexically."""
    s = str(event_id)
    return (0, int(s)) if s.isdigit() else (1, s)


def _merge_manifest_events(path: Path, new_events: list[dict]) -> list[dict]:
    """Union prior manifest events with this run's, keyed by event_id (new replaces old)."""
    by_id: dict[str, dict] = {}
    if path.exists():
        try:
            prev = json.loads(path.read_text(encoding="utf-8"))
            for e in prev.get("events", []):
                if e.get("event_id") is not None:
                    by_id[str(e["event_id"])] = e
        except (json.JSONDecodeError, OSError) as exc:
            print(
                f"  [warn] ignoring unreadable {path.name} ({exc}); rebuilding it from this "
                "run's events only"
            )
    for e in new_events:
        by_id[str(e["event_id"])] = e
    return sorted(by_id.values(), key=lambda e: _event_sort_key(e["event_id"]))


def _atomic_write_json(path: Path, obj) -> None:
    """Write JSON via a temp file + os.replace so an interrupted run can't truncate the manifest."""
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Build dense [time,leg,feature] ML tensors.")
    ap.add_argument("events", nargs="*", help="event ids (default: all under --lean-dir)")
    ap.add_argument("--lean-dir", default="data/dataset")
    ap.add_argument("--out-dir", default="data/tensor")
    ap.add_argument("--grid-ms", type=int, default=1000, help="grid step in ms (default 1000)")
    ap.add_argument(
        "--label-horizon",
        type=int,
        default=0,
        help="if >0, also store forward mid change this many bins ahead (default 0)",
    )
    args = ap.parse_args(argv)

    lean = Path(args.lean_dir)
    dirs = (
        [lean / f"event-{e.replace('event-', '')}" for e in args.events]
        if args.events
        else sorted(p for p in lean.glob("event-*") if (p / "quotes.parquet").exists())
    )
    dirs = [d for d in dirs if (d / "quotes.parquet").exists()]
    if not dirs:
        print(f"no lean event dirs with quotes.parquet under {lean}")
        return 1
    out = Path(args.out_dir)
    results = []
    for d in dirs:
        r = build_event(d, out, args.grid_ms, args.label_horizon)
        results.append(r)
        print(f"  {r['event_id']}: X{r['shape']}  {r['mb']} MB", flush=True)
    # Merge into the shared manifest rather than recreating it: events from prior runs are kept,
    # events rebuilt in this run replace their old entry.
    manifest_path = out / "_tensor_manifest.json"
    merged = _merge_manifest_events(manifest_path, results)
    _atomic_write_json(
        manifest_path, {"grid_ms": args.grid_ms, "features": FEATURES, "events": merged}
    )
    print(f"\nBuilt {len(results)} tensor(s) into {out} ({len(merged)} total in manifest)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
