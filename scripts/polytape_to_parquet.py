"""Turn raw polytape captures into lean, dtype-optimized Parquet streams per event.

Pipeline:
    data/zipped/*.tar.gz  --(extract_raw.py)-->  data/raw/event-<id>/{book.jsonl,meta.json}
    data/raw/event-<id>/  --(this script)-->     data/dataset/event-<id>/{quotes,trades}.parquet

The raw book.jsonl interleaves the whole CLOB channel (book snapshots, price_change level deltas,
last_trade_price prints). This exporter replays the L2 book in event time and writes split,
RAM-cheap streams keeping only the 3 "Yes" legs (the No tokens are an exact mirror -> dropped):

    quotes.parquet   ts_us(i64), seq(i32), leg(i8), best_bid/best_ask(i16 x1000, -1 = empty side),
                     best_bid_size/best_ask_size(f32)            -- dense BBO, the workhorse
    trades.parquet   ts_us, seq, leg(i8), side(i8: +1 BUY / -1 SELL), price(i16 x1000), size(f32)
    book_l2.parquet  ts_us, seq, leg, side(+1 bid/-1 ask), price(i16), size(f32), is_snapshot(i8)
                     -- full-depth level stream, OPT-IN via --with-l2 (heavy)
    meta.json        leg<->asset/market/title/tick map, window, full range, price_scale,
                     taker_fee_bps (venue cost for models), schema

Dtypes: prices are exact on the 0.001 grid -> int16 x1000 (lossless); sizes -> float32; ts as
int64 microseconds; leg/side as int8. ~150x smaller in RAM than a wide float64 table.

Incremental: every run writes into the SAME data/dataset/. A re-converted event's folder is
replaced cleanly, and _dataset_manifest.json is MERGED by event_id (prior events kept, this run's
events replace their old entries) -- so you can convert new batches into one dataset over time
instead of keeping a separate output dir per batch.

Windowing: by default only the last 3 hours before each match ends are kept, plus a 10-minute tail
past the match's last timestamp so the resolution tick is included (data ends at resolution).
Tune with --window-hours / --end-offset-min, or --window-hours 0 for the full capture.

Usage:
    uv run python scripts/polytape_to_parquet.py                 # all events -> data/dataset
    uv run python scripts/polytape_to_parquet.py 351748          # only that event
    uv run python scripts/polytape_to_parquet.py --with-l2       # also write full-depth book_l2
    uv run python scripts/polytape_to_parquet.py --window-hours 0   # full capture, no trimming

Requires: pyarrow  (pip install pyarrow / uv sync)
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ModuleNotFoundError:  # pragma: no cover - friendly hint
    sys.exit("This script needs pyarrow. Install it with:  pip install pyarrow")

try:  # optional speedup; stdlib json is the fallback
    import orjson

    def _loads(line: str):
        return orjson.loads(line)
except ModuleNotFoundError:  # pragma: no cover

    def _loads(line: str):
        return json.loads(line)


SCHEMA_VERSION = 3
PRICE_SCALE = 1000
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

QUOTES_SCHEMA = pa.schema(
    [
        ("ts_us", pa.int64()),
        ("seq", pa.int32()),
        ("leg", pa.int8()),
        ("best_bid", pa.int16()),  # x1000, -1 = no quote that side
        ("best_ask", pa.int16()),  # x1000, -1 = no quote that side
        ("best_bid_size", pa.float32()),
        ("best_ask_size", pa.float32()),
    ]
)
TRADES_SCHEMA = pa.schema(
    [
        ("ts_us", pa.int64()),
        ("seq", pa.int32()),
        ("leg", pa.int8()),
        ("side", pa.int8()),  # +1 BUY, -1 SELL (aggressor)
        ("price", pa.int16()),  # x1000
        ("size", pa.float32()),
    ]
)
L2_SCHEMA = pa.schema(
    [
        ("ts_us", pa.int64()),
        ("seq", pa.int32()),
        ("leg", pa.int8()),
        ("side", pa.int8()),  # +1 bid, -1 ask
        ("price", pa.int16()),  # x1000
        ("size", pa.float32()),  # new level size (0 = removed)
        ("is_snapshot", pa.int8()),  # 1 = part of a full book reset, 0 = delta
    ]
)


def _iso_us(s: str | None) -> int | None:
    """ISO-8601 (trailing Z) -> integer microseconds since epoch."""
    if not s:
        return None
    dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    delta = dt - _EPOCH
    return delta.days * 86_400_000_000 + delta.seconds * 1_000_000 + delta.microseconds


def _iso(us: int | None) -> str | None:
    if us is None:
        return None
    dt = datetime.fromtimestamp(us / 1_000_000, tz=timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


def _f(x) -> float | None:
    if x in (None, ""):
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _p16(x):
    return None if x is None else int(round(x * PRICE_SCALE))


def _best(bids: dict, asks: dict):
    bb = max(bids) if bids else None
    ba = min(asks) if asks else None
    bbs = bids.get(bb) if bb is not None else None
    bas = asks.get(ba) if ba is not None else None
    return bb, ba, bbs, bas


def _extract_ts_recv(line: str) -> str | None:
    """Fast pull of the envelope ts_recv without parsing the whole record."""
    i = line.find('"ts_recv"')
    if i < 0:
        return None
    j = line.find('"', i + 9)
    if j < 0:
        return None
    k = line.find('"', j + 1)
    return line[j + 1 : k] if k > 0 else None


def scan_range(path: Path) -> tuple[int | None, int | None]:
    """One cheap pass over a book.jsonl -> (min, max) ts_recv in microseconds (match-end anchor)."""
    lo = hi = None
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            ts = _extract_ts_recv(line)
            if ts is None:
                continue
            us = _iso_us(ts)
            if us is None:
                continue
            lo = us if lo is None or us < lo else lo
            hi = us if hi is None or us > hi else hi
    return lo, hi


def _asset_map(meta: dict) -> dict[str, dict]:
    """asset_id -> {group_title, outcome, market} from meta.event.markets (token[0]=Yes)."""
    out: dict[str, dict] = {}
    for mkt in (meta.get("event") or {}).get("markets") or []:
        title = mkt.get("groupItemTitle")
        cond = mkt.get("conditionId")
        for i, tok in enumerate(mkt.get("clobTokenIds") or []):
            out[str(tok)] = {
                "group_title": title,
                "outcome": "Yes" if i == 0 else "No",
                "market": cond,
            }
    return out


class _Table:
    """Buffers rows column-wise and flushes Parquet row groups with bounded memory."""

    def __init__(self, path: Path, schema: pa.Schema, compression: str, row_group: int):
        self.path = path
        self.schema = schema
        self.compression = compression
        self.row_group = row_group
        self.names = [f.name for f in schema]
        self.cols: dict[str, list] = {n: [] for n in self.names}
        self.writer: pq.ParquetWriter | None = None
        self.buffered = 0

    def add(self, row: dict) -> None:
        for n in self.names:
            self.cols[n].append(row.get(n))
        self.buffered += 1
        if self.buffered >= self.row_group:
            self._flush()

    def _flush(self) -> None:
        if self.buffered == 0:
            return
        arrays = [pa.array(self.cols[f.name], type=f.type) for f in self.schema]
        batch = pa.record_batch(arrays, schema=self.schema)
        if self.writer is None:
            self.writer = pq.ParquetWriter(self.path, self.schema, compression=self.compression)
        self.writer.write_batch(batch)
        for n in self.names:
            self.cols[n].clear()
        self.buffered = 0

    def close(self) -> None:
        self._flush()
        if self.writer is None:  # never got a row -> still write an empty, typed file
            self.writer = pq.ParquetWriter(self.path, self.schema, compression=self.compression)
            self.writer.write_table(self.schema.empty_table())
        self.writer.close()


def convert_event(
    event_id: str,
    meta: dict,
    book_path: Path,
    out_dir: Path,
    *,
    compression: str,
    row_group: int,
    keep_lo_us: int | None = None,
    keep_hi_us: int | None = None,
    full_first_us: int | None = None,
    full_last_us: int | None = None,
    window_hours: float | None = None,
    end_offset_min: float | None = None,
    with_l2: bool = False,
    taker_fee_bps: float = 75.0,
    limit: int | None = None,
    progress_every: int = 0,
) -> dict:
    """Export lean split streams (quotes/trades[/book_l2] + meta) by replaying the L2 book.

    The book is reconstructed in event time so the BBO stream carries best sizes densely
    (in the raw feed they live only on the ~4% book-snapshot rows). Only the 3 Yes legs are kept.
    """
    amap = _asset_map(meta)
    yes = sorted(a for a, info in amap.items() if info.get("outcome") == "Yes")
    leg_of = {a: i for i, a in enumerate(yes)}
    n = len(yes)
    part = out_dir / f"event-{event_id}"
    if part.exists():  # clean replace: drop stale streams (e.g. an old book_l2)
        shutil.rmtree(part)
    part.mkdir(parents=True, exist_ok=True)
    quotes = _Table(part / "quotes.parquet", QUOTES_SCHEMA, compression, row_group)
    trades = _Table(part / "trades.parquet", TRADES_SCHEMA, compression, row_group)
    l2 = _Table(part / "book_l2.parquet", L2_SCHEMA, compression, row_group) if with_l2 else None

    bids = [dict() for _ in range(n)]
    asks = [dict() for _ in range(n)]
    last_q = [None] * n
    tick = [None] * n
    counts = {"quotes": 0, "trades": 0, "book_l2": 0}
    opened = False
    seq = -1

    def emit_quote(g, ts, sq, in_win):
        bb, ba, bbs, bas = _best(bids[g], asks[g])
        bi = _p16(bb) if bb is not None else -1  # -1 = no quote that side (keeps int16 non-null)
        ai = _p16(ba) if ba is not None else -1
        key = (bi, ai, bbs, bas)
        if key == last_q[g] or (bi == -1 and ai == -1):
            return
        last_q[g] = key
        if in_win:
            quotes.add(
                {
                    "ts_us": ts,
                    "seq": sq,
                    "leg": g,
                    "best_bid": bi,
                    "best_ask": ai,
                    "best_bid_size": bbs,
                    "best_ask_size": bas,
                }
            )
            counts["quotes"] += 1

    with open(book_path, encoding="utf-8") as fh:
        for seq, line in enumerate(fh):
            if limit is not None and seq >= limit:
                seq -= 1
                break
            if not line.strip():
                continue
            try:
                rec = _loads(line)
            except Exception:
                continue
            raw = rec.get("raw") or {}
            et = raw.get("event_type")
            ts = _iso_us(rec.get("ts_recv"))
            in_win = keep_lo_us is None or (ts is not None and keep_lo_us <= ts <= keep_hi_us)
            if progress_every and seq and seq % progress_every == 0:
                print(f"  [{event_id}] {seq:,} lines...", file=sys.stderr, flush=True)
            if in_win and not opened:  # seed all legs' BBO at window open
                opened = True
                for g in range(n):
                    last_q[g] = None
                    emit_quote(g, ts, seq, True)

            if et == "book":
                g = leg_of.get(raw.get("asset_id"))
                if g is None:
                    continue
                asks[g] = {
                    float(x["price"]): float(x["size"])
                    for x in (raw.get("asks") or [])
                    if float(x.get("size") or 0) > 0
                }
                bids[g] = {
                    float(x["price"]): float(x["size"])
                    for x in (raw.get("bids") or [])
                    if float(x.get("size") or 0) > 0
                }
                tk = _f(raw.get("tick_size"))
                if tk is not None:
                    tick[g] = tk
                if l2 and in_win:
                    for sgn, bk in ((1, bids[g]), (-1, asks[g])):
                        for p, s in bk.items():
                            l2.add(
                                {
                                    "ts_us": ts,
                                    "seq": seq,
                                    "leg": g,
                                    "side": sgn,
                                    "price": _p16(p),
                                    "size": s,
                                    "is_snapshot": 1,
                                }
                            )
                            counts["book_l2"] += 1
                emit_quote(g, ts, seq, in_win)
            elif et == "price_change":
                for ch in raw.get("price_changes") or []:
                    g = leg_of.get(ch.get("asset_id"))
                    if g is None:
                        continue
                    p, s = _f(ch.get("price")), _f(ch.get("size"))
                    sgn = 1 if ch.get("side") == "BUY" else -1
                    bk = bids[g] if sgn == 1 else asks[g]
                    if p is not None:
                        if s is None or s <= 0:
                            bk.pop(p, None)
                        else:
                            bk[p] = s
                    if l2 and in_win and p is not None:
                        l2.add(
                            {
                                "ts_us": ts,
                                "seq": seq,
                                "leg": g,
                                "side": sgn,
                                "price": _p16(p),
                                "size": s or 0.0,
                                "is_snapshot": 0,
                            }
                        )
                        counts["book_l2"] += 1
                    emit_quote(g, ts, seq, in_win)
            elif et == "last_trade_price":
                g = leg_of.get(raw.get("asset_id"))
                if g is None or not in_win:
                    continue
                trades.add(
                    {
                        "ts_us": ts,
                        "seq": seq,
                        "leg": g,
                        "side": 1 if raw.get("side") == "BUY" else -1,
                        "price": _p16(_f(raw.get("price"))),
                        "size": _f(raw.get("size")) or 0.0,
                    }
                )
                counts["trades"] += 1

    quotes.close()
    trades.close()
    if l2:
        l2.close()

    legs_meta = [
        {
            "leg": leg_of[a],
            "asset_id": a,
            "market": amap[a].get("market"),
            "group_title": amap[a].get("group_title"),
            "outcome": "Yes",
            "tick_size": tick[leg_of[a]],
        }
        for a in yes
    ]
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "format": "lean",
        "event_id": event_id,
        "title": (meta.get("event") or {}).get("title"),
        "slug": (meta.get("event") or {}).get("slug"),
        "price_scale": PRICE_SCALE,
        "price_dtype": "int16",
        "price_na_sentinel": -1,
        "size_dtype": "float32",
        "ts_unit": "us",
        "taker_fee_bps": taker_fee_bps,
        "legs": legs_meta,
        "counts": counts,
        "streams": ["quotes", "trades"] + (["book_l2"] if with_l2 else []),
        "full_data_first_ts_recv": _iso(full_first_us),
        "full_data_last_ts_recv": _iso(full_last_us),
        "window": (
            None
            if keep_lo_us is None
            else {
                "hours": window_hours,
                "end_offset_min": end_offset_min,
                "match_end_ts_recv": _iso(full_last_us),
                "keep_from_ts_recv": _iso(keep_lo_us),
                "keep_to_ts_recv": _iso(keep_hi_us),
            }
        ),
        "lines_read": seq + 1,
    }
    (part / "meta.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


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
                "run's events only",
                file=sys.stderr,
            )
    for e in new_events:
        by_id[str(e["event_id"])] = e
    return sorted(by_id.values(), key=lambda e: _event_sort_key(e["event_id"]))


def _atomic_write_json(path: Path, obj) -> None:
    """Write JSON via a temp file + os.replace so an interrupted run can't truncate the manifest."""
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Convert raw polytape captures into lean per-event Parquet streams.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Output: data/dataset/event-<id>/{quotes,trades}.parquet + meta.json.",
    )
    ap.add_argument(
        "events",
        nargs="*",
        help="event ids to convert (default: all in --raw-dir; 'event-' prefix ok)",
    )
    ap.add_argument(
        "--raw-dir", default="data/raw", help="dir of event-<id>/ folders (default data/raw)"
    )
    ap.add_argument("--out-dir", default="data/dataset", help="output dir (default data/dataset)")
    ap.add_argument(
        "--window-hours",
        type=float,
        default=3.0,
        help="keep this many hours before each match's last timestamp; "
        "0 = full capture (default 3)",
    )
    ap.add_argument(
        "--end-offset-min",
        type=float,
        default=10.0,
        help="extend the window this many minutes past the match's last timestamp (default 10)",
    )
    ap.add_argument(
        "--with-l2",
        action="store_true",
        help="also write book_l2.parquet (full-depth level stream; heavy)",
    )
    ap.add_argument(
        "--taker-fee-bps",
        type=float,
        default=75.0,
        help="venue taker fee written to meta for cost models (default 75)",
    )
    ap.add_argument(
        "--compression",
        default="zstd",
        choices=["zstd", "snappy", "gzip", "none"],
        help="Parquet codec (default zstd)",
    )
    ap.add_argument(
        "--row-group-size",
        type=int,
        default=50_000,
        help="rows buffered per Parquet row group (default 50000)",
    )
    ap.add_argument("--limit", type=int, default=None, help="max lines per event (for quick tests)")
    ap.add_argument(
        "--progress-every",
        type=int,
        default=1_000_000,
        help="print a progress line every N input lines (0 to silence)",
    )
    args = ap.parse_args(argv)

    raw = Path(args.raw_dir)
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    compression = "none" if args.compression == "none" else args.compression
    window_hours = args.window_hours if args.window_hours and args.window_hours > 0 else None
    end_offset = args.end_offset_min or 0.0

    available = {
        d.name[len("event-") :]: d
        for d in sorted(raw.glob("event-*"))
        if (d / "book.jsonl").exists()
    }
    if not available:
        print(f"no event-*/book.jsonl found under {raw}", file=sys.stderr)
        return 1
    if args.events:
        want = [e[len("event-") :] if e.startswith("event-") else e for e in args.events]
        for e in want:
            if e not in available:
                print(f"  [skip] no data for event {e} under {raw}", file=sys.stderr)
        selected = [(e, available[e]) for e in want if e in available]
    else:
        selected = list(available.items())
    if not selected:
        print("nothing to convert", file=sys.stderr)
        return 1

    manifests = []
    for eid, ev_dir in selected:
        book = ev_dir / "book.jsonl"
        meta_path = ev_dir / "meta.json"
        meta = (
            json.loads(meta_path.read_text(encoding="utf-8"))
            if meta_path.exists()
            else {"event_id": eid}
        )

        keep_lo = keep_hi = full_first = full_last = None
        if window_hours:
            full_first, full_last = scan_range(book)  # pass 1: find the match-end anchor
            if full_last is not None:
                keep_lo = full_last - int(window_hours * 3600 * 1_000_000)  # 3h before match end
                keep_hi = full_last + int(end_offset * 60 * 1_000_000)  # +10m tail past it

        print(f"== event {eid} -> {out / ('event-' + eid)} ==", file=sys.stderr, flush=True)
        m = convert_event(
            eid,
            meta,
            book,
            out,
            compression=compression,
            row_group=args.row_group_size,
            keep_lo_us=keep_lo,
            keep_hi_us=keep_hi,
            full_first_us=full_first,
            full_last_us=full_last,
            window_hours=window_hours,
            end_offset_min=end_offset,
            with_l2=args.with_l2,
            taker_fee_bps=args.taker_fee_bps,
            limit=args.limit,
            progress_every=args.progress_every,
        )
        manifests.append(m)
        print(f"   {m['counts']}  legs={len(m['legs'])}", file=sys.stderr, flush=True)

    # Merge into the shared manifest rather than recreating it: events from prior runs are kept,
    # events re-converted in this run replace their old entry.
    manifest_path = out / "_dataset_manifest.json"
    merged = _merge_manifest_events(manifest_path, manifests)
    _atomic_write_json(
        manifest_path,
        {
            "schema_version": SCHEMA_VERSION,
            "format": "lean",
            "window": (
                {"hours": window_hours, "end_offset_min": end_offset} if window_hours else None
            ),
            "events": merged,
        },
    )
    print(
        f"\nConverted {len(manifests)} event(s) into {out} ({len(merged)} total in manifest)",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
