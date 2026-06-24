"""Backfill missed comments for a parent Series (or Event) into a sidecar JSONL.

Recovers comments that the live recorder failed to capture — e.g. the World Cup
case where every match comment is parented to the Series (``11433``), not the
event, so an event-keyed firehose filter dropped 100% of them (see
``polytape/streams/rtds.py``). Pages Gamma's public ``/comments`` endpoint for
each parent and writes the same capture envelope the recorder produces, so the
sidecar merges cleanly with the live ``comments.jsonl`` (dedup by envelope id).

Envelopes are byte-compatible with live capture: identifier fields are salted
with the SAME ``POLYTAPE_SALT`` the recorder uses (set it in the environment), so
a given author hashes to the same value here and in the live stream. Holdings are
fetched with ``get_positions=true``; note they reflect *fetch* time, not the
comment's post time — exactly like the recorder's own reconnect backfill.

Usage (run with the recorder's venv so the salt and polytape are available)::

    POLYTAPE_SALT=... python scripts/backfill_series_comments.py \
        --series-id 11433 --out /data/run-wc/comments-backfill.jsonl \
        --since 2026-06-19T16:20:21Z

Writes to a *sidecar* file, never the live ``comments.jsonl`` or ``meta.json``,
so it is safe to run while the recorder is live and is fully reversible (delete
the sidecar). The final comment record is ``comments.jsonl`` ∪ the sidecar,
de-duplicated by envelope id.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

import httpx

from polytape.config import STREAM_COMMENTS
from polytape.envelope import Hasher, build_envelope, iso_to_datetime, utc_now_iso

GAMMA_BASE_URL = "https://gamma-api.polymarket.com"
_USER_AGENT = "polytape-backfill (read-only comment recovery)"


def _parent_args(args: argparse.Namespace) -> list[tuple[str, str]]:
    """(id, parent_entity_type) pairs to page, from --series-id / --event-id."""
    targets: list[tuple[str, str]] = []
    targets += [(str(s), "Series") for s in (args.series_id or ())]
    targets += [(str(e), "Event") for e in (args.event_id or ())]
    return targets


def fetch_page(
    client: httpx.Client,
    parent_id: str,
    parent_type: str,
    *,
    offset: int,
    limit: int,
    get_positions: bool,
) -> list[dict[str, Any]]:
    """One page of comments for a parent, newest-first.

    Paging newest-first lets a bounded ``--since`` window stop early. Concurrent
    head growth (new comments posted mid-walk) only re-shows rows already paged
    (de-duped by id), never skips one, because nothing is deleted from the head.
    """
    params = {
        "parent_entity_type": parent_type,
        "parent_entity_id": parent_id,
        "order": "createdAt",
        "ascending": "false",
        "limit": str(limit),
        "offset": str(offset),
    }
    if get_positions:
        params["get_positions"] = "true"
    resp = client.get("/comments", params=params)
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, list):
        raise SystemExit(f"unexpected /comments response type: {type(data).__name__}")
    return data


def backfill_parent(
    client: httpx.Client,
    parent_id: str,
    parent_type: str,
    *,
    since: Any,
    out: Any,
    seen: set[str],
    hasher: Hasher | None,
    limit: int,
    max_pages: int,
    page_delay: float,
    get_positions: bool,
) -> int:
    """Page one parent newest-first back to ``since``; write new envelopes. Returns count.

    Advances the offset by the actual batch length (the API does not strictly honor
    ``limit``) and stops once a comment older than ``since`` is seen — newest-first,
    so everything beyond it is older too. ``seen`` de-dupes overlap from head growth.
    """
    written = 0
    offset = 0
    reached_window_start = False
    for _ in range(max_pages):
        batch = fetch_page(
            client,
            parent_id,
            parent_type,
            offset=offset,
            limit=limit,
            get_positions=get_positions,
        )
        if not batch:
            break  # reached the oldest comment
        for comment in batch:
            created = iso_to_datetime(str(comment.get("createdAt", "")))
            if since is not None and created is not None and created < since:
                reached_window_start = True
                continue  # predates the recording window (and so does everything after)
            env = build_envelope(STREAM_COMMENTS, comment, hasher=hasher, ts_recv=utc_now_iso())
            mid = env["id"]
            if mid in seen:
                continue
            seen.add(mid)
            out.write(json.dumps(env, ensure_ascii=False) + "\n")
            written += 1
        if reached_window_start:
            break
        offset += len(batch)
        time.sleep(page_delay)
    print(f"  {parent_type} {parent_id}: wrote {written} comment(s)")
    return written


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="backfill_series_comments",
        description="Recover missed comments for a parent Series/Event into a sidecar JSONL.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--series-id", action="append", metavar="ID", help="Parent Series id (repeatable)."
    )
    parser.add_argument(
        "--event-id", action="append", metavar="ID", help="Parent Event id (repeatable)."
    )
    parser.add_argument("--out", required=True, metavar="PATH", help="Sidecar JSONL to append to.")
    parser.add_argument(
        "--since",
        metavar="ISO",
        help="Only keep comments with createdAt >= this ISO time (the recording window start).",
    )
    parser.add_argument(
        "--no-hash",
        action="store_true",
        help="Write identifiers verbatim. Default hashes with POLYTAPE_SALT (match live capture).",
    )
    parser.add_argument("--page-size", type=int, default=100, metavar="N")
    parser.add_argument("--max-pages", type=int, default=2000, metavar="N")
    parser.add_argument("--page-delay", type=float, default=0.25, metavar="SECONDS")
    parser.add_argument(
        "--no-positions",
        action="store_true",
        help="Skip author holdings (get_positions). Default fetches them, as the recorder does.",
    )
    args = parser.parse_args(argv)

    targets = _parent_args(args)
    if not targets:
        parser.error("at least one --series-id or --event-id is required")

    since = iso_to_datetime(args.since) if args.since else None
    if args.since and since is None:
        parser.error(f"could not parse --since {args.since!r}")

    hasher: Hasher | None = None
    if not args.no_hash:
        if not os.environ.get("POLYTAPE_SALT"):
            print(
                "WARNING: POLYTAPE_SALT is not set; a random salt will be used and hashed "
                "identifiers will NOT match the live capture. Set POLYTAPE_SALT to the "
                "recorder's salt, or pass --no-hash deliberately.",
                file=sys.stderr,
            )
        hasher = Hasher()
        print(f"hashing identifiers (salt fingerprint {hasher.fingerprint})")

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    # Resume-safe: skip envelope ids already present in the sidecar.
    seen: set[str] = set()
    if out_path.exists():
        with out_path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    seen.add(json.loads(line)["id"])
                except (json.JSONDecodeError, KeyError):
                    continue
        print(f"resuming: {len(seen)} envelope id(s) already in {out_path}")

    total = 0
    with (
        httpx.Client(
            base_url=GAMMA_BASE_URL,
            timeout=30.0,
            headers={"User-Agent": _USER_AGENT, "Accept": "application/json"},
        ) as client,
        out_path.open("a", encoding="utf-8", newline="\n") as out,
    ):
        for parent_id, parent_type in targets:
            total += backfill_parent(
                client,
                parent_id,
                parent_type,
                since=since,
                out=out,
                seen=seen,
                hasher=hasher,
                limit=args.page_size,
                max_pages=args.max_pages,
                page_delay=args.page_delay,
                get_positions=not args.no_positions,
            )
    print(f"done: wrote {total} new comment(s) to {out_path} ({len(seen)} total in file)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
