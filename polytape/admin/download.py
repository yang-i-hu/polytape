"""Build downloadable archives of a polytape multi-event run — the whole run or
selected matches — for the admin dashboard's (login-gated) download endpoint.

A "match" is **not** a directory. The multi-event recorder writes one combined
``book.jsonl`` / ``comments.jsonl`` / ``meta.json`` for the whole run, and every
record is attributed to an event: book records by ``raw.market`` (a condition id,
mapped to its event via ``meta.events[].markets[].conditionId``), comments by
``parentEntityID``. So a per-match export is a *filtered slice* of the combined
files, re-emitted under the familiar ``event-<id>/`` layout; the whole-run export
is the three combined files verbatim.

Everything here is read-only — it never opens the recorder's files for writing.
Filtering streams line-by-line into a scratch dir (flat memory even on a multi-GB
``book.jsonl``, and byte-exact: the original line bytes are re-emitted, only a
decoded copy is parsed for attribution). The archive is then streamed as a gzip
tar straight to the client through an OS pipe, so nothing is buffered whole.
"""

from __future__ import annotations

import json
import logging
import os
import tarfile
import threading
from collections.abc import Callable, Iterable, Iterator
from functools import partial
from pathlib import Path
from typing import Any, BinaryIO

logger = logging.getLogger("polytape.admin.download")

_STREAMS = ("book", "comments")
_READ_CHUNK = 8 * 1024 * 1024  # 8 MiB scan chunks; flat memory on a multi-GB file.


# --------------------------------------------------------------------------- #
# Run metadata + event attribution (pure)
# --------------------------------------------------------------------------- #


def load_run_meta(run_dir: Path) -> dict[str, Any]:
    """Read the run's ``meta.json`` (raises ``OSError`` / ``JSONDecodeError``)."""
    return json.loads((run_dir / "meta.json").read_text(encoding="utf-8"))


def known_event_ids(meta: dict[str, Any]) -> list[str]:
    """Event ids present in the run, in meta order."""
    return [str(e.get("id")) for e in (meta.get("events") or []) if e.get("id") is not None]


def _cond_to_event(meta: dict[str, Any]) -> dict[str, str]:
    """Map each market condition id -> its event id (book attribution)."""
    out: dict[str, str] = {}
    for event in meta.get("events") or []:
        eid = str(event.get("id"))
        for market in event.get("markets") or []:
            cond = market.get("conditionId")
            if cond:
                out[str(cond)] = eid
    return out


def _book_event(raw: dict[str, Any], cond2event: dict[str, str]) -> str | None:
    market = raw.get("market")
    return cond2event.get(str(market)) if market is not None else None


def _core(raw: dict[str, Any]) -> dict[str, Any]:
    payload = raw.get("payload")
    return payload if isinstance(payload, dict) else raw


class _CommentRouter:
    """Attribute comments by ``parentEntityID`` and reactions by ``commentID``.

    Mirrors the recorder's RTDS routing (``CommentStream.resolve_event_id`` /
    ``on_written`` in ``polytape/streams/rtds.py``): a reaction carries no
    ``parentEntityID`` and is routed via a ``commentID -> event`` map seeded from
    the comments seen *earlier in the file*. The recorder only keeps a reaction
    whose parent comment it had already seen, so the parent always precedes the
    reaction in append order — a single forward pass attributes every recorded
    reaction. (A reaction to an unseen comment is dropped, exactly as the recorder
    drops it.) Without this, per-match slices would silently lose every reaction
    while still shipping a ``counts.comments`` that tallied them.
    """

    def __init__(self) -> None:
        self._comment_event: dict[str, str] = {}

    def __call__(self, raw: dict[str, Any]) -> str | None:
        core = _core(raw)
        parent = core.get("parentEntityID")
        if parent is not None:
            cid = core.get("id")
            if cid is not None:
                self._comment_event[str(cid)] = str(parent)  # seed reaction attribution
            return str(parent)
        comment_id = core.get("commentID")
        if comment_id is not None:
            return self._comment_event.get(str(comment_id))
        return None


def per_event_meta(meta: dict[str, Any], event_id: str, *, exported_at: str) -> dict[str, Any]:
    """A standalone ``meta.json`` for one event, sliced out of the run meta."""
    event = next((e for e in (meta.get("events") or []) if str(e.get("id")) == event_id), None)
    markets = (event or {}).get("markets") or []
    conds = [m.get("conditionId") for m in markets if m.get("conditionId")]
    tokens = [t for m in markets for t in (m.get("clobTokenIds") or [])]
    # Whitelist the hashing fields rather than copying the dict wholesale: only the
    # non-reversible fingerprint should ever leave the box, never a raw salt if one
    # were ever (mis)placed there.
    hashing = meta.get("hashing") or {}
    return {
        "polytape_version": meta.get("polytape_version"),
        "event_id": event_id,
        "run_name": meta.get("run_name"),
        "streams": meta.get("streams"),
        "holdings_captured": meta.get("holdings_captured"),
        "hashing": {
            "enabled": hashing.get("enabled"),
            "salt_fingerprint": hashing.get("salt_fingerprint"),
        },
        "started_at": meta.get("started_at"),
        "stopped_at": meta.get("stopped_at"),
        "counts": (meta.get("counts_by_event") or {}).get(event_id, {}),
        "market_ids": conds,
        "clob_token_ids": tokens,
        "event": event,
        "gaps": meta.get("gaps") or [],
        "source": {
            "kind": "filtered-slice",
            "run_name": meta.get("run_name"),
            "exported_at": exported_at,
            "note": (
                "records for this event were filtered from a combined multi-event run; "
                "gaps are run-wide, and a still-recording match's final line may be partial "
                "(consistent up to the last complete line)."
            ),
        },
    }


# --------------------------------------------------------------------------- #
# Filtering a run into per-event scratch files
# --------------------------------------------------------------------------- #


def _scan_and_route(
    src: Path,
    stream: str,
    wanted: set[str],
    attribute: Callable[[dict[str, Any]], str | None],
    writer_for: Callable[[str, str], BinaryIO],
    chunk_bytes: int,
) -> None:
    """Stream ``src`` and append each wanted line (byte-exact) to its event writer."""
    buf = b""
    with open(src, "rb") as fh:
        while True:
            chunk = fh.read(chunk_bytes)
            if not chunk:
                break
            buf += chunk
            nl = buf.rfind(b"\n")
            if nl == -1:
                continue  # no complete line yet (records are far smaller than the chunk)
            block, buf = buf[: nl + 1], buf[nl + 1 :]
            for raw_line in block.split(b"\n"):
                if not raw_line:
                    continue
                try:
                    rec = json.loads(raw_line)
                except (json.JSONDecodeError, UnicodeDecodeError):
                    continue
                if not isinstance(rec, dict):
                    continue
                eid = attribute(rec.get("raw") or {})
                if eid is not None and eid in wanted:
                    writer_for(eid, stream).write(raw_line + b"\n")
    # Any trailing partial line (a live recorder mid-append) is intentionally dropped.


def filter_run(
    run_dir: Path,
    event_ids: Iterable[str],
    dest_dir: Path,
    *,
    meta: dict[str, Any] | None = None,
    exported_at: str,
    chunk_bytes: int = _READ_CHUNK,
) -> list[tuple[str, Path]]:
    """Filter the combined run files into ``dest_dir/event-<id>/`` and list archive entries.

    Scans each combined file once, routing every attributed line to its event's
    writer — so selecting several matches stays a single pass per stream. Always
    writes a per-event ``meta.json`` slice (even for a match with no records yet),
    so the archive documents exactly what was selected. Returns the archive
    entries ``[(arcname, path), ...]``.
    """
    meta = meta if meta is not None else load_run_meta(run_dir)
    cond2event = _cond_to_event(meta)
    wanted = {str(e) for e in event_ids}
    writers: dict[tuple[str, str], BinaryIO] = {}

    def writer_for(eid: str, stream: str) -> BinaryIO:
        key = (eid, stream)
        handle = writers.get(key)
        if handle is None:
            (dest_dir / f"event-{eid}").mkdir(parents=True, exist_ok=True)
            handle = open(dest_dir / f"event-{eid}" / f"{stream}.jsonl", "wb")
            writers[key] = handle
        return handle

    try:
        for stream in _STREAMS:
            src = run_dir / f"{stream}.jsonl"
            if not src.exists():
                continue
            attribute: Callable[[dict[str, Any]], str | None]
            if stream == "book":
                attribute = partial(_book_event, cond2event=cond2event)
            else:
                # Stateful: seeds a commentID->event map as it scans so reactions
                # (no parentEntityID) are attributed, mirroring the recorder.
                attribute = _CommentRouter()
            _scan_and_route(src, stream, wanted, attribute, writer_for, chunk_bytes)
    finally:
        for handle in writers.values():
            handle.close()

    entries: list[tuple[str, Path]] = []
    for eid in sorted(wanted):
        ev_dir = dest_dir / f"event-{eid}"
        ev_dir.mkdir(parents=True, exist_ok=True)
        meta_path = ev_dir / "meta.json"
        meta_path.write_text(
            json.dumps(per_event_meta(meta, eid, exported_at=exported_at), indent=2) + "\n",
            encoding="utf-8",
        )
        for name in ("meta.json", "book.jsonl", "comments.jsonl"):
            path = ev_dir / name
            if path.exists():
                entries.append((f"event-{eid}/{name}", path))
    return entries


def whole_run_entries(run_dir: Path, meta: dict[str, Any] | None = None) -> list[tuple[str, Path]]:
    """Archive entries for the whole run verbatim — the three combined files."""
    root = run_dir.name or "run"
    entries: list[tuple[str, Path]] = []
    for name in ("meta.json", "book.jsonl", "comments.jsonl"):
        path = run_dir / name
        if path.exists():
            entries.append((f"{root}/{name}", path))
    return entries


# --------------------------------------------------------------------------- #
# Streaming the archive (gzip tar, flat memory via an OS pipe)
# --------------------------------------------------------------------------- #


def stream_targz(
    entries: list[tuple[str, Path]], *, on_done: Callable[[], None] | None = None
) -> Iterator[bytes]:
    """Yield a gzip-tar of ``entries`` as it is written (constant memory).

    A worker thread writes the tar into one end of an OS pipe while this generator
    reads chunks from the other — so even a multi-GB member streams without being
    buffered. ``on_done`` (e.g. scratch-dir cleanup) runs once the stream is fully
    consumed or the client disconnects.
    """
    read_fd, write_fd = os.pipe()
    errors: list[BaseException] = []

    def _write() -> None:
        try:
            with os.fdopen(write_fd, "wb") as wf, tarfile.open(mode="w|gz", fileobj=wf) as tar:
                for arcname, path in entries:
                    tar.add(path, arcname=arcname, recursive=False)
        except BaseException as exc:  # noqa: BLE001 - surface via log; reader sees EOF
            errors.append(exc)

    worker = threading.Thread(target=_write, daemon=True)
    worker.start()
    reader = os.fdopen(read_fd, "rb")
    try:
        while True:
            data = reader.read(65536)
            if not data:
                break
            yield data
    finally:
        reader.close()
        worker.join()
        if errors:
            logger.warning("download archive writer error: %r", errors[0])
        if on_done is not None:
            try:
                on_done()
            except Exception:  # noqa: BLE001
                logger.exception("download cleanup failed")
