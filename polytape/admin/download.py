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
import re
import tarfile
import threading
from collections.abc import Callable, Iterable, Iterator
from functools import partial
from pathlib import Path
from typing import Any, BinaryIO

from polytape.admin import registry as _reg

logger = logging.getLogger("polytape.admin.download")

_STREAMS = ("book", "comments")
_READ_CHUNK = 8 * 1024 * 1024  # 8 MiB scan chunks; flat memory on a multi-GB file.


# --------------------------------------------------------------------------- #
# Human-friendly archive filename: event id + FIFA team codes
# --------------------------------------------------------------------------- #

# FIFA 3-letter codes for the 2026 World Cup teams. Keys match the Gamma event-title
# sides / market groupItemTitle verbatim (incl. the tricky ones: "Korea Republic",
# "IR Iran", "Türkiye", "South Africa", "Saudi Arabia", "Cabo Verde", "DR Congo",
# "Côte d'Ivoire", "Curaçao"). An unmapped team falls back to a derived A-Z token.
_FIFA_CODES = {
    "Algeria": "ALG",
    "Argentina": "ARG",
    "Australia": "AUS",
    "Austria": "AUT",
    "Belgium": "BEL",
    "Bosnia-Herzegovina": "BIH",
    "Brazil": "BRA",
    "Cabo Verde": "CPV",
    "Canada": "CAN",
    "Colombia": "COL",
    "Croatia": "CRO",
    "Curaçao": "CUW",
    "Czechia": "CZE",
    "Côte d'Ivoire": "CIV",
    "DR Congo": "COD",
    "Ecuador": "ECU",
    "Egypt": "EGY",
    "England": "ENG",
    "France": "FRA",
    "Germany": "GER",
    "Ghana": "GHA",
    "Haiti": "HAI",
    "IR Iran": "IRN",
    "Iraq": "IRQ",
    "Japan": "JPN",
    "Jordan": "JOR",
    "Korea Republic": "KOR",
    "Mexico": "MEX",
    "Morocco": "MAR",
    "Netherlands": "NED",
    "New Zealand": "NZL",
    "Norway": "NOR",
    "Panama": "PAN",
    "Paraguay": "PAR",
    "Portugal": "POR",
    "Qatar": "QAT",
    "Saudi Arabia": "KSA",
    "Scotland": "SCO",
    "Senegal": "SEN",
    "South Africa": "RSA",
    "Spain": "ESP",
    "Sweden": "SWE",
    "Switzerland": "SUI",
    "Tunisia": "TUN",
    "Türkiye": "TUR",
    "United States": "USA",
    "Uruguay": "URU",
    "Uzbekistan": "UZB",
}

# Title sides: "United States vs. Australia" / "Spain vs Saudi Arabia" -> the two teams.
_VS_SPLIT = re.compile(r"\s+vs\.?\s+", re.IGNORECASE)


def team_code(name: str) -> str:
    """Short uppercase code for a team — its FIFA code if known, else a derived A-Z
    token (so an unmapped / future team still yields a safe filename component)."""
    code = _FIFA_CODES.get(name.strip())
    if code:
        return code
    derived = "".join(ch for ch in name.upper() if "A" <= ch <= "Z")[:3]
    return derived or "UNK"


def _title_sides(entry: dict[str, Any] | None) -> tuple[str, str] | None:
    """The two team names of a match from its title ('A vs. B'); None if not parseable."""
    if not entry:
        return None
    parts = _VS_SPLIT.split(str(entry.get("title") or "").strip(), maxsplit=1)
    if len(parts) == 2 and parts[0].strip() and parts[1].strip():
        return parts[0].strip(), parts[1].strip()
    return None


def match_archive_name(event_id: str, entry: dict[str, Any] | None) -> str:
    """Download filename for ONE match: ``event-<id>-<HOME>-<AWAY>.tar.gz`` (e.g.
    ``event-351743-USA-AUS.tar.gz``). Falls back to ``event-<id>.tar.gz`` when the
    title can't be split into two sides. Tokens are A-Z FIFA codes, so always safe."""
    sides = _title_sides(entry)
    if sides:
        return f"event-{event_id}-{team_code(sides[0])}-{team_code(sides[1])}.tar.gz"
    return f"event-{event_id}.tar.gz"


# --------------------------------------------------------------------------- #
# Run metadata + event attribution (pure)
# --------------------------------------------------------------------------- #


def load_run_meta(run_dir: Path) -> dict[str, Any]:
    """Read the run's ``meta.json`` (raises ``OSError`` / ``JSONDecodeError``)."""
    return json.loads((run_dir / "meta.json").read_text(encoding="utf-8"))


def known_event_ids(meta: dict[str, Any]) -> list[str]:
    """Event ids present in the run, in meta order."""
    return [str(e.get("id")) for e in (meta.get("events") or []) if e.get("id") is not None]


def registry_known_ids(registry: dict[str, dict[str, Any]]) -> list[str]:
    """Event ids of the merged run registry (all matches, incl. finished)."""
    return _reg.known_ids(registry)


def registry_cond_to_event(registry: dict[str, dict[str, Any]]) -> dict[str, str]:
    """conditionId -> event_id over the merged registry (first claim, by schedule)."""
    return _reg.cond_to_event(registry)


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


def per_event_meta(
    meta: dict[str, Any],
    event_id: str,
    *,
    exported_at: str,
    registry: dict[str, dict[str, Any]] | None = None,
    written_counts: dict[str, int] | None = None,
) -> dict[str, Any]:
    """A standalone ``meta.json`` for one event, sliced out of the run meta.

    Identity (event / markets / conditionIds) comes from the run registry when given
    — so it works for a FINISHED match whose entry already left ``meta.events`` — and
    falls back to ``meta.events``. Counts prefer the recorder's dedup-aware
    ``counts_by_event`` (authoritative for a live match) and fall back to the actual
    filtered-line tally (``written_counts``) for a finished match absent from it.
    """
    event = next((e for e in (meta.get("events") or []) if str(e.get("id")) == event_id), None)
    reg_entry = (registry or {}).get(event_id)
    if event is not None:
        identity: dict[str, Any] | None = event
        markets = event.get("markets") or []
    elif reg_entry is not None:
        markets = reg_entry.get("markets") or []
        identity = {
            "id": event_id,
            "title": reg_entry.get("title"),
            "slug": reg_entry.get("slug"),
            "markets": markets,
        }
    else:
        identity, markets = None, []
    conds = [m.get("conditionId") for m in markets if m.get("conditionId")]
    tokens = [t for m in markets for t in (m.get("clobTokenIds") or [])]
    # Whitelist the hashing fields rather than copying the dict wholesale: only the
    # non-reversible fingerprint should ever leave the box, never a raw salt if one
    # were ever (mis)placed there.
    hashing = meta.get("hashing") or {}
    # Membership test (not ``or``): a live match's recorder count is authoritative even
    # if it were ever an empty dict; only a finished match (absent entirely) uses the tally.
    counts_by_event = meta.get("counts_by_event") or {}
    counts = counts_by_event[event_id] if event_id in counts_by_event else (written_counts or {})
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
        "counts": counts,
        "market_ids": conds,
        "clob_token_ids": tokens,
        "event": identity,
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
) -> dict[tuple[str, str], int]:
    """Stream ``src`` and append each wanted line (byte-exact) to its event writer.

    Returns a ``{(event_id, stream): lines_written}`` tally so the slice's ``meta.json``
    can report a finished match's actual count (it is absent from ``counts_by_event``).
    """
    tally: dict[tuple[str, str], int] = {}
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
                    tally[(eid, stream)] = tally.get((eid, stream), 0) + 1
    # Any trailing partial line (a live recorder mid-append) is intentionally dropped.
    return tally


def filter_run(
    run_dir: Path,
    event_ids: Iterable[str],
    dest_dir: Path,
    *,
    meta: dict[str, Any] | None = None,
    registry: dict[str, dict[str, Any]] | None = None,
    exported_at: str,
    chunk_bytes: int = _READ_CHUNK,
) -> list[tuple[str, Path]]:
    """Filter the combined run files into ``dest_dir/event-<id>/`` and list archive entries.

    Scans each combined file once, routing every attributed line to its event's
    writer — so selecting several matches stays a single pass per stream. When a
    ``registry`` is given (all matches incl. finished), book records attribute via its
    conditionId map, so FINISHED matches' records (gone from ``meta.events``) still
    route. Always writes a per-event ``meta.json`` slice (even for a match with no
    records yet). Returns the archive entries ``[(arcname, path), ...]``.
    """
    meta = meta if meta is not None else load_run_meta(run_dir)
    if registry:
        # Mirror the reader's attribution EXACTLY (reader._book_event): the registry
        # covers finished matches, but the current meta (open set) WINS on any shared
        # conditionId. So a record the dashboard counts under event X always downloads
        # under X too — and a meta conditionId a registry entry happens to lack (e.g. a
        # fixture whose market wasn't deployed at discovery time) is never dropped.
        cond2event = {**registry_cond_to_event(registry), **_cond_to_event(meta)}
    else:
        cond2event = _cond_to_event(meta)
    wanted = {str(e) for e in event_ids}
    writers: dict[tuple[str, str], BinaryIO] = {}
    tally: dict[tuple[str, str], int] = {}

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
            for key, n in _scan_and_route(
                src, stream, wanted, attribute, writer_for, chunk_bytes
            ).items():
                tally[key] = tally.get(key, 0) + n
    finally:
        for handle in writers.values():
            handle.close()

    entries: list[tuple[str, Path]] = []
    for eid in sorted(wanted):
        ev_dir = dest_dir / f"event-{eid}"
        ev_dir.mkdir(parents=True, exist_ok=True)
        meta_path = ev_dir / "meta.json"
        written = {s: tally[(eid, s)] for s in _STREAMS if tally.get((eid, s))}
        meta_path.write_text(
            json.dumps(
                per_event_meta(
                    meta, eid, exported_at=exported_at, registry=registry, written_counts=written
                ),
                indent=2,
            )
            + "\n",
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
