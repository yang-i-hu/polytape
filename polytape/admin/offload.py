"""Offload FINISHED matches and CLOSED daily monolith segments to Cloud Storage,
reclaiming SSD on an open-ended run.

Two independent sweeps over one run directory (layout in :mod:`polytape.writer`)::

    <run>/
    ├── book.jsonl                       legacy monolith — never touched here
    ├── book.<YYYY-MM-DD>.jsonl          daily segments  — the SEGMENT sweep
    ├── segments/book.<day>.offloaded.json   segment marker (written by this module)
    └── matches/
        ├── event-<id>/{book.jsonl,meta.json}   per-match natives — the MATCH sweep
        └── event-<id>.offloaded.json           match marker (written by this module)

**Matches.** A finished match (rolled out of ``meta.events``) is immutable — the
recorder never appends to it again. Its per-match native dir
(``matches/event-<id>/{book.jsonl,meta.json}``) duplicates data already held in
the monolith, so it can be packaged, uploaded to a (Coldline) GCS bucket as
``<prefix>/event-<id>.tar.gz``, and removed from local disk — freeing SSD that
would otherwise grow forever (~½ the run's footprint is these per-match duplicates).
A marker (``matches/event-<id>.offloaded.json``) records the object so (a) the admin
download path can serve the match with a signed URL, and (b) a re-run skips
already-offloaded matches (idempotent).

**Segments.** An open-ended run's monolith is split into daily segments
``book.<YYYY-MM-DD>.jsonl`` (UTC day of ``ts_recv``). A segment is CLOSED once its
day is before today, nothing has written to it for ``min_age_s`` (15 min by default,
comfortably past the roll-over) and the recorder's ``meta.json#segments`` does not
name it as open. A closed segment is compressed with the ``zstd`` CLI
(``zstd -T0 -6 -q -o <scratch>.zst <segment>``; injectable for tests), uploaded to
``<prefix>/segments/book.<day>.jsonl.zst``, verified byte-exact in
GCS, recorded in the marker ``segments/book.<day>.offloaded.json`` and only then
deleted locally, along with the scratch ``.zst``. The segment sweep never touches
today's segment, the legacy ``book.jsonl`` or any per-match native; a failure at any
step leaves the local file intact for the next run, and the markers make re-runs
idempotent.

**Safety (both sweeps).** A local file is deleted ONLY after its object is uploaded
AND verified present in GCS with a byte-exact size match, and the marker is durable
on disk. Never delete a marker without also removing its GCS object.

**Graceful degradation.** Even without the admin's signed-URL fast path, a
download of an offloaded match still works: with the native dir gone,
``download.have_native_matches`` returns False and the route falls back to the
monolith ``filter_run`` scan — slower, but correct while the monolith segments that
hold the match are still local.

Everything here except :class:`GcsBackend` and :func:`zstd_compress` is pure/offline
and unit-tested with a fake backend + fake compressor; the ``google.cloud.storage``
import is lazy so tests never need it, and no test spawns ``zstd``.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import tarfile
import tempfile
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any, Protocol

from polytape.writer import parse_segment_name

logger = logging.getLogger("polytape.admin.offload")

OFFLOAD_MARKER_SCHEMA = 1
SEGMENT_MARKER_SCHEMA = 1
DEFAULT_OBJECT_PREFIX = "matches"
DEFAULT_STORAGE_CLASS = "COLDLINE"
DEFAULT_SIGNED_URL_TTL_S = 3600  # 1 hour is plenty for a browser/harvest fetch.
SEGMENTS_DIRNAME = "segments"
#: A closed segment must have been quiet this long before it is offloaded. The writer
#: rolls at the first write after midnight UTC, so 15 min is far past any roll-over.
DEFAULT_SEGMENT_MIN_AGE_S = 900
#: zstd CLI options: all cores, level 6 (fast, ~5-8x on JSONL), quiet.
ZSTD_ARGS = ("-T0", "-6", "-q")
_READ_CHUNK = 8 * 1024 * 1024


# --------------------------------------------------------------------------- #
# Markers (pure)
# --------------------------------------------------------------------------- #


def marker_path(matches_dir: str | Path, event_id: str) -> Path:
    """Path of the offload marker for one event (sits beside its ``event-<id>/`` dir)."""
    return Path(matches_dir) / f"event-{event_id}.offloaded.json"


def read_marker(matches_dir: str | Path, event_id: str) -> dict[str, Any] | None:
    """Return the parsed marker for ``event_id``, or ``None`` if absent/unreadable."""
    try:
        data = json.loads(marker_path(matches_dir, event_id).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return None
    return data if isinstance(data, dict) else None


def is_offloaded(matches_dir: str | Path, event_id: str) -> bool:
    """True iff a complete offload marker exists for ``event_id``."""
    marker = read_marker(matches_dir, event_id)
    return bool(marker and marker.get("gs_uri"))


def object_name(prefix: str, event_id: str) -> str:
    """GCS object name for a match archive: ``<prefix>/event-<id>.tar.gz``."""
    return f"{prefix.rstrip('/')}/event-{event_id}.tar.gz"


def segment_marker_path(run_dir: str | Path, segment: str) -> Path:
    """Marker for a daily segment: ``<run>/segments/book.<day>.offloaded.json``."""
    stem = segment[: -len(".jsonl")] if segment.endswith(".jsonl") else segment
    return Path(run_dir) / SEGMENTS_DIRNAME / f"{stem}.offloaded.json"


def read_segment_marker(run_dir: str | Path, segment: str) -> dict[str, Any] | None:
    """The parsed marker for segment file name ``segment``, or ``None`` if absent/unreadable."""
    try:
        data = json.loads(segment_marker_path(run_dir, segment).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return None
    return data if isinstance(data, dict) else None


def is_segment_offloaded(run_dir: str | Path, segment: str) -> bool:
    """True iff a complete offload marker exists for the segment file name ``segment``."""
    marker = read_segment_marker(run_dir, segment)
    return bool(marker and marker.get("gs_uri"))


def segment_object_name(prefix: str, segment: str) -> str:
    """GCS object name for a segment: ``<prefix>/segments/<segment>.zst``
    (e.g. ``run-maker/segments/book.2026-09-02.jsonl.zst``). The prefix is the
    campaign's namespace in the bucket (finished matches sit beside it as
    ``<prefix>/event-<id>.tar.gz``), so one prefix per run keeps runs apart."""
    return f"{prefix.rstrip('/')}/{SEGMENTS_DIRNAME}/{segment}.zst"


# --------------------------------------------------------------------------- #
# Selection (which finished matches / closed segments can be offloaded)
# --------------------------------------------------------------------------- #


def open_event_ids(meta: dict[str, Any]) -> set[str]:
    """The currently-OPEN (still-recording) event ids from ``meta.events``."""
    return {str(e.get("id")) for e in (meta.get("events") or []) if e.get("id") is not None}


def native_event_dirs(run_dir: str | Path) -> list[str]:
    """Event ids that have a per-match native ``book.jsonl`` under ``matches/``."""
    base = Path(run_dir) / "matches"
    if not base.exists():
        return []
    out: list[str] = []
    for d in sorted(base.glob("event-*")):
        if d.is_dir() and (d / "book.jsonl").exists():
            out.append(d.name[len("event-") :])
    return out


def offloadable_event_ids(run_dir: str | Path, meta: dict[str, Any]) -> list[str]:
    """Finished matches whose native dir can be offloaded now.

    A match qualifies iff it has a native ``matches/event-<id>/book.jsonl``, is NOT
    in the current open set (i.e. finished/immutable), and is not already offloaded.
    The monolith is the backstop, so this is deliberately simple — it never touches
    a still-recording match.
    """
    matches_dir = Path(run_dir) / "matches"
    open_ids = open_event_ids(meta)
    out: list[str] = []
    for eid in native_event_dirs(run_dir):
        if eid in open_ids:
            continue  # still recording — never touch
        if is_offloaded(matches_dir, eid):
            continue  # already archived
        out.append(eid)
    return out


def open_segments(meta: dict[str, Any]) -> set[str]:
    """Segment file names the recorder reports as OPEN (``meta.segments[<stream>].current``)."""
    out: set[str] = set()
    segments = meta.get("segments")
    if isinstance(segments, dict):
        for per_stream in segments.values():
            if isinstance(per_stream, dict) and isinstance(per_stream.get("current"), str):
                out.add(per_stream["current"])
    return out


def local_segments(run_dir: str | Path) -> list[str]:
    """File names of every daily segment (``<stream>.<YYYY-MM-DD>.jsonl``) in the run
    dir's top level, sorted (= chronological within a stream)."""
    run_dir = Path(run_dir)
    if not run_dir.is_dir():
        return []
    return sorted(p.name for p in run_dir.iterdir() if p.is_file() and parse_segment_name(p.name))


def offloadable_segments(
    run_dir: str | Path,
    meta: dict[str, Any],
    *,
    today: str,
    min_age_s: float = DEFAULT_SEGMENT_MIN_AGE_S,
    now_s: float | None = None,
) -> list[str]:
    """Closed segments that can be offloaded now (file names, chronological).

    A segment qualifies iff ALL hold: its day is strictly before ``today``
    (``YYYY-MM-DD``, UTC — a plain string compare); the recorder's ``meta.segments``
    does not name it as the open segment; its mtime is at least ``min_age_s`` old
    (nothing wrote to it recently); and it has no offload marker. A segment that has a
    marker but is still on disk is left alone with a warning — a human must decide,
    since deleting it unverified is the one thing this module never does.
    """
    now_s = time.time() if now_s is None else now_s
    run_dir = Path(run_dir)
    still_open = open_segments(meta)
    out: list[str] = []
    for name in local_segments(run_dir):
        _stream, day = parse_segment_name(name)  # type: ignore[misc]  # local_segments filtered
        if day >= today:
            continue  # today's segment (or a clock-skewed future one) — never touch
        if name in still_open:
            continue  # the recorder says it is still appending — never touch
        if is_segment_offloaded(run_dir, name):
            logger.warning(
                "segment %s has an offload marker but is still on disk; leaving it alone", name
            )
            continue
        try:
            age = now_s - (run_dir / name).stat().st_mtime
        except OSError:
            continue
        if age < min_age_s:
            continue  # quiet time not reached (a roll-over might still be settling)
        out.append(name)
    return out


# --------------------------------------------------------------------------- #
# GCS backend (thin; injectable for tests)
# --------------------------------------------------------------------------- #


class Backend(Protocol):
    """Minimal object-store surface the offloader needs (see :class:`GcsBackend`)."""

    def upload(self, local_path: Path, name: str) -> dict[str, Any]: ...
    def stat(self, name: str) -> dict[str, Any] | None: ...
    def signed_url(self, name: str, ttl_seconds: int) -> str: ...


class GcsBackend:
    """:class:`Backend` over Google Cloud Storage (``google-cloud-storage``).

    Authenticated by a service-account key file (``key_file`` /
    ``GOOGLE_APPLICATION_CREDENTIALS``) that needs only object access on the one
    bucket. Signing is done locally with the key, so no extra IAM role is required
    for signed URLs. The library import is lazy so importing this module stays free.
    """

    def __init__(
        self,
        bucket: str,
        *,
        key_file: str | Path | None = None,
        storage_class: str | None = DEFAULT_STORAGE_CLASS,
    ) -> None:
        from google.cloud import storage  # lazy: only the VM/offloader needs it

        bucket = bucket[len("gs://") :].split("/", 1)[0] if bucket.startswith("gs://") else bucket
        if key_file:
            self._client = storage.Client.from_service_account_json(str(key_file))
        else:
            self._client = storage.Client()
        self._bucket = self._client.bucket(bucket)
        self._storage_class = storage_class

    def upload(self, local_path: Path, name: str) -> dict[str, Any]:
        blob = self._bucket.blob(name)
        if self._storage_class:
            blob.storage_class = self._storage_class
        # upload_from_filename computes+checks crc32c end-to-end, so a corrupted
        # transfer raises rather than silently storing bad bytes.
        blob.upload_from_filename(str(local_path))
        blob.reload()
        return {
            "gs_uri": f"gs://{self._bucket.name}/{name}",
            "size": blob.size,
            "crc32c": blob.crc32c,
            "storage_class": blob.storage_class,
        }

    def stat(self, name: str) -> dict[str, Any] | None:
        blob = self._bucket.get_blob(name)
        if blob is None:
            return None
        return {"size": blob.size, "crc32c": blob.crc32c}

    def signed_url(self, name: str, ttl_seconds: int) -> str:
        from datetime import timedelta

        blob = self._bucket.blob(name)
        return blob.generate_signed_url(
            version="v4", expiration=timedelta(seconds=ttl_seconds), method="GET"
        )


# --------------------------------------------------------------------------- #
# Compression (zstd CLI; injectable for tests)
# --------------------------------------------------------------------------- #

#: ``compressor(src, dest)`` writes a compressed copy of ``src`` to ``dest`` (which does
#: not exist yet) and raises ``OSError`` on failure. Tests inject a pure-Python fake.
Compressor = Callable[[Path, Path], None]


def zstd_compress(src: Path, dest: Path, *, zstd_bin: str = "zstd") -> None:
    """Compress ``src`` into ``dest`` with the ``zstd`` CLI: ``zstd -T0 -6 -q -o dest src``.

    The CLI streams the (multi-GB) file with all cores and constant memory — far
    better than any pure-Python option, and it is the only reason the offloader
    shells out. Raises ``OSError`` if the binary is missing, exits non-zero, or
    produces no output; the source is never modified.
    """
    argv = [zstd_bin, *ZSTD_ARGS, "-o", str(dest), str(src)]
    try:
        proc = subprocess.run(argv, capture_output=True, text=True, check=False)
    except OSError as exc:  # binary missing / not executable
        raise OSError(f"cannot run {zstd_bin!r} (is zstd installed?): {exc}") from exc
    if proc.returncode != 0:
        raise OSError(f"{zstd_bin} exited {proc.returncode} for {src.name}: {proc.stderr.strip()}")
    if not dest.is_file():
        raise OSError(f"{zstd_bin} produced no output at {dest}")


def _count_lines(path: Path, chunk: int = _READ_CHUNK) -> int:
    """Count newlines with a flat-memory streaming read (never loads the file whole)."""
    n = 0
    with open(path, "rb") as fh:
        while True:
            block = fh.read(chunk)
            if not block:
                return n
            n += block.count(b"\n")


# --------------------------------------------------------------------------- #
# Offload one match (tar native -> upload -> verify -> marker -> delete)
# --------------------------------------------------------------------------- #


def _tar_native_dir(native_dir: Path, event_id: str, dest: Path) -> None:
    """Package a match's native files into ``event-<id>/{meta.json,book.jsonl}`` in a
    gzip tar — byte-exact ``book.jsonl``, so an offloaded archive matches a native/
    filtered download."""
    with tarfile.open(dest, "w:gz") as tar:
        for name in ("meta.json", "book.jsonl"):
            path = native_dir / name
            if path.exists():
                tar.add(path, arcname=f"event-{event_id}/{name}", recursive=False)


def _publish_marker(path: Path, marker: dict[str, Any]) -> None:
    """Write a marker atomically (``.partial`` + ``os.replace``): once it exists, the
    object it names has been verified in GCS."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".partial")
    tmp.write_text(json.dumps(marker) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def _verify_uploaded(name: str, backend: Backend, local_size: int, what: str) -> dict[str, Any]:
    """Stat ``name`` in the store and require a byte-exact size match; returns the stat."""
    remote = backend.stat(name)
    if remote is None or int(remote.get("size", -1)) != local_size:
        raise OSError(f"offload verify failed for {what}: local={local_size} remote={remote}")
    return remote


def offload_one(
    run_dir: str | Path,
    event_id: str,
    backend: Backend,
    *,
    prefix: str = DEFAULT_OBJECT_PREFIX,
    scratch_dir: str | Path | None = None,
    now_iso: str,
) -> dict[str, Any]:
    """Offload ONE finished match's native dir to GCS and remove it locally.

    Steps, in order (the delete is LAST and only after a verified upload):
      1. tar ``matches/event-<id>/`` into a scratch ``event-<id>.tar.gz``,
      2. upload to ``<prefix>/event-<id>.tar.gz``,
      3. verify the object exists in GCS with the exact local tar size,
      4. write the marker ``matches/event-<id>.offloaded.json`` (atomically),
      5. delete the local native dir and the scratch tar.

    Returns the marker dict. Raises on any failure BEFORE the marker is written,
    leaving the native dir intact (safe to retry).
    """
    run_dir = Path(run_dir)
    matches_dir = run_dir / "matches"
    native_dir = matches_dir / f"event-{event_id}"
    if not (native_dir / "book.jsonl").exists():
        raise FileNotFoundError(f"no native book.jsonl for event {event_id}")

    name = object_name(prefix, event_id)
    scratch_root = Path(scratch_dir) if scratch_dir else None
    if scratch_root:
        scratch_root.mkdir(parents=True, exist_ok=True)
    tmp = Path(tempfile.mkdtemp(prefix="polytape-offload-", dir=scratch_root))
    try:
        tar_path = tmp / f"event-{event_id}.tar.gz"
        _tar_native_dir(native_dir, event_id, tar_path)
        local_size = tar_path.stat().st_size

        uploaded = backend.upload(tar_path, name)
        _verify_uploaded(name, backend, local_size, f"event {event_id}")

        marker = {
            "schema": OFFLOAD_MARKER_SCHEMA,
            "event_id": event_id,
            "gs_uri": uploaded["gs_uri"],
            "object": name,
            "size": local_size,
            "crc32c": uploaded.get("crc32c"),
            "offloaded_at": now_iso,
        }
        # marker published atomically — implies a verified object
        _publish_marker(marker_path(matches_dir, event_id), marker)

        # Only now, with the object verified and the marker durable, reclaim the disk.
        shutil.rmtree(native_dir)
        logger.info(
            "offloaded event %s -> %s (%d bytes), local dir removed",
            event_id,
            uploaded["gs_uri"],
            local_size,
        )
        return marker
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def run_offload(
    run_dir: str | Path,
    backend: Backend,
    *,
    prefix: str = DEFAULT_OBJECT_PREFIX,
    scratch_dir: str | Path | None = None,
    limit: int | None = None,
    now_iso_fn: Any = None,
) -> list[str]:
    """Offload all currently-offloadable finished matches (up to ``limit``).

    Reloads ``meta.json`` once for the open set. Per-match failures are logged and
    skipped (the native dir stays intact for a later retry). Returns offloaded ids.
    """
    from polytape.envelope import utc_now_iso

    now_iso_fn = now_iso_fn or utc_now_iso
    run_dir = Path(run_dir)
    meta = _load_meta(run_dir)
    if meta is None:
        logger.warning("offload: run meta.json unreadable; nothing to do")
        return []
    candidates = offloadable_event_ids(run_dir, meta)
    if limit is not None:
        candidates = candidates[:limit]
    done: list[str] = []
    for eid in candidates:
        try:
            offload_one(
                run_dir, eid, backend, prefix=prefix, scratch_dir=scratch_dir, now_iso=now_iso_fn()
            )
            done.append(eid)
        except Exception:  # noqa: BLE001 - never let one match abort the batch
            logger.warning("offload failed for event %s (native dir kept)", eid, exc_info=True)
    if done:
        logger.info("offload pass complete: %d match(es) archived", len(done))
    return done


# --------------------------------------------------------------------------- #
# Offload one segment (zstd -> upload -> verify -> marker -> delete)
# --------------------------------------------------------------------------- #


def offload_segment(
    run_dir: str | Path,
    segment: str,
    backend: Backend,
    *,
    prefix: str = DEFAULT_OBJECT_PREFIX,
    scratch_dir: str | Path | None = None,
    now_iso: str,
    compressor: Compressor = zstd_compress,
    count_lines: bool = True,
) -> dict[str, Any]:
    """Offload ONE closed daily segment (file name ``segment``) to GCS and delete it locally.

    Steps, in order (the delete is LAST and only after a verified upload):
      1. (optionally) count the segment's lines for the marker,
      2. compress into a scratch ``<segment>.zst`` with ``compressor``,
      3. upload to ``<prefix>/segments/<segment>.zst``,
      4. verify the object exists in GCS with the exact local ``.zst`` size (and that
         the segment did not grow meanwhile),
      5. write the marker ``segments/book.<day>.offloaded.json`` (atomically),
      6. delete the local segment and the scratch ``.zst``.

    Caller is responsible for selection (:func:`offloadable_segments`); this function
    only refuses a name that is not a segment. Returns the marker::

        {"schema": 1, "kind": "segment", "segment": "book.2026-09-02.jsonl",
         "stream": "book", "day": "2026-09-02", "gs_uri": "gs://...", "object": "...",
         "bytes": <.zst size>, "raw_bytes": <.jsonl size>, "crc32c": ..., "lines": N|null,
         "compression": "zstd", "offloaded_at": "..."}

    Raises on any failure BEFORE the marker is written, leaving the segment intact.
    """
    parsed = parse_segment_name(segment)
    if parsed is None:
        raise ValueError(f"not a daily segment file name: {segment!r}")
    stream, day = parsed
    run_dir = Path(run_dir)
    src = run_dir / segment
    if not src.is_file():
        raise FileNotFoundError(f"no local segment {segment}")

    name = segment_object_name(prefix, segment)
    scratch_root = Path(scratch_dir) if scratch_dir else None
    if scratch_root:
        scratch_root.mkdir(parents=True, exist_ok=True)
    tmp = Path(tempfile.mkdtemp(prefix="polytape-offload-", dir=scratch_root))
    try:
        raw_bytes = src.stat().st_size
        lines = _count_lines(src) if count_lines else None
        zst = tmp / f"{segment}.zst"
        compressor(src, zst)
        zst_bytes = zst.stat().st_size
        if src.stat().st_size != raw_bytes:
            raise OSError(f"segment {segment} grew during compression; leaving it for next run")

        uploaded = backend.upload(zst, name)
        remote = _verify_uploaded(name, backend, zst_bytes, f"segment {segment}")
        up_crc, rm_crc = uploaded.get("crc32c"), remote.get("crc32c")
        if up_crc and rm_crc and up_crc != rm_crc:
            raise OSError(f"offload verify failed for segment {segment}: crc32c mismatch")
        crc = up_crc or rm_crc

        marker = {
            "schema": SEGMENT_MARKER_SCHEMA,
            "kind": "segment",
            "segment": segment,
            "stream": stream,
            "day": day,
            "gs_uri": uploaded["gs_uri"],
            "object": name,
            "bytes": zst_bytes,
            "raw_bytes": raw_bytes,
            "crc32c": crc,
            "lines": lines,
            "compression": "zstd",
            "offloaded_at": now_iso,
        }
        _publish_marker(segment_marker_path(run_dir, segment), marker)

        # Only now, with the object verified and the marker durable, reclaim the disk.
        src.unlink()
        logger.info(
            "offloaded segment %s -> %s (%d -> %d bytes), local file removed",
            segment,
            uploaded["gs_uri"],
            raw_bytes,
            zst_bytes,
        )
        return marker
    finally:
        shutil.rmtree(tmp, ignore_errors=True)  # the scratch .zst goes with it


def offload_segments(
    run_dir: str | Path,
    backend: Backend,
    *,
    today: str,
    compressor: Compressor = zstd_compress,
    min_age_s: float = DEFAULT_SEGMENT_MIN_AGE_S,
    prefix: str = DEFAULT_OBJECT_PREFIX,
    scratch_dir: str | Path | None = None,
    limit: int | None = None,
    now_s: float | None = None,
    now_iso_fn: Any = None,
    count_lines: bool = True,
) -> list[str]:
    """Offload every closed daily segment (see :func:`offloadable_segments`), up to ``limit``.

    ``today`` is the current UTC day (``YYYY-MM-DD``); only segments of EARLIER days
    are candidates. Reads ``meta.json`` once for the recorder's open segment (an
    unreadable meta just drops that extra guard — the day and quiet-time rules still
    hold). Per-segment failures are logged and skipped, leaving the local file intact
    for the next run. Returns the offloaded segment file names, chronological.
    """
    from polytape.envelope import utc_now_iso

    now_iso_fn = now_iso_fn or utc_now_iso
    run_dir = Path(run_dir)
    meta = _load_meta(run_dir)
    if meta is None:
        logger.warning("offload: run meta.json unreadable; open-segment guard unavailable")
        meta = {}
    candidates = offloadable_segments(run_dir, meta, today=today, min_age_s=min_age_s, now_s=now_s)
    if limit is not None:
        candidates = candidates[:limit]
    done: list[str] = []
    for segment in candidates:
        try:
            offload_segment(
                run_dir,
                segment,
                backend,
                prefix=prefix,
                scratch_dir=scratch_dir,
                now_iso=now_iso_fn(),
                compressor=compressor,
                count_lines=count_lines,
            )
            done.append(segment)
        except Exception:  # noqa: BLE001 - never let one segment abort the batch
            logger.warning(
                "segment offload failed for %s (local file kept)", segment, exc_info=True
            )
    if done:
        logger.info("segment offload pass complete: %d segment(s) archived", len(done))
    return done


def _load_meta(run_dir: Path) -> dict[str, Any] | None:
    """The run's ``meta.json`` as a dict, or ``None`` if missing/unreadable/not an object."""
    try:
        meta = json.loads((run_dir / "meta.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return None
    return meta if isinstance(meta, dict) else None


def signed_download_url(
    matches_dir: str | Path,
    event_id: str,
    backend: Backend,
    *,
    ttl_seconds: int = DEFAULT_SIGNED_URL_TTL_S,
) -> str | None:
    """A time-limited GCS GET URL for an offloaded match, or ``None`` if not offloaded."""
    marker = read_marker(matches_dir, event_id)
    if not marker or not marker.get("object"):
        return None
    return backend.signed_url(str(marker["object"]), ttl_seconds)


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #


def main(argv: list[str] | None = None) -> int:
    import argparse
    from functools import partial

    from polytape.envelope import utc_now_iso

    ap = argparse.ArgumentParser(
        prog="python -m polytape.admin.offload",
        description=(
            "Offload finished matches' per-match files, then closed daily monolith "
            "segments, to a GCS bucket, freeing SSD."
        ),
    )
    ap.add_argument("--run-dir", default=os.environ.get("POLYTAPE_RUN_DIR", "/data/run-wc"))
    ap.add_argument(
        "--bucket",
        default=os.environ.get("POLYTAPE_GCS_BUCKET"),
        help="Target bucket (name or gs:// URI). Required.",
    )
    ap.add_argument(
        "--key-file",
        default=(
            os.environ.get("POLYTAPE_GCS_KEY") or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        ),
        help="Service-account JSON key with object access on the bucket.",
    )
    ap.add_argument(
        "--prefix", default=os.environ.get("POLYTAPE_GCS_PREFIX", DEFAULT_OBJECT_PREFIX)
    )
    ap.add_argument(
        "--storage-class",
        default=os.environ.get("POLYTAPE_GCS_STORAGE_CLASS", DEFAULT_STORAGE_CLASS),
    )
    ap.add_argument(
        "--scratch-dir",
        default=os.environ.get("POLYTAPE_SCRATCH_DIR"),
        help="Where the transient tar / .zst is staged (put it on the run volume).",
    )
    ap.add_argument(
        "--limit", type=int, default=None, help="Offload at most N matches and N segments."
    )
    sweep = ap.add_mutually_exclusive_group()
    sweep.add_argument(
        "--matches-only", action="store_true", help="Run only the finished-matches sweep."
    )
    sweep.add_argument(
        "--segments-only", action="store_true", help="Run only the daily-segments sweep."
    )
    ap.add_argument(
        "--min-age",
        type=float,
        default=float(os.environ.get("POLYTAPE_SEGMENT_MIN_AGE_S", DEFAULT_SEGMENT_MIN_AGE_S)),
        help="Seconds a closed segment must have been quiet before it is offloaded.",
    )
    ap.add_argument(
        "--zstd",
        default=os.environ.get("POLYTAPE_ZSTD", "zstd"),
        help="zstd binary used to compress segments (must be installed on the host).",
    )
    ap.add_argument(
        "--no-count-lines",
        action="store_true",
        help="Skip the line count in segment markers (saves one extra read per segment).",
    )
    ap.add_argument("--list", action="store_true", help="List what is offloadable; do nothing.")
    ap.add_argument("--log-level", default="INFO")
    args = ap.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    if not args.bucket:
        ap.error("--bucket (or $POLYTAPE_GCS_BUCKET) is required")

    do_matches = not args.segments_only
    do_segments = not args.matches_only
    today = utc_now_iso()[:10]

    if args.list:
        meta = _load_meta(Path(args.run_dir))
        if do_matches:
            if meta is None:
                ap.error(f"cannot read run meta: {Path(args.run_dir) / 'meta.json'}")
            ids = offloadable_event_ids(args.run_dir, meta)
            print(f"offloadable matches ({len(ids)}): {' '.join(ids) or '(none)'}")
        if do_segments:
            segs = offloadable_segments(
                args.run_dir, meta or {}, today=today, min_age_s=args.min_age
            )
            print(f"offloadable segments ({len(segs)}): {' '.join(segs) or '(none)'}")
        return 0

    backend = GcsBackend(args.bucket, key_file=args.key_file, storage_class=args.storage_class)
    if do_matches:
        done = run_offload(
            args.run_dir,
            backend,
            prefix=args.prefix,
            scratch_dir=args.scratch_dir,
            limit=args.limit,
        )
        print(f"offloaded {len(done)} match(es): {' '.join(done) or '(none)'}")
    if do_segments:
        done_segments = offload_segments(
            args.run_dir,
            backend,
            today=today,
            compressor=partial(zstd_compress, zstd_bin=args.zstd),
            min_age_s=args.min_age,
            prefix=args.prefix,
            scratch_dir=args.scratch_dir,
            limit=args.limit,
            count_lines=not args.no_count_lines,
        )
        print(f"offloaded {len(done_segments)} segment(s): {' '.join(done_segments) or '(none)'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
