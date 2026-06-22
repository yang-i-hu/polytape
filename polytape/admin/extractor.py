"""Pre-build per-match download archives for FINISHED matches (near-instant downloads).

A finished match (rolled out of ``meta.events``) is immutable — the recorder never
appends to it again — so its filtered slice can be extracted from the combined
``book.jsonl`` / ``comments.jsonl`` ONCE and cached as ``event-<id>.tar.gz``. The
download route then serves that file directly instead of re-scanning the ~17 GB run
on every request.

A slow background task (in :mod:`polytape.admin.app`, off the event loop) builds the
pending finished matches in a single shared :func:`polytape.admin.download.filter_run`
pass and publishes each archive atomically (``.partial`` -> ``os.replace``) with a
completion marker written LAST, so a half-written archive is never served. Disabled
(no ``extract_dir``) => the download route behaves exactly as before.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import re
import shutil
import tarfile
import tempfile
from pathlib import Path

from polytape.admin import download as dl
from polytape.envelope import utc_now_iso

logger = logging.getLogger("polytape.admin.extractor")

EXTRACT_SCHEMA = 1
DEFAULT_CAP_BYTES = 4 * 1024 * 1024 * 1024  # 4 GiB of cached extracts

# Real ids are Gamma numeric strings; this is defense-in-depth so a malformed id can
# never make archive_path/marker_path point outside extract_dir (path traversal).
_SAFE_EID = re.compile(r"\A[A-Za-z0-9_-]+\Z")


def valid_event_id(event_id: str) -> bool:
    """True iff ``event_id`` is safe to interpolate into a cache filename (no separators)."""
    return isinstance(event_id, str) and bool(_SAFE_EID.match(event_id))


def archive_path(extract_dir: str | Path, event_id: str) -> Path:
    return Path(extract_dir) / f"event-{event_id}.tar.gz"


def marker_path(extract_dir: str | Path, event_id: str) -> Path:
    return Path(extract_dir) / f"event-{event_id}.done.json"


def has_complete_extract(extract_dir: str | Path, event_id: str) -> bool:
    """True iff a finished, fully-published extract exists (marker AND tarball)."""
    if not valid_event_id(event_id):
        return False
    return (
        marker_path(extract_dir, event_id).exists() and archive_path(extract_dir, event_id).exists()
    )


def _write_marker(extract_dir: Path, event_id: str, *, exported_at: str) -> None:
    """Write the completion marker LAST (temp + os.replace), so it implies a whole tar."""
    try:
        size = archive_path(extract_dir, event_id).stat().st_size
    except OSError:
        size = None
    payload = {
        "schema": EXTRACT_SCHEMA,
        "event_id": event_id,
        "built_at": utc_now_iso(),
        "exported_at": exported_at,
        "size": size,
    }
    mp = marker_path(extract_dir, event_id)
    tmp = mp.with_name(mp.name + ".partial")
    tmp.write_text(json.dumps(payload) + "\n", encoding="utf-8")
    os.replace(tmp, mp)


def build_extracts(
    run_dir: str | Path,
    extract_dir: str | Path,
    event_ids: list[str],
    *,
    registry: dict[str, dict] | None = None,
    meta: dict | None = None,
) -> list[str]:
    """Extract each event in ``event_ids`` to ``event-<id>.tar.gz`` — ONE shared scan.

    Reuses :func:`download.filter_run` to filter all requested matches out of the
    combined files in a single pass, then packages + atomically publishes each as its
    own archive (marker last). Per-event failures are logged and skipped. MUST be
    called inside ``asyncio.to_thread`` — it does a full-run scan.
    """
    extract_dir = Path(extract_dir)
    extract_dir.mkdir(parents=True, exist_ok=True)
    safe_ids = [e for e in event_ids if valid_event_id(e)]
    for bad in [e for e in event_ids if e not in safe_ids]:
        logger.warning("extractor: skipping unsafe event id %r", bad)
    if not safe_ids:
        return []
    exported_at = utc_now_iso()
    scratch = Path(tempfile.mkdtemp(prefix="polytape-extract-"))
    built: list[str] = []
    try:
        entries = dl.filter_run(
            run_dir, safe_ids, scratch, meta=meta, registry=registry, exported_at=exported_at
        )
        by_eid: dict[str, list[tuple[str, Path]]] = {}
        for arcname, path in entries:  # arcname = "event-<id>/<file>"
            head = arcname.split("/", 1)[0]
            eid = head[len("event-") :] if head.startswith("event-") else head
            by_eid.setdefault(eid, []).append((arcname, path))
        for eid, ents in by_eid.items():
            tar_path = archive_path(extract_dir, eid)
            tmp = tar_path.with_name(tar_path.name + ".partial")
            try:
                with tarfile.open(tmp, "w:gz") as tar:
                    for arcname, path in ents:
                        tar.add(path, arcname=arcname, recursive=False)
                os.replace(tmp, tar_path)  # publish the tar atomically...
                _write_marker(extract_dir, eid, exported_at=exported_at)  # ...then the marker
                built.append(eid)
            except OSError:
                logger.warning("extract for event %s failed", eid, exc_info=True)
                with contextlib.suppress(OSError):
                    tmp.unlink()
                continue
    finally:
        shutil.rmtree(scratch, ignore_errors=True)
    return built


def enforce_cap(extract_dir: str | Path, cap_bytes: int = DEFAULT_CAP_BYTES) -> None:
    """Evict oldest extracts (marker-first) until total size is under ``cap_bytes``.

    A tarball's bytes count toward the total even when its marker is missing/corrupt, and
    such tarballs are evicted FIRST (treated as oldest) — so a bad marker can never let the
    cache grow without bound past the cap.
    """
    extract_dir = Path(extract_dir)
    # (built_at, event_id, size); empty built_at sorts first => evicted first.
    rows: list[tuple[str, str, int]] = []
    for marker in extract_dir.glob("event-*.done.json"):
        eid = marker.name[len("event-") : -len(".done.json")]
        try:
            size = archive_path(extract_dir, eid).stat().st_size
        except OSError:
            continue  # marker with no tarball -> nothing to count or evict
        try:
            built_at = str(json.loads(marker.read_text(encoding="utf-8")).get("built_at", ""))
        except (OSError, json.JSONDecodeError):
            built_at = ""  # unparseable marker -> evict first, but DO count its bytes
        rows.append((built_at, eid, size))
    total = sum(size for _, _, size in rows)
    if total <= cap_bytes:
        return
    for _built_at, eid, size in sorted(rows):  # oldest built_at (and "" markers) first
        if total <= cap_bytes:
            break
        # Drop the marker FIRST so a concurrent reader treats it as incomplete, then the tar.
        marker_path(extract_dir, eid).unlink(missing_ok=True)
        archive_path(extract_dir, eid).unlink(missing_ok=True)
        total -= size
