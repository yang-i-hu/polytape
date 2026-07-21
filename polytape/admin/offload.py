"""Offload FINISHED matches to Cloud Storage, reclaiming SSD.

A finished match (rolled out of ``meta.events``) is immutable — the recorder
never appends to it again. Its per-match native dir
(``matches/event-<id>/{book.jsonl,meta.json}``) duplicates data already held in
the monolithic ``book.jsonl``, so it can be packaged, uploaded to a (Coldline)
GCS bucket, and removed from local disk — freeing SSD that would otherwise grow
forever (~½ the run's footprint is these per-match duplicates).

**Safety.** A local dir is deleted ONLY after its object is uploaded AND verified
present in GCS with a byte-exact size match. Even a bug cannot lose data: the
monolith retains every record, and until a match is offloaded its native dir is
untouched. A small marker (``matches/event-<id>.offloaded.json``) records the
object so (a) the admin download path can serve the match with a signed URL, and
(b) a re-run skips already-offloaded matches (idempotent).

**Completeness.** A match that straddled the per-match dual-write deploy has a
PARTIAL native ``book.jsonl`` (only the post-deploy tail). Selection runs the same
:func:`polytape.admin.download.have_native_matches` gate as the admin download
path, so a partial native is never archived as the match's canonical copy — it is
skipped (with a warning) until rebuilt from the monolith.

**Graceful degradation.** Even without the admin's signed-URL fast path, a
download of an offloaded match still works: with the native dir gone,
``download.have_native_matches`` returns False and the route falls back to the
monolith ``filter_run`` scan — slower, but correct (the monolith has the data).

Everything here except :class:`GcsBackend` is pure/offline and unit-tested with a
fake backend; the ``google.cloud.storage`` import is lazy so tests never need it.
"""

from __future__ import annotations

import json
import logging
import os
import tarfile
import tempfile
from pathlib import Path
from typing import Any, Protocol

logger = logging.getLogger("polytape.admin.offload")

OFFLOAD_MARKER_SCHEMA = 1
DEFAULT_OBJECT_PREFIX = "matches"
DEFAULT_STORAGE_CLASS = "COLDLINE"
DEFAULT_SIGNED_URL_TTL_S = 3600  # 1 hour is plenty for a browser/harvest fetch.


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


# --------------------------------------------------------------------------- #
# Selection (which finished matches can be offloaded)
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

    A match qualifies iff it has a COMPLETE native ``matches/event-<id>/book.jsonl``
    (same :func:`~polytape.admin.download.have_native_matches` gate as the admin
    download path — a partial native from a match that straddled the per-match
    deploy must never become the archived canonical copy), is NOT in the current
    open set (i.e. finished/immutable), and is not already offloaded. The monolith
    is the backstop, so this is deliberately simple — it never touches a
    still-recording match.
    """
    from polytape.admin.download import have_native_matches

    run_dir = Path(run_dir)
    matches_dir = run_dir / "matches"
    open_ids = open_event_ids(meta)
    out: list[str] = []
    for eid in native_event_dirs(run_dir):
        if eid in open_ids:
            continue  # still recording — never touch
        if is_offloaded(matches_dir, eid):
            continue  # already archived
        if not have_native_matches(run_dir, [eid], meta):
            logger.warning(
                "offload: event %s native book.jsonl is PARTIAL (fewer lines than the "
                "monolith count in meta.counts_by_event); skipping — rebuild it from "
                "the monolith before offloading",
                eid,
            )
            continue
        out.append(eid)
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
    import shutil

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

        remote = backend.stat(name)
        if remote is None or int(remote.get("size", -1)) != local_size:
            raise OSError(
                f"offload verify failed for event {event_id}: local={local_size} remote={remote}"
            )

        marker = {
            "schema": OFFLOAD_MARKER_SCHEMA,
            "event_id": event_id,
            "gs_uri": uploaded["gs_uri"],
            "object": name,
            "size": local_size,
            "crc32c": uploaded.get("crc32c"),
            "offloaded_at": now_iso,
        }
        mp = marker_path(matches_dir, event_id)
        mp_tmp = mp.with_name(mp.name + ".partial")
        mp_tmp.write_text(json.dumps(marker) + "\n", encoding="utf-8")
        os.replace(mp_tmp, mp)  # marker published atomically — implies a verified object

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
    try:
        meta = json.loads((run_dir / "meta.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
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

    ap = argparse.ArgumentParser(
        prog="python -m polytape.admin.offload",
        description="Offload finished matches' per-match files to a GCS bucket, freeing SSD.",
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
        help="Where the transient tar is staged (put it on the run volume).",
    )
    ap.add_argument("--limit", type=int, default=None, help="Offload at most N matches this run.")
    ap.add_argument("--list", action="store_true", help="List offloadable matches; do nothing.")
    ap.add_argument("--log-level", default="INFO")
    args = ap.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    if not args.bucket:
        ap.error("--bucket (or $POLYTAPE_GCS_BUCKET) is required")

    if args.list:
        try:
            meta = json.loads((Path(args.run_dir) / "meta.json").read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            ap.error(f"cannot read run meta: {exc}")
        ids = offloadable_event_ids(args.run_dir, meta)
        print(f"offloadable ({len(ids)}): {' '.join(ids) or '(none)'}")
        return 0

    backend = GcsBackend(args.bucket, key_file=args.key_file, storage_class=args.storage_class)
    done = run_offload(
        args.run_dir, backend, prefix=args.prefix, scratch_dir=args.scratch_dir, limit=args.limit
    )
    print(f"offloaded {len(done)} match(es): {' '.join(done) or '(none)'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
