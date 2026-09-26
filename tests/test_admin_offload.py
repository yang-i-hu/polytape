"""Tests for offloading finished matches to Cloud Storage (polytape/admin/offload.py).

The GCS backend is faked, so nothing here touches the network or needs
``google-cloud-storage`` (the real client is imported lazily inside ``GcsBackend``).
"""

from __future__ import annotations

import json
import tarfile

import pytest

from polytape.admin import offload as ofl

# --------------------------------------------------------------------------- #
# Fake backend + a small run fixture
# --------------------------------------------------------------------------- #


class FakeBackend:
    """In-memory object store standing in for GCS."""

    def __init__(self, *_a, **_k) -> None:
        self.objects: dict[str, bytes] = {}
        self.fail_upload = False

    def upload(self, local_path, name):
        if self.fail_upload:
            raise OSError("simulated upload failure")
        data = open(local_path, "rb").read()
        self.objects[name] = data
        return {
            "gs_uri": f"gs://fake/{name}",
            "size": len(data),
            "crc32c": "c",
            "storage_class": "COLDLINE",
        }

    def stat(self, name):
        d = self.objects.get(name)
        return None if d is None else {"size": len(d), "crc32c": "c"}

    def signed_url(self, name, ttl_seconds):
        return f"https://signed.example/{name}?ttl={ttl_seconds}"


def _native(run_dir, event_id, *, book_lines=("a", "b"), meta=None):
    d = run_dir / "matches" / f"event-{event_id}"
    d.mkdir(parents=True)
    # newline="" so \n is written verbatim (match the recorder; avoid Windows \r\n).
    (d / "book.jsonl").write_text(
        "".join(line + "\n" for line in book_lines), encoding="utf-8", newline=""
    )
    (d / "meta.json").write_text(json.dumps(meta or {"event_id": event_id}), encoding="utf-8")
    return d


def _write_meta(run_dir, open_ids, counts_by_event=None):
    meta = {"events": [{"id": e} for e in open_ids]}
    if counts_by_event is not None:
        meta["counts_by_event"] = counts_by_event
    (run_dir / "meta.json").write_text(json.dumps(meta), encoding="utf-8")
    return meta


# --------------------------------------------------------------------------- #
# Selection
# --------------------------------------------------------------------------- #


def test_offloadable_excludes_open_and_already_offloaded(tmp_path):
    _write_meta(tmp_path, open_ids=["1001"])  # 1001 is still recording
    _native(tmp_path, "1001")  # open -> never offload
    _native(tmp_path, "0900")  # finished -> offloadable
    _native(tmp_path, "0901")  # finished, but already offloaded (marker below)
    ofl.marker_path(tmp_path / "matches", "0901").write_text(
        json.dumps({"gs_uri": "gs://x/y", "object": "matches/event-0901.tar.gz"}), encoding="utf-8"
    )
    assert ofl.offloadable_event_ids(tmp_path, {"events": [{"id": "1001"}]}) == ["0900"]


def test_offloadable_none_when_no_matches_dir(tmp_path):
    assert ofl.offloadable_event_ids(tmp_path, {"events": []}) == []


def test_offloadable_excludes_partial_native(tmp_path):
    """A match that straddled the per-match deploy has a PARTIAL native book.jsonl
    (fewer lines than the monolith count) — it must never be selected for offload."""
    meta = _write_meta(
        tmp_path,
        open_ids=[],
        counts_by_event={"0900": {"book": 5}, "0901": {"book": 2}},
    )
    _native(tmp_path, "0900", book_lines=["r4", "r5"])  # partial: 2 < 5 -> skipped
    _native(tmp_path, "0901", book_lines=["r1", "r2"])  # complete: 2 >= 2
    _native(tmp_path, "0902")  # no monolith count -> cannot dispute; offloadable
    assert ofl.offloadable_event_ids(tmp_path, meta) == ["0901", "0902"]


# --------------------------------------------------------------------------- #
# offload_one: tar -> upload -> verify -> marker -> delete
# --------------------------------------------------------------------------- #


def test_offload_one_uploads_marks_and_deletes(tmp_path):
    _native(tmp_path, "0900", book_lines=["r1", "r2", "r3"])
    be = FakeBackend()
    marker = ofl.offload_one(tmp_path, "0900", be, now_iso="2026-07-02T00:00:00Z")

    # object uploaded under matches/event-0900.tar.gz, with the canonical inner layout
    assert marker["object"] == "matches/event-0900.tar.gz"
    assert "matches/event-0900.tar.gz" in be.objects
    import io

    tf = tarfile.open(fileobj=io.BytesIO(be.objects["matches/event-0900.tar.gz"]), mode="r:gz")
    names = sorted(m.name for m in tf.getmembers())
    assert names == ["event-0900/book.jsonl", "event-0900/meta.json"]
    body = tf.extractfile("event-0900/book.jsonl").read().decode()
    assert body == "r1\nr2\nr3\n"  # byte-exact book

    # marker written and native dir removed
    assert ofl.is_offloaded(tmp_path / "matches", "0900")
    assert not (tmp_path / "matches" / "event-0900").exists()
    on_disk = json.loads((tmp_path / "matches" / "event-0900.offloaded.json").read_text())
    assert on_disk["gs_uri"] == "gs://fake/matches/event-0900.tar.gz"
    assert on_disk["schema"] == ofl.OFFLOAD_MARKER_SCHEMA


def test_offload_one_keeps_native_on_verify_mismatch(tmp_path, monkeypatch):
    _native(tmp_path, "0900")
    be = FakeBackend()
    # Make the remote report a wrong size -> verify fails AFTER upload, BEFORE marker/delete.
    monkeypatch.setattr(be, "stat", lambda name: {"size": 999999})
    with pytest.raises(OSError, match="verify failed"):
        ofl.offload_one(tmp_path, "0900", be, now_iso="t")
    assert (tmp_path / "matches" / "event-0900" / "book.jsonl").exists()  # native intact
    assert not ofl.is_offloaded(tmp_path / "matches", "0900")  # no marker written


def test_offload_one_keeps_native_on_upload_failure(tmp_path):
    _native(tmp_path, "0900")
    be = FakeBackend()
    be.fail_upload = True
    with pytest.raises(OSError):
        ofl.offload_one(tmp_path, "0900", be, now_iso="t")
    assert (tmp_path / "matches" / "event-0900").exists()
    assert not ofl.is_offloaded(tmp_path / "matches", "0900")


# --------------------------------------------------------------------------- #
# run_offload: batch + resilience
# --------------------------------------------------------------------------- #


def test_run_offload_batch_and_skips_failures(tmp_path):
    _write_meta(tmp_path, open_ids=["1001"])
    _native(tmp_path, "1001")  # open -> skipped
    _native(tmp_path, "0900")
    _native(tmp_path, "0901")
    be = FakeBackend()
    done = ofl.run_offload(tmp_path, be)
    assert sorted(done) == ["0900", "0901"]
    assert not (tmp_path / "matches" / "event-0900").exists()
    assert (tmp_path / "matches" / "event-1001").exists()  # untouched
    # idempotent: a second pass finds nothing new
    assert ofl.run_offload(tmp_path, be) == []


def test_run_offload_skips_partial_native(tmp_path):
    """End to end: a partial native is NOT uploaded/marked/deleted; the complete
    sibling still offloads normally."""
    _write_meta(tmp_path, open_ids=[], counts_by_event={"0900": {"book": 5}, "0901": {"book": 2}})
    _native(tmp_path, "0900", book_lines=["r4", "r5"])  # post-deploy tail only (2 of 5)
    _native(tmp_path, "0901", book_lines=["r1", "r2"])
    be = FakeBackend()
    assert ofl.run_offload(tmp_path, be) == ["0901"]
    # the partial match is untouched: native kept, no marker, nothing in GCS
    assert (tmp_path / "matches" / "event-0900" / "book.jsonl").exists()
    assert not ofl.is_offloaded(tmp_path / "matches", "0900")
    assert "matches/event-0900.tar.gz" not in be.objects


def test_run_offload_limit(tmp_path):
    _write_meta(tmp_path, open_ids=[])
    _native(tmp_path, "0900")
    _native(tmp_path, "0901")
    be = FakeBackend()
    done = ofl.run_offload(tmp_path, be, limit=1)
    assert len(done) == 1


# --------------------------------------------------------------------------- #
# signed_download_url
# --------------------------------------------------------------------------- #


def test_signed_download_url_for_offloaded_and_missing(tmp_path):
    _native(tmp_path, "0900")
    be = FakeBackend()
    ofl.offload_one(tmp_path, "0900", be, now_iso="t")
    url = ofl.signed_download_url(tmp_path / "matches", "0900", be, ttl_seconds=60)
    assert url == "https://signed.example/matches/event-0900.tar.gz?ttl=60"
    assert ofl.signed_download_url(tmp_path / "matches", "9999", be) is None  # not offloaded


# --------------------------------------------------------------------------- #
# Admin download route: offloaded single match -> 302 signed URL
# --------------------------------------------------------------------------- #


def test_download_offloaded_match_redirects(tmp_path, monkeypatch):
    pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient

    from polytape.admin import control
    from polytape.admin.app import create_app
    from polytape.admin.reader import RunReader

    # A run whose meta knows event 1001; its native dir has been offloaded (marker only).
    (tmp_path / "meta.json").write_text(
        json.dumps(
            {
                "run_name": "wc",
                "streams": ["book"],
                "counts_by_event": {"1001": {"book": 3}},
                "events": [{"id": "1001", "title": "A vs. B", "markets": [{"conditionId": "0xA"}]}],
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "matches").mkdir()
    ofl.marker_path(tmp_path / "matches", "1001").write_text(
        json.dumps(
            {
                "schema": 1,
                "event_id": "1001",
                "gs_uri": "gs://fake/matches/event-1001.tar.gz",
                "object": "matches/event-1001.tar.gz",
                "size": 10,
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(ofl, "GcsBackend", FakeBackend)

    reader = RunReader(tmp_path, env_file=tmp_path / "missing.env")
    app = create_app(
        reader,
        admin_token="secret",
        audit=control.AuditLog(tmp_path / "audit.jsonl"),
        sessions=control.Sessions(),
        gcs_bucket="fake-bucket",
    )
    client = TestClient(app)
    assert client.post("/api/login", json={"token": "secret"}).status_code == 200

    r = client.get("/api/download?event=1001", follow_redirects=False)
    assert r.status_code == 302
    assert r.headers["location"] == "https://signed.example/matches/event-1001.tar.gz?ttl=3600"
    # audited as served from the offload archive
    audit = (tmp_path / "audit.jsonl").read_text(encoding="utf-8").splitlines()
    served = [json.loads(x) for x in audit if json.loads(x).get("action") == "download"]
    assert served and served[-1]["served"] == "offload"


def test_matches_marks_offloaded(tmp_path):
    (tmp_path / "meta.json").write_text(
        json.dumps({"events": [], "counts_by_event": {"0900": {"book": 5}}}), encoding="utf-8"
    )
    (tmp_path / "matches").mkdir()
    ofl.marker_path(tmp_path / "matches", "0900").write_text(
        json.dumps({"gs_uri": "gs://x/y", "object": "matches/event-0900.tar.gz"}), encoding="utf-8"
    )
    from polytape.admin import registry as reg
    from polytape.admin.reader import RunReader

    reg.write_registry_atomic(
        tmp_path / "registry.json",
        [{"event_id": "0900", "title": "X", "date": "2026-06-19", "closed": True, "markets": []}],
        now_iso="2026-07-02T00:00:00Z",
    )
    r = RunReader(tmp_path, env_file=tmp_path / "m.env", registry_file=tmp_path / "registry.json")
    r.update()
    row = next(m for m in r.matches() if m["event_id"] == "0900")
    assert row["offloaded"] is True and row["downloadable"] is True
