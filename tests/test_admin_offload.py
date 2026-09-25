"""Tests for offloading finished matches to Cloud Storage (polytape/admin/offload.py).

The GCS backend is faked, so nothing here touches the network or needs
``google-cloud-storage`` (the real client is imported lazily inside ``GcsBackend``).

What "finished" means on a set that is re-discovered every 10 minutes is the heart of
it: quiet time + out of the recorder's open set + out of the installed event set, both
sets re-read before each match is touched, a match that re-entered the set archived as
its next part, the marker durable before the delete, crc32c verified against the local
checksum, and no staging on a volume without headroom.
"""

from __future__ import annotations

import base64
import collections
import io
import json
import logging
import os
import sys
import tarfile
import time
from pathlib import Path

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
        self.on_upload = None  # callback(name), run while the upload is "in flight"
        self.local_crc = "c"  # what upload() reports: the LOCAL checksum
        self.remote_crc = "c"  # what stat() reports: what GCS stored

    def upload(self, local_path, name):
        if self.fail_upload:
            raise OSError("simulated upload failure")
        if self.on_upload is not None:
            self.on_upload(name)
        data = Path(local_path).read_bytes()
        self.objects[name] = data
        return {
            "gs_uri": f"gs://fake/{name}",
            "size": len(data),
            "crc32c": self.local_crc,
            "storage_class": "COLDLINE",
        }

    def stat(self, name):
        d = self.objects.get(name)
        return None if d is None else {"size": len(d), "crc32c": self.remote_crc}

    def signed_url(self, name, ttl_seconds):
        return f"https://signed.example/{name}?ttl={ttl_seconds}"


def _age(path: Path, seconds: float) -> None:
    t = time.time() - seconds
    os.utime(path, (t, t))


def _native(run_dir, event_id, *, book_lines=("a", "b"), meta=None, age_s=3600.0):
    """A per-match native dir whose ``book.jsonl`` has been quiet for ``age_s`` (an hour
    by default — comfortably past the 30-minute quiet time, i.e. finished)."""
    d = run_dir / "matches" / f"event-{event_id}"
    d.mkdir(parents=True)
    # newline="" so \n is written verbatim (match the recorder; avoid Windows \r\n).
    (d / "book.jsonl").write_text(
        "".join(line + "\n" for line in book_lines), encoding="utf-8", newline=""
    )
    (d / "meta.json").write_text(json.dumps(meta or {"event_id": event_id}), encoding="utf-8")
    _age(d / "book.jsonl", age_s)
    return d


def _book(run_dir, event_id) -> Path:
    return run_dir / "matches" / f"event-{event_id}" / "book.jsonl"


def _write_meta(run_dir, open_ids):
    (run_dir / "meta.json").write_text(
        json.dumps({"events": [{"id": e} for e in open_ids]}), encoding="utf-8"
    )


def _events_file(path: Path, ids, *, closed=()) -> Path:
    """An installed campaign events file naming ``ids`` (those in ``closed`` as closed)."""
    path.write_text(
        json.dumps([{"event_id": i, "closed": i in closed, "record_markets": []} for i in ids]),
        encoding="utf-8",
    )
    return path


@pytest.fixture(autouse=True)
def _no_headroom(monkeypatch):
    """tmp_path sits on whatever disk CI has; the headroom rule gets its own test."""
    monkeypatch.setattr(ofl, "DEFAULT_HEADROOM_BYTES", 0)


# --------------------------------------------------------------------------- #
# Selection: quiet time, the recorder's open set, the installed event set
# --------------------------------------------------------------------------- #


def test_offloadable_excludes_open_fresh_and_protected_matches(tmp_path):
    _write_meta(tmp_path, open_ids=["1001"])  # 1001 is still recording
    _native(tmp_path, "1001")  # open -> never offload, however quiet
    _native(tmp_path, "0900")  # finished (quiet for an hour) -> offloadable
    _native(tmp_path, "0902", age_s=0)  # absent from the set but written just now -> NOT finished
    _native(tmp_path, "0903")  # quiet, but the installed set still names it -> protected
    meta = {"events": [{"id": "1001"}]}
    assert ofl.offloadable_event_ids(tmp_path, meta, protected_ids={"0903"}) == ["0900"]
    # the quiet time is a knob (0 = the old "absent from meta.events" rule)
    assert ofl.offloadable_event_ids(tmp_path, meta, min_age_s=0, protected_ids={"0903"}) == [
        "0900",
        "0902",
    ]
    # ...and is measured against an injectable clock
    later = time.time() + 3600
    assert ofl.offloadable_event_ids(tmp_path, meta, now_s=later) == ["0900", "0902", "0903"]
    assert ofl.DEFAULT_MATCH_MIN_AGE_S >= 1800


def test_offloadable_none_when_no_matches_dir(tmp_path):
    assert ofl.offloadable_event_ids(tmp_path, {"events": []}) == []


def test_events_file_ids_reads_the_installed_open_set(tmp_path, caplog):
    path = tmp_path / "campaign_events.json"
    path.write_text(
        json.dumps(
            [
                {"event_id": "1", "closed": False},
                {"event_id": "2", "closed": True},
                {"id": " 3 "},  # the recorder's loader accepts `id` too
                {"event_id": ""},
                "junk",
                None,
            ]
        ),
        encoding="utf-8",
    )
    assert ofl.events_file_ids(path) == {"1", "3"}
    assert ofl.events_file_ids(None) == set()
    with caplog.at_level(logging.WARNING, logger="polytape.admin.offload"):
        assert ofl.events_file_ids(tmp_path / "missing.json") == set()
        (tmp_path / "obj.json").write_text("{}", encoding="utf-8")
        assert ofl.events_file_ids(tmp_path / "obj.json") == set()
    assert caplog.text.count("guard is off") == 2


def test_leftover_native_after_a_failed_delete_is_left_alone(tmp_path, monkeypatch, caplog):
    # The marker was published and the delete failed: the native still has exactly the
    # size + mtime the marker recorded -> not re-archived, a human is warned.
    _native(tmp_path, "0900")
    _write_meta(tmp_path, open_ids=[])
    real_rmtree = ofl.shutil.rmtree
    monkeypatch.setattr(
        ofl.shutil,
        "rmtree",
        lambda p, **k: None if Path(p).name == "event-0900" else real_rmtree(p, **k),
    )
    ofl.offload_one(tmp_path, "0900", FakeBackend(), now_iso="t")
    monkeypatch.undo()
    assert ofl.is_offloaded(tmp_path / "matches", "0900") and _book(tmp_path, "0900").exists()
    with caplog.at_level(logging.WARNING, logger="polytape.admin.offload"):
        assert ofl.offloadable_event_ids(tmp_path, {"events": []}) == []
    assert "still on disk" in caplog.text
    # ...but once the native CHANGED (the match came back), it is a candidate again
    with open(_book(tmp_path, "0900"), "a", encoding="utf-8", newline="") as fh:
        fh.write("more\n")
    _age(_book(tmp_path, "0900"), 3600)
    assert ofl.offloadable_event_ids(tmp_path, {"events": []}) == ["0900"]
    # A schema-1 marker (no native signature) beside a native reads as one archived part;
    # the native is a candidate for part 2.
    ofl.marker_path(tmp_path / "matches", "0900").write_text(
        json.dumps({"gs_uri": "gs://x/y", "object": "matches/event-0900.tar.gz"}), encoding="utf-8"
    )
    assert ofl.marker_parts(ofl.read_marker(tmp_path / "matches", "0900")) == [
        {
            "part": 1,
            "object": "matches/event-0900.tar.gz",
            "gs_uri": "gs://x/y",
            "size": None,
            "crc32c": None,
            "offloaded_at": None,
        }
    ]
    assert ofl.offloadable_event_ids(tmp_path, {"events": []}) == ["0900"]
    assert ofl.marker_parts(None) == [] and ofl.marker_parts({"schema": 2}) == []


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
    assert on_disk["schema"] == ofl.OFFLOAD_MARKER_SCHEMA == 2
    assert on_disk["crc32c"] == "c"
    (part,) = on_disk["parts"]
    assert part["part"] == 1 and part["object"] == on_disk["object"]
    assert part["native_size"] == len(b"r1\nr2\nr3\n") and isinstance(part["native_mtime_ns"], int)
    assert not (tmp_path / "matches" / "event-0900.offloaded.json.partial").exists()


def test_offload_one_keeps_native_on_verify_mismatch(tmp_path, monkeypatch):
    _native(tmp_path, "0900")
    be = FakeBackend()
    # Make the remote report a wrong size -> verify fails AFTER upload, BEFORE marker/delete.
    monkeypatch.setattr(be, "stat", lambda name: {"size": 999999})
    with pytest.raises(OSError, match="verify failed"):
        ofl.offload_one(tmp_path, "0900", be, now_iso="t")
    assert _book(tmp_path, "0900").exists()  # native intact
    assert not ofl.is_offloaded(tmp_path / "matches", "0900")  # no marker written


def test_offload_one_verifies_the_local_crc32c_against_the_stored_one(tmp_path):
    # The check is local bytes vs what GCS stored — never the server against itself.
    _native(tmp_path, "0900")
    be = FakeBackend()
    be.local_crc, be.remote_crc = "AAAAAA==", "BBBBBB=="
    with pytest.raises(OSError, match="crc32c mismatch"):
        ofl.offload_one(tmp_path, "0900", be, now_iso="t")
    assert _book(tmp_path, "0900").exists()
    assert not ofl.is_offloaded(tmp_path / "matches", "0900")
    # An unknown local checksum (google-crc32c missing) degrades to the size check and
    # the marker records what GCS stored.
    be.local_crc = None
    marker = ofl.offload_one(tmp_path, "0900", be, now_iso="t")
    assert marker["crc32c"] == "BBBBBB=="


def test_offload_one_keeps_native_on_upload_failure(tmp_path):
    _native(tmp_path, "0900")
    be = FakeBackend()
    be.fail_upload = True
    with pytest.raises(OSError):
        ofl.offload_one(tmp_path, "0900", be, now_iso="t")
    assert (tmp_path / "matches" / "event-0900").exists()
    assert not ofl.is_offloaded(tmp_path / "matches", "0900")


def test_offload_one_refuses_to_delete_a_native_that_changed_during_the_upload(tmp_path):
    # The recorder came back for this match while its tar was in flight: the archive
    # would be partial and the delete would pull the file from under a live handle.
    _native(tmp_path, "0900", book_lines=["pre"])
    be = FakeBackend()

    def recorder_appends(name):
        with open(_book(tmp_path, "0900"), "a", encoding="utf-8", newline="") as fh:
            fh.write("live\n")

    be.on_upload = recorder_appends
    with pytest.raises(OSError, match="changed during the offload"):
        ofl.offload_one(tmp_path, "0900", be, now_iso="t")
    assert _book(tmp_path, "0900").read_text(encoding="utf-8") == "pre\nlive\n"  # intact
    assert not ofl.is_offloaded(tmp_path / "matches", "0900")  # no marker
    # Next run (quiet again): the whole file is archived.
    be.on_upload = None
    _age(_book(tmp_path, "0900"), 3600)
    ofl.offload_one(tmp_path, "0900", be, now_iso="t")
    tf = tarfile.open(fileobj=io.BytesIO(be.objects["matches/event-0900.tar.gz"]), mode="r:gz")
    assert tf.extractfile("event-0900/book.jsonl").read() == b"pre\nlive\n"


def test_reentered_match_is_archived_as_the_next_part(tmp_path, caplog):
    # Archived once (pre-game native), then the event re-entered the open set
    # (postponement / a restart that failed to resolve it): the recorder re-creates the
    # native and records the game. That native must be archived too — loudly, as part 2.
    _native(tmp_path, "0900", book_lines=["pre1", "pre2"])
    be = FakeBackend()
    ofl.offload_one(tmp_path, "0900", be, now_iso="t1")
    assert not (tmp_path / "matches" / "event-0900").exists()
    _native(tmp_path, "0900", book_lines=["g1", "g2", "g3"])
    _write_meta(tmp_path, open_ids=[])
    with caplog.at_level(logging.WARNING, logger="polytape.admin.offload"):
        assert ofl.run_offload(tmp_path, be) == ["0900"]
    assert "re-entered the open set after part 1" in caplog.text
    assert sorted(be.objects) == ["matches/event-0900.part2.tar.gz", "matches/event-0900.tar.gz"]
    tf = tarfile.open(
        fileobj=io.BytesIO(be.objects["matches/event-0900.part2.tar.gz"]), mode="r:gz"
    )
    assert tf.extractfile("event-0900/book.jsonl").read() == b"g1\ng2\ng3\n"
    assert not (tmp_path / "matches" / "event-0900").exists()  # reclaimed again
    marker = ofl.read_marker(tmp_path / "matches", "0900")
    assert marker["object"] == "matches/event-0900.tar.gz"  # part 1 stays the canonical object
    assert [p["part"] for p in marker["parts"]] == [1, 2]
    assert marker["parts"][1]["object"] == "matches/event-0900.part2.tar.gz"
    assert marker["parts"][1]["native_size"] == len(b"g1\ng2\ng3\n")
    assert marker["parts"][1]["offloaded_at"] != "t1"
    # the signed-URL path still serves part 1; a third native becomes part 3
    url = ofl.signed_download_url(tmp_path / "matches", "0900", be, ttl_seconds=60)
    assert url == "https://signed.example/matches/event-0900.tar.gz?ttl=60"
    _native(tmp_path, "0900", book_lines=["late"])
    ofl.offload_one(tmp_path, "0900", be, now_iso="t3")
    assert "matches/event-0900.part3.tar.gz" in be.objects
    assert len(ofl.marker_parts(ofl.read_marker(tmp_path / "matches", "0900"))) == 3
    assert ofl.object_name("p", "1") == "p/event-1.tar.gz"
    assert ofl.object_name("p/", "1", 2) == "p/event-1.part2.tar.gz"


def test_marker_is_fsynced_and_published_before_the_delete(tmp_path, monkeypatch):
    # Order on the way out: fsync the marker, rename it into place, only then rmtree —
    # a hard reset right after the delete cannot leave an empty marker behind.
    _native(tmp_path, "0900")
    events: list[str] = []
    real_fsync, real_replace, real_rmtree = os.fsync, os.replace, ofl.shutil.rmtree

    def fsync(fd):
        events.append("fsync")
        return real_fsync(fd)

    def replace(src, dst):
        events.append("replace")
        return real_replace(src, dst)

    def rmtree(path, **kw):
        if Path(path).name == "event-0900":
            events.append("rmtree")
        return real_rmtree(path, **kw)

    monkeypatch.setattr(ofl.os, "fsync", fsync)
    monkeypatch.setattr(ofl.os, "replace", replace)
    monkeypatch.setattr(ofl.shutil, "rmtree", rmtree)
    ofl.offload_one(tmp_path, "0900", FakeBackend(), now_iso="t")
    assert events.index("fsync") < events.index("replace") < events.index("rmtree")
    assert not (tmp_path / "matches" / "event-0900.offloaded.json.partial").exists()
    assert ofl.is_offloaded(tmp_path / "matches", "0900")


def test_headroom_check_refuses_to_stage_on_a_tight_volume(tmp_path, monkeypatch, caplog):
    # The scratch sits on the run volume: staging into ENOSPC would take the recorder
    # down before anything is freed, so a tight volume skips the archive with a warning.
    _native(tmp_path, "0900", book_lines=["x" * 100] * 10)  # 1010 bytes -> ~252 staging
    _write_meta(tmp_path, open_ids=[])
    be = FakeBackend()
    usage = collections.namedtuple("usage", "total used free")
    monkeypatch.setattr(ofl.shutil, "disk_usage", lambda p: usage(100, 90, 10))
    with pytest.raises(OSError, match="not enough free space"):
        ofl.offload_one(tmp_path, "0900", be, now_iso="t", headroom_bytes=1000)
    with pytest.raises(OSError, match="not enough free space"):
        ofl.offload_one(tmp_path, "0900", be, now_iso="t", headroom_bytes=0)  # staging alone
    assert be.objects == {} and _book(tmp_path, "0900").exists()
    with caplog.at_level(logging.WARNING, logger="polytape.admin.offload"):
        assert ofl.run_offload(tmp_path, be, headroom_bytes=1000) == []
    assert "not enough free space" in caplog.text
    # plenty of room -> proceeds; a volume that cannot be stat'ed never blocks the sweep
    monkeypatch.setattr(ofl.shutil, "disk_usage", lambda p: usage(10**12, 0, 10**12))
    assert ofl.run_offload(tmp_path, be, headroom_bytes=1000) == ["0900"]
    _native(tmp_path, "0901")

    def boom(p):
        raise OSError("no statvfs")

    monkeypatch.setattr(ofl.shutil, "disk_usage", boom)
    ofl.offload_one(tmp_path, "0901", be, now_iso="t", headroom_bytes=10**12)
    assert ofl.is_offloaded(tmp_path / "matches", "0901")


def test_local_crc32c_falls_back_to_size_only_without_the_library(tmp_path, monkeypatch, caplog):
    path = tmp_path / "blob.bin"
    path.write_bytes(b"hello polytape\n")
    monkeypatch.setitem(sys.modules, "google_crc32c", None)  # import fails
    with caplog.at_level(logging.WARNING, logger="polytape.admin.offload"):
        assert ofl.local_crc32c(path) is None
    assert "size-only" in caplog.text


def test_local_crc32c_matches_the_gcs_encoding(tmp_path):
    crc = pytest.importorskip("google_crc32c")  # a transitive dep of google-cloud-storage
    path = tmp_path / "blob.bin"
    path.write_bytes(b"hello polytape\n" * 1000)
    expected = base64.b64encode(crc.value(path.read_bytes()).to_bytes(4, "big")).decode("ascii")
    assert ofl.local_crc32c(path, chunk=7) == expected  # streamed, in GCS's base64 form


# --------------------------------------------------------------------------- #
# run_offload: batch + resilience + re-checks before each match
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


def test_run_offload_limit(tmp_path):
    _write_meta(tmp_path, open_ids=[])
    _native(tmp_path, "0900")
    _native(tmp_path, "0901")
    be = FakeBackend()
    done = ofl.run_offload(tmp_path, be, limit=1)
    assert len(done) == 1


def test_run_offload_respects_the_installed_event_set(tmp_path):
    # A process that failed to resolve 0900 once has it missing from meta.events, but
    # the installed file still says "record it" -> not finished.
    _write_meta(tmp_path, open_ids=[])
    _native(tmp_path, "0900")
    events = _events_file(tmp_path / "campaign_events.json", ["0900"])
    be = FakeBackend()
    assert ofl.run_offload(tmp_path, be, events_file=events) == []
    assert _book(tmp_path, "0900").exists()
    _events_file(events, ["0900"], closed=["0900"])  # rolled out (closed) -> finished
    assert ofl.run_offload(tmp_path, be, events_file=events) == ["0900"]


def test_run_offload_rechecks_both_open_sets_before_each_match(tmp_path):
    # Candidates are computed once, but a pass runs one upload per match and the world
    # moves meanwhile: the recorder restarted with 2 open again and the refresh put 3
    # back into the installed set while 1 was uploading -> neither is touched.
    _write_meta(tmp_path, open_ids=[])
    for eid in ("1", "2", "3"):
        _native(tmp_path, eid)
    events = _events_file(tmp_path / "campaign_events.json", [])
    be = FakeBackend()

    def world_moves(name):
        if name.endswith("event-1.tar.gz"):
            _write_meta(tmp_path, open_ids=["2"])
            _events_file(events, ["3"])

    be.on_upload = world_moves
    assert ofl.run_offload(tmp_path, be, events_file=events) == ["1"]
    assert _book(tmp_path, "2").exists() and _book(tmp_path, "3").exists()
    assert sorted(be.objects) == ["matches/event-1.tar.gz"]


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
