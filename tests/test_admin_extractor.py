"""Tests for the background extractor (pre-built per-match archives) + the download
fast-path that serves them. Pure extractor tests need no fastapi; the fast-path test
builds the real app via TestClient (skipped without fastapi)."""

from __future__ import annotations

import io
import json
import tarfile

import pytest

from polytape.admin import extractor
from polytape.admin import registry as reg
from polytape.envelope import utc_now_iso


def _book(market: str, rid: str) -> dict:
    ts = utc_now_iso()
    return {
        "stream": "book",
        "id": rid,
        "ts_recv": ts,
        "ts_server": ts,
        "raw": {"event_type": "book", "market": market, "asset_id": market + "-Y"},
    }


def _write_jsonl(path, records):
    path.write_text("".join(json.dumps(r) + "\n" for r in records), encoding="utf-8")


def _members(raw: bytes) -> dict[str, bytes]:
    out: dict[str, bytes] = {}
    with tarfile.open(fileobj=io.BytesIO(raw), mode="r:gz") as tar:
        for m in tar.getmembers():
            if m.isfile():
                out[m.name] = tar.extractfile(m).read()
    return out


# finished event 0900 (conditionId 0xZ) with two book records; 0xQ belongs to no event
def _run(tmp_path):
    (tmp_path / "meta.json").write_text(
        json.dumps({"run_name": "wc", "counts_by_event": {}, "events": []}), encoding="utf-8"
    )
    _write_jsonl(
        tmp_path / "book.jsonl", [_book("0xZ", "z1"), _book("0xZ", "z2"), _book("0xQ", "q1")]
    )
    (tmp_path / "comments.jsonl").write_text("", encoding="utf-8")


_REG = {
    "0900": {
        "event_id": "0900",
        "title": "Z vs Y",
        "slug": "fifwc-z-y-2026-06-19",
        "date": "2026-06-19",
        "closed": True,
        "markets": [{"conditionId": "0xZ", "clobTokenIds": ["0xZ-Y"]}],
    }
}


# --------------------------------------------------------------------------- #
# Pure extractor
# --------------------------------------------------------------------------- #


def test_build_extracts_creates_archive_and_marker(tmp_path):
    _run(tmp_path)
    ed = tmp_path / "extracts"
    built = extractor.build_extracts(
        tmp_path, ed, ["0900"], registry=_REG, meta={"events": [], "counts_by_event": {}}
    )
    assert built == ["0900"]
    assert extractor.has_complete_extract(ed, "0900")
    members = _members((ed / "event-0900.tar.gz").read_bytes())
    ids = [json.loads(line)["id"] for line in members["event-0900/book.jsonl"].splitlines()]
    assert ids == ["z1", "z2"]  # only the finished match's records (not 0xQ)
    assert json.loads(members["event-0900/meta.json"])["event_id"] == "0900"
    marker = json.loads(extractor.marker_path(ed, "0900").read_text(encoding="utf-8"))
    assert marker["event_id"] == "0900" and marker["size"] > 0


def test_has_complete_extract_requires_marker(tmp_path):
    extractor.archive_path(tmp_path, "X").write_bytes(b"x")  # tarball but no marker
    assert not extractor.has_complete_extract(tmp_path, "X")


def test_has_complete_extract_rejects_unsafe_id(tmp_path):
    # A traversal/separator id can never point outside extract_dir (defense in depth).
    assert not extractor.valid_event_id("../escape")
    assert not extractor.valid_event_id("a/b")
    assert not extractor.has_complete_extract(tmp_path, "../escape")


def test_build_extracts_skips_unsafe_ids(tmp_path):
    _run(tmp_path)
    built = extractor.build_extracts(
        tmp_path, tmp_path / "extracts", ["../escape"], registry=_REG, meta={"events": []}
    )
    assert built == []  # unsafe id filtered out, nothing built (no crash)


def test_enforce_cap_counts_and_evicts_corrupt_marker(tmp_path):
    # A tarball whose marker is unparseable must still count toward the cap AND be evicted
    # first — otherwise a bad marker would let the cache grow unbounded past the cap.
    extractor.archive_path(tmp_path, "GOOD").write_bytes(b"x" * 100)
    extractor.marker_path(tmp_path, "GOOD").write_text(
        json.dumps({"event_id": "GOOD", "built_at": "2026-06-20T00:00:00Z", "size": 100}),
        encoding="utf-8",
    )
    extractor.archive_path(tmp_path, "BAD").write_bytes(b"x" * 100)
    extractor.marker_path(tmp_path, "BAD").write_text("not json{", encoding="utf-8")
    extractor.enforce_cap(tmp_path, cap_bytes=150)  # 200 total -> must drop one
    assert not extractor.archive_path(tmp_path, "BAD").exists()  # corrupt-marker tar evicted first
    assert extractor.has_complete_extract(tmp_path, "GOOD")


def test_enforce_cap_evicts_oldest(tmp_path):
    for eid, built in [("A", "2026-06-19T00:00:00Z"), ("B", "2026-06-20T00:00:00Z")]:
        extractor.archive_path(tmp_path, eid).write_bytes(b"x" * 100)
        extractor.marker_path(tmp_path, eid).write_text(
            json.dumps({"event_id": eid, "built_at": built, "size": 100}), encoding="utf-8"
        )
    extractor.enforce_cap(tmp_path, cap_bytes=150)  # must drop one to get under 150
    assert not extractor.has_complete_extract(tmp_path, "A")  # oldest evicted
    assert extractor.has_complete_extract(tmp_path, "B")


# --------------------------------------------------------------------------- #
# Download fast-path (serves the pre-built extract, not a fresh scan)
# --------------------------------------------------------------------------- #


def test_download_serves_prebuilt_extract(tmp_path):
    pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient

    from polytape.admin import control
    from polytape.admin.app import create_app
    from polytape.admin.reader import RunReader

    _run(tmp_path)
    ed = tmp_path / "extracts"
    ed.mkdir()
    # A SENTINEL archive so we can prove it's served verbatim (not rebuilt by a scan).
    extractor.archive_path(ed, "0900").write_bytes(b"SENTINEL-TARGZ-BYTES")
    extractor.marker_path(ed, "0900").write_text(
        json.dumps({"event_id": "0900", "built_at": "t", "size": 20}), encoding="utf-8"
    )
    reg.write_registry_atomic(
        tmp_path / "registry.json",
        [
            {
                "event_id": "0900",
                "title": "Brazil vs. Argentina",
                "date": "2026-06-19",
                "closed": True,
                "markets": [{"conditionId": "0xZ", "clobTokenIds": []}],
            }
        ],
        now_iso="t",
    )
    reader = RunReader(
        tmp_path,
        env_file=tmp_path / "x.env",
        matches_file=tmp_path / "x.json",
        registry_file=tmp_path / "registry.json",
    )
    reader.update()
    app = create_app(
        reader,
        admin_token="secret",
        extract_dir=ed,
        registry_refresh_s=0,  # no Gamma / extractor loop in the test
        extract_refresh_s=0,
        audit=control.AuditLog(tmp_path / "audit.jsonl"),
        sessions=control.Sessions(),
    )
    client = TestClient(app)
    assert client.post("/api/login", json={"token": "secret"}).status_code == 200
    r = client.get("/api/download?event=0900")
    assert r.status_code == 200
    assert r.content == b"SENTINEL-TARGZ-BYTES"  # served the pre-built extract, no scan
    # Filename carries the event id + FIFA codes of both sides (Brazil vs. Argentina).
    assert "event-0900-BRA-ARG.tar.gz" in r.headers.get("content-disposition", "")
