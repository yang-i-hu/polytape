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


# --------------------------------------------------------------------------- #
# Combined multi-match download (stitched from cached extracts; no run scan)
# --------------------------------------------------------------------------- #

# Two events: 0900 (cond 0xZ, two book records) and 0901 (cond 0xW, one).
_REG2 = [
    {
        "event_id": "0900",
        "title": "Z vs. Y",
        "date": "2026-06-19",
        "closed": True,
        "markets": [{"conditionId": "0xZ", "clobTokenIds": ["0xZ-Y"]}],
    },
    {
        "event_id": "0901",
        "title": "W vs. V",
        "date": "2026-06-20",
        "closed": True,
        "markets": [{"conditionId": "0xW", "clobTokenIds": ["0xW-Y"]}],
    },
]
_REG2_BY_ID = {e["event_id"]: e for e in _REG2}


def _run2(tmp_path, *, open_events=()):
    """A combined run with two matches' book records. Events named in ``open_events`` go
    into meta.events (still OPEN); the rest are finished (registry-only, immutable)."""
    events = [
        {"id": e["event_id"], "title": e["title"], "markets": e["markets"]}
        for e in _REG2
        if e["event_id"] in open_events
    ]
    meta = {"run_name": "wc", "counts_by_event": {}, "events": events}
    (tmp_path / "meta.json").write_text(json.dumps(meta), encoding="utf-8")
    _write_jsonl(
        tmp_path / "book.jsonl",
        [_book("0xZ", "z1"), _book("0xZ", "z2"), _book("0xW", "w1")],
    )
    (tmp_path / "comments.jsonl").write_text("", encoding="utf-8")
    reg.write_registry_atomic(tmp_path / "registry.json", _REG2, now_iso="t")


def _login_client(tmp_path, ed, *, extract_refresh_s=0):
    from fastapi.testclient import TestClient

    from polytape.admin import control
    from polytape.admin.app import create_app
    from polytape.admin.reader import RunReader

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
        registry_refresh_s=0,  # no Gamma / background extractor loop in the test
        extract_refresh_s=extract_refresh_s,
        audit=control.AuditLog(tmp_path / "audit.jsonl"),
        sessions=control.Sessions(),
    )
    client = TestClient(app)
    assert client.post("/api/login", json={"token": "secret"}).status_code == 200
    return client


def _last_download(tmp_path):
    lines = (tmp_path / "audit.jsonl").read_text(encoding="utf-8").splitlines()
    rows = [json.loads(x) for x in lines if json.loads(x).get("action") == "download"]
    return rows[-1]


def test_stream_combined_targz_merges_extracts(tmp_path):
    _run2(tmp_path)
    ed = tmp_path / "extracts"
    extractor.build_extracts(
        tmp_path, ed, ["0900", "0901"], registry=_REG2_BY_ID, meta={"events": []}
    )
    handles = [
        open(extractor.archive_path(ed, "0900"), "rb"),
        open(extractor.archive_path(ed, "0901"), "rb"),
    ]
    done = []
    raw = b"".join(extractor.stream_combined_targz(handles, on_done=lambda: done.append(True)))
    members = _members(raw)
    z = [json.loads(x)["id"] for x in members["event-0900/book.jsonl"].splitlines()]
    w = [json.loads(x)["id"] for x in members["event-0901/book.jsonl"].splitlines()]
    assert z == ["z1", "z2"] and w == ["w1"]  # both matches' records, verbatim
    assert "event-0900/meta.json" in members and "event-0901/meta.json" in members
    assert done == [True]  # cleanup ran after the stream drained
    assert all(fh.closed for fh in handles)  # every handle closed by the streamer


def test_download_multi_select_served_from_extracts(tmp_path):
    pytest.importorskip("fastapi")
    _run2(tmp_path)
    ed = tmp_path / "extracts"
    extractor.build_extracts(
        tmp_path, ed, ["0900", "0901"], registry=_REG2_BY_ID, meta={"events": []}
    )
    client = _login_client(tmp_path, ed)
    r = client.get("/api/download?event=0900&event=0901")
    assert r.status_code == 200
    assert r.headers["content-type"] == "application/gzip"
    assert 'filename="polytape-2-matches.tar.gz"' in r.headers["content-disposition"]
    members = _members(r.content)
    assert "event-0900/book.jsonl" in members and "event-0901/book.jsonl" in members
    audit = _last_download(tmp_path)
    assert audit["result"] == "ok" and audit.get("served") == "extract"


def test_download_multi_select_builds_missing_on_demand(tmp_path):
    pytest.importorskip("fastapi")
    _run2(tmp_path)
    ed = tmp_path / "extracts"
    ed.mkdir()
    assert not extractor.has_complete_extract(ed, "0900")  # nothing pre-built
    client = _login_client(tmp_path, ed)
    r = client.get("/api/download?event=0900&event=0901")
    assert r.status_code == 200
    members = _members(r.content)
    assert "event-0900/book.jsonl" in members and "event-0901/book.jsonl" in members
    # built on demand AND left cached, so the NEXT download of either is scan-free
    assert extractor.has_complete_extract(ed, "0900")
    assert extractor.has_complete_extract(ed, "0901")
    assert _last_download(tmp_path).get("served") == "extract"


def test_download_multi_select_with_open_match_falls_back_to_scan(tmp_path):
    pytest.importorskip("fastapi")
    _run2(tmp_path, open_events=("0901",))  # 0901 still recording -> not cacheable
    ed = tmp_path / "extracts"
    ed.mkdir()
    extractor.build_extracts(  # 0900 pre-built; 0901 is open so it has no extract
        tmp_path, ed, ["0900"], registry=_REG2_BY_ID, meta={"events": []}
    )
    client = _login_client(tmp_path, ed)
    r = client.get("/api/download?event=0900&event=0901")
    assert r.status_code == 200
    members = _members(r.content)
    # the full-run scan still returns BOTH matches...
    assert "event-0900/book.jsonl" in members and "event-0901/book.jsonl" in members
    # ...but it was NOT served from the cache (an open match can't be pre-built).
    assert _last_download(tmp_path).get("served") != "extract"
