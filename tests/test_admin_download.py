"""Tests for the admin match-data download: filtering, archiving, and the HTTP gate.

The pure filtering/archive tests need no fastapi. The endpoint tests build the
real FastAPI app via a TestClient and are skipped if fastapi is unavailable.
"""

from __future__ import annotations

import io
import json
import tarfile
import tempfile
from pathlib import Path

import pytest

from polytape.admin import download as dl
from polytape.envelope import utc_now_iso

# --------------------------------------------------------------------------- #
# Fixtures: a small combined multi-event run
# --------------------------------------------------------------------------- #


def _meta() -> dict:
    def mk(c):
        return {"id": "m", "conditionId": c, "clobTokenIds": [c + "-Y", c + "-N"]}

    return {
        "polytape_version": "0.1.0",
        "run_name": "wc",
        "streams": ["comments", "book"],
        "holdings_captured": True,
        "hashing": {"enabled": True, "salt_fingerprint": "abcd1234"},
        "started_at": "2026-06-19T16:20:00.000000Z",
        "stopped_at": None,
        "counts": {"book": 4, "comments": 5},
        "counts_by_event": {"1001": {"book": 2, "comments": 2}, "1002": {"book": 1, "comments": 2}},
        "events": [
            {
                "id": "1001",
                "title": "A vs. B",
                "slug": "fifwc-a-b-2026-06-19",
                "markets": [mk("0xA1"), mk("0xA2")],
            },
            {
                "id": "1002",
                "title": "C vs. D",
                "slug": "fifwc-c-d-2026-06-20",
                "markets": [mk("0xB1")],
            },
        ],
    }


def _book(market: str, rid: str) -> dict:
    ts = utc_now_iso()
    return {
        "stream": "book",
        "id": rid,
        "ts_recv": ts,
        "ts_server": ts,
        "raw": {"event_type": "book", "market": market, "asset_id": market + "-Y"},
    }


def _comment(parent: int, cid: str) -> dict:
    ts = utc_now_iso()
    return {
        "stream": "comments",
        "id": cid,
        "ts_recv": ts,
        "ts_server": ts,
        "raw": {"type": "comment_created", "payload": {"id": cid, "parentEntityID": parent}},
    }


def _reaction(comment_id: str, rid: str) -> dict:
    ts = utc_now_iso()
    # A reaction carries no parentEntityID; it references the comment it reacts to,
    # so it must be attributed via the commentID -> event map.
    return {
        "stream": "comments",
        "id": rid,
        "ts_recv": ts,
        "ts_server": ts,
        "raw": {"type": "reaction_created", "payload": {"id": rid, "commentID": comment_id}},
    }


def _write_jsonl(path, records):
    path.write_text("".join(json.dumps(r) + "\n" for r in records), encoding="utf-8")


def _setup_run(run_dir):
    (run_dir / "meta.json").write_text(json.dumps(_meta()), encoding="utf-8")
    _write_jsonl(
        run_dir / "book.jsonl",
        [
            _book("0xA1", "b1"),  # -> event 1001
            _book("0xA2", "b2"),  # -> event 1001
            _book("0xB1", "b3"),  # -> event 1002
            _book("0xZZ", "b4"),  # -> unknown market, attributed to no event
        ],
    )
    _write_jsonl(
        run_dir / "comments.jsonl",
        [
            _comment(1001, "c1"),  # -> event 1001
            _reaction("c1", "r1"),  # reaction to c1 -> event 1001 (via commentID map)
            _comment(1002, "c2"),  # -> event 1002
            _reaction("c2", "r2"),  # reaction to c2 -> event 1002
            _reaction(
                "cX", "r3"
            ),  # reaction to an unseen comment -> dropped (as the recorder drops it)
        ],
    )


def _members(raw: bytes) -> dict[str, bytes]:
    out: dict[str, bytes] = {}
    with tarfile.open(fileobj=io.BytesIO(raw), mode="r:gz") as tar:
        for m in tar.getmembers():
            if m.isfile():
                out[m.name] = tar.extractfile(m).read()
    return out


def _ids(path) -> list[str]:
    return [json.loads(line)["id"] for line in path.read_text(encoding="utf-8").splitlines()]


# --------------------------------------------------------------------------- #
# Pure: filtering + attribution + archiving
# --------------------------------------------------------------------------- #


def test_filter_run_attributes_records_to_the_right_event(tmp_path):
    _setup_run(tmp_path)
    dest = tmp_path / "out"
    entries = dl.filter_run(tmp_path, ["1001"], dest, exported_at="2026-06-21T00:00:00Z")
    names = {arc for arc, _ in entries}
    assert "event-1001/book.jsonl" in names
    assert "event-1001/comments.jsonl" in names
    assert "event-1001/meta.json" in names
    assert not any(arc.startswith("event-1002/") for arc in names)  # only 1001 selected

    assert _ids(dest / "event-1001/book.jsonl") == ["b1", "b2"]  # not b3 (1002)/b4 (unknown)
    assert _ids(dest / "event-1001/comments.jsonl") == ["c1", "r1"]  # comment + its reaction


def test_filter_run_includes_reactions_via_comment_map(tmp_path):
    # Reactions carry no parentEntityID; they must be attributed by commentID, so a
    # per-match slice keeps the match's reactions (not just its top-level comments).
    _setup_run(tmp_path)
    dl.filter_run(tmp_path, ["1001"], tmp_path / "a", exported_at="2026-06-21T00:00:00Z")
    dl.filter_run(tmp_path, ["1002"], tmp_path / "b", exported_at="2026-06-21T00:00:00Z")
    assert _ids(tmp_path / "a/event-1001/comments.jsonl") == ["c1", "r1"]
    assert _ids(tmp_path / "b/event-1002/comments.jsonl") == [
        "c2",
        "r2",
    ]  # r3 (orphan) never appears


def test_filter_run_multi_select_single_pass(tmp_path):
    _setup_run(tmp_path)
    dest = tmp_path / "out"
    dl.filter_run(tmp_path, ["1001", "1002"], dest, exported_at="2026-06-21T00:00:00Z")
    assert _ids(dest / "event-1002/book.jsonl") == ["b3"]
    assert _ids(dest / "event-1001/book.jsonl") == ["b1", "b2"]


def test_filter_run_is_byte_exact(tmp_path):
    _setup_run(tmp_path)
    original = (tmp_path / "book.jsonl").read_bytes().splitlines()  # b1,b2,b3,b4
    dest = tmp_path / "out"
    dl.filter_run(tmp_path, ["1001"], dest, exported_at="2026-06-21T00:00:00Z")
    filtered = (dest / "event-1001/book.jsonl").read_bytes().splitlines()
    assert filtered == original[:2]  # exact bytes of the b1, b2 lines


def test_filter_run_drops_partial_trailing_line(tmp_path):
    _setup_run(tmp_path)
    with open(tmp_path / "book.jsonl", "a", encoding="utf-8") as fh:
        fh.write(json.dumps(_book("0xA1", "b5_partial")))  # no trailing newline
    dest = tmp_path / "out"
    dl.filter_run(tmp_path, ["1001"], dest, exported_at="2026-06-21T00:00:00Z")
    assert _ids(dest / "event-1001/book.jsonl") == ["b1", "b2"]  # half-written line not consumed


def test_per_event_meta_slice(tmp_path):
    meta = _meta()
    sliced = dl.per_event_meta(meta, "1001", exported_at="2026-06-21T00:00:00Z")
    assert sliced["event_id"] == "1001"
    assert sliced["counts"] == {"book": 2, "comments": 2}  # per-event counts, not run totals
    assert sliced["market_ids"] == ["0xA1", "0xA2"]
    assert sliced["hashing"] == {"enabled": True, "salt_fingerprint": "abcd1234"}  # whitelisted
    assert sliced["event"]["title"] == "A vs. B"
    assert sliced["source"]["kind"] == "filtered-slice"


def test_whole_run_entries(tmp_path):
    _setup_run(tmp_path)
    entries = dl.whole_run_entries(tmp_path)
    root = tmp_path.name
    assert {arc for arc, _ in entries} == {
        f"{root}/meta.json",
        f"{root}/book.jsonl",
        f"{root}/comments.jsonl",
    }
    # Whole-run streams the combined files VERBATIM: every entry points straight at the
    # run dir, so no filtered copy is made and this path never needs (or overflows)
    # scratch — only the per-match filter path does.
    assert all(Path(p).parent == tmp_path for _, p in entries)


# --------------------------------------------------------------------------- #
# Pure: scratch dir placement (off the small root fs / onto the run volume)
# --------------------------------------------------------------------------- #


def test_make_scratch_dir_uses_given_root(tmp_path):
    root = tmp_path / "scratch"
    d = dl.make_scratch_dir(root, prefix="polytape-dl-")
    assert d.is_dir()
    assert d.parent == root  # created UNDER the chosen volume, not the system /tmp
    assert d.name.startswith("polytape-dl-")


def test_make_scratch_dir_creates_missing_root(tmp_path):
    root = tmp_path / "a" / "b" / "scratch"  # several missing parents
    assert not root.exists()
    d = dl.make_scratch_dir(root, prefix="x-")
    assert d.parent == root and root.is_dir()  # root tree created on demand


def test_make_scratch_dir_none_uses_system_tmp(tmp_path):
    d = dl.make_scratch_dir(None, prefix="polytape-dl-")
    try:
        assert d.is_dir()
        assert d.parent == Path(tempfile.gettempdir())  # falls back to the system temp dir
    finally:
        d.rmdir()


def test_stream_targz_roundtrip_and_cleanup(tmp_path):
    _setup_run(tmp_path)
    dest = tmp_path / "out"
    entries = dl.filter_run(tmp_path, ["1001"], dest, exported_at="2026-06-21T00:00:00Z")
    done = []
    raw = b"".join(dl.stream_targz(entries, on_done=lambda: done.append(True)))
    members = _members(raw)
    assert "event-1001/book.jsonl" in members and "event-1001/meta.json" in members
    assert json.loads(members["event-1001/meta.json"])["event_id"] == "1001"
    assert done == [True]  # cleanup callback fired after the stream drained


# --------------------------------------------------------------------------- #
# HTTP: the gated endpoint (skipped without fastapi)
# --------------------------------------------------------------------------- #


@pytest.fixture
def client_factory(tmp_path):
    pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient

    from polytape.admin import control
    from polytape.admin.app import create_app
    from polytape.admin.reader import RunReader

    _setup_run(tmp_path)

    def _make(*, secret: str | None = "secret", scratch_dir=None):
        reader = RunReader(
            tmp_path, env_file=tmp_path / "missing.env", matches_file=tmp_path / "missing.json"
        )
        audit = control.AuditLog(tmp_path / "audit.jsonl")
        app = create_app(
            reader,
            admin_token=secret,
            audit=audit,
            sessions=control.Sessions(),
            scratch_dir=scratch_dir,
        )
        return TestClient(app), audit

    return _make


def _login(client, token="secret"):
    r = client.post("/api/login", json={"token": token})
    assert r.status_code == 200, r.text


def test_download_requires_login(client_factory):
    client, _ = client_factory()
    r = client.get("/api/download?all=1")  # no session cookie
    assert r.status_code == 403


def test_download_disabled_without_secret(client_factory):
    client, _ = client_factory(secret=None)  # controls (and download) off
    r = client.get("/api/download?all=1")
    assert r.status_code == 503


def test_download_whole_run(client_factory, tmp_path):
    client, _ = client_factory()
    _login(client)
    r = client.get("/api/download?all=1")
    assert r.status_code == 200
    assert r.headers["content-type"] == "application/gzip"
    assert ".tar.gz" in r.headers["content-disposition"]
    members = _members(r.content)
    root = tmp_path.name
    assert f"{root}/book.jsonl" in members and f"{root}/meta.json" in members


def test_download_selected_match_filtered(client_factory):
    client, _ = client_factory()
    _login(client)
    r = client.get("/api/download?event=1001")
    assert r.status_code == 200
    # Filename now carries the event id + both sides' codes (title "A vs. B").
    assert 'filename="event-1001-A-B.tar.gz"' in r.headers["content-disposition"]
    members = _members(r.content)
    book = members["event-1001/book.jsonl"].splitlines()
    assert [json.loads(line)["id"] for line in book] == ["b1", "b2"]  # only 1001's book records
    assert not any(name.startswith("event-1002/") for name in members)


def test_download_unknown_event_400(client_factory):
    client, _ = client_factory()
    _login(client)
    r = client.get("/api/download?event=999999")
    assert r.status_code == 400


def test_download_rejects_cross_site(client_factory, tmp_path):
    client, _ = client_factory()
    _login(client)
    r = client.get("/api/download?all=1", headers={"Sec-Fetch-Site": "cross-site"})
    assert r.status_code == 403
    results = [
        json.loads(line).get("result")
        for line in (tmp_path / "audit.jsonl").read_text().splitlines()
    ]
    assert "cross-site-blocked" in results  # the CSRF attempt is audited


def test_download_is_audited(client_factory, tmp_path):
    client, audit = client_factory()
    _login(client)
    client.get("/api/download?event=1001")
    lines = (tmp_path / "audit.jsonl").read_text(encoding="utf-8").splitlines()
    dl_events = [json.loads(line) for line in lines if json.loads(line).get("action") == "download"]
    assert dl_events and dl_events[-1]["result"] == "ok" and dl_events[-1]["scope"] == "1001"


def test_download_filter_uses_configured_scratch_dir(client_factory, tmp_path, monkeypatch):
    # The fixture run is still OPEN (events live in meta), so there is no cached extract and
    # the route filters into scratch. That scratch must land under the CONFIGURED dir (the
    # big /data volume on prod), not the default system tmp; only the inner copy is removed
    # on completion, leaving the root intact for the next download.
    scratch_root = tmp_path / "scratch"
    seen: list[tuple] = []
    real = dl.make_scratch_dir

    def spy(root, *, prefix):
        seen.append((root, prefix))
        return real(root, prefix=prefix)

    monkeypatch.setattr(dl, "make_scratch_dir", spy)
    client, _ = client_factory(scratch_dir=scratch_root)
    _login(client)
    r = client.get("/api/download?event=1001")
    assert r.status_code == 200
    assert seen == [(scratch_root, "polytape-dl-")]  # routed onto the chosen volume
    assert scratch_root.is_dir()  # root kept...
    assert list(scratch_root.iterdir()) == []  # ...but the (multi-GB) inner copy is cleaned up


def test_download_scratch_unavailable_returns_507(client_factory, monkeypatch):
    # If the scratch volume is full/unwritable, scratch creation raises OSError; the route
    # must surface a 507 (could-not-build), NOT crash into a 500 — this is the [Errno 28]
    # path the prod journal hit, now off the small root fs.
    def boom(root, *, prefix):
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(dl, "make_scratch_dir", boom)
    client, _ = client_factory()
    _login(client)
    r = client.get("/api/download?event=1001")
    assert r.status_code == 507
