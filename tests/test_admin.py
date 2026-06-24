"""Tests for the metadata-driven admin RunReader (offline; no fastapi required).

The reader reads everything it shows from meta.json + the registry — no scan of the
JSONL log — so these write a small meta.json and assert the surfaced views.
"""

from __future__ import annotations

import json

import pytest

from polytape.admin.reader import RunReader
from polytape.envelope import utc_now_iso


def _meta(
    *,
    counts=None,
    by_event=None,
    last_ts=None,
    last_record_at=None,
    gaps=None,
    open_events=("1001", "1002"),
    started_at="2026-06-19T16:20:00.000000Z",
) -> dict:
    """A meta.json like the recorder writes: open set + cumulative counts + freshness."""

    def ev(eid, title, slug, conds):
        return {
            "id": eid,
            "title": title,
            "slug": slug,
            "markets": [{"conditionId": c, "clobTokenIds": ["y", "n"]} for c in conds],
        }

    catalog = {
        "1001": ev("1001", "A vs. B", "fifwc-a-b-2026-06-19", ["0xA1", "0xA2", "0xA3"]),
        "1002": ev("1002", "C vs. D", "fifwc-c-d-2026-06-20", ["0xB1", "0xB2", "0xB3"]),
    }
    return {
        "started_at": started_at,
        "events": [catalog[e] for e in open_events if e in catalog],
        "counts": counts or {},
        "counts_by_event": by_event or {},
        "last_ts_by_event": last_ts or {},
        "last_record_at": last_record_at,
        "gaps": gaps or [],
    }


def _reader(tmp_path, meta, *, registry=None, **kw):
    (tmp_path / "meta.json").write_text(json.dumps(meta), encoding="utf-8")
    if registry is not None:
        (tmp_path / "registry.json").write_text(json.dumps(registry), encoding="utf-8")
    r = RunReader(
        tmp_path,
        env_file=tmp_path / "missing.env",
        registry_file=tmp_path / "registry.json",
        **kw,
    )
    # Don't spawn a real `systemctl` in tests.
    r._systemctl = lambda: {"active": "active", "restarts": 0, "since": None}  # type: ignore[method-assign]
    return r


def test_status_counts_and_coverage_from_meta(tmp_path):
    meta = _meta(
        counts={"book": 1234, "comments": 56},
        by_event={"1001": {"book": 1234, "comments": 56}},  # only 1001 has book data
        last_record_at=utc_now_iso(),
    )
    r = _reader(tmp_path, meta)
    r.update()
    st = r.status()
    assert st["records"] == {"book": 1234, "comments": 56}
    assert st["open_matches"] == 2
    # coverage: 1001's 3 condition ids seen (it has book data) of 6 open-set markets
    assert st["coverage"] == {"seen": 3, "total": 6}
    assert st["last_record_age_s"] is not None and st["last_record_age_s"] < 60


def test_status_needs_no_log_scan(tmp_path):
    # The decisive property: counts come from meta even with NO book.jsonl on disk — the
    # reader never scans the (multi-GB) log. The old drain would have shown 0 here.
    meta = _meta(counts={"book": 9_000_000, "comments": 4000})
    r = _reader(tmp_path, meta)
    assert not (tmp_path / "book.jsonl").exists()
    r.update()
    assert r.status()["records"] == {"book": 9_000_000, "comments": 4000}


def test_matches_per_event_status_and_counts(tmp_path):
    fresh = utc_now_iso()
    meta = _meta(
        by_event={"1001": {"book": 10, "comments": 2}},
        last_ts={"1001": fresh},  # 1001 ticking now -> live; 1002 has no data -> pending
    )
    r = _reader(tmp_path, meta)
    r.update()
    by_id = {m["event_id"]: m for m in r.matches()}
    assert by_id["1001"]["counts"] == {"book": 10, "comments": 2}
    assert by_id["1001"]["title"] == "A vs. B" and by_id["1001"]["date"] == "2026-06-19"
    assert by_id["1001"]["status"] == "live" and by_id["1001"]["downloadable"] is True
    assert by_id["1002"]["status"] == "pending" and by_id["1002"]["counts"] == {}
    # stable schedule-date order (1001 before 1002), not recency
    assert [m["event_id"] for m in r.matches()] == ["1001", "1002"]


def test_matches_quiet_when_stale(tmp_path):
    meta = _meta(
        by_event={"1001": {"book": 10}},
        last_ts={"1001": "2026-06-19T16:20:00.000000Z"},  # long ago vs now -> quiet
    )
    r = _reader(tmp_path, meta)
    r.update()
    assert {m["event_id"]: m["status"] for m in r.matches()}["1001"] == "quiet"


def test_finished_match_from_registry(tmp_path):
    # 0900 is NOT in the open set (rolled out) but is in the registry with recorded data ->
    # listed as finished, counted from meta, and downloadable.
    meta = _meta(open_events=("1001",), by_event={"0900": {"book": 7}, "1001": {"book": 3}})
    registry = {
        "schema": 1,
        "events": [
            {
                "event_id": "0900",
                "title": "Z vs. Y",
                "date": "2026-06-18",
                "closed": True,
                "markets": [{"conditionId": "0xZ", "clobTokenIds": []}],
            }
        ],
    }
    r = _reader(tmp_path, meta, registry=registry)
    r.update()
    by_id = {m["event_id"]: m for m in r.matches()}
    assert by_id["0900"]["status"] == "finished"
    assert by_id["0900"]["counts"] == {"book": 7} and by_id["0900"]["downloadable"] is True
    assert by_id["0900"]["title"] == "Z vs. Y"
    assert by_id["1001"]["status"] in ("live", "quiet", "pending")


def test_extractable_event_ids_finished_with_data(tmp_path):
    meta = _meta(open_events=("1001",), by_event={"0900": {"book": 7}, "0901": {}})
    registry = {
        "schema": 1,
        "events": [
            {"event_id": "0900", "title": "Z", "date": "2026-06-18", "closed": True, "markets": []},
            {"event_id": "0901", "title": "W", "date": "2026-06-17", "closed": True, "markets": []},
        ],
    }
    r = _reader(tmp_path, meta, registry=registry)
    r.update()
    # 0900 finished + has data -> extractable; 0901 finished but no data -> not; 1001 open -> not
    assert r.extractable_event_ids() == ["0900"]


def test_freshness_prefers_last_record_at(tmp_path):
    meta = _meta(last_record_at=utc_now_iso(), last_ts={"1001": "2026-06-19T00:00:00.000000Z"})
    r = _reader(tmp_path, meta)
    r.update()
    # overall freshness from last_record_at (recent), not the stale per-event value
    assert r.status()["last_record_age_s"] < 60


def test_gaps_surfaced_in_status(tmp_path):
    meta = _meta(
        gaps=[
            {"stream": "book", "downtime_seconds": 3.0},
            {"stream": "comments", "downtime_seconds": 1.0},
        ]
    )
    r = _reader(tmp_path, meta)
    r.update()
    assert r.status()["gaps"] == 2


def test_heartbeat_armed_from_env_file(tmp_path):
    (tmp_path / "meta.json").write_text(json.dumps(_meta()), encoding="utf-8")
    env = tmp_path / "p.env"
    env.write_text(
        "POLYTAPE_SALT=s\nPOLYTAPE_HEARTBEAT_URL=https://hc-ping.com/x\n", encoding="utf-8"
    )
    assert RunReader(tmp_path, env_file=env).status()["heartbeat_armed"] is True
    env.write_text("POLYTAPE_SALT=s\n", encoding="utf-8")
    assert RunReader(tmp_path, env_file=env).status()["heartbeat_armed"] is False


def test_systemctl_cached_once_per_tick(tmp_path):
    r = _reader(tmp_path, _meta())
    calls = [0]

    def fake():
        calls[0] += 1
        return {"active": "active", "restarts": 0, "since": None}

    r._systemctl = fake  # type: ignore[method-assign]
    r.update()
    assert calls[0] == 1  # one subprocess per update() tick
    r.status()
    r.status()
    assert calls[0] == 1  # status() reads the cache, never re-spawns
    assert r.status()["recorder"]["active"] == "active"


def test_corrupt_meta_keeps_prior_snapshot(tmp_path):
    r = _reader(tmp_path, _meta(counts={"book": 5}))
    r.update()
    assert r.status()["records"]["book"] == 5
    (tmp_path / "meta.json").write_text("{ not json", encoding="utf-8")
    r.update()  # must not crash; keeps the last good snapshot
    assert r.status()["records"]["book"] == 5


def test_app_endpoints_status_matches_only(tmp_path):
    # End-to-end: status + matches serve from meta; the removed live/preview routes 404.
    pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient

    from polytape.admin.app import create_app

    meta = _meta(counts={"book": 11, "comments": 2}, by_event={"1001": {"book": 11}})
    reader = _reader(tmp_path, meta)
    app = create_app(reader, poll_interval=3600, registry_refresh_s=0, extract_refresh_s=0)
    with TestClient(app) as c:
        st = c.get("/api/status").json()
        assert st["records"] == {"book": 11, "comments": 2}
        ms = c.get("/api/matches").json()
        assert {m["event_id"] for m in ms} == {"1001", "1002"}
        assert c.get("/api/live").status_code == 404  # live feed removed
        assert c.get("/api/matches/1001").status_code == 404  # order-book preview removed
