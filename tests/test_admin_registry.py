"""Tests for the cumulative run registry: pure parsing/collision, and the reader/
download integration that lists, counts, orders, and downloads FINISHED matches
(rolled out of meta.events) — without any network (the registry file is injected).
"""

from __future__ import annotations

import json

from polytape.admin import download as dl
from polytape.admin import registry as reg
from polytape.admin.reader import RunReader
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


def _meta_open() -> dict:
    """What the recorder writes: the current open set in ``events`` (finished 0900 is gone
    from there), but CUMULATIVE per-event counts + freshness that still include 0900 (the
    recorder carries them across roll-overs). Open matches are fresh -> "live"."""

    def mk(c):
        return {"id": "m", "conditionId": c, "clobTokenIds": [c + "-Y", c + "-N"]}

    fresh = utc_now_iso()
    return {
        "started_at": "2026-06-21T00:00:00Z",
        "run_name": "wc",
        "counts_by_event": {"0900": {"book": 2}, "1001": {"book": 1}, "1002": {"book": 1}},
        "last_ts_by_event": {"1001": fresh, "1002": fresh},
        "last_record_at": fresh,
        "events": [
            {
                "id": "1001",
                "title": "A vs B",
                "slug": "fifwc-a-b-2026-06-21",
                "markets": [mk("0xA1")],
            },
            {
                "id": "1002",
                "title": "C vs D",
                "slug": "fifwc-c-d-2026-06-22",
                "markets": [mk("0xB1")],
            },
        ],
    }


def _registry_events() -> list[dict]:
    """All run matches incl. FINISHED 0900 (earliest date) + the two open ones."""

    def m(c, label):
        return {"conditionId": c, "clobTokenIds": [c + "-Y", c + "-N"], "groupItemTitle": label}

    return [
        {
            "event_id": "0900",
            "title": "Z vs Y",
            "slug": "fifwc-z-y-2026-06-19",
            "date": "2026-06-19",
            "closed": True,
            "markets": [m("0xZ", "Z")],
        },
        {
            "event_id": "1001",
            "title": "A vs B",
            "slug": "fifwc-a-b-2026-06-21",
            "date": "2026-06-21",
            "closed": False,
            "markets": [m("0xA1", "A")],
        },
        {
            "event_id": "1002",
            "title": "C vs D",
            "slug": "fifwc-c-d-2026-06-22",
            "date": "2026-06-22",
            "closed": False,
            "markets": [m("0xB1", "C")],
        },
    ]


def _setup(tmp_path, *, with_registry=True):
    (tmp_path / "meta.json").write_text(json.dumps(_meta_open()), encoding="utf-8")
    _write_jsonl(
        tmp_path / "book.jsonl",
        [_book("0xA1", "a1"), _book("0xB1", "b1"), _book("0xZ", "z1"), _book("0xZ", "z2")],
    )
    (tmp_path / "comments.jsonl").write_text("", encoding="utf-8")
    if with_registry:
        reg.write_registry_atomic(
            tmp_path / "registry.json", _registry_events(), now_iso="2026-06-21T00:00:00Z"
        )


def _reader(tmp_path):
    return RunReader(
        tmp_path,
        env_file=tmp_path / "x.env",
        registry_file=tmp_path / "registry.json",
    )


# --------------------------------------------------------------------------- #
# Pure registry module
# --------------------------------------------------------------------------- #


def test_cond_collision_keeps_first_claim():
    evs = [
        {
            "event_id": "B",
            "date": "2026-06-20",
            "markets": [{"conditionId": "0xX", "clobTokenIds": []}],
        },
        {
            "event_id": "A",
            "date": "2026-06-19",
            "markets": [{"conditionId": "0xX", "clobTokenIds": []}],
        },
    ]
    R = reg.build_registry(evs)
    assert R.cond2event["0xX"] == "A"  # earlier-scheduled match keeps the conditionId
    assert R.collisions == 1


def test_load_registry_tolerates_raw_discovery_shape(tmp_path):
    raw = [  # a bare list with the raw list_wc_matches `match_date` key
        {
            "event_id": "0900",
            "title": "Z",
            "match_date": "2026-06-19",
            "closed": True,
            "markets": [{"conditionId": "0xZ", "clobTokenIds": ["t"]}],
        }
    ]
    (tmp_path / "reg.json").write_text(json.dumps(raw), encoding="utf-8")
    R = reg.load_registry(tmp_path / "reg.json")
    assert R.date["0900"] == "2026-06-19" and R.cond2event["0xZ"] == "0900"


def test_load_registry_absent_is_empty(tmp_path):
    R = reg.load_registry(tmp_path / "nope.json")
    assert R.events == {} and R.cond2event == {}


# --------------------------------------------------------------------------- #
# Reader integration
# --------------------------------------------------------------------------- #


def test_finished_match_listed_and_counted(tmp_path):
    _setup(tmp_path)
    r = _reader(tmp_path)
    r.update()
    by = {m["event_id"]: m for m in r.matches()}
    assert "0900" in by  # the finished match is listed
    assert by["0900"]["status"] == "finished"
    assert by["0900"]["counts"]["book"] == 2  # counted via the registry conditionId map
    assert by["0900"]["downloadable"] is True
    assert by["1001"]["status"] == "live"
    # finished matches must NOT inflate the open-set metrics
    assert r.status()["open_matches"] == 2


def test_matches_stable_schedule_order(tmp_path):
    _setup(tmp_path)
    r = _reader(tmp_path)
    r.update()
    order1 = [m["event_id"] for m in r.matches()]
    assert order1 == ["0900", "1001", "1002"]  # by scheduled date, ascending
    # fresh activity on a later match must NOT reorder rows (the old recency-sort bug)
    with open(tmp_path / "book.jsonl", "a", encoding="utf-8") as fh:
        fh.write(json.dumps(_book("0xB1", "b2")) + "\n")
    r.update()
    assert [m["event_id"] for m in r.matches()] == order1


def test_status_open_set_overrides_closed_flag(tmp_path):
    _setup(tmp_path)
    evs = _registry_events()
    for e in evs:
        if e["event_id"] == "1001":
            e["closed"] = True  # registry says closed, but it's in the live open set
    reg.write_registry_atomic(tmp_path / "registry.json", evs, now_iso="t")
    r = _reader(tmp_path)
    r.update()
    by = {m["event_id"]: m for m in r.matches()}
    assert by["1001"]["status"] in ("live", "quiet", "pending")  # never "finished"


def test_registry_absent_degrades_to_meta(tmp_path):
    _setup(tmp_path, with_registry=False)
    r = _reader(tmp_path)
    r.update()
    ids = {m["event_id"] for m in r.matches()}
    assert ids == {"1001", "1002"}  # only open meta events, exactly today's behaviour
    assert "0900" not in ids  # finished records uncounted/unlisted without the registry


def test_download_registry_matches_listing(tmp_path):
    _setup(tmp_path)
    r = _reader(tmp_path)
    r.update()
    meta = json.loads((tmp_path / "meta.json").read_text(encoding="utf-8"))
    dlreg = r.download_registry(meta)
    listed = {m["event_id"] for m in r.matches() if m["downloadable"]}
    assert listed <= set(dlreg)  # everything selectable in the UI passes the gate
    assert "0900" in dlreg  # the finished match is selectable for download


# --------------------------------------------------------------------------- #
# Download of a finished match
# --------------------------------------------------------------------------- #


def test_filter_run_finished_event_attributes(tmp_path):
    _setup(tmp_path)
    meta = json.loads((tmp_path / "meta.json").read_text(encoding="utf-8"))
    r = _reader(tmp_path)
    r.update()
    dlreg = r.download_registry(meta)
    dest = tmp_path / "out"
    dl.filter_run(tmp_path, ["0900"], dest, meta=meta, registry=dlreg, exported_at="t")
    ids = [
        json.loads(line)["id"] for line in (dest / "event-0900/book.jsonl").read_text().splitlines()
    ]
    assert ids == ["z1", "z2"]  # finished match's records, attributed via the registry
    slice_meta = json.loads((dest / "event-0900/meta.json").read_text(encoding="utf-8"))
    assert slice_meta["counts"]["book"] == 2  # filtered tally (absent from counts_by_event)
    assert slice_meta["event"]["title"] == "Z vs Y"  # identity recovered from the registry


def test_filter_run_meta_wins_when_registry_lacks_conditionid(tmp_path):
    # A registry entry for an OPEN match whose market has a null conditionId must NOT
    # mask the real conditionId meta carries — otherwise the dashboard counts the
    # records but the download yields an empty slice. meta wins on the shared event.
    _setup(tmp_path)
    meta = json.loads((tmp_path / "meta.json").read_text(encoding="utf-8"))
    evs = _registry_events()
    for e in evs:
        if e["event_id"] == "1001":
            e["markets"] = [{"conditionId": None, "clobTokenIds": []}]  # registry lost the cond
    reg.write_registry_atomic(tmp_path / "registry.json", evs, now_iso="t")
    r = _reader(tmp_path)
    r.update()
    dlreg = r.download_registry(meta)
    dest = tmp_path / "out"
    dl.filter_run(tmp_path, ["1001"], dest, meta=meta, registry=dlreg, exported_at="t")
    ids = [
        json.loads(line)["id"] for line in (dest / "event-1001/book.jsonl").read_text().splitlines()
    ]
    assert ids == ["a1"]  # 0xA1 routed via meta (registry entry's null cond didn't drop it)
