"""Per-event market allow-list (a matches file's ``record_markets``) — fully offline.

Covers the whole path: matches file -> :func:`polytape.cli.load_matches` ->
:attr:`polytape.config.Config.event_markets` -> :meth:`GammaClient.resolve_events`
(Gamma faked with ``httpx.MockTransport``) -> the ``meta.json`` snapshot and the
CLOB subscribe frame.
"""

from __future__ import annotations

import asyncio
import json
import logging

import httpx
import pytest

from polytape import app
from polytape.cli import MatchSelection, load_matches, parse_args
from polytape.config import Config
from polytape.gamma import GammaClient, GammaError, _apply_allowlist, _parse_event
from polytape.writer import CaptureWriter


def _market(mid: str, cond: str, tokens: tuple[str, str], kind: str | None = None) -> dict:
    m = {"id": mid, "conditionId": cond, "clobTokenIds": json.dumps(list(tokens))}
    if kind:
        m["sportsMarketType"] = kind
    return m


# A cfb-like event: moneyline, main spread, an alternate spread, main total, and a prop.
EVENT_A = {
    "id": "9001",
    "title": "A vs. B",
    "slug": "cfb-a-b-2026-09-05",
    "markets": [
        _market("m-ml", "0xml", ("ml-y", "ml-n"), "moneyline"),
        _market("m-sp", "0xsp", ("sp-y", "sp-n"), "spreads"),
        _market("m-sp-alt", "0xspalt", ("spa-y", "spa-n"), "spreads"),
        _market("m-tot", "0xtot", ("tot-y", "tot-n"), "totals"),
        _market("m-prop", "0xprop", ("pr-y", "pr-n")),
    ],
}
# No allow-list for this one in any test -> every market is recorded.
EVENT_B = {
    "id": "9002",
    "title": "C vs. D",
    "slug": "cfb-c-d-2026-09-05",
    "markets": [
        _market("m-b1", "0xb1", ("b1-y", "b1-n")),
        _market("m-b2", "0xb2", ("b2-y", "b2-n")),
    ],
}
EVENTS = {"9001": EVENT_A, "9002": EVENT_B}


def _gamma() -> GammaClient:
    """A GammaClient whose ``GET /events/{id}`` is served from ``EVENTS`` (no network)."""

    def handler(req: httpx.Request) -> httpx.Response:
        eid = req.url.path.rsplit("/", 1)[-1]
        if eid in EVENTS:
            return httpx.Response(200, json=EVENTS[eid])
        return httpx.Response(404, json={"error": "not found"})

    http = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="https://gamma-api.polymarket.com"
    )
    return GammaClient(client=http, backoff_base=0.001)


def _write_matches(tmp_path, matches) -> str:
    p = tmp_path / "campaign.json"
    p.write_text(json.dumps(matches), encoding="utf-8")
    return str(p)


def _ids(event) -> list[str]:
    return [m.id for m in event.markets]


# -- load_matches --------------------------------------------------------- #


def test_load_matches_without_record_markets_records_all(tmp_path):
    # wc_matches.json shape: moneyline_markets is informational, there is no record_markets.
    path = _write_matches(
        tmp_path,
        [
            {"event_id": "1001", "closed": False, "moneyline_markets": [{"conditionId": "0xA"}]},
            {"event_id": "1002", "closed": False, "record_markets": []},  # explicit empty = all
        ],
    )
    sel = load_matches(path)
    assert isinstance(sel, MatchSelection)
    assert sel.event_ids == ("1001", "1002")
    assert sel.event_markets == {}


def test_load_matches_reads_ids_and_condition_ids(tmp_path):
    path = _write_matches(
        tmp_path,
        [
            {
                "event_id": "9001",
                "closed": False,
                "record_markets": [
                    {"id": "m-ml", "conditionId": "0xml", "clobTokenIds": ["ml-y", "ml-n"]},
                    {"id": "m-sp"},  # id only
                    {"conditionId": " 0xtot "},  # conditionId only (whitespace stripped)
                    "m-extra",  # bare string
                    {"id": 123456},  # numeric id coerced to a string
                    {"id": "m-ml"},  # duplicate -> once
                ],
            }
        ],
    )
    sel = load_matches(path)
    assert sel.event_ids == ("9001",)
    assert sel.event_markets == {
        "9001": ("m-ml", "0xml", "m-sp", "0xtot", "m-extra", "123456"),
    }


def test_load_matches_unusable_entries_and_closed(tmp_path, caplog):
    path = _write_matches(
        tmp_path,
        [
            # closed -> skipped under open_only
            {"event_id": "9001", "closed": True, "record_markets": [{"id": "m-ml"}]},
            # asks to restrict but names nothing usable -> event skipped, NOT widened to "all"
            {"event_id": "9002", "closed": False, "record_markets": [{"clobTokenIds": ["x"]}]},
            # one bad entry is ignored, the good one is kept
            {"event_id": "9003", "closed": False, "record_markets": [{"id": "ok"}, {"foo": 1}]},
        ],
    )
    with caplog.at_level(logging.WARNING, logger="polytape"):
        sel = load_matches(path)
    assert sel.event_ids == ("9003",)
    assert sel.event_markets == {"9003": ("ok",)}
    assert "9002" in caplog.text and "skipping event" in caplog.text
    assert "{'foo': 1}" in caplog.text  # the ignored entry is named
    assert load_matches(path, open_only=False).event_ids == ("9001", "9003")


def test_load_matches_duplicate_event_unions_allowlists(tmp_path):
    path = _write_matches(
        tmp_path,
        [
            {"event_id": "9001", "closed": False, "record_markets": [{"id": "m-ml"}]},
            {
                "event_id": "9001",
                "closed": False,
                "record_markets": [{"id": "m-sp"}, {"id": "m-ml"}],
            },
        ],
    )
    sel = load_matches(path)
    assert sel.event_ids == ("9001",)
    assert sel.event_markets == {"9001": ("m-ml", "m-sp")}


def test_load_matches_malformed_file_raises(tmp_path):
    path = _write_matches(
        tmp_path, [{"event_id": "9001", "closed": False, "record_markets": "m-ml"}]
    )
    with pytest.raises(ValueError, match="record_markets must be a list"):
        load_matches(path)
    with pytest.raises(ValueError, match="JSON list"):
        load_matches(_write_matches(tmp_path, {"event_id": "9001"}))
    with pytest.raises(ValueError, match="match objects"):
        load_matches(_write_matches(tmp_path, ["9001"]))


# -- Config ---------------------------------------------------------------- #


def test_config_event_markets_frozen_and_normalized():
    cfg = Config(
        event_ids=("9001", "9002"),
        event_markets={"9001": ["m-ml", " 0xsp ", "m-ml", ""], "9002": []},
    )
    # de-duplicated, stripped, and an empty list means "no allow-list" (dropped)
    assert dict(cfg.event_markets) == {"9001": ("m-ml", "0xsp")}
    with pytest.raises(TypeError):
        cfg.event_markets["9001"] = ("x",)  # read-only mapping
    assert Config(event_id="1").event_markets == {}


@pytest.mark.parametrize(
    "bad",
    [
        {"9001": ["m-ml", 123]},  # a non-string market id
        {"9001": "m-ml"},  # a bare string instead of a list
        {9001: ["m-ml"]},  # a non-string event id
        {"9001": None},
    ],
)
def test_config_event_markets_rejects_non_strings(bad):
    with pytest.raises(ValueError, match="event_markets"):
        Config(event_id="9001", event_markets=bad)


def test_cli_matches_file_carries_event_markets(tmp_path):
    path = _write_matches(
        tmp_path,
        [
            {
                "event_id": "9001",
                "closed": False,
                "record_markets": [{"id": "m-ml", "conditionId": "0xml"}],
            },
            {"event_id": "9002", "closed": False},
        ],
    )
    cfg = parse_args(["--matches-file", path, "--run-name", "camp", "--out", str(tmp_path)])
    assert cfg.event_ids == ("9001", "9002")
    assert dict(cfg.event_markets) == {"9001": ("m-ml", "0xml")}
    # an --event-id run never carries an allow-list
    assert parse_args(["--event-id", "9001"]).event_markets == {}


def test_cli_malformed_record_markets_is_a_usage_error(tmp_path):
    path = _write_matches(
        tmp_path, [{"event_id": "9001", "closed": False, "record_markets": {"id": "x"}}]
    )
    with pytest.raises(SystemExit):
        parse_args(["--matches-file", path])


# -- resolution (Gamma faked) --------------------------------------------- #


def test_apply_allowlist_matches_id_or_condition_id_and_skips_unknown(caplog):
    info = _parse_event(EVENT_A, "9001")
    with caplog.at_level(logging.WARNING, logger="polytape.gamma"):
        kept = _apply_allowlist(
            info.markets, ("0xtot", "m-ml", "m-sp", "0xml", "nope"), event_id="9001"
        )
    # event order is kept; the alternate line and the prop are gone
    assert [m.id for m in kept] == ["m-ml", "m-sp", "m-tot"]
    # only the id that names no market is reported (m-ml matched by id AND conditionId)
    assert "nope" in caplog.text and "0xml" not in caplog.text
    with pytest.raises(GammaError, match="none of record_markets"):
        _apply_allowlist(info.markets, ("nope",), event_id="9001")
    assert _apply_allowlist(info.markets, (), event_id="9001") == info.markets


async def test_resolve_events_applies_per_event_allowlist():
    g = _gamma()
    events = await g.resolve_events(
        ["9001", "9002"], event_markets={"9001": ("m-ml", "0xsp", "0xtot")}
    )
    by_id = {e.event_id: e for e in events}
    assert _ids(by_id["9001"]) == ["m-ml", "m-sp", "m-tot"]
    assert by_id["9001"].clob_token_ids == ("ml-y", "ml-n", "sp-y", "sp-n", "tot-y", "tot-n")
    assert _ids(by_id["9002"]) == ["m-b1", "m-b2"]  # no allow-list -> every market
    await g.aclose()


async def test_resolve_events_drops_event_whose_allowlist_matches_nothing(caplog):
    g = _gamma()
    with caplog.at_level(logging.WARNING, logger="polytape.gamma"):
        events = await g.resolve_events(["9001", "9002"], event_markets={"9001": ("stale-id",)})
    assert [e.event_id for e in events] == ["9002"]  # dropped with a warning; run continues
    assert "could not resolve event 9001" in caplog.text and "stale-id" in caplog.text
    with pytest.raises(GammaError, match="no events could be resolved"):  # fatal only if NONE
        await g.resolve_events(["9001"], event_markets={"9001": ("stale-id",)})
    await g.aclose()


async def test_resolve_events_composes_with_global_market_id():
    g = _gamma()
    # intersection: allow-list {ml, sp, tot} with --market-id {sp, prop} -> {sp}
    events = await g.resolve_events(
        ["9001"], ("m-sp", "m-prop"), event_markets={"9001": ("m-ml", "m-sp", "m-tot")}
    )
    assert _ids(events[0]) == ["m-sp"]
    # --market-id names only a market the allow-list excluded -> empty intersection -> dropped
    with pytest.raises(GammaError):
        await g.resolve_events(["9001"], ("m-prop",), event_markets={"9001": ("m-ml",)})
    # the global override alone still works exactly as before
    events = await g.resolve_events(["9001", "9002"], ("0xprop", "m-b2"))
    assert [_ids(e) for e in events] == [["m-prop"], ["m-b2"]]
    await g.aclose()


# -- meta.json ------------------------------------------------------------- #


async def test_meta_json_snapshot_reflects_filtered_markets(tmp_path):
    allow = {"9001": ("m-ml", "m-tot")}
    g = _gamma()
    events = await g.resolve_events(["9001", "9002"], event_markets=allow)
    await g.aclose()
    cfg = Config(event_ids=("9001", "9002"), run_name="camp", out_dir=tmp_path, event_markets=allow)
    with CaptureWriter(cfg, event_infos=events) as w:
        w.write("book", {"event_type": "book", "market": "0xml", "hash": "0xH"}, event_id="9001")

    meta = json.loads((cfg.event_dir / "meta.json").read_text(encoding="utf-8"))
    snap = {e["id"]: e for e in meta["events"]}
    assert [m["id"] for m in snap["9001"]["markets"]] == ["m-ml", "m-tot"]
    assert [m["conditionId"] for m in snap["9001"]["markets"]] == ["0xml", "0xtot"]
    assert [m["id"] for m in snap["9002"]["markets"]] == ["m-b1", "m-b2"]
    # the run-level id lists exclude the filtered-out markets/tokens too
    assert meta["market_ids"] == ["0xml", "0xtot", "0xb1", "0xb2"]
    assert meta["clob_token_ids"] == [
        "ml-y", "ml-n", "tot-y", "tot-n", "b1-y", "b1-n", "b2-y", "b2-n"
    ]  # fmt: skip
    assert meta["event"]["markets"][0]["id"] == "m-ml"  # primary-event back-compat field
    per_match = json.loads(
        (cfg.event_dir / "matches" / "event-9001" / "meta.json").read_text(encoding="utf-8")
    )
    assert [m["id"] for m in per_match["event"]["markets"]] == ["m-ml", "m-tot"]


# -- end to end through app.run -------------------------------------------- #


async def test_app_run_subscribes_and_records_only_allowlisted_markets(tmp_path, make_connect):
    cfg = Config(
        event_ids=("9001", "9002"),
        run_name="camp",
        out_dir=tmp_path,
        event_markets={"9001": ("0xml",)},
    )

    def _book(asset: str, cond: str, h: str) -> str:
        return json.dumps(
            {
                "event_type": "book",
                "asset_id": asset,
                "market": cond,
                "hash": h,
                "timestamp": "1700000000000",
            }
        )

    frames = [
        _book("ml-y", "0xml", "0xKEEP"),  # allow-listed market of 9001
        _book("pr-y", "0xprop", "0xDROP"),  # 9001's excluded prop market
        _book("b1-y", "0xb1", "0xB"),  # 9002 (no allow-list)
    ]
    connect = make_connect(by_url={"clob": frames}, blocking=True)
    gamma = _gamma()
    task = asyncio.create_task(app.run(cfg, gamma=gamma, connect=connect))
    await asyncio.sleep(0.3)
    task.cancel()
    assert await task == 0
    await gamma.aclose()

    # the subscribe frame carries only the allow-listed tokens (+ all of event 9002's)
    sent = [json.loads(f) for f in connect.wss["clob"].sent if f.startswith("{")]
    subscribed = next(f for f in sent if f.get("type") == "market")["assets_ids"]
    assert subscribed == ["ml-y", "ml-n", "b1-y", "b1-n", "b2-y", "b2-n"]

    meta = json.loads((cfg.event_dir / "meta.json").read_text(encoding="utf-8"))
    assert meta["counts"] == {"book": 2}  # the excluded market's message was not recorded
    assert meta["counts_by_event"] == {"9001": {"book": 1}, "9002": {"book": 1}}
