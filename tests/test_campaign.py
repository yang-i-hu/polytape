"""Campaign discovery: the pure rules (polytape.campaign) and the script wrapper — offline."""

from __future__ import annotations

import importlib.util
import json
import random
import re
import urllib.error
from datetime import datetime, timedelta, timezone, tzinfo
from pathlib import Path

import pytest

from polytape import campaign as cp
from polytape.cli import load_matches
from polytape.streams import clob

_SPEC = importlib.util.spec_from_file_location(
    "list_campaign_events",
    Path(__file__).resolve().parents[1] / "scripts" / "list_campaign_events.py",
)
lce = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(lce)

# 2026-09-03T20:00Z == 4pm EDT (the example ladder event in the task brief).
NOW = datetime(2026, 9, 3, 20, 0, tzinfo=timezone.utc)
GAME_START = "2026-09-05 01:40:00+00"  # Gamma's market-level gameStartTime format


# --------------------------------------------------------------------------- #
# Fixture builders (shapes copied from live Gamma payloads, values shortened)
# --------------------------------------------------------------------------- #


def mkt(
    mid,
    mtype,
    prices=("0.5", "0.5"),
    *,
    line=None,
    closed=False,
    tokens=None,
    start=GAME_START,
    outcomes=("Yes", "No"),
    question=None,
    group=None,
):
    return {
        "id": str(mid),
        "sportsMarketType": mtype,
        "line": line,
        "gameStartTime": start,
        "outcomePrices": json.dumps(list(prices)),
        "outcomes": json.dumps(list(outcomes)),
        "clobTokenIds": json.dumps(list(tokens))
        if tokens is not None
        else json.dumps([f"{mid}y", f"{mid}n"]),
        "conditionId": f"0xc{mid}",
        "closed": closed,
        "active": True,
        "question": question or f"q{mid}",
        "groupItemTitle": group,
    }


def event(eid, slug, markets, *, closed=False, title=None, start="2026-08-29T13:00:48Z"):
    return {
        "id": str(eid),
        "slug": slug,
        "title": title or f"Event {eid}",
        "closed": closed,
        "active": True,
        "startDate": start,
        "endDate": "2026-09-12T01:40:00Z",
        "gameStartTime": None,
        "markets": markets,
    }


def mlb_game():
    """mlb-nyy-sd-2026-09-04 as seen live: moneyline, nrfi, 4 spreads, 3 totals, extras."""
    return event(
        930715,
        "mlb-nyy-sd-2026-09-04",
        [
            mkt(3973269, "moneyline", ("0.545", "0.455"), outcomes=("NYY", "SD")),
            mkt(3973270, "nrfi", ("0.445", "0.555")),
            mkt(4192710, "spreads", ("0.44", "0.56"), line=-1.5),
            mkt(4192711, "totals", ("0.5", "0.5"), line=7.5),
            mkt(4192712, "baseball_team_first_five_spread", line=-1.5),
            mkt(4192750, "spreads", ("0.44", "0.56"), line=-1.5),
            mkt(4192751, "spreads", ("0.485", "0.515"), line=-2.5),
            mkt(4192752, "spreads", ("0.48", "0.52"), line=-2.5),
            mkt(4195318, "totals", ("0.5", "0.5"), line=6.5),
            mkt(4195319, "totals", ("0.5", "0.5"), line=8.5),
        ],
        title="New York Yankees vs. San Diego Padres",
    )


def epl_more_markets():
    """epl-lee-new-2026-09-14-more-markets: spreads, six totals, other props (no moneyline)."""
    return event(
        945983,
        "epl-lee-new-2026-09-14-more-markets",
        [
            mkt(4052199, "spreads", ("0.165", "0.835"), line=-1.5, start="2026-09-04 19:00:00+00"),
            mkt(4052207, "totals", ("0.51", "0.49"), line=0.5, start="2026-09-04 19:00:00+00"),
            mkt(4052209, "totals", ("0.785", "0.215"), line=1.5, start="2026-09-04 19:00:00+00"),
            mkt(4052211, "totals", ("0.55", "0.45"), line=2.5, start="2026-09-04 19:00:00+00"),
            mkt(4052213, "totals", ("0.33", "0.67"), line=3.5, start="2026-09-04 19:00:00+00"),
            mkt(4052219, "both_teams_to_score", ("0.59", "0.41"), start="2026-09-04 19:00:00+00"),
            mkt(
                4052223,
                "first_half_totals",
                ("0.5", "0.5"),
                line=0.5,
                start="2026-09-04 19:00:00+00",
            ),
        ],
        title="Leeds United FC vs. Newcastle United FC - More Markets",
    )


def epl_main():
    """epl-lee-new-2026-09-14: the 3-way moneyline (three Yes/No markets), nothing else."""
    return event(
        945719,
        "epl-lee-new-2026-09-14",
        [
            mkt(4050627, "moneyline", ("0.385", "0.615"), start="2026-09-04 19:00:00+00"),
            mkt(4050636, "moneyline", ("0.275", "0.725"), start="2026-09-04 19:00:00+00"),
            mkt(4050646, "moneyline", ("0.36", "0.64"), start="2026-09-04 19:00:00+00"),
        ],
        title="Leeds United FC vs. Newcastle United FC",
    )


def ladder(eid="960280", slug="bitcoin-above-on-september-3-2026-4pm-et", n=20, *, closed=False):
    markets = [
        mkt(4193202 + i, None, ("0.025", "0.975"), start=None, group=f"{83_000 - 200 * i:,}")
        for i in range(n)
    ]
    ev = event(eid, slug, markets, closed=closed, start="2026-09-03T18:40:15Z")
    ev["title"] = "Bitcoin above ___ on September 3, 4PM ET?"
    ev["endDate"] = "2026-09-03T20:00:00Z"
    return ev


SPEC_OBJ = {
    "run_name": "maker",
    "lookahead_h": 72,
    "lookback_h": 8,
    "sports": [
        {"tag": "mlb", "types": ["moneyline", "spreads", "totals"], "lines": "all"},
        {"tag": "cfb", "types": ["moneyline", "spreads", "totals"], "lines": "main"},
        {"tag": "epl", "types": ["moneyline", "totals"], "lines": "main"},
    ],
    "ladders": {"assets": ["bitcoin", "ethereum"], "hours_ahead": 2},
}
MLB = cp.SportsSpec("mlb", ("moneyline", "spreads", "totals"), "all")
EPL = cp.SportsSpec("epl", ("moneyline", "totals"), "main")
WINDOW = {"now": NOW, "lookback_h": 8, "lookahead_h": 72}


# --------------------------------------------------------------------------- #
# Spec
# --------------------------------------------------------------------------- #


def test_load_spec_full_and_defaults():
    spec = cp.load_spec(SPEC_OBJ)
    assert spec.run_name == "maker" and spec.lookahead_h == 72 and spec.lookback_h == 8
    assert [s.tag for s in spec.sports] == ["mlb", "cfb", "epl"]
    assert spec.sports[0].lines == "all" and spec.sports[1].lines == "main"
    assert spec.sports[2].types == ("moneyline", "totals")
    assert spec.ladders == cp.LadderSpec(("bitcoin", "ethereum"), 2)
    # Defaults: 72h/8h, lines=main, hours_ahead=2, ladders absent -> None.
    lean = cp.load_spec({"sports": [{"tag": "nhl", "types": ["totals"]}]})
    assert (lean.lookahead_h, lean.lookback_h, lean.ladders) == (72.0, 8.0, None)
    assert lean.sports[0].lines == "main"
    assert lean.sports[0].types == ("moneyline", "totals")  # moneyline is always included


def test_load_spec_rejects_malformed():
    with pytest.raises(ValueError):
        cp.load_spec([])
    with pytest.raises(ValueError):
        cp.load_spec({"sports": [{"tag": "mlb", "types": ["totals"], "lines": "some"}]})
    with pytest.raises(ValueError):
        cp.load_spec({"sports": [{"types": ["totals"]}]})  # no tag
    with pytest.raises(ValueError):
        cp.load_spec({"sports": [{"tag": "mlb", "types": "totals"}]})  # not a list
    with pytest.raises(ValueError):
        cp.load_spec({"sports": [{"tag": "mlb"}], "lookahead_h": -1})
    with pytest.raises(ValueError):
        cp.load_spec({"ladders": {"assets": ["bitcoin"], "hours_ahead": "2"}})
    with pytest.raises(ValueError):
        cp.load_spec({"sports": [], "ladders": {"assets": []}})  # names no family


# --------------------------------------------------------------------------- #
# Time parsing + window
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("2026-09-05 01:40:00+00", datetime(2026, 9, 5, 1, 40, tzinfo=timezone.utc)),
        ("2026-09-03T18:40:15Z", datetime(2026, 9, 3, 18, 40, 15, tzinfo=timezone.utc)),
        ("2026-01-21T20:45:03.607Z", datetime(2026, 1, 21, 20, 45, 3, 607000, tzinfo=timezone.utc)),
        (
            "2026-02-19T03:08:09.276545Z",
            datetime(2026, 2, 19, 3, 8, 9, 276545, tzinfo=timezone.utc),
        ),
        ("2026-09-05T01:40:00.5Z", datetime(2026, 9, 5, 1, 40, 0, 500000, tzinfo=timezone.utc)),
        ("2026-09-04T21:40:00-04:00", datetime(2026, 9, 5, 1, 40, tzinfo=timezone.utc)),
        ("2026-09-05T01:40:00", datetime(2026, 9, 5, 1, 40, tzinfo=timezone.utc)),  # naive=UTC
        ("2026-09-04", datetime(2026, 9, 4, tzinfo=timezone.utc)),
    ],
)
def test_parse_gamma_time_formats(raw, expected):
    assert cp.parse_gamma_time(raw) == expected


def test_parse_gamma_time_garbage_is_none():
    for raw in (None, "", "   ", "soon", 12345, "2026-13-45T00:00:00Z"):
        assert cp.parse_gamma_time(raw) is None
    assert cp.iso_utc(None) is None
    assert cp.iso_utc(datetime(2026, 9, 5, 1, 40, tzinfo=timezone.utc)) == "2026-09-05T01:40:00Z"


def test_market_start_is_the_kickoff_never_the_listing_date():
    ev = event(1, "x", [], start="2026-09-01T00:00:00Z")
    assert cp.market_start(ev, {"gameStartTime": GAME_START}) == cp.parse_gamma_time(GAME_START)
    ev["gameStartTime"] = "2026-09-02 12:00:00+00"
    assert cp.market_start(ev, {}) == datetime(2026, 9, 2, 12, tzinfo=timezone.utc)
    # No kick-off anywhere -> unknown. NOT the event's startDate: for Gamma sports
    # events that is the LISTING time, not a game time.
    ev["gameStartTime"] = None
    assert cp.market_start(ev, {"gameStartTime": "garbage"}) is None
    assert cp.market_start(ev, {}) is None


def test_sports_market_without_a_kickoff_is_never_recorded_as_a_game():
    # "Pro Football: X vs. Y Season Series Winner": sportsMarketType=moneyline, no
    # gameStartTime, listed (startDate) an hour ago — inside lookback_h by LISTING time.
    fresh = (NOW - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    ev = event(
        48,
        "pro-football-cardinals-vs-rams-season-series-winner",
        [mkt(1, "moneyline", start=None), mkt(2, "moneyline", start=None)],
        start=fresh,
    )
    nfl = cp.SportsSpec("nfl", ("moneyline", "spreads", "totals"), "main")
    assert cp.select_sports_markets(ev, nfl, **WINDOW) == []
    assert cp.sports_entry(ev, nfl, **WINDOW) is None


def test_in_window_is_inclusive_and_rejects_unknown():
    assert cp.in_window(NOW - timedelta(hours=8), NOW, 8, 72)
    assert cp.in_window(NOW + timedelta(hours=72), NOW, 8, 72)
    assert not cp.in_window(NOW - timedelta(hours=8, seconds=1), NOW, 8, 72)
    assert not cp.in_window(NOW + timedelta(hours=72, seconds=1), NOW, 8, 72)
    assert not cp.in_window(None, NOW, 8, 72)


# --------------------------------------------------------------------------- #
# Main-line choice
# --------------------------------------------------------------------------- #


def test_pick_main_line_closest_to_half_then_lowest_id():
    main = mkt(4045722, "spreads", ("0.52", "0.48"), line=-9.5)
    alt = mkt(4177475, "spreads", ("0.62", "0.38"), line=-7.5)
    assert cp.pick_main_line([alt, main]) is main
    # Exact tie (unpriced alternates all show 0.5/0.5): the lowest id — the line listed
    # first — wins, comparing ids as NUMBERS (not strings).
    a, b = mkt(1000, "totals", line=55.5), mkt(999, "totals", line=54.5)
    assert cp.pick_main_line([a, b]) is b
    # Unparseable / missing prices rank last; nothing -> None.
    junk = mkt(1, "totals", line=1.5)
    junk["outcomePrices"] = "not json"
    assert cp.pick_main_line([junk, a]) is a
    assert cp.pick_main_line([]) is None
    assert cp.price_distance({"outcomePrices": '["0.4", "0.6"]'}) == pytest.approx(0.1)
    assert cp.price_distance({}) == float("inf")


def qmkt(mid, mtype, prices, *, line, bid, ask, spread, liq, **kw):
    """A market carrying Gamma's quote fields (bestBid / bestAsk / spread / liquidityNum)."""
    m = mkt(mid, mtype, prices, line=line, **kw)
    m.update({"bestBid": bid, "bestAsk": ask, "spread": spread, "liquidityNum": liq})
    return m


def cmu_unm_spreads():
    """Central Michigan vs. New Mexico spreads as seen live (2026-09-03T20:36Z): every
    alternate is pre-generated, and the ones with no book at all sit at exactly
    0.50/0.50 — Gamma's outcomePrices is the bid/ask midpoint of a 0.01/0.99 rail book."""
    return [
        qmkt(
            3988845,
            "spreads",
            ("0.465", "0.535"),
            line=-11.5,
            bid=0.46,
            ask=0.47,
            spread=0.01,
            liq=3545.0,
        ),  # noqa: E501
        qmkt(
            3988846,
            "spreads",
            ("0.505", "0.495"),
            line=-10.5,
            bid=0.49,
            ask=0.52,
            spread=0.03,
            liq=3701.7,
        ),  # noqa: E501
        qmkt(
            4101342,
            "spreads",
            ("0.625", "0.375"),
            line=-0.5,
            bid=0.44,
            ask=0.81,
            spread=0.37,
            liq=415.0,
        ),  # noqa: E501
        qmkt(
            4101343, "spreads", ("0.5", "0.5"), line=-1.5, bid=0.01, ask=0.99, spread=0.98, liq=0.32
        ),  # noqa: E501
        qmkt(
            4101344, "spreads", ("0.5", "0.5"), line=-2.5, bid=0.01, ask=0.99, spread=0.98, liq=0.32
        ),  # noqa: E501
        qmkt(
            4101350,
            "spreads",
            ("0.545", "0.455"),
            line=-8.5,
            bid=0.53,
            ask=0.56,
            spread=0.03,
            liq=3231.0,
        ),  # noqa: E501
    ]


def test_is_quoted_needs_a_real_two_sided_book():
    assert cp.is_quoted({"bestBid": 0.49, "bestAsk": 0.52, "spread": 0.03})
    assert cp.is_quoted({"bestBid": "0.49", "bestAsk": "0.52", "spread": "0.03"})  # number-strings
    assert not cp.is_quoted({"bestBid": 0.01, "bestAsk": 0.99, "spread": 0.98})  # the rails
    assert not cp.is_quoted({"bestBid": 0.44, "bestAsk": 0.81, "spread": 0.37})  # too wide
    assert not cp.is_quoted({"bestBid": 0.05, "bestAsk": 0.25, "spread": 0.20})  # bid on the rail
    assert not cp.is_quoted({"bestBid": 0.75, "bestAsk": 0.95, "spread": 0.20})  # ask on the rail
    assert not cp.is_quoted({"bestBid": None, "bestAsk": 0.5, "spread": 0.01})  # null (128/5k live)
    assert not cp.is_quoted({"bestBid": 0.49, "bestAsk": 0.52})  # missing spread
    assert not cp.is_quoted({"bestBid": True, "bestAsk": 0.52, "spread": 0.03})
    assert cp.liquidity({"liquidityNum": "12.5"}) == 12.5
    assert cp.liquidity({}) == 0.0 and cp.liquidity({"liquidityNum": "nan"}) == 0.0


def test_pick_main_line_prefers_a_quoted_book_over_the_placeholder_midpoint():
    pick = cp.pick_main_line(cmu_unm_spreads())
    assert (pick["id"], pick["line"]) == ("3988846", -10.5)  # 0.505/0.495, $3.7k, 3c wide
    # Among quoted lines: closest to 0.50 first, then the most liquid, then the lowest id.
    a = qmkt(10, "totals", ("0.51", "0.49"), line=52.5, bid=0.50, ask=0.52, spread=0.02, liq=100.0)
    b = qmkt(11, "totals", ("0.51", "0.49"), line=53.5, bid=0.50, ask=0.52, spread=0.02, liq=900.0)
    c = qmkt(12, "totals", ("0.51", "0.49"), line=54.5, bid=0.50, ask=0.52, spread=0.02, liq=900.0)
    assert cp.pick_main_line([a, b, c]) is b and cp.pick_main_line([c, b]) is b
    d = qmkt(13, "totals", ("0.5", "0.5"), line=55.5, bid=0.49, ask=0.51, spread=0.02, liq=1.0)
    assert cp.pick_main_line([a, b, c, d]) is d  # closest wins over liquidity
    # Nothing quoted at all: the most liquid, then closest to 0.50, then lowest id.
    dead = [
        qmkt(20, "totals", ("0.5", "0.5"), line=33.5, bid=0.01, ask=0.99, spread=0.98, liq=0.34),
        qmkt(22, "totals", ("0.5", "0.5"), line=54.5, bid=0.01, ask=0.99, spread=0.98, liq=8113.0),
        qmkt(21, "totals", ("0.5", "0.5"), line=53.5, bid=0.01, ask=0.99, spread=0.98, liq=8113.0),
    ]
    assert cp.pick_main_line(dead)["id"] == "21"


def test_pick_main_line_is_sticky_while_the_previous_pick_stays_a_main_line():
    lines = cmu_unm_spreads()
    by_id = {m["id"]: m for m in lines}
    assert cp.pick_main_line(lines, previous_id="3988846") is by_id["3988846"]
    # The odds moved: -11.5 is now closest to 0.50, but -10.5 is still a quoted main
    # line -> kept (a fresh pick would flip, and every flip restarts the recorder).
    by_id["3988845"]["outcomePrices"] = '["0.5", "0.5"]'
    by_id["3988846"]["outcomePrices"] = '["0.55", "0.45"]'
    assert cp.pick_main_line(lines) is by_id["3988845"]
    assert cp.pick_main_line(lines, previous_id="3988846") is by_id["3988846"]
    assert cp.pick_main_line(lines, previous_id=3988846) is by_id["3988846"]  # int id is fine
    # Released when the previous pick lost its quote (back on the rails)...
    by_id["3988846"].update({"bestBid": 0.01, "bestAsk": 0.99, "spread": 0.98})
    assert cp.pick_main_line(lines, previous_id="3988846") is by_id["3988845"]
    by_id["3988846"].update({"bestBid": 0.53, "bestAsk": 0.56, "spread": 0.03})
    # ...or drifted out of the main-line band (further than STICKY_MAX_DISTANCE from 0.50)...
    by_id["3988846"]["outcomePrices"] = '["0.72", "0.28"]'
    assert cp.pick_main_line(lines, previous_id="3988846") is by_id["3988845"]
    by_id["3988846"]["outcomePrices"] = '["0.7", "0.3"]'  # exactly on the edge: still main
    assert cp.pick_main_line(lines, previous_id="3988846") is by_id["3988846"]
    # ...or is simply no longer a candidate (closed / delisted / unknown id).
    assert cp.pick_main_line(lines, previous_id="999") is by_id["3988845"]
    without = [m for m in lines if m["id"] != "3988846"]
    assert cp.pick_main_line(without, previous_id="3988846") is by_id["3988845"]
    # An unquoted previous pick (chosen when nothing was quoted) gives way to a real book.
    dead = [qmkt(20, "totals", ("0.5", "0.5"), line=33.5, bid=0.01, ask=0.99, spread=0.98, liq=0.3)]
    assert cp.pick_main_line(dead, previous_id="20") is dead[0]
    live = qmkt(21, "totals", ("0.5", "0.5"), line=53.5, bid=0.49, ask=0.51, spread=0.02, liq=100.0)
    assert cp.pick_main_line([*dead, live], previous_id="20") is live


def test_previous_picks_collects_non_moneyline_picks_per_event():
    entries = [
        {
            "event_id": "1",
            "record_markets": [
                {"id": "10", "sportsMarketType": "moneyline"},
                {"id": "11", "sportsMarketType": "spreads"},
                {"id": "12", "sportsMarketType": "totals"},
            ],
        },
        {
            "event_id": "2",
            "record_markets": [
                {"id": 20, "sportsMarketType": "totals"},
                {"id": "", "sportsMarketType": "spreads"},
                {"id": "22", "sportsMarketType": None},
            ],
        },
        {"event_id": "", "record_markets": [{"id": "30", "sportsMarketType": "totals"}]},
        {"event_id": "3", "record_markets": "junk"},
        "junk",
        None,
    ]
    assert cp.previous_picks(entries) == {
        "1": {"spreads": "11", "totals": "12"},
        "2": {"totals": "20"},
    }
    assert cp.previous_picks([]) == {}


def test_select_main_mode_keeps_the_previous_line_while_it_is_quoted():
    ev = event(
        77, "cfb-cmich-unm-2026-09-04", [mkt(1, "moneyline", ("0.6", "0.4")), *cmu_unm_spreads()]
    )
    cfb = cp.SportsSpec("cfb", ("moneyline", "spreads", "totals"), "main")
    fresh = cp.select_sports_markets(ev, cfb, **WINDOW)
    assert [(m["sportsMarketType"], m["id"]) for m in fresh] == [
        ("moneyline", "1"),
        ("spreads", "3988846"),
    ]
    sticky = cp.select_sports_markets(ev, cfb, previous={"spreads": "3988845"}, **WINDOW)
    assert [m["id"] for m in sticky] == ["1", "3988845"]
    # a previous pick for a type the spec does not record is ignored; a stale id re-picks
    stale = cp.select_sports_markets(ev, cfb, previous={"totals": "9", "spreads": "9"}, **WINDOW)
    assert [m["id"] for m in stale] == ["1", "3988846"]
    # ...and it threads through sports_entries via previous_picks(<installed file>),
    # with the quote the pick was made on written into the record for auditing.
    spec = cp.load_spec(SPEC_OBJ)
    installed = cp.sports_entries({"cfb": [ev]}, spec, NOW, previous={"77": {"spreads": "3988845"}})
    assert [m["id"] for m in installed[0]["record_markets"]] == ["1", "3988845"]
    again = cp.sports_entries({"cfb": [ev]}, spec, NOW, previous=cp.previous_picks(installed))
    assert [m["id"] for m in again[0]["record_markets"]] == ["1", "3988845"]
    spread = again[0]["record_markets"][1]
    assert (spread["bestBid"], spread["bestAsk"], spread["spread"], spread["liquidityNum"]) == (
        0.46,
        0.47,
        0.01,
        3545.0,
    )


def test_select_main_mode_keeps_all_moneylines_and_one_line_per_type():
    # Soccer: 3-way moneyline -> all three kept; only one of six totals.
    ml = cp.select_sports_markets(epl_main(), EPL, **WINDOW)
    assert [m["id"] for m in ml] == ["4050627", "4050636", "4050646"]
    more = cp.select_sports_markets(epl_more_markets(), EPL, **WINDOW)
    assert [(m["sportsMarketType"], m["line"]) for m in more] == [("totals", 0.5)]
    # spreads are not in EPL's types even though the event has them.
    assert all(m["sportsMarketType"] != "spreads" for m in more)


def test_select_all_mode_keeps_every_line_of_wanted_types_only():
    chosen = cp.select_sports_markets(mlb_game(), MLB, **WINDOW)
    assert [m["sportsMarketType"] for m in chosen] == [
        "moneyline",
        "spreads",
        "spreads",
        "spreads",
        "spreads",
        "totals",
        "totals",
        "totals",
    ]
    assert [m["id"] for m in chosen[1:5]] == ["4192710", "4192750", "4192751", "4192752"]
    # main mode on the same event: one spread (closest to 0.5 = -2.5 SD) + one total (7.5,
    # the lowest id among the 0.5/0.5 ties).
    main = cp.select_sports_markets(mlb_game(), cp.SportsSpec("mlb", MLB.types, "main"), **WINDOW)
    assert [(m["sportsMarketType"], m["id"]) for m in main] == [
        ("moneyline", "3973269"),
        ("spreads", "4192751"),
        ("totals", "4192711"),
    ]


def test_select_skips_closed_tokenless_and_out_of_window_markets():
    ev = event(
        7,
        "nfl-a-b-2026-09-06",
        [
            mkt(10, "moneyline", closed=True),
            mkt(11, "moneyline", tokens=[]),
            mkt(12, "moneyline", start="2026-09-03 11:59:59+00"),  # 8h + 1s ago
            mkt(13, "moneyline", start="2026-09-06 20:00:01+00"),  # 72h + 1s ahead
            mkt(14, "moneyline", start="2026-09-03 12:00:00+00"),  # exactly 8h ago
            mkt(15, "moneyline", start="2026-09-06 20:00:00+00"),  # exactly 72h ahead
        ],
    )
    nfl = cp.SportsSpec("nfl", ("moneyline", "spreads", "totals"), "main")
    assert [m["id"] for m in cp.select_sports_markets(ev, nfl, **WINDOW)] == ["14", "15"]
    # --no-open-only keeps the closed market too.
    ids = [m["id"] for m in cp.select_sports_markets(ev, nfl, open_only=False, **WINDOW)]
    assert ids == ["10", "14", "15"]


# --------------------------------------------------------------------------- #
# Entries
# --------------------------------------------------------------------------- #


def test_sports_entry_contract():
    entry = cp.sports_entry(mlb_game(), MLB, **WINDOW)
    assert entry is not None
    assert set(entry) == {
        "event_id",
        "title",
        "slug",
        "match_date",
        "closed",
        "active",
        "startDate",
        "endDate",
        "gameStartTime",
        "family",
        "record_markets",
    }
    assert entry["event_id"] == "930715" and entry["family"] == "sports:mlb"
    assert entry["match_date"] == "2026-09-04"  # from the slug (shared slug_date helper)
    assert entry["gameStartTime"] == "2026-09-05T01:40:00Z"  # normalized UTC ISO
    assert entry["closed"] is False and entry["active"] is True
    first = entry["record_markets"][0]
    assert first == {
        "id": "3973269",
        "conditionId": "0xc3973269",
        "clobTokenIds": ["3973269y", "3973269n"],
        "sportsMarketType": "moneyline",
        "line": None,
        "question": "q3973269",
        "outcomes": ["NYY", "SD"],
        "outcomePrices": ["0.545", "0.455"],
        "groupItemTitle": None,
        "closed": False,
        # the quote the main-line pick is made on (None when Gamma sends none)
        "bestBid": None,
        "bestAsk": None,
        "spread": None,
        "liquidityNum": None,
    }
    assert len(entry["record_markets"]) == 8 and len(cp.entry_tokens(entry)) == 16
    # An event with nothing selectable yields no entry.
    assert cp.sports_entry(event(9, "mlb-x-y-2026-09-04", []), MLB, **WINDOW) is None


def test_sports_entries_dedupe_across_tags_and_skip_closed_events():
    spec = cp.load_spec(SPEC_OBJ)
    shared = mlb_game()  # listed under both tags -> emitted once, under the first (mlb)
    closed = event(5, "cfb-a-b-2026-09-05", [mkt(50, "moneyline")], closed=True)
    events = {"mlb": [shared], "cfb": [shared, closed, epl_more_markets()], "epl": [epl_main()]}
    entries = cp.sports_entries(events, spec, NOW)
    assert [(e["event_id"], e["family"]) for e in entries] == [
        ("930715", "sports:mlb"),
        ("945983", "sports:cfb"),
        ("945719", "sports:epl"),
    ]
    # cfb's spec (spreads+totals, main) picks one spread + one total from the EPL props event.
    assert [m["sportsMarketType"] for m in entries[1]["record_markets"]] == ["spreads", "totals"]
    # An unknown tag simply contributes nothing.
    assert cp.sports_entries({"nba": [shared]}, spec, NOW) == []


# --------------------------------------------------------------------------- #
# Ladders
# --------------------------------------------------------------------------- #


def test_ladder_slugs_et_format_and_hours():
    spec = cp.LadderSpec(("bitcoin", "ethereum"), 2)
    slugs = cp.ladder_slugs(spec, NOW)  # 4pm EDT -> 3pm, 4pm, 5pm, 6pm
    assert slugs[:4] == [
        ("bitcoin", "bitcoin-above-on-september-3-2026-3pm-et"),
        ("bitcoin", "bitcoin-above-on-september-3-2026-4pm-et"),
        ("bitcoin", "bitcoin-above-on-september-3-2026-5pm-et"),
        ("bitcoin", "bitcoin-above-on-september-3-2026-6pm-et"),
    ]
    assert slugs[4][1] == "ethereum-above-on-september-3-2026-3pm-et" and len(slugs) == 8
    # Minutes are floored; a partial hour still maps to the same slugs.
    assert cp.ladder_slugs(spec, NOW.replace(minute=37, second=9)) == slugs


def test_ladder_slug_midnight_noon_and_day_rollover():
    # 03:30Z Sep 4 == 11:30pm EDT Sep 3 -> 10pm, 11pm (Sep 3), 12am, 1am (Sep 4)
    late = datetime(2026, 9, 4, 3, 30, tzinfo=timezone.utc)
    assert [s for _, s in cp.ladder_slugs(cp.LadderSpec(("bitcoin",), 2), late)] == [
        "bitcoin-above-on-september-3-2026-10pm-et",
        "bitcoin-above-on-september-3-2026-11pm-et",
        "bitcoin-above-on-september-4-2026-12am-et",
        "bitcoin-above-on-september-4-2026-1am-et",
    ]
    noon = datetime(2026, 9, 3, 16, 0, tzinfo=timezone.utc)  # 12pm EDT
    assert cp.ladder_slug("ethereum", noon) == "ethereum-above-on-september-3-2026-12pm-et"
    # Winter: EST (UTC-5) — 2026-12-01T17:00Z is 12pm EST; day/month without zero padding.
    winter = datetime(2026, 12, 1, 17, 0, tzinfo=timezone.utc)
    assert cp.ladder_slug("bitcoin", winter) == "bitcoin-above-on-december-1-2026-12pm-et"


def test_ladder_slugs_unique_across_dst_fall_back():
    # 2026-11-01 06:00Z is 1am EST, one hour after 1am EDT (05:00Z): the repeated hour
    # yields the same slug twice; the list stays unique.
    now = datetime(2026, 11, 1, 5, 30, tzinfo=timezone.utc)
    slugs = cp.ladder_slugs(cp.LadderSpec(("bitcoin",), 2), now)
    assert len(slugs) == len(set(slugs)) == 3
    assert slugs[1][1] == "bitcoin-above-on-november-1-2026-1am-et"


@pytest.mark.parametrize(
    "utc, local, name",
    [
        # 2026: DST starts Mar 8 07:00Z, ends Nov 1 06:00Z.
        (datetime(2026, 3, 8, 6, 59, tzinfo=timezone.utc), (2026, 3, 8, 1, 59), "EST"),
        (datetime(2026, 3, 8, 7, 0, tzinfo=timezone.utc), (2026, 3, 8, 3, 0), "EDT"),
        (datetime(2026, 9, 3, 20, 0, tzinfo=timezone.utc), (2026, 9, 3, 16, 0), "EDT"),
        (datetime(2026, 11, 1, 5, 59, tzinfo=timezone.utc), (2026, 11, 1, 1, 59), "EDT"),
        (datetime(2026, 11, 1, 6, 0, tzinfo=timezone.utc), (2026, 11, 1, 1, 0), "EST"),
        (datetime(2026, 12, 1, 17, 0, tzinfo=timezone.utc), (2026, 12, 1, 12, 0), "EST"),
        # 2027: Mar 14 / Nov 7.
        (datetime(2027, 3, 14, 7, 0, tzinfo=timezone.utc), (2027, 3, 14, 3, 0), "EDT"),
        (datetime(2027, 11, 7, 6, 0, tzinfo=timezone.utc), (2027, 11, 7, 1, 0), "EST"),
    ],
)
def test_us_eastern_fallback_rule(utc, local, name):
    # The no-tz-database fallback must agree with the IANA rule at the DST edges...
    got = utc.astimezone(cp._USEastern())
    assert (got.year, got.month, got.day, got.hour, got.minute) == local
    assert got.tzname() == name and got.astimezone(timezone.utc) == utc
    # ...and with zoneinfo itself wherever a tz database is installed.
    try:
        from zoneinfo import ZoneInfo

        ref = utc.astimezone(ZoneInfo("America/New_York"))
    except Exception:  # noqa: BLE001 - no tz database on this host
        return
    assert (ref.year, ref.month, ref.day, ref.hour, ref.minute, ref.fold) == (*local, got.fold)


def test_eastern_is_a_tzinfo_and_slugs_do_not_depend_on_the_source():
    assert isinstance(cp.eastern(), tzinfo)
    when = datetime(2026, 11, 1, 6, 30, tzinfo=timezone.utc)  # 1:30am EST, after fall-back
    assert cp.ladder_slug("bitcoin", when) == "bitcoin-above-on-november-1-2026-1am-et"


def test_ladder_entry_records_all_strikes_and_honours_open_only():
    entry = cp.ladder_entry(ladder(), "bitcoin")
    assert entry is not None and entry["family"] == "ladder:bitcoin"
    assert entry["event_id"] == "960280" and entry["match_date"] == "2026-09-03"
    assert entry["gameStartTime"] == "2026-09-03T18:40:15Z"  # the ladder's open
    assert len(entry["record_markets"]) == 20 and len(cp.entry_tokens(entry)) == 40
    assert entry["record_markets"][0]["groupItemTitle"] == "83,000"
    assert entry["record_markets"][0]["sportsMarketType"] is None
    assert cp.ladder_entry(None, "bitcoin") is None
    assert cp.ladder_entry({}, "bitcoin") is None
    assert cp.ladder_entry(ladder(closed=True), "bitcoin") is None
    kept = cp.ladder_entry(ladder(closed=True), "bitcoin", open_only=False)
    assert kept is not None and kept["closed"] is True
    # A strike without token ids is skipped; an event with none is dropped.
    bare = ladder(n=2)
    bare["markets"][0]["clobTokenIds"] = "[]"
    assert len(cp.ladder_entry(bare, "bitcoin")["record_markets"]) == 1
    for m in bare["markets"]:
        m["clobTokenIds"] = None
    assert cp.ladder_entry(bare, "bitcoin") is None


def test_ladder_entry_drops_a_resolved_ladder_gamma_has_not_closed_yet():
    # Live: 33 min after the 4PM ET ladder resolved, Gamma still said closed=false with
    # one strike open -> a 1-market entry, and one more restart when it finally flips.
    lad = ladder()  # endDate 2026-09-03T20:00:00Z, closed=false, 20 open strikes
    assert cp.ladder_entry(lad, "bitcoin", now=NOW) is not None  # at the resolution instant
    assert cp.ladder_entry(lad, "bitcoin", now=NOW + cp.LADDER_GRACE) is not None  # in grace
    late = NOW + cp.LADDER_GRACE + timedelta(seconds=1)
    assert cp.ladder_entry(lad, "bitcoin", now=late) is None
    assert cp.ladder_finished(lad, late) and not cp.ladder_finished(lad, None)
    assert cp.ladder_entry(lad, "bitcoin", now=late, open_only=False) is not None  # --no-open-only
    lad["endDate"] = None
    assert cp.ladder_entry(lad, "bitcoin", now=late) is not None  # unknown end: the flag decides


def test_summarize_lists_every_spec_family_even_when_empty():
    # An off-season league, a typo'd tag and a truncated listing must not all look the
    # same (a family silently missing from the summary): every spec family gets a row.
    spec = cp.load_spec(SPEC_OBJ)
    assert cp.spec_families(spec) == [
        "sports:mlb",
        "sports:cfb",
        "sports:epl",
        "ladder:bitcoin",
        "ladder:ethereum",
    ]
    entries = cp.sports_entries({"mlb": [mlb_game()]}, spec, NOW)
    rows = cp.summarize(entries, families=cp.spec_families(spec))
    assert [(r["family"], r["events"]) for r in rows] == [
        ("ladder:bitcoin", 0),
        ("ladder:ethereum", 0),
        ("sports:cfb", 0),
        ("sports:epl", 0),
        ("sports:mlb", 1),
        ("TOTAL", 1),
    ]
    assert "sports:cfb" in cp.format_summary(rows)


# --------------------------------------------------------------------------- #
# Ordering, sharding, summary
# --------------------------------------------------------------------------- #


def test_shard_cap_and_count_match_the_recorder():
    assert cp.SHARD_CAP == clob._SHARD_CAP
    rng = random.Random(7)
    for _ in range(200):
        groups = [
            [f"t{i}-{j}" for j in range(rng.randint(0, 60))] for i in range(rng.randint(0, 30))
        ]
        assert cp.count_shards(groups) == len(clob.shard_tokens(groups))
    assert cp.count_shards([]) == 0
    assert cp.count_shards([["a"] * 180, ["b"]]) == 2
    with pytest.raises(ValueError):
        cp.count_shards([["a"] * 181])
    with pytest.raises(ValueError):
        clob.shard_tokens([["a"] * 181])


def test_sort_entries_schedule_order_unknown_last():
    e1 = {
        "gameStartTime": "2026-09-05T01:40:00Z",
        "family": "sports:mlb",
        "slug": "b",
        "event_id": "1",
    }
    e2 = {
        "gameStartTime": "2026-09-04T19:00:00Z",
        "family": "sports:epl",
        "slug": "a",
        "event_id": "2",
    }
    e3 = {"gameStartTime": None, "family": "ladder:bitcoin", "slug": "z", "event_id": "3"}
    e4 = {
        "gameStartTime": "2026-09-05T01:40:00Z",
        "family": "ladder:bitcoin",
        "slug": "c",
        "event_id": "4",
    }
    assert [e["event_id"] for e in cp.sort_entries([e1, e3, e2, e4])] == ["2", "4", "1", "3"]


def test_summarize_rows_and_total_shards():
    spec = cp.load_spec(SPEC_OBJ)
    entries = cp.sports_entries({"mlb": [mlb_game()], "epl": [epl_main()]}, spec, NOW)
    entries.append(cp.ladder_entry(ladder(), "bitcoin"))
    # cap=50: mlb (16) + epl (6) share a shard, the 40-token ladder needs its own.
    rows = cp.summarize(entries, cap=50)
    assert rows == [
        {"family": "ladder:bitcoin", "events": 1, "markets": 20, "tokens": 40, "shards": 1},
        {"family": "sports:epl", "events": 1, "markets": 3, "tokens": 6, "shards": 1},
        {"family": "sports:mlb", "events": 1, "markets": 8, "tokens": 16, "shards": 1},
        {"family": "TOTAL", "events": 3, "markets": 31, "tokens": 62, "shards": 2},
    ]
    # Ladder first: 40 + 16 > 50 -> mlb opens a second shard that epl then joins.
    assert cp.summarize([entries[2], entries[0], entries[1]], cap=50)[-1]["shards"] == 2
    assert cp.summarize(entries)[-1]["shards"] == 1  # all 62 fit one real 180-token shard
    text = cp.format_summary(rows, cap=50)
    assert "shards@50" in text and "TOTAL" in text and "sports:mlb" in text


# --------------------------------------------------------------------------- #
# The script (network faked through _get)
# --------------------------------------------------------------------------- #


class FakeGamma:
    """Routes ``_get`` calls: tag listings (paged) and slug lookups; records every request."""

    def __init__(self, pages: dict[str, list[list[dict]]], by_slug: dict[str, dict] | None = None):
        self.pages = pages  # tag -> list of pages (each a list of events)
        self.by_slug = by_slug or {}
        self.calls: list[dict] = []
        self.fail_422_at: dict[str, int] = {}  # tag -> offset that 422s

    def __call__(self, path, params):
        assert path == "/events"
        self.calls.append(dict(params))
        if "slug" in params:
            ev = self.by_slug.get(params["slug"])
            return [ev] if ev else []
        tag, offset = params["tag_slug"], int(params["offset"])
        if self.fail_422_at.get(tag) == offset:
            raise urllib.error.HTTPError("u", 422, "offset too large", None, None)
        pages = self.pages.get(tag, [])
        idx = offset // lce.PAGE
        return pages[idx] if idx < len(pages) else []


@pytest.fixture
def no_delay(monkeypatch):
    monkeypatch.setattr(lce, "PAGE_DELAY", 0)


def test_fetch_tag_events_pages_fully_and_stops_on_short_page(no_delay, monkeypatch):
    full = [event(i, f"mlb-a-b-2026-09-0{i % 9 + 1}", []) for i in range(100)]
    short = [event(100, "mlb-c-d-2026-09-05", [])]
    fake = FakeGamma({"mlb": [full, short]})
    monkeypatch.setattr(lce, "_get", fake)
    got = lce.fetch_tag_events("mlb")
    assert len(got) == 101
    assert [c["offset"] for c in fake.calls] == [0, 100]
    assert fake.calls[0]["closed"] == "false" and fake.calls[0]["limit"] == 100
    assert fake.calls[0]["order"] == "startDate" and fake.calls[0]["ascending"] == "false"
    # An empty first page is fine (unknown tag): no events, one request.
    fake.calls.clear()
    assert lce.fetch_tag_events("nope") == [] and len(fake.calls) == 1


def test_fetch_tag_events_handles_gamma_offset_cap(no_delay, monkeypatch):
    full = [event(i, "cfb-a-b-2026-09-05", []) for i in range(100)]
    fake = FakeGamma({"cfb": [full] * 40})  # would page forever
    fake.fail_422_at["cfb"] = 300  # Gamma's cap: keep the 3 pages fetched before it
    monkeypatch.setattr(lce, "_get", fake)
    assert len(lce.fetch_tag_events("cfb")) == 300
    # Without a 422, the local cap stops at MAX_OFFSET (20 pages of 100).
    fake.calls.clear()
    fake.fail_422_at.clear()
    assert len(lce.fetch_tag_events("cfb")) == lce.MAX_OFFSET
    assert len(fake.calls) == lce.MAX_OFFSET // lce.PAGE
    # Other HTTP errors propagate (main reports them and exits 1).
    fake.fail_422_at.clear()

    def boom(path, params):
        raise urllib.error.HTTPError("u", 500, "boom", None, None)

    monkeypatch.setattr(lce, "_get", boom)
    with pytest.raises(urllib.error.HTTPError):
        lce.fetch_tag_events("cfb")


def test_fetch_tag_events_reports_pages_and_truncation(no_delay, monkeypatch):
    full = [event(i, "cfb-a-b-2026-09-05", []) for i in range(100)]
    fake = FakeGamma({"cfb": [full, full, [event(1000, "x", [])]], "mlb": [[]]})
    monkeypatch.setattr(lce, "_get", fake)
    stats: dict[str, object] = {}
    assert len(lce.fetch_tag_events("cfb", stats=stats)) == 201
    assert stats == {"events": 201, "pages": 3, "truncated": None}
    stats = {}
    assert lce.fetch_tag_events("mlb", stats=stats) == []
    assert stats == {"events": 0, "pages": 1, "truncated": None}  # a typo'd tag looks like this
    fake = FakeGamma({"cfb": [full] * 40})
    fake.fail_422_at["cfb"] = 300
    monkeypatch.setattr(lce, "_get", fake)
    stats = {}
    assert len(lce.fetch_tag_events("cfb", stats=stats)) == 300
    assert stats["truncated"] == "Gamma 422 at offset 300" and stats["pages"] == 3
    fake.fail_422_at.clear()
    stats = {}
    assert len(lce.fetch_tag_events("cfb", stats=stats)) == lce.MAX_OFFSET
    assert stats["truncated"] == f"local cap at offset {lce.MAX_OFFSET}"
    line = lce.format_tag_stats(
        {
            "mlb": {"events": 343, "pages": 4, "truncated": None},
            "cfb": stats,
            "nope": {"events": 0, "pages": 1, "truncated": None},
        }
    )
    assert line == (
        f"tags: mlb=343/4p cfb={lce.MAX_OFFSET}/{lce.MAX_OFFSET // lce.PAGE}p nope=0/1p"
        f"  TRUNCATED: cfb (local cap at offset {lce.MAX_OFFSET})"
    )
    assert lce.format_tag_stats({}) == "tags: (none)"


def test_fetch_event_by_slug(monkeypatch):
    fake = FakeGamma({}, {"bitcoin-above-on-september-3-2026-4pm-et": ladder()})
    monkeypatch.setattr(lce, "_get", fake)
    assert lce.fetch_event_by_slug("bitcoin-above-on-september-3-2026-4pm-et")["id"] == "960280"
    assert lce.fetch_event_by_slug("bitcoin-above-on-september-3-2026-9pm-et") is None

    def not_found(path, params):
        raise urllib.error.HTTPError("u", 404, "nope", None, None)

    monkeypatch.setattr(lce, "_get", not_found)
    assert lce.fetch_event_by_slug("x") is None


def _write_spec(tmp_path, obj=SPEC_OBJ):
    path = tmp_path / "campaign.json"
    path.write_text(json.dumps(obj), encoding="utf-8")
    return path


def test_main_end_to_end_writes_contract_and_summary(tmp_path, monkeypatch, capsys, no_delay):
    by_slug = {
        "bitcoin-above-on-september-3-2026-4pm-et": ladder(),
        "bitcoin-above-on-september-3-2026-3pm-et": ladder("960279", closed=True),
        "ethereum-above-on-september-3-2026-5pm-et": ladder(
            "960301", "ethereum-above-on-september-3-2026-5pm-et", n=5
        ),
    }
    fake = FakeGamma(
        {"mlb": [[mlb_game()]], "cfb": [[]], "epl": [[epl_main(), epl_more_markets()]]},
        by_slug,
    )
    monkeypatch.setattr(lce, "_get", fake)
    out = tmp_path / "campaign_events.json"
    rc = lce.main(
        ["--spec", str(_write_spec(tmp_path)), "--out", str(out), "--now", "2026-09-03T20:00:00Z"]
    )
    assert rc == 0
    data = json.loads(out.read_text(encoding="utf-8"))
    # Schedule order: EPL game (Sep 4 19:00Z) before MLB (Sep 5 01:40Z); ladders by their open.
    assert [(e["event_id"], e["family"]) for e in data] == [
        ("960280", "ladder:bitcoin"),
        ("960301", "ladder:ethereum"),
        ("945719", "sports:epl"),
        ("945983", "sports:epl"),
        ("930715", "sports:mlb"),
    ]
    for entry in data:
        assert {"event_id", "title", "slug", "closed", "active", "startDate", "endDate"} <= set(
            entry
        )
        assert {"gameStartTime", "family", "record_markets"} <= set(entry)
        for m in entry["record_markets"]:
            assert {
                "id",
                "conditionId",
                "clobTokenIds",
                "sportsMarketType",
                "line",
                "question",
            } <= set(m)
            assert m["clobTokenIds"]
    # The recorder's existing matches-file loader reads it as-is.
    assert load_matches(str(out)).event_ids == ("960280", "960301", "945719", "945983", "930715")
    # Ladder slugs were looked up for now-1..now+2 for both assets; the closed one was dropped.
    looked_up = [c["slug"] for c in fake.calls if "slug" in c]
    assert len(looked_up) == 8 and "bitcoin-above-on-september-3-2026-3pm-et" in looked_up
    # Summary table on stdout; the per-tag diagnostics on stderr (for the journal).
    captured = capsys.readouterr()
    text = captured.out
    assert "campaign 'maker' @ 2026-09-03T20:00:00Z: 5 event(s)" in text
    assert "shards@180" in text and "TOTAL" in text
    assert "sports:mlb" in text and "ladder:ethereum" in text
    assert "New York Yankees vs. San Diego Padres" in text
    assert re.search(r"^sports:cfb\s+0\s+0\s+0\s+0$", text, re.M)  # a 0 row, not a missing one
    assert "tags: mlb=1/1p cfb=0/1p epl=2/1p" in captured.err and "TRUNCATED" not in captured.err


def test_main_previous_keeps_the_pick_and_tolerates_a_bad_previous_file(
    tmp_path, monkeypatch, capsys, no_delay
):
    spec = {"sports": [{"tag": "cfb", "types": ["moneyline", "spreads"], "lines": "main"}]}
    lines = cmu_unm_spreads()
    game = event(77, "cfb-cmich-unm-2026-09-04", [mkt(1, "moneyline", ("0.6", "0.4")), *lines])
    monkeypatch.setattr(lce, "_get", FakeGamma({"cfb": [[game]]}))
    out = tmp_path / "events.json"
    args = [
        "--spec",
        str(_write_spec(tmp_path, spec)),
        "--out",
        str(out),
        "--now",
        "2026-09-03T20:00:00Z",
    ]

    def picked():
        return [m["id"] for m in json.loads(out.read_text(encoding="utf-8"))[0]["record_markets"]]

    assert lce.main(args) == 0 and picked() == ["1", "3988846"]
    assert "tags: cfb=1/1p" in capsys.readouterr().err
    installed = tmp_path / "installed.json"
    installed.write_bytes(out.read_bytes())
    # The odds moved: a fresh pick flips to -11.5; with --previous the installed pick stays.
    by_id = {m["id"]: m for m in lines}
    by_id["3988845"]["outcomePrices"] = '["0.5", "0.5"]'
    by_id["3988846"]["outcomePrices"] = '["0.55", "0.45"]'
    assert lce.main(args) == 0 and picked() == ["1", "3988845"]
    assert lce.main([*args, "--previous", str(installed)]) == 0 and picked() == ["1", "3988846"]
    # A missing / malformed previous file is reported and simply means no stickiness.
    assert lce.main([*args, "--previous", str(tmp_path / "missing.json")]) == 0
    assert picked() == ["1", "3988845"]
    assert "previous events file unusable" in capsys.readouterr().err
    (tmp_path / "obj.json").write_text("{}", encoding="utf-8")
    assert lce.main([*args, "--previous", str(tmp_path / "obj.json")]) == 0
    assert picked() == ["1", "3988845"]
    assert "not a JSON list" in capsys.readouterr().err
    assert lce.load_previous(None) == {} and lce.load_previous("") == {}


def test_main_no_open_only_keeps_closed_ladder(tmp_path, monkeypatch, capsys, no_delay):
    spec = {"ladders": {"assets": ["bitcoin"], "hours_ahead": 0}}
    by_slug = {"bitcoin-above-on-september-3-2026-3pm-et": ladder("960279", closed=True)}
    fake = FakeGamma({}, by_slug)
    monkeypatch.setattr(lce, "_get", fake)
    out = tmp_path / "o.json"
    args = [
        "--spec",
        str(_write_spec(tmp_path, spec)),
        "--out",
        str(out),
        "--now",
        "2026-09-03T20:00:00Z",
    ]
    assert lce.main(args) == 0
    assert json.loads(out.read_text(encoding="utf-8")) == []
    assert lce.main([*args, "--no-open-only"]) == 0
    data = json.loads(out.read_text(encoding="utf-8"))
    assert [e["event_id"] for e in data] == ["960279"] and data[0]["closed"] is True
    assert (
        load_matches(str(out)).event_ids == ()
    )  # the recorder's default --open-only still skips it
    assert load_matches(str(out), open_only=False).event_ids == ("960279",)


def test_main_reports_gamma_failure_and_bad_spec(tmp_path, monkeypatch, capsys, no_delay):
    def down(path, params):
        raise urllib.error.URLError("no route")

    monkeypatch.setattr(lce, "_get", down)
    out = tmp_path / "o.json"
    assert lce.main(["--spec", str(_write_spec(tmp_path)), "--out", str(out)]) == 1
    assert not out.exists()
    assert "error fetching from Gamma" in capsys.readouterr().err
    bad = _write_spec(tmp_path, {"sports": [{"tag": "mlb", "lines": "best"}]})
    assert lce.main(["--spec", str(bad), "--out", str(out)]) == 2
    assert lce.main(["--spec", str(tmp_path / "missing.json"), "--out", str(out)]) == 2
    assert lce.main(["--spec", str(_write_spec(tmp_path)), "--out", str(out), "--now", "?"]) == 2


def test_production_spec_loads():
    path = Path(__file__).resolve().parents[1] / "deploy" / "campaign.json"
    spec = cp.load_spec(json.loads(path.read_text(encoding="utf-8")))
    assert spec.run_name == "maker" and spec.lookahead_h == 72 and spec.lookback_h == 8
    tags = [s.tag for s in spec.sports]
    assert tags[:3] == ["mlb", "nfl", "cfb"]
    # Gamma's own tag slugs (from /sports + /tags): Serie A is "sea", not "serie-a".
    assert {"epl", "la-liga", "ucl", "bundesliga", "sea", "ligue-1", "nba", "nhl"} <= set(tags)
    assert {s.lines for s in spec.sports if s.tag != "mlb"} == {"main"}
    assert spec.sports[0].lines == "all"
    assert spec.ladders == cp.LadderSpec(("bitcoin", "ethereum"), 2)
