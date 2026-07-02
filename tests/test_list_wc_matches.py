"""Tests for the match-discovery script (scripts/list_wc_matches.py) — offline, Gamma faked."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

from polytape.admin import registry as reg

_SPEC = importlib.util.spec_from_file_location(
    "list_wc_matches",
    Path(__file__).resolve().parents[1] / "scripts" / "list_wc_matches.py",
)
wc = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(wc)


def test_slug_date_is_the_shared_helper():
    # The heuristic lives ONCE, in polytape.admin.registry; the script imports it.
    assert wc.slug_date is reg.slug_date


def _event() -> dict:
    return {
        "id": "1001",
        "title": "A vs. B",
        "slug": "fifwc-a-b-2026-06-19",
        "closed": False,
        "active": True,
        "markets": [
            {
                "sportsMarketType": "moneyline",
                "question": "Will A win?",
                "groupItemTitle": "A",
                "conditionId": "0xA",
                "outcomes": '["Yes", "No"]',
                "clobTokenIds": '["t1", "t2"]',
                "closed": False,
            }
        ],
    }


def test_tag_flag_threads_to_gamma(tmp_path, monkeypatch, capsys):
    seen: list[str] = []

    def fake_get(path, params):
        seen.append(params["tag_slug"])
        return [_event()]  # single short page -> no paging, no sleep

    monkeypatch.setattr(wc, "_get", fake_get)
    out = tmp_path / "matches.json"
    assert wc.main(["--out", str(out), "--tag", "premier-league"]) == 0
    assert seen == ["premier-league", "premier-league"]  # open + closed sweeps
    data = json.loads(out.read_text(encoding="utf-8"))
    assert data[0]["match_date"] == "2026-06-19"  # via the shared slug_date
    assert "premier-league" in capsys.readouterr().out  # summary names the tag


def test_tag_defaults_to_env_then_world_cup(tmp_path, monkeypatch):
    seen: list[str] = []
    monkeypatch.setattr(wc, "_get", lambda path, params: seen.append(params["tag_slug"]) or [])
    monkeypatch.setenv("POLYTAPE_TAG_SLUG", "serie-a")
    assert wc.main(["--out", str(tmp_path / "m.json"), "--open-only"]) == 0
    assert seen == ["serie-a"]
    seen.clear()
    monkeypatch.delenv("POLYTAPE_TAG_SLUG")
    assert wc.main(["--out", str(tmp_path / "m.json"), "--open-only"]) == 0
    assert seen == ["fifa-world-cup"]
