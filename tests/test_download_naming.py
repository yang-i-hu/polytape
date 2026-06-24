"""Tests for the human-friendly download filename (event id + FIFA team codes)."""

from __future__ import annotations

from polytape.admin import download as dl


def test_team_code_known_fifa_codes():
    # The non-obvious ones are exactly why a map (not a heuristic) is used.
    assert dl.team_code("United States") == "USA"
    assert dl.team_code("Korea Republic") == "KOR"
    assert dl.team_code("IR Iran") == "IRN"
    assert dl.team_code("Türkiye") == "TUR"
    assert dl.team_code("South Africa") == "RSA"
    assert dl.team_code("Saudi Arabia") == "KSA"
    assert dl.team_code("Cabo Verde") == "CPV"
    assert dl.team_code("DR Congo") == "COD"
    assert dl.team_code("Côte d'Ivoire") == "CIV"
    assert dl.team_code("Curaçao") == "CUW"
    assert dl.team_code(" Australia ") == "AUS"  # tolerant of surrounding space


def test_team_code_fallback_for_unmapped():
    assert dl.team_code("Wakanda") == "WAK"  # derived: first 3 A-Z, uppercased
    assert dl.team_code("X") == "X"
    assert dl.team_code("123") == "UNK"  # nothing A-Z -> safe placeholder


def test_match_archive_name_uses_both_sides():
    assert (
        dl.match_archive_name("351743", {"title": "United States vs. Australia"})
        == "event-351743-USA-AUS.tar.gz"
    )
    # 'vs' without the period also splits.
    assert (
        dl.match_archive_name("999", {"title": "Spain vs Saudi Arabia"})
        == "event-999-ESP-KSA.tar.gz"
    )


def test_match_archive_name_falls_back_when_unparseable():
    assert dl.match_archive_name("5", {"title": "TBD"}) == "event-5.tar.gz"  # no 'vs'
    assert dl.match_archive_name("5", {}) == "event-5.tar.gz"  # no title
    assert dl.match_archive_name("5", None) == "event-5.tar.gz"  # no entry
