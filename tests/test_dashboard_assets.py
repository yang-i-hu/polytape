"""Guard the dashboard's inline JS wiring.

The dashboard is a single self-contained ``index.html`` (the project is
deliberately Node-free, so there is no JS test runner). These string-level
assertions are a cheap regression net: they fail loudly if a handler/endpoint
that the live verification relied on is renamed or dropped, even though they
don't execute the JS.
"""

from __future__ import annotations

from importlib import resources


def _index_html() -> str:
    return resources.files("polytape.monitor").joinpath("index.html").read_text(encoding="utf-8")


def test_related_events_feature_is_wired():
    html = _index_html()
    required = [
        'id="relatedbtn"',  # the button exists
        "findRelated",  # fetches /api/related
        "renderRelated",  # renders the list
        "/api/related",  # hits the endpoint
        "data-record",  # Record button carries the id
    ]
    missing = [tok for tok in required if tok not in html]
    assert not missing, f"dashboard is missing related-events wiring: {missing}"


def test_dashboard_sends_no_dead_recorder_params():
    """Regression: the recorder CLI defines no stream/hash toggles. The dashboard
    must not send start params that the control plane would map to them."""
    html = _index_html()
    assert "entity_type:" not in html
    assert "series_comments" not in html
    assert "comments:" not in html  # no comments toggle in a request body
    assert 'id="in-comments"' not in html
    assert 'id="in-book"' not in html
    assert 'id="in-hash"' not in html
