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


def test_active_chat_feature_is_wired():
    html = _index_html()
    required = [
        'id="activechatbtn"',  # the button exists
        "findActiveChat",  # fetches /api/active-chat
        "renderActiveChat",  # renders the ranked list
        "/api/active-chat",  # hits the endpoint
        "data-chatrec",  # Record button carries the id (Event rows only)
        "series chat records automatically",  # Series rows: disabled button + tooltip
    ]
    missing = [tok for tok in required if tok not in html]
    assert not missing, f"dashboard is missing active-chat wiring: {missing}"


def test_record_from_active_chat_is_comments_only():
    # The Record button on a busy (Event) chat must start a comments-only capture
    # (book off) — chat-first sessions rarely want the book noise.
    html = _index_html()
    # the chatrec handler builds a body with comments:true and book:false
    assert "comments: true, book: false" in html


def test_dashboard_sends_no_dead_recorder_params():
    """Regression: the recorder CLI defines neither ``--include-series-comments``
    nor ``--entity-type``. The dashboard must not send start params that the
    control plane would map to them — series chat is always recorded, and a bare
    Series capture is unsupported (its Record button is disabled instead)."""
    html = _index_html()
    # `parent_entity_type` (reading the API response) is fine; an `entity_type:`
    # key in a request body is not.
    assert "entity_type:" not in html
    assert "series_comments" not in html
    assert 'id="in-series"' not in html  # the vestigial "series chat" checkbox
