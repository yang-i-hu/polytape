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
        'id="activechatbtn"',          # the button exists
        "findActiveChat",              # fetches /api/active-chat
        "renderActiveChat",            # renders the ranked list
        "/api/active-chat",            # hits the endpoint
        "data-chatrec",                # Record button carries the id
        "data-chattype",               # ...and the entity type (Event/Series)
        "entity_type",                 # ...which is sent on the start request
    ]
    missing = [tok for tok in required if tok not in html]
    assert not missing, f"dashboard is missing active-chat wiring: {missing}"


def test_record_from_active_chat_is_comments_only():
    # The Record button on a busy chat must start a comments-only capture (book off),
    # so it works for a parent-Series id that has no markets.
    html = _index_html()
    # the chatrec handler builds a body with comments:true and book:false
    assert "comments: true, book: false" in html
