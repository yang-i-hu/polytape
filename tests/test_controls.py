"""Tests for the admin control-plane security primitives (offline; no fastapi)."""

from __future__ import annotations

import json

import pytest

from polytape.admin import control


def test_actions_excludes_stop():
    # Operator-stop is intentionally absent: systemctl stop is unrecoverable.
    assert "stop" not in control.ACTIONS
    assert set(control.ACTIONS) == {"restart", "refresh", "arm-heartbeat"}


def test_validate_heartbeat_url_accepts_real_urls():
    assert control.validate_heartbeat_url(
        "https://hc-ping.com/0d8e7c2a-1111-2222-3333-444455556666"
    )
    assert control.validate_heartbeat_url("https://example.com:8443/ping/abc-123")
    assert control.validate_heartbeat_url("https://h.example.com/p?token=ab12%20")


def test_validate_heartbeat_url_rejects_injection_and_bad_scheme():
    bad = [
        "http://hc-ping.com/x",  # not https
        "https://hc-ping.com/x\nPOLYTAPE_SALT=evil",  # newline -> env-file injection
        "https://hc-ping.com/x\rPOLYTAPE_SALT=evil",
        "https://hc-ping.com/\x00",  # NUL
        'https://hc-ping.com/"x"',  # quote
        "https://hc-ping.com/`id`",  # backtick
        "https://hc-ping.com/$SALT",  # dollar (env expansion)
        "https://hc-ping.com/a b",  # raw space
        "ftp://hc-ping.com/x",
        "",
        "https://",
        "https://x/" + "a" * 400,  # too long
        12345,  # not a string
    ]
    for u in bad:
        assert not control.validate_heartbeat_url(u), repr(u)


def test_token_ok_constant_time_compare():
    assert control.token_ok("s3cr3t", "s3cr3t")
    assert not control.token_ok("wrong", "s3cr3t")
    assert not control.token_ok("s3cr3t", None)  # no secret configured -> controls off
    assert not control.token_ok("", "s3cr3t")
    assert not control.token_ok(None, "s3cr3t")


def test_fingerprint_is_short_and_not_the_value():
    fp = control.fingerprint("https://hc-ping.com/secret-uuid")
    assert len(fp) == 12 and "hc-ping" not in fp


def test_sessions_mint_validate_expire_drop():
    clock = [1000.0]
    s = control.Sessions(ttl_s=100.0, mono=lambda: clock[0])
    sid = s.mint()
    assert s.valid(sid)
    assert not s.valid("nope") and not s.valid(None)
    clock[0] = 1101.0  # past the TTL
    assert not s.valid(sid)
    sid2 = s.mint()
    assert s.valid(sid2)
    s.drop(sid2)
    assert not s.valid(sid2)


def test_rate_limiter_min_interval_per_action():
    clock = [0.0]
    rl = control.RateLimiter(mono=lambda: clock[0])
    assert rl.allow("restart", 30.0)  # first attempt
    assert not rl.allow("restart", 30.0)  # too soon
    clock[0] = 31.0
    assert rl.allow("restart", 30.0)  # after the interval
    assert rl.allow("refresh", 30.0)  # independent per action


def test_audit_log_appends_jsonl(tmp_path):
    path = tmp_path / "logs" / "audit.jsonl"  # parent dir created on demand
    a = control.AuditLog(path, now=lambda: "2026-06-19T00:00:00.000000Z")
    a.write(action="restart", result="ok", session_fp="abc123")
    a.write(action="arm-heartbeat", result="denied")
    lines = path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 2
    rec = json.loads(lines[0])
    assert rec["action"] == "restart" and rec["result"] == "ok" and rec["ts"].endswith("Z")


def test_audit_log_swallows_write_errors(tmp_path):
    blocker = tmp_path / "blocker"
    blocker.write_text("x", encoding="utf-8")  # a file where a dir is expected
    a = control.AuditLog(blocker / "sub" / "audit.jsonl")
    a.write(action="restart", result="ok")  # must silently no-op, never raise


def test_intent_broker_writes_atomic_action_file(tmp_path):
    intent = tmp_path / "intent"
    b = control.IntentBroker(intent, now=lambda: "2026-06-19T00:00:00Z")
    b.dispatch("restart")
    assert (intent / "restart").exists()
    assert not (intent / "restart.tmp").exists()  # temp cleaned up by os.replace


def test_intent_broker_stages_heartbeat_url_separately(tmp_path):
    intent = tmp_path / "intent"
    b = control.IntentBroker(intent, now=lambda: "2026-06-19T00:00:00Z")
    b.dispatch("arm-heartbeat", url="https://hc-ping.com/abc")
    assert (intent / "arm-heartbeat").exists()
    staged = (intent.parent / "heartbeat.url").read_text(encoding="utf-8").strip()
    assert staged == "https://hc-ping.com/abc"  # URL in staging file, not name/argv


def test_intent_broker_rejects_unknown_action(tmp_path):
    b = control.IntentBroker(tmp_path / "intent")
    with pytest.raises(ValueError):
        b.dispatch("stop")  # not in ACTIONS


# --- endpoint-level tests (require fastapi; installed via the dev extra) ------- #

pytest.importorskip("fastapi")

from fastapi.testclient import TestClient  # noqa: E402

from polytape.admin.app import create_app  # noqa: E402
from polytape.admin.reader import RunReader  # noqa: E402


class _FakeBroker:
    """Records dispatch calls instead of writing real intent files / running systemctl."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str | None]] = []

    def dispatch(self, action: str, *, url: str | None = None) -> None:
        self.calls.append((action, url))


def _reader(tmp_path):
    (tmp_path / "meta.json").write_text(
        json.dumps({"started_at": "2026-06-19T00:00:00Z", "events": []}), encoding="utf-8"
    )
    return RunReader(tmp_path, env_file=tmp_path / "x.env", matches_file=tmp_path / "x.json")


def _app(tmp_path, *, secret="s3cr3t"):
    broker = _FakeBroker()
    audit = control.AuditLog(tmp_path / "audit.jsonl")
    app = create_app(
        _reader(tmp_path), poll_interval=3600, admin_token=secret, broker=broker, audit=audit
    )
    return app, broker, audit


def test_endpoint_controls_disabled_without_secret(tmp_path):
    app = create_app(_reader(tmp_path), poll_interval=3600, admin_token=None, broker=_FakeBroker())
    with TestClient(app) as c:
        assert c.get("/api/session").json()["controls_enabled"] is False
        assert c.post("/api/control/restart", json={"confirm": "polytape"}).status_code == 503


def test_endpoint_requires_login_then_succeeds(tmp_path):
    app, broker, _ = _app(tmp_path)
    with TestClient(app) as c:
        assert (
            c.post("/api/control/restart", json={"confirm": "polytape"}).status_code == 403
        )  # no login
        assert c.post("/api/login", json={"token": "nope"}).status_code == 403  # bad secret
        assert c.post("/api/login", json={"token": "s3cr3t"}).status_code == 200  # good -> cookie
        assert c.get("/api/session").json()["authed"] is True
        assert c.post("/api/control/restart", json={}).status_code == 400  # missing confirm
        r = c.post("/api/control/restart", json={"confirm": "polytape"})
        assert r.status_code == 200 and r.json()["action"] == "restart"
        assert broker.calls == [("restart", None)]


def test_endpoint_stop_is_unknown_action(tmp_path):
    app, broker, _ = _app(tmp_path)
    with TestClient(app) as c:
        c.post("/api/login", json={"token": "s3cr3t"})
        assert c.post("/api/control/stop", json={"confirm": "polytape"}).status_code == 404
        assert broker.calls == []  # operator-stop is not reachable via the API


def test_endpoint_arm_heartbeat_url_validation(tmp_path):
    app, broker, _ = _app(tmp_path)
    good = "https://hc-ping.com/abc-123"
    with TestClient(app) as c:
        c.post("/api/login", json={"token": "s3cr3t"})
        assert (
            c.post(
                "/api/control/arm-heartbeat", json={"confirm": "arm", "url": "http://x/y"}
            ).status_code
            == 400
        )
        inj = "https://hc-ping.com/x\nPOLYTAPE_SALT=evil"
        assert (
            c.post("/api/control/arm-heartbeat", json={"confirm": "arm", "url": inj}).status_code
            == 400
        )
        r = c.post("/api/control/arm-heartbeat", json={"confirm": "arm", "url": good})
        assert r.status_code == 200 and broker.calls == [("arm-heartbeat", good)]
    text = (tmp_path / "audit.jsonl").read_text(encoding="utf-8")
    assert good not in text and "url_fp" in text  # audit carries a fingerprint, not the URL


def test_endpoint_rate_limited(tmp_path):
    app, broker, _ = _app(tmp_path)
    with TestClient(app) as c:
        c.post("/api/login", json={"token": "s3cr3t"})
        assert c.post("/api/control/refresh", json={"confirm": "refresh"}).status_code == 200
        assert (
            c.post("/api/control/refresh", json={"confirm": "refresh"}).status_code == 429
        )  # too soon
        assert broker.calls == [("refresh", None)]  # only the first dispatched
