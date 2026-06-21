"""Security primitives + privilege broker for the admin control plane (Capability 2).

The admin sidecar runs UNPRIVILEGED and must never invoke ``systemctl`` itself. A
guarded ``POST /api/control/{action}`` validates the request with the helpers here,
then :class:`IntentBroker` drops an *intent file* into a RuntimeDirectory that a
separate root-owned systemd ``.path`` unit watches; the root helper
(``deploy/polytape-control.sh``) is the single privileged choke point. No ``sudo``,
no sandbox relaxation.

Design notes baked in from the adversarial security review:
- ``POLYTAPE_ADMIN_TOKEN`` is a MANDATORY shared secret (the SameSite cookie / in-page
  token alone does not defend the "tunnel left open / local process" threat). It is
  compared with :func:`hmac.compare_digest` and exchanged for a TTL session.
- ``stop`` is intentionally absent: ``systemctl stop`` is unrecoverable
  (``Restart=always`` does not undo it) and ``restart`` covers the legitimate need.
- The heartbeat URL is strictly allow-listed: it is later written into the recorder's
  systemd ``EnvironmentFile``, where a newline/quote/``$`` could inject a second key or
  clobber ``POLYTAPE_SALT``.
- The URL is staged in a fixed file, never passed on argv (avoids ``ps`` leakage), and
  the action is encoded only as an allow-listed filename (no command interpolation).

Every side effect (intent write, clock) is an injectable seam, so the module is
exercised offline with no real privilege, filesystem layout, or ``systemctl``.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import secrets
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from polytape.envelope import utc_now_iso

# Operator actions. 'stop' is deliberately ABSENT (unrecoverable; restart suffices).
# confirm = the exact phrase the operator must re-type, re-verified server-side, so a
# valid session alone cannot fire an action.
ACTIONS: dict[str, dict[str, Any]] = {
    "restart": {"needs_url": False, "min_interval_s": 30.0, "confirm": "polytape"},
    "refresh": {"needs_url": False, "min_interval_s": 30.0, "confirm": "refresh"},
    "arm-heartbeat": {"needs_url": True, "min_interval_s": 10.0, "confirm": "arm"},
}

# Strict heartbeat-URL allow-list. Conservative on purpose: host + optional port +
# simple path + simple query, https only. The explicit bad-char check below is
# belt-and-suspenders against EnvironmentFile injection.
_URL_RE = re.compile(
    r"^https://"
    r"[A-Za-z0-9.\-]{1,253}"  # host
    r"(:\d{1,5})?"  # optional port
    r"(/[A-Za-z0-9._~/\-]*)?"  # optional path
    r"(\?[A-Za-z0-9._~=&%\-]*)?$"  # optional simple query
)
_URL_FORBIDDEN = set("\r\n\x00\"'`$ \t\\")


def validate_heartbeat_url(url: Any) -> bool:
    """True iff ``url`` is a safe https heartbeat URL that cannot corrupt the env file."""
    if not isinstance(url, str) or not (8 < len(url) <= 300):
        return False
    if any(c in _URL_FORBIDDEN for c in url):
        return False
    if any(ord(c) < 0x20 or ord(c) == 0x7F for c in url):
        return False
    return bool(_URL_RE.fullmatch(url))  # fullmatch: independent of the ^...$ anchors


def token_ok(provided: Any, secret: str | None) -> bool:
    """Constant-time compare of a provided secret against the configured one."""
    if not secret or not isinstance(provided, str) or not provided:
        return False
    return hmac.compare_digest(provided, secret)


def fingerprint(value: str) -> str:
    """Short, non-reversible tag for audit logs (never log the raw secret/URL)."""
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:12]


class Sessions:
    """Opaque-id session store with a TTL. Issued only after a valid secret login."""

    def __init__(
        self, *, ttl_s: float = 1800.0, mono: Callable[[], float] = time.monotonic
    ) -> None:
        self._ttl = ttl_s
        self._mono = mono
        self._store: dict[str, float] = {}  # sid -> expiry (monotonic)

    def mint(self) -> str:
        sid = secrets.token_urlsafe(32)
        self._store[sid] = self._mono() + self._ttl
        return sid

    def valid(self, sid: str | None) -> bool:
        if not sid:
            return False
        exp = self._store.get(sid)
        if exp is None:
            return False
        if exp < self._mono():
            self._store.pop(sid, None)
            return False
        return True

    def drop(self, sid: str | None) -> None:
        if sid:
            self._store.pop(sid, None)


class RateLimiter:
    """Per-action minimum interval — anti-fat-finger (NOT a security boundary)."""

    def __init__(self, *, mono: Callable[[], float] = time.monotonic) -> None:
        self._mono = mono
        self._last: dict[str, float] = {}

    def allow(self, action: str, min_interval_s: float) -> bool:
        now = self._mono()
        last = self._last.get(action)
        if last is not None and (now - last) < min_interval_s:
            return False
        self._last[action] = now
        return True


class LoginThrottle:
    """Global failed-login throttle. Behind the SSH tunnel the source is always
    127.0.0.1 so per-IP keying is useless — key globally. After ``max_fails``
    failures within ``window_s`` the secret is locked out for ``window_s``; this
    blunts the online brute force the whole "the secret is the boundary" design
    leans on. The caller also imposes a fixed per-failure delay."""

    def __init__(
        self,
        *,
        max_fails: int = 5,
        window_s: float = 60.0,
        mono: Callable[[], float] = time.monotonic,
    ) -> None:
        self._max = max_fails
        self._window = window_s
        self._mono = mono
        self._fails: list[float] = []
        self._locked_until = 0.0

    def locked(self) -> bool:
        return self._mono() < self._locked_until

    def record_failure(self) -> None:
        now = self._mono()
        self._fails = [t for t in self._fails if now - t < self._window]
        self._fails.append(now)
        if len(self._fails) >= self._max:
            self._locked_until = now + self._window
            self._fails = []

    def record_success(self) -> None:
        self._fails = []
        self._locked_until = 0.0


class AuditLog:
    """Append-only JSONL audit, written OUTSIDE the recorder's run dir."""

    def __init__(self, path: str | Path, *, now: Callable[[], str] = utc_now_iso) -> None:
        self._path = Path(path)
        self._now = now

    def write(self, **fields: Any) -> None:
        rec = {"ts": self._now(), **fields}
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            with open(self._path, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(rec) + "\n")
        except OSError:
            pass  # an audit-write failure must never break the request path


class IntentBroker:
    """Drops an intent file the root ``.path`` unit acts on — the ONLY privileged seam.

    The action is the (allow-listed) file name; the heartbeat URL is staged in a fixed
    sibling file, never in the name or on argv. The root helper re-validates everything.
    """

    def __init__(self, intent_dir: str | Path, *, now: Callable[[], str] = utc_now_iso) -> None:
        self._dir = Path(intent_dir)
        self._now = now

    def dispatch(self, action: str, *, url: str | None = None) -> None:
        if action not in ACTIONS:  # defense in depth; callers already validate
            raise ValueError(f"unknown action: {action!r}")
        self._dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        if action == "arm-heartbeat":
            staging = self._dir.parent / "heartbeat.url"
            staging.write_text((url or "") + "\n", encoding="utf-8")
        # Stage the temp OUTSIDE the watched intent dir, so the root helper's
        # stray-file sweep can never delete a half-published intent mid-write.
        tmp = self._dir.parent / f".{action}.tmp"
        tmp.write_text(self._now() + "\n", encoding="utf-8")
        os.replace(tmp, self._dir / action)  # atomic publish
