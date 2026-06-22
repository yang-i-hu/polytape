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
import logging
import os
import re
import secrets
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from polytape.envelope import utc_now_iso

logger = logging.getLogger("polytape.admin.control")

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
    """Opaque-id session store with a TTL, optionally persisted across restarts.

    Stores only ``sha256(sid)`` — a *verifier*, never the replayable id — with a
    **wall-clock** expiry (a persisted *monotonic* deadline would be meaningless
    after a restart). With ``store_path`` set, the store is persisted atomically at
    mode 0600 so an admin restart no longer logs everyone out. The shared secret
    (:func:`token_ok`) remains the ONLY thing that can MINT a session; persistence
    merely caches sessions already issued to a secret holder.
    """

    def __init__(
        self,
        *,
        ttl_s: float = 1800.0,
        clock: Callable[[], float] = time.time,
        store_path: str | Path | None = None,
    ) -> None:
        self._ttl = ttl_s
        self._clock = clock
        self._path = Path(store_path) if store_path else None
        self._store: dict[str, float] = {}  # sha256(sid) -> wall-clock expiry
        self._load()

    @property
    def ttl(self) -> float:
        """Session lifetime in seconds — the login cookie's max-age should match this."""
        return self._ttl

    @staticmethod
    def _key(sid: str) -> str:
        return hashlib.sha256(sid.encode("utf-8")).hexdigest()

    def _prune(self, now: float) -> None:
        """Drop expired verifiers so the store can't grow without bound over a long run."""
        for k in [k for k, exp in self._store.items() if exp <= now]:
            del self._store[k]

    def _load(self) -> None:
        if self._path is None:
            return
        try:
            data = json.loads(self._path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, UnicodeDecodeError):
            return  # missing/garbage -> empty store (exactly today's behaviour)
        if not isinstance(data, dict):
            return
        now = self._clock()
        self._store = {
            k: float(v)
            for k, v in data.items()
            if isinstance(k, str) and isinstance(v, (int, float)) and float(v) > now
        }

    def _save(self) -> None:
        if self._path is None:
            return
        try:  # atomic + 0600; best-effort (a persist failure must never break a request)
            self._path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            # Per-pid temp name so a second writer can never O_TRUNC ours mid-write; the
            # os.replace then publishes atomically (a reader never sees a torn file). No
            # fsync: losing the cache on a hard crash just means re-login, and atomicity
            # (the real requirement) holds without it.
            tmp = self._path.with_name(f"{self._path.name}.{os.getpid()}.tmp")
            fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            try:
                os.write(fd, json.dumps(self._store).encode("utf-8"))
            finally:
                os.close(fd)
            os.replace(tmp, self._path)
        except OSError:
            # Degrade to in-memory only (a restart then logs everyone out) — but SAY so;
            # a silent failure would hide a broken persistence dir from the operator.
            logger.warning("session store persist failed (%s)", self._path, exc_info=True)

    def mint(self) -> str:
        sid = secrets.token_urlsafe(32)
        now = self._clock()
        self._prune(now)  # bound the store: evict expired verifiers before adding one
        self._store[self._key(sid)] = now + self._ttl
        self._save()
        return sid

    def valid(self, sid: str | None) -> bool:
        if not sid:
            return False
        key = self._key(sid)
        exp = self._store.get(key)
        if exp is None:
            return False
        if exp < self._clock():
            # Lazily drop from memory only — NO _save() on this hot read path; the entry
            # is re-pruned on the next mint() / restart anyway, so persisting here is pure
            # churn (and a blocking write) for no benefit.
            self._store.pop(key, None)
            return False
        return True

    def drop(self, sid: str | None) -> None:
        if sid and self._store.pop(self._key(sid), None) is not None:
            self._save()


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
