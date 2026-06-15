"""Capture envelope: wrap a raw feed message with dual timestamps and an id.

Every recorded message is wrapped as::

    {"stream": ..., "id": ..., "ts_recv": ..., "ts_server": ..., "raw": ...}

This module also owns identifier hashing (privacy default) and the per-stream
logic for deriving the dedup ``id`` and the server timestamp. Everything here is
pure (no I/O), so it is fully unit-testable offline.

See ``README.md`` ("Record envelope") and ``PROTOCOL.md`` for field provenance.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import datetime, timezone
from typing import Any

from polytape.config import STREAM_BOOK, STREAM_COMMENTS

# Identifier keys hashed by default, by stream. Comments carry personal data
# (wallet addresses, display handles); book messages do not. Keys are matched
# anywhere they appear in the (possibly nested) payload.
DEFAULT_HASH_FIELDS: dict[str, frozenset[str]] = {
    STREAM_COMMENTS: frozenset(
        {"userAddress", "replyAddress", "proxyWallet", "baseAddress", "name", "pseudonym"}
    ),
    STREAM_BOOK: frozenset(),
}

_FRACTION_RE = re.compile(r"\.(\d+)")


# --------------------------------------------------------------------------- #
# Timestamps
# --------------------------------------------------------------------------- #


def utc_now_iso() -> str:
    """Current UTC time as ISO-8601 with microseconds and a ``Z`` suffix."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


def _fmt(dt: datetime) -> str:
    """Format a datetime as canonical UTC ISO-8601 with a ``Z`` suffix."""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


def _normalize_fractional(text: str) -> str:
    """Pad/truncate fractional seconds to exactly 6 digits (3.10 ``fromisoformat``)."""
    m = _FRACTION_RE.search(text)
    if not m:
        return text
    frac = (m.group(1) + "000000")[:6]
    return text[: m.start()] + "." + frac + text[m.end() :]


def iso_to_datetime(value: str) -> datetime | None:
    """Parse an ISO-8601 string (with ``Z`` or offset) to an aware UTC datetime."""
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    if text[-1] in ("Z", "z"):
        text = text[:-1] + "+00:00"
    text = _normalize_fractional(text)
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _iso_from_string(value: str) -> str | None:
    dt = iso_to_datetime(value)
    return _fmt(dt) if dt is not None else None


def _iso_from_epoch(value: Any) -> str | None:
    """Convert an epoch number (seconds or milliseconds) to canonical UTC ISO."""
    try:
        n = float(value)
    except (TypeError, ValueError):
        return None
    seconds = n / 1000 if n > 1e11 else n  # >1e11 => milliseconds
    try:
        dt = datetime.fromtimestamp(seconds, tz=timezone.utc)
    except (OverflowError, OSError, ValueError):
        return None
    return _fmt(dt)


def _comment_core(raw: dict[str, Any]) -> dict[str, Any]:
    """The comment object, whether it's a live RTDS frame or a flat backfill comment."""
    payload = raw.get("payload")
    return payload if isinstance(payload, dict) else raw


def parse_server_ts(stream: str, raw: dict[str, Any]) -> str | None:
    """Extract the server-side timestamp for a message, normalized to UTC ISO.

    Comments prefer the content time ``createdAt`` (ISO), falling back to the
    RTDS envelope ``timestamp`` (epoch). Book messages use the top-level
    ``timestamp`` (epoch milliseconds). Returns ``None`` if none is usable.
    """
    if not isinstance(raw, dict):
        return None
    if stream == STREAM_COMMENTS:
        created = _comment_core(raw).get("createdAt")
        if isinstance(created, str) and created:
            iso = _iso_from_string(created)
            if iso is not None:
                return iso
        if raw.get("timestamp") is not None:
            return _iso_from_epoch(raw["timestamp"])
        return None
    if stream == STREAM_BOOK:
        if raw.get("timestamp") is not None:
            return _iso_from_epoch(raw["timestamp"])
        return None
    return None


# --------------------------------------------------------------------------- #
# Dedup id
# --------------------------------------------------------------------------- #


def content_id(raw: Any) -> str:
    """Deterministic content hash, used when a message has no native id."""
    blob = json.dumps(raw, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)
    return "sha256:" + hashlib.sha256(blob.encode("utf-8")).hexdigest()


def extract_id(stream: str, raw: dict[str, Any]) -> str:
    """Derive a stable, per-stream dedup id for a message.

    Comments use the comment/reaction ``id`` (same value live or from backfill).
    Book ``book`` messages use ``hash``; ``last_trade_price`` uses
    ``transaction_hash``; everything else falls back to a content hash.
    """
    if not isinstance(raw, dict):
        return content_id(raw)
    if stream == STREAM_COMMENTS:
        cid = _comment_core(raw).get("id")
        return str(cid) if cid is not None else content_id(raw)
    if stream == STREAM_BOOK:
        event_type = raw.get("event_type")
        if event_type == "book" and raw.get("hash"):
            return str(raw["hash"])
        if event_type == "last_trade_price" and raw.get("transaction_hash"):
            return str(raw["transaction_hash"])
        return content_id(raw)
    return content_id(raw)


# --------------------------------------------------------------------------- #
# Hashing / redaction
# --------------------------------------------------------------------------- #


class Hasher:
    """Salted SHA-256 hasher for identifier fields.

    The salt is taken from ``POLYTAPE_SALT`` if set (stable across runs),
    otherwise a random 16-byte salt is generated. The salt is never exposed; a
    short :attr:`fingerprint` lets two captures be compared for a shared salt.
    """

    __slots__ = ("_salt", "fingerprint")

    def __init__(self, salt: bytes | str | None = None) -> None:
        if salt is None:
            env = os.environ.get("POLYTAPE_SALT")
            salt = env.encode("utf-8") if env else os.urandom(16)
        elif isinstance(salt, str):
            salt = salt.encode("utf-8")
        self._salt: bytes = salt
        self.fingerprint: str = hashlib.sha256(salt).hexdigest()[:8]

    def hash_value(self, value: Any) -> str:
        """Return the salted SHA-256 hex digest of ``value``."""
        return hashlib.sha256(self._salt + b"\x1f" + str(value).encode("utf-8")).hexdigest()


def redact(obj: Any, fields: frozenset[str], hasher: Hasher) -> Any:
    """Recursively replace string values of ``fields`` keys with their hash.

    Mutates ``obj`` in place (it is the freshly-parsed message) and returns it.
    """
    if isinstance(obj, dict):
        for key, value in obj.items():
            if key in fields and isinstance(value, str) and value:
                obj[key] = hasher.hash_value(value)
            else:
                redact(value, fields, hasher)
    elif isinstance(obj, list):
        for item in obj:
            redact(item, fields, hasher)
    return obj


# --------------------------------------------------------------------------- #
# Envelope
# --------------------------------------------------------------------------- #


def build_envelope(
    stream: str,
    raw: dict[str, Any],
    *,
    hasher: Hasher | None = None,
    hash_fields: frozenset[str] | None = None,
    ts_recv: str | None = None,
) -> dict[str, Any]:
    """Wrap a raw message in the capture envelope.

    When ``hasher`` is provided, identifier fields in ``raw`` are redacted in
    place *before* the id/timestamp are read (neither of which is a hashed
    field). ``hash_fields`` overrides the per-stream defaults.
    """
    if hasher is not None:
        fields = (
            hash_fields if hash_fields is not None else DEFAULT_HASH_FIELDS.get(stream, frozenset())
        )
        if fields:
            redact(raw, fields, hasher)
    return {
        "stream": stream,
        "id": extract_id(stream, raw),
        "ts_recv": ts_recv if ts_recv is not None else utc_now_iso(),
        "ts_server": parse_server_ts(stream, raw),
        "raw": raw,
    }
