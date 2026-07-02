"""Capture envelope: wrap a raw feed message with dual timestamps and an id.

Every recorded message is wrapped as::

    {"stream": ..., "id": ..., "ts_recv": ..., "ts_server": ..., "raw": ...}

This module owns the per-stream logic for deriving the dedup ``id`` and the
server timestamp. Everything here is pure (no I/O), so it is fully
unit-testable offline.

See ``README.md`` ("Record envelope") and ``PROTOCOL.md`` for field provenance.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from typing import Any

from polytape.config import STREAM_BOOK

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


def parse_server_ts(stream: str, raw: dict[str, Any]) -> str | None:
    """Extract the server-side timestamp for a message, normalized to UTC ISO.

    Book messages use the top-level ``timestamp`` (epoch milliseconds).
    Returns ``None`` if none is usable.
    """
    if not isinstance(raw, dict):
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

    Book ``book`` messages use ``hash``; ``last_trade_price`` uses
    ``transaction_hash``; everything else falls back to a content hash.
    """
    if not isinstance(raw, dict):
        return content_id(raw)
    if stream == STREAM_BOOK:
        event_type = raw.get("event_type")
        if event_type == "book" and raw.get("hash"):
            return str(raw["hash"])
        if event_type == "last_trade_price" and raw.get("transaction_hash"):
            return str(raw["transaction_hash"])
        return content_id(raw)
    return content_id(raw)


# --------------------------------------------------------------------------- #
# Envelope
# --------------------------------------------------------------------------- #


def build_envelope(
    stream: str,
    raw: dict[str, Any],
    *,
    ts_recv: str | None = None,
) -> dict[str, Any]:
    """Wrap a raw message in the capture envelope."""
    return {
        "stream": stream,
        "id": extract_id(stream, raw),
        "ts_recv": ts_recv if ts_recv is not None else utc_now_iso(),
        "ts_server": parse_server_ts(stream, raw),
        "raw": raw,
    }
