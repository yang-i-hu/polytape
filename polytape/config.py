"""Runtime configuration for a polytape capture session."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

# Canonical stream names — used for output file names, the envelope ``stream``
# field, and ``meta.json``.
STREAM_COMMENTS = "comments"
STREAM_BOOK = "book"

_VALID_LOG_LEVELS = frozenset({"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"})


@dataclass(frozen=True, slots=True)
class Config:
    """Validated configuration for one capture run.

    A ``Config`` is valid by construction: ``__post_init__`` raises
    :class:`ValueError` for any invalid combination, so the rest of the
    application can assume the values are sound.

    Attributes:
        event_id: Polymarket Event ID to record. Must be numeric for a live
            capture; may be any non-empty string under ``dry_run``.
        out_dir: Output root directory. Data is written to
            ``out_dir / 'event-<event_id>'``.
        comments: Whether to record the RTDS comment stream.
        book: Whether to record the CLOB order-book stream.
        hash_usernames: Whether to salt-and-hash identifier fields in comment
            payloads before writing (privacy default). ``--no-hash`` clears it.
        market_ids: Optional explicit market id(s) to record instead of every
            market in the event. Empty means "auto-resolve from the event".
        dry_run: Feed synthetic messages through the pipeline with no network.
        log_level: Python logging level name (upper-case).
    """

    event_id: str
    out_dir: Path = Path("./data")
    comments: bool = True
    book: bool = True
    hash_usernames: bool = True
    market_ids: tuple[str, ...] = ()
    dry_run: bool = False
    log_level: str = "INFO"

    def __post_init__(self) -> None:
        if not str(self.event_id).strip():
            raise ValueError("event_id must be a non-empty string")
        if not self.comments and not self.book:
            raise ValueError(
                "at least one stream must be enabled "
                "(do not combine --no-comments with --no-book)"
            )
        if not self.dry_run and not str(self.event_id).isdigit():
            raise ValueError(
                f"event_id must be numeric for a live capture, got {self.event_id!r} "
                "(use --dry-run for offline testing with a synthetic id)"
            )
        if self.log_level.upper() not in _VALID_LOG_LEVELS:
            raise ValueError(f"invalid log level: {self.log_level!r}")

    @property
    def event_dir(self) -> Path:
        """Directory holding this event's output files (``out_dir/event-<id>``)."""
        return self.out_dir / f"event-{self.event_id}"

    @property
    def enabled_streams(self) -> tuple[str, ...]:
        """Names of the streams enabled for this run, in a stable order."""
        streams: list[str] = []
        if self.comments:
            streams.append(STREAM_COMMENTS)
        if self.book:
            streams.append(STREAM_BOOK)
        return tuple(streams)
