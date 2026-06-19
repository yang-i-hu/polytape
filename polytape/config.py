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
    """Validated configuration for one capture run (one *or more* events).

    A ``Config`` is valid by construction: ``__post_init__`` raises
    :class:`ValueError` for any invalid combination.

    ``event_ids`` is the canonical set of events to record. ``event_id`` is a
    single-event convenience: if given, it is folded into ``event_ids``; in all
    cases ``event_id`` is back-filled to the primary (first) id for logging and
    the ``meta.json`` snapshot.

    Attributes:
        event_id: A single Polymarket Event ID (convenience; folded into
            ``event_ids`` and then set to the primary id).
        event_ids: The events to record. Each must be numeric for a live capture;
            any non-empty string is allowed under ``dry_run``.
        run_name: Label for a multi-event run; output goes to ``out_dir/run-<name>``.
        out_dir: Output root. Single event -> ``out_dir/event-<id>``; multiple
            events (or an explicit ``run_name``) -> ``out_dir/run-<name>``.
        comments: Whether to record the RTDS comment stream.
        book: Whether to record the CLOB order-book stream.
        hash_usernames: Whether to salt-and-hash identifier fields (privacy default).
        market_ids: Optional explicit market id(s) to record instead of every
            market in each event. Empty means "auto-resolve".
        dry_run: Feed synthetic messages through the pipeline with no network.
        log_level: Python logging level name (upper-case).
    """

    event_id: str | None = None
    event_ids: tuple[str, ...] = ()
    run_name: str | None = None
    out_dir: Path = Path("./data")
    comments: bool = True
    book: bool = True
    hash_usernames: bool = True
    market_ids: tuple[str, ...] = ()
    dry_run: bool = False
    log_level: str = "INFO"

    def __post_init__(self) -> None:
        ids = tuple(str(e).strip() for e in self.event_ids) if self.event_ids else ()
        if not ids and self.event_id is not None:
            ids = (str(self.event_id).strip(),)
        ids = tuple(i for i in ids if i)
        if not ids:
            raise ValueError("at least one event id is required (event_id or event_ids)")
        # Frozen dataclass: settle the canonical fields via object.__setattr__.
        object.__setattr__(self, "event_ids", ids)
        object.__setattr__(self, "event_id", ids[0])
        if not self.comments and not self.book:
            raise ValueError(
                "at least one stream must be enabled "
                "(do not combine --no-comments with --no-book)"
            )
        if not self.dry_run:
            non_numeric = [i for i in ids if not i.isdigit()]
            if non_numeric:
                raise ValueError(
                    f"event ids must be numeric for a live capture, got {non_numeric!r} "
                    "(use --dry-run for offline testing with synthetic ids)"
                )
        if self.log_level.upper() not in _VALID_LOG_LEVELS:
            raise ValueError(f"invalid log level: {self.log_level!r}")

    @property
    def is_multi(self) -> bool:
        """Whether this run records several events (or an explicit ``run_name``)."""
        return len(self.event_ids) > 1 or self.run_name is not None

    @property
    def event_dir(self) -> Path:
        """Directory holding this run's output files.

        Single event -> ``out_dir/event-<id>`` (the original layout); multiple
        events (or an explicit ``run_name``) -> ``out_dir/run-<name>``.
        """
        if self.is_multi:
            return self.out_dir / f"run-{self.run_name or 'multi'}"
        return self.out_dir / f"event-{self.event_ids[0]}"

    @property
    def enabled_streams(self) -> tuple[str, ...]:
        """Names of the streams enabled for this run, in a stable order."""
        streams: list[str] = []
        if self.comments:
            streams.append(STREAM_COMMENTS)
        if self.book:
            streams.append(STREAM_BOOK)
        return tuple(streams)
