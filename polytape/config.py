"""Runtime configuration for a polytape capture session."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType

# Canonical stream name — used for output file names, the envelope ``stream``
# field, and ``meta.json``.
STREAM_BOOK = "book"

_VALID_LOG_LEVELS = frozenset({"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"})


def _normalize_event_markets(raw: Mapping[str, object]) -> Mapping[str, tuple[str, ...]]:
    """Validate + freeze an ``event_id -> market ids`` allow-list mapping.

    Keys and every listed id must be strings (a bare int would silently match
    nothing at resolve time, so it is rejected here instead). Ids are stripped and
    de-duplicated order-preservingly; an event whose list is empty is dropped from
    the mapping, since "no allow-list" means "record every market".
    """
    if not isinstance(raw, Mapping):
        raise ValueError(f"event_markets must be a mapping, got {type(raw).__name__}")
    out: dict[str, tuple[str, ...]] = {}
    for key, ids in raw.items():
        if not isinstance(key, str) or not key.strip():
            raise ValueError(f"event_markets keys must be non-empty event id strings, got {key!r}")
        if isinstance(ids, str) or not hasattr(ids, "__iter__"):
            raise ValueError(
                f"event_markets[{key!r}] must be a list/tuple of market ids, got {ids!r}"
            )
        cleaned: dict[str, None] = {}
        for mid in ids:
            if not isinstance(mid, str):
                raise ValueError(f"event_markets[{key!r}]: market ids must be strings, got {mid!r}")
            if mid.strip():
                cleaned[mid.strip()] = None
        if cleaned:
            out[key.strip()] = tuple(cleaned)
    return MappingProxyType(out)


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
        market_ids: Optional explicit market id(s) to record instead of every
            market in each event (the global ``--market-id`` override). Empty
            means "auto-resolve". Applies to *every* event and composes with
            ``event_markets`` as an intersection.
        event_markets: Optional per-event allow-list: ``event_id -> market ids``
            (Gamma market ids and/or ``conditionId``s), read from a matches file's
            ``record_markets`` entries. An event absent from the mapping records
            every market it has (the pre-campaign behaviour). Frozen and validated:
            every id must be a string; empty lists are dropped.
        per_match: Also write each event-tagged record to a per-match file under
            ``event_dir/matches/event-<id>/<stream>.jsonl`` (the PRIMARY, ready-to-use
            per-match output), in addition to the monolithic ``<stream>.jsonl`` (kept
            as the complete append-only backup). Lets a finished match be consumed /
            offloaded without scanning the whole run. On by default.
        dry_run: Feed synthetic messages through the pipeline with no network.
        log_level: Python logging level name (upper-case).
    """

    event_id: str | None = None
    event_ids: tuple[str, ...] = ()
    run_name: str | None = None
    out_dir: Path = Path("./data")
    market_ids: tuple[str, ...] = ()
    event_markets: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    per_match: bool = True
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
        object.__setattr__(self, "event_markets", _normalize_event_markets(self.event_markets))
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
        return (STREAM_BOOK,)
