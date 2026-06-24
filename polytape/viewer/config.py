"""Validated configuration for a ``polytape-view`` session (mirrors polytape/config.py)."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

_VALID_LOG_LEVELS = frozenset({"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"})


@dataclass(frozen=True, slots=True)
class ViewerConfig:
    """Configuration for the viewer server, valid by construction.

    Exactly one of three launch modes is expressed:

    * single capture by explicit dir — ``event_dir_override`` set;
    * single capture by ``data_root`` + ``event_id`` (mirrors the recorder's flags);
    * multi-capture picker — ``event_id`` is ``None`` and ``data_root`` is scanned.
    """

    data_root: Path
    event_id: str | None = None
    event_dir_override: Path | None = None
    host: str = "127.0.0.1"
    port: int = 8770
    poll_interval: float = 0.25
    keyframe_every: int = 250
    open_browser: bool = True
    log_level: str = "INFO"

    def __post_init__(self) -> None:
        if not str(self.data_root).strip():
            raise ValueError("data_root must be a non-empty path")
        if not 1 <= self.port <= 65535:
            raise ValueError(f"port must be in 1..65535, got {self.port}")
        if self.poll_interval <= 0:
            raise ValueError("poll_interval must be > 0")
        if self.keyframe_every < 1:
            raise ValueError("keyframe_every must be >= 1")
        if self.log_level.upper() not in _VALID_LOG_LEVELS:
            raise ValueError(f"invalid log level: {self.log_level!r}")

    @property
    def event_dir(self) -> Path | None:
        """The single capture directory, or ``None`` in multi-capture mode."""
        if self.event_dir_override is not None:
            return self.event_dir_override
        if self.event_id is not None:
            return self.data_root / f"event-{self.event_id}"
        return None

    @property
    def single_event(self) -> bool:
        return self.event_dir is not None
