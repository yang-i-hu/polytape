"""Thin file I/O for a capture directory (the only module that touches disk).

:class:`CaptureReader` reads ``meta.json`` (reloading when it changes on disk) and
incrementally tails ``book.jsonl`` by byte offset, yielding only complete,
newline-terminated JSON lines (a trailing partial line is left for next time).
All interpretation of the envelopes is delegated to
:mod:`polytape.viewer.reconstruct`; nothing here knows feed semantics.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger("polytape.viewer.reader")


class CaptureReader:
    """Reads one ``event-<id>`` capture directory."""

    def __init__(self, event_dir: Path) -> None:
        self.event_dir = Path(event_dir)
        self.book_path = self.event_dir / "book.jsonl"
        self.meta_path = self.event_dir / "meta.json"
        self._meta: dict[str, Any] | None = None
        self._meta_sig: tuple[float, int] | None = None

    # -- meta --------------------------------------------------------------- #

    def read_meta(self) -> dict[str, Any] | None:
        """Return parsed ``meta.json``, reloading only when its mtime/size change.

        ``meta.json`` is written atomically by the recorder (temp + replace), so a
        read never sees a half-written file. Returns the last good value (or
        ``None``) if the file is missing or transiently unparseable.
        """
        try:
            st = self.meta_path.stat()
        except OSError:
            return self._meta
        sig = (st.st_mtime, st.st_size)
        if self._meta is not None and sig == self._meta_sig:
            return self._meta
        try:
            meta = json.loads(self.meta_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return self._meta
        if isinstance(meta, dict):
            self._meta = meta
            self._meta_sig = sig
        return self._meta

    # -- book tail ---------------------------------------------------------- #

    def tail(self, since_offset: int) -> tuple[list[dict[str, Any]], int, bool]:
        """Read newly-appended complete lines from ``book.jsonl``.

        Returns ``(envelopes, new_offset, reset)``. ``reset`` is ``True`` when the
        file shrank/rotated (a new capture run), in which case reading restarted
        from offset 0 and the caller should discard prior state. Only bytes up to
        the last newline are consumed; a trailing partial line is re-read next call.
        """
        try:
            size = self.book_path.stat().st_size
        except OSError:
            return [], since_offset, False

        reset = False
        if size < since_offset:
            since_offset = 0
            reset = True
        if size == since_offset:
            return [], since_offset, reset

        try:
            with open(self.book_path, "rb") as handle:
                handle.seek(since_offset)
                data = handle.read()
        except OSError:
            return [], since_offset, reset

        last_newline = data.rfind(b"\n")
        if last_newline == -1:
            return [], since_offset, reset  # only a partial line so far
        consumed = data[: last_newline + 1]
        new_offset = since_offset + len(consumed)

        envelopes: list[dict[str, Any]] = []
        for line in consumed.split(b"\n"):
            if not line.strip():
                continue
            try:
                env = json.loads(line.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                logger.debug("skipping malformed book line at ~%d", new_offset)
                continue
            if isinstance(env, dict):
                envelopes.append(env)
        return envelopes, new_offset, reset

    # -- discovery ---------------------------------------------------------- #

    @staticmethod
    def list_events(data_root: Path) -> list[dict[str, Any]]:
        """List ``event-<id>`` capture dirs under ``data_root`` (newest first)."""
        root = Path(data_root)
        events: list[dict[str, Any]] = []
        if not root.is_dir():
            return events
        for child in sorted(root.glob("event-*")):
            if not child.is_dir():
                continue
            if not (child / "book.jsonl").exists() and not (child / "meta.json").exists():
                continue
            events.append({"event_id": child.name[len("event-") :], "dir": child.name})
        return events
