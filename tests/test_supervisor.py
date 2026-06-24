"""Tests for the reconnect supervisor and comment backfill callback."""

from __future__ import annotations

import asyncio
import json

import pytest

from polytape.supervisor import StreamSupervisor, make_comment_backfill
from polytape.writer import CaptureWriter, FatalRecorderError


class _Recorder:
    """Minimal stream stub: counts run_once calls, runs on_connect, stops after N."""

    stream = "comments"

    def __init__(self, *, stop_after, raises=False):
        self.event_id = "20200"
        self.event_ids = {"20200"}
        self.series_ids = set()
        self.last_comment_id = None
        self.calls = 0
        self.sup: StreamSupervisor | None = None
        self._stop_after = stop_after
        self._raises = raises

    def last_comment_id_for(self, event_id):
        return self.last_comment_id

    async def run_once(self, *, on_connect=None):
        self.calls += 1
        if on_connect is not None:
            await on_connect()
        if self.calls >= self._stop_after:
            self.sup.stop()
        if self._raises:
            raise RuntimeError("boom")

    def backfill_targets(self):
        return [("Event", self.event_id)]

    def cursor_for(self, parent_id):
        return self.last_comment_id


def test_backoff_curve():
    s = StreamSupervisor.__new__(StreamSupervisor)
    s._base_delay, s._max_delay, s._jitter = 1.0, 10.0, 0.0
    assert [s._backoff(a) for a in range(5)] == [1.0, 2.0, 4.0, 8.0, 10.0]


async def test_sleep_or_stop_returns_early_on_stop():
    s = _Recorder(stop_after=1)
    sup = StreamSupervisor(s, writer=None)
    sup.stop()
    await asyncio.wait_for(sup._sleep_or_stop(100.0), timeout=1.0)  # returns immediately


async def test_reconnect_records_gaps_and_backfills(make_config):
    cfg = make_config(book=False)

    class _Gamma:
        def __init__(self):
            self.n = 0

        async def backfill_since(self, parent_id, last, *, parent_entity_type="Event"):
            self.n += 1
            return [{"id": f"bf{self.n}-{k}"} for k in (1, 2)]

    with CaptureWriter(cfg) as w:
        stream = _Recorder(stop_after=3)
        gamma = _Gamma()
        sup = StreamSupervisor(
            stream,
            writer=w,
            backfill=make_comment_backfill(stream, gamma, w),
            base_delay=0.001,
            max_delay=0.002,
            reset_after=0.0,
            jitter=0.0,
        )
        stream.sup = sup
        await asyncio.wait_for(sup.run(), timeout=5.0)
        assert stream.calls == 3
        assert gamma.n == 2  # first connect has no gap; 2 reconnects backfill
        assert w.counts["comments"] == 4
    meta = json.loads((cfg.event_dir / "meta.json").read_text(encoding="utf-8"))
    assert len(meta["gaps"]) == 2
    assert all(g["backfilled"] == 2 and g["note"] == "reconnect" for g in meta["gaps"])


async def test_supervisor_retries_through_errors_then_stops(make_config):
    cfg = make_config(book=False)
    with CaptureWriter(cfg) as w:
        stream = _Recorder(stop_after=3, raises=True)
        sup = StreamSupervisor(
            stream, writer=w, base_delay=0.001, max_delay=0.002, reset_after=99, jitter=0.0
        )
        stream.sup = sup
        await asyncio.wait_for(sup.run(), timeout=5.0)
        assert stream.calls == 3  # kept retrying through RuntimeError, then stopped


async def test_comment_backfill_dedups_overlap(make_config):
    cfg = make_config(book=False)
    with CaptureWriter(cfg) as w:
        w.write("comments", {"payload": {"id": "live1"}})  # already recorded live

        class _Stream:
            stream = "comments"
            event_ids = {"20200"}
            series_ids = set()

            def last_comment_id_for(self, event_id):
                return "live1"

        class _Gamma:
            async def backfill_since(self, parent_id, last, *, parent_entity_type="Event"):
                return [{"id": "live1"}, {"id": "bf1"}, {"id": "bf2"}]

        backfill = make_comment_backfill(_Stream(), _Gamma(), w)
        assert await backfill() == 2  # live1 overlaps -> not re-counted
        assert w.counts["comments"] == 3


async def test_comment_backfill_pages_series_with_series_type(make_config):
    # A series-parented comment feed (e.g. the World Cup) must be paged with
    # parent_entity_type="Series"; the per-Event query returns nothing.
    cfg = make_config(book=False)
    with CaptureWriter(cfg) as w:

        class _Stream:
            stream = "comments"
            event_ids = {"351771"}
            series_ids = {"11433"}

            def last_comment_id_for(self, parent_id):
                return None

        seen: list[tuple[str, str]] = []

        class _Gamma:
            async def backfill_since(self, parent_id, last, *, parent_entity_type="Event"):
                seen.append((str(parent_id), parent_entity_type))
                # Only the Series feed carries comments for this product.
                if parent_entity_type == "Series":
                    return [{"id": "c1", "parentEntityID": 11433}]
                return []

        backfill = make_comment_backfill(_Stream(), _Gamma(), w)
        assert await backfill() == 1
        assert ("351771", "Event") in seen and ("11433", "Series") in seen
        assert w.counts["comments"] == 1


class _FatalStream:
    """A stream stub whose session raises a fatal (unrecoverable) error."""

    stream = "comments"

    def __init__(self):
        self.calls = 0

    async def run_once(self, *, on_connect=None):
        self.calls += 1
        raise FatalRecorderError("disk full")


async def test_supervisor_reraises_fatal_without_looping(make_config):
    cfg = make_config(book=False)
    with CaptureWriter(cfg) as w:
        stream = _FatalStream()
        sup = StreamSupervisor(stream, writer=w, base_delay=0.001, max_delay=0.002, jitter=0.0)
        with pytest.raises(FatalRecorderError):
            await asyncio.wait_for(sup.run(), timeout=2.0)
        assert stream.calls == 1  # fatal stops immediately; no reconnect loop
