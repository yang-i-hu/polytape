"""Tests for the reconnect supervisor."""

from __future__ import annotations

import asyncio
import json

import pytest

from polytape.supervisor import StreamSupervisor
from polytape.writer import CaptureWriter, FatalRecorderError


class _Recorder:
    """Minimal stream stub: counts run_once calls, runs on_connect, stops after N."""

    stream = "book"

    def __init__(self, *, stop_after, raises=False):
        self.calls = 0
        self.sup: StreamSupervisor | None = None
        self._stop_after = stop_after
        self._raises = raises

    async def run_once(self, *, on_connect=None):
        self.calls += 1
        if on_connect is not None:
            await on_connect()
        if self.calls >= self._stop_after:
            self.sup.stop()
        if self._raises:
            raise RuntimeError("boom")


def test_backoff_curve():
    s = StreamSupervisor.__new__(StreamSupervisor)
    s._base_delay, s._max_delay, s._jitter = 1.0, 10.0, 0.0
    assert [s._backoff(a) for a in range(5)] == [1.0, 2.0, 4.0, 8.0, 10.0]


async def test_sleep_or_stop_returns_early_on_stop():
    s = _Recorder(stop_after=1)
    sup = StreamSupervisor(s, writer=None)
    sup.stop()
    await asyncio.wait_for(sup._sleep_or_stop(100.0), timeout=1.0)  # returns immediately


async def test_reconnect_records_gaps(make_config):
    cfg = make_config()
    with CaptureWriter(cfg) as w:
        stream = _Recorder(stop_after=3)
        sup = StreamSupervisor(
            stream,
            writer=w,
            base_delay=0.001,
            max_delay=0.002,
            reset_after=0.0,
            jitter=0.0,
        )
        stream.sup = sup
        await asyncio.wait_for(sup.run(), timeout=5.0)
        assert stream.calls == 3
    meta = json.loads((cfg.event_dir / "meta.json").read_text(encoding="utf-8"))
    assert len(meta["gaps"]) == 2  # first connect has no gap; 2 reconnects record one
    assert all(g["note"] == "reconnect" for g in meta["gaps"])


async def test_supervisor_retries_through_errors_then_stops(make_config):
    cfg = make_config()
    with CaptureWriter(cfg) as w:
        stream = _Recorder(stop_after=3, raises=True)
        sup = StreamSupervisor(
            stream, writer=w, base_delay=0.001, max_delay=0.002, reset_after=99, jitter=0.0
        )
        stream.sup = sup
        await asyncio.wait_for(sup.run(), timeout=5.0)
        assert stream.calls == 3  # kept retrying through RuntimeError, then stopped


class _FatalStream:
    """A stream stub whose session raises a fatal (unrecoverable) error."""

    stream = "book"

    def __init__(self):
        self.calls = 0

    async def run_once(self, *, on_connect=None):
        self.calls += 1
        raise FatalRecorderError("disk full")


async def test_supervisor_reraises_fatal_without_looping(make_config):
    cfg = make_config()
    with CaptureWriter(cfg) as w:
        stream = _FatalStream()
        sup = StreamSupervisor(stream, writer=w, base_delay=0.001, max_delay=0.002, jitter=0.0)
        with pytest.raises(FatalRecorderError):
            await asyncio.wait_for(sup.run(), timeout=2.0)
        assert stream.calls == 1  # fatal stops immediately; no reconnect loop
