"""Read-only live monitor for a polytape capture.

A passive observer: it reads the exact artifacts a capture already writes — the
append-only ``*.jsonl`` stream files and the atomically-rewritten ``meta.json`` —
and serves a small localhost web dashboard so you can watch a recording happen.

It never imports the recorder's network or writer hot path and runs in its own
process, so it adds **zero** work to a live capture. See :mod:`polytape.monitor.reader`
for the tailing logic and :mod:`polytape.monitor.server` for the HTTP layer.

Run it with::

    python -m polytape.monitor --out ./data --open
"""

from __future__ import annotations

from polytape.monitor.reader import CaptureMonitor

__all__ = ["CaptureMonitor"]
