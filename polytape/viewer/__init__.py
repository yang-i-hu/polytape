"""polytape viewer — a local web UI to replay and live-follow a recorded book.

The viewer READS a recorder capture directory (``book.jsonl`` + ``meta.json``);
it never connects to Polymarket. Order-book reconstruction is server-side and
canonical (see :mod:`polytape.viewer.reconstruct`); the browser renders only
already-reconstructed state delivered over a small JSON + SSE API.
"""

from __future__ import annotations

from polytape.viewer.book import OrderBook
from polytape.viewer.reconstruct import Reconstructor, normalize_book_event

API_VERSION = "v1"

__all__ = ["API_VERSION", "OrderBook", "Reconstructor", "normalize_book_event"]
