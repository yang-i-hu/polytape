"""polytape — record Polymarket's public real-time comment and order-book feeds.

A passive, read-only recorder. It never authenticates, never trades, and talks
only to public Polymarket endpoints. See ``README.md`` for the user-facing
interface and ``PROTOCOL.md`` for the verified wire formats.
"""

from __future__ import annotations

# Keep in sync with [project].version in pyproject.toml.
__version__ = "0.1.0"

__all__ = ["__version__"]
