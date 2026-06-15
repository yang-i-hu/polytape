"""Async client for Polymarket's public Gamma REST API.

Two jobs (see ``PROTOCOL.md`` §3):

1. Resolve an Event ID to its markets and CLOB token ids (to seed the websocket
   subscriptions), via ``GET /events/{id}``.
2. Backfill comments missed during a disconnect, via paged ``GET /comments``.

All endpoints are public and unauthenticated. Network access is isolated in
:class:`GammaClient`; the payload-shaping logic lives in module-level pure
functions (``_parse_event`` etc.) so it can be unit-tested without a network.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, replace
from typing import Any

import httpx

from polytape import __version__

logger = logging.getLogger("polytape.gamma")

GAMMA_BASE_URL = "https://gamma-api.polymarket.com"
_USER_AGENT = f"polytape/{__version__} (read-only feed recorder)"


class GammaError(RuntimeError):
    """Raised when a Gamma response cannot be used (missing event, bad shape...)."""


@dataclass(frozen=True, slots=True)
class Market:
    """A single market within an event.

    Attributes:
        id: Numeric Gamma market id (e.g. ``"239826"``).
        condition_id: On-chain CTF condition id (``conditionId``, ``0x...``).
        token_ids: Parsed CLOB token ids, conventionally ``(yes, no)``.
    """

    id: str
    condition_id: str
    token_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class EventInfo:
    """Resolved event metadata used to drive the recorder.

    Attributes:
        event_id: The numeric event id (also the RTDS ``parentEntityID``).
        title: Event title, if present.
        slug: Event slug, if present.
        markets: The markets to record (after any ``--market-id`` filtering).
        raw: The raw event object, kept verbatim for the ``meta.json`` snapshot.
    """

    event_id: str
    title: str | None
    slug: str | None
    markets: tuple[Market, ...]
    raw: dict[str, Any]

    @property
    def gamma_market_ids(self) -> tuple[str, ...]:
        """Numeric Gamma market ids."""
        return tuple(m.id for m in self.markets if m.id)

    @property
    def condition_ids(self) -> tuple[str, ...]:
        """On-chain condition ids (used as ``market_ids`` in ``meta.json``)."""
        return tuple(m.condition_id for m in self.markets if m.condition_id)

    @property
    def clob_token_ids(self) -> tuple[str, ...]:
        """All CLOB token ids across the markets, order-preserving and de-duped."""
        ordered: dict[str, None] = {}
        for market in self.markets:
            for token in market.token_ids:
                ordered[token] = None
        return tuple(ordered)


# --------------------------------------------------------------------------- #
# Pure parsing helpers (no network) — kept testable in isolation.
# --------------------------------------------------------------------------- #


def _as_event_object(payload: Any) -> dict[str, Any]:
    """Normalize a ``/events`` response to a single event object.

    ``GET /events/{id}`` returns an object; ``GET /events?id=`` returns a
    one-element array. Accept either.
    """
    if isinstance(payload, list):
        if not payload:
            raise GammaError("event not found (empty list from /events)")
        payload = payload[0]
    if not isinstance(payload, dict):
        raise GammaError(f"unexpected /events response type: {type(payload).__name__}")
    return payload


def _parse_token_ids(market: dict[str, Any]) -> tuple[str, ...]:
    """Parse a market's ``clobTokenIds`` (a JSON-encoded *string*) into token ids."""
    raw = market.get("clobTokenIds")
    if raw is None or raw == "":
        return ()
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise GammaError(f"could not parse clobTokenIds {raw!r}") from exc
    if not isinstance(raw, list):
        raise GammaError(f"clobTokenIds is not a list: {raw!r}")
    return tuple(str(token) for token in raw)


def _parse_market(market: dict[str, Any]) -> Market:
    """Build a :class:`Market` from a raw Gamma market object."""
    return Market(
        id=str(market.get("id", "")),
        condition_id=str(market.get("conditionId", "")),
        token_ids=_parse_token_ids(market),
    )


def _parse_event(payload: Any, event_id: str) -> EventInfo:
    """Build an :class:`EventInfo` from a raw ``/events`` response."""
    obj = _as_event_object(payload)
    markets_raw = obj.get("markets") or []
    if not isinstance(markets_raw, list):
        raise GammaError(f"event {event_id}: 'markets' is not a list")
    markets = tuple(_parse_market(m) for m in markets_raw)
    return EventInfo(
        event_id=str(obj.get("id", event_id)),
        title=obj.get("title"),
        slug=obj.get("slug"),
        markets=markets,
        raw=obj,
    )


def _filter_markets(markets: tuple[Market, ...], market_ids: tuple[str, ...]) -> tuple[Market, ...]:
    """Restrict ``markets`` to those matching ``market_ids`` (by Gamma id or condition id)."""
    if not market_ids:
        return markets
    wanted = set(market_ids)
    selected = tuple(m for m in markets if m.id in wanted or m.condition_id in wanted)
    if not selected:
        raise GammaError(
            f"no markets matched --market-id {sorted(wanted)}; "
            f"available gamma ids={[m.id for m in markets]} "
            f"condition ids={[m.condition_id for m in markets]}"
        )
    return selected


# --------------------------------------------------------------------------- #
# Network client.
# --------------------------------------------------------------------------- #


class GammaClient:
    """Async, read-only client for the Gamma REST API.

    Usable as an async context manager::

        async with GammaClient() as gamma:
            event = await gamma.resolve_event("12345")

    A pre-built ``httpx.AsyncClient`` may be injected (e.g. with a
    ``MockTransport``) for testing; in that case the caller owns its lifecycle.
    """

    def __init__(
        self,
        base_url: str = GAMMA_BASE_URL,
        *,
        timeout: float = 15.0,
        max_retries: int = 4,
        backoff_base: float = 0.5,
        page_delay: float = 0.25,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._max_retries = max_retries
        self._backoff_base = backoff_base
        self._page_delay = page_delay
        if client is not None:
            self._client = client
            self._owns_client = False
        else:
            self._client = httpx.AsyncClient(
                base_url=base_url,
                timeout=timeout,
                headers={"User-Agent": _USER_AGENT, "Accept": "application/json"},
            )
            self._owns_client = True

    async def __aenter__(self) -> GammaClient:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        """Close the underlying client if this instance created it."""
        if self._owns_client:
            await self._client.aclose()

    async def _get(self, path: str, params: dict[str, str] | None = None) -> Any:
        """GET ``path`` and return parsed JSON, retrying transient failures.

        Retries network errors and HTTP 5xx with exponential backoff; HTTP 4xx
        is raised immediately (not retried).
        """
        for attempt in range(self._max_retries + 1):
            try:
                resp = await self._client.get(path, params=params)
                resp.raise_for_status()
                return resp.json()
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code < 500 or attempt >= self._max_retries:
                    raise
                reason = f"HTTP {exc.response.status_code}"
            except httpx.TransportError as exc:
                if attempt >= self._max_retries:
                    raise
                reason = type(exc).__name__
            delay = self._backoff_base * (2**attempt)
            logger.warning(
                "Gamma GET %s failed (%s); retry %d/%d in %.1fs",
                path,
                reason,
                attempt + 1,
                self._max_retries,
                delay,
            )
            await asyncio.sleep(delay)
        # Loop always returns or raises above.
        raise GammaError(f"Gamma GET {path} exhausted retries")  # pragma: no cover

    async def resolve_event(self, event_id: str, market_ids: tuple[str, ...] = ()) -> EventInfo:
        """Resolve an Event ID to its markets and CLOB token ids.

        Args:
            event_id: Numeric Polymarket event id.
            market_ids: Optional ``--market-id`` overrides; matched against each
                market's Gamma id or condition id.

        Raises:
            GammaError: if the event is missing or the response is unusable.
        """
        try:
            payload = await self._get(f"/events/{event_id}")
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                raise GammaError(f"event {event_id} not found (HTTP 404)") from exc
            raise
        info = _parse_event(payload, event_id)
        if market_ids:
            info = replace(info, markets=_filter_markets(info.markets, market_ids))
        if not info.clob_token_ids:
            logger.warning(
                "event %s has no CLOB token ids; the book stream has nothing to subscribe to",
                event_id,
            )
        logger.info(
            "resolved event %s: %d market(s), %d CLOB token id(s)",
            event_id,
            len(info.markets),
            len(info.clob_token_ids),
        )
        return info

    async def fetch_comments(
        self,
        event_id: str,
        *,
        limit: int = 100,
        offset: int = 0,
        ascending: bool = True,
    ) -> list[dict[str, Any]]:
        """Fetch a single page of comments for an event.

        Mirrors ``GET /comments?parent_entity_type=Event&parent_entity_id=...``.
        """
        params = {
            "parent_entity_type": "Event",
            "parent_entity_id": str(event_id),
            "order": "createdAt",
            "ascending": "true" if ascending else "false",
            "limit": str(limit),
            "offset": str(offset),
        }
        data = await self._get("/comments", params=params)
        if not isinstance(data, list):
            raise GammaError(f"unexpected /comments response type: {type(data).__name__}")
        return data

    async def backfill_since(
        self,
        event_id: str,
        last_seen_id: str | None = None,
        *,
        page_size: int = 100,
        max_pages: int = 50,
    ) -> list[dict[str, Any]]:
        """Fetch comments created since ``last_seen_id`` (exclusive).

        The Gamma API has no ``since``/``after`` cursor, so this pages
        **descending** (newest first) and stops as soon as ``last_seen_id`` is
        reached — fetching only the missed tail rather than the whole history.

        Args:
            event_id: Numeric event id.
            last_seen_id: Id of the last comment already recorded. ``None`` pulls
                up to ``max_pages`` of the most recent comments.
            page_size: Comments per page.
            max_pages: Safety bound on pages walked.

        Returns:
            Missed comments in chronological (oldest-first) order. Downstream
            de-duplication still guards against any overlap with the live stream.
        """
        collected: list[dict[str, Any]] = []
        reached = False
        for page in range(max_pages):
            batch = await self.fetch_comments(
                event_id, limit=page_size, offset=page * page_size, ascending=False
            )
            if not batch:
                break
            for comment in batch:
                if last_seen_id is not None and str(comment.get("id")) == str(last_seen_id):
                    reached = True
                    break
                collected.append(comment)
            if reached or len(batch) < page_size:
                break
            await asyncio.sleep(self._page_delay)  # be polite between pages
        collected.reverse()  # chronological order
        if last_seen_id is not None and not reached:
            logger.warning(
                "backfill for event %s did not reach last-seen id %s within %d page(s); "
                "possible gap in recovered comments",
                event_id,
                last_seen_id,
                max_pages,
            )
        logger.info("backfilled %d comment(s) for event %s", len(collected), event_id)
        return collected
