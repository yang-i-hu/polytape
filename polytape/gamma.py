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
from urllib.parse import urlparse

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
    series_ids: tuple[str, ...] = ()  # parent series (e.g. a sports league/tournament)

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
    series_raw = obj.get("series") or []
    series_ids = (
        tuple(str(s["id"]) for s in series_raw if isinstance(s, dict) and s.get("id"))
        if isinstance(series_raw, list)
        else ()
    )
    return EventInfo(
        event_id=str(obj.get("id", event_id)),
        title=obj.get("title"),
        slug=obj.get("slug"),
        markets=markets,
        raw=obj,
        series_ids=series_ids,
    )


def parse_event_ref(text: str) -> tuple[str, str]:
    """Classify an event reference as a numeric id or a slug (pure, no network).

    Accepts a numeric event id (``"351729"``), a bare slug
    (``"fifwc-ksa-ury-2026-06-15"``), or a Polymarket URL
    (``"https://polymarket.com/sports/world-cup/fifwc-ksa-ury-2026-06-15"``) —
    from which the last path segment is taken as the slug.

    Returns ``("id", value)`` or ``("slug", value)``.
    """
    text = (text or "").strip()
    if not text:
        raise GammaError("empty event reference")
    if text.isdigit():
        return ("id", text)
    if "://" in text or "/" in text or text.lower().startswith("polymarket"):
        parsed = urlparse(text if "://" in text else "https://" + text)
        segments = [seg for seg in parsed.path.split("/") if seg]
        if not segments:
            raise GammaError(f"could not find a slug in URL {text!r}")
        return ("slug", segments[-1])
    return ("slug", text)


def resolve_event_id(
    ref: str,
    *,
    base_url: str = GAMMA_BASE_URL,
    timeout: float = 10.0,
    client: httpx.Client | None = None,
) -> str:
    """Resolve an event id, slug, or Polymarket URL to a numeric event id.

    A numeric reference is returned as-is (no network). A slug/URL is looked up
    via ``GET /events?slug=<slug>``. Synchronous (used by the dashboard control
    plane); raises :class:`GammaError` if the slug cannot be resolved.
    """
    kind, value = parse_event_ref(ref)
    if kind == "id":
        return value
    owns = client is None
    if client is None:
        client = httpx.Client(
            base_url=base_url,
            timeout=timeout,
            headers={"User-Agent": _USER_AGENT, "Accept": "application/json"},
        )
    try:
        resp = client.get("/events", params={"slug": value})
        resp.raise_for_status()
        data = resp.json()
    except httpx.HTTPError as exc:
        raise GammaError(f"could not resolve slug {value!r}: {exc}") from exc
    finally:
        if owns:
            client.close()
    events = data if isinstance(data, list) else [data]
    for event in events:
        if isinstance(event, dict) and event.get("id"):
            logger.info("resolved %r -> event %s (%s)", ref, event["id"], event.get("title"))
            return str(event["id"])
    raise GammaError(f"no event found for slug {value!r}")


def _get_json(client: httpx.Client, path: str, params: dict[str, str] | None = None) -> Any:
    resp = client.get(path, params=params)
    resp.raise_for_status()
    return resp.json()


def _first_event(payload: Any) -> dict[str, Any] | None:
    if isinstance(payload, list):
        return next((e for e in payload if isinstance(e, dict) and e.get("id")), None)
    return payload if isinstance(payload, dict) and payload.get("id") else None


def related_events(
    ref: str,
    *,
    base_url: str = GAMMA_BASE_URL,
    timeout: float = 12.0,
    client: httpx.Client | None = None,
    limit: int = 80,
) -> dict[str, Any]:
    """List the events related to ``ref`` — the other open events in its series.

    Resolves the referenced event (id/slug/URL), finds its parent series, and
    returns that series' currently-open events (e.g. the other matches in a
    tournament) so the user can pick which to record. Synchronous; used by the
    dashboard. Raises :class:`GammaError` if the reference can't be resolved.
    """
    kind, value = parse_event_ref(ref)
    own = client is None
    if client is None:
        client = httpx.Client(
            base_url=base_url,
            timeout=timeout,
            headers={"User-Agent": _USER_AGENT, "Accept": "application/json"},
        )
    try:
        source = _first_event(
            _get_json(client, f"/events/{value}")
            if kind == "id"
            else _get_json(client, "/events", {"slug": value})
        )
        if source is None:
            raise GammaError(f"could not resolve {ref!r}")
        series = source.get("series") or []
        series_id = (
            str(series[0]["id"])
            if series and isinstance(series[0], dict) and series[0].get("id")
            else None
        )
        if series_id:
            listing = _get_json(
                client,
                "/events",
                {
                    "series_id": series_id,
                    "closed": "false",
                    "order": "startDate",
                    "ascending": "true",
                    "limit": str(limit),
                },
            )
            events = listing if isinstance(listing, list) else []
        else:
            events = [source]
    except httpx.HTTPError as exc:
        raise GammaError(f"could not list related events for {ref!r}: {exc}") from exc
    finally:
        if own:
            client.close()

    compact = [
        {
            "event_id": str(e.get("id")),
            "slug": e.get("slug"),
            "title": e.get("title") or e.get("slug"),
            "closed": bool(e.get("closed")),
            "start_date": e.get("startDate"),
        }
        for e in events
        if isinstance(e, dict) and e.get("id")
    ]
    return {
        "source_event_id": str(source.get("id")),
        "series_id": series_id,
        "series_title": (series[0].get("title") if series_id else None),
        "events": compact,
    }


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
        parent_entity_id: str,
        *,
        parent_entity_type: str = "Event",
        limit: int = 100,
        offset: int = 0,
        ascending: bool = True,
    ) -> list[dict[str, Any]]:
        """Fetch a single page of comments for a parent entity.

        Mirrors ``GET /comments?parent_entity_type=<type>&parent_entity_id=...``.
        ``parent_entity_type`` is ``"Event"`` by default, or ``"Series"`` for the
        parent league/tournament chat.
        """
        params = {
            "parent_entity_type": parent_entity_type,
            "parent_entity_id": str(parent_entity_id),
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
        parent_entity_id: str,
        last_seen_id: str | None = None,
        *,
        parent_entity_type: str = "Event",
        page_size: int = 100,
        max_pages: int = 50,
    ) -> list[dict[str, Any]]:
        """Fetch comments for a parent entity created since ``last_seen_id`` (exclusive).

        The Gamma API has no ``since``/``after`` cursor, so this pages
        **descending** (newest first) and stops as soon as ``last_seen_id`` is
        reached — fetching only the missed tail rather than the whole history.

        Args:
            parent_entity_id: Numeric id of the parent (an event id, or a series id).
            last_seen_id: Id of the last comment already recorded for this parent.
                ``None`` pulls up to ``max_pages`` of the most recent comments.
            parent_entity_type: ``"Event"`` (default) or ``"Series"``.
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
                parent_entity_id,
                parent_entity_type=parent_entity_type,
                limit=page_size,
                offset=page * page_size,
                ascending=False,
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
                "backfill for %s %s did not reach last-seen id %s within %d page(s); "
                "possible gap in recovered comments",
                parent_entity_type,
                parent_entity_id,
                last_seen_id,
                max_pages,
            )
        logger.info(
            "backfilled %d comment(s) for %s %s",
            len(collected),
            parent_entity_type,
            parent_entity_id,
        )
        return collected
