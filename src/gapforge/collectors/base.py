"""Common collector protocol, bounded HTTP client, and budget helpers."""

from __future__ import annotations

import asyncio
import json
import random
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

import httpx

from gapforge.domain.contracts import (
    Availability,
    CollectRequest,
    CollectResult,
    CollectedItem,
    Source,
    SourceWarning,
)

MAX_RESPONSE_BYTES = 2_000_000
DEFAULT_TIMEOUT = httpx.Timeout(10.0, connect=5.0)


class Collector(Protocol):
    async def collect(self, request: CollectRequest) -> CollectResult: ...


class CollectorError(RuntimeError):
    """Base class for sanitized source failures."""


class CollectorBudgetExceeded(CollectorError):
    pass


class CollectorResponseError(CollectorError):
    def __init__(self, message: str, *, retryable: bool = False) -> None:
        super().__init__(message)
        self.retryable = retryable


async def collect_isolated(
    jobs: Sequence[tuple[Source, Collector, CollectRequest]],
) -> tuple[CollectResult, ...]:
    """Run collectors independently so one unexpected failure cannot erase peer results."""

    async def safe_collect(
        source: Source, collector: Collector, request: CollectRequest
    ) -> CollectResult:
        try:
            return await collector.collect(request)
        except Exception as exc:  # boundary isolates third-party client defects
            return CollectResult(
                source=source,
                availability=Availability.SOURCE_UNAVAILABLE,
                request_count=0,
                warnings=(
                    SourceWarning(
                        code="COLLECTOR_FAILED",
                        message=f"collector failed: {type(exc).__name__}",
                        retryable=False,
                    ),
                ),
            )

    return tuple(await asyncio.gather(*(safe_collect(*job) for job in jobs)))


@dataclass(slots=True)
class RequestBudget:
    maximum: int
    used: int = 0

    def consume(self) -> None:
        if self.used >= self.maximum:
            raise CollectorBudgetExceeded("collector request budget exhausted")
        self.used += 1


Sleeper = Callable[[float], Awaitable[None]]
Jitter = Callable[[float, float], float]


async def bounded_get_json(
    client: httpx.AsyncClient,
    url: str,
    *,
    budget: RequestBudget,
    params: dict[str, str | int] | None = None,
    headers: dict[str, str] | None = None,
    attempts: int = 3,
    sleeper: Sleeper = asyncio.sleep,
    jitter: Jitter = random.uniform,
) -> object:
    return await bounded_request_json(
        client,
        "GET",
        url,
        budget=budget,
        params=params,
        headers=headers,
        attempts=attempts,
        sleeper=sleeper,
        jitter=jitter,
    )


async def bounded_request_json(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    *,
    budget: RequestBudget,
    params: dict[str, str | int] | None = None,
    data: dict[str, str] | None = None,
    headers: dict[str, str] | None = None,
    attempts: int = 3,
    sleeper: Sleeper = asyncio.sleep,
    jitter: Jitter = random.uniform,
) -> object:
    """Fetch bounded JSON with safe retries and no secret-bearing errors."""
    if not 1 <= attempts <= 3:
        raise ValueError("attempts must be between 1 and 3")
    last_error: Exception | None = None
    last_retryable = False
    for attempt in range(attempts):
        budget.consume()
        response: httpx.Response | None = None
        try:
            request = client.build_request(
                method,
                url,
                params=params,
                data=data,
                headers=headers,
                timeout=DEFAULT_TIMEOUT,
            )
            response = await client.send(request, stream=True, follow_redirects=False)
            if response.status_code in {429, 500, 502, 503, 504}:
                raise httpx.HTTPStatusError(
                    "transient source response",
                    request=response.request,
                    response=response,
                )
            response.raise_for_status()
            declared = response.headers.get("content-length")
            if declared:
                try:
                    declared_size = int(declared)
                except ValueError as exc:
                    raise CollectorResponseError(
                        "source response has invalid length"
                    ) from exc
                if declared_size > MAX_RESPONSE_BYTES:
                    raise CollectorResponseError("source response exceeded byte limit")
            body = bytearray()
            async for chunk in response.aiter_bytes():
                body.extend(chunk)
                if len(body) > MAX_RESPONSE_BYTES:
                    raise CollectorResponseError("source response exceeded byte limit")
            return json.loads(body)
        except (
            httpx.TimeoutException,
            httpx.NetworkError,
            httpx.HTTPStatusError,
        ) as exc:
            last_error = exc
            retryable = not isinstance(
                exc, httpx.HTTPStatusError
            ) or exc.response.status_code in {
                429,
                500,
                502,
                503,
                504,
            }
            last_retryable = retryable
            if (
                not retryable
                or attempt + 1 >= attempts
                or budget.used >= budget.maximum
            ):
                break
            await sleeper(0.05 * (2**attempt) + jitter(0.0, 0.02))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise CollectorResponseError("source returned invalid JSON") from exc
        finally:
            if response is not None:
                await response.aclose()
    raise CollectorResponseError(
        f"source request failed: {type(last_error).__name__}",
        retryable=last_retryable,
    ) from last_error


def cap_thread_items(
    items: Sequence[CollectedItem], maximum: int
) -> tuple[CollectedItem, ...]:
    """Retain a parent plus at most 20 comments and annotate diminishing comment weight."""
    grouped: dict[str, list[CollectedItem]] = {}
    for item in items:
        thread = item.parent_thread_id or item.external_id
        grouped.setdefault(thread, []).append(item)

    accepted: list[CollectedItem] = []
    for thread in sorted(grouped):
        group = grouped[thread]
        parents = sorted(
            (item for item in group if item.parent_thread_id is None),
            key=lambda item: (item.source_created_at, item.external_id),
        )[:1]
        comments = sorted(
            (item for item in group if item.parent_thread_id is not None),
            key=lambda item: (item.source_created_at, item.external_id),
        )[:20]
        ordered = [*parents, *comments]
        for item in ordered:
            if len(accepted) >= maximum:
                return tuple(accepted)
            comment_number = (
                comments.index(item) + 1 if item.parent_thread_id is not None else 0
            )
            weight = 1.0 if comment_number <= 5 else round(5 / comment_number, 4)
            accepted.append(
                item.model_copy(
                    update={"metadata": {**item.metadata, "thread_weight": weight}}
                )
            )
    return tuple(accepted)


def in_window(item: CollectedItem, request: CollectRequest) -> bool:
    return request.since <= item.source_created_at < request.until


def utc_from_timestamp(value: int | float | str | None) -> datetime:
    if value is None:
        return datetime.now(UTC)
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    return datetime.fromtimestamp(float(value), tz=UTC)
