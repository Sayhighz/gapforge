"""Bounded Brave Search client and result provenance registry."""

from __future__ import annotations

from datetime import UTC, datetime

import httpx
from pydantic import HttpUrl

from gapforge.collectors.base import CollectorResponseError, RequestBudget, bounded_get_json
from gapforge.domain.contracts import (
    Availability,
    ClaimKind,
    SearchResponse,
    SearchResult,
    SourceWarning,
)


class ApprovedUrlRegistry:
    """Exact normalized URL allowlist built only from search or explicit user input."""

    def __init__(self, search_results: tuple[SearchResult, ...], explicit_urls: tuple[str, ...] = ()) -> None:
        self._urls = {str(result.url) for result in search_results}
        self._urls.update(str(HttpUrl(url)) for url in explicit_urls)

    def require_approved(self, url: str) -> str:
        normalized = str(HttpUrl(url))
        if normalized not in self._urls:
            raise ValueError("URL was not supplied by search results or explicit user input")
        return normalized


def snippet_supports_claim(kind: ClaimKind) -> bool:
    """Search snippets may orient research but cannot prove price or feature claims."""
    return kind not in {ClaimKind.PRICE, ClaimKind.FEATURE}


class BraveSearchProvider:
    endpoint = "https://api.search.brave.com/res/v1/web/search"

    def __init__(self, client: httpx.AsyncClient, api_key: str | None) -> None:
        self._client = client
        self._api_key = api_key

    async def search(self, query: str, *, max_results: int = 10, max_requests: int = 1) -> SearchResponse:
        clean_query = " ".join(query.split())
        if not clean_query or len(clean_query) > 500:
            raise ValueError("search query must contain 1-500 characters")
        if not 1 <= max_results <= 20 or not 1 <= max_requests <= 20:
            raise ValueError("search caps exceeded")
        if not self._api_key:
            return SearchResponse(
                availability=Availability.RESEARCH_UNAVAILABLE,
                query=clean_query,
                request_count=0,
                warnings=(
                    SourceWarning(
                        code="BRAVE_CREDENTIALS_MISSING",
                        message="Brave Search credentials are not configured",
                        retryable=False,
                    ),
                ),
            )
        budget = RequestBudget(max_requests)
        try:
            payload = await bounded_get_json(
                self._client,
                self.endpoint,
                budget=budget,
                params={"q": clean_query, "count": max_results},
                headers={"Accept": "application/json", "X-Subscription-Token": self._api_key},
            )
            web = payload.get("web") if isinstance(payload, dict) else None
            raw_results = web.get("results") if isinstance(web, dict) else None
            if not isinstance(raw_results, list):
                raise CollectorResponseError("Brave response missing web results")
            observed_at = datetime.now(UTC)
            results: list[SearchResult] = []
            for raw in raw_results[:max_results]:
                if not isinstance(raw, dict) or not raw.get("url") or not raw.get("title"):
                    continue
                try:
                    results.append(
                        SearchResult(
                            id=f"brave-{len(results) + 1}",
                            title=str(raw["title"])[:500],
                            url=HttpUrl(str(raw["url"])),
                            snippet=str(raw.get("description") or "No snippet")[:500],
                            observed_at=observed_at,
                            rank=len(results) + 1,
                        )
                    )
                except ValueError:
                    continue
            return SearchResponse(
                availability=Availability.AVAILABLE,
                query=clean_query,
                results=tuple(results),
                request_count=budget.used,
            )
        except CollectorResponseError as exc:
            return SearchResponse(
                availability=Availability.RESEARCH_UNAVAILABLE,
                query=clean_query,
                request_count=budget.used,
                warnings=(SourceWarning(code="BRAVE_UNAVAILABLE", message=str(exc), retryable=True),),
            )

