"""Credential-free Hacker News collector using the public Algolia API."""

from __future__ import annotations

import httpx
from pydantic import HttpUrl

from gapforge.collectors.base import (
    CollectorResponseError,
    RequestBudget,
    bounded_get_json,
    cap_thread_items,
    in_window,
    utc_from_timestamp,
)
from gapforge.domain.contracts import (
    Availability,
    CollectedItem,
    CollectRequest,
    CollectResult,
    Engagement,
    Source,
    SourceCheckpoint,
    SourceWarning,
)


class HackerNewsCollector:
    endpoint = "https://hn.algolia.com/api/v1/search_by_date"

    def __init__(self, client: httpx.AsyncClient) -> None:
        self._client = client

    async def collect(self, request: CollectRequest) -> CollectResult:
        budget = RequestBudget(request.max_requests)
        params: dict[str, str | int] = {
            "query": request.intent.concept,
            "tags": "(story,comment)",
            "numericFilters": (
                f"created_at_i>={int(request.since.timestamp())},"
                f"created_at_i<{int(request.until.timestamp())}"
            ),
            "hitsPerPage": min(request.max_signals, 100),
            "page": int(request.checkpoint.cursor)
            if request.checkpoint and request.checkpoint.cursor
            else 0,
        }
        try:
            payload = await bounded_get_json(
                self._client, self.endpoint, budget=budget, params=params
            )
            if not isinstance(payload, dict) or not isinstance(payload.get("hits"), list):
                raise CollectorResponseError("HN response missing hits")
            normalized = [
                item
                for hit in payload["hits"]
                if (item := self._normalize(hit)) is not None and in_window(item, request)
            ]
            items = cap_thread_items(normalized, request.max_signals)
            next_page = int(payload.get("page", 0)) + 1
            exhausted = next_page >= int(payload.get("nbPages", 1))
            checkpoint = SourceCheckpoint(
                source=Source.HACKER_NEWS,
                cursor=None if exhausted else str(next_page),
                watermark=max((item.source_created_at for item in items), default=request.since),
            )
            return CollectResult(
                source=Source.HACKER_NEWS,
                availability=Availability.AVAILABLE,
                items=items,
                checkpoint=checkpoint,
                request_count=budget.used,
            )
        except CollectorResponseError as exc:
            return CollectResult(
                source=Source.HACKER_NEWS,
                availability=Availability.SOURCE_UNAVAILABLE,
                request_count=budget.used,
                warnings=(
                    SourceWarning(code="HN_UNAVAILABLE", message=str(exc), retryable=exc.retryable),
                ),
            )

    @staticmethod
    def _normalize(hit: object) -> CollectedItem | None:
        if not isinstance(hit, dict):
            return None
        external_id = str(hit.get("objectID", "")).strip()
        if not external_id:
            return None
        story_id = str(hit.get("story_id") or external_id)
        is_comment = hit.get("comment_text") is not None
        body = hit.get("comment_text") if is_comment else hit.get("story_text")
        title = None if is_comment else hit.get("title")
        if not body and not title:
            return None
        return CollectedItem(
            source=Source.HACKER_NEWS,
            external_id=external_id,
            canonical_url=HttpUrl(f"https://news.ycombinator.com/item?id={external_id}"),
            parent_thread_id=story_id if is_comment else None,
            author_identity=str(hit["author"]) if hit.get("author") else None,
            title=str(title)[:500] if title else None,
            body=str(body)[:20_000] if body else None,
            source_created_at=utc_from_timestamp(hit.get("created_at_i")),
            engagement=Engagement(
                score=max(0, int(hit.get("points") or 0)),
                comments=max(0, int(hit.get("num_comments") or 0)),
            ),
            metadata={"story_id": story_id, "source_api": "hn_algolia"},
        )
