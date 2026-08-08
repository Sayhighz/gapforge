"""Reddit collector using official OAuth endpoints only."""

from __future__ import annotations

import base64

import httpx
from pydantic import HttpUrl

from gapforge.collectors.base import CollectorResponseError, RequestBudget, bounded_get_json, cap_thread_items, utc_from_timestamp
from gapforge.domain.contracts import (
    Availability,
    CollectRequest,
    CollectResult,
    CollectedItem,
    Engagement,
    Source,
    SourceCheckpoint,
    SourceWarning,
)


class RedditCollector:
    endpoint = "https://oauth.reddit.com"
    token_endpoint = "https://www.reddit.com/api/v1/access_token"

    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        client_id: str | None,
        client_secret: str | None,
        user_agent: str = "gapforge/0.1",
    ) -> None:
        self._client = client
        self._client_id = client_id
        self._client_secret = client_secret
        self._user_agent = user_agent

    async def collect(self, request: CollectRequest) -> CollectResult:
        if not self._client_id or not self._client_secret:
            return CollectResult(
                source=Source.REDDIT,
                availability=Availability.SOURCE_UNAVAILABLE,
                request_count=0,
                warnings=(
                    SourceWarning(
                        code="REDDIT_CREDENTIALS_MISSING",
                        message="Reddit OAuth client credentials are not configured",
                        retryable=False,
                    ),
                ),
            )
        budget = RequestBudget(request.max_requests)
        try:
            token = await self._access_token(budget)
            after = request.checkpoint.cursor if request.checkpoint else None
            payload = await bounded_get_json(
                self._client,
                f"{self.endpoint}/search",
                budget=budget,
                params={"q": request.intent.concept, "sort": "new", "t": "year", "limit": min(100, request.max_signals), **({"after": after} if after else {})},
                headers={"Authorization": f"Bearer {token}", "User-Agent": self._user_agent},
            )
            data = payload.get("data") if isinstance(payload, dict) else None
            if not isinstance(data, dict):
                raise CollectorResponseError("Reddit response missing data")
            children = data.get("children")
            if not isinstance(children, list):
                raise CollectorResponseError("Reddit response missing children")
            items = [item for child in children if (item := self._normalize(child)) is not None]
            capped = cap_thread_items(items, request.max_signals)
            return CollectResult(
                source=Source.REDDIT,
                availability=Availability.AVAILABLE,
                items=capped,
                checkpoint=SourceCheckpoint(
                    source=Source.REDDIT,
                    cursor=str(data.get("after")) if data.get("after") else None,
                    watermark=max((item.source_created_at for item in capped), default=request.since),
                ),
                request_count=budget.used,
            )
        except CollectorResponseError as exc:
            return CollectResult(
                source=Source.REDDIT,
                availability=Availability.SOURCE_UNAVAILABLE,
                request_count=budget.used,
                warnings=(SourceWarning(code="REDDIT_UNAVAILABLE", message=str(exc), retryable=True),),
            )

    async def _access_token(self, budget: RequestBudget) -> str:
        budget.consume()
        assert self._client_id is not None and self._client_secret is not None
        basic = base64.b64encode(f"{self._client_id}:{self._client_secret}".encode()).decode()
        try:
            response = await self._client.post(
                self.token_endpoint,
                data={"grant_type": "client_credentials"},
                headers={"Authorization": f"Basic {basic}", "User-Agent": self._user_agent},
                timeout=10,
            )
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict) or not isinstance(payload.get("access_token"), str):
                raise CollectorResponseError("Reddit token response is invalid")
            return str(payload["access_token"])
        except (httpx.HTTPError, ValueError) as exc:
            raise CollectorResponseError("Reddit OAuth authentication failed") from exc

    @staticmethod
    def _normalize(child: object) -> CollectedItem | None:
        if not isinstance(child, dict) or not isinstance(child.get("data"), dict):
            return None
        raw = child["data"]
        if raw.get("distinguished") or str(raw.get("author", "")).lower().endswith("bot"):
            return None
        identifier = str(raw.get("name") or raw.get("id") or "")
        title, body = str(raw.get("title") or ""), str(raw.get("selftext") or raw.get("body") or "")
        if not identifier or not (title or body):
            return None
        permalink = str(raw.get("permalink") or "")
        parent = str(raw.get("link_id") or "") or None
        return CollectedItem(
            source=Source.REDDIT,
            external_id=identifier,
            canonical_url=HttpUrl(f"https://www.reddit.com{permalink}"),
            parent_thread_id=parent,
            author_identity=None if raw.get("author") in {None, "[deleted]"} else str(raw["author"]),
            title=title[:500] or None,
            body=body[:20_000] or None,
            source_created_at=utc_from_timestamp(raw.get("created_utc")),
            engagement=Engagement(
                score=max(0, int(raw.get("score") or 0)),
                comments=max(0, int(raw.get("num_comments") or 0)),
            ),
            metadata={"subreddit": raw.get("subreddit"), "source_api": "reddit_oauth"},
        )
