"""GitHub issue collector using the official REST API."""

from __future__ import annotations

import re

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
    CollectRequest,
    CollectResult,
    CollectedItem,
    Engagement,
    Source,
    SourceCheckpoint,
    SourceWarning,
)

GENERIC_BUG = re.compile(r"^(bug|issue|problem|help|error)(?:\W|$)", re.IGNORECASE)
TEMPLATE_MARKERS = ("### description", "<!--", "steps to reproduce", "checklist")


class GitHubCollector:
    endpoint = "https://api.github.com"

    def __init__(self, client: httpx.AsyncClient, token: str | None = None) -> None:
        self._client = client
        self._token = token

    @property
    def headers(self) -> dict[str, str]:
        headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        return headers

    async def collect(self, request: CollectRequest) -> CollectResult:
        budget = RequestBudget(request.max_requests)
        page = (
            int(request.checkpoint.cursor)
            if request.checkpoint and request.checkpoint.cursor
            else 1
        )
        query = f"{request.intent.concept} is:issue created:{request.since.date()}..{request.until.date()}"
        try:
            payload = await bounded_get_json(
                self._client,
                f"{self.endpoint}/search/issues",
                budget=budget,
                params={
                    "q": query,
                    "sort": "updated",
                    "order": "desc",
                    "per_page": min(100, request.max_signals),
                    "page": page,
                },
                headers=self.headers,
            )
            if not isinstance(payload, dict) or not isinstance(
                payload.get("items"), list
            ):
                raise CollectorResponseError("GitHub response missing items")
            items: list[CollectedItem] = []
            for raw in payload["items"]:
                normalized = self._normalize_issue(raw)
                if normalized is None or not in_window(normalized, request):
                    continue
                items.append(normalized)
                if len(items) >= request.max_signals:
                    break
                comments_url = (
                    raw.get("comments_url") if isinstance(raw, dict) else None
                )
                if (
                    comments_url
                    and int(raw.get("comments") or 0)
                    and budget.used < budget.maximum
                ):
                    comments = await bounded_get_json(
                        self._client,
                        str(comments_url),
                        budget=budget,
                        params={"per_page": min(20, request.max_signals - len(items))},
                        headers=self.headers,
                    )
                    if isinstance(comments, list):
                        items.extend(
                            item
                            for comment in comments[:20]
                            if (item := self._normalize_comment(comment, normalized))
                            is not None
                            and in_window(item, request)
                        )
            capped = cap_thread_items(items, request.max_signals)
            return CollectResult(
                source=Source.GITHUB,
                availability=Availability.AVAILABLE,
                items=capped,
                checkpoint=SourceCheckpoint(
                    source=Source.GITHUB,
                    cursor=str(page + 1) if len(payload["items"]) else None,
                    watermark=max(
                        (item.source_created_at for item in capped),
                        default=request.since,
                    ),
                ),
                request_count=budget.used,
            )
        except CollectorResponseError as exc:
            return CollectResult(
                source=Source.GITHUB,
                availability=Availability.SOURCE_UNAVAILABLE,
                request_count=budget.used,
                warnings=(
                    SourceWarning(
                        code="GITHUB_UNAVAILABLE",
                        message=str(exc),
                        retryable=exc.retryable,
                    ),
                ),
            )

    @staticmethod
    def _is_bot(raw: dict[str, object]) -> bool:
        user = raw.get("user")
        return isinstance(user, dict) and (
            str(user.get("type", "")).lower() == "bot"
            or str(user.get("login", "")).lower().endswith("[bot]")
        )

    @classmethod
    def _normalize_issue(cls, raw: object) -> CollectedItem | None:
        if not isinstance(raw, dict) or "pull_request" in raw or cls._is_bot(raw):
            return None
        title, body = str(raw.get("title") or ""), str(raw.get("body") or "")
        lowered = body.lower()
        if (
            not title
            or GENERIC_BUG.match(title.strip())
            or sum(marker in lowered for marker in TEMPLATE_MARKERS) >= 2
        ):
            return None
        user = raw.get("user")
        login = (
            str(user.get("login"))
            if isinstance(user, dict) and user.get("login")
            else None
        )
        return CollectedItem(
            source=Source.GITHUB,
            external_id=str(raw.get("id")),
            canonical_url=HttpUrl(str(raw.get("html_url"))),
            parent_thread_id=None,
            author_identity=login,
            title=title[:500],
            body=body[:20_000] or None,
            source_created_at=utc_from_timestamp(raw.get("created_at")),
            source_edited_at=utc_from_timestamp(raw["updated_at"])
            if raw.get("updated_at")
            else None,
            engagement=Engagement(
                comments=max(0, int(raw.get("comments") or 0)), reactions=0
            ),
            metadata={
                "repository_url": raw.get("repository_url"),
                "number": raw.get("number"),
            },
        )

    @classmethod
    def _normalize_comment(
        cls, raw: object, parent: CollectedItem
    ) -> CollectedItem | None:
        if not isinstance(raw, dict) or cls._is_bot(raw) or not raw.get("body"):
            return None
        user = raw.get("user")
        login = (
            str(user.get("login"))
            if isinstance(user, dict) and user.get("login")
            else None
        )
        return CollectedItem(
            source=Source.GITHUB,
            external_id=str(raw.get("id")),
            canonical_url=HttpUrl(str(raw.get("html_url"))),
            parent_thread_id=parent.external_id,
            author_identity=login,
            body=str(raw["body"])[:20_000],
            source_created_at=utc_from_timestamp(raw.get("created_at")),
            source_edited_at=utc_from_timestamp(raw["updated_at"])
            if raw.get("updated_at")
            else None,
            metadata={"issue_external_id": parent.external_id},
        )
