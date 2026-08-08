from datetime import UTC, datetime, timedelta
from uuid import uuid4

import httpx
import pytest

from gapforge.collectors.base import RequestBudget, bounded_get_json, cap_thread_items, collect_isolated
from gapforge.collectors.github import GitHubCollector
from gapforge.collectors.hacker_news import HackerNewsCollector
from gapforge.collectors.reddit import RedditCollector
from gapforge.domain.contracts import Availability, CollectRequest, CollectResult, QueryIntent, QueryIntentKind, Source

NOW = datetime(2026, 8, 9, tzinfo=UTC)


def request(source: Source, max_requests: int = 10, max_signals: int = 30) -> CollectRequest:
    return CollectRequest(
        mission_revision_id=uuid4(),
        intent=QueryIntent(id="q-1", kind=QueryIntentKind.BROAD, concept="invoice pain", sources=(source,), rationale="manual work"),
        since=NOW - timedelta(days=30), until=NOW, max_requests=max_requests, max_signals=max_signals,
    )


@pytest.mark.asyncio
async def test_hn_normalizes_and_limits_each_thread_to_parent_plus_twenty() -> None:
    timestamp = int((NOW - timedelta(days=1)).timestamp())
    hits = [{"objectID": "p", "title": "Invoices consume Fridays", "author": "alice", "created_at_i": timestamp}]
    hits.extend(
        {"objectID": f"c{i}", "story_id": "p", "comment_text": f"same pain {i}", "author": f"u{i}", "created_at_i": timestamp + i + 1}
        for i in range(25)
    )
    transport = httpx.MockTransport(lambda req: httpx.Response(200, json={"hits": hits, "page": 0, "nbPages": 1}))
    async with httpx.AsyncClient(transport=transport) as client:
        result = await HackerNewsCollector(client).collect(request(Source.HACKER_NEWS))
    assert result.availability is Availability.AVAILABLE
    assert len(result.items) == 21
    assert result.items[6].metadata["thread_weight"] < 1
    assert result.request_count == 1


def test_comment_only_thread_caps_at_twenty_and_weights_after_five() -> None:
    items = tuple(
        HackerNewsCollector._normalize({
            "objectID": f"c{i}", "story_id": "missing", "comment_text": f"pain {i}",
            "author": f"u{i}", "created_at_i": int((NOW - timedelta(days=1)).timestamp()) + i,
        })
        for i in range(25)
    )
    capped = cap_thread_items(tuple(item for item in items if item is not None), 30)
    assert len(capped) == 20
    assert [item.metadata["thread_weight"] for item in capped[:5]] == [1.0] * 5
    assert capped[5].metadata["thread_weight"] < 1


@pytest.mark.asyncio
async def test_github_filters_pr_bot_template_and_generic_bug() -> None:
    valid = {
        "id": 7, "number": 4, "title": "Monthly reconciliation takes all Friday", "body": "We export three CSVs manually",
        "html_url": "https://github.com/acme/app/issues/4", "repository_url": "https://api.github.com/repos/acme/app",
        "comments_url": "https://api.github.com/repos/acme/app/issues/4/comments", "comments": 1,
        "created_at": "2026-08-01T00:00:00Z", "updated_at": "2026-08-02T00:00:00Z", "user": {"login": "alice", "type": "User"},
    }
    invalid = [
        {**valid, "id": 8, "pull_request": {}},
        {**valid, "id": 9, "user": {"login": "dependabot[bot]", "type": "Bot"}},
        {**valid, "id": 10, "title": "Bug: broken"},
        {**valid, "id": 11, "body": "### Description\n<!-- fill -->\nsteps to reproduce"},
    ]

    def handler(req: httpx.Request) -> httpx.Response:
        if req.url.path == "/search/issues":
            return httpx.Response(200, json={"items": [valid, *invalid]})
        return httpx.Response(200, json=[{
            "id": 70, "body": "I spend two hours on this too", "html_url": "https://github.com/acme/app/issues/4#issuecomment-70",
            "created_at": "2026-08-03T00:00:00Z", "updated_at": "2026-08-03T00:00:00Z", "user": {"login": "bob", "type": "User"},
        }])

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await GitHubCollector(client, token="secret").collect(request(Source.GITHUB))
    assert [item.external_id for item in result.items] == ["7", "70"]
    assert result.items[1].parent_thread_id == "7"
    assert result.request_count == 2


@pytest.mark.asyncio
async def test_reddit_missing_credentials_is_structured_and_makes_no_request() -> None:
    calls = 0

    def handler(req: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(500)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await RedditCollector(client, client_id=None, client_secret=None).collect(request(Source.REDDIT))
    assert result.availability is Availability.SOURCE_UNAVAILABLE
    assert result.warnings[0].code == "REDDIT_CREDENTIALS_MISSING"
    assert calls == 0


@pytest.mark.asyncio
async def test_reddit_filters_results_to_exact_requested_window() -> None:
    async def handler(req: httpx.Request) -> httpx.Response:
        if req.url.path == "/api/v1/access_token":
            return httpx.Response(200, json={"access_token": "access"})
        inside = int((NOW - timedelta(days=1)).timestamp())
        outside = int((NOW - timedelta(days=40)).timestamp())
        def child(identifier: str, created: int) -> dict[str, object]:
            return {"data": {
                "name": identifier, "title": f"Pain {identifier}", "selftext": "manual work", "permalink": f"/r/work/{identifier}",
                "author": "human", "created_utc": created, "score": 100, "num_comments": 5,
            }}
        return httpx.Response(200, json={"data": {"children": [child("inside", inside), child("outside", outside)], "after": None}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await RedditCollector(client, client_id="id", client_secret="secret").collect(request(Source.REDDIT))
    assert [item.external_id for item in result.items] == ["inside"]


@pytest.mark.asyncio
async def test_retries_are_bounded_by_request_budget() -> None:
    calls = 0

    def handler(req: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(503, request=req)

    async def no_sleep(seconds: float) -> None:
        assert seconds > 0

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(Exception, match="source request failed"):
            await bounded_get_json(client, "https://example.com", budget=RequestBudget(2), sleeper=no_sleep)
    assert calls == 2


@pytest.mark.asyncio
async def test_collector_failure_is_isolated_from_success() -> None:
    class Broken:
        async def collect(self, collect_request: CollectRequest) -> CollectResult:
            raise RuntimeError("secret-bearing internal message")

    class Working:
        async def collect(self, collect_request: CollectRequest) -> CollectResult:
            return CollectResult(source=Source.HACKER_NEWS, availability=Availability.AVAILABLE, request_count=0)

    results = await collect_isolated(
        (
            (Source.REDDIT, Broken(), request(Source.REDDIT)),
            (Source.HACKER_NEWS, Working(), request(Source.HACKER_NEWS)),
        )
    )
    assert results[0].availability is Availability.SOURCE_UNAVAILABLE
    assert "secret-bearing" not in results[0].warnings[0].message
    assert results[1].availability is Availability.AVAILABLE
