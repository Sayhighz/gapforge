"""Production assembly for the durable ``research.run`` evidence handler."""

from __future__ import annotations

from datetime import UTC, datetime

import httpx

from gapforge.collectors import GitHubCollector, HackerNewsCollector, RedditCollector
from gapforge.config import Settings
from gapforge.domain.contracts import FetchResult, Source
from gapforge.integration.evidence_store import SqlAlchemyEvidencePipelineStore
from gapforge.integration.semantic import SemanticReasoner
from gapforge.search.brave import ApprovedUrlRegistry, BraveSearchProvider
from gapforge.security.static_fetch import StaticFetcher
from gapforge.storage.database import Database
from gapforge.storage.models import ResearchTask
from gapforge.worker import TaskHandlerResult

from .evidence_pipeline import EvidencePipeline


class StaticFetchAdapter:
    """Adapt the safe fetcher's registry API to the pipeline's explicit URL tuple."""

    def __init__(self, fetcher: StaticFetcher) -> None:
        self.fetcher = fetcher

    async def fetch(self, url: str, *, approved_urls: tuple[str, ...]) -> FetchResult:
        return await self.fetcher.fetch(
            url,
            ApprovedUrlRegistry((), explicit_urls=approved_urls),
        )


class ResearchRunHandler:
    """Own one bounded HTTP-client lifecycle per leased research task."""

    def __init__(
        self,
        *,
        database: Database,
        settings: Settings,
        reasoner: SemanticReasoner,
    ) -> None:
        self.database = database
        self.settings = settings
        self.reasoner = reasoner

    async def __call__(self, task: ResearchTask) -> TaskHandlerResult:
        timeout = httpx.Timeout(15.0, connect=5.0)
        limits = httpx.Limits(max_connections=10, max_keepalive_connections=5)
        async with httpx.AsyncClient(timeout=timeout, limits=limits) as client:
            github_token = _secret(self.settings.github_token)
            reddit_client_id = _secret(self.settings.reddit_client_id)
            reddit_client_secret = _secret(self.settings.reddit_client_secret)
            pipeline = EvidencePipeline(
                store=SqlAlchemyEvidencePipelineStore(
                    self.database.session_factory,
                    author_hmac_secret=(
                        value.encode()
                        if (value := _secret(self.settings.author_hmac_key))
                        else None
                    ),
                    clock=_utc_now,
                ),
                reasoner=self.reasoner,
                collectors={
                    Source.HACKER_NEWS: HackerNewsCollector(client),
                    Source.GITHUB: GitHubCollector(client, github_token),
                    Source.REDDIT: RedditCollector(
                        client,
                        client_id=reddit_client_id,
                        client_secret=reddit_client_secret,
                    ),
                },
                competitor_search=BraveSearchProvider(
                    client,
                    _secret(self.settings.brave_api_key),
                ),
                safe_fetch=StaticFetchAdapter(StaticFetcher(client)),
                clock=_utc_now,
            )
            return await pipeline(task)


def _secret(value: object) -> str | None:
    getter = getattr(value, "get_secret_value", None)
    if getter is None:
        return None
    secret = getter()
    return secret if isinstance(secret, str) and secret else None


def _utc_now() -> datetime:
    return datetime.now(UTC)
