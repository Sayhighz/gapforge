from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

import pytest

from gapforge.config import AgentProviderName, Settings
from gapforge.domain.contracts import Availability, FetchResult
from gapforge.runtime import research_handler
from gapforge.runtime.research_handler import ResearchRunHandler, StaticFetchAdapter
from gapforge.storage.models import ResearchTask
from gapforge.worker import TaskHandlerResult

NOW = datetime(2026, 8, 9, 12, tzinfo=UTC)


class RecordingClient:
    def __init__(self) -> None:
        self.entered = False
        self.closed = False

    async def __aenter__(self) -> RecordingClient:
        self.entered = True
        return self

    async def __aexit__(self, *_args: object) -> None:
        self.closed = True


def _task() -> ResearchTask:
    run_id = uuid4()
    return ResearchTask(
        id=uuid4(),
        run_id=run_id,
        task_type="research.run",
        status="LEASED",
        priority=1,
        idempotency_key="run-root:v1",
        payload={"run_id": str(run_id)},
        checkpoint={},
        attempt_count=1,
        max_attempts=3,
        available_at=NOW,
        lease_owner="worker-1",
        lease_expires_at=NOW,
    )


@pytest.mark.asyncio
async def test_production_handler_owns_http_lifecycle_and_configures_all_ports(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = RecordingClient()
    captured: dict[str, object] = {}

    class FakePipeline:
        def __init__(self, **kwargs: object) -> None:
            captured.update(kwargs)

        async def __call__(self, task: ResearchTask) -> TaskHandlerResult:
            assert task.task_type == "research.run"
            assert client.entered is True
            assert client.closed is False
            return TaskHandlerResult(payload={"completed_stage": "FINAL"})

    def fake_store(
        session_factory: object,
        *,
        author_hmac_secret: bytes | None,
        clock: object,
        artifact_writer: object,
    ) -> object:
        captured["session_factory"] = session_factory
        captured["author_hmac_secret"] = author_hmac_secret
        captured["store_clock"] = clock
        captured["artifact_writer"] = artifact_writer
        return object()

    monkeypatch.setattr(research_handler.httpx, "AsyncClient", lambda **_kwargs: client)
    monkeypatch.setattr(research_handler, "EvidencePipeline", FakePipeline)
    monkeypatch.setattr(research_handler, "SqlAlchemyEvidencePipelineStore", fake_store)
    monkeypatch.setattr(
        research_handler,
        "HackerNewsCollector",
        lambda value: ("hn", value),
    )
    monkeypatch.setattr(
        research_handler,
        "GitHubCollector",
        lambda value, token: ("github", value, token),
    )
    monkeypatch.setattr(
        research_handler,
        "RedditCollector",
        lambda value, **kwargs: ("reddit", value, kwargs),
    )
    monkeypatch.setattr(
        research_handler,
        "BraveSearchProvider",
        lambda value, api_key: ("brave", value, api_key),
    )
    monkeypatch.setattr(research_handler, "StaticFetcher", lambda value: ("fetch", value))
    database = SimpleNamespace(session_factory="sessions")
    reasoner = object()
    settings = Settings(
        database_url="postgresql+asyncpg://gapforge:gapforge@localhost/gapforge",
        agent_provider=AgentProviderName.FAKE,
        github_token="github-secret",
        reddit_client_id="reddit-id",
        reddit_client_secret="reddit-secret",
        brave_api_key="brave-secret",
        author_hmac_key="author-secret",
    )
    handler = ResearchRunHandler(
        database=database,  # type: ignore[arg-type]
        settings=settings,
        reasoner=reasoner,  # type: ignore[arg-type]
    )

    result = await handler(_task())

    assert result.payload == {"completed_stage": "FINAL"}
    assert client.closed is True
    assert captured["reasoner"] is reasoner
    assert captured["author_hmac_secret"] == b"author-secret"
    assert isinstance(captured["artifact_writer"], research_handler.ResearchArtifactWriter)
    collectors = captured["collectors"]
    assert isinstance(collectors, dict)
    assert {source.value for source in collectors} == {
        "HACKER_NEWS",
        "GITHUB",
        "REDDIT",
    }
    assert captured["competitor_search"] == ("brave", client, "brave-secret")


@pytest.mark.asyncio
async def test_static_fetch_adapter_builds_an_exact_approved_registry() -> None:
    class FakeFetcher:
        async def fetch(self, url: str, registry: object) -> FetchResult:
            require_approved = registry.require_approved
            assert require_approved(url) == "https://approved.example/path"
            with pytest.raises(ValueError, match="not supplied"):
                require_approved("https://invented.example/")
            return FetchResult(availability=Availability.CONTENT_UNAVAILABLE)

    adapter = StaticFetchAdapter(FakeFetcher())  # type: ignore[arg-type]

    result = await adapter.fetch(
        "https://approved.example/path",
        approved_urls=("https://approved.example/path",),
    )

    assert result.availability is Availability.CONTENT_UNAVAILABLE
