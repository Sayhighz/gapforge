from __future__ import annotations

import asyncio
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
from sqlalchemy import func, select
from typer.testing import CliRunner

from gapforge.integration.semantic import AuditedSemanticReasoner
from gapforge.providers import contracts as provider_contracts
from gapforge.providers.codex_cli import CodexCliProvider
from gapforge.providers.process import AsyncProcessRunner, ProcessResult
from gapforge.queue.control import RunController
from gapforge.runtime import RunScheduler, RunScheduleRequest
from gapforge.smoke import (
    app,
    run_codex_live_smoke,
    run_credentialed_source_smoke,
    run_hacker_news_smoke,
    run_missing_credential_smoke,
)
from gapforge.storage.database import Database
from gapforge.storage.models import AgentCall, ProviderCallLease, ResearchRun, ResearchTask
from gapforge.storage.uow import SqlAlchemyUnitOfWork

cli = CliRunner()
NOW = datetime(2026, 8, 9, 12, tzinfo=UTC)


class UnauthenticatedCodexRunner(AsyncProcessRunner):
    def __init__(self) -> None:
        self.commands: list[list[str]] = []

    async def run(
        self,
        command: list[str],
        *,
        stdin: bytes,
        env: dict[str, str],
        cwd: Path,
        timeout_seconds: float,
        max_output_bytes: int,
        termination_grace_seconds: float = 2.0,
    ) -> ProcessResult:
        del stdin, env, cwd, timeout_seconds, max_output_bytes, termination_grace_seconds
        self.commands.append(command)
        if command[-1] == "--version":
            return ProcessResult(0, b"codex-cli 0.test\n", b"", False, False, False)
        return ProcessResult(1, b"", b"Not logged in\n", False, False, False)


class SuccessfulCodexSmokeRunner(AsyncProcessRunner):
    def __init__(self) -> None:
        self.calls: list[tuple[list[str], dict[str, str], float, int]] = []

    async def run(
        self,
        command: list[str],
        *,
        stdin: bytes,
        env: dict[str, str],
        cwd: Path,
        timeout_seconds: float,
        max_output_bytes: int,
        termination_grace_seconds: float = 2.0,
    ) -> ProcessResult:
        del stdin, cwd, termination_grace_seconds
        self.calls.append((command, env, timeout_seconds, max_output_bytes))
        if command[-1] == "--version":
            return ProcessResult(0, b"codex-cli 0.test\n", b"", False, False, False)
        if command[-1] == "status":
            return ProcessResult(0, b"Logged in using ChatGPT\n", b"", False, False, False)
        output_index = command.index("--output-last-message") + 1
        await asyncio.to_thread(
            Path(command[output_index]).write_text,
            '{"ok":true}',
            encoding="utf-8",
        )
        return ProcessResult(
            0,
            b'{"type":"turn.completed","usage":{"input_tokens":1,"output_tokens":1}}\n',
            b"",
            False,
            False,
            False,
        )


class ScriptedSmokeProvider:
    def __init__(
        self,
        response: provider_contracts.AgentResult | Exception,
    ) -> None:
        self.response = response
        self.requests: list[provider_contracts.AgentRequest] = []

    async def run(
        self,
        request: provider_contracts.AgentRequest,
    ) -> provider_contracts.AgentResult:
        self.requests.append(request)
        if isinstance(self.response, Exception):
            raise self.response
        return self.response


def _smoke_provider_result(
    status: provider_contracts.AgentStatus,
) -> provider_contracts.AgentResult:
    return provider_contracts.AgentResult(
        status=status,
        provider="codex_cli",
        requested_model="",
        effort=provider_contracts.ReasoningEffort.LOW,
        duration_ms=1,
        error_class="ExpectedSmokeFailure",
        error_message="sanitized test failure",
    )


@pytest.mark.asyncio
async def test_missing_credentials_fail_gracefully_without_network_or_ai_call(
    tmp_path: Path,
) -> None:
    def reject_network(request: httpx.Request) -> httpx.Response:
        raise AssertionError(f"unexpected network request: {request.url}")

    runner = UnauthenticatedCodexRunner()
    provider = CodexCliProvider(
        binary=sys.executable,
        codex_home=tmp_path / "empty-codex-home",
        runner=runner,
        parent_env={"PATH": "/usr/bin"},
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(reject_network)) as client:
        report = await run_missing_credential_smoke(client=client, codex=provider)

    assert report.schema_version == "1.0"
    assert report.ok is True
    assert [
        (check.name, check.status, check.outcome, check.request_count) for check in report.checks
    ] == [
        ("github", "CREDENTIAL_MISSING", "EXPECTED_UNAVAILABLE", 0),
        ("reddit", "SOURCE_UNAVAILABLE", "EXPECTED_UNAVAILABLE", 0),
        ("brave", "RESEARCH_UNAVAILABLE", "EXPECTED_UNAVAILABLE", 0),
        ("codex", "AUTH_REQUIRED", "EXPECTED_UNAVAILABLE", 0),
    ]
    assert [command[-1] for command in runner.commands] == ["--version", "status"]
    assert all("exec" not in command for command in runner.commands)
    assert "Not logged in" not in report.model_dump_json()


def test_missing_credential_smoke_cli_emits_one_json_document() -> None:
    result = cli.invoke(
        app,
        ["missing-credentials", "--codex-binary", sys.executable, "--json"],
    )

    assert result.exit_code == 0, result.stderr
    assert result.stdout.count("\n") == 1
    payload = json.loads(result.stdout)
    assert payload["mode"] == "missing-credentials"
    assert payload["ok"] is True
    assert [check["name"] for check in payload["checks"]] == [
        "github",
        "reddit",
        "brave",
        "codex",
    ]
    assert {check["outcome"] for check in payload["checks"]} == {"EXPECTED_UNAVAILABLE"}


@pytest.mark.asyncio
async def test_hacker_news_live_smoke_is_one_request_and_five_items_max() -> None:
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "hits": [
                    {
                        "objectID": "hn-smoke-1",
                        "title": "Manual reconciliation takes hours",
                        "story_text": "We repeatedly copy ledger rows by hand.",
                        "created_at_i": int(NOW.timestamp()) - 60,
                        "author": "known-user",
                    }
                ],
                "page": 0,
                "nbPages": 1,
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        report = await run_hacker_news_smoke(
            client=client,
            query="manual reconciliation",
            now=NOW,
        )

    assert report.schema_version == "1.0"
    assert report.ok is True
    assert len(requests) == 1
    assert requests[0].url.host == "hn.algolia.com"
    assert requests[0].url.params["hitsPerPage"] == "5"
    assert report.checks[0].model_dump() == {
        "name": "hacker_news",
        "status": "AVAILABLE",
        "outcome": "AVAILABLE",
        "request_count": 1,
        "item_count": 1,
        "warning_codes": (),
    }


@pytest.mark.asyncio
async def test_hacker_news_live_smoke_distinguishes_malformed_failure() -> None:
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json={"unexpected": str(request.url)})
        )
    ) as client:
        report = await run_hacker_news_smoke(client=client, query="workflow pain", now=NOW)

    assert report.ok is False
    assert report.checks[0].model_dump() == {
        "name": "hacker_news",
        "status": "SOURCE_UNAVAILABLE",
        "outcome": "FAILURE",
        "request_count": 1,
        "item_count": 0,
        "warning_codes": ("HN_UNAVAILABLE",),
    }


def test_hacker_news_smoke_is_an_explicit_manual_command() -> None:
    result = cli.invoke(app, ["hacker-news", "--help"])

    assert result.exit_code == 0
    assert "one bounded live HN request" in result.stdout
    assert "manual" in result.stdout.lower()


@pytest.mark.asyncio
async def test_credentialed_source_smoke_refuses_any_missing_secret_before_network() -> None:
    requests: list[httpx.Request] = []

    def reject_network(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        raise AssertionError("credential preflight must run before network")

    async with httpx.AsyncClient(transport=httpx.MockTransport(reject_network)) as client:
        report = await run_credentialed_source_smoke(
            client=client,
            query="workflow pain",
            github_token=None,
            reddit_client_id="reddit-id",
            reddit_client_secret="reddit-secret",
            brave_api_key="brave-key",
            now=NOW,
        )

    assert report.schema_version == "1.0"
    assert report.ok is False
    assert requests == []
    assert [check.model_dump() for check in report.checks] == [
        {
            "name": "github",
            "status": "CREDENTIAL_MISSING",
            "outcome": "FAILURE",
            "request_count": 0,
            "item_count": 0,
            "warning_codes": ("GITHUB_CREDENTIAL_MISSING",),
        },
        {
            "name": "reddit",
            "status": "NOT_RUN",
            "outcome": "FAILURE",
            "request_count": 0,
            "item_count": 0,
            "warning_codes": ("SMOKE_PREFLIGHT_FAILED",),
        },
        {
            "name": "brave",
            "status": "NOT_RUN",
            "outcome": "FAILURE",
            "request_count": 0,
            "item_count": 0,
            "warning_codes": ("SMOKE_PREFLIGHT_FAILED",),
        },
    ]


@pytest.mark.asyncio
async def test_credentialed_source_smoke_bounds_each_real_source_call() -> None:
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.host == "api.github.com":
            return httpx.Response(200, json={"items": []})
        if request.url.path == "/api/v1/access_token":
            return httpx.Response(200, json={"access_token": "temporary-reddit-token"})
        if request.url.host == "oauth.reddit.com":
            return httpx.Response(200, json={"data": {"children": [], "after": None}})
        if request.url.host == "api.search.brave.com":
            return httpx.Response(200, json={"web": {"results": []}})
        raise AssertionError(f"unexpected smoke endpoint: {request.url}")

    secrets = ("github-secret", "reddit-id", "reddit-secret", "brave-secret")
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        report = await run_credentialed_source_smoke(
            client=client,
            query="workflow pain",
            github_token=secrets[0],
            reddit_client_id=secrets[1],
            reddit_client_secret=secrets[2],
            brave_api_key=secrets[3],
            now=NOW,
        )

    assert report.ok is True
    assert [(check.name, check.request_count, check.item_count) for check in report.checks] == [
        ("github", 1, 0),
        ("reddit", 2, 0),
        ("brave", 1, 0),
    ]
    assert {check.status for check in report.checks} == {"AVAILABLE"}
    assert {check.outcome for check in report.checks} == {"AVAILABLE"}
    assert len(requests) == 4
    rendered = report.model_dump_json()
    assert all(secret not in rendered for secret in secrets)
    assert "temporary-reddit-token" not in rendered


def test_credentialed_source_cli_fails_closed_before_network_without_environment() -> None:
    result = cli.invoke(
        app,
        ["credentialed-sources", "--json"],
        env={
            "GITHUB_TOKEN": "",
            "REDDIT_CLIENT_ID": "",
            "REDDIT_CLIENT_SECRET": "",
            "BRAVE_API_KEY": "",
        },
    )

    assert result.exit_code == 1
    assert result.stdout.count("\n") == 1
    payload = json.loads(result.stdout)
    assert payload["mode"] == "credentialed-sources"
    assert payload["ok"] is False
    assert {check["status"] for check in payload["checks"]} == {"CREDENTIAL_MISSING"}


@pytest.mark.asyncio
async def test_codex_live_smoke_requires_explicit_confirmation_before_any_subprocess() -> None:
    report = await run_codex_live_smoke(confirmed=False)

    assert report.schema_version == "1.0"
    assert report.ok is False
    assert report.checks[0].model_dump() == {
        "name": "codex",
        "status": "NOT_RUN",
        "outcome": "FAILURE",
        "request_count": 0,
        "item_count": 0,
        "warning_codes": ("EXPLICIT_CONFIRMATION_REQUIRED",),
    }


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_codex_live_smoke_makes_one_bounded_secret_free_semantic_call(
    tmp_path: Path,
    migrated_postgres_url: str,
) -> None:
    runner = SuccessfulCodexSmokeRunner()
    provider = CodexCliProvider(
        binary=sys.executable,
        codex_home=tmp_path / "codex",
        runner=runner,
        parent_env={
            "PATH": "/usr/bin",
            "DATABASE_URL": "postgresql://private",
            "GITHUB_TOKEN": "github-secret",
            "REDDIT_CLIENT_SECRET": "reddit-secret",
            "BRAVE_API_KEY": "brave-secret",
        },
    )
    database = Database.from_url(migrated_postgres_url)
    reasoner = AuditedSemanticReasoner(
        database.session_factory,
        provider,
        lease_owner="platform-smoke-test",
        provider_name="codex_cli",
    )

    try:
        report = await run_codex_live_smoke(
            database=database,
            reasoner=reasoner,
            worker_id="platform-smoke-test",
            confirmed=True,
        )

        async with database.session() as session:
            audits = (
                await session.scalars(
                    select(AgentCall).where(
                        AgentCall.output_schema_name == "platform-codex-smoke-v1"
                    )
                )
            ).all()
            assert len(audits) == 1
            audit = audits[0]
            run = await session.get(ResearchRun, audit.run_id)
            task = await session.get(ResearchTask, audit.task_id)
    finally:
        await database.dispose()

    assert report.ok is True
    assert report.checks[0].model_dump() == {
        "name": "codex",
        "status": "AVAILABLE",
        "outcome": "AVAILABLE",
        "request_count": 1,
        "item_count": 0,
        "warning_codes": (),
    }
    semantic_calls = [call for call in runner.calls if "exec" in call[0]]
    assert len(semantic_calls) == 1
    command, environment, timeout_seconds, max_output_bytes = semantic_calls[0]
    assert "read-only" in command
    assert "--ignore-user-config" in command
    assert timeout_seconds == 30
    assert max_output_bytes == 4096
    assert audit.operation == "relevance"
    assert audit.status == "COMPLETED"
    assert audit.repair_attempts == 0
    assert run is not None and run.status == "COMPLETED"
    assert task is not None and task.status == "SUCCEEDED"
    assert (
        not {
            "DATABASE_URL",
            "GITHUB_TOKEN",
            "REDDIT_CLIENT_SECRET",
            "BRAVE_API_KEY",
        }
        & environment.keys()
    )


@pytest.mark.postgres
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("response", "audit_status", "report_status"),
    (
        (
            _smoke_provider_result(provider_contracts.AgentStatus.INVALID_OUTPUT),
            "INVALID_OUTPUT",
            "INVALID_OUTPUT",
        ),
        (
            _smoke_provider_result(provider_contracts.AgentStatus.TIMEOUT),
            "TIMEOUT",
            "TIMEOUT",
        ),
        (
            _smoke_provider_result(provider_contracts.AgentStatus.AUTH_REQUIRED),
            "AUTH_REQUIRED",
            "AUTH_REQUIRED",
        ),
        (
            _smoke_provider_result(provider_contracts.AgentStatus.ERROR),
            "FAILED",
            "ERROR",
        ),
        (RuntimeError("provider internals must be sanitized"), "FAILED", "ERROR"),
    ),
    ids=("invalid", "timeout", "auth", "provider-error", "provider-exception"),
)
async def test_codex_live_smoke_failure_is_one_attempt_and_terminal(
    migrated_postgres_url: str,
    response: provider_contracts.AgentResult | Exception,
    audit_status: str,
    report_status: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    boundary = ScriptedSmokeProvider(response)
    worker_id = f"platform-smoke-{report_status.lower()}"
    reasoner = AuditedSemanticReasoner(
        database.session_factory,
        boundary,
        lease_owner=worker_id,
        provider_name="codex_cli",
    )
    async with database.session() as session:
        before_ids = set(await session.scalars(select(AgentCall.id)))

    try:
        report = await run_codex_live_smoke(
            database=database,
            reasoner=reasoner,
            worker_id=worker_id,
            confirmed=True,
        )

        async with database.session() as session:
            audits = (
                await session.scalars(select(AgentCall).where(AgentCall.id.not_in(before_ids)))
            ).all()
            assert len(audits) == 1
            audit = audits[0]
            run = await session.get(ResearchRun, audit.run_id)
            task = await session.get(ResearchTask, audit.task_id)
            lease_count = await session.scalar(
                select(func.count())
                .select_from(ProviderCallLease)
                .where(ProviderCallLease.run_id == audit.run_id)
            )
    finally:
        await database.dispose()

    assert report.ok is False
    assert report.checks[0].status == report_status
    assert report.checks[0].request_count == 1
    assert len(boundary.requests) == 1
    assert audit.status == audit_status
    assert audit.repair_attempts == 0
    assert run is not None and run.status not in {"QUEUED", "RUNNING"}
    assert task is not None and task.status == "FAILED"
    assert task.max_attempts == 1
    assert task.attempt_count == 1
    assert task.lease_owner is None
    assert lease_count == 0


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_codex_live_smoke_fails_closed_while_unrelated_run_is_active(
    migrated_postgres_url: str,
) -> None:
    database = Database.from_url(migrated_postgres_url)
    boundary = ScriptedSmokeProvider(RuntimeError("must not run"))
    reasoner = AuditedSemanticReasoner(
        database.session_factory,
        boundary,
        lease_owner="platform-smoke-busy",
        provider_name="codex_cli",
    )
    async with SqlAlchemyUnitOfWork(database.session_factory) as uow:
        _, revision = await uow.missions.create_with_revision(
            title="Unrelated active work",
            mission_text="Keep the global run admission gate occupied.",
            original_language="en",
            output_locale="en",
        )
        assert uow.session is not None
        scheduled = await RunScheduler(uow.session).schedule(
            request=RunScheduleRequest.model_validate(
                {
                    "mission_revision_id": revision.id,
                    "mode": "HUNT",
                    "priority": 0,
                    "budget_limits": {
                        "max_agent_calls_per_run": 1,
                        "max_parallel_agent_calls": 1,
                        "max_run_duration_minutes": 1,
                    },
                }
            )
        )
        await RunController(uow.session).start(scheduled.run.id)
        await uow.commit()

    try:
        report = await run_codex_live_smoke(
            database=database,
            reasoner=reasoner,
            worker_id="platform-smoke-busy",
            confirmed=True,
        )

        assert report.ok is False
        assert report.checks[0].status == "ERROR"
        assert report.checks[0].warning_codes == ("PLATFORM_SMOKE_BUSY",)
        assert report.checks[0].request_count == 0
        assert boundary.requests == []
    finally:
        async with database.session() as session, session.begin():
            run = await session.get(ResearchRun, scheduled.run.id)
            task = await session.get(ResearchTask, scheduled.task.id)
            assert run is not None and task is not None
            run.status = "CANCELLED"
            run.completed_at = datetime.now(UTC)
            task.status = "CANCELLED"
            task.completed_at = datetime.now(UTC)
        await database.dispose()


def test_codex_live_cli_requires_the_separate_run_confirmation() -> None:
    result = cli.invoke(app, ["credentialed-codex", "--json"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["mode"] == "credentialed-codex"
    assert payload["checks"][0]["status"] == "NOT_RUN"
    assert payload["checks"][0]["request_count"] == 0
