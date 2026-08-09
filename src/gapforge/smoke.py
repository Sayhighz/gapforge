"""Bounded, explicit live-integration smoke checks."""

from __future__ import annotations

import asyncio
import os
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Annotated, Literal
from uuid import NAMESPACE_URL, uuid5

import httpx
import typer
from pydantic import BaseModel, ConfigDict, Field

from gapforge.collectors.github import GitHubCollector
from gapforge.collectors.hacker_news import HackerNewsCollector
from gapforge.collectors.reddit import RedditCollector
from gapforge.domain.contracts import (
    CollectRequest,
    QueryIntent,
    QueryIntentKind,
    Source,
)
from gapforge.providers.codex_cli import CodexCliProvider
from gapforge.providers.contracts import AgentRequest, AgentStatus, ReasoningEffort
from gapforge.search.brave import BraveSearchProvider

app = typer.Typer(no_args_is_help=True, help="Run explicit bounded GapForge integration smokes.")


@app.callback()
def smoke() -> None:
    """Run one explicitly selected smoke mode."""


SmokeStatus = Literal[
    "AVAILABLE",
    "CREDENTIAL_MISSING",
    "SOURCE_UNAVAILABLE",
    "RESEARCH_UNAVAILABLE",
    "AUTH_REQUIRED",
    "INVALID_OUTPUT",
    "TIMEOUT",
    "ERROR",
    "BINARY_UNAVAILABLE",
    "NOT_RUN",
    "UNEXPECTED_AUTHENTICATED",
]
SmokeOutcome = Literal["EXPECTED_UNAVAILABLE", "AVAILABLE", "FAILURE"]


class SmokeCheck(BaseModel):
    """One bounded and sanitized integration observation."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    name: Literal["hacker_news", "github", "reddit", "brave", "codex"]
    status: SmokeStatus
    outcome: SmokeOutcome
    request_count: int = Field(ge=0, le=2)
    item_count: int = Field(default=0, ge=0, le=5)
    warning_codes: tuple[str, ...] = Field(default_factory=tuple, max_length=4)


class SmokeReport(BaseModel):
    """Stable result for an explicit smoke mode."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["1.0"] = "1.0"
    mode: Literal[
        "missing-credentials",
        "hacker-news",
        "credentialed-sources",
        "credentialed-codex",
    ]
    ok: bool
    checks: tuple[SmokeCheck, ...] = Field(min_length=1, max_length=5)


async def run_missing_credential_smoke(
    *,
    client: httpx.AsyncClient,
    codex: CodexCliProvider,
) -> SmokeReport:
    """Prove gated dependencies degrade without network-bearing credentials or AI calls."""

    until = datetime.now(UTC)
    intent = QueryIntent(
        id="missing-credential-smoke",
        kind=QueryIntentKind.BROAD,
        concept="bounded workflow pain smoke",
        sources=(Source.REDDIT,),
        rationale="Verify missing credentials fail closed without issuing source requests.",
    )
    request = CollectRequest(
        mission_revision_id=uuid5(NAMESPACE_URL, "gapforge:missing-credential-smoke"),
        intent=intent,
        since=until - timedelta(days=1),
        until=until,
        max_requests=1,
        max_signals=1,
    )
    reddit = await RedditCollector(client, client_id=None, client_secret=None).collect(request)
    brave = await BraveSearchProvider(client, api_key=None).search(
        "bounded workflow pain smoke",
        max_results=1,
        max_requests=1,
    )
    codex_probe = await codex.probe()
    codex_status: SmokeStatus
    codex_outcome: SmokeOutcome
    if not codex_probe.binary_available:
        codex_status = "BINARY_UNAVAILABLE"
        codex_outcome = "FAILURE"
    elif not codex_probe.authenticated:
        codex_status = "AUTH_REQUIRED"
        codex_outcome = "EXPECTED_UNAVAILABLE"
    else:
        # This mode must use an isolated empty CODEX_HOME. Authentication here is unsafe.
        codex_status = "UNEXPECTED_AUTHENTICATED"
        codex_outcome = "FAILURE"

    checks = (
        SmokeCheck(
            name="github",
            status="CREDENTIAL_MISSING",
            outcome="EXPECTED_UNAVAILABLE",
            request_count=0,
            warning_codes=("GITHUB_CREDENTIAL_REQUIRED_FOR_LIVE_SMOKE",),
        ),
        SmokeCheck(
            name="reddit",
            status=reddit.availability.value,
            outcome="EXPECTED_UNAVAILABLE",
            request_count=reddit.request_count,
            warning_codes=tuple(warning.code for warning in reddit.warnings),
        ),
        SmokeCheck(
            name="brave",
            status=brave.availability.value,
            outcome="EXPECTED_UNAVAILABLE",
            request_count=brave.request_count,
            warning_codes=tuple(warning.code for warning in brave.warnings),
        ),
        SmokeCheck(
            name="codex",
            status=codex_status,
            outcome=codex_outcome,
            request_count=0,
        ),
    )
    return SmokeReport(
        mode="missing-credentials",
        ok=(
            checks[0].status == "CREDENTIAL_MISSING"
            and checks[0].request_count == 0
            and checks[1].status == "SOURCE_UNAVAILABLE"
            and checks[1].request_count == 0
            and checks[2].status == "RESEARCH_UNAVAILABLE"
            and checks[2].request_count == 0
            and checks[3].status == "AUTH_REQUIRED"
            and not codex_probe.authenticated
        ),
        checks=checks,
    )


async def run_hacker_news_smoke(
    *,
    client: httpx.AsyncClient,
    query: str,
    now: datetime | None = None,
) -> SmokeReport:
    """Make exactly one bounded credential-free HN request."""

    until = now or datetime.now(UTC)
    intent = QueryIntent(
        id="hacker-news-live-smoke",
        kind=QueryIntentKind.BROAD,
        concept=query,
        sources=(Source.HACKER_NEWS,),
        rationale="Explicit credential-free HN live smoke.",
    )
    result = await HackerNewsCollector(client).collect(
        CollectRequest(
            mission_revision_id=uuid5(NAMESPACE_URL, "gapforge:hacker-news-live-smoke"),
            intent=intent,
            since=until - timedelta(days=30),
            until=until,
            max_requests=1,
            max_signals=5,
        )
    )
    available = result.availability.value == "AVAILABLE"
    check = SmokeCheck(
        name="hacker_news",
        status=result.availability.value,
        outcome="AVAILABLE" if available else "FAILURE",
        request_count=result.request_count,
        item_count=len(result.items),
        warning_codes=tuple(warning.code for warning in result.warnings),
    )
    return SmokeReport(mode="hacker-news", ok=available, checks=(check,))


async def run_credentialed_source_smoke(
    *,
    client: httpx.AsyncClient,
    query: str,
    github_token: str | None,
    reddit_client_id: str | None,
    reddit_client_secret: str | None,
    brave_api_key: str | None,
    now: datetime | None = None,
) -> SmokeReport:
    """Run bounded credential-gated source calls after an all-or-nothing preflight."""

    credentials = {
        "github": github_token,
        "reddit": reddit_client_id if reddit_client_id and reddit_client_secret else None,
        "brave": brave_api_key,
    }
    missing = {name for name, value in credentials.items() if not value or not value.strip()}
    if missing:
        checks = tuple(
            SmokeCheck(
                name=name,
                status="CREDENTIAL_MISSING" if name in missing else "NOT_RUN",
                outcome="FAILURE",
                request_count=0,
                warning_codes=(
                    f"{name.upper()}_CREDENTIAL_MISSING"
                    if name in missing
                    else "SMOKE_PREFLIGHT_FAILED",
                ),
            )
            for name in ("github", "reddit", "brave")
        )
        return SmokeReport(mode="credentialed-sources", ok=False, checks=checks)

    until = now or datetime.now(UTC)

    def request(source: Source, *, max_requests: int) -> CollectRequest:
        return CollectRequest(
            mission_revision_id=uuid5(NAMESPACE_URL, "gapforge:credentialed-source-smoke"),
            intent=QueryIntent(
                id=f"{source.value.lower()}-live-smoke",
                kind=QueryIntentKind.BROAD,
                concept=query,
                sources=(source,),
                rationale=f"Explicit credential-gated {source.value} live smoke.",
            ),
            since=until - timedelta(days=30),
            until=until,
            max_requests=max_requests,
            max_signals=1,
        )

    assert github_token is not None
    assert reddit_client_id is not None
    assert reddit_client_secret is not None
    assert brave_api_key is not None
    github = await GitHubCollector(client, token=github_token).collect(
        request(Source.GITHUB, max_requests=1)
    )
    reddit = await RedditCollector(
        client,
        client_id=reddit_client_id,
        client_secret=reddit_client_secret,
    ).collect(request(Source.REDDIT, max_requests=2))
    brave = await BraveSearchProvider(client, api_key=brave_api_key).search(
        query,
        max_results=1,
        max_requests=1,
    )

    checks = (
        SmokeCheck(
            name="github",
            status=github.availability.value,
            outcome="AVAILABLE" if github.availability.value == "AVAILABLE" else "FAILURE",
            request_count=github.request_count,
            item_count=len(github.items),
            warning_codes=tuple(warning.code for warning in github.warnings),
        ),
        SmokeCheck(
            name="reddit",
            status=reddit.availability.value,
            outcome="AVAILABLE" if reddit.availability.value == "AVAILABLE" else "FAILURE",
            request_count=reddit.request_count,
            item_count=len(reddit.items),
            warning_codes=tuple(warning.code for warning in reddit.warnings),
        ),
        SmokeCheck(
            name="brave",
            status=brave.availability.value,
            outcome="AVAILABLE" if brave.availability.value == "AVAILABLE" else "FAILURE",
            request_count=brave.request_count,
            item_count=len(brave.results),
            warning_codes=tuple(warning.code for warning in brave.warnings),
        ),
    )
    return SmokeReport(
        mode="credentialed-sources",
        ok=all(check.outcome == "AVAILABLE" for check in checks),
        checks=checks,
    )


async def run_codex_live_smoke(
    *,
    provider: CodexCliProvider,
    confirmed: bool,
) -> SmokeReport:
    """Run one separately confirmed, bounded Codex semantic smoke."""

    if not confirmed:
        return SmokeReport(
            mode="credentialed-codex",
            ok=False,
            checks=(
                SmokeCheck(
                    name="codex",
                    status="NOT_RUN",
                    outcome="FAILURE",
                    request_count=0,
                    warning_codes=("EXPLICIT_CONFIRMATION_REQUIRED",),
                ),
            ),
        )
    result = await provider.run(
        AgentRequest(
            operation="integration_smoke",
            instructions='Return exactly one JSON object with the value {"ok": true}.',
            evidence={},
            output_schema={
                "type": "object",
                "properties": {"ok": {"const": True}},
                "required": ["ok"],
                "additionalProperties": False,
            },
            effort=ReasoningEffort.LOW,
            timeout_seconds=30,
            max_output_bytes=4096,
            allow_repair=False,
        )
    )
    succeeded = result.status is AgentStatus.SUCCESS and result.data == {"ok": True}
    failure_status: dict[AgentStatus, SmokeStatus] = {
        AgentStatus.SUCCESS: "INVALID_OUTPUT",
        AgentStatus.AUTH_REQUIRED: "AUTH_REQUIRED",
        AgentStatus.INVALID_OUTPUT: "INVALID_OUTPUT",
        AgentStatus.TIMEOUT: "TIMEOUT",
        AgentStatus.ERROR: "ERROR",
    }
    return SmokeReport(
        mode="credentialed-codex",
        ok=succeeded,
        checks=(
            SmokeCheck(
                name="codex",
                status="AVAILABLE" if succeeded else failure_status[result.status],
                outcome="AVAILABLE" if succeeded else "FAILURE",
                request_count=1,
                warning_codes=() if succeeded else (f"CODEX_{result.status.value}",),
            ),
        ),
    )


@app.command("credentialed-codex")
def credentialed_codex(
    confirm: Annotated[
        str,
        typer.Option("--confirm", help="Type run to authorize exactly one bounded semantic call."),
    ] = "",
    json_output: Annotated[
        bool,
        typer.Option("--json", help="Emit one machine-readable JSON document."),
    ] = False,
) -> None:
    """Make one separately confirmed bounded Codex CLI semantic call."""

    provider = CodexCliProvider(
        binary=os.environ.get("CODEX_BINARY", "codex"),
        codex_home=Path(os.environ.get("CODEX_HOME", "/var/lib/gapforge/codex")),
    )
    report = asyncio.run(run_codex_live_smoke(provider=provider, confirmed=confirm == "run"))
    if json_output:
        typer.echo(report.model_dump_json())
    else:
        check = report.checks[0]
        typer.echo(f"{check.name}: {check.status}")
    if not report.ok:
        raise typer.Exit(code=1)


@app.command("credentialed-sources")
def credentialed_sources(
    query: Annotated[
        str,
        typer.Option("--query", help="Bounded source search concept (1-500 characters)."),
    ] = "manual workflow pain",
    json_output: Annotated[
        bool,
        typer.Option("--json", help="Emit one machine-readable JSON document."),
    ] = False,
) -> None:
    """Make bounded GitHub, Reddit, and Brave calls as an explicit manual smoke."""

    async def operation() -> SmokeReport:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(10.0, connect=5.0),
            follow_redirects=False,
        ) as client:
            return await run_credentialed_source_smoke(
                client=client,
                query=query,
                github_token=os.environ.get("GITHUB_TOKEN"),
                reddit_client_id=os.environ.get("REDDIT_CLIENT_ID"),
                reddit_client_secret=os.environ.get("REDDIT_CLIENT_SECRET"),
                brave_api_key=os.environ.get("BRAVE_API_KEY"),
            )

    report = asyncio.run(operation())
    if json_output:
        typer.echo(report.model_dump_json())
    else:
        for check in report.checks:
            typer.echo(f"{check.name}: {check.status} ({check.item_count} items)")
    if not report.ok:
        raise typer.Exit(code=1)


@app.command("hacker-news")
def hacker_news(
    query: Annotated[
        str,
        typer.Option("--query", help="Bounded HN search concept (1-500 characters)."),
    ] = "manual workflow pain",
    json_output: Annotated[
        bool,
        typer.Option("--json", help="Emit one machine-readable JSON document."),
    ] = False,
) -> None:
    """Make one bounded live HN request as an explicit manual smoke."""

    async def operation() -> SmokeReport:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(10.0, connect=5.0),
            follow_redirects=False,
        ) as client:
            return await run_hacker_news_smoke(client=client, query=query)

    report = asyncio.run(operation())
    if json_output:
        typer.echo(report.model_dump_json())
    else:
        check = report.checks[0]
        typer.echo(f"{check.name}: {check.status} ({check.item_count} items)")
    if not report.ok:
        raise typer.Exit(code=1)


@app.command("missing-credentials")
def missing_credentials(
    codex_binary: Annotated[
        str,
        typer.Option(
            "--codex-binary",
            help="Codex executable to probe without making a semantic call.",
        ),
    ] = "codex",
    json_output: Annotated[
        bool,
        typer.Option("--json", help="Emit one machine-readable JSON document."),
    ] = False,
) -> None:
    """Verify missing Reddit, Brave, and isolated Codex credentials fail closed."""

    async def operation() -> SmokeReport:
        with tempfile.TemporaryDirectory(prefix="gapforge-smoke-codex-") as temporary:
            provider = CodexCliProvider(
                binary=codex_binary,
                codex_home=Path(temporary) / "codex",
            )
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(10.0, connect=5.0),
                follow_redirects=False,
            ) as client:
                return await run_missing_credential_smoke(client=client, codex=provider)

    report = asyncio.run(operation())
    if json_output:
        typer.echo(report.model_dump_json())
    else:
        for check in report.checks:
            typer.echo(f"{check.name}: {check.status}")
    if not report.ok:
        raise typer.Exit(code=1)
