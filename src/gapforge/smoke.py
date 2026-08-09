"""Bounded, explicit live-integration smoke checks."""

from __future__ import annotations

import asyncio
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Annotated, Literal
from uuid import NAMESPACE_URL, uuid5

import httpx
import typer
from pydantic import BaseModel, ConfigDict, Field

from gapforge.collectors.hacker_news import HackerNewsCollector
from gapforge.collectors.reddit import RedditCollector
from gapforge.domain.contracts import (
    CollectRequest,
    QueryIntent,
    QueryIntentKind,
    Source,
)
from gapforge.providers.codex_cli import CodexCliProvider
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
    "BINARY_UNAVAILABLE",
    "UNEXPECTED_AUTHENTICATED",
]
SmokeOutcome = Literal["EXPECTED_UNAVAILABLE", "AVAILABLE", "FAILURE"]


class SmokeCheck(BaseModel):
    """One bounded and sanitized integration observation."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    name: Literal["hacker_news", "github", "reddit", "brave", "codex"]
    status: SmokeStatus
    outcome: SmokeOutcome
    request_count: int = Field(ge=0, le=1)
    item_count: int = Field(default=0, ge=0, le=5)
    warning_codes: tuple[str, ...] = Field(default_factory=tuple, max_length=4)


class SmokeReport(BaseModel):
    """Stable result for an explicit smoke mode."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    mode: Literal["missing-credentials", "hacker-news"]
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
