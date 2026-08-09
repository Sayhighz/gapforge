from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
from typer.testing import CliRunner

from gapforge.providers.codex_cli import CodexCliProvider
from gapforge.providers.process import AsyncProcessRunner, ProcessResult
from gapforge.smoke import app, run_hacker_news_smoke, run_missing_credential_smoke

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
