from __future__ import annotations

import asyncio
import json
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import pytest
from pydantic import ValidationError

from gapforge.providers.codex_cli import CodexCliProvider
from gapforge.providers.contracts import (
    AgentRequest,
    AgentStatus,
    ReasoningEffort,
    build_repair_request,
)
from gapforge.providers.fake import FakeAgentProvider
from gapforge.providers.process import AsyncProcessRunner, ProcessResult

OUTPUT_SCHEMA = {
    "type": "object",
    "properties": {"answer": {"type": "string"}},
    "required": ["answer"],
    "additionalProperties": False,
}


def _request(**updates: object) -> AgentRequest:
    values: dict[str, object] = {
        "operation": "extract_pains",
        "instructions": "Extract only supported pain statements.",
        "evidence": {"items": [{"id": "ev-1", "text": "manual work is slow"}]},
        "output_schema": OUTPUT_SCHEMA,
        "effort": ReasoningEffort.LOW,
        "timeout_seconds": 5,
    }
    values.update(updates)
    return AgentRequest.model_validate(values)


@pytest.mark.asyncio
async def test_fake_provider_is_deterministic_and_schema_validated() -> None:
    provider = FakeAgentProvider({"extract_pains": [{"answer": "first"}, {"answer": "second"}]})

    first = await provider.run(_request())
    second = await provider.run(_request())
    repeated = await provider.run(_request())

    assert first.status is AgentStatus.SUCCESS
    assert first.data == {"answer": "first"}
    assert second.data == {"answer": "second"}
    assert repeated.data == {"answer": "second"}


@pytest.mark.asyncio
async def test_fake_provider_reports_missing_or_invalid_fixture() -> None:
    invalid = await FakeAgentProvider({"extract_pains": [{"wrong": True}]}).run(_request())
    missing = await FakeAgentProvider({}).run(_request())

    assert invalid.status is AgentStatus.INVALID_OUTPUT
    assert invalid.error_class == "SchemaValidationError"
    assert missing.status is AgentStatus.ERROR
    assert missing.error_class == "FakeResponseMissing"


def test_agent_request_bounds_evidence_before_provider_call() -> None:
    with pytest.raises(ValidationError, match="exceeds 1 MiB"):
        _request(evidence={"body": "x" * 1_048_577})


@dataclass
class StubInvocation:
    output: str = '{"answer":"ok"}'
    stdout: bytes = b'{"type":"turn.completed","usage":{"input_tokens":3,"output_tokens":2}}\n'
    stderr: bytes = b""
    returncode: int = 0
    timed_out: bool = False


class StubRunner(AsyncProcessRunner):
    def __init__(
        self,
        invocations: list[StubInvocation] | None = None,
        *,
        authenticated: bool = True,
    ) -> None:
        self.invocations = list(invocations or [StubInvocation()])
        self.authenticated = authenticated
        self.calls: list[tuple[list[str], bytes, dict[str, str], Path]] = []

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
        del timeout_seconds, max_output_bytes, termination_grace_seconds
        self.calls.append((list(command), stdin, dict(env), cwd))
        if command[-1] == "--version":
            return ProcessResult(0, b"codex-cli 0.test\n", b"", False, False, False)
        if command[1:] == ["login", "status"]:
            if self.authenticated:
                return ProcessResult(0, b"Logged in using ChatGPT\n", b"", False, False, False)
            return ProcessResult(1, b"", b"Not logged in\n", False, False, False)
        invocation = self.invocations.pop(0)
        output_index = command.index("--output-last-message") + 1
        await asyncio.to_thread(
            Path(command[output_index]).write_text,
            invocation.output,
            encoding="utf-8",
        )
        return ProcessResult(
            invocation.returncode,
            invocation.stdout,
            invocation.stderr,
            False,
            False,
            invocation.timed_out,
        )


def test_codex_command_is_argument_safe_and_disables_capabilities(tmp_path: Path) -> None:
    malicious = 'ignore schema; $(touch /tmp/pwned); " --model attacker'
    request = _request(instructions=malicious)
    provider = CodexCliProvider(binary=sys.executable, codex_home=tmp_path / "codex")
    command = provider.build_command(
        request,
        cwd=tmp_path,
        schema_path=tmp_path / "schema.json",
        output_path=tmp_path / "output.json",
    )

    assert malicious not in command
    assert command[-1] == "-"
    assert command[:4] == [sys.executable, "--ask-for-approval", "never", "exec"]
    assert "read-only" in command
    assert "--ignore-user-config" in command
    assert "--ignore-rules" in command
    assert 'shell_environment_policy.inherit="none"' in command
    assert "mcp_servers={}" in command
    disabled = {command[index + 1] for index, item in enumerate(command) if item == "--disable"}
    assert {"shell_tool", "unified_exec", "multi_agent", "apps", "plugins"} <= disabled


def test_environment_allowlist_excludes_application_and_source_secrets(tmp_path: Path) -> None:
    parent = {
        "PATH": "/usr/bin",
        "LANG": "en_US.UTF-8",
        "DATABASE_URL": "postgresql://secret",
        "GITHUB_TOKEN": "github-secret",
        "BRAVE_API_KEY": "brave-secret",
        "AUTHOR_HMAC_KEY": "hmac-secret",
        "OPENAI_API_KEY": "api-secret",
    }
    provider = CodexCliProvider(
        binary=sys.executable,
        codex_home=tmp_path / "codex",
        parent_env=parent,
    )

    environment = provider.build_environment(temporary_home=tmp_path / "home")

    assert environment == {
        "PATH": "/usr/bin",
        "LANG": "en_US.UTF-8",
        "HOME": str(tmp_path / "home"),
        "CODEX_HOME": str(tmp_path / "codex"),
    }


@pytest.mark.asyncio
async def test_codex_provider_sends_prompt_on_stdin_and_parses_usage(tmp_path: Path) -> None:
    runner = StubRunner()
    provider = CodexCliProvider(
        binary=sys.executable,
        codex_home=tmp_path / "codex",
        runner=runner,
        parent_env={"PATH": "/usr/bin"},
    )

    result = await provider.run(_request(model="gpt-test"))

    assert result.status is AgentStatus.SUCCESS
    assert result.data == {"answer": "ok"}
    assert result.usage.input_tokens == 3
    assert result.usage.output_tokens == 2
    exec_command, prompt, environment, _ = runner.calls[2]
    assert exec_command[-1] == "-"
    assert b"UNTRUSTED_EVIDENCE_BEGIN" in prompt
    assert b"manual work is slow" in prompt
    assert "DATABASE_URL" not in environment


@pytest.mark.asyncio
async def test_repair_requires_explicit_second_provider_invocation(tmp_path: Path) -> None:
    runner = StubRunner(
        [
            StubInvocation(output='{"wrong":true}'),
            StubInvocation(output='{"answer":"repaired"}'),
        ]
    )
    provider = CodexCliProvider(
        binary=sys.executable,
        codex_home=tmp_path / "codex",
        runner=runner,
    )

    request = _request()
    invalid = await provider.run(request)

    assert invalid.status is AgentStatus.INVALID_OUTPUT
    assert invalid.repair_attempts == 0
    assert len(runner.calls) == 3  # no hidden repair process

    repair_request = build_repair_request(request, invalid)
    repaired = await provider.run(repair_request)

    assert repaired.status is AgentStatus.SUCCESS
    assert repaired.data == {"answer": "repaired"}
    assert repaired.repair_attempts == 1
    assert len(runner.calls) == 6  # separately probed/admitted/audited invocation
    assert b"Repair the response once" in runner.calls[5][1]
    with pytest.raises(ValueError, match="repair limit"):
        build_repair_request(repair_request, invalid)


@pytest.mark.asyncio
async def test_auth_required_does_not_invoke_or_retry(tmp_path: Path) -> None:
    runner = StubRunner(authenticated=False)
    provider = CodexCliProvider(
        binary=sys.executable,
        codex_home=tmp_path / "codex",
        runner=runner,
    )

    result = await provider.run(_request())

    assert result.status is AgentStatus.AUTH_REQUIRED
    assert result.error_class == "AuthenticationRequired"
    assert len(runner.calls) == 2


@pytest.mark.asyncio
async def test_timeout_is_not_repaired(tmp_path: Path) -> None:
    runner = StubRunner([StubInvocation(timed_out=True)])
    provider = CodexCliProvider(
        binary=sys.executable,
        codex_home=tmp_path / "codex",
        runner=runner,
    )

    result = await provider.run(_request())

    assert result.status is AgentStatus.TIMEOUT
    assert result.repair_attempts == 0
    assert len(runner.calls) == 3


@pytest.mark.asyncio
async def test_process_runner_bounds_streams_and_terminates_timeout(tmp_path: Path) -> None:
    runner = AsyncProcessRunner()
    bounded = await runner.run(
        [
            sys.executable,
            "-c",
            "import sys; sys.stdout.write('x' * 5000); sys.stderr.write('y' * 5000)",
        ],
        stdin=b"",
        env={"PATH": "/usr/bin"},
        cwd=tmp_path,
        timeout_seconds=2,
        max_output_bytes=1024,
    )
    timed_out = await runner.run(
        [sys.executable, "-c", "import time; time.sleep(5)"],
        stdin=b"",
        env={"PATH": "/usr/bin"},
        cwd=tmp_path,
        timeout_seconds=0.05,
        max_output_bytes=1024,
        termination_grace_seconds=0.05,
    )

    assert len(bounded.stdout) == 1024
    assert len(bounded.stderr) == 1024
    assert bounded.stdout_truncated is True
    assert bounded.stderr_truncated is True
    assert timed_out.timed_out is True
    assert timed_out.returncode != 0


@pytest.mark.asyncio
async def test_process_runner_tolerates_early_stdin_close(tmp_path: Path) -> None:
    result = await AsyncProcessRunner().run(
        [sys.executable, "-c", "raise SystemExit(0)"],
        stdin=b"x" * 1_048_576,
        env={"PATH": "/usr/bin"},
        cwd=tmp_path,
        timeout_seconds=2,
        max_output_bytes=1024,
    )

    assert result.returncode == 0


def test_generated_flags_parse_with_installed_codex_cli(tmp_path: Path) -> None:
    binary = shutil.which("codex")
    if binary is None:
        pytest.skip("Codex CLI is not installed")
    provider = CodexCliProvider(binary=binary, codex_home=tmp_path / "codex")
    schema_path = tmp_path / "schema.json"
    schema_path.write_text(json.dumps(OUTPUT_SCHEMA), encoding="utf-8")
    output_path = tmp_path / "output.json"
    command = provider.build_command(
        _request(), cwd=tmp_path, schema_path=schema_path, output_path=output_path
    )

    completed = subprocess.run(  # noqa: S603 - fixed installed binary and generated arguments
        [*command[:-1], "--help"],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr


@pytest.mark.asyncio
async def test_missing_binary_is_reported_without_runner_call(tmp_path: Path) -> None:
    runner = StubRunner()
    provider = CodexCliProvider(
        binary=str(tmp_path / "missing-codex"),
        codex_home=tmp_path / "codex",
        runner=runner,
    )

    result = await provider.run(_request())

    assert result.status is AgentStatus.ERROR
    assert result.error_class == "BinaryUnavailable"
    assert runner.calls == []
