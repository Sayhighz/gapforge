"""Bounded asynchronous subprocess execution with process-group termination."""

from __future__ import annotations

import asyncio
import os
import signal
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class ProcessResult:
    returncode: int
    stdout: bytes
    stderr: bytes
    stdout_truncated: bool
    stderr_truncated: bool
    timed_out: bool


class AsyncProcessRunner:
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
        process = await asyncio.create_subprocess_exec(
            *command,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=cwd,
            env=env,
            start_new_session=True,
        )
        assert process.stdin is not None
        assert process.stdout is not None
        assert process.stderr is not None
        stdout_task = asyncio.create_task(self._read_bounded(process.stdout, max_output_bytes))
        stderr_task = asyncio.create_task(self._read_bounded(process.stderr, max_output_bytes))
        with suppress(BrokenPipeError, ConnectionResetError):
            process.stdin.write(stdin)
            await process.stdin.drain()
        with suppress(BrokenPipeError, ConnectionResetError):
            process.stdin.close()
        timed_out = False
        try:
            await asyncio.wait_for(process.wait(), timeout_seconds)
        except TimeoutError:
            timed_out = True
            await self._terminate_group(process, termination_grace_seconds)
        stdout, stdout_truncated = await stdout_task
        stderr, stderr_truncated = await stderr_task
        return ProcessResult(
            returncode=process.returncode or 0,
            stdout=stdout,
            stderr=stderr,
            stdout_truncated=stdout_truncated,
            stderr_truncated=stderr_truncated,
            timed_out=timed_out,
        )

    @staticmethod
    async def _read_bounded(
        stream: asyncio.StreamReader, max_output_bytes: int
    ) -> tuple[bytes, bool]:
        retained = bytearray()
        total = 0
        while chunk := await stream.read(65_536):
            total += len(chunk)
            remaining = max_output_bytes - len(retained)
            if remaining > 0:
                retained.extend(chunk[:remaining])
        return bytes(retained), total > max_output_bytes

    @staticmethod
    async def _terminate_group(process: asyncio.subprocess.Process, grace_seconds: float) -> None:
        if process.returncode is not None:
            return
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            return
        try:
            await asyncio.wait_for(process.wait(), grace_seconds)
        except TimeoutError:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            await process.wait()
