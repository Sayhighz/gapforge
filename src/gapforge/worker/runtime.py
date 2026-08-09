"""Composable durable worker; research task handlers are registered by integration."""

from __future__ import annotations

import asyncio
import json
import math
from collections.abc import Callable, Mapping
from contextlib import suppress
from datetime import timedelta
from types import MappingProxyType
from typing import Protocol

from pydantic import BaseModel, ConfigDict, Field, StrictBool, field_validator

from gapforge.queue.control import RunController
from gapforge.queue.repository import DurableQueue
from gapforge.queue.retry import ErrorKind, classify_error, classify_exception
from gapforge.storage.database import Database
from gapforge.storage.models import ResearchTask

_MAX_RESULT_BYTES = 1_000_000
_MAX_RESULT_DEPTH = 32
_MAX_RESULT_KEYS = 10_000


def _validate_json_value(value: object, *, depth: int, key_count: list[int]) -> None:
    if depth > _MAX_RESULT_DEPTH:
        raise ValueError("task result exceeds maximum nesting depth")
    if value is None or isinstance(value, (bool, str)):
        return
    if isinstance(value, int):
        if not -(2**63) <= value < 2**63:
            raise ValueError("task result integer is outside the signed 64-bit range")
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("task result numbers must be finite")
        return
    if isinstance(value, list):
        for item in value:
            _validate_json_value(item, depth=depth + 1, key_count=key_count)
        return
    if isinstance(value, dict):
        if any(not isinstance(key, str) for key in value):
            raise ValueError("task result object keys must be strings")
        key_count[0] += len(value)
        if key_count[0] > _MAX_RESULT_KEYS:
            raise ValueError("task result exceeds maximum object key count")
        for item in value.values():
            _validate_json_value(item, depth=depth + 1, key_count=key_count)
        return
    raise ValueError(f"task result contains non-JSON value {type(value).__name__}")


class ResearchTaskHandler(Protocol):
    async def __call__(self, task: ResearchTask) -> TaskHandlerResult | dict[str, object]: ...


class TaskHandlerResult(BaseModel):
    """Typed task success with an explicit useful-artifact signal."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    payload: dict[str, object] = Field(default_factory=dict)
    useful_artifact: StrictBool = False

    @field_validator("payload", mode="before")
    @classmethod
    def validate_payload(cls, value: object) -> object:
        if not isinstance(value, dict):
            raise ValueError("task result payload must be an object")
        _validate_json_value(value, depth=0, key_count=[0])
        encoded = json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode()
        if len(encoded) > _MAX_RESULT_BYTES:
            raise ValueError("task result exceeds maximum serialized size")
        return value


class TaskHandlerError(Exception):
    """Typed, sanitized failure raised across the handler/runtime boundary."""

    def __init__(self, kind: ErrorKind, *, error_class: str) -> None:
        if not error_class or len(error_class) > 128:
            raise ValueError("error_class must contain 1-128 characters")
        super().__init__(error_class)
        self.kind = kind
        self.error_class = error_class


class TaskHandlerRegistry:
    """Immutable registry seam populated by the research orchestrator."""

    def __init__(self, handlers: Mapping[str, ResearchTaskHandler] | None = None) -> None:
        copied = dict(handlers or {})
        if any(not task_type.strip() for task_type in copied):
            raise ValueError("task handler types must be non-empty")
        if any(not callable(handler) for handler in copied.values()):
            raise ValueError("task handlers must be callable")
        self._handlers = MappingProxyType(copied)

    @property
    def task_types(self) -> frozenset[str]:
        return frozenset(self._handlers)

    def resolve(self, task_type: str) -> ResearchTaskHandler:
        try:
            return self._handlers[task_type]
        except KeyError as error:
            raise LookupError(f"no handler registered for task type {task_type}") from error


class Worker:
    def __init__(
        self,
        database: Database,
        *,
        worker_id: str,
        handlers: TaskHandlerRegistry | Mapping[str, ResearchTaskHandler],
        lease_duration: timedelta = timedelta(minutes=5),
        heartbeat_interval_seconds: float | None = None,
    ) -> None:
        self.database = database
        self.worker_id = worker_id
        self.registry = (
            handlers if isinstance(handlers, TaskHandlerRegistry) else TaskHandlerRegistry(handlers)
        )
        self.lease_duration = lease_duration
        default_heartbeat = max(0.05, lease_duration.total_seconds() / 3)
        self.heartbeat_interval_seconds = heartbeat_interval_seconds or default_heartbeat
        if self.heartbeat_interval_seconds >= lease_duration.total_seconds():
            raise ValueError("heartbeat interval must be shorter than the lease")

    async def run_once(self) -> bool:
        if not self.registry.task_types:
            return False
        async with self.database.session() as session:
            await RunController(session).admit_next(
                allowed_task_types=self.registry.task_types,
            )
            queue = DurableQueue(session)
            task = await queue.claim(
                worker_id=self.worker_id,
                lease_duration=self.lease_duration,
                allowed_task_types=self.registry.task_types,
            )
            await session.commit()
        if task is None:
            return False
        handler = self.registry.resolve(task.task_type)
        handler_task = asyncio.create_task(handler(task))
        try:
            while not handler_task.done():
                done, _ = await asyncio.wait(
                    {handler_task}, timeout=self.heartbeat_interval_seconds
                )
                if done:
                    break
                try:
                    async with self.database.session() as session:
                        renewed = await DurableQueue(session).renew_lease(
                            task.id,
                            worker_id=self.worker_id,
                            lease_duration=self.lease_duration,
                        )
                        await session.commit()
                except PermissionError:
                    handler_task.cancel()
                    with suppress(asyncio.CancelledError):
                        await handler_task
                    return True
                if renewed is None:
                    handler_task.cancel()
                    with suppress(asyncio.CancelledError):
                        await handler_task
                    return True
            raw_result = await handler_task
            result = (
                raw_result
                if isinstance(raw_result, TaskHandlerResult)
                else TaskHandlerResult.model_validate({"payload": raw_result})
            )
        except Exception as error:
            if not handler_task.done():
                handler_task.cancel()
                with suppress(asyncio.CancelledError):
                    await handler_task
            if isinstance(error, TaskHandlerError):
                decision = classify_error(
                    error.kind,
                    attempt=task.attempt_count,
                    seed=str(task.id),
                )
                sanitized_error = error.error_class
            else:
                decision = classify_exception(error, attempt=task.attempt_count, seed=str(task.id))
                sanitized_error = type(error).__name__
            async with self.database.session() as session:
                await DurableQueue(session).fail(
                    task.id,
                    worker_id=self.worker_id,
                    decision=decision,
                    sanitized_error=sanitized_error,
                )
                await RunController(session).finalize_if_idle(task.run_id)
                await session.commit()
            return True
        async with self.database.session() as session:
            await DurableQueue(session).succeed(
                task.id,
                worker_id=self.worker_id,
                result=result.payload,
                useful_artifact=result.useful_artifact,
            )
            await RunController(session).finalize_if_idle(task.run_id)
            await session.commit()
        return True

    async def run_forever(
        self,
        *,
        poll_interval_seconds: float = 2.0,
        stop: Callable[[], bool] = lambda: False,
    ) -> None:
        if poll_interval_seconds <= 0:
            raise ValueError("poll interval must be positive")
        while not stop():
            worked = await self.run_once()
            if not worked:
                await asyncio.sleep(poll_interval_seconds)
