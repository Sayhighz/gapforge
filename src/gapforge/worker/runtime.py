"""Composable durable worker; research task handlers are registered by integration."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from contextlib import suppress
from datetime import timedelta
from typing import Protocol

from gapforge.queue.repository import DurableQueue
from gapforge.queue.retry import classify_exception
from gapforge.storage.database import Database
from gapforge.storage.models import ResearchTask


class ResearchTaskHandler(Protocol):
    async def __call__(self, task: ResearchTask) -> dict[str, object]: ...


class Worker:
    def __init__(
        self,
        database: Database,
        *,
        worker_id: str,
        handlers: Mapping[str, ResearchTaskHandler],
        lease_duration: timedelta = timedelta(minutes=5),
        heartbeat_interval_seconds: float | None = None,
    ) -> None:
        self.database = database
        self.worker_id = worker_id
        self.handlers = dict(handlers)
        self.lease_duration = lease_duration
        default_heartbeat = max(0.05, lease_duration.total_seconds() / 3)
        self.heartbeat_interval_seconds = heartbeat_interval_seconds or default_heartbeat
        if self.heartbeat_interval_seconds >= lease_duration.total_seconds():
            raise ValueError("heartbeat interval must be shorter than the lease")

    async def run_once(self) -> bool:
        if not self.handlers:
            return False
        async with self.database.session() as session:
            task = await DurableQueue(session).claim(
                worker_id=self.worker_id,
                lease_duration=self.lease_duration,
                allowed_task_types=frozenset(self.handlers),
            )
            await session.commit()
        if task is None:
            return False
        handler = self.handlers[task.task_type]
        handler_task = asyncio.create_task(handler(task))
        try:
            while not handler_task.done():
                done, _ = await asyncio.wait(
                    {handler_task}, timeout=self.heartbeat_interval_seconds
                )
                if done:
                    break
                async with self.database.session() as session:
                    await DurableQueue(session).renew_lease(
                        task.id,
                        worker_id=self.worker_id,
                        lease_duration=self.lease_duration,
                    )
                    await session.commit()
            result = await handler_task
        except Exception as error:
            if not handler_task.done():
                handler_task.cancel()
                with suppress(asyncio.CancelledError):
                    await handler_task
            decision = classify_exception(error, attempt=task.attempt_count, seed=str(task.id))
            async with self.database.session() as session:
                await DurableQueue(session).fail(
                    task.id,
                    worker_id=self.worker_id,
                    decision=decision,
                    sanitized_error=type(error).__name__,
                )
                await session.commit()
            return True
        async with self.database.session() as session:
            await DurableQueue(session).succeed(
                task.id,
                worker_id=self.worker_id,
                result=result,
            )
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
