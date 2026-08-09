"""Unit-of-work seam for transactionally consistent persistence."""

from __future__ import annotations

from types import TracebackType
from typing import Protocol, Self

from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import Session

from gapforge.storage.models import APPEND_ONLY_MODELS
from gapforge.storage.repositories import MissionRepository, RunRepository


@event.listens_for(Session, "before_flush")
def _protect_append_only_rows(session: Session, _flush_context: object, _instances: object) -> None:
    protected = APPEND_ONLY_MODELS
    if any(isinstance(entity, protected) for entity in session.dirty):
        raise ValueError("append-only records cannot be updated")
    if any(isinstance(entity, protected) for entity in session.deleted):
        raise ValueError("append-only records cannot be deleted")


class UnitOfWork(Protocol):
    missions: MissionRepository
    runs: RunRepository

    async def __aenter__(self) -> Self: ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...

    async def commit(self) -> None: ...

    async def rollback(self) -> None: ...


class SqlAlchemyUnitOfWork:
    """Open one session/transaction and expose repository boundaries."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory
        self.session: AsyncSession | None = None

    async def __aenter__(self) -> Self:
        self.session = self._session_factory()
        self.missions = MissionRepository(self.session)
        self.runs = RunRepository(self.session)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self.session is None:
            return
        if exc_type is not None:
            await self.session.rollback()
        await self.session.close()
        self.session = None

    def _require_session(self) -> AsyncSession:
        if self.session is None:
            raise RuntimeError("unit of work has not been entered")
        return self.session

    async def commit(self) -> None:
        await self._require_session().commit()

    async def rollback(self) -> None:
        await self._require_session().rollback()
