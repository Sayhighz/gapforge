"""Repository interfaces keep callers independent from SQLAlchemy session mechanics."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Protocol, TypeVar
from uuid import UUID

from sqlalchemy import Select, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from gapforge.storage.models import MissionRevision, ResearchMission, ResearchRun

ModelT = TypeVar("ModelT")


class Repository(Protocol[ModelT]):
    async def add(self, entity: ModelT) -> ModelT: ...

    async def get(self, entity_id: UUID) -> ModelT | None: ...


class SqlAlchemyRepository[ModelT]:
    """Small generic base; domain-specific reads belong on named repositories."""

    def __init__(self, session: AsyncSession, model: type[ModelT]) -> None:
        self.session = session
        self.model = model

    async def add(self, entity: ModelT) -> ModelT:
        self.session.add(entity)
        await self.session.flush()
        return entity

    async def get(self, entity_id: UUID) -> ModelT | None:
        return await self.session.get(self.model, entity_id)


class MissionRepository(SqlAlchemyRepository[ResearchMission]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, ResearchMission)

    async def list(self, *, limit: int = 100) -> Sequence[ResearchMission]:
        statement = select(ResearchMission).order_by(ResearchMission.created_at).limit(limit)
        return tuple((await self.session.scalars(statement)).all())

    async def latest_revision(self, mission_id: UUID) -> MissionRevision | None:
        statement = (
            select(MissionRevision)
            .where(MissionRevision.mission_id == mission_id)
            .order_by(MissionRevision.revision_number.desc())
            .limit(1)
        )
        return (await self.session.scalars(statement)).first()

    async def create_with_revision(
        self,
        *,
        title: str,
        mission_text: str,
        original_language: str,
        output_locale: str,
        interpretation: dict[str, object] | None = None,
    ) -> tuple[ResearchMission, MissionRevision]:
        mission = ResearchMission(title=title, status="DRAFT")
        self.session.add(mission)
        await self.session.flush()
        revision = MissionRevision(
            mission_id=mission.id,
            revision_number=1,
            parent_revision_id=None,
            change_reason="initial creation",
            mission_text=mission_text,
            original_language=original_language,
            output_locale=output_locale,
            interpretation=interpretation or {},
        )
        self.session.add(revision)
        await self.session.flush()
        return mission, revision

    async def revise(
        self,
        mission_id: UUID,
        *,
        mission_text: str,
        change_reason: str,
        original_language: str,
        output_locale: str,
        interpretation: dict[str, object] | None = None,
    ) -> MissionRevision:
        mission_statement: Select[tuple[ResearchMission]] = (
            select(ResearchMission).where(ResearchMission.id == mission_id).with_for_update()
        )
        mission = await self.session.scalar(mission_statement)
        if mission is None:
            raise LookupError(f"mission {mission_id} does not exist")
        latest_number = await self.session.scalar(
            select(func.max(MissionRevision.revision_number)).where(
                MissionRevision.mission_id == mission_id
            )
        )
        parent = await self.latest_revision(mission_id)
        revision = MissionRevision(
            mission_id=mission_id,
            revision_number=(latest_number or 0) + 1,
            parent_revision_id=parent.id if parent else None,
            change_reason=change_reason,
            mission_text=mission_text,
            original_language=original_language,
            output_locale=output_locale,
            interpretation=interpretation or {},
        )
        self.session.add(revision)
        await self.session.flush()
        return revision

    async def set_status(self, mission_id: UUID, status: str) -> ResearchMission:
        if status not in {"ACTIVE", "PAUSED", "ARCHIVED"}:
            raise ValueError(f"unsupported mission status transition target: {status}")
        mission = await self.get(mission_id)
        if mission is None:
            raise LookupError(f"mission {mission_id} does not exist")
        now = datetime.now(UTC)
        mission.status = status
        if status == "ACTIVE":
            mission.activated_at = now
        elif status == "PAUSED":
            mission.paused_at = now
        else:
            mission.archived_at = now
        await self.session.flush()
        return mission


class RunRepository(SqlAlchemyRepository[ResearchRun]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, ResearchRun)

    async def list(self, *, limit: int = 100) -> Sequence[ResearchRun]:
        statement = select(ResearchRun).order_by(ResearchRun.created_at.desc()).limit(limit)
        return tuple((await self.session.scalars(statement)).all())
