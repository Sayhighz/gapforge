"""Atomic persistence services for normalized research artifacts and decisions."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from typing import Literal
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from gapforge.storage import models

MergeAction = Literal["ACCEPT", "REJECT", "REVERSE"]


class MergeDecisionConflictError(RuntimeError):
    """A manual merge action is invalid for the candidate's current state."""


class MergeDecisionService:
    """Apply one serialized merge decision and append its immutable audit event."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        clock: Callable[[], datetime],
    ) -> None:
        self._session_factory = session_factory
        self._clock = clock

    async def decide(
        self,
        candidate_id: UUID,
        *,
        action: MergeAction,
        actor: str,
        reason: str,
    ) -> models.MergeDecisionEvent:
        if action not in {"ACCEPT", "REJECT", "REVERSE"}:
            raise ValueError("merge action must be ACCEPT, REJECT, or REVERSE")
        actor = actor.strip()
        reason = reason.strip()
        if not actor or len(actor) > 160:
            raise ValueError("merge decision actor must contain 1 to 160 characters")
        if not reason or len(reason) > 500:
            raise ValueError("merge decision reason must contain 1 to 500 characters")
        target = {"ACCEPT": "ACCEPTED", "REJECT": "REJECTED", "REVERSE": "REVERSED"}[action]
        async with self._session_factory() as session:
            candidate = await session.scalar(
                select(models.MergeCandidate)
                .where(models.MergeCandidate.id == candidate_id)
                .with_for_update()
            )
            if candidate is None:
                raise LookupError("merge candidate not found")
            if action == "REVERSE":
                if candidate.status != "ACCEPTED":
                    raise MergeDecisionConflictError(
                        f"REVERSE requires ACCEPTED, not {candidate.status}"
                    )
            elif candidate.status != "PENDING":
                raise MergeDecisionConflictError(
                    f"{action} requires PENDING; current state is {candidate.status}"
                )
            event = models.MergeDecisionEvent(
                candidate_id=candidate.id,
                decision_number=(
                    await session.scalar(
                        select(func.max(models.MergeDecisionEvent.decision_number)).where(
                            models.MergeDecisionEvent.candidate_id == candidate.id
                        )
                    )
                    or 0
                )
                + 1,
                action=action,
                from_status=candidate.status,
                to_status=target,
                actor=actor,
                reason=reason,
                created_at=self._clock(),
            )
            session.add(event)
            await session.flush()
            candidate.status = target
            candidate.decided_at = event.created_at
            candidate.decided_by = actor
            candidate.decision_reason = reason
            candidate.decision_event_id = event.id
            await session.commit()
            return event
