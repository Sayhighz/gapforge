"""Explicit post-VALIDATE Product Hypothesis persistence and query service."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from typing import cast
from uuid import UUID, uuid5

from sqlalchemy import desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from gapforge.analysis.hypotheses import create_product_hypothesis
from gapforge.domain import contracts as domain
from gapforge.storage import models


class ProductHypothesisConflictError(RuntimeError):
    """An explicit request identity was reused with different immutable content."""


class ProductHypothesisStateError(RuntimeError):
    """The assessment is not currently eligible for a Product Hypothesis."""


class ProductHypothesisService:
    """Create and retrieve explicit, immutable Product Hypotheses without an agent call."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._clock = clock or (lambda: datetime.now(UTC))

    async def create(
        self,
        assessment_id: UUID,
        *,
        request_id: str,
        proposition: str,
    ) -> domain.ProductHypothesis:
        request_id = request_id.strip()
        if not request_id or len(request_id) > 160:
            raise ValueError("request ID must contain 1 to 160 characters")
        if any(ord(character) < 32 or ord(character) == 127 for character in request_id):
            raise ValueError("request ID cannot contain control characters")
        proposition = proposition.strip()
        if not proposition or len(proposition) > 20_000:
            raise ValueError("proposition must contain 1 to 20000 characters")
        if "\x00" in proposition:
            raise ValueError("proposition cannot contain a NUL character")
        content = {
            "schema_version": "0.1",
            "proposition": proposition,
        }

        async with self._session_factory() as session:
            assessment = await session.scalar(
                select(models.MissionOpportunityAssessment)
                .where(models.MissionOpportunityAssessment.id == assessment_id)
                .with_for_update()
            )
            if assessment is None:
                raise LookupError("assessment not found")
            identifier = uuid5(assessment.id, f"product-hypothesis:{request_id}")
            existing = await session.get(models.ProductHypothesis, identifier)
            if existing is not None:
                if (
                    existing.assessment_id != assessment.id
                    or existing.requested_by != request_id
                    or existing.content != content
                ):
                    raise ProductHypothesisConflictError(
                        "Product Hypothesis request identity already has different content"
                    )
                return _domain_product_hypothesis(existing)

            if assessment.verdict != "VALIDATE" or assessment.lifecycle_status != "VALIDATE":
                raise ProductHypothesisStateError(
                    "Product Hypothesis requires a current VALIDATE assessment"
                )
            snapshot = await _latest_validate_snapshot(session, assessment.id)
            if snapshot is None:
                raise ProductHypothesisStateError(
                    "Product Hypothesis requires a persisted VALIDATE final snapshot"
                )

            created_at = self._clock()
            typed_assessment = _domain_assessment(assessment, snapshot)
            requested = create_product_hypothesis(
                hypothesis_id=str(identifier),
                assessment=typed_assessment,
                explicit_request_id=request_id,
                proposition=proposition,
                created_at=created_at,
            )
            row = models.ProductHypothesis(
                id=identifier,
                assessment_id=assessment.id,
                requested_by=requested.explicit_request_id,
                content=content,
                evidence_card_id=snapshot.evidence_card_id,
                created_at=created_at,
            )
            session.add(row)
            await session.flush()
            await session.commit()
            return _domain_product_hypothesis(row)

    async def get(self, identifier: UUID) -> domain.ProductHypothesis:
        async with self._session_factory() as session:
            row = await session.get(models.ProductHypothesis, identifier)
            if row is None:
                raise LookupError("Product Hypothesis not found")
            return _domain_product_hypothesis(row)


async def _latest_validate_snapshot(
    session: AsyncSession, assessment_id: UUID
) -> models.FinalAssessmentSnapshot | None:
    return cast(
        models.FinalAssessmentSnapshot | None,
        await session.scalar(
            select(models.FinalAssessmentSnapshot)
            .join(
                models.ResearchRun,
                models.ResearchRun.id == models.FinalAssessmentSnapshot.run_id,
            )
            .where(
                models.FinalAssessmentSnapshot.assessment_id == assessment_id,
                models.FinalAssessmentSnapshot.verdict == "VALIDATE",
            )
            .order_by(
                desc(func.coalesce(models.ResearchRun.started_at, models.ResearchRun.created_at)),
                desc(models.FinalAssessmentSnapshot.round_number),
                desc(models.FinalAssessmentSnapshot.created_at),
                desc(models.FinalAssessmentSnapshot.id),
            )
            .limit(1)
        ),
    )


def _domain_assessment(
    row: models.MissionOpportunityAssessment,
    snapshot: models.FinalAssessmentSnapshot,
) -> domain.MissionOpportunityAssessment:
    return domain.MissionOpportunityAssessment(
        id=str(row.id),
        mission_revision_id=row.mission_revision_id,
        opportunity_id=str(row.opportunity_id),
        lifecycle_state=domain.LifecycleState(row.lifecycle_status),
        relevance=float(row.relevance),
        verdict=domain.Verdict(row.verdict) if row.verdict else None,
        competitor_research_status=domain.CompetitorResearchStatus(row.competitor_research_status),
        score_snapshot_id=str(snapshot.score_snapshot_id),
        evidence_card_id=str(snapshot.evidence_card_id),
        rejected_at=row.rejected_at,
        assessed_at=row.updated_at,
    )


def _domain_product_hypothesis(row: models.ProductHypothesis) -> domain.ProductHypothesis:
    if set(row.content) != {"schema_version", "proposition"}:
        raise ValueError("persisted Product Hypothesis content has an invalid schema")
    if row.content["schema_version"] != "0.1" or not isinstance(row.content["proposition"], str):
        raise ValueError("persisted Product Hypothesis content has invalid values")
    return domain.ProductHypothesis(
        id=str(row.id),
        assessment_id=str(row.assessment_id),
        explicit_request_id=row.requested_by,
        proposition=row.content["proposition"],
        created_at=row.created_at,
    )
