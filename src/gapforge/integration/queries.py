"""Typed read surfaces over persisted research and immutable snapshot lineage."""

from __future__ import annotations

import json
from typing import Any
from uuid import UUID

from sqlalchemy import desc, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from gapforge.analysis.evidence import EvidenceRecord
from gapforge.domain import contracts as domain
from gapforge.integration.mappers import atomic_claim_from_storage
from gapforge.integration.persistence import validation_from_storage
from gapforge.reports.renderers import (
    OpportunityReportData,
    ReportOpportunity,
    RunReportData,
    validated_claim_view,
)
from gapforge.storage import models


class ResearchQueryService:
    """Expose bounded, deterministic projections without leaking ORM rows."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def opportunities(self) -> list[dict[str, Any]]:
        async with self._session_factory() as session:
            rows = tuple(
                await session.scalars(
                    select(models.Opportunity).order_by(
                        models.Opportunity.created_at,
                        models.Opportunity.id,
                    )
                )
            )
            equivalence = await _accepted_equivalence(session)
            return [
                {
                    "id": row.id,
                    "canonical_key": row.canonical_key,
                    "title": row.title,
                    "canonical_problem_id": row.canonical_problem_id,
                    "equivalent_problem_ids": equivalence.get(row.canonical_problem_id, []),
                }
                for row in rows
            ]

    async def opportunity(self, identifier: UUID) -> dict[str, Any]:
        async with self._session_factory() as session:
            row = await session.get(models.Opportunity, identifier)
            if row is None:
                raise LookupError("opportunity not found")
            equivalence = await _accepted_equivalence(session)
            assessments = tuple(
                await session.scalars(
                    select(models.MissionOpportunityAssessment)
                    .where(models.MissionOpportunityAssessment.opportunity_id == identifier)
                    .order_by(
                        models.MissionOpportunityAssessment.created_at,
                        models.MissionOpportunityAssessment.id,
                    )
                )
            )
            assessment_data = [await _assessment_projection(session, item) for item in assessments]
            return {
                "id": row.id,
                "canonical_key": row.canonical_key,
                "title": row.title,
                "canonical_problem_id": row.canonical_problem_id,
                "origin_gap_hypothesis_id": row.gap_hypothesis_id,
                "equivalent_problem_ids": equivalence.get(row.canonical_problem_id, []),
                "assessments": assessment_data,
            }

    async def evidence(self, *, limit: int) -> list[dict[str, Any]]:
        async with self._session_factory() as session:
            revisions = tuple(
                await session.scalars(
                    select(models.RawSignalRevision)
                    .order_by(
                        desc(models.RawSignalRevision.observed_at),
                        models.RawSignalRevision.domain_revision_id,
                    )
                    .limit(limit)
                )
            )
            return [
                {
                    "kind": "raw_signal_revision",
                    "id": row.domain_revision_id,
                    "storage_id": row.id,
                    "observed_at": row.observed_at,
                    "original_language": row.original_language,
                    "tombstone": row.is_tombstone,
                }
                for row in revisions
            ]

    async def evidence_item(self, identifier: str) -> dict[str, Any]:
        async with self._session_factory() as session:
            revision = await session.scalar(
                select(models.RawSignalRevision).where(
                    models.RawSignalRevision.domain_revision_id == identifier
                )
            )
            if revision is not None:
                signal = await session.get(models.RawSignal, revision.raw_signal_id)
                assert signal is not None
                return {
                    "kind": "raw_signal_revision",
                    "id": revision.domain_revision_id,
                    "storage_id": revision.id,
                    "source": signal.source,
                    "external_id": signal.external_id,
                    "canonical_url": signal.canonical_url,
                    "title": revision.title,
                    "body": revision.body,
                    "observed_at": revision.observed_at,
                    "original_language": revision.original_language,
                    "content_hash": revision.content_hash.hex(),
                    "tombstone": revision.is_tombstone,
                }
            try:
                storage_id = UUID(identifier)
            except ValueError as exc:
                raise LookupError("evidence not found") from exc
            card = await session.get(models.EvidenceCard, storage_id)
            if card is not None:
                return {
                    "kind": "evidence_card",
                    "id": card.id,
                    "opportunity_id": card.opportunity_id,
                    "run_id": card.run_id,
                    "metrics": card.metrics,
                    "confidence": card.confidence,
                    "supporting_claim_ids": card.supporting_claim_ids,
                    "contradicting_claim_ids": card.contradicting_claim_ids,
                    "representative_evidence_ids": card.representative_signal_ids,
                    "missing_evidence": card.missing_evidence,
                }
            claim = await session.get(models.AtomicClaim, storage_id)
            if claim is not None:
                return {
                    "kind": "atomic_claim",
                    "id": claim.id,
                    "status": claim.status,
                    "claim_type": claim.claim_type,
                    "text": claim.text,
                    "evidence_ids": claim.evidence_ids,
                    "citations": claim.citations,
                    "contradicts_claim_ids": claim.contradicts_claim_ids,
                }
            raise LookupError("evidence not found")

    async def changes(self, *, limit: int) -> list[dict[str, Any]]:
        async with self._session_factory() as session:
            rows = tuple(
                await session.scalars(
                    select(models.LifecycleEvent)
                    .order_by(
                        desc(models.LifecycleEvent.created_at),
                        desc(models.LifecycleEvent.event_number),
                        models.LifecycleEvent.assessment_id,
                    )
                    .limit(limit)
                )
            )
            return [_lifecycle_view(row) for row in rows]

    async def rejected(self) -> list[dict[str, Any]]:
        async with self._session_factory() as session:
            rows = tuple(
                await session.scalars(
                    select(models.MissionOpportunityAssessment)
                    .where(models.MissionOpportunityAssessment.lifecycle_status == "REJECTED")
                    .order_by(
                        desc(models.MissionOpportunityAssessment.rejected_at),
                        models.MissionOpportunityAssessment.id,
                    )
                )
            )
            return [await _assessment_projection(session, row) for row in rows]

    async def merge_candidates(self) -> list[dict[str, Any]]:
        async with self._session_factory() as session:
            rows = tuple(
                await session.scalars(
                    select(models.MergeCandidate).order_by(
                        desc(models.MergeCandidate.created_at),
                        models.MergeCandidate.id,
                    )
                )
            )
            return [_merge_candidate_view(row) for row in rows]

    async def merge_history(self, identifier: UUID) -> list[dict[str, Any]]:
        async with self._session_factory() as session:
            if await session.get(models.MergeCandidate, identifier) is None:
                raise LookupError("merge candidate not found")
            rows = tuple(
                await session.scalars(
                    select(models.MergeDecisionEvent)
                    .where(models.MergeDecisionEvent.candidate_id == identifier)
                    .order_by(models.MergeDecisionEvent.decision_number)
                )
            )
            return [
                {
                    "id": row.id,
                    "candidate_id": row.candidate_id,
                    "decision_number": row.decision_number,
                    "action": row.action,
                    "from_status": row.from_status,
                    "to_status": row.to_status,
                    "actor": row.actor,
                    "reason": row.reason,
                    "created_at": row.created_at,
                }
                for row in rows
            ]

    async def run_report(self, run_id: UUID) -> RunReportData:
        async with self._session_factory() as session:
            await session.execute(
                text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            )
            run = await session.get(models.ResearchRun, run_id)
            if run is None:
                raise LookupError("run not found")
            revision = await session.get(models.MissionRevision, run.mission_revision_id)
            assert revision is not None
            events = tuple(
                await session.scalars(
                    select(models.LifecycleEvent)
                    .join(
                        models.MissionOpportunityAssessment,
                        models.MissionOpportunityAssessment.id
                        == models.LifecycleEvent.assessment_id,
                    )
                    .where(
                        models.LifecycleEvent.run_id == run.id,
                        models.MissionOpportunityAssessment.mission_revision_id == revision.id,
                    )
                    .order_by(
                        models.LifecycleEvent.assessment_id,
                        desc(models.LifecycleEvent.event_number),
                    )
                )
            )
            latest: dict[UUID, models.LifecycleEvent] = {}
            for event in events:
                latest.setdefault(event.assessment_id, event)
            report_items = []
            for event in latest.values():
                if not _is_final_snapshot_event(event):
                    continue
                report_items.append(await _report_opportunity(session, event, revision))
            warnings = tuple(
                json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
                for value in run.warnings
            )
            return RunReportData(
                run_id=str(run.id),
                mission_revision_id=str(revision.id),
                status=domain.RunStatus(run.status),
                started_at=run.started_at or run.created_at,
                finished_at=run.completed_at,
                output_locale=revision.output_locale,
                warnings=warnings,
                opportunities=tuple(report_items),
            )

    async def opportunity_report(self, opportunity_id: UUID) -> OpportunityReportData:
        async with self._session_factory() as session:
            await session.execute(
                text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            )
            event = await session.scalar(
                select(models.LifecycleEvent)
                .join(
                    models.MissionOpportunityAssessment,
                    models.MissionOpportunityAssessment.id == models.LifecycleEvent.assessment_id,
                )
                .join(
                    models.ResearchRun,
                    models.ResearchRun.id == models.LifecycleEvent.run_id,
                )
                .where(models.MissionOpportunityAssessment.opportunity_id == opportunity_id)
                .order_by(
                    desc(models.ResearchRun.started_at),
                    desc(models.LifecycleEvent.event_number),
                    desc(models.LifecycleEvent.created_at),
                )
            )
            if event is None or not _is_final_snapshot_event(event):
                raise LookupError("opportunity has no persisted final report snapshot")
            assessment = await session.get(models.MissionOpportunityAssessment, event.assessment_id)
            assert assessment is not None
            revision = await session.get(models.MissionRevision, assessment.mission_revision_id)
            assert revision is not None
            report = await _report_opportunity(session, event, revision)
            return OpportunityReportData(report, revision.output_locale)


async def _accepted_equivalence(session: AsyncSession) -> dict[UUID, list[UUID]]:
    rows = tuple(
        await session.scalars(
            select(models.MergeCandidate).where(models.MergeCandidate.status == "ACCEPTED")
        )
    )
    adjacency: dict[UUID, set[UUID]] = {}
    for row in rows:
        adjacency.setdefault(row.left_problem_id, set()).add(row.right_problem_id)
        adjacency.setdefault(row.right_problem_id, set()).add(row.left_problem_id)
    result: dict[UUID, list[UUID]] = {}
    for root in adjacency:
        seen = {root}
        pending = [root]
        while pending:
            current = pending.pop()
            for neighbor in adjacency.get(current, set()):
                if neighbor not in seen:
                    seen.add(neighbor)
                    pending.append(neighbor)
        result[root] = sorted(seen - {root})
    return result


async def _assessment_projection(
    session: AsyncSession,
    row: models.MissionOpportunityAssessment,
) -> dict[str, Any]:
    event = await session.scalar(
        select(models.LifecycleEvent)
        .where(models.LifecycleEvent.assessment_id == row.id)
        .order_by(desc(models.LifecycleEvent.event_number))
        .limit(1)
    )
    score_id = event.details.get("score_snapshot_id") if event is not None else None
    score = await session.get(models.OpportunityScoreSnapshot, UUID(score_id)) if score_id else None
    return {
        "id": row.id,
        "opportunity_id": row.opportunity_id,
        "mission_revision_id": row.mission_revision_id,
        "status": row.lifecycle_status,
        "verdict": row.verdict,
        "score_snapshot_id": score.id if score else None,
        "score": score.final_score if score else None,
        "gap_hypothesis_id": event.gap_hypothesis_id if event else None,
        "rejected_at": row.rejected_at,
        "latest_event": _lifecycle_view(event) if event else None,
    }


def _lifecycle_view(row: models.LifecycleEvent) -> dict[str, Any]:
    return {
        "id": row.id,
        "assessment_id": row.assessment_id,
        "event_number": row.event_number,
        "run_id": row.run_id,
        "gap_hypothesis_id": row.gap_hypothesis_id,
        "from_status": row.from_status,
        "to_status": row.to_status,
        "reason": row.reason,
        "details": row.details,
        "created_at": row.created_at,
    }


def _merge_candidate_view(row: models.MergeCandidate) -> dict[str, Any]:
    return {
        "id": row.id,
        "left_problem_id": row.left_problem_id,
        "right_problem_id": row.right_problem_id,
        "similarity": row.similarity,
        "status": row.status,
        "rationale": row.rationale,
        "decided_at": row.decided_at,
        "decided_by": row.decided_by,
        "decision_reason": row.decision_reason,
        "decision_event_id": row.decision_event_id,
    }


def _is_final_snapshot_event(event: models.LifecycleEvent) -> bool:
    return set(event.details) >= {
        "evidence_card_id",
        "score_snapshot_id",
        "critic_result_id",
        "gap_hypothesis_id",
        "verdict",
        "gates",
    }


async def _report_opportunity(
    session: AsyncSession,
    event: models.LifecycleEvent,
    revision: models.MissionRevision,
) -> ReportOpportunity:
    assessment = await session.get(models.MissionOpportunityAssessment, event.assessment_id)
    if assessment is None:
        raise ValueError("report event references unknown assessment")
    opportunity = await session.get(models.Opportunity, assessment.opportunity_id)
    if opportunity is None:
        raise ValueError("report assessment references unknown opportunity")
    try:
        card_id = UUID(str(event.details["evidence_card_id"]))
        score_id = UUID(str(event.details["score_snapshot_id"]))
        critic_id = UUID(str(event.details["critic_result_id"]))
        gap_id = UUID(str(event.details["gap_hypothesis_id"]))
    except (KeyError, ValueError) as exc:
        raise ValueError("report lifecycle event has invalid snapshot identity") from exc
    if gap_id != event.gap_hypothesis_id:
        raise ValueError("report lifecycle gap detail disagrees with normalized FK")
    card = await session.get(models.EvidenceCard, card_id)
    score = await session.get(models.OpportunityScoreSnapshot, score_id)
    critic = await session.get(models.CriticResult, critic_id)
    gap = await session.get(models.GapHypothesis, gap_id)
    if card is None or score is None or critic is None or gap is None:
        raise ValueError("report snapshot references missing artifacts")
    decision = await validation_from_storage(
        session,
        assessment=assessment,
        card=card,
        score=score,
        critic=critic,
        gap=gap,
        mission_revision_id=revision.id,
    )
    if event.details["verdict"] != decision.verdict.value or assessment.verdict != decision.verdict:
        raise ValueError("report verdict disagrees with persisted validation snapshot")
    typed_card, typed_score = await _typed_card_and_score(
        session,
        assessment=assessment,
        card=card,
        score=score,
        mission_revision_id=revision.id,
    )
    claims = await _report_claims(session, card)
    return ReportOpportunity(
        opportunity_id=str(opportunity.id),
        title=opportunity.title,
        verdict=decision.verdict,
        score=typed_score,
        evidence_card=typed_card,
        claims=claims,
        validation=decision,
    )


async def _typed_card_and_score(
    session: AsyncSession,
    *,
    assessment: models.MissionOpportunityAssessment,
    card: models.EvidenceCard,
    score: models.OpportunityScoreSnapshot,
    mission_revision_id: UUID,
) -> tuple[domain.EvidenceCard, domain.OpportunityScoreSnapshot]:
    from gapforge.integration.mappers import evidence_card_from_storage, score_snapshot_from_storage

    revisions = {
        row.id: row.domain_revision_id
        for row in await session.scalars(
            select(models.RawSignalRevision).where(
                models.RawSignalRevision.id.in_(card.representative_signal_ids)
            )
        )
    }
    claim_ids = {*card.supporting_claim_ids, *card.contradicting_claim_ids}
    claims = {
        value: str(value)
        for value in await session.scalars(
            select(models.AtomicClaim.id).where(models.AtomicClaim.id.in_(claim_ids))
        )
    }
    return (
        evidence_card_from_storage(
            card,
            revision_identifiers=revisions,
            claim_identifiers=claims,
        ),
        score_snapshot_from_storage(
            score,
            opportunity_id=assessment.opportunity_id,
            mission_revision_id=mission_revision_id,
        ),
    )


async def _report_claims(
    session: AsyncSession,
    card: models.EvidenceCard,
) -> tuple[Any, ...]:
    claim_ids = {*card.supporting_claim_ids, *card.contradicting_claim_ids}
    claim_rows = tuple(
        await session.scalars(
            select(models.AtomicClaim).where(models.AtomicClaim.id.in_(claim_ids))
        )
    )
    evidence_ids = {value for row in claim_rows for value in row.evidence_ids}
    revision_rows = tuple(
        await session.scalars(
            select(models.RawSignalRevision).where(models.RawSignalRevision.id.in_(evidence_ids))
        )
    )
    competitor_rows = tuple(
        await session.scalars(
            select(models.CompetitorEvidence).where(models.CompetitorEvidence.id.in_(evidence_ids))
        )
    )
    revision_identifiers = {row.id: row.domain_revision_id for row in revision_rows}
    captured_identifiers = {row.id: str(row.id) for row in competitor_rows}
    evidence: dict[str, EvidenceRecord] = {}
    for revision_row in revision_rows:
        signal = await session.get(models.RawSignal, revision_row.raw_signal_id)
        assert signal is not None
        evidence[revision_row.domain_revision_id] = EvidenceRecord(
            revision_row.domain_revision_id,
            domain.Source(signal.source),
            signal.canonical_url,
            " ".join(value for value in (revision_row.title, revision_row.body) if value),
            revision_row.observed_at,
        )
    for competitor_row in competitor_rows:
        evidence[str(competitor_row.id)] = EvidenceRecord(
            str(competitor_row.id),
            domain.Source.STATIC_WEB,
            competitor_row.source_url,
            competitor_row.captured_excerpt,
            competitor_row.observed_at,
        )
    views = []
    for claim_row in claim_rows:
        claim = atomic_claim_from_storage(
            claim_row,
            revision_identifiers=revision_identifiers,
            captured_evidence_identifiers=captured_identifiers,
        )
        views.append(
            validated_claim_view(
                claim,
                evidence,
                known_claim_ids=frozenset(str(value) for value in claim_ids),
            )
        )
    return tuple(views)
