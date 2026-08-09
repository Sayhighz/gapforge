"""Atomic persistence services for normalized research artifacts and decisions."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from datetime import datetime
from decimal import Decimal
from typing import Any, Literal, Protocol, cast
from uuid import NAMESPACE_URL, UUID, uuid5

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from gapforge.analysis.lifecycle import ReopenEvidence, can_reopen_rejected, transition_lifecycle
from gapforge.analysis.normalization import normalize_text
from gapforge.domain import contracts as domain
from gapforge.integration.mappers import (
    evidence_card_from_storage,
    evidence_card_to_storage_values,
    score_snapshot_from_storage,
    score_snapshot_to_storage_values,
    storage_uuid_for_identifier,
)
from gapforge.scoring.engine import ValidationDecision, validation_decision
from gapforge.storage import models

MergeAction = Literal["ACCEPT", "REJECT", "REVERSE"]
_ARTIFACT_NAMESPACE = uuid5(NAMESPACE_URL, "https://gapforge.dev/v0.1/artifacts")


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


class ArtifactMissionRevision(Protocol):
    id: UUID


class ArtifactContext(Protocol):
    run_id: UUID
    task_id: UUID
    task_attempt: int
    mission_revision: ArtifactMissionRevision
    collection_until: datetime


class ArtifactStageCommit(Protocol):
    stage: str
    payload: dict[str, Any]


class ResearchArtifactWriter:
    """Persist validated pipeline artifacts inside the caller-owned stage transaction."""

    async def persist_stage(
        self,
        session: AsyncSession,
        context: ArtifactContext,
        commit: ArtifactStageCommit,
    ) -> None:
        handlers = {
            "EXTRACT": self._persist_extract,
            "EXTRACT_R2": self._persist_extract,
            "CLUSTER": self._persist_cluster,
            "CLUSTER_R2": self._persist_cluster,
            "GAP": self._persist_gap,
            "GAP_R2": self._persist_gap,
            "CARD_SCORE": self._persist_card_score,
            "HYPOTHESIS": self._persist_hypothesis,
            "HYPOTHESIS_R2": self._persist_hypothesis,
            "CRITIC": self._persist_critic,
            "FINAL": self._persist_final,
        }
        if commit.stage == "CRITIC_R2":
            payload = dict(commit.payload)
            payload.pop("_input_bounds", None)
            await self._persist_critic(session, context, payload, round_number=2)
            return
        if commit.stage == "CARD_SCORE_R2":
            payload = dict(commit.payload)
            payload.pop("_input_bounds", None)
            await self._persist_card_score(session, context, payload, round_number=2)
            return
        handler = handlers.get(commit.stage)
        if handler is None:
            if commit.stage in {
                "EXISTING",
                "QUERY_PLAN",
                "COLLECT",
                "COLLECT_R2",
                "COMPETITOR_RESEARCH",
                "RESEARCH_MORE_CONTROL",
                "RESEARCH_MORE_RESULT",
            }:
                return
            raise ValueError(f"unsupported artifact stage: {commit.stage}")
        payload = dict(commit.payload)
        payload.pop("_input_bounds", None)
        await handler(session, context, payload)

    async def _persist_extract(
        self,
        session: AsyncSession,
        _context: ArtifactContext,
        payload: dict[str, Any],
    ) -> None:
        _require_keys("EXTRACT", payload, {"pain_signals"})
        signals = tuple(domain.PainSignal.model_validate(item) for item in payload["pain_signals"])
        _require_unique_ids("pain signals", signals)
        revisions = await _raw_revisions_by_domain_id(
            session, {item.raw_signal_revision_id for item in signals}
        )
        for signal in signals:
            revision = revisions.get(signal.raw_signal_revision_id)
            if revision is None:
                raise ValueError(f"unknown raw signal revision: {signal.raw_signal_revision_id}")
            identifier = _uuid_text(signal.id, "pain signal")
            values = {
                "raw_signal_revision_id": revision.id,
                "extraction_version": "extract-v1",
                "cluster_status": "UNCLUSTERED",
                "pain": signal.pain,
                "user_context": signal.user_context,
                "jtbd": signal.job_to_be_done,
                "severity": Decimal(str(signal.severity)),
                "frequency": Decimal(str(signal.frequency)),
                "workaround": signal.workaround,
                "existing_solution": signal.existing_solution,
                "signals": {
                    "switching": signal.switching_signal,
                    "payment": signal.payment_signal,
                    "urgency": signal.urgency_signal,
                    "emotion": signal.emotion_signal,
                },
                "confidence": Decimal(str(signal.confidence)),
                "excerpt": signal.excerpt,
            }
            await _add_or_verify(session, models.PainSignal, identifier, values, "pain signal")
        await session.flush()

    async def _persist_cluster(
        self,
        session: AsyncSession,
        _context: ArtifactContext,
        payload: dict[str, Any],
    ) -> None:
        _require_keys("CLUSTER", payload, {"problems", "clusters", "memberships"})
        problems = tuple(
            domain.CanonicalProblem.model_validate(item) for item in payload["problems"]
        )
        clusters = tuple(domain.ProblemCluster.model_validate(item) for item in payload["clusters"])
        memberships = tuple(
            domain.ProblemClusterMembership.model_validate(item) for item in payload["memberships"]
        )
        _require_unique_ids("canonical problems", problems)
        _require_unique_ids("problem clusters", clusters)
        problem_ids = {_uuid_text(item.id, "canonical problem") for item in problems}
        cluster_ids = {_uuid_text(item.id, "problem cluster") for item in clusters}
        referenced_problem_ids = {
            _uuid_text(item.canonical_problem_id, "canonical problem") for item in clusters
        }
        if not referenced_problem_ids <= problem_ids:
            raise ValueError("cluster references unknown canonical problem")
        pain_ids = {_uuid_text(item.pain_signal_id, "pain signal") for item in memberships}
        known_pain_ids = set(
            await session.scalars(
                select(models.PainSignal.id).where(models.PainSignal.id.in_(pain_ids))
            )
        )
        if pain_ids != known_pain_ids:
            raise ValueError("cluster membership references unknown pain signal")
        if {_uuid_text(item.cluster_id, "problem cluster") for item in memberships} - cluster_ids:
            raise ValueError("cluster membership references unknown cluster")

        problem_by_id = {_uuid_text(item.id, "canonical problem"): item for item in problems}
        for identifier, problem in problem_by_id.items():
            await _add_or_verify(
                session,
                models.CanonicalProblem,
                identifier,
                {
                    "canonical_key": str(identifier),
                    "title": problem.summary,
                    "summary": problem.summary,
                    "status": "ACTIVE",
                },
                "canonical problem",
            )
        await session.flush()
        for cluster in clusters:
            identifier = _uuid_text(cluster.id, "problem cluster")
            problem_id = _uuid_text(cluster.canonical_problem_id, "canonical problem")
            await _add_or_verify(
                session,
                models.ProblemCluster,
                identifier,
                {
                    "canonical_problem_id": problem_id,
                    "status": cluster.state,
                    "summary": problem_by_id[problem_id].summary,
                    "last_growth_at": cluster.last_growth_at,
                },
                "problem cluster",
            )
        await session.flush()
        for membership in memberships:
            cluster_id = _uuid_text(membership.cluster_id, "problem cluster")
            pain_id = _uuid_text(membership.pain_signal_id, "pain signal")
            identifier = storage_uuid_for_identifier(
                "problem-cluster-membership", f"{cluster_id}:{pain_id}"
            )
            await _add_or_verify(
                session,
                models.ProblemClusterMembership,
                identifier,
                {
                    "problem_cluster_id": cluster_id,
                    "pain_signal_id": pain_id,
                    "similarity": 1.0,
                    "rationale": "validated pipeline membership",
                    "created_at": membership.accepted_at,
                    "removed_at": None,
                    "removed_reason": None,
                },
                "problem cluster membership",
            )
            pain = await session.get(models.PainSignal, pain_id)
            assert pain is not None
            pain.cluster_status = "CLUSTERED"
        await session.flush()

    async def _persist_gap(
        self,
        session: AsyncSession,
        context: ArtifactContext,
        payload: dict[str, Any],
    ) -> None:
        _require_keys(
            "GAP",
            payload,
            {
                "claims",
                "competitors",
                "competitor_evidence",
                "gaps",
                "opportunities",
                "opportunity_fit",
            },
        )
        claims = tuple(domain.AtomicClaim.model_validate(item) for item in payload["claims"])
        competitors = tuple(
            domain.Competitor.model_validate(item) for item in payload["competitors"]
        )
        competitor_evidence = tuple(
            domain.CompetitorEvidence.model_validate(item)
            for item in payload["competitor_evidence"]
        )
        gaps = tuple(domain.GapHypothesis.model_validate(item) for item in payload["gaps"])
        opportunities = tuple(
            domain.Opportunity.model_validate(item) for item in payload["opportunities"]
        )
        for label, artifacts in (
            ("claims", claims),
            ("competitors", competitors),
            ("competitor evidence", competitor_evidence),
            ("gaps", gaps),
            ("opportunities", opportunities),
        ):
            _require_unique_ids(label, artifacts)
        problem_ids = {
            _uuid_text(value, "canonical problem")
            for item in gaps
            for value in (item.canonical_problem_id,)
        }
        stored_problem_ids = set(
            await session.scalars(
                select(models.CanonicalProblem.id).where(
                    models.CanonicalProblem.id.in_(problem_ids)
                )
            )
        )
        if problem_ids != stored_problem_ids:
            raise ValueError("gap references unknown canonical problem")
        competitor_ids = {_uuid_text(item.id, "competitor") for item in competitors}
        evidence_ids = {_uuid_text(item.id, "competitor evidence") for item in competitor_evidence}
        claim_ids = {_uuid_text(item.id, "atomic claim") for item in claims}
        if {
            _uuid_text(item.competitor_id, "competitor") for item in competitor_evidence
        } - competitor_ids:
            raise ValueError("competitor evidence references unknown competitor")
        if {
            _uuid_text(value, "atomic claim")
            for item in competitor_evidence
            for value in item.claim_ids
        } - claim_ids:
            raise ValueError("competitor evidence references unknown claim")
        raw_domain_ids = {
            value for item in claims for value in item.evidence_ids if not _is_uuid(value)
        } | {value for item in gaps for value in item.user_evidence_ids}
        raw_revisions = await _raw_revisions_by_domain_id(session, raw_domain_ids)
        if set(raw_revisions) != raw_domain_ids:
            raise ValueError("claim or gap references unknown raw evidence")
        evidence_map = {key: value.id for key, value in raw_revisions.items()} | {
            str(value): value for value in evidence_ids
        }
        for claim in claims:
            if set(claim.evidence_ids) - set(evidence_map):
                raise ValueError("claim references unknown evidence")
            if {
                _uuid_text(value, "atomic claim") for value in claim.contradicts_claim_ids
            } - claim_ids:
                raise ValueError("claim contradiction references unknown claim")
        for gap in gaps:
            if set(gap.user_evidence_ids) - set(raw_revisions):
                raise ValueError("gap references unknown user evidence")
            if {
                _uuid_text(value, "competitor evidence") for value in gap.competitor_evidence_ids
            } - evidence_ids:
                raise ValueError("gap references unknown competitor evidence")

        competitor_identity_ids: dict[tuple[str, str], UUID] = {}
        for competitor in competitors:
            identifier = _uuid_text(competitor.id, "competitor")
            normalized_name = normalize_text(competitor.name)
            canonical_url = str(competitor.canonical_url or "")
            natural_identity = (normalized_name, canonical_url)
            prior_id = competitor_identity_ids.setdefault(natural_identity, identifier)
            if prior_id != identifier:
                raise ValueError("competitor natural identity maps to conflicting IDs")
            stored_id = await session.scalar(
                select(models.Competitor.id).where(
                    models.Competitor.normalized_name == normalized_name,
                    models.Competitor.canonical_url == canonical_url,
                )
            )
            if stored_id is not None and stored_id != identifier:
                raise ValueError("competitor natural identity maps to a conflicting stored ID")
            await _add_or_verify(
                session,
                models.Competitor,
                identifier,
                {
                    "name": competitor.name,
                    "normalized_name": normalized_name,
                    "canonical_url": canonical_url,
                    "alternative_type": competitor.kind.value,
                },
                "competitor",
            )
        await session.flush()
        for claim in claims:
            identifier = _uuid_text(claim.id, "atomic claim")
            await _add_or_verify(
                session,
                models.AtomicClaim,
                identifier,
                {
                    "subject_type": "GLOBAL_CLAIM",
                    "subject_id": identifier,
                    "claim_type": claim.kind.value,
                    "text": claim.text,
                    "status": claim.status.value,
                    "evidence_ids": [evidence_map[value] for value in claim.evidence_ids],
                    "citations": [
                        {
                            "evidence_id": item.evidence_id,
                            "source_url": str(item.source_url),
                            "excerpt": item.excerpt,
                            "observed_at": item.observed_at.isoformat(),
                        }
                        for item in claim.citations
                    ],
                    "contradicts_claim_ids": [
                        _uuid_text(value, "atomic claim") for value in claim.contradicts_claim_ids
                    ],
                    "observed_at": max(
                        (item.observed_at for item in claim.citations), default=None
                    ),
                },
                "atomic claim",
            )
        for evidence in competitor_evidence:
            identifier = _uuid_text(evidence.id, "competitor evidence")
            await _add_or_verify(
                session,
                models.CompetitorEvidence,
                identifier,
                {
                    "competitor_id": _uuid_text(evidence.competitor_id, "competitor"),
                    "source_url": str(evidence.source_url),
                    "captured_excerpt": evidence.captured_excerpt,
                    "observed_at": evidence.observed_at,
                    "content_hash": bytes.fromhex(evidence.content_hash),
                    "evidence_kind": evidence.evidence_kind,
                    "claim_ids": [
                        _uuid_text(value, "atomic claim") for value in evidence.claim_ids
                    ],
                    "metadata_json": evidence.metadata,
                },
                "competitor evidence",
            )
        await session.flush()
        gap_by_id: dict[UUID, domain.GapHypothesis] = {}
        for gap in gaps:
            identifier = _uuid_text(gap.id, "gap hypothesis")
            gap_by_id[identifier] = gap
            await _add_or_verify(
                session,
                models.GapHypothesis,
                identifier,
                {
                    "canonical_problem_id": _uuid_text(
                        gap.canonical_problem_id, "canonical problem"
                    ),
                    "gap_type": gap.gap_type.value,
                    "statement": gap.statement,
                    "user_evidence_ids": [
                        raw_revisions[value].id for value in gap.user_evidence_ids
                    ],
                    "competitor_evidence_ids": [
                        _uuid_text(value, "competitor evidence")
                        for value in gap.competitor_evidence_ids
                    ],
                    "contradicting_claim_ids": [],
                },
                "gap hypothesis",
            )
        await session.flush()
        competitor_status = await _competitor_status(session, context.task_id)
        for opportunity in opportunities:
            identifier = _uuid_text(opportunity.id, "opportunity")
            gap_id = _uuid_text(opportunity.gap_hypothesis_id, "gap hypothesis")
            current_gap = gap_by_id.get(gap_id)
            if current_gap is None:
                raise ValueError("opportunity references unknown gap hypothesis")
            problem_id = _uuid_text(current_gap.canonical_problem_id, "canonical problem")
            existing_opportunity = await session.get(models.Opportunity, identifier)
            origin_gap_id = (
                existing_opportunity.gap_hypothesis_id
                if existing_opportunity is not None
                else gap_id
            )
            await _add_or_verify(
                session,
                models.Opportunity,
                identifier,
                {
                    "canonical_problem_id": problem_id,
                    "gap_hypothesis_id": origin_gap_id,
                    "canonical_key": str(identifier),
                    "title": opportunity.title,
                },
                "opportunity",
            )
        await session.flush()
        for opportunity in opportunities:
            opportunity_id = _uuid_text(opportunity.id, "opportunity")
            gap_id = _uuid_text(opportunity.gap_hypothesis_id, "gap hypothesis")
            assessment_id = uuid5(context.mission_revision.id, f"assessment:{opportunity_id}")
            assessment = await session.get(models.MissionOpportunityAssessment, assessment_id)
            if assessment is None:
                assessment = models.MissionOpportunityAssessment(
                    id=assessment_id,
                    mission_revision_id=context.mission_revision.id,
                    opportunity_id=opportunity_id,
                    lifecycle_status="DISCOVERED",
                    relevance=Decimal("1"),
                    verdict=None,
                    competitor_research_status=competitor_status,
                    rejected_at=None,
                )
                session.add(assessment)
                await session.flush()
                session.add(
                    models.LifecycleEvent(
                        id=uuid5(
                            context.run_id,
                            f"lifecycle:{opportunity_id}:DISCOVERED",
                        ),
                        assessment_id=assessment_id,
                        event_number=1,
                        run_id=context.run_id,
                        gap_hypothesis_id=gap_id,
                        from_status=None,
                        to_status="DISCOVERED",
                        reason="opportunity discovered in pipeline gap research",
                        details={},
                        created_at=context.collection_until,
                    )
                )
                await session.flush()
                session.add(
                    models.LifecycleEvent(
                        id=uuid5(
                            context.run_id,
                            f"lifecycle:{opportunity_id}:RESEARCHING",
                        ),
                        assessment_id=assessment_id,
                        event_number=2,
                        run_id=context.run_id,
                        gap_hypothesis_id=gap_id,
                        from_status="DISCOVERED",
                        to_status="RESEARCHING",
                        reason="pipeline gap research completed",
                        details={},
                        created_at=context.collection_until,
                    )
                )
                await session.flush()
                await session.refresh(assessment)
            elif assessment.competitor_research_status != competitor_status:
                assessment.competitor_research_status = competitor_status
        await session.flush()

    async def _persist_card_score(
        self,
        session: AsyncSession,
        context: ArtifactContext,
        payload: dict[str, Any],
        *,
        round_number: int = 1,
    ) -> None:
        _require_keys("CARD_SCORE", payload, {"cards", "scores"})
        cards = tuple(domain.EvidenceCard.model_validate(item) for item in payload["cards"])
        scores = tuple(
            domain.OpportunityScoreSnapshot.model_validate(item) for item in payload["scores"]
        )
        if {item.opportunity_id for item in cards} != {item.opportunity_id for item in scores}:
            raise ValueError("cards and score snapshots must cover the same opportunities")
        representative_ids = {value for card in cards for value in card.representative_evidence_ids}
        raw_rows = await _raw_revisions_by_domain_id(session, representative_ids)
        revision_ids = {key: item.id for key, item in raw_rows.items()}
        requested_claim_ids = {
            _uuid_text(value, "atomic claim")
            for card in cards
            for value in (*card.supporting_claim_ids, *card.contradicting_claim_ids)
        }
        known_claim_ids = set(
            await session.scalars(
                select(models.AtomicClaim.id).where(models.AtomicClaim.id.in_(requested_claim_ids))
            )
        )
        claim_ids = {str(value): value for value in known_claim_ids}
        for card in cards:
            opportunity_id = _uuid_text(card.opportunity_id, "opportunity")
            expected_card_id = uuid5(
                context.run_id,
                (f"card:{opportunity_id}" if round_number == 1 else f"card:2:{opportunity_id}"),
            )
            if _uuid_text(card.id, "Evidence Card") != expected_card_id:
                raise ValueError("Evidence Card ID does not match its stage snapshot identity")
            opportunity = await session.get(models.Opportunity, opportunity_id)
            if opportunity is None:
                raise ValueError("Evidence Card references unknown opportunity")
            card_values = evidence_card_to_storage_values(
                card,
                canonical_problem_id=opportunity.canonical_problem_id,
                run_id=context.run_id,
                algorithm_version="evidence-card-v1",
                revision_ids=revision_ids,
                claim_ids=claim_ids,
            )
            await _add_or_verify(
                session,
                models.EvidenceCard,
                card_values["id"],
                {key: value for key, value in card_values.items() if key != "id"},
                "Evidence Card",
            )
        await session.flush()
        for score in scores:
            if score.mission_revision_id != context.mission_revision.id:
                raise ValueError("score snapshot belongs to another mission revision")
            opportunity_id = _uuid_text(score.opportunity_id, "opportunity")
            expected_score_id = uuid5(
                context.run_id,
                (f"score:{opportunity_id}" if round_number == 1 else f"score:2:{opportunity_id}"),
            )
            if _uuid_text(score.id, "score snapshot") != expected_score_id:
                raise ValueError("score ID does not match its stage snapshot identity")
            assessment = await _assessment(session, context, opportunity_id)
            score_values = score_snapshot_to_storage_values(
                score, assessment_id=assessment.id, run_id=context.run_id
            )
            await _add_or_verify(
                session,
                models.OpportunityScoreSnapshot,
                score_values["id"],
                {key: value for key, value in score_values.items() if key != "id"},
                "score snapshot",
            )
        await session.flush()
        if round_number == 2:
            for card in cards:
                await _maybe_reopen_rejected(
                    session,
                    context,
                    _uuid_text(card.id, "Evidence Card"),
                )
            await session.flush()

    async def _persist_hypothesis(
        self,
        session: AsyncSession,
        context: ArtifactContext,
        payload: dict[str, Any],
    ) -> None:
        _require_keys(
            "HYPOTHESIS",
            payload,
            {"hypotheses", "hypothesis_card_links"},
        )
        hypotheses = tuple(
            domain.ProblemHypothesis.model_validate(item) for item in payload["hypotheses"]
        )
        _require_unique_ids("problem hypotheses", hypotheses)
        links = _hypothesis_card_links(payload["hypothesis_card_links"])
        hypothesis_ids = {_uuid_text(item.id, "problem hypothesis") for item in hypotheses}
        if set(links) != hypothesis_ids:
            raise ValueError("hypothesis card links must cover every hypothesis exactly once")
        opportunity_ids = [item[0] for item in links.values()]
        card_ids = [item[1] for item in links.values()]
        if len(opportunity_ids) != len(set(opportunity_ids)):
            raise ValueError("hypothesis card links must cover each opportunity once")
        if len(card_ids) != len(set(card_ids)):
            raise ValueError("hypothesis card links must use distinct Evidence Cards")
        requested_claim_ids = {
            _uuid_text(value, "atomic claim")
            for hypothesis in hypotheses
            for value in (
                *hypothesis.supporting_claim_ids,
                *hypothesis.contradicting_claim_ids,
            )
        }
        claim_ids = set(
            await session.scalars(
                select(models.AtomicClaim.id).where(models.AtomicClaim.id.in_(requested_claim_ids))
            )
        )
        for hypothesis in hypotheses:
            identifier = _uuid_text(hypothesis.id, "problem hypothesis")
            problem_id = _uuid_text(hypothesis.canonical_problem_id, "canonical problem")
            supporting = {
                _uuid_text(value, "atomic claim") for value in hypothesis.supporting_claim_ids
            }
            contradicting = {
                _uuid_text(value, "atomic claim") for value in hypothesis.contradicting_claim_ids
            }
            if not supporting | contradicting <= claim_ids:
                raise ValueError("problem hypothesis references unknown claim")
            opportunity_id, card_id = links[identifier]
            _validate_hypothesis_id(
                hypothesis,
                opportunity_id=opportunity_id,
                evidence_card_id=card_id,
                identifier=identifier,
            )
            card = await session.get(models.EvidenceCard, card_id)
            opportunity = await session.get(models.Opportunity, opportunity_id)
            if (
                card is None
                or opportunity is None
                or card.run_id != context.run_id
                or card.opportunity_id != opportunity_id
                or opportunity.canonical_problem_id != problem_id
            ):
                raise ValueError("problem hypothesis card link has invalid persisted lineage")
            await _add_or_verify(
                session,
                models.ProblemHypothesis,
                identifier,
                {
                    "canonical_problem_id": problem_id,
                    "evidence_card_id": card_id,
                    "icp": hypothesis.icp,
                    "jtbd": hypothesis.job_to_be_done,
                    "trigger": hypothesis.trigger,
                    "current_behavior": hypothesis.current_behavior,
                    "pain": hypothesis.pain,
                    "workflow_failure": hypothesis.workflow_failure,
                    "falsifier": hypothesis.falsification_test,
                    "supporting_claim_ids": sorted(supporting),
                    "contradicting_claim_ids": sorted(contradicting),
                },
                "problem hypothesis",
            )
        await session.flush()

    async def _persist_critic(
        self,
        session: AsyncSession,
        context: ArtifactContext,
        payload: dict[str, Any],
        *,
        round_number: int = 1,
    ) -> None:
        _require_keys("CRITIC", payload, {"results"})
        results = tuple(domain.CriticResult.model_validate(item) for item in payload["results"])
        call_id = uuid5(
            context.run_id,
            f"{context.task_id}:CRITIC:{round_number}:{context.task_attempt}",
        )
        call = await session.get(models.AgentCall, call_id)
        if call is not None and call.status == "INVALID_OUTPUT":
            repair_call_id = uuid5(call_id, "repair:1")
            repair_call = await session.get(models.AgentCall, repair_call_id)
            if repair_call is not None and repair_call.status == "COMPLETED":
                call_id, call = repair_call_id, repair_call
        if (
            call is None
            or call.run_id != context.run_id
            or call.operation != "critic"
            or call.status != "COMPLETED"
        ):
            raise ValueError("critic result has no matching provider audit")
        for result in results:
            opportunity_id = _uuid_text(result.opportunity_id, "opportunity")
            assessment = await _assessment(session, context, opportunity_id)
            identifier = uuid5(
                context.run_id,
                f"critic:{round_number}:{opportunity_id}",
            )
            await _add_or_verify(
                session,
                models.CriticResult,
                identifier,
                {
                    "assessment_id": assessment.id,
                    "run_id": context.run_id,
                    "agent_call_id": call_id,
                    "verdict": result.verdict.value,
                    "confidence": Decimal(str(result.confidence)),
                    "fatal_flags": list(result.fatal_flags),
                    "weak_assumptions": list(result.weak_assumptions),
                    "contradictions": list(result.contradictions),
                    "missing_evidence": list(result.missing_evidence),
                    "recommended_intents": [
                        item.model_dump(mode="json") for item in result.recommended_intents
                    ],
                    "summary": result.summary,
                },
                "critic result",
            )
        await session.flush()

    async def _persist_final(
        self,
        session: AsyncSession,
        context: ArtifactContext,
        payload: dict[str, Any],
    ) -> None:
        _require_keys("FINAL", payload, {"decisions"})
        decisions = payload["decisions"]
        if not isinstance(decisions, list):
            raise ValueError("FINAL decisions must be a list")
        for raw in decisions:
            if not isinstance(raw, dict) or set(raw) != {
                "opportunity_id",
                "verdict",
                "gates",
                "round_number",
                "evidence_card_id",
                "score_snapshot_id",
                "critic_result_id",
                "gap_hypothesis_id",
            }:
                raise ValueError("FINAL decision shape is invalid")
            round_number = raw["round_number"]
            if type(round_number) is not int or round_number not in {1, 2}:
                raise ValueError("FINAL round_number must be 1 or 2")
            card_id = _uuid_text(str(raw["evidence_card_id"]), "Evidence Card")
            score_id = _uuid_text(str(raw["score_snapshot_id"]), "score snapshot")
            critic_id = _uuid_text(str(raw["critic_result_id"]), "critic result")
            gap_id = _uuid_text(str(raw["gap_hypothesis_id"]), "gap hypothesis")
            card = await session.get(models.EvidenceCard, card_id)
            if card is None:
                raise ValueError("FINAL references unknown Evidence Card")
            opportunity_id = card.opportunity_id
            requested_opportunity_id = _uuid_text(str(raw["opportunity_id"]), "opportunity")
            if requested_opportunity_id != opportunity_id:
                raise ValueError("FINAL opportunity does not match its Evidence Card")
            assessment = await _assessment(session, context, opportunity_id)
            score, critic, gap = await _final_artifacts(
                session,
                context,
                assessment,
                card,
                score_id=score_id,
                critic_id=critic_id,
                gap_id=gap_id,
                round_number=round_number,
            )
            decision = await validation_from_storage(
                session,
                assessment=assessment,
                card=card,
                score=score,
                critic=critic,
                gap=gap,
                mission_revision_id=context.mission_revision.id,
            )
            verdict = decision.verdict
            if str(raw["verdict"]) != verdict.value:
                raise ValueError("FINAL verdict disagrees with persisted validation artifacts")
            gates = [
                {
                    "name": gate.name,
                    "passed": gate.passed,
                    "actual": gate.actual,
                    "required": gate.required,
                }
                for gate in decision.gates
            ]
            snapshot_id = uuid5(
                context.run_id,
                f"final:{round_number}:{opportunity_id}",
            )
            await _add_or_verify(
                session,
                models.FinalAssessmentSnapshot,
                snapshot_id,
                {
                    "assessment_id": assessment.id,
                    "run_id": context.run_id,
                    "gap_hypothesis_id": gap_id,
                    "evidence_card_id": card_id,
                    "score_snapshot_id": score_id,
                    "critic_result_id": critic_id,
                    "round_number": round_number,
                    "verdict": verdict.value,
                    "competitor_research_status": assessment.competitor_research_status,
                    "gates": gates,
                },
                "final assessment snapshot",
            )
            await session.flush()
            if assessment.lifecycle_status == "VALIDATE":
                if verdict is not domain.Verdict.VALIDATE:
                    raise ValueError(
                        "terminal VALIDATE assessment cannot be changed by a later run"
                    )
                continue
            target = {
                domain.Verdict.REJECT: "REJECTED",
                domain.Verdict.RESEARCH_MORE: "RESEARCH_MORE",
                domain.Verdict.VALIDATE: "VALIDATE",
            }[verdict]
            event_id = uuid5(
                context.run_id,
                f"lifecycle:{round_number}:{opportunity_id}:{target}",
            )
            existing = await session.get(models.LifecycleEvent, event_id)
            if existing is not None:
                continue
            previous = assessment.lifecycle_status
            transition_lifecycle(
                domain.LifecycleState(previous),
                domain.LifecycleState(target),
                validation_passed=decision.passed,
            )
            event_number = await _next_lifecycle_number(session, assessment.id)
            session.add(
                models.LifecycleEvent(
                    id=event_id,
                    assessment_id=assessment.id,
                    event_number=event_number,
                    run_id=context.run_id,
                    gap_hypothesis_id=gap_id,
                    from_status=previous,
                    to_status=target,
                    reason="deterministic final gate decision",
                    details={
                        "round_number": round_number,
                        "evidence_card_id": str(card_id),
                        "score_snapshot_id": str(score_id),
                        "critic_result_id": str(critic_id),
                        "gap_hypothesis_id": str(gap_id),
                        "gates": gates,
                        "verdict": verdict.value,
                    },
                    created_at=context.collection_until,
                )
            )
            await session.flush()
            await session.refresh(assessment)
        await session.flush()


def _require_keys(stage: str, payload: Mapping[str, Any], expected: set[str]) -> None:
    unknown = set(payload) - expected
    missing = expected - set(payload)
    if unknown:
        raise ValueError(f"unknown {stage} payload keys: {sorted(unknown)}")
    if missing:
        raise ValueError(f"missing {stage} payload keys: {sorted(missing)}")


def _require_unique_ids(label: str, artifacts: tuple[Any, ...]) -> None:
    identifiers = [item.id for item in artifacts]
    if len(identifiers) != len(set(identifiers)):
        raise ValueError(f"{label} IDs must be unique")


def _uuid_text(value: str, label: str) -> UUID:
    try:
        parsed = UUID(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} ID must be a canonical UUID") from exc
    if value != str(parsed):
        raise ValueError(f"{label} ID must be a canonical UUID")
    return parsed


def _is_uuid(value: str) -> bool:
    try:
        return value == str(UUID(value))
    except ValueError:
        return False


async def _raw_revisions_by_domain_id(
    session: AsyncSession, identifiers: set[str]
) -> dict[str, models.RawSignalRevision]:
    if not identifiers:
        return {}
    rows = tuple(
        await session.scalars(
            select(models.RawSignalRevision).where(
                models.RawSignalRevision.domain_revision_id.in_(identifiers)
            )
        )
    )
    return {item.domain_revision_id: item for item in rows}


async def _add_or_verify(
    session: AsyncSession,
    model: type[Any],
    identifier: UUID,
    values: dict[str, Any],
    label: str,
) -> Any:
    existing = await session.get(model, identifier)
    if existing is None:
        entity = model(id=identifier, **values)
        session.add(entity)
        return entity
    for name, value in values.items():
        if getattr(existing, name) != value:
            raise ValueError(f"{label} ID was reused with different content")
    return existing


async def _competitor_status(session: AsyncSession, task_id: UUID) -> str:
    task = await session.get(models.ResearchTask, task_id)
    if task is None:
        raise ValueError("artifact context references unknown research task")
    pipeline = task.checkpoint.get("pipeline")
    stages = pipeline.get("stages") if isinstance(pipeline, dict) else None
    stage = stages.get("COMPETITOR_RESEARCH") if isinstance(stages, dict) else None
    payload = stage.get("payload") if isinstance(stage, dict) else None
    status = payload.get("status") if isinstance(payload, dict) else None
    if status not in {"COMPLETE", "INCOMPLETE", "RESEARCH_UNAVAILABLE"}:
        raise ValueError("competitor research status is not durable")
    return cast(str, status)


async def _assessment(
    session: AsyncSession,
    context: ArtifactContext,
    opportunity_id: UUID,
) -> models.MissionOpportunityAssessment:
    row = await session.scalar(
        select(models.MissionOpportunityAssessment)
        .where(
            models.MissionOpportunityAssessment.mission_revision_id == context.mission_revision.id,
            models.MissionOpportunityAssessment.opportunity_id == opportunity_id,
        )
        .with_for_update()
    )
    if row is None:
        raise ValueError("artifact references unknown mission opportunity assessment")
    return row


def _hypothesis_card_links(raw: object) -> dict[UUID, tuple[UUID, UUID]]:
    if not isinstance(raw, list):
        raise ValueError("hypothesis_card_links must be a list")
    result: dict[UUID, tuple[UUID, UUID]] = {}
    for item in raw:
        if not isinstance(item, dict) or set(item) != {
            "schema_version",
            "hypothesis_id",
            "opportunity_id",
            "evidence_card_id",
        }:
            raise ValueError("hypothesis card link shape is invalid")
        if item["schema_version"] != "0.1":
            raise ValueError("hypothesis card link schema version is invalid")
        hypothesis_id = _uuid_text(str(item["hypothesis_id"]), "problem hypothesis")
        if hypothesis_id in result:
            raise ValueError("hypothesis card link IDs must be unique")
        result[hypothesis_id] = (
            _uuid_text(str(item["opportunity_id"]), "opportunity"),
            _uuid_text(str(item["evidence_card_id"]), "Evidence Card"),
        )
    return result


async def _maybe_reopen_rejected(
    session: AsyncSession,
    context: ArtifactContext,
    current_card_id: UUID,
) -> None:
    current = await session.get(models.EvidenceCard, current_card_id)
    if current is None:
        raise ValueError("R2 reopen requires its persisted Evidence Card")
    assessment = await _assessment(session, context, current.opportunity_id)
    if assessment.lifecycle_status != "REJECTED":
        return
    if assessment.rejected_at is None:
        raise ValueError("rejected assessment is missing its rejection timestamp")
    previous_id = uuid5(context.run_id, f"card:{current.opportunity_id}")
    previous = await session.get(models.EvidenceCard, previous_id)
    if previous is None or previous.run_id != context.run_id:
        raise ValueError("R2 reopen requires the exact R1 Evidence Card")
    current_authors = set(current.metrics.get("known_author_ids", []))
    previous_authors = set(previous.metrics.get("known_author_ids", []))
    current_sources = set(current.metrics.get("user_sources", []))
    previous_sources = set(previous.metrics.get("user_sources", []))
    current_paid = current.metrics.get("paid_or_wtp_signals")
    previous_paid = previous.metrics.get("paid_or_wtp_signals")
    if not isinstance(current_paid, int) or not isinstance(previous_paid, int):
        raise ValueError("Evidence Card reopen metrics are malformed")
    evidence = ReopenEvidence(
        new_independent_users=len(current_authors - previous_authors),
        new_source=bool(current_sources - previous_sources),
        first_wtp_or_spend=previous_paid == 0 and current_paid > 0,
    )
    if not can_reopen_rejected(
        rejected_at=assessment.rejected_at,
        now=context.collection_until,
        evidence=evidence,
    ):
        return
    event_id = uuid5(
        context.run_id,
        f"lifecycle:2:{current.opportunity_id}:REOPEN",
    )
    if await session.get(models.LifecycleEvent, event_id) is not None:
        return
    session.add(
        models.LifecycleEvent(
            id=event_id,
            assessment_id=assessment.id,
            event_number=await _next_lifecycle_number(session, assessment.id),
            run_id=context.run_id,
            gap_hypothesis_id=await _checkpoint_gap_id(
                session,
                context.task_id,
                current.opportunity_id,
            ),
            from_status="REJECTED",
            to_status="RESEARCH_MORE",
            reason="new independent evidence passed rejected-opportunity reopen gate",
            details={
                "round_number": 2,
                "evidence_card_id": str(current.id),
                "new_independent_users": evidence.new_independent_users,
                "new_source": evidence.new_source,
                "first_wtp_or_spend": evidence.first_wtp_or_spend,
            },
            created_at=context.collection_until,
        )
    )
    await session.flush()
    await session.refresh(assessment)


def _validate_hypothesis_id(
    hypothesis: domain.ProblemHypothesis,
    *,
    opportunity_id: UUID,
    evidence_card_id: UUID,
    identifier: UUID,
) -> None:
    identity = [
        "problem-hypothesis",
        str(opportunity_id),
        str(evidence_card_id),
        hypothesis.canonical_problem_id,
        normalize_text(hypothesis.icp),
        normalize_text(hypothesis.job_to_be_done),
        normalize_text(hypothesis.trigger),
        normalize_text(hypothesis.current_behavior),
        normalize_text(hypothesis.pain),
        normalize_text(hypothesis.workflow_failure),
        normalize_text(hypothesis.falsification_test),
        sorted(hypothesis.supporting_claim_ids),
        sorted(hypothesis.contradicting_claim_ids),
    ]
    canonical = json.dumps(
        identity,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    if identifier != uuid5(_ARTIFACT_NAMESPACE, canonical):
        raise ValueError("problem hypothesis ID does not bind its explicit card lineage")


async def _next_lifecycle_number(session: AsyncSession, assessment_id: UUID) -> int:
    latest = await session.scalar(
        select(func.max(models.LifecycleEvent.event_number)).where(
            models.LifecycleEvent.assessment_id == assessment_id
        )
    )
    return (latest or 0) + 1


async def _final_artifacts(
    session: AsyncSession,
    context: ArtifactContext,
    assessment: models.MissionOpportunityAssessment,
    card: models.EvidenceCard,
    *,
    score_id: UUID,
    critic_id: UUID,
    gap_id: UUID,
    round_number: int,
) -> tuple[
    models.OpportunityScoreSnapshot,
    models.CriticResult,
    models.GapHypothesis,
]:
    score = await session.get(models.OpportunityScoreSnapshot, score_id)
    critic = await session.get(models.CriticResult, critic_id)
    gap = await session.get(models.GapHypothesis, gap_id)
    opportunity = await session.get(models.Opportunity, assessment.opportunity_id)
    checkpoint_gap_id = await _checkpoint_gap_id(
        session,
        context.task_id,
        assessment.opportunity_id,
        round_number=round_number,
    )
    expected_critic_id = uuid5(
        context.run_id,
        f"critic:{round_number}:{assessment.opportunity_id}",
    )
    if (
        card.run_id != context.run_id
        or card.opportunity_id != assessment.opportunity_id
        or score is None
        or score.assessment_id != assessment.id
        or score.run_id != context.run_id
        or critic is None
        or critic.id != expected_critic_id
        or critic.assessment_id != assessment.id
        or critic.run_id != context.run_id
        or gap is None
        or opportunity is None
        or gap.id != checkpoint_gap_id
        or gap.canonical_problem_id != opportunity.canonical_problem_id
    ):
        raise ValueError("FINAL artifact IDs do not form one run/opportunity snapshot")
    return score, critic, gap


async def validation_from_storage(
    session: AsyncSession,
    *,
    assessment: models.MissionOpportunityAssessment,
    card: models.EvidenceCard,
    score: models.OpportunityScoreSnapshot,
    critic: models.CriticResult,
    gap: models.GapHypothesis,
    mission_revision_id: UUID,
    competitor_research_status: str | None = None,
) -> ValidationDecision:
    revisions = {
        item.id: item.domain_revision_id
        for item in await session.scalars(
            select(models.RawSignalRevision).where(
                models.RawSignalRevision.id.in_(card.representative_signal_ids)
            )
        )
    }
    requested_claim_ids = {
        *card.supporting_claim_ids,
        *card.contradicting_claim_ids,
    }
    claims = {
        item: str(item)
        for item in await session.scalars(
            select(models.AtomicClaim.id).where(models.AtomicClaim.id.in_(requested_claim_ids))
        )
    }
    typed_card = evidence_card_from_storage(
        card,
        revision_identifiers=revisions,
        claim_identifiers=claims,
    )
    typed_score = score_snapshot_from_storage(
        score,
        opportunity_id=assessment.opportunity_id,
        mission_revision_id=mission_revision_id,
    )
    typed_critic = domain.CriticResult(
        opportunity_id=str(assessment.opportunity_id),
        verdict=critic.verdict,
        confidence=float(critic.confidence),
        fatal_flags=tuple(critic.fatal_flags),
        weak_assumptions=tuple(critic.weak_assumptions),
        contradictions=tuple(critic.contradictions),
        missing_evidence=tuple(critic.missing_evidence),
        recommended_intents=tuple(critic.recommended_intents),
        summary=critic.summary,
    )
    gap_evidence_present = False
    if gap.user_evidence_ids and gap.competitor_evidence_ids:
        known_user = set(
            await session.scalars(
                select(models.RawSignalRevision.id).where(
                    models.RawSignalRevision.id.in_(gap.user_evidence_ids)
                )
            )
        )
        known_competitor = set(
            await session.scalars(
                select(models.CompetitorEvidence.id).where(
                    models.CompetitorEvidence.id.in_(gap.competitor_evidence_ids)
                )
            )
        )
        gap_evidence_present = known_user == set(gap.user_evidence_ids) and known_competitor == set(
            gap.competitor_evidence_ids
        )
    return validation_decision(
        card=typed_card,
        score=typed_score,
        competitor_research=domain.CompetitorResearchStatus(
            competitor_research_status or assessment.competitor_research_status
        ),
        gap_evidence_present=gap_evidence_present,
        critic=typed_critic,
    )


async def _checkpoint_gap_id(
    session: AsyncSession,
    task_id: UUID,
    opportunity_id: UUID,
    *,
    round_number: int = 2,
) -> UUID:
    task = await session.get(models.ResearchTask, task_id)
    if task is None:
        raise ValueError("artifact context references unknown research task")
    pipeline = task.checkpoint.get("pipeline")
    stages = pipeline.get("stages") if isinstance(pipeline, dict) else None
    if not isinstance(stages, dict):
        raise ValueError("research task has no durable pipeline stages")
    stage_name = "GAP" if round_number == 1 else "GAP_R2"
    stage = stages.get(stage_name)
    payload = stage.get("payload") if isinstance(stage, dict) else None
    opportunities = payload.get("opportunities") if isinstance(payload, dict) else None
    if isinstance(opportunities, list):
        for raw in opportunities:
            if not isinstance(raw, dict) or str(raw.get("id")) != str(opportunity_id):
                continue
            return _uuid_text(str(raw.get("gap_hypothesis_id")), "gap hypothesis")
    raise ValueError("no durable gap snapshot exists for opportunity reopen")
