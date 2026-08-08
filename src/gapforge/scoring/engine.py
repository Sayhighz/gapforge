"""Explainable two-axis opportunity scoring and non-bypassable validation gates."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from gapforge.domain.contracts import (
    CompetitorResearchStatus,
    CriticResult,
    EvidenceCard,
    OpportunityScoreSnapshot,
    ScoreComponents,
    Verdict,
)

EVIDENCE_WEIGHTS = {
    "severity": 0.20,
    "frequency": 0.15,
    "independent_diversity": 0.20,
    "behavioral_workaround": 0.15,
    "wtp_or_spend": 0.15,
    "recency_trend": 0.15,
}
FIT_WEIGHTS = {
    "gap_strength": 0.20,
    "competitor_dissatisfaction": 0.15,
    "reachability": 0.15,
    "technical_feasibility": 0.15,
    "small_team_feasibility": 0.10,
    "inverse_switching_friction": 0.10,
    "why_now": 0.15,
}


@dataclass(frozen=True, slots=True)
class ScoringInputs:
    severity: float
    frequency: float
    independent_diversity: float
    behavioral_workaround: float
    wtp_or_spend: float
    recency_trend: float
    gap_strength: float
    competitor_dissatisfaction: float
    reachability: float
    technical_feasibility: float
    small_team_feasibility: float
    inverse_switching_friction: float
    why_now: float
    penalties: tuple[tuple[str, float], ...] = ()

    def __post_init__(self) -> None:
        values = (*self.evidence_values().values(), *self.fit_values().values())
        if any(not 0 <= value <= 100 for value in values):
            raise ValueError("score inputs must be between 0 and 100")
        if any(not 0 <= value <= 100 for _, value in self.penalties):
            raise ValueError("penalties must be between 0 and 100")
        if len({name for name, _ in self.penalties}) != len(self.penalties):
            raise ValueError("penalty names must be unique")

    def evidence_values(self) -> dict[str, float]:
        return {name: float(getattr(self, name)) for name in EVIDENCE_WEIGHTS}

    def fit_values(self) -> dict[str, float]:
        return {name: float(getattr(self, name)) for name in FIT_WEIGHTS}


def weighted_axis(components: ScoreComponents) -> float:
    return sum(
        components.values[name] * components.weights[name] for name in components.values
    )


def score_opportunity(
    *,
    snapshot_id: str,
    opportunity_id: str,
    mission_revision_id: UUID,
    inputs: ScoringInputs,
    evidence_confidence: float,
    created_at: datetime,
) -> OpportunityScoreSnapshot:
    evidence = ScoreComponents(
        values=inputs.evidence_values(), weights=EVIDENCE_WEIGHTS
    )
    fit = ScoreComponents(values=inputs.fit_values(), weights=FIT_WEIGHTS)
    evidence_axis, fit_axis = weighted_axis(evidence), weighted_axis(fit)
    pre_penalty = math.sqrt(evidence_axis * fit_axis)
    penalties = dict(inputs.penalties)
    final = max(0.0, pre_penalty - sum(penalties.values()))
    raw_metrics = {**inputs.evidence_values(), **inputs.fit_values()}
    explanation = (
        f"EvidenceStrength={evidence_axis:.2f}",
        f"OpportunityFit={fit_axis:.2f}",
        f"geometric_mean=sqrt({evidence_axis:.2f}*{fit_axis:.2f})={pre_penalty:.2f}",
        f"penalties={sum(penalties.values()):.2f}",
    )
    return OpportunityScoreSnapshot(
        id=snapshot_id,
        opportunity_id=opportunity_id,
        mission_revision_id=mission_revision_id,
        evidence_strength=evidence,
        opportunity_fit=fit,
        raw_metrics=raw_metrics,
        penalties=penalties,
        pre_penalty_score=round(pre_penalty, 6),
        final_score=round(final, 6),
        evidence_confidence=evidence_confidence,
        explanation=explanation,
        created_at=created_at,
    )


def append_score_snapshot(
    history: tuple[OpportunityScoreSnapshot, ...],
    snapshot: OpportunityScoreSnapshot,
) -> tuple[OpportunityScoreSnapshot, ...]:
    if any(item.id == snapshot.id for item in history):
        raise ValueError("score snapshots are append-only and IDs cannot be reused")
    if history and snapshot.created_at < history[-1].created_at:
        raise ValueError("score snapshots must be chronological")
    return (*history, snapshot)


@dataclass(frozen=True, slots=True)
class GateResult:
    name: str
    passed: bool
    actual: str
    required: str


@dataclass(frozen=True, slots=True)
class ValidationDecision:
    verdict: Verdict
    gates: tuple[GateResult, ...]

    @property
    def passed(self) -> bool:
        return all(gate.passed for gate in self.gates)


def validation_decision(
    *,
    card: EvidenceCard | None,
    score: OpportunityScoreSnapshot,
    competitor_research: CompetitorResearchStatus,
    gap_evidence_present: bool,
    critic: CriticResult,
) -> ValidationDecision:
    artifact_ids = {score.opportunity_id, critic.opportunity_id}
    if card is not None:
        artifact_ids.add(card.opportunity_id)
    if len(artifact_ids) != 1:
        raise ValueError("validation artifacts belong to different opportunities")
    author_count = len(card.known_author_ids) if card else 0
    thread_count = len(card.thread_ids) if card else 0
    source_count = len(card.user_sources) if card else 0
    workaround_count = card.behavioral_workarounds if card else 0
    paid_count = card.paid_or_wtp_signals if card else 0
    confidence = score.evidence_confidence
    confidence_consistent = card is not None and math.isclose(
        card.confidence, confidence, abs_tol=1e-9
    )
    gates = (
        GateResult("evidence_card", card is not None, str(card is not None), "present"),
        GateResult("known_authors", author_count >= 5, str(author_count), ">=5"),
        GateResult("independent_threads", thread_count >= 3, str(thread_count), ">=3"),
        GateResult("user_sources", source_count >= 2, str(source_count), ">=2"),
        GateResult(
            "behavioral_workaround", workaround_count >= 1, str(workaround_count), ">=1"
        ),
        GateResult("wtp_or_spend", paid_count >= 1, str(paid_count), ">=1"),
        GateResult(
            "competitor_research",
            competitor_research is CompetitorResearchStatus.COMPLETE,
            competitor_research.value,
            "COMPLETE",
        ),
        GateResult(
            "gap_evidence", gap_evidence_present, str(gap_evidence_present), "present"
        ),
        GateResult(
            "fatal_flags",
            len(critic.fatal_flags) == 0,
            str(len(critic.fatal_flags)),
            "0",
        ),
        GateResult(
            "overall_score", score.final_score >= 70, f"{score.final_score:.2f}", ">=70"
        ),
        GateResult(
            "evidence_confidence_consistency",
            confidence_consistent,
            str(confidence_consistent),
            "card=snapshot",
        ),
        GateResult(
            "evidence_confidence", confidence >= 0.65, f"{confidence:.2f}", ">=0.65"
        ),
        GateResult(
            "critic_verdict",
            critic.verdict is Verdict.VALIDATE,
            critic.verdict.value,
            "VALIDATE",
        ),
        GateResult(
            "critic_confidence",
            critic.confidence >= 0.70,
            f"{critic.confidence:.2f}",
            ">=0.70",
        ),
    )
    if all(gate.passed for gate in gates):
        verdict = Verdict.VALIDATE
    elif critic.verdict is Verdict.REJECT:
        verdict = Verdict.REJECT
    else:
        verdict = Verdict.RESEARCH_MORE
    return ValidationDecision(verdict, gates)
