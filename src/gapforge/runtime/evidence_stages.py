"""Typed semantic stage outputs for the integrated evidence pipeline."""

from __future__ import annotations

from gapforge.domain.contracts import (
    AtomicClaim,
    CanonicalProblem,
    Competitor,
    CompetitorEvidence,
    CompetitorResearchStatus,
    Contract,
    CriticResult,
    GapHypothesis,
    Opportunity,
    PainSignal,
    ProblemCluster,
    ProblemClusterMembership,
    ProblemHypothesis,
)


class PainExtractionBatch(Contract):
    pain_signals: tuple[PainSignal, ...]


class ClusterBatch(Contract):
    problems: tuple[CanonicalProblem, ...]
    clusters: tuple[ProblemCluster, ...]
    memberships: tuple[ProblemClusterMembership, ...]


class ScoreInputContract(Contract):
    opportunity_id: str
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


class GapResearchBatch(Contract):
    claims: tuple[AtomicClaim, ...]
    competitors: tuple[Competitor, ...]
    competitor_evidence: tuple[CompetitorEvidence, ...]
    gaps: tuple[GapHypothesis, ...]
    opportunities: tuple[Opportunity, ...]
    score_inputs: tuple[ScoreInputContract, ...]
    competitor_research_status: CompetitorResearchStatus


class HypothesisBatch(Contract):
    hypotheses: tuple[ProblemHypothesis, ...]


class CriticBatch(Contract):
    results: tuple[CriticResult, ...]
