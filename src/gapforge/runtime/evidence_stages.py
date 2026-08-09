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
    FetchSnapshot,
    GapHypothesis,
    Opportunity,
    PainSignal,
    ProblemCluster,
    ProblemClusterMembership,
    ProblemHypothesis,
    SearchResponse,
)


class PainExtractionBatch(Contract):
    pain_signals: tuple[PainSignal, ...]


class ClusterBatch(Contract):
    problems: tuple[CanonicalProblem, ...]
    clusters: tuple[ProblemCluster, ...]
    memberships: tuple[ProblemClusterMembership, ...]


class CompetitorResearchCheckpoint(Contract):
    status: CompetitorResearchStatus
    searches: tuple[SearchResponse, ...]
    snapshots: tuple[FetchSnapshot, ...]
    warning_codes: tuple[str, ...] = ()


class OpportunityFitContract(Contract):
    """Semantic fit judgments; Python derives every evidence-strength component."""

    opportunity_id: str
    gap_strength: float
    competitor_dissatisfaction: float
    reachability: float
    technical_feasibility: float
    small_team_feasibility: float
    inverse_switching_friction: float
    why_now: float
    penalties: tuple[tuple[str, float], ...] = ()
    explanation: tuple[str, ...] = ()


class GapResearchBatch(Contract):
    claims: tuple[AtomicClaim, ...]
    competitors: tuple[Competitor, ...]
    competitor_evidence: tuple[CompetitorEvidence, ...]
    gaps: tuple[GapHypothesis, ...]
    opportunities: tuple[Opportunity, ...]
    opportunity_fit: tuple[OpportunityFitContract, ...]


class HypothesisCardLink(Contract):
    hypothesis_id: str
    opportunity_id: str
    evidence_card_id: str


class HypothesisBatch(Contract):
    hypotheses: tuple[ProblemHypothesis, ...]
    hypothesis_card_links: tuple[HypothesisCardLink, ...]


class CriticBatch(Contract):
    results: tuple[CriticResult, ...]
