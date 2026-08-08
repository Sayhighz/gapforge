"""Blind critic construction, validation, and bounded research-more derivation."""

from __future__ import annotations

from gapforge.domain.contracts import (
    AtomicClaim,
    CriticInput,
    CriticResult,
    EvidenceCard,
    GapHypothesis,
    ProblemHypothesis,
    QueryIntent,
)


def build_critic_input(
    *,
    opportunity_id: str,
    problem_hypothesis: ProblemHypothesis,
    evidence_card: EvidenceCard,
    gap_hypothesis: GapHypothesis,
    competitor_claims: tuple[AtomicClaim, ...],
    permitted_claim_ids: tuple[str, ...],
) -> CriticInput:
    """Input intentionally has no prior verdict, product pitch, or Product Hypothesis."""
    return CriticInput(
        opportunity_id=opportunity_id,
        problem_hypothesis=problem_hypothesis,
        evidence_card=evidence_card,
        gap_hypothesis=gap_hypothesis,
        competitor_claims=competitor_claims,
        permitted_claim_ids=permitted_claim_ids,
    )


def validate_critic_result(
    result: CriticResult, permitted_claim_ids: frozenset[str]
) -> None:
    if not set(result.contradictions) <= permitted_claim_ids:
        raise ValueError("critic references unpermitted contradiction claims")


def research_more_intents(
    result: CriticResult, *, completed_rounds: int, remaining_agent_calls: int
) -> tuple[QueryIntent, ...]:
    if not 0 <= completed_rounds <= 2 or remaining_agent_calls < 0:
        raise ValueError("invalid research budget state")
    if completed_rounds >= 2 or remaining_agent_calls == 0:
        return ()
    return result.recommended_intents[: min(4, remaining_agent_calls)]
