"""Problem/gap falsifiability and alternative-coverage validation."""

from __future__ import annotations

from datetime import datetime

from gapforge.domain.contracts import (
    AlternativeKind,
    Competitor,
    GapHypothesis,
    MissionOpportunityAssessment,
    ProblemHypothesis,
    ProductHypothesis,
    Verdict,
)

NON_SOFTWARE = {
    AlternativeKind.SPREADSHEET,
    AlternativeKind.MANUAL_WORK,
    AlternativeKind.EMPLOYEE,
    AlternativeKind.AGENCY,
    AlternativeKind.INTERNAL_SCRIPT,
    AlternativeKind.DO_NOTHING,
}


def validate_problem_hypothesis(
    hypothesis: ProblemHypothesis, permitted_claim_ids: frozenset[str]
) -> None:
    invented = (
        set(hypothesis.supporting_claim_ids) | set(hypothesis.contradicting_claim_ids)
    ) - permitted_claim_ids
    if invented:
        raise ValueError("problem hypothesis references unpermitted claims")
    falsification = hypothesis.falsification_test.casefold()
    if not any(
        token in falsification
        for token in (
            "if ",
            "when ",
            "less than",
            "fewer than",
            "no more than",
            "fails",
        )
    ):
        raise ValueError("problem hypothesis requires a falsifiable condition")


def validate_alternative_coverage(competitors: tuple[Competitor, ...]) -> None:
    kinds = {competitor.kind for competitor in competitors}
    if AlternativeKind.DO_NOTHING not in kinds:
        raise ValueError("competitor set must include doing nothing")
    if not kinds.intersection(NON_SOFTWARE - {AlternativeKind.DO_NOTHING}):
        raise ValueError("competitor set must include a non-software alternative")


def validate_gap_hypothesis(
    hypothesis: GapHypothesis,
    *,
    permitted_user_evidence: frozenset[str],
    permitted_competitor_evidence: frozenset[str],
) -> None:
    if not set(hypothesis.user_evidence_ids) <= permitted_user_evidence:
        raise ValueError("gap references unpermitted user evidence")
    if not set(hypothesis.competitor_evidence_ids) <= permitted_competitor_evidence:
        raise ValueError("gap references unpermitted competitor evidence")


def create_product_hypothesis(
    *,
    hypothesis_id: str,
    assessment: MissionOpportunityAssessment,
    explicit_request_id: str,
    proposition: str,
    created_at: datetime,
) -> ProductHypothesis:
    if assessment.verdict is not Verdict.VALIDATE:
        raise ValueError("Product Hypothesis requires a VALIDATE assessment")
    if not explicit_request_id.strip():
        raise ValueError("Product Hypothesis requires an explicit request")
    return ProductHypothesis(
        id=hypothesis_id,
        assessment_id=assessment.id,
        explicit_request_id=explicit_request_id,
        proposition=proposition,
        created_at=created_at,
    )
