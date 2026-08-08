from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from pydantic import ValidationError

from gapforge.analysis.critic import build_critic_input, research_more_intents, validate_critic_result
from gapforge.analysis.hypotheses import validate_alternative_coverage, validate_gap_hypothesis, validate_problem_hypothesis
from gapforge.analysis.lifecycle import ReopenEvidence, TrendObservation, can_reopen_rejected, classify_trend, transition_lifecycle
from gapforge.domain.contracts import (
    AtomicClaim,
    AlternativeKind,
    ClaimKind,
    CompetitorResearchStatus,
    Competitor,
    CriticInput,
    CriticResult,
    EpistemicStatus,
    EvidenceCard,
    GapHypothesis,
    GapType,
    LifecycleState,
    ProblemHypothesis,
    QueryIntent,
    QueryIntentKind,
    Source,
    TrendLabel,
    Verdict,
)
from gapforge.scoring.engine import ScoringInputs, append_score_snapshot, score_opportunity, validation_decision

NOW = datetime(2026, 8, 9, tzinfo=UTC)


def score(value: float = 100):
    inputs = ScoringInputs(
        severity=value, frequency=value, independent_diversity=value, behavioral_workaround=value,
        wtp_or_spend=value, recency_trend=value, gap_strength=value, competitor_dissatisfaction=value,
        reachability=value, technical_feasibility=value, small_team_feasibility=value,
        inverse_switching_friction=value, why_now=value,
    )
    return score_opportunity(
        snapshot_id="score-1", opportunity_id="o-1", mission_revision_id=uuid4(),
        inputs=inputs, evidence_confidence=0.9, created_at=NOW,
    )


def card() -> EvidenceCard:
    return EvidenceCard(
        id="card-1", opportunity_id="o-1", known_author_ids=tuple(f"a-{i}" for i in range(5)),
        thread_ids=("t-1", "t-2", "t-3"), user_sources=(Source.GITHUB, Source.REDDIT),
        observed_days=(NOW,), severity=0.8, behavioral_workarounds=1, paid_or_wtp_signals=1,
        confidence=0.9,
    )


def critic(**updates: object) -> CriticResult:
    base: dict[str, object] = {
        "opportunity_id": "o-1", "verdict": Verdict.VALIDATE, "confidence": 0.9, "summary": "Evidence clears gates",
    }
    base.update(updates)
    return CriticResult.model_validate(base)


def test_geometric_mean_penalties_explanation_and_append_only_history() -> None:
    asymmetric = ScoringInputs(
        severity=100, frequency=100, independent_diversity=100, behavioral_workaround=100,
        wtp_or_spend=100, recency_trend=100, gap_strength=25, competitor_dissatisfaction=25,
        reachability=25, technical_feasibility=25, small_team_feasibility=25,
        inverse_switching_friction=25, why_now=25, penalties=(("concentration", 5),),
    )
    snapshot = score_opportunity(
        snapshot_id="s-1", opportunity_id="o-1", mission_revision_id=uuid4(), inputs=asymmetric,
        evidence_confidence=0.8, created_at=NOW,
    )
    assert snapshot.pre_penalty_score == 50
    assert snapshot.final_score == 45
    assert "geometric_mean" in snapshot.explanation[2]
    history = append_score_snapshot((), snapshot)
    with pytest.raises(ValueError, match="append-only"):
        append_score_snapshot(history, snapshot)


@pytest.mark.parametrize(
    ("mutation", "failed_gate"),
    [
        ({"card": None}, "evidence_card"),
        ({"card": card().model_copy(update={"known_author_ids": ("a",)})}, "known_authors"),
        ({"card": card().model_copy(update={"thread_ids": ("t",)})}, "independent_threads"),
        ({"card": card().model_copy(update={"user_sources": (Source.GITHUB,)})}, "user_sources"),
        ({"card": card().model_copy(update={"behavioral_workarounds": 0})}, "behavioral_workaround"),
        ({"card": card().model_copy(update={"paid_or_wtp_signals": 0})}, "wtp_or_spend"),
        ({"competitor_research": CompetitorResearchStatus.INCOMPLETE}, "competitor_research"),
        ({"gap_evidence_present": False}, "gap_evidence"),
        ({"critic": critic(fatal_flags=("fatal",))}, "fatal_flags"),
        ({"score": score(60)}, "overall_score"),
        ({"card": card().model_copy(update={"confidence": 0.5})}, "evidence_confidence"),
        ({"critic": critic(verdict=Verdict.RESEARCH_MORE)}, "critic_verdict"),
        ({"critic": critic(confidence=0.6)}, "critic_confidence"),
    ],
)
def test_each_validate_hard_gate_fails_independently(mutation: dict[str, object], failed_gate: str) -> None:
    arguments: dict[str, object] = {
        "card": card(), "score": score(), "competitor_research": CompetitorResearchStatus.COMPLETE,
        "gap_evidence_present": True, "critic": critic(),
    }
    arguments.update(mutation)
    decision = validation_decision(**arguments)  # type: ignore[arg-type]
    assert decision.verdict is not Verdict.VALIDATE
    assert not next(gate for gate in decision.gates if gate.name == failed_gate).passed


def hypothesis() -> ProblemHypothesis:
    return ProblemHypothesis(
        id="p-1", canonical_problem_id="cp-1", icp="accountants", job_to_be_done="reconcile invoices",
        trigger="month end", current_behavior="manual CSV exports", pain="takes a day",
        workflow_failure="systems do not reconcile", falsification_test="Fails if fewer than five users confirm",
        supporting_claim_ids=("c-1",),
    )


def gap() -> GapHypothesis:
    return GapHypothesis(
        id="g-1", canonical_problem_id="cp-1", gap_type=GapType.WORKFLOW, statement="No end-to-end reconciliation",
        user_evidence_ids=("e-1",), competitor_evidence_ids=("e-2",),
    )


def test_critic_input_is_blind_schema_and_citations_are_allowlisted() -> None:
    claim = AtomicClaim(id="c-1", text="Users export CSV", kind=ClaimKind.COMPETITOR, status=EpistemicStatus.HYPOTHESIS)
    blind = build_critic_input(
        opportunity_id="o-1", problem_hypothesis=hypothesis(), evidence_card=card(), gap_hypothesis=gap(),
        competitor_claims=(claim,), permitted_claim_ids=("c-1",),
    )
    assert "verdict" not in blind.model_dump()
    assert "product_hypothesis" not in blind.model_dump()
    with pytest.raises(ValidationError):
        CriticInput.model_validate({**blind.model_dump(), "prior_verdict": "VALIDATE"})
    with pytest.raises(ValueError, match="unpermitted"):
        validate_critic_result(critic(contradictions=("invented",)), frozenset({"c-1"}))


def test_hypotheses_are_falsifiable_allowlisted_and_cover_nonsoftware_alternatives() -> None:
    validate_problem_hypothesis(hypothesis(), frozenset({"c-1"}))
    vague = hypothesis().model_copy(update={"falsification_test": "Ask whether people like it"})
    with pytest.raises(ValueError, match="falsifiable"):
        validate_problem_hypothesis(vague, frozenset({"c-1"}))
    alternatives = (
        Competitor(id="c-1", name="Spreadsheet", kind=AlternativeKind.SPREADSHEET),
        Competitor(id="c-2", name="Keep current process", kind=AlternativeKind.DO_NOTHING),
    )
    validate_alternative_coverage(alternatives)
    with pytest.raises(ValueError, match="doing nothing"):
        validate_alternative_coverage(alternatives[:1])
    validate_gap_hypothesis(
        gap(), permitted_user_evidence=frozenset({"e-1"}), permitted_competitor_evidence=frozenset({"e-2"}),
    )
    with pytest.raises(ValueError, match="competitor"):
        validate_gap_hypothesis(
            gap(), permitted_user_evidence=frozenset({"e-1"}), permitted_competitor_evidence=frozenset(),
        )


def test_research_more_respects_round_and_call_budgets() -> None:
    intents = tuple(
        QueryIntent(id=f"q-{i}", kind=QueryIntentKind.TARGETED, concept="pain", sources=(Source.GITHUB,), rationale="gap")
        for i in range(4)
    )
    result = critic(verdict=Verdict.RESEARCH_MORE, recommended_intents=intents)
    assert len(research_more_intents(result, completed_rounds=1, remaining_agent_calls=2)) == 2
    assert research_more_intents(result, completed_rounds=2, remaining_agent_calls=6) == ()


def test_trend_requires_five_signals_three_authors_two_threads_and_volume_normalization() -> None:
    viral = tuple(TrendObservation(f"s-{i}", f"a-{i % 4}", "one-thread", NOW - timedelta(days=1)) for i in range(10))
    assert classify_trend(viral, as_of=NOW, current_examined_volume=100, previous_examined_volume=100).label is TrendLabel.INSUFFICIENT_DATA
    diverse = tuple(TrendObservation(f"s-{i}", f"a-{i % 3}", f"t-{i % 2}", NOW - timedelta(days=1)) for i in range(6))
    result = classify_trend(diverse, as_of=NOW, current_examined_volume=20, previous_examined_volume=100)
    assert result.label is TrendLabel.RISING


def test_reopen_cooldown_wtp_exception_and_lifecycle_guard() -> None:
    rejected = NOW - timedelta(days=10)
    assert not can_reopen_rejected(rejected_at=rejected, now=NOW, evidence=ReopenEvidence(new_independent_users=3))
    assert can_reopen_rejected(rejected_at=rejected, now=NOW, evidence=ReopenEvidence(first_wtp_or_spend=True))
    assert transition_lifecycle(LifecycleState.REJECTED, LifecycleState.RESEARCH_MORE, reopen_allowed=True) is LifecycleState.RESEARCH_MORE
    with pytest.raises(ValueError):
        transition_lifecycle(LifecycleState.REJECTED, LifecycleState.VALIDATE, reopen_allowed=True)
    with pytest.raises(ValueError, match="validation gates"):
        transition_lifecycle(LifecycleState.RESEARCHING, LifecycleState.VALIDATE)
    assert transition_lifecycle(
        LifecycleState.RESEARCHING, LifecycleState.VALIDATE, validation_passed=True
    ) is LifecycleState.VALIDATE
