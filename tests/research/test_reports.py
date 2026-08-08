from datetime import UTC, datetime
from uuid import uuid4

import pytest

from gapforge.analysis.evidence import EvidenceRecord, EvidenceValidationError
from gapforge.domain.contracts import (
    AtomicClaim,
    ClaimKind,
    CompetitorResearchStatus,
    CriticResult,
    EpistemicStatus,
    EvidenceCard,
    RunStatus,
    Source,
    Verdict,
)
from gapforge.reports.renderers import (
    OpportunityReportData,
    ReportOpportunity,
    RunReportData,
    render_opportunity_report,
    render_run_report,
    run_report_paths,
    validated_claim_view,
    write_report_atomic,
)
from gapforge.scoring.engine import (
    ScoringInputs,
    score_opportunity,
    validation_decision,
)

NOW = datetime(2026, 8, 9, tzinfo=UTC)


def report_opportunity() -> tuple[ReportOpportunity, object]:
    evidence = EvidenceRecord(
        "e-1",
        Source.GITHUB,
        "https://github.com/acme/app/issues/1",
        "ฉันส่งออก CSV ทุกวัน",
        NOW,
    )
    claim = AtomicClaim(
        id="c-1",
        text="ผู้ใช้ส่งออก CSV ทุกวัน",
        kind=ClaimKind.USER_PAIN,
        status=EpistemicStatus.SUPPORTED,
        evidence_ids=("e-1",),
    )
    view = validated_claim_view(
        claim,
        {"e-1": evidence},
        original_quote="ฉันส่งออก CSV ทุกวัน",
        original_language="th",
        translation="I export CSV every day",
        translation_locale="en",
    )
    card = EvidenceCard(
        id="card-1",
        opportunity_id="o-1",
        known_author_ids=tuple(f"a-{i}" for i in range(5)),
        thread_ids=("t-1", "t-2", "t-3"),
        user_sources=(Source.GITHUB, Source.REDDIT),
        observed_days=(NOW,),
        severity=0.8,
        behavioral_workarounds=1,
        paid_or_wtp_signals=1,
        confidence=0.9,
    )
    inputs = ScoringInputs(
        **{
            name: 100
            for name in (
                "severity",
                "frequency",
                "independent_diversity",
                "behavioral_workaround",
                "wtp_or_spend",
                "recency_trend",
                "gap_strength",
                "competitor_dissatisfaction",
                "reachability",
                "technical_feasibility",
                "small_team_feasibility",
                "inverse_switching_friction",
                "why_now",
            )
        }
    )
    score = score_opportunity(
        snapshot_id="s-1",
        opportunity_id="o-1",
        mission_revision_id=uuid4(),
        inputs=inputs,
        evidence_confidence=0.9,
        created_at=NOW,
    )
    critic = CriticResult(
        opportunity_id="o-1", verdict=Verdict.VALIDATE, confidence=0.9, summary="passes"
    )
    decision = validation_decision(
        card=card,
        score=score,
        competitor_research=CompetitorResearchStatus.COMPLETE,
        gap_evidence_present=True,
        critic=critic,
    )
    return ReportOpportunity(
        "o-1",
        "Invoice <script>alert(1)</script>",
        Verdict.VALIDATE,
        score,
        card,
        (view,),
    ), decision


def test_run_and_opportunity_reports_are_deterministic_safe_and_label_translation() -> (
    None
):
    opportunity, decision = report_opportunity()
    run = RunReportData(
        "run-1",
        "revision-1",
        RunStatus.COMPLETED,
        NOW,
        NOW,
        "th",
        ("warning B", "warning A"),
        (opportunity,),
    )
    first = render_run_report(run)
    assert first == render_run_report(run)
    assert "&lt;script&gt;" in first and "<script>" not in first
    assert "Original quote (th): “ฉันส่งออก CSV ทุกวัน”" in first
    assert "Translation (en): “I export CSV every day”" in first
    detailed = render_opportunity_report(
        OpportunityReportData(opportunity, decision, "th")
    )
    assert "| evidence_card | yes" in detailed
    assert "gapforge-score-v1" in detailed


def test_report_claim_wrapper_rejects_unvalidated_claim() -> None:
    claim = AtomicClaim(
        id="c-1",
        text="Unsupported",
        kind=ClaimKind.OTHER,
        status=EpistemicStatus.SUPPORTED,
        evidence_ids=("invented",),
    )
    with pytest.raises(EvidenceValidationError):
        validated_claim_view(claim, {})


def test_atomic_report_write_and_safe_paths(tmp_path) -> None:
    path = tmp_path / "reports" / "latest.md"
    write_report_atomic(path, "first\n")
    write_report_atomic(path, "second\n")
    assert path.read_text() == "second\n"
    dated, latest = run_report_paths(tmp_path / "reports", "abc-123", NOW)
    assert dated.name == "run-abc-123.md"
    assert dated.parent.name == "2026-08-09"
    assert latest.name == "latest.md"
    with pytest.raises(ValueError, match="unsafe"):
        run_report_paths(tmp_path, "../escape", NOW)
