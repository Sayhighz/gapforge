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


def report_opportunity() -> ReportOpportunity:
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
        severity=100,
        frequency=100,
        independent_diversity=100,
        behavioral_workaround=100,
        wtp_or_spend=100,
        recency_trend=100,
        gap_strength=100,
        competitor_dissatisfaction=100,
        reachability=100,
        technical_feasibility=100,
        small_team_feasibility=100,
        inverse_switching_friction=100,
        why_now=100,
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
        decision,
    )


def test_run_and_opportunity_reports_are_deterministic_safe_and_localized() -> None:
    opportunity = report_opportunity()
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
    assert "# รายงานการวิจัย GapForge" in first
    assert "ข้อความต้นฉบับ (th): “ฉันส่งออก CSV ทุกวัน”" in first
    assert "คำแปล (en): “I export CSV every day”" in first
    detailed = render_opportunity_report(OpportunityReportData(opportunity, "th"))
    assert "| evidence_card | ใช่" in detailed
    assert "gapforge-score-v1" in detailed


def test_matching_translation_is_preferred_and_unknown_locale_falls_back() -> None:
    opportunity = report_opportunity()
    english = render_opportunity_report(OpportunityReportData(opportunity, "en"))
    assert "**SUPPORTED** — I export CSV every day" in english
    assert "Original quote (th): “ฉันส่งออก CSV ทุกวัน”" in english
    fallback = render_opportunity_report(OpportunityReportData(opportunity, "fr"))
    assert "Presentation locale: `en (fallback from fr)`" in fallback


def test_report_claim_wrapper_rejects_unvalidated_claim_and_quote() -> None:
    claim = AtomicClaim(
        id="c-1",
        text="Unsupported",
        kind=ClaimKind.OTHER,
        status=EpistemicStatus.SUPPORTED,
        evidence_ids=("invented",),
    )
    with pytest.raises(EvidenceValidationError):
        validated_claim_view(claim, {})
    evidence = EvidenceRecord("e-1", Source.GITHUB, "https://example.com", "captured text", NOW)
    grounded = AtomicClaim(
        id="c-2",
        text="Captured",
        kind=ClaimKind.OTHER,
        status=EpistemicStatus.SUPPORTED,
        evidence_ids=("e-1",),
    )
    with pytest.raises(ValueError, match="original quote"):
        validated_claim_view(
            grounded,
            {"e-1": evidence},
            original_quote="invented quote",
            original_language="en",
        )


def test_report_rejects_mixed_artifacts_and_false_verdict() -> None:
    opportunity = report_opportunity()
    with pytest.raises(ValueError, match="different opportunities"):
        ReportOpportunity(
            "other",
            opportunity.title,
            opportunity.verdict,
            opportunity.score,
            opportunity.evidence_card,
            opportunity.claims,
            opportunity.validation,
        )
    with pytest.raises(ValueError, match="validation decision"):
        ReportOpportunity(
            opportunity.opportunity_id,
            opportunity.title,
            Verdict.RESEARCH_MORE,
            opportunity.score,
            opportunity.evidence_card,
            opportunity.claims,
            opportunity.validation,
        )


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
