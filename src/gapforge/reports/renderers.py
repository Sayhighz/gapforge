"""Deterministic Markdown report rendering from previously validated domain data."""

from __future__ import annotations

import html
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from gapforge.analysis.evidence import EvidenceRecord, validate_atomic_claim
from gapforge.analysis.normalization import normalize_text
from gapforge.domain.contracts import (
    AtomicClaim,
    EvidenceCard,
    OpportunityScoreSnapshot,
    RunStatus,
    Verdict,
)
from gapforge.scoring.engine import ValidationDecision

LABELS = {
    "en": {
        "run_report": "GapForge Run Report",
        "run": "Run",
        "mission_revision": "Mission revision",
        "status": "Status",
        "started": "Started",
        "finished": "Finished",
        "incomplete": "incomplete",
        "presentation_locale": "Presentation locale",
        "warnings": "Warnings",
        "none": "None",
        "opportunities": "Opportunities",
        "id": "ID",
        "verdict": "Verdict",
        "score": "Score",
        "evidence_confidence": "Evidence confidence",
        "counts": "Known authors / threads / sources",
        "claims": "Validated claims",
        "no_claims": "No validated claims",
        "no_opportunities": "No opportunities cleared research output gates.",
        "original_quote": "Original quote",
        "translation": "Translation",
        "evidence": "Evidence",
        "validation_gates": "Validation gates",
        "gate": "Gate",
        "pass": "Pass",
        "actual": "Actual",
        "required": "Required",
        "yes": "yes",
        "no": "no",
        "score_explanation": "Score explanation",
        "penalties": "Penalties",
        "known_authors": "Known authors",
        "independent_threads": "Independent threads",
        "user_sources": "User sources",
        "behavioral_workarounds": "Behavioral workarounds",
        "wtp": "WTP/spend signals",
        "score_algorithm": "Score algorithm",
        "final_score": "Final score",
        "opportunity": "Opportunity",
    },
    "th": {
        "run_report": "รายงานการวิจัย GapForge",
        "run": "รอบการวิจัย",
        "mission_revision": "รุ่นภารกิจ",
        "status": "สถานะ",
        "started": "เริ่ม",
        "finished": "เสร็จสิ้น",
        "incomplete": "ยังไม่เสร็จ",
        "presentation_locale": "ภาษาที่แสดง",
        "warnings": "คำเตือน",
        "none": "ไม่มี",
        "opportunities": "โอกาส",
        "id": "รหัส",
        "verdict": "ผลตัดสิน",
        "score": "คะแนน",
        "evidence_confidence": "ความมั่นใจของหลักฐาน",
        "counts": "ผู้ใช้ / กระทู้ / แหล่งข้อมูลอิสระ",
        "claims": "ข้อสรุปที่ผ่านการตรวจสอบ",
        "no_claims": "ไม่มีข้อสรุปที่ผ่านการตรวจสอบ",
        "no_opportunities": "ไม่มีโอกาสที่ผ่านเกณฑ์ผลลัพธ์การวิจัย",
        "original_quote": "ข้อความต้นฉบับ",
        "translation": "คำแปล",
        "evidence": "หลักฐาน",
        "validation_gates": "เกณฑ์การตรวจสอบ",
        "gate": "เกณฑ์",
        "pass": "ผ่าน",
        "actual": "ค่าจริง",
        "required": "ข้อกำหนด",
        "yes": "ใช่",
        "no": "ไม่",
        "score_explanation": "คำอธิบายคะแนน",
        "penalties": "ค่าปรับ",
        "known_authors": "ผู้ใช้ที่ทราบตัวตนอิสระ",
        "independent_threads": "กระทู้อิสระ",
        "user_sources": "แหล่งหลักฐานผู้ใช้",
        "behavioral_workarounds": "วิธีแก้ขัดเชิงพฤติกรรม",
        "wtp": "สัญญาณการจ่ายหรือความเต็มใจจ่าย",
        "score_algorithm": "อัลกอริทึมคะแนน",
        "final_score": "คะแนนสุดท้าย",
        "opportunity": "โอกาส",
    },
}


def _md(value: str) -> str:
    """Keep evidence inert in Markdown, including embedded HTML and newlines."""
    return html.escape(" ".join(value.split()), quote=False).replace("|", "\\|")


def _catalog(locale: str) -> tuple[dict[str, str], str]:
    requested = locale.casefold()
    selected = "th" if requested == "th" or requested.startswith("th-") else "en"
    expected = requested == selected or requested.startswith(f"{selected}-")
    display = selected if expected else f"{selected} (fallback from {locale})"
    return LABELS[selected], display


@dataclass(frozen=True, slots=True)
class ValidatedClaimView:
    claim: AtomicClaim
    original_quote: str | None = None
    original_language: str | None = None
    translation: str | None = None
    translation_locale: str | None = None

    def __post_init__(self) -> None:
        if bool(self.translation) != bool(self.translation_locale):
            raise ValueError("translation text and locale must be provided together")


def validated_claim_view(
    claim: AtomicClaim,
    evidence: dict[str, EvidenceRecord],
    *,
    known_claim_ids: frozenset[str] = frozenset(),
    original_quote: str | None = None,
    original_language: str | None = None,
    translation: str | None = None,
    translation_locale: str | None = None,
) -> ValidatedClaimView:
    validate_atomic_claim(claim, evidence, known_claim_ids=known_claim_ids)
    if original_quote:
        if not original_language:
            raise ValueError("original quote requires an original language")
        permitted = (record.text for key, record in evidence.items() if key in claim.evidence_ids)
        if not any(normalize_text(original_quote) in normalize_text(text) for text in permitted):
            raise ValueError("original quote is not present in permitted captured evidence")
    return ValidatedClaimView(
        claim,
        original_quote,
        original_language,
        translation,
        translation_locale,
    )


@dataclass(frozen=True, slots=True)
class ReportOpportunity:
    opportunity_id: str
    title: str
    verdict: Verdict
    score: OpportunityScoreSnapshot
    evidence_card: EvidenceCard
    claims: tuple[ValidatedClaimView, ...]
    validation: ValidationDecision

    def __post_init__(self) -> None:
        if {
            self.opportunity_id,
            self.score.opportunity_id,
            self.evidence_card.opportunity_id,
        } != {self.opportunity_id}:
            raise ValueError("report artifacts belong to different opportunities")
        if self.verdict is not self.validation.verdict:
            raise ValueError("report verdict does not match validation decision")


@dataclass(frozen=True, slots=True)
class RunReportData:
    run_id: str
    mission_revision_id: str
    status: RunStatus
    started_at: datetime
    finished_at: datetime | None
    output_locale: str
    warnings: tuple[str, ...]
    opportunities: tuple[ReportOpportunity, ...]


@dataclass(frozen=True, slots=True)
class OpportunityReportData:
    opportunity: ReportOpportunity
    output_locale: str


def _render_claim(view: ValidatedClaimView, labels: dict[str, str], locale: str) -> list[str]:
    requested = locale.casefold().split("-", 1)[0]
    translated = (
        view.translation
        and view.translation_locale
        and view.translation_locale.casefold().split("-", 1)[0] == requested
    )
    presentation = view.translation if translated else view.claim.text
    lines = [f"- **{view.claim.status.value}** — {_md(presentation or view.claim.text)}"]
    if view.original_quote:
        language = _md(view.original_language or "und")
        lines.append(f"  - {labels['original_quote']} ({language}): “{_md(view.original_quote)}”")
    if view.translation:
        translation_locale = _md(view.translation_locale or "")
        lines.append(
            f"  - {labels['translation']} ({translation_locale}): “{_md(view.translation)}”"
        )
    if view.claim.evidence_ids:
        evidence = ", ".join(f"`{_md(item)}`" for item in sorted(view.claim.evidence_ids))
        lines.append(f"  - {labels['evidence']}: {evidence}")
    return lines


def render_run_report(data: RunReportData) -> str:
    labels, display_locale = _catalog(data.output_locale)
    finished = data.finished_at.isoformat() if data.finished_at else labels["incomplete"]
    lines = [
        f"# {labels['run_report']}",
        "",
        f"- {labels['run']}: `{_md(data.run_id)}`",
        f"- {labels['mission_revision']}: `{_md(data.mission_revision_id)}`",
        f"- {labels['status']}: `{data.status.value}`",
        f"- {labels['started']}: `{data.started_at.isoformat()}`",
        f"- {labels['finished']}: `{finished}`",
        f"- {labels['presentation_locale']}: `{_md(display_locale)}`",
        "",
        f"## {labels['warnings']}",
        "",
    ]
    lines.extend(f"- {_md(warning)}" for warning in sorted(data.warnings))
    if not data.warnings:
        lines.append(f"- {labels['none']}")
    lines.extend(("", f"## {labels['opportunities']}", ""))
    for opportunity in sorted(data.opportunities, key=lambda item: item.opportunity_id):
        evidence_card = opportunity.evidence_card
        counts = (
            len(evidence_card.known_author_ids),
            len(evidence_card.thread_ids),
            len(evidence_card.user_sources),
        )
        lines.extend(
            (
                f"### {_md(opportunity.title)}",
                "",
                f"- {labels['id']}: `{_md(opportunity.opportunity_id)}`",
                f"- {labels['verdict']}: `{opportunity.verdict.value}`",
                f"- {labels['score']}: `{opportunity.score.final_score:.2f}`",
                f"- {labels['evidence_confidence']}: `{evidence_card.confidence:.2f}`",
                f"- {labels['counts']}: `{counts[0]}` / `{counts[1]}` / `{counts[2]}`",
                "",
                f"{labels['claims']}:",
                "",
            )
        )
        for claim in sorted(opportunity.claims, key=lambda item: item.claim.id):
            lines.extend(_render_claim(claim, labels, data.output_locale))
        if not opportunity.claims:
            lines.append(f"- {labels['no_claims']}")
        lines.append("")
    if not data.opportunities:
        lines.extend((labels["no_opportunities"], ""))
    return "\n".join(lines).rstrip() + "\n"


def render_opportunity_report(data: OpportunityReportData) -> str:
    item = data.opportunity
    labels, display_locale = _catalog(data.output_locale)
    lines = [
        f"# {_md(item.title)}",
        "",
        f"- {labels['opportunity']}: `{_md(item.opportunity_id)}`",
        f"- {labels['verdict']}: `{item.verdict.value}`",
        f"- {labels['score_algorithm']}: `{item.score.algorithm_version}`",
        f"- {labels['final_score']}: `{item.score.final_score:.2f}`",
        f"- {labels['presentation_locale']}: `{_md(display_locale)}`",
        "",
        f"## {labels['validation_gates']}",
        "",
        f"| {labels['gate']} | {labels['pass']} | {labels['actual']} | {labels['required']} |",
        "|---|---:|---:|---:|",
    ]
    for gate in item.validation.gates:
        passed = labels["yes"] if gate.passed else labels["no"]
        lines.append(f"| {_md(gate.name)} | {passed} | {_md(gate.actual)} | {_md(gate.required)} |")
    lines.extend(("", f"## {labels['score_explanation']}", ""))
    lines.extend(f"- {_md(value)}" for value in item.score.explanation)
    if item.score.penalties:
        lines.extend(("", f"{labels['penalties']}:", ""))
        lines.extend(
            f"- {_md(name)}: `{value:.2f}`" for name, value in sorted(item.score.penalties.items())
        )
    lines.extend(("", f"## {labels['evidence']}", ""))
    lines.extend(
        (
            f"- {labels['known_authors']}: `{len(item.evidence_card.known_author_ids)}`",
            f"- {labels['independent_threads']}: `{len(item.evidence_card.thread_ids)}`",
            f"- {labels['user_sources']}: `{len(item.evidence_card.user_sources)}`",
            f"- {labels['behavioral_workarounds']}: `{item.evidence_card.behavioral_workarounds}`",
            f"- {labels['wtp']}: `{item.evidence_card.paid_or_wtp_signals}`",
            "",
            f"## {labels['claims']}",
            "",
        )
    )
    for claim in sorted(item.claims, key=lambda value: value.claim.id):
        lines.extend(_render_claim(claim, labels, data.output_locale))
    if not item.claims:
        lines.append(f"- {labels['none']}")
    return "\n".join(lines).rstrip() + "\n"


def write_report_atomic(path: Path, content: str) -> None:
    """Replace a report atomically without exposing partial files."""
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary_path = Path(temporary)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, path)
    finally:
        temporary_path.unlink(missing_ok=True)


def run_report_paths(base: Path, run_id: str, started_at: datetime) -> tuple[Path, Path]:
    safe = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"
    if not run_id or any(character not in safe for character in run_id):
        raise ValueError("run ID is unsafe for a report filename")
    dated = base / started_at.date().isoformat() / f"run-{run_id}.md"
    return dated, base / "latest.md"
