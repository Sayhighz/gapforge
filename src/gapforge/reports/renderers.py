"""Deterministic Markdown report rendering from previously validated domain data."""

from __future__ import annotations

import html
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from gapforge.analysis.evidence import EvidenceRecord, validate_atomic_claim
from gapforge.domain.contracts import (
    AtomicClaim,
    EvidenceCard,
    OpportunityScoreSnapshot,
    RunStatus,
    Verdict,
)
from gapforge.scoring.engine import ValidationDecision


def _md(value: str) -> str:
    """Keep evidence inert in Markdown, including embedded HTML and newlines."""
    return html.escape(" ".join(value.split()), quote=False).replace("|", "\\|")


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
    validation: ValidationDecision
    output_locale: str


def _render_claim(view: ValidatedClaimView) -> list[str]:
    lines = [f"- **{view.claim.status.value}** — {_md(view.claim.text)}"]
    if view.original_quote:
        language = _md(view.original_language or "und")
        lines.append(f"  - Original quote ({language}): “{_md(view.original_quote)}”")
    if view.translation:
        lines.append(f"  - Translation ({_md(view.translation_locale or '')}): “{_md(view.translation)}”")
    if view.claim.evidence_ids:
        lines.append(f"  - Evidence: {', '.join(f'`{_md(item)}`' for item in sorted(view.claim.evidence_ids))}")
    return lines


def render_run_report(data: RunReportData) -> str:
    lines = [
        "# GapForge Run Report",
        "",
        f"- Run: `{_md(data.run_id)}`",
        f"- Mission revision: `{_md(data.mission_revision_id)}`",
        f"- Status: `{data.status.value}`",
        f"- Started: `{data.started_at.isoformat()}`",
        f"- Finished: `{data.finished_at.isoformat() if data.finished_at else 'incomplete'}`",
        f"- Output locale: `{_md(data.output_locale)}`",
        "",
        "## Warnings",
        "",
    ]
    lines.extend(f"- {_md(warning)}" for warning in sorted(data.warnings))
    if not data.warnings:
        lines.append("- None")
    lines.extend(("", "## Opportunities", ""))
    for opportunity in sorted(data.opportunities, key=lambda item: item.opportunity_id):
        lines.extend(
            (
                f"### {_md(opportunity.title)}",
                "",
                f"- ID: `{_md(opportunity.opportunity_id)}`",
                f"- Verdict: `{opportunity.verdict.value}`",
                f"- Score: `{opportunity.score.final_score:.2f}`",
                f"- Evidence confidence: `{opportunity.evidence_card.confidence:.2f}`",
                f"- Known authors / threads / sources: `{len(opportunity.evidence_card.known_author_ids)}` / `{len(opportunity.evidence_card.thread_ids)}` / `{len(opportunity.evidence_card.user_sources)}`",
                "",
                "Claims:",
                "",
            )
        )
        for claim in sorted(opportunity.claims, key=lambda item: item.claim.id):
            lines.extend(_render_claim(claim))
        if not opportunity.claims:
            lines.append("- No validated claims")
        lines.append("")
    if not data.opportunities:
        lines.append("No opportunities cleared research output gates.")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def render_opportunity_report(data: OpportunityReportData) -> str:
    item = data.opportunity
    lines = [
        f"# {_md(item.title)}",
        "",
        f"- Opportunity: `{_md(item.opportunity_id)}`",
        f"- Verdict: `{item.verdict.value}`",
        f"- Score algorithm: `{item.score.algorithm_version}`",
        f"- Final score: `{item.score.final_score:.2f}`",
        f"- Output locale: `{_md(data.output_locale)}`",
        "",
        "## Validation gates",
        "",
        "| Gate | Pass | Actual | Required |",
        "|---|---:|---:|---:|",
    ]
    for gate in data.validation.gates:
        lines.append(
            f"| {_md(gate.name)} | {'yes' if gate.passed else 'no'} | {_md(gate.actual)} | {_md(gate.required)} |"
        )
    lines.extend(("", "## Score explanation", ""))
    lines.extend(f"- {_md(value)}" for value in item.score.explanation)
    if item.score.penalties:
        lines.extend(("", "Penalties:", ""))
        lines.extend(f"- {_md(name)}: `{value:.2f}`" for name, value in sorted(item.score.penalties.items()))
    lines.extend(("", "## Evidence", ""))
    lines.extend(
        (
            f"- Known authors: `{len(item.evidence_card.known_author_ids)}`",
            f"- Independent threads: `{len(item.evidence_card.thread_ids)}`",
            f"- User sources: `{len(item.evidence_card.user_sources)}`",
            f"- Behavioral workarounds: `{item.evidence_card.behavioral_workarounds}`",
            f"- WTP/spend signals: `{item.evidence_card.paid_or_wtp_signals}`",
            "",
            "## Validated claims",
            "",
        )
    )
    for claim in sorted(item.claims, key=lambda value: value.claim.id):
        lines.extend(_render_claim(claim))
    if not item.claims:
        lines.append("- None")
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
    if not run_id or any(character not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_" for character in run_id):
        raise ValueError("run ID is unsafe for a report filename")
    dated = base / started_at.date().isoformat() / f"run-{run_id}.md"
    return dated, base / "latest.md"
