from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path

import pytest

from gapforge.domain.contracts import RunStatus
from gapforge.reports.artifacts import (
    ReportArtifactConflictError,
    ReportArtifactPathError,
    ReportArtifactStore,
)
from gapforge.reports.renderers import RunReportData, render_run_report

STARTED_AT = datetime(2026, 8, 9, 12, 30, tzinfo=UTC)


def _run_report(*, run_id: str = "run-123", locale: str = "en") -> RunReportData:
    return RunReportData(
        run_id=run_id,
        mission_revision_id="revision-123",
        status=RunStatus.COMPLETED,
        started_at=STARTED_AT,
        finished_at=STARTED_AT,
        output_locale=locale,
        warnings=(),
        opportunities=(),
    )


def test_persist_run_report_uses_canonical_paths_and_is_idempotent(tmp_path: Path) -> None:
    reports_dir = tmp_path / "reports"
    store = ReportArtifactStore(reports_dir)
    report = _run_report()

    first = store.persist_run(report)
    first_run_mtime = first.run_path.stat().st_mtime_ns
    first_latest_mtime = first.latest_path.stat().st_mtime_ns
    restarted = ReportArtifactStore(reports_dir).persist_run(report)

    assert first.run_path == reports_dir / "2026-08-09" / "run-run-123.md"
    assert first.latest_path == reports_dir / "latest.md"
    assert first.run_path.read_text(encoding="utf-8") == render_run_report(report)
    assert first.latest_path.read_bytes() == first.run_path.read_bytes()
    assert restarted == first
    assert first.run_path.stat().st_mtime_ns == first_run_mtime
    assert first.latest_path.stat().st_mtime_ns == first_latest_mtime


def test_same_run_with_changed_content_is_an_explicit_conflict(tmp_path: Path) -> None:
    reports_dir = tmp_path / "reports"
    store = ReportArtifactStore(reports_dir)
    original = _run_report()
    store.persist_run(original)
    original_latest = (reports_dir / "latest.md").read_bytes()
    changed = RunReportData(
        run_id=original.run_id,
        mission_revision_id=original.mission_revision_id,
        status=original.status,
        started_at=original.started_at,
        finished_at=original.finished_at,
        output_locale=original.output_locale,
        warnings=("late warning",),
        opportunities=original.opportunities,
    )

    with pytest.raises(ReportArtifactConflictError, match="different content"):
        store.persist_run(changed)

    assert (reports_dir / "latest.md").read_bytes() == original_latest


def test_interrupted_latest_replace_never_exposes_partial_content_and_restart_repairs(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "reports"
    previous = _run_report(run_id="previous")
    ReportArtifactStore(reports_dir).persist_run(previous)
    previous_latest = (reports_dir / "latest.md").read_bytes()
    current = _run_report(run_id="current", locale="th")

    def fail_latest(source: Path, destination: Path) -> None:
        if destination.name == "latest.md":
            raise OSError("simulated interruption")
        os.replace(source, destination)

    with pytest.raises(OSError, match="simulated interruption"):
        ReportArtifactStore(reports_dir, replace_file=fail_latest).persist_run(current)

    current_path = reports_dir / "2026-08-09" / "run-current.md"
    assert current_path.read_text(encoding="utf-8") == render_run_report(current)
    assert (reports_dir / "latest.md").read_bytes() == previous_latest
    assert not tuple(reports_dir.rglob("*.tmp"))

    repaired = ReportArtifactStore(reports_dir).persist_run(current)
    assert repaired.latest_path.read_bytes() == current_path.read_bytes()


def test_report_store_refuses_traversal_symlinks_and_unsafe_run_ids(tmp_path: Path) -> None:
    with pytest.raises(ReportArtifactPathError, match="traversal"):
        ReportArtifactStore(tmp_path / "safe" / ".." / "escape")
    with pytest.raises(ReportArtifactPathError, match="bounded"):
        ReportArtifactStore(Path("x" * 4097))

    outside = tmp_path / "outside"
    outside.mkdir()
    linked = tmp_path / "linked"
    linked.symlink_to(outside, target_is_directory=True)
    with pytest.raises(ReportArtifactPathError, match="symbolic link"):
        ReportArtifactStore(linked).persist_run(_run_report())

    with pytest.raises(ReportArtifactPathError, match="unsafe"):
        ReportArtifactStore(tmp_path / "reports").persist_run(_run_report(run_id="../escape"))


def test_persisted_thai_and_english_reports_are_deterministic_and_distinct(
    tmp_path: Path,
) -> None:
    english = _run_report(run_id="english", locale="en")
    thai = _run_report(run_id="thai", locale="th")

    english_artifact = ReportArtifactStore(tmp_path / "en").persist_run(english)
    thai_artifact = ReportArtifactStore(tmp_path / "th").persist_run(thai)

    assert english_artifact.run_path.read_text(encoding="utf-8") == render_run_report(english)
    assert thai_artifact.run_path.read_text(encoding="utf-8") == render_run_report(thai)
    assert "# GapForge Run Report" in english_artifact.run_path.read_text(encoding="utf-8")
    assert "# รายงานการวิจัย GapForge" in thai_artifact.run_path.read_text(encoding="utf-8")
    assert english_artifact.content_sha256 != thai_artifact.content_sha256
