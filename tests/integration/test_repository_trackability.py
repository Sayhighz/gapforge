from __future__ import annotations

import subprocess
from pathlib import Path
from shutil import which


def test_source_report_package_is_not_hidden_by_generated_report_ignore_rule() -> None:
    repository = Path(__file__).resolve().parents[2]
    git = which("git")
    assert git is not None
    result = subprocess.run(  # noqa: S603 - fixed executable and static arguments
        [git, "check-ignore", "--no-index", "src/gapforge/reports/__init__.py"],
        cwd=repository,
        capture_output=True,
        check=False,
        text=True,
    )

    assert result.returncode == 1, result.stdout
