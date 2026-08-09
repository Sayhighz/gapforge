from __future__ import annotations

import shlex
from pathlib import Path
from typing import Any

from typer.main import get_command

from gapforge.cli import app

ROOT = Path(__file__).parents[2]
COMMANDS = ROOT / ".agents/skills/business-gap/references/commands.md"


def _leaf_commands(command: Any, prefix: tuple[str, ...] = ()) -> dict[tuple[str, ...], Any]:
    children = getattr(command, "commands", None)
    if not isinstance(children, dict):
        return {prefix: command}
    paths: dict[tuple[str, ...], Any] = {}
    for name, child in children.items():
        paths.update(_leaf_commands(child, (*prefix, name)))
    return paths


def test_repository_skill_examples_name_only_exposed_cli_commands() -> None:
    documented = [
        shlex.split(line)[1:]
        for line in COMMANDS.read_text(encoding="utf-8").splitlines()
        if line.startswith("gap ")
    ]
    implemented = _leaf_commands(get_command(app))

    assert documented
    for arguments in documented:
        matches = [path for path in implemented if arguments[: len(path)] == list(path)]
        assert matches, arguments
        path = max(matches, key=len)
        with implemented[path].make_context(path[-1], arguments[len(path) :]):
            pass


def test_product_hypothesis_requires_explicit_user_content_and_real_cli_surface() -> None:
    skill = (ROOT / ".agents/skills/business-gap/SKILL.md").read_text(encoding="utf-8")
    commands = COMMANDS.read_text(encoding="utf-8")
    assert "only when the user explicitly requests it" in skill
    assert "supplies the proposition and request ID" in skill
    assert "gap product-hypothesis create <assessment-id>" in commands
    assert "makes no Codex or AI API call" in commands
