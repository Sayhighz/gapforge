from __future__ import annotations

import shlex
from pathlib import Path
from typing import Any

from typer.main import get_command

from gapforge.cli import app

ROOT = Path(__file__).parents[2]
COMMANDS = ROOT / ".agents/skills/business-gap/references/commands.md"


def _leaf_commands(command: Any, prefix: tuple[str, ...] = ()) -> set[tuple[str, ...]]:
    children = getattr(command, "commands", None)
    if not isinstance(children, dict):
        return {prefix}
    paths: set[tuple[str, ...]] = set()
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
        assert any(arguments[: len(path)] == list(path) for path in implemented), arguments
    assert not any(arguments[:2] == ["product-hypothesis", "create"] for arguments in documented)
