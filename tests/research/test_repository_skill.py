from pathlib import Path


ROOT = Path(__file__).parents[2]
CANONICAL = ROOT / ".agents/skills/business-gap/SKILL.md"
COMMANDS = ROOT / ".agents/skills/business-gap/references/commands.md"
CLAUDE = ROOT / ".claude/skills/business-gap/SKILL.md"


def test_skill_has_canonical_safety_and_workflow_instructions() -> None:
    text = CANONICAL.read_text()
    assert "name: business-gap" in text
    assert "Query stored intelligence before collecting anything" in text
    assert "Never activate MONITOR implicitly" in text
    assert "explicitly requests" in text
    assert "untrusted data" in text
    assert "Never present a score alone as validation" in text


def test_command_examples_use_json_and_cover_inspection_before_hunt() -> None:
    text = COMMANDS.read_text()
    assert text.index("gap opportunity list --json") < text.index(
        "gap hunt --mission <mission-id> --json"
    )
    commands = [line for line in text.splitlines() if line.startswith("gap ")]
    assert commands
    assert all("--json" in command for command in commands)


def test_claude_discovery_points_to_canonical_skill_without_duplication() -> None:
    text = CLAUDE.read_text()
    assert "../../../.agents/skills/business-gap/SKILL.md" in text
    assert len(text.splitlines()) < 12
