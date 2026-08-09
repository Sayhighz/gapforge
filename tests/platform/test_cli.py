from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import func, select
from typer.testing import CliRunner

from gapforge.cli import app
from gapforge.storage.database import Database
from gapforge.storage.models import ResearchRun
from gapforge.storage.uow import SqlAlchemyUnitOfWork

runner = CliRunner()


@pytest.fixture
def cli_database_url(migrated_postgres_url: str, monkeypatch: pytest.MonkeyPatch) -> str:
    monkeypatch.setenv("DATABASE_URL", migrated_postgres_url)
    monkeypatch.setenv("AGENT_PROVIDER", "fake")
    return migrated_postgres_url


def _invoke_json(arguments: list[str]) -> tuple[dict[str, object], int, str]:
    result = runner.invoke(app, [*arguments, "--json"])
    assert result.stdout.count("\n") == 1
    return json.loads(result.stdout), result.exit_code, result.stderr


def test_mission_lifecycle_preserves_thai_and_immutable_revisions(
    cli_database_url: str,
) -> None:
    created, exit_code, stderr = _invoke_json(["mission", "create", "ค้นหาปัญหาบัญชีของธุรกิจขนาดเล็ก"])
    assert exit_code == 0
    assert stderr == ""
    assert created == {
        "schema_version": "1.0",
        "command": "mission.create",
        "data": created["data"],
        "warnings": [],
        "error": None,
    }
    data = created["data"]
    assert isinstance(data, dict)
    assert data["status"] == "DRAFT"
    assert data["revision"]["output_locale"] == "th"
    mission_id = str(data["id"])

    revised, exit_code, _ = _invoke_json(
        [
            "mission",
            "revise",
            mission_id,
            "ค้นหาปัญหาบัญชีและภาษี",
            "--reason",
            "include tax",
        ]
    )
    assert exit_code == 0
    assert revised["data"]["revision_number"] == 2
    assert revised["data"]["parent_revision_id"] == data["revision"]["id"]

    for command, expected in (
        ("activate", "ACTIVE"),
        ("pause", "PAUSED"),
        ("archive", "ARCHIVED"),
    ):
        envelope, exit_code, _ = _invoke_json(["mission", command, mission_id])
        assert exit_code == 0
        assert envelope["data"]["status"] == expected

    blocked, exit_code, stderr = _invoke_json(["mission", "activate", mission_id])
    assert exit_code == 4
    assert stderr == ""
    assert blocked["error"]["code"] == "INVALID_STATE"


@pytest.mark.parametrize(
    "arguments",
    [
        ["mission", "create", ""],
        ["mission", "create", "valid mission", "--title", "x" * 241],
        ["mission", "create", "valid mission", "--output-locale", "not_a_locale"],
    ],
)
def test_mission_input_validation_returns_stable_error(
    cli_database_url: str, arguments: list[str]
) -> None:
    envelope, exit_code, stderr = _invoke_json(arguments)

    assert exit_code == 2
    assert stderr == ""
    assert envelope["error"]["code"] == "INVALID_ARGUMENT"


def test_hunt_and_run_inspection_use_stable_json(cli_database_url: str) -> None:
    created, _, _ = _invoke_json(["mission", "create", "Find expensive reconciliation work"])
    mission_id = str(created["data"]["id"])

    hunt, exit_code, _ = _invoke_json(["hunt", "--mission", mission_id])
    assert exit_code == 0
    assert hunt["data"]["mode"] == "HUNT"
    assert hunt["data"]["status"] == "QUEUED"
    assert hunt["data"]["budget_limits"]["max_agent_calls_per_run"] == 6

    shown, exit_code, _ = _invoke_json(["run", "show", str(hunt["data"]["id"])])
    assert exit_code == 0
    assert shown["data"]["tasks"] == []


@pytest.mark.postgres
async def test_monitor_conflict_uses_savepoint_and_reports_real_queues(
    cli_database_url: str,
) -> None:
    database = Database.from_url(cli_database_url)
    revision_ids = []
    try:
        async with SqlAlchemyUnitOfWork(database.session_factory) as uow:
            for number in range(3):
                mission, revision = await uow.missions.create_with_revision(
                    title=f"Monitor mission {number}",
                    mission_text=f"monitor mission {number}",
                    original_language="en",
                    output_locale="en",
                )
                await uow.missions.set_status(mission.id, "ACTIVE")
                revision_ids.append(revision.id)
            existing = ResearchRun(
                mission_revision_id=revision_ids[1],
                mode="MONITOR",
                status="QUEUED",
                priority=100,
                deadline_at=datetime.now(UTC) + timedelta(minutes=30),
                budget_limits={"max_agent_calls_per_run": 6},
                budget_used={},
                warnings=[],
                last_checkpoint={},
            )
            assert uow.session is not None
            uow.session.add(existing)
            await uow.commit()

        result = await asyncio_to_thread_cli(["monitor", "--once", "--json"])
        assert result.exit_code == 0
        envelope = json.loads(result.stdout)
        queued_revision_ids = {item["mission_revision_id"] for item in envelope["data"]["queued"]}
        assert str(revision_ids[0]) in queued_revision_ids
        assert str(revision_ids[2]) in queued_revision_ids
        assert str(revision_ids[1]) not in queued_revision_ids
        assert len(envelope["warnings"]) >= 1
        assert "warnings" not in envelope["data"]

        async with database.session() as session:
            persisted = await session.scalar(
                select(func.count())
                .select_from(ResearchRun)
                .where(ResearchRun.mission_revision_id.in_(revision_ids))
            )
        assert persisted == 3
    finally:
        await database.dispose()


async def asyncio_to_thread_cli(arguments: list[str]) -> object:
    import asyncio

    return await asyncio.to_thread(runner.invoke, app, arguments)


def test_invalid_identifier_uses_json_error_envelope(cli_database_url: str) -> None:
    envelope, exit_code, stderr = _invoke_json(["mission", "show", "not-a-uuid"])

    assert exit_code == 2
    assert stderr == ""
    assert envelope["error"] == {
        "code": "INVALID_ARGUMENT",
        "message": "invalid mission ID",
    }


def test_admin_sql_cli_requires_guard_and_keeps_stdout_clean(cli_database_url: str) -> None:
    missing_guard, exit_code, stderr = _invoke_json(["admin", "sql", "SELECT 1 AS n"])
    assert exit_code == 2
    assert stderr == ""
    assert missing_guard["error"]["code"] == "INVALID_ARGUMENT"

    selected, exit_code, stderr = _invoke_json(["admin", "sql", "SELECT 1 AS n", "--read-only"])
    assert exit_code == 0
    assert stderr == ""
    assert selected["data"] == {
        "columns": ["n"],
        "rows": [{"n": 1}],
        "truncated": False,
    }


def test_health_cli_emits_stable_envelope(cli_database_url: str) -> None:
    envelope, exit_code, stderr = _invoke_json(["health"])

    assert exit_code == 0
    assert stderr == ""
    assert envelope["command"] == "health"
    assert envelope["data"]["status"] == "degraded"


def test_required_command_tree_is_exposed() -> None:
    for arguments in (
        ["mission", "--help"],
        ["run", "--help"],
        ["opportunity", "--help"],
        ["evidence", "--help"],
        ["merge-candidate", "--help"],
        ["report", "--help"],
        ["backup", "--help"],
        ["admin", "--help"],
    ):
        result = runner.invoke(app, arguments)
        assert result.exit_code == 0, result.output
