from __future__ import annotations

import asyncio
import hashlib
import shutil
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.engine import make_url

from gapforge.backup import BackupError, BackupService
from gapforge.providers.process import AsyncProcessRunner, ProcessResult
from gapforge.storage.database import Database
from gapforge.storage.models import ResearchMission
from gapforge.storage.uow import SqlAlchemyUnitOfWork


class BackupRunner(AsyncProcessRunner):
    def __init__(self, *, archive_valid: bool = True, restore_valid: bool = True) -> None:
        self.calls: list[tuple[list[str], dict[str, str]]] = []
        self.archive_valid = archive_valid
        self.restore_valid = restore_valid

    async def run(
        self,
        command: list[str],
        *,
        stdin: bytes,
        env: dict[str, str],
        cwd: Path,
        timeout_seconds: float,
        max_output_bytes: int,
        termination_grace_seconds: float = 2.0,
    ) -> ProcessResult:
        del stdin, cwd, timeout_seconds, max_output_bytes, termination_grace_seconds
        self.calls.append((list(command), dict(env)))
        if command[-1] == "--version":
            return ProcessResult(0, b"pg_dump (PostgreSQL) 16.9\n", b"", False, False, False)
        if command[0] == "pg_dump":
            await asyncio.to_thread(
                Path(command[command.index("--file") + 1]).write_bytes,
                b"custom-format-dump",
            )
        if command[0] == "pg_restore" and command[1] == "--list" and not self.archive_valid:
            return ProcessResult(1, b"", b"untrusted database error", False, False, False)
        if command[0] == "pg_restore" and command[1] != "--list" and not self.restore_valid:
            return ProcessResult(1, b"", b"restore failed", False, False, False)
        return ProcessResult(0, b"archive listing\n", b"", False, False, False)


@pytest.mark.asyncio
async def test_backup_create_verify_is_atomic_and_keeps_password_out_of_argv(
    tmp_path: Path,
) -> None:
    runner = BackupRunner()
    service = BackupService(
        database_url="postgresql+asyncpg://backup-user:private-password@db.internal:5432/app",
        backups_dir=tmp_path / "backups",
        runner=runner,
        parent_env={
            "PATH": "/usr/bin",
            "DATABASE_URL": "must-not-leak",
            "GITHUB_TOKEN": "must-not-leak",
        },
        now=lambda: datetime(2026, 8, 9, 3, 0, tzinfo=UTC),
    )

    record = await service.create()
    verified = await service.verify(record.name)

    assert record.name.startswith("gapforge-20260809T030000Z-")
    assert verified.archive_readable is True
    assert verified.sha256 == hashlib.sha256(b"custom-format-dump").hexdigest()
    assert not tuple((tmp_path / "backups").glob(".gapforge-backup-*"))
    for command, environment in runner.calls:
        assert "private-password" not in " ".join(command)
        assert "DATABASE_URL" not in environment
        assert "GITHUB_TOKEN" not in environment
        assert environment.get("PGPASSWORD") == "private-password"


@pytest.mark.asyncio
async def test_corrupt_archive_fails_before_pg_restore(tmp_path: Path) -> None:
    runner = BackupRunner()
    service = BackupService(
        database_url="postgresql://user:password@localhost/source",
        backups_dir=tmp_path,
        runner=runner,
        now=lambda: datetime(2026, 8, 9, tzinfo=UTC),
    )
    record = await service.create()
    (tmp_path / record.name).write_bytes(b"corrupt")
    calls_before_verify = len(runner.calls)

    with pytest.raises(BackupError, match="checksum mismatch"):
        await service.verify(record.name)

    assert len(runner.calls) == calls_before_verify


@pytest.mark.asyncio
async def test_create_removes_unreadable_archive_before_publishing(tmp_path: Path) -> None:
    service = BackupService(
        database_url="postgresql://user:password@localhost/source",
        backups_dir=tmp_path,
        runner=BackupRunner(archive_valid=False),
        now=lambda: datetime(2026, 8, 9, tzinfo=UTC),
    )

    with pytest.raises(BackupError, match="archive verification"):
        await service.create()

    assert not await asyncio.to_thread(lambda: tuple(tmp_path.iterdir()))


@pytest.mark.asyncio
async def test_restore_verifies_first_requires_confirmation_and_safe_target(tmp_path: Path) -> None:
    runner = BackupRunner()
    service = BackupService(
        database_url="postgresql://user:password@localhost/source",
        backups_dir=tmp_path,
        runner=runner,
        now=lambda: datetime(2026, 8, 9, tzinfo=UTC),
    )
    record = await service.create()

    with pytest.raises(BackupError, match="--yes"):
        await service.restore(record.name, target_database="restored", confirmed=False)
    with pytest.raises(BackupError, match="target database"):
        await service.restore(
            record.name,
            target_database='bad";drop database source',
            confirmed=True,
        )

    result = await service.restore(record.name, target_database="restored_copy", confirmed=True)

    assert result["status"] == "restored"
    restore_commands = [command for command, _ in runner.calls if command[0] == "pg_restore"]
    assert restore_commands[0][1] == "--list"
    assert "--exit-on-error" in restore_commands[-1]
    createdb_command = next(command for command, _ in runner.calls if command[0] == "createdb")
    assert createdb_command[createdb_command.index("--maintenance-db") + 1] == "postgres"


@pytest.mark.asyncio
async def test_failed_restore_drops_target_through_maintenance_database(tmp_path: Path) -> None:
    runner = BackupRunner(restore_valid=False)
    service = BackupService(
        database_url="postgresql://user:password@localhost/source",
        backups_dir=tmp_path,
        runner=runner,
        now=lambda: datetime(2026, 8, 9, tzinfo=UTC),
    )
    record = await service.create()

    with pytest.raises(BackupError, match="pg_restore"):
        await service.restore(record.name, target_database="restored_copy", confirmed=True)

    dropdb_command = next(command for command, _ in runner.calls if command[0] == "dropdb")
    assert dropdb_command[dropdb_command.index("--maintenance-db") + 1] == "postgres"


@pytest.mark.asyncio
async def test_backup_maintenance_database_is_configurable(tmp_path: Path) -> None:
    runner = BackupRunner()
    service = BackupService(
        database_url="postgresql://user:password@localhost/source",
        backups_dir=tmp_path,
        runner=runner,
        maintenance_database="administration",
        now=lambda: datetime(2026, 8, 9, tzinfo=UTC),
    )
    record = await service.create()

    await service.restore(record.name, target_database="restored_copy", confirmed=True)

    createdb_command = next(command for command, _ in runner.calls if command[0] == "createdb")
    assert createdb_command[createdb_command.index("--maintenance-db") + 1] == "administration"


def test_backup_rejects_unsafe_maintenance_database(tmp_path: Path) -> None:
    with pytest.raises(BackupError, match="maintenance database"):
        BackupService(
            database_url="postgresql://user:password@localhost/source",
            backups_dir=tmp_path,
            maintenance_database="../postgres",
        )


@pytest.mark.asyncio
async def test_backup_paths_reject_traversal_and_symlinks(tmp_path: Path) -> None:
    service = BackupService(
        database_url="postgresql://user@localhost/source",
        backups_dir=tmp_path / "backups",
        runner=BackupRunner(),
    )
    service.list_backups()
    with pytest.raises(BackupError, match="name is invalid"):
        await service.verify("../outside.dump")

    linked_root = tmp_path / "linked"
    linked_root.symlink_to(tmp_path / "backups", target_is_directory=True)
    linked = BackupService(
        database_url="postgresql://user@localhost/source",
        backups_dir=linked_root,
        runner=BackupRunner(),
    )
    with pytest.raises(BackupError, match="symbolic link"):
        linked.list_backups()


@pytest.mark.asyncio
async def test_retention_keeps_union_of_daily_weekly_monthly_buckets(tmp_path: Path) -> None:
    service = BackupService(
        database_url="postgresql://user@localhost/source",
        backups_dir=tmp_path,
        runner=BackupRunner(),
    )
    newest = datetime(2026, 8, 9, 12, tzinfo=UTC)
    created: list[tuple[str, datetime]] = []
    for index in range(220):
        timestamp = newest - timedelta(days=index)
        name = f"gapforge-{timestamp:%Y%m%dT%H%M%SZ}-{index:08x}.dump"
        archive = tmp_path / name
        archive.write_bytes(name.encode())
        checksum = hashlib.sha256(name.encode()).hexdigest()
        archive.with_suffix(".dump.sha256").write_text(f"{checksum}  {name}\n", encoding="ascii")
        created.append((name, timestamp))

    expected: set[str] = set()
    for limit, key in (
        (7, lambda value: value.date()),
        (4, lambda value: value.isocalendar()[:2]),
        (6, lambda value: (value.year, value.month)),
    ):
        seen: set[object] = set()
        for name, timestamp in created:
            bucket = key(timestamp)
            if bucket in seen:
                continue
            seen.add(bucket)
            expected.add(name)
            if len(seen) == limit:
                break

    removed = await service.apply_retention()

    assert {record.name for record in service.list_backups()} == expected
    assert len(removed) == len(created) - len(expected)
    assert len(expected) <= 17


def _postgres_client_tools() -> bool:
    tools = ("pg_dump", "pg_restore", "createdb", "dropdb")
    if any(shutil.which(tool) is None for tool in tools):
        return False
    pg_dump = shutil.which("pg_dump")
    assert pg_dump is not None
    completed = subprocess.run(  # noqa: S603 -- resolved trusted PostgreSQL executable
        [pg_dump, "--version"],
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    return completed.returncode == 0 and "PostgreSQL) 16." in completed.stdout


@pytest.mark.postgres
async def test_backup_round_trip_restores_temporary_database(
    migrated_postgres_url: str,
    tmp_path: Path,
) -> None:
    if not _postgres_client_tools():
        pytest.skip("PostgreSQL 16 client tools are not installed")
    source = Database.from_url(migrated_postgres_url)
    title = f"Backup sentinel {uuid4()}"
    async with SqlAlchemyUnitOfWork(source.session_factory) as uow:
        mission, _ = await uow.missions.create_with_revision(
            title=title,
            mission_text="prove restore round trip",
            original_language="en",
            output_locale="en",
        )
        await uow.commit()
    await source.dispose()

    target = f"gapforge_restore_{uuid4().hex[:12]}"
    service = BackupService(database_url=migrated_postgres_url, backups_dir=tmp_path)
    record = await service.create()
    await service.restore(record.name, target_database=target, confirmed=True)
    restored_url = (
        make_url(migrated_postgres_url).set(database=target).render_as_string(hide_password=False)
    )
    restored = Database.from_url(restored_url)
    try:
        async with restored.session() as session:
            persisted = await session.scalar(
                select(ResearchMission).where(ResearchMission.id == mission.id)
            )
            assert persisted is not None
            assert persisted.title == title
    finally:
        await restored.dispose()
        cleanup = await service._run(
            [
                service.dropdb_binary,
                *service.connection.maintenance_arguments(),
                "--if-exists",
                target,
            ],
            timeout_seconds=60,
        )
        assert cleanup.returncode == 0
