"""Atomic custom-format PostgreSQL backups with checksums and tiered retention."""

from __future__ import annotations

import asyncio
import hashlib
import os
import re
import tempfile
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from pydantic import BaseModel, ConfigDict
from sqlalchemy.engine import make_url

from gapforge.providers.process import AsyncProcessRunner, ProcessResult

_BACKUP_NAME = re.compile(r"^gapforge-(\d{8}T\d{6}Z)-([0-9a-f]{8})\.dump$")
_DATABASE_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SAFE_ENV_KEYS = ("LANG", "LC_ALL", "PATH", "SSL_CERT_DIR", "SSL_CERT_FILE", "TZ")


class BackupError(RuntimeError):
    """A sanitized backup failure safe to return through the CLI."""


class BackupRecord(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str
    created_at: datetime
    size_bytes: int
    sha256: str | None


class BackupVerification(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str
    sha256: str
    size_bytes: int
    archive_readable: bool


class _Connection:
    def __init__(self, database_url: str) -> None:
        parsed = make_url(database_url)
        if not parsed.drivername.startswith("postgresql"):
            raise BackupError("backup requires a PostgreSQL DATABASE_URL")
        if not parsed.database or not parsed.username:
            raise BackupError("DATABASE_URL must include database and username")
        self.host = parsed.host or "localhost"
        self.port = parsed.port or 5432
        self.username = parsed.username
        self.password = parsed.password
        self.database = parsed.database

    def arguments(self, *, database: str | None = None) -> list[str]:
        return [
            "--host",
            self.host,
            "--port",
            str(self.port),
            "--username",
            self.username,
            "--dbname",
            database or self.database,
        ]

    def maintenance_arguments(self) -> list[str]:
        return [
            "--host",
            self.host,
            "--port",
            str(self.port),
            "--username",
            self.username,
            "--maintenance-db",
            self.database,
        ]


class BackupService:
    """Run PostgreSQL client tools without exposing credentials in process arguments."""

    def __init__(
        self,
        *,
        database_url: str,
        backups_dir: Path,
        runner: AsyncProcessRunner | None = None,
        parent_env: dict[str, str] | None = None,
        now: Callable[[], datetime] | None = None,
        pg_dump_binary: str = "pg_dump",
        pg_restore_binary: str = "pg_restore",
        createdb_binary: str = "createdb",
        dropdb_binary: str = "dropdb",
    ) -> None:
        self.connection = _Connection(database_url)
        self.backups_dir = backups_dir
        self.runner = runner or AsyncProcessRunner()
        self.parent_env = dict(parent_env if parent_env is not None else os.environ)
        self.now = now or (lambda: datetime.now(UTC))
        self.pg_dump_binary = pg_dump_binary
        self.pg_restore_binary = pg_restore_binary
        self.createdb_binary = createdb_binary
        self.dropdb_binary = dropdb_binary

    async def create(self) -> BackupRecord:
        root = self._safe_root(create=True)
        await self._require_pg_dump_16()
        created_at = self.now().astimezone(UTC).replace(microsecond=0)
        name = f"gapforge-{created_at:%Y%m%dT%H%M%SZ}-{uuid4().hex[:8]}.dump"
        archive = root / name
        checksum_path = archive.with_suffix(".dump.sha256")
        temporary_archive = self._temporary_path(root, suffix=".dump")
        temporary_checksum = self._temporary_path(root, suffix=".sha256")
        try:
            result = await self._run(
                [
                    self.pg_dump_binary,
                    *self.connection.arguments(),
                    "--format=custom",
                    "--compress=zstd:6",
                    "--no-owner",
                    "--no-privileges",
                    "--file",
                    str(temporary_archive),
                ],
                timeout_seconds=900,
            )
            self._require_success(result, "pg_dump")
            checksum = await asyncio.to_thread(self._hash_file, temporary_archive)
            await self._validate_archive(temporary_archive, checksum)
            temporary_checksum.write_text(f"{checksum}  {name}\n", encoding="ascii")
            os.replace(temporary_checksum, checksum_path)
            os.replace(temporary_archive, archive)
        finally:
            temporary_archive.unlink(missing_ok=True)
            temporary_checksum.unlink(missing_ok=True)
        record = self._record(archive)
        await self.apply_retention()
        return record

    def list_backups(self) -> tuple[BackupRecord, ...]:
        root = self._safe_root(create=True)
        records: list[BackupRecord] = []
        for archive in root.glob("gapforge-*.dump"):
            if (
                archive.is_symlink()
                or not archive.is_file()
                or not _BACKUP_NAME.fullmatch(archive.name)
            ):
                continue
            records.append(self._record(archive))
        return tuple(sorted(records, key=lambda item: (item.created_at, item.name), reverse=True))

    async def verify(self, name: str) -> BackupVerification:
        archive = self._archive_path(name)
        expected = self._read_checksum(archive)
        actual = await self._validate_archive(archive, expected)
        return BackupVerification(
            name=archive.name,
            sha256=actual,
            size_bytes=archive.stat().st_size,
            archive_readable=True,
        )

    async def restore(self, name: str, *, target_database: str, confirmed: bool) -> dict[str, str]:
        if not confirmed:
            raise BackupError("restore requires explicit --yes confirmation")
        if not _DATABASE_NAME.fullmatch(target_database):
            raise BackupError("target database name is invalid")
        await self.verify(name)
        archive = self._archive_path(name)
        create_result = await self._run(
            [
                self.createdb_binary,
                *self.connection.maintenance_arguments(),
                "--encoding=UTF8",
                target_database,
            ],
            timeout_seconds=60,
        )
        self._require_success(create_result, "createdb (target must not already exist)")
        try:
            restore_result = await self._run(
                [
                    self.pg_restore_binary,
                    *self.connection.arguments(database=target_database),
                    "--exit-on-error",
                    "--no-owner",
                    "--no-privileges",
                    str(archive),
                ],
                timeout_seconds=900,
            )
            self._require_success(restore_result, "pg_restore")
        except Exception:
            await self._run(
                [
                    self.dropdb_binary,
                    *self.connection.maintenance_arguments(),
                    "--if-exists",
                    target_database,
                ],
                timeout_seconds=60,
            )
            raise
        return {"backup": archive.name, "target_database": target_database, "status": "restored"}

    async def apply_retention(self) -> tuple[str, ...]:
        """Keep the union of newest 7 daily, 4 ISO-weekly, and 6 monthly buckets."""

        records = self.list_backups()
        keep: set[str] = set()
        buckets: tuple[tuple[int, Callable[[datetime], object]], ...] = (
            (7, lambda value: value.date()),
            (4, lambda value: value.isocalendar()[:2]),
            (6, lambda value: (value.year, value.month)),
        )
        for limit, bucket_key in buckets:
            seen: set[object] = set()
            for record in records:
                key = bucket_key(record.created_at)
                if key in seen:
                    continue
                seen.add(key)
                keep.add(record.name)
                if len(seen) == limit:
                    break
        removed: list[str] = []
        for record in records:
            if record.name in keep:
                continue
            archive = self._archive_path(record.name)
            checksum = self._checksum_path(archive)
            archive.unlink()
            checksum.unlink(missing_ok=True)
            removed.append(record.name)
        return tuple(removed)

    async def _validate_archive(self, archive: Path, expected_checksum: str) -> str:
        actual = await asyncio.to_thread(self._hash_file, archive)
        if actual != expected_checksum:
            raise BackupError("backup checksum mismatch")
        result = await self._run(
            [self.pg_restore_binary, "--list", str(archive)], timeout_seconds=120
        )
        self._require_success(result, "pg_restore archive verification")
        return actual

    async def _require_pg_dump_16(self) -> None:
        result = await self._run([self.pg_dump_binary, "--version"], timeout_seconds=15)
        self._require_success(result, "pg_dump version check")
        match = re.search(rb"PostgreSQL\)\s+(\d+)", result.stdout)
        if match is None or int(match.group(1)) < 16:
            raise BackupError("pg_dump major version 16 or newer is required")

    async def _run(self, command: list[str], *, timeout_seconds: float) -> ProcessResult:
        environment = {
            key: self.parent_env[key] for key in _SAFE_ENV_KEYS if self.parent_env.get(key)
        }
        if self.connection.password:
            environment["PGPASSWORD"] = self.connection.password
        return await self.runner.run(
            command,
            stdin=b"",
            env=environment,
            cwd=self._safe_root(create=True),
            timeout_seconds=timeout_seconds,
            max_output_bytes=65_536,
        )

    @staticmethod
    def _require_success(result: ProcessResult, operation: str) -> None:
        if result.timed_out:
            raise BackupError(f"{operation} timed out")
        if result.returncode != 0:
            raise BackupError(f"{operation} failed with exit code {result.returncode}")

    def _safe_root(self, *, create: bool) -> Path:
        if self.backups_dir.exists() and self.backups_dir.is_symlink():
            raise BackupError("BACKUPS_DIR cannot be a symbolic link")
        if create:
            self.backups_dir.mkdir(parents=True, exist_ok=True)
        if not self.backups_dir.is_dir():
            raise BackupError("BACKUPS_DIR is not a directory")
        return self.backups_dir.resolve(strict=True)

    def _archive_path(self, name: str) -> Path:
        if Path(name).name != name or not _BACKUP_NAME.fullmatch(name):
            raise BackupError("backup name is invalid")
        candidate = self._safe_root(create=False) / name
        if candidate.is_symlink() or not candidate.is_file():
            raise BackupError("backup does not exist or is not a regular file")
        return candidate

    def _checksum_path(self, archive: Path) -> Path:
        checksum = archive.with_suffix(".dump.sha256")
        if checksum.is_symlink():
            raise BackupError("backup checksum cannot be a symbolic link")
        return checksum

    def _read_checksum(self, archive: Path) -> str:
        checksum_path = self._checksum_path(archive)
        if not checksum_path.is_file():
            raise BackupError("backup checksum is missing")
        try:
            checksum, filename = checksum_path.read_text(encoding="ascii").strip().split("  ", 1)
        except (UnicodeDecodeError, ValueError) as error:
            raise BackupError("backup checksum is malformed") from error
        if not _SHA256.fullmatch(checksum) or filename != archive.name:
            raise BackupError("backup checksum is malformed")
        return checksum

    def _record(self, archive: Path) -> BackupRecord:
        match = _BACKUP_NAME.fullmatch(archive.name)
        if match is None:
            raise BackupError("backup name is invalid")
        created_at = datetime.strptime(match.group(1), "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
        try:
            checksum = self._read_checksum(archive)
        except BackupError:
            checksum = None
        return BackupRecord(
            name=archive.name,
            created_at=created_at,
            size_bytes=archive.stat().st_size,
            sha256=checksum,
        )

    @staticmethod
    def _hash_file(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    @staticmethod
    def _temporary_path(root: Path, *, suffix: str) -> Path:
        descriptor, raw_path = tempfile.mkstemp(prefix=".gapforge-backup-", suffix=suffix, dir=root)
        os.close(descriptor)
        return Path(raw_path)
