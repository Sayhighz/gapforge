"""Crash-safe persistence for deterministic Markdown report artifacts."""

from __future__ import annotations

import fcntl
import hashlib
import os
import stat
import tempfile
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path

from gapforge.reports.renderers import RunReportData, render_run_report, run_report_paths

MAX_REPORT_BYTES = 5 * 1024 * 1024
MAX_REPORTS_PATH_BYTES = 4096

ReplaceFile = Callable[[Path, Path], None]


class ReportArtifactError(RuntimeError):
    """Base error for report artifact persistence."""


class ReportArtifactConflictError(ReportArtifactError):
    """A canonical immutable artifact already exists with different content."""


class ReportArtifactPathError(ReportArtifactError, ValueError):
    """The configured report destination or artifact path is unsafe."""


@dataclass(frozen=True, slots=True)
class RunReportArtifact:
    """Identity and digest of one persisted deterministic run report."""

    run_path: Path
    latest_path: Path
    content_sha256: str


def _replace_file(source: Path, destination: Path) -> None:
    os.replace(source, destination)


@dataclass(slots=True)
class ReportArtifactStore:
    """Persist immutable run reports and an atomically replaceable latest report."""

    reports_dir: Path
    max_report_bytes: int = MAX_REPORT_BYTES
    replace_file: ReplaceFile = field(default=_replace_file, repr=False)

    def __post_init__(self) -> None:
        self.reports_dir = Path(self.reports_dir)
        _validate_config_path(self.reports_dir)
        if not 1 <= self.max_report_bytes <= MAX_REPORT_BYTES:
            raise ReportArtifactPathError(
                f"max report size must be bounded between 1 and {MAX_REPORT_BYTES} bytes"
            )

    def persist_run(self, data: RunReportData) -> RunReportArtifact:
        """Render and durably persist a run report without overwriting changed history."""

        root = _prepare_directory(self.reports_dir)
        try:
            run_path, latest_path = run_report_paths(root, data.run_id, data.started_at)
        except ValueError as error:
            raise ReportArtifactPathError(str(error)) from error
        dated_directory = _prepare_directory(run_path.parent, expected_parent=root)
        run_path = dated_directory / run_path.name
        latest_path = root / latest_path.name
        payload = render_run_report(data).encode("utf-8")
        if not payload or len(payload) > self.max_report_bytes:
            raise ReportArtifactPathError(
                f"rendered report must be between 1 and {self.max_report_bytes} bytes"
            )
        if b"\x00" in payload:
            raise ReportArtifactPathError("rendered report contains a NUL byte")

        with _artifact_lock(root):
            existing_run = _read_regular_file(run_path, self.max_report_bytes)
            if existing_run is not None and existing_run != payload:
                raise ReportArtifactConflictError(
                    f"run report {run_path.name} already exists with different content"
                )
            if existing_run is None:
                _atomic_replace(run_path, payload, self.replace_file)

            existing_latest = _read_regular_file(latest_path, self.max_report_bytes)
            if existing_latest != payload:
                _atomic_replace(latest_path, payload, self.replace_file)

        return RunReportArtifact(
            run_path=run_path,
            latest_path=latest_path,
            content_sha256=hashlib.sha256(payload).hexdigest(),
        )


def _validate_config_path(path: Path) -> None:
    raw = os.fspath(path)
    if not raw or raw == ".":
        raise ReportArtifactPathError("reports directory must be a dedicated path")
    if "\x00" in raw:
        raise ReportArtifactPathError("reports directory contains a NUL byte")
    if len(os.fsencode(raw)) > MAX_REPORTS_PATH_BYTES:
        raise ReportArtifactPathError("reports directory path is not bounded")
    if ".." in path.parts:
        raise ReportArtifactPathError("reports directory path traversal is not allowed")


def _prepare_directory(path: Path, *, expected_parent: Path | None = None) -> Path:
    _refuse_existing_symlink_components(path)
    existed = path.exists()
    try:
        path.mkdir(parents=True, exist_ok=True)
    except OSError as error:
        raise ReportArtifactPathError(f"cannot create reports directory: {error}") from error
    _refuse_existing_symlink_components(path)
    if not path.is_dir():
        raise ReportArtifactPathError(f"report destination is not a directory: {path}")
    resolved = path.resolve(strict=True)
    if expected_parent is not None and resolved.parent != expected_parent.resolve(strict=True):
        raise ReportArtifactPathError("report destination escapes its configured directory")
    if expected_parent is not None and not existed:
        _fsync_directory(expected_parent)
    return path


def _refuse_existing_symlink_components(path: Path) -> None:
    absolute = path if path.is_absolute() else Path.cwd() / path
    candidates = (*reversed(absolute.parents), absolute)
    for candidate in candidates:
        if candidate.is_symlink():
            raise ReportArtifactPathError(
                f"symbolic link is not allowed in reports path: {candidate}"
            )


@contextmanager
def _artifact_lock(root: Path) -> Iterator[None]:
    lock_path = root / ".artifacts.lock"
    if lock_path.is_symlink():
        raise ReportArtifactPathError("report artifact lock cannot be a symbolic link")
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(lock_path, flags, 0o600)
    except OSError as error:
        raise ReportArtifactPathError(f"cannot open report artifact lock: {error}") from error
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def _read_regular_file(path: Path, max_bytes: int) -> bytes | None:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return None
    if not stat.S_ISREG(metadata.st_mode):
        raise ReportArtifactPathError(f"report artifact is not a regular file: {path}")
    if metadata.st_size > max_bytes:
        raise ReportArtifactPathError(f"existing report artifact exceeds {max_bytes} bytes")
    try:
        return path.read_bytes()
    except OSError as error:
        raise ReportArtifactPathError(f"cannot read report artifact: {error}") from error


def _atomic_replace(path: Path, payload: bytes, replace_file: ReplaceFile) -> None:
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        replace_file(temporary_path, path)
        _fsync_directory(path.parent)
    finally:
        temporary_path.unlink(missing_ok=True)


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_CLOEXEC", 0)
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
