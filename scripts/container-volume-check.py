#!/usr/bin/env python3
"""Runtime checks for the dedicated Compose Codex authentication volume."""

from __future__ import annotations

import errno
import os
import stat
import sys
from pathlib import Path

MARKER_NAME = ".gapforge-volume-check"
MARKER_CONTENT = b"gapforge-auth-volume-check-v1\n"
FORBIDDEN_ENV = (
    "AUTHOR_HMAC_KEY",
    "BRAVE_API_KEY",
    "DATABASE_URL",
    "GITHUB_TOKEN",
    "REDDIT_CLIENT_ID",
    "REDDIT_CLIENT_SECRET",
)


def codex_home() -> Path:
    path = Path(os.environ.get("CODEX_HOME", ""))
    if not path.is_absolute() or path.is_symlink() or not path.is_dir():
        raise RuntimeError("CODEX_HOME must be an existing absolute non-symlink directory")
    metadata = path.stat()
    if metadata.st_uid != os.getuid() or metadata.st_gid != os.getgid():
        raise RuntimeError("CODEX_HOME must be owned by the non-root container user")
    return path


def write_marker() -> None:
    path = codex_home() / MARKER_NAME
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
    try:
        os.write(descriptor, MARKER_CONTENT)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    if stat.S_IMODE(path.stat().st_mode) != 0o600:
        raise RuntimeError("auth-volume marker permissions are not 0600")


def verify_readonly() -> None:
    root = codex_home()
    if any(os.environ.get(key) for key in FORBIDDEN_ENV):
        raise RuntimeError("auth-only service inherited an application or source secret")
    marker = root / MARKER_NAME
    if marker.is_symlink() or marker.read_bytes() != MARKER_CONTENT:
        raise RuntimeError("auth-volume marker did not survive container recreation")
    probe = root / ".gapforge-readonly-probe"
    try:
        probe.write_bytes(b"must-not-write")
    except OSError as error:
        if error.errno not in {errno.EACCES, errno.EROFS}:
            raise
    else:
        probe.unlink(missing_ok=True)
        raise RuntimeError("codex-status auth volume is unexpectedly writable")


def main() -> int:
    if os.getuid() == 0 or os.getgid() == 0:
        raise RuntimeError("auth-volume checks must run as the non-root application user")
    if sys.argv[1:] == ["auth-write"]:
        write_marker()
        return 0
    if sys.argv[1:] == ["auth-readonly"]:
        verify_readonly()
        return 0
    raise RuntimeError("expected auth-write or auth-readonly")


if __name__ == "__main__":
    raise SystemExit(main())
