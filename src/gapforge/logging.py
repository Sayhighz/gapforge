"""Structured logging with defensive credential redaction."""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from gapforge.config import Settings

REDACTED = "[REDACTED]"
_SENSITIVE_FRAGMENTS = (
    "authorization",
    "cookie",
    "credential",
    "password",
    "secret",
    "token",
    "api_key",
    "apikey",
    "database_url",
)
_STANDARD_RECORD_FIELDS = frozenset(logging.makeLogRecord({}).__dict__)


def _sensitive_key(key: str) -> bool:
    normalized = key.casefold().replace("-", "_")
    return any(fragment in normalized for fragment in _SENSITIVE_FRAGMENTS)


def redact(value: Any, secrets: frozenset[str] = frozenset(), *, key: str = "") -> Any:
    """Recursively redact secret-bearing fields and configured secret values."""

    if key and _sensitive_key(key):
        return REDACTED
    if isinstance(value, str):
        result = value
        for secret in secrets:
            if secret:
                result = result.replace(secret, REDACTED)
        return result
    if isinstance(value, Mapping):
        return {
            str(item_key): redact(item, secrets, key=str(item_key))
            for item_key, item in value.items()
        }
    if isinstance(value, tuple):
        return tuple(redact(item, secrets) for item in value)
    if isinstance(value, list):
        return [redact(item, secrets) for item in value]
    return value


class JsonFormatter(logging.Formatter):
    """One-record-per-line JSON formatter suitable for worker logs."""

    def __init__(self, secrets: frozenset[str] = frozenset()) -> None:
        super().__init__()
        self._secrets = secrets

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        for key, value in record.__dict__.items():
            if key not in _STANDARD_RECORD_FIELDS and key not in {"message", "asctime"}:
                payload[key] = value
        return json.dumps(redact(payload, self._secrets), ensure_ascii=False, default=str)


def configure_logging(settings: Settings) -> None:
    """Configure the root logger without allowing diagnostics on stdout."""

    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter(settings.secret_values()))
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(settings.log_level.upper())
