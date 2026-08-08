"""Pure error classification and bounded retry timing."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from enum import StrEnum


class ErrorKind(StrEnum):
    AUTH_REQUIRED = "AUTH_REQUIRED"
    BUDGET_EXHAUSTED = "BUDGET_EXHAUSTED"
    DEADLINE_EXCEEDED = "DEADLINE_EXCEEDED"
    INTEGRITY = "INTEGRITY"
    INVALID_OUTPUT = "INVALID_OUTPUT"
    RATE_LIMITED = "RATE_LIMITED"
    TIMEOUT = "TIMEOUT"
    TRANSIENT_NETWORK = "TRANSIENT_NETWORK"
    SOURCE_UNAVAILABLE = "SOURCE_UNAVAILABLE"
    PERMANENT = "PERMANENT"


@dataclass(frozen=True, slots=True)
class RetryDecision:
    kind: ErrorKind
    retryable: bool
    delay_seconds: float = 0.0


def retry_delay_seconds(attempt: int, seed: str, *, base_seconds: float = 1.0) -> float:
    """Return capped exponential delay with deterministic 0-25% jitter."""

    if attempt < 1:
        raise ValueError("attempt must be at least 1")
    digest = hashlib.sha256(f"{seed}:{attempt}".encode()).digest()
    jitter = int.from_bytes(digest[:2]) / 65535 * 0.25
    exponential = base_seconds * float(2 ** (attempt - 1))
    return min(exponential * (1 + jitter), 60.0)


def classify_error(kind: ErrorKind, *, attempt: int, seed: str) -> RetryDecision:
    """Classify retry behavior without consulting mutable process state."""

    retryable = kind in {
        ErrorKind.RATE_LIMITED,
        ErrorKind.TIMEOUT,
        ErrorKind.TRANSIENT_NETWORK,
    }
    return RetryDecision(
        kind=kind,
        retryable=retryable,
        delay_seconds=retry_delay_seconds(attempt, seed) if retryable else 0.0,
    )
