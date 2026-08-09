"""Privacy-preserving, source-scoped author identities."""

from __future__ import annotations

import hashlib
import hmac
import re
from dataclasses import dataclass

from gapforge.domain.contracts import Source

BOT_PATTERN = re.compile(r"(?:\[bot\]|[-_]?bot|automated|service)$", re.IGNORECASE)
UNKNOWN_IDENTITIES = {
    "",
    "anonymous",
    "anon",
    "deleted",
    "[deleted]",
    "unknown",
    "none",
    "null",
}


@dataclass(frozen=True, slots=True)
class AuthorIdentity:
    pseudonym: str | None
    known_human: bool
    reason: str


def pseudonymize_author(source: Source, identity: str | None, secret: bytes) -> AuthorIdentity:
    if len(secret) < 32:
        raise ValueError("author HMAC secret must be at least 32 bytes")
    normalized = " ".join((identity or "").strip().casefold().split())
    if normalized in UNKNOWN_IDENTITIES:
        return AuthorIdentity(None, False, "UNKNOWN")
    if BOT_PATTERN.search(normalized):
        return AuthorIdentity(None, False, "BOT_OR_SERVICE")
    digest = hmac.new(secret, f"{source.value}\0{normalized}".encode(), hashlib.sha256).hexdigest()
    return AuthorIdentity(f"author_{digest[:32]}", True, "KNOWN")
