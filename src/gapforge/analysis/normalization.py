"""Pure normalization and immutable raw-signal revision helpers."""

from __future__ import annotations

import hashlib
import ipaddress
import re
import unicodedata
from datetime import datetime
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from gapforge.domain.contracts import RawSignalRevision

TRACKING_KEYS = {"fbclid", "gclid", "mc_cid", "mc_eid", "ref", "source"}


def normalize_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", value)
    return " ".join(normalized.casefold().split())


def content_hash(title: str | None, body: str | None) -> str:
    canonical = f"{normalize_text(title or '')}\n{normalize_text(body or '')}"
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def normalize_url(value: str) -> str:
    parsed = urlsplit(value.strip())
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.hostname:
        raise ValueError("URL must be absolute HTTP(S)")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("URL credentials are not allowed")
    raw_host = parsed.hostname
    try:
        address = ipaddress.ip_address(raw_host)
        host = f"[{address.compressed}]" if address.version == 6 else address.compressed
    except ValueError:
        host = raw_host.encode("idna").decode("ascii").lower()
    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError("URL port is invalid") from exc
    if (parsed.scheme.lower(), port) in {("http", 80), ("https", 443)}:
        port = None
    netloc = host + (f":{port}" if port else "")
    path = re.sub(r"/{2,}", "/", parsed.path or "/")
    if path != "/":
        path = path.rstrip("/")
    query = urlencode(
        sorted(
            (key, value)
            for key, value in parse_qsl(parsed.query, keep_blank_values=True)
            if key.lower() not in TRACKING_KEYS and not key.lower().startswith("utm_")
        )
    )
    return urlunsplit((parsed.scheme.lower(), netloc, path, query, ""))


def append_revision(
    history: tuple[RawSignalRevision, ...],
    *,
    raw_signal_id: str,
    title: str | None,
    body: str | None,
    observed_at: datetime,
    deleted: bool = False,
) -> tuple[RawSignalRevision, ...]:
    digest = content_hash(None, "[deleted]") if deleted else content_hash(title, body)
    if (
        history
        and history[-1].content_hash == digest
        and history[-1].tombstone == deleted
    ):
        return history
    revision = RawSignalRevision(
        id=f"{raw_signal_id}:r{len(history) + 1}",
        raw_signal_id=raw_signal_id,
        revision=len(history) + 1,
        title=None if deleted else title,
        body=None if deleted else body,
        content_hash=digest,
        observed_at=observed_at,
        tombstone=deleted,
    )
    return (*history, revision)
