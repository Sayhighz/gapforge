"""Deterministic exact and MinHash near-duplicate detection without embeddings."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

from gapforge.analysis.normalization import content_hash, normalize_text, normalize_url
from gapforge.domain.contracts import CollectedItem


@dataclass(frozen=True, slots=True)
class DedupKey:
    source_external_id: tuple[str, str]
    normalized_url: str
    normalized_content_hash: str


@dataclass(frozen=True, slots=True)
class LexicalCandidateQuery:
    normalized_text: str
    terms: tuple[str, ...]
    trigram_threshold: float = 0.65
    limit: int = 50


def dedup_key(item: CollectedItem) -> DedupKey:
    return DedupKey(
        source_external_id=(item.source.value, item.external_id),
        normalized_url=normalize_url(str(item.canonical_url)),
        normalized_content_hash=content_hash(item.title, item.body),
    )


def exact_duplicate_reason(left: DedupKey, right: DedupKey) -> str | None:
    if left.source_external_id == right.source_external_id:
        return "SOURCE_EXTERNAL_ID"
    if left.normalized_url == right.normalized_url:
        return "NORMALIZED_URL"
    if left.normalized_content_hash == right.normalized_content_hash:
        return "CONTENT_HASH"
    return None


def _shingles(value: str, width: int = 3) -> set[str]:
    words = normalize_text(value).split()
    if len(words) < width:
        return {" ".join(words)} if words else set()
    return {
        " ".join(words[index : index + width])
        for index in range(len(words) - width + 1)
    }


def minhash_signature(value: str, permutations: int = 64) -> tuple[int, ...]:
    if not 16 <= permutations <= 256:
        raise ValueError("permutations must be between 16 and 256")
    shingles = _shingles(value)
    if not shingles:
        return tuple(0 for _ in range(permutations))
    return tuple(
        min(
            int.from_bytes(
                hashlib.blake2b(
                    shingle.encode(), digest_size=8, person=index.to_bytes(8, "big")
                ).digest(),
                "big",
            )
            for shingle in shingles
        )
        for index in range(permutations)
    )


def minhash_similarity(left: tuple[int, ...], right: tuple[int, ...]) -> float:
    if not left or len(left) != len(right):
        raise ValueError("MinHash signatures must be non-empty and equal length")
    return sum(a == b for a, b in zip(left, right, strict=True)) / len(left)


def lexical_candidate_query(value: str) -> LexicalCandidateQuery:
    normalized = normalize_text(value)[:2_000]
    terms = tuple(
        sorted(set(normalized.split()), key=lambda term: (-len(term), term))[:20]
    )
    return LexicalCandidateQuery(normalized_text=normalized, terms=terms)
