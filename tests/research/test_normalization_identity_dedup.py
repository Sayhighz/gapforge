from datetime import UTC, datetime

import pytest

from gapforge.analysis.deduplication import (
    dedup_key,
    exact_duplicate_reason,
    minhash_signature,
    minhash_similarity,
)
from gapforge.analysis.identity import pseudonymize_author
from gapforge.analysis.normalization import (
    append_revision,
    normalize_text,
    normalize_url,
)
from gapforge.domain.contracts import CollectedItem, Source

NOW = datetime(2026, 8, 9, tzinfo=UTC)


def item(identifier: str, url: str, body: str) -> CollectedItem:
    return CollectedItem(
        source=Source.GITHUB,
        external_id=identifier,
        canonical_url=url,
        body=body,
        source_created_at=NOW,
    )


def test_url_text_and_exact_duplicate_order_are_deterministic() -> None:
    assert normalize_text("  Café\nWORK  ") == "café work"
    assert (
        normalize_url("HTTPS://Example.COM:443/a//b/?utm_source=x&b=2&a=1#frag")
        == "https://example.com/a/b?a=1&b=2"
    )
    left = dedup_key(item("1", "https://example.com/post?utm_source=x", "Manual work"))
    assert (
        exact_duplicate_reason(
            left, dedup_key(item("1", "https://other.example/post", "different"))
        )
        == "SOURCE_EXTERNAL_ID"
    )
    assert (
        exact_duplicate_reason(
            left, dedup_key(item("2", "https://example.com/post", "different"))
        )
        == "NORMALIZED_URL"
    )
    assert (
        exact_duplicate_reason(
            left, dedup_key(item("3", "https://third.example/post", "manual  work"))
        )
        == "CONTENT_HASH"
    )
    assert normalize_url("https://[2001:db8::1]:443/a") == "https://[2001:db8::1]/a"
    with pytest.raises(ValueError, match="credentials"):
        normalize_url("https://user:secret@example.com/path")


def test_minhash_near_duplicate_is_stable_and_bounded() -> None:
    left = minhash_signature("manual invoice export takes several hours every friday")
    right = minhash_signature(
        "manual invoice export takes several hours every friday for accountants"
    )
    assert left == minhash_signature(
        "manual invoice export takes several hours every friday"
    )
    assert minhash_similarity(left, right) > 0.4
    with pytest.raises(ValueError):
        minhash_signature("x", permutations=2)


def test_author_pseudonyms_are_source_scoped_and_unknown_bots_do_not_count() -> None:
    secret = b"s" * 32
    first = pseudonymize_author(Source.GITHUB, " Alice ", secret)
    same = pseudonymize_author(Source.GITHUB, "alice", secret)
    other_source = pseudonymize_author(Source.REDDIT, "alice", secret)
    assert first == same
    assert first.pseudonym != other_source.pseudonym
    assert (
        pseudonymize_author(Source.GITHUB, "dependabot[bot]", secret).known_human
        is False
    )
    assert pseudonymize_author(Source.GITHUB, "[deleted]", secret).pseudonym is None
    with pytest.raises(ValueError, match="32 bytes"):
        pseudonymize_author(Source.GITHUB, "alice", b"short")


def test_edits_and_tombstones_append_history_without_duplicate_revisions() -> None:
    history = append_revision(
        (), raw_signal_id="raw-1", title="Pain", body="old", observed_at=NOW
    )
    unchanged = append_revision(
        history, raw_signal_id="raw-1", title=" pain ", body="OLD", observed_at=NOW
    )
    edited = append_revision(
        unchanged, raw_signal_id="raw-1", title="Pain", body="new", observed_at=NOW
    )
    deleted = append_revision(
        edited,
        raw_signal_id="raw-1",
        title=None,
        body=None,
        observed_at=NOW,
        deleted=True,
    )
    assert len(history) == len(unchanged) == 1
    assert [revision.revision for revision in deleted] == [1, 2, 3]
    assert deleted[-1].tombstone
