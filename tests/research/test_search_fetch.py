from collections.abc import AsyncIterator
from datetime import UTC, datetime

import httpx
import pytest

from gapforge.domain.contracts import Availability, ClaimKind, SearchResult
from gapforge.search.brave import ApprovedUrlRegistry, BraveSearchProvider, snippet_supports_claim
from gapforge.security.static_fetch import StaticFetcher


async def public_resolver(hostname: str) -> tuple[str, ...]:
    return ("93.184.216.34",)


@pytest.mark.asyncio
async def test_brave_missing_key_degrades_and_result_normalization() -> None:
    calls = 0

    def handler(req: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        assert req.headers["x-subscription-token"] == "key"
        return httpx.Response(200, json={"web": {"results": [
            {"title": "Acme pricing", "url": "https://acme.example/pricing", "description": "$20 plan"},
            {"title": "Bad URL", "url": "file:///etc/passwd", "description": "ignore"},
        ]}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        unavailable = await BraveSearchProvider(client, None).search("billing pain")
        available = await BraveSearchProvider(client, "key").search("billing pain")
    assert unavailable.availability is Availability.RESEARCH_UNAVAILABLE
    assert unavailable.request_count == 0
    assert len(available.results) == 1
    assert calls == 1
    assert not snippet_supports_claim(ClaimKind.PRICE)
    assert not snippet_supports_claim(ClaimKind.FEATURE)


@pytest.mark.asyncio
async def test_brave_result_ids_are_stable_by_url_and_do_not_collide_by_rank() -> None:
    responses = [
        {"title": "First", "url": "https://first.example/page", "description": "one"},
        {"title": "Second", "url": "https://second.example/page", "description": "two"},
        {"title": "First renamed", "url": "https://first.example/page", "description": "updated"},
    ]
    call = 0

    def handler(req: httpx.Request) -> httpx.Response:
        nonlocal call
        payload = responses[call]
        call += 1
        return httpx.Response(200, json={"web": {"results": [payload]}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        provider = BraveSearchProvider(client, "key")
        first = await provider.search("one")
        second = await provider.search("two")
        repeated = await provider.search("one again")
    assert first.results[0].id != second.results[0].id
    assert first.results[0].id == repeated.results[0].id


def approved_registry(url: str = "https://example.com/page") -> ApprovedUrlRegistry:
    return ApprovedUrlRegistry((SearchResult(
        id="s-1", title="Page", url=url, snippet="Page", observed_at=datetime(2026, 8, 9, tzinfo=UTC), rank=1,
    ),))


@pytest.mark.asyncio
@pytest.mark.parametrize("address", ["127.0.0.1", "169.254.169.254", "10.0.0.1", "::1"])
async def test_fetch_blocks_private_loopback_link_local_and_metadata(address: str) -> None:
    calls = 0

    async def resolver(hostname: str) -> tuple[str, ...]:
        return (address,)

    def handler(req: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, text="should not fetch", headers={"content-type": "text/plain"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await StaticFetcher(client, resolver=resolver).fetch("https://example.com/page", approved_registry())
    assert result.availability is Availability.CONTENT_UNAVAILABLE
    assert calls == 0


@pytest.mark.asyncio
async def test_fetch_pins_validated_ip_and_extracts_visible_text() -> None:
    def handler(req: httpx.Request) -> httpx.Response:
        assert req.url.host == "93.184.216.34"
        assert req.headers["host"] == "example.com"
        return httpx.Response(
            200,
            content=b"<html><style>hidden</style><body>Useful <b>workflow</b><script>bad()</script></body></html>",
            headers={"content-type": "text/html; charset=utf-8"},
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await StaticFetcher(client, resolver=public_resolver).fetch("https://example.com/page", approved_registry())
    assert result.availability is Availability.AVAILABLE
    assert result.snapshot is not None
    assert result.snapshot.text == "Useful workflow"
    assert len(result.snapshot.sha256) == 64


@pytest.mark.asyncio
async def test_redirect_to_private_and_dns_rebinding_seam_are_blocked() -> None:
    resolutions = {"example.com": ("93.184.216.34",), "internal.test": ("10.0.0.2",)}

    async def resolver(hostname: str) -> tuple[str, ...]:
        return resolutions[hostname]

    def handler(req: httpx.Request) -> httpx.Response:
        return httpx.Response(302, headers={"location": "http://internal.test/secret"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await StaticFetcher(client, resolver=resolver).fetch("https://example.com/page", approved_registry())
    assert result.availability is Availability.CONTENT_UNAVAILABLE


@pytest.mark.asyncio
async def test_oversized_unsupported_and_agent_created_urls_fail_closed() -> None:
    def handler(req: httpx.Request) -> httpx.Response:
        if req.url.path == "/pdf":
            return httpx.Response(200, content=b"pdf", headers={"content-type": "application/pdf"})
        return httpx.Response(200, content=b"x" * 11, headers={"content-type": "text/plain"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        fetcher = StaticFetcher(client, resolver=public_resolver, max_bytes=10)
        oversized = await fetcher.fetch("https://example.com/page", approved_registry())
        unsupported = await fetcher.fetch("https://example.com/pdf", approved_registry("https://example.com/pdf"))
        invented = await fetcher.fetch("https://attacker.example/", approved_registry())
    assert oversized.availability is Availability.CONTENT_UNAVAILABLE
    assert unsupported.availability is Availability.CONTENT_UNAVAILABLE
    assert invented.availability is Availability.CONTENT_UNAVAILABLE


@pytest.mark.asyncio
async def test_unsupported_response_stream_is_always_closed() -> None:
    class TrackingStream(httpx.AsyncByteStream):
        def __init__(self) -> None:
            self.closed = False

        async def __aiter__(self) -> AsyncIterator[bytes]:
            yield b"binary"

        async def aclose(self) -> None:
            self.closed = True

    stream = TrackingStream()

    def handler(req: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=stream, headers={"content-type": "application/octet-stream"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await StaticFetcher(client, resolver=public_resolver).fetch("https://example.com/page", approved_registry())
    assert result.availability is Availability.CONTENT_UNAVAILABLE
    assert stream.closed
