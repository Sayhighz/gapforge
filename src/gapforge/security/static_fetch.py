"""SSRF-resistant, bounded static web fetcher."""

from __future__ import annotations

import hashlib
import ipaddress
import socket
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from html.parser import HTMLParser
from urllib.parse import urljoin, urlsplit, urlunsplit

import httpx
from pydantic import HttpUrl

from gapforge.domain.contracts import Availability, FetchResult, FetchSnapshot, SourceWarning
from gapforge.search.brave import ApprovedUrlRegistry

Resolver = Callable[[str], Awaitable[tuple[str, ...]]]
ALLOWED_CONTENT_TYPES = {"text/html", "text/plain"}


class UnsafeUrlError(ValueError):
    pass


class ContentUnavailableError(RuntimeError):
    pass


async def system_resolver(hostname: str) -> tuple[str, ...]:
    loop = __import__("asyncio").get_running_loop()
    records = await loop.getaddrinfo(hostname, None, type=socket.SOCK_STREAM)
    return tuple(sorted({str(record[4][0]) for record in records}))


def public_ip(value: str) -> bool:
    address = ipaddress.ip_address(value)
    return not (
        address.is_private
        or address.is_loopback
        or address.is_link_local
        or address.is_multicast
        or address.is_reserved
        or address.is_unspecified
    )


def validate_url(url: str) -> tuple[str, int | None]:
    parsed = urlsplit(url)
    if parsed.scheme not in {"http", "https"}:
        raise UnsafeUrlError("only http and https URLs are allowed")
    if not parsed.hostname or parsed.username or parsed.password:
        raise UnsafeUrlError("URL must have a host and no credentials")
    try:
        port = parsed.port
    except ValueError as exc:
        raise UnsafeUrlError("URL port is invalid") from exc
    if port not in {None, 80, 443}:
        raise UnsafeUrlError("only standard HTTP ports are allowed")
    return parsed.hostname, port


class _VisibleTextParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._hidden_depth = 0
        self.parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() in {"script", "style", "noscript", "template", "svg"}:
            self._hidden_depth += 1

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() in {"script", "style", "noscript", "template", "svg"} and self._hidden_depth:
            self._hidden_depth -= 1

    def handle_data(self, data: str) -> None:
        if not self._hidden_depth and data.strip():
            self.parts.append(data.strip())

    def text(self) -> str:
        return " ".join(" ".join(self.parts).split())


class StaticFetcher:
    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        resolver: Resolver = system_resolver,
        max_redirects: int = 5,
        max_bytes: int = 1_000_000,
        timeout_seconds: float = 10.0,
    ) -> None:
        if not 0 <= max_redirects <= 5 or not 1 <= max_bytes <= 2_000_000 or not 0 < timeout_seconds <= 30:
            raise ValueError("fetch limits exceed safe bounds")
        self._client = client
        self._resolver = resolver
        self._max_redirects = max_redirects
        self._max_bytes = max_bytes
        self._timeout = httpx.Timeout(timeout_seconds, connect=min(5.0, timeout_seconds))

    async def fetch(self, url: str, registry: ApprovedUrlRegistry) -> FetchResult:
        try:
            current = registry.require_approved(url)
            original = current
            response: httpx.Response | None = None
            for redirect_count in range(self._max_redirects + 1):
                response = await self._request_pinned(current)
                if response.is_redirect:
                    if redirect_count >= self._max_redirects:
                        raise ContentUnavailableError("redirect limit exceeded")
                    location = response.headers.get("location")
                    if not location:
                        raise ContentUnavailableError("redirect missing location")
                    await response.aclose()
                    current = urljoin(current, location)
                    continue
                break
            assert response is not None
            response.raise_for_status()
            content_type = response.headers.get("content-type", "").split(";", 1)[0].lower()
            if content_type not in ALLOWED_CONTENT_TYPES:
                raise ContentUnavailableError("unsupported content type")
            body = await self._bounded_body(response)
            text = body.decode(response.encoding or "utf-8", errors="replace")
            if content_type == "text/html":
                parser = _VisibleTextParser()
                parser.feed(text)
                text = parser.text()
            text = " ".join(text.split())
            if not text:
                raise ContentUnavailableError("page contains no visible static text")
            return FetchResult(
                availability=Availability.AVAILABLE,
                snapshot=FetchSnapshot(
                    url=HttpUrl(original),
                    final_url=HttpUrl(current),
                    text=text[:20_000],
                    content_type=content_type,
                    sha256=hashlib.sha256(body).hexdigest(),
                    observed_at=datetime.now(UTC),
                ),
            )
        except (UnsafeUrlError, ContentUnavailableError, httpx.HTTPError, ValueError) as exc:
            return FetchResult(
                availability=Availability.CONTENT_UNAVAILABLE,
                warnings=(
                    SourceWarning(
                        code="CONTENT_UNAVAILABLE",
                        message=f"static fetch unavailable: {type(exc).__name__}",
                        retryable=isinstance(exc, (httpx.TimeoutException, httpx.NetworkError)),
                    ),
                ),
            )

    async def _request_pinned(self, url: str) -> httpx.Response:
        hostname, port = validate_url(url)
        addresses = await self._resolver(hostname)
        if not addresses or any(not public_ip(address) for address in addresses):
            raise UnsafeUrlError("DNS returned a non-public address")
        address = addresses[0]
        parsed = urlsplit(url)
        display_address = f"[{address}]" if ":" in address else address
        netloc = display_address + (f":{port}" if port else "")
        pinned_url = urlunsplit((parsed.scheme, netloc, parsed.path or "/", parsed.query, ""))
        host_header = hostname + (f":{port}" if port else "")
        request = self._client.build_request(
            "GET",
            pinned_url,
            headers={"Host": host_header, "Accept": "text/html,text/plain", "User-Agent": "GapForge/0.1"},
            timeout=self._timeout,
        )
        request.extensions["sni_hostname"] = hostname.encode("idna")
        return await self._client.send(request, stream=True, follow_redirects=False)

    async def _bounded_body(self, response: httpx.Response) -> bytes:
        declared = response.headers.get("content-length")
        if declared and int(declared) > self._max_bytes:
            await response.aclose()
            raise ContentUnavailableError("response exceeds byte limit")
        body = bytearray()
        async for chunk in response.aiter_bytes():
            body.extend(chunk)
            if len(body) > self._max_bytes:
                await response.aclose()
                raise ContentUnavailableError("response exceeds byte limit")
        await response.aclose()
        return bytes(body)
