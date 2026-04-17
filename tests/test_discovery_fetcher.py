from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import httpx
import pytest

from subsystem_announcement.discovery.envelope import AnnouncementEnvelope
from subsystem_announcement.discovery.errors import (
    DocumentFetchError,
    NonOfficialSourceError,
)
from subsystem_announcement.discovery.fetcher import (
    fetch_official_document,
    validate_official_url,
)


def _envelope(url: str = "https://static.sse.com.cn/disclosure/ann-1.pdf"):
    return AnnouncementEnvelope(
        announcement_id="ann-1",
        ts_code="600000.SH",
        title="重大合同公告",
        publish_time=datetime(2026, 4, 18, 9, 30, tzinfo=timezone.utc),
        official_url=url,
        source_exchange="sse",
        attachment_type="pdf",
    )


def test_validate_official_url_rejects_non_official_domain() -> None:
    envelope = _envelope("https://news.example.com/ann-1.pdf")

    with pytest.raises(NonOfficialSourceError, match="ann-1"):
        validate_official_url(envelope)


def test_fetch_rejects_non_official_domain_before_network_request() -> None:
    request_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal request_count
        request_count += 1
        return httpx.Response(200, content=b"unexpected")

    async def scenario() -> None:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
        ) as client:
            with pytest.raises(NonOfficialSourceError):
                await fetch_official_document(
                    _envelope("https://news.example.com/ann-1.pdf"),
                    client=client,
                )

    asyncio.run(scenario())

    assert request_count == 0


def test_fetch_official_document_returns_200_body() -> None:
    async def scenario() -> bytes:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda request: httpx.Response(200, content=b"pdf bytes"),
            ),
        ) as client:
            return await fetch_official_document(_envelope(), client=client)

    assert asyncio.run(scenario()) == b"pdf bytes"


def test_fetch_official_document_fails_404_without_retry() -> None:
    request_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal request_count
        request_count += 1
        return httpx.Response(404, content=b"missing")

    async def scenario() -> None:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
        ) as client:
            with pytest.raises(DocumentFetchError, match="status_code=404"):
                await fetch_official_document(_envelope(), client=client)

    asyncio.run(scenario())

    assert request_count == 1


def test_fetch_official_document_retries_429_then_succeeds() -> None:
    statuses = [429, 429, 200]

    def handler(request: httpx.Request) -> httpx.Response:
        status = statuses.pop(0)
        return httpx.Response(status, content=b"ok" if status == 200 else b"retry")

    async def scenario() -> bytes:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
        ) as client:
            return await fetch_official_document(_envelope(), client=client)

    assert asyncio.run(scenario()) == b"ok"
    assert statuses == []


def test_fetch_official_document_retries_5xx_then_succeeds() -> None:
    statuses = [503, 502, 200]

    def handler(request: httpx.Request) -> httpx.Response:
        status = statuses.pop(0)
        return httpx.Response(status, content=b"ok" if status == 200 else b"retry")

    async def scenario() -> bytes:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
        ) as client:
            return await fetch_official_document(_envelope(), client=client)

    assert asyncio.run(scenario()) == b"ok"
    assert statuses == []


def test_fetch_official_document_reports_timeout_after_bounded_attempts() -> None:
    request_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal request_count
        request_count += 1
        raise httpx.ReadTimeout("slow official source", request=request)

    async def scenario() -> None:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
        ) as client:
            with pytest.raises(DocumentFetchError, match="attempts=2"):
                await fetch_official_document(
                    _envelope(),
                    client=client,
                    timeout_seconds=0.1,
                    max_attempts=2,
                )

    asyncio.run(scenario())

    assert request_count == 2
