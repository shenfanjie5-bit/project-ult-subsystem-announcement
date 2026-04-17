"""Official source validation and byte fetching for announcements."""

from __future__ import annotations

import asyncio
from collections.abc import Iterable
from urllib.parse import urlsplit

import httpx

from .envelope import AnnouncementEnvelope
from .errors import DocumentFetchError, NonOfficialSourceError

_OFFICIAL_DISCLOSURE_DOMAINS: frozenset[str] = frozenset(
    {
        "bse.cn",
        "cninfo.com.cn",
        "neeq.com.cn",
        "sse.com.cn",
        "szse.cn",
    }
)


def validate_official_url(envelope: AnnouncementEnvelope) -> None:
    """Reject non-official disclosure URLs before any network request."""

    url = str(envelope.official_url)
    parsed = urlsplit(url)
    host = (parsed.hostname or "").lower().rstrip(".")
    scheme = parsed.scheme.lower()
    if scheme not in {"http", "https"} or not _is_official_host(host):
        raise NonOfficialSourceError(
            "Non-official announcement URL rejected before fetch: "
            f"announcement_id={envelope.announcement_id} url={url}"
        )


async def fetch_official_document(
    envelope: AnnouncementEnvelope,
    *,
    client: httpx.AsyncClient | None = None,
    timeout_seconds: float = 30.0,
    max_attempts: int = 3,
) -> bytes:
    """Fetch official announcement document bytes using httpx only."""

    validate_official_url(envelope)
    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")

    if client is not None:
        return await _fetch_with_client(
            envelope,
            client=client,
            timeout_seconds=timeout_seconds,
            max_attempts=max_attempts,
        )

    async with httpx.AsyncClient(timeout=timeout_seconds) as managed_client:
        return await _fetch_with_client(
            envelope,
            client=managed_client,
            timeout_seconds=timeout_seconds,
            max_attempts=max_attempts,
        )


def _is_official_host(host: str) -> bool:
    return any(_host_matches(host, domain) for domain in _OFFICIAL_DISCLOSURE_DOMAINS)


def _host_matches(host: str, domain: str) -> bool:
    return host == domain or host.endswith(f".{domain}")


async def _fetch_with_client(
    envelope: AnnouncementEnvelope,
    *,
    client: httpx.AsyncClient,
    timeout_seconds: float,
    max_attempts: int,
) -> bytes:
    url = str(envelope.official_url)
    retry_statuses = {429, *range(500, 600)}
    last_error: Exception | None = None
    last_status: int | None = None

    for attempt in range(1, max_attempts + 1):
        try:
            response = await client.get(
                url,
                follow_redirects=True,
                timeout=timeout_seconds,
            )
        except httpx.TimeoutException as exc:
            last_error = exc
            if attempt == max_attempts:
                break
            await asyncio.sleep(0)
            continue
        except httpx.RequestError as exc:
            raise DocumentFetchError(
                "Failed to fetch official announcement document: "
                f"announcement_id={envelope.announcement_id} url={url} error={exc}"
            ) from exc

        last_status = response.status_code
        if response.status_code < 400:
            return response.content
        if response.status_code not in retry_statuses:
            raise DocumentFetchError(
                "Failed to fetch official announcement document: "
                f"announcement_id={envelope.announcement_id} url={url} "
                f"status_code={response.status_code}"
            )
        if attempt < max_attempts:
            await asyncio.sleep(0)

    detail = (
        f"error={last_error}"
        if last_error is not None
        else f"status_code={last_status}"
    )
    raise DocumentFetchError(
        "Failed to fetch official announcement document after bounded retries: "
        f"announcement_id={envelope.announcement_id} url={url} attempts={max_attempts} "
        f"{detail}"
    )


def official_disclosure_domains() -> Iterable[str]:
    """Return configured official disclosure domain suffixes for diagnostics."""

    return tuple(sorted(_OFFICIAL_DISCLOSURE_DOMAINS))
