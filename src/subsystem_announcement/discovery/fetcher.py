"""Official source validation and byte fetching for announcements."""

from __future__ import annotations

import asyncio
from collections.abc import Iterable
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import TYPE_CHECKING
from urllib.parse import urljoin, urlsplit

from .envelope import AnnouncementEnvelope
from .errors import DocumentFetchError, NonOfficialSourceError

if TYPE_CHECKING:
    import httpx

_OFFICIAL_DISCLOSURE_DOMAINS: frozenset[str] = frozenset(
    {
        "bse.cn",
        "cninfo.com.cn",
        "neeq.com.cn",
        "sse.com.cn",
        "szse.cn",
    }
)
_REDIRECT_STATUSES: frozenset[int] = frozenset({301, 302, 303, 307, 308})
_RETRY_STATUSES: frozenset[int] = frozenset({429, *range(500, 600)})
_RETRY_BASE_DELAY_SECONDS = 0.5
_MAX_REDIRECTS = 10


def validate_official_url(envelope: AnnouncementEnvelope) -> None:
    """Reject non-official disclosure URLs before any network request."""

    _validate_official_url_text(
        str(envelope.official_url),
        announcement_id=envelope.announcement_id,
    )


def _validate_official_url_text(url: str, *, announcement_id: str) -> None:
    parsed = urlsplit(url)
    host = (parsed.hostname or "").lower().rstrip(".")
    scheme = parsed.scheme.lower()
    if scheme not in {"http", "https"} or not _is_official_host(host):
        raise NonOfficialSourceError(
            "Non-official announcement URL rejected before fetch: "
            f"announcement_id={announcement_id} url={url}"
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

    import httpx

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
    import httpx

    url = str(envelope.official_url)
    last_error: Exception | None = None
    last_status: int | None = None

    for attempt in range(1, max_attempts + 1):
        try:
            response = await _get_with_official_redirects(
                envelope,
                client=client,
                url=url,
                timeout_seconds=timeout_seconds,
            )
        except httpx.TimeoutException as exc:
            last_error = exc
            if attempt == max_attempts:
                break
            await _sleep_before_retry(attempt=attempt)
            continue
        except httpx.RequestError as exc:
            raise DocumentFetchError(
                "Failed to fetch official announcement document: "
                f"announcement_id={envelope.announcement_id} url={url} error={exc}"
            ) from exc

        last_status = response.status_code
        if 200 <= response.status_code < 300:
            return response.content
        if response.status_code not in _RETRY_STATUSES:
            raise DocumentFetchError(
                "Failed to fetch official announcement document: "
                f"announcement_id={envelope.announcement_id} url={url} "
                f"status_code={response.status_code}"
            )
        if attempt < max_attempts:
            await _sleep_before_retry(attempt=attempt, response=response)

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


async def _get_with_official_redirects(
    envelope: AnnouncementEnvelope,
    *,
    client: httpx.AsyncClient,
    url: str,
    timeout_seconds: float,
) -> httpx.Response:
    current_url = url
    for _ in range(_MAX_REDIRECTS + 1):
        _validate_official_url_text(
            current_url,
            announcement_id=envelope.announcement_id,
        )
        response = await client.get(
            current_url,
            follow_redirects=False,
            timeout=timeout_seconds,
        )
        if response.status_code not in _REDIRECT_STATUSES:
            return response

        location = response.headers.get("location")
        if not location:
            raise DocumentFetchError(
                "Official announcement redirect missing Location header: "
                f"announcement_id={envelope.announcement_id} url={current_url} "
                f"status_code={response.status_code}"
            )
        current_url = urljoin(current_url, location)
        _validate_official_url_text(
            current_url,
            announcement_id=envelope.announcement_id,
        )

    raise DocumentFetchError(
        "Official announcement redirect limit exceeded: "
        f"announcement_id={envelope.announcement_id} url={url} "
        f"max_redirects={_MAX_REDIRECTS}"
    )


async def _sleep_before_retry(
    *,
    attempt: int,
    response: httpx.Response | None = None,
) -> None:
    await asyncio.sleep(_retry_delay_seconds(attempt=attempt, response=response))


def _retry_delay_seconds(
    *,
    attempt: int,
    response: httpx.Response | None = None,
) -> float:
    if response is not None and response.status_code == 429:
        retry_after = _parse_retry_after(response.headers.get("retry-after"))
        if retry_after is not None:
            return retry_after
    return _RETRY_BASE_DELAY_SECONDS * (2 ** (attempt - 1))


def _parse_retry_after(value: str | None) -> float | None:
    if value is None:
        return None
    stripped = value.strip()
    if not stripped:
        return None
    try:
        return max(0.0, float(stripped))
    except ValueError:
        pass
    try:
        retry_at = parsedate_to_datetime(stripped)
    except (TypeError, ValueError):
        return None
    if retry_at.tzinfo is None:
        retry_at = retry_at.replace(tzinfo=timezone.utc)
    return max(0.0, (retry_at - datetime.now(timezone.utc)).total_seconds())
