"""Official announcement discovery entrypoint."""

from __future__ import annotations

import httpx

from subsystem_announcement.config import AnnouncementConfig

from .cache import AnnouncementDocumentCache
from .dedupe import AnnouncementDedupeStore, compute_content_hash
from .document import AnnouncementDiscoveryResult, AnnouncementDocumentArtifact
from .envelope import AnnouncementEnvelope
from .fetcher import fetch_official_document, validate_official_url

__all__: list[str] = [
    "AnnouncementDiscoveryResult",
    "AnnouncementDocumentArtifact",
    "AnnouncementEnvelope",
    "consume_announcement_ref",
]


async def consume_announcement_ref(
    envelope: AnnouncementEnvelope,
    config: AnnouncementConfig,
    *,
    client: httpx.AsyncClient | None = None,
) -> AnnouncementDiscoveryResult:
    """Convert one official announcement reference into a cached artifact."""

    validate_official_url(envelope)
    dedupe_store = AnnouncementDedupeStore(config.artifact_root)
    existing_by_id = dedupe_store.find_by_announcement_id(envelope.announcement_id)
    if existing_by_id is not None:
        return AnnouncementDiscoveryResult(status="duplicate", document=existing_by_id)

    content = await fetch_official_document(envelope, client=client)
    content_hash = compute_content_hash(content)
    existing_by_hash = dedupe_store.find_by_content_hash(content_hash)
    if existing_by_hash is not None:
        dedupe_store.record(existing_by_hash, announcement_id=envelope.announcement_id)
        return AnnouncementDiscoveryResult(
            status="duplicate",
            document=existing_by_hash,
        )

    artifact = AnnouncementDocumentCache(config).put(envelope, content)
    dedupe_store.record(artifact)
    return AnnouncementDiscoveryResult(status="fetched", document=artifact)
