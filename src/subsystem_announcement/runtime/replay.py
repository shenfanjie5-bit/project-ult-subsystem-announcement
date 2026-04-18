"""Offline replay helpers for cached announcement documents."""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field

from subsystem_announcement.config import AnnouncementConfig
from subsystem_announcement.discovery import (
    AnnouncementDiscoveryResult,
    AnnouncementEnvelope,
)
from subsystem_announcement.discovery.dedupe import AnnouncementDedupeStore
from subsystem_announcement.discovery.document import AnnouncementDocumentArtifact
from subsystem_announcement.extract import extract_fact_candidates
from subsystem_announcement.graph import GraphFunc, derive_graph_delta_candidates
from subsystem_announcement.index import (
    AnnouncementRetrievalArtifact,
    build_retrieval_artifact,
    write_retrieval_artifact,
)
from subsystem_announcement.parse import parse_announcement
from subsystem_announcement.parse.artifact import load_parsed_artifact
from subsystem_announcement.signals import SignalFunc, derive_signal_candidates

from .pipeline import AnnouncementPipeline, ExtractFunc, ParseFunc
from .sdk_adapter import AnnouncementSubsystem
from .submit import SubmitIdempotencyStore
from .trace import AnnouncementExtractionRun, TraceStore


class ReplayError(RuntimeError):
    """Raised when a cached announcement cannot be replayed."""


class ReplayRequest(BaseModel):
    """Request to replay one cached announcement through the pipeline."""

    model_config = ConfigDict(extra="forbid")

    announcement_id: str = Field(min_length=1)
    trace_path: Path | None = None
    rebuild_index: bool = False


class ReplayResult(BaseModel):
    """Result of replaying one cached announcement."""

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")

    announcement_id: str = Field(min_length=1)
    run: AnnouncementExtractionRun
    document_artifact_path: Path
    replayed_at: datetime
    used_cached_document: bool = True
    retrieval_artifact_path: Path | None = None


BuildRetrievalFunc = Callable[
    ...,
    AnnouncementRetrievalArtifact | Awaitable[AnnouncementRetrievalArtifact],
]


def envelope_from_document_artifact(
    document: AnnouncementDocumentArtifact,
) -> AnnouncementEnvelope:
    """Rebuild a replay envelope from cached official document metadata."""

    missing_fields = [
        field_name
        for field_name in ("ts_code", "title", "publish_time")
        if getattr(document, field_name) is None
    ]
    if missing_fields:
        raise ReplayError(
            "Cached document cannot be replayed because envelope metadata is "
            f"incomplete: announcement_id={document.announcement_id} "
            f"missing={', '.join(missing_fields)}"
        )
    return AnnouncementEnvelope(
        announcement_id=document.announcement_id,
        ts_code=document.ts_code or "",
        title=document.title or "",
        publish_time=document.publish_time,  # type: ignore[arg-type]
        official_url=document.official_url,
        source_exchange=document.source_exchange,
        attachment_type=document.attachment_type,
    )


def load_cached_document_for_replay(
    announcement_id: str,
    *,
    config: AnnouncementConfig,
    dedupe_store: AnnouncementDedupeStore | None = None,
) -> AnnouncementDocumentArtifact:
    """Load a cached document through the discovery dedupe index."""

    store = dedupe_store or AnnouncementDedupeStore(config.artifact_root)
    document = store.find_by_announcement_id(announcement_id)
    if document is None:
        raise ReplayError(
            "No cached announcement document found for replay: "
            f"announcement_id={announcement_id} artifact_root={config.artifact_root}"
        )
    return document


async def replay_announcement(
    request: ReplayRequest,
    config: AnnouncementConfig,
    *,
    subsystem: AnnouncementSubsystem | None = None,
    idempotency_store: SubmitIdempotencyStore | None = None,
    trace_store: TraceStore | None = None,
    parse_func: ParseFunc = parse_announcement,
    extract_func: ExtractFunc = extract_fact_candidates,
    signal_func: SignalFunc = derive_signal_candidates,
    graph_func: GraphFunc = derive_graph_delta_candidates,
) -> ReplayResult:
    """Replay one announcement from local cache without official URL access."""

    document = load_cached_document_for_replay(
        request.announcement_id,
        config=config,
    )
    envelope = envelope_from_document_artifact(document)

    async def cached_discovery(
        discovered_envelope: AnnouncementEnvelope,
        _config: AnnouncementConfig,
    ) -> AnnouncementDiscoveryResult:
        if discovered_envelope.announcement_id != document.announcement_id:
            raise ReplayError(
                "Replay discovery received an unexpected announcement_id: "
                f"expected={document.announcement_id} "
                f"actual={discovered_envelope.announcement_id}"
            )
        return AnnouncementDiscoveryResult(status="duplicate", document=document)

    pipeline = AnnouncementPipeline(
        config,
        subsystem=subsystem,
        discovery_func=cached_discovery,
        parse_func=parse_func,
        extract_func=extract_func,
        signal_func=signal_func,
        graph_func=graph_func,
        idempotency_store=idempotency_store,
        trace_store=trace_store,
    )
    run = await pipeline.process_envelope(envelope)
    retrieval_artifact_path = None
    if request.rebuild_index and run.parsed_artifact_path is not None:
        retrieval_artifact_path = await _rebuild_index(
            run.parsed_artifact_path,
            config=config,
        )

    return ReplayResult(
        announcement_id=request.announcement_id,
        run=run,
        document_artifact_path=document.local_path,
        replayed_at=datetime.now(timezone.utc),
        used_cached_document=True,
        retrieval_artifact_path=retrieval_artifact_path,
    )


async def _rebuild_index(
    parsed_artifact_path: Path,
    *,
    config: AnnouncementConfig,
    rebuild_index_func: BuildRetrievalFunc = build_retrieval_artifact,
) -> Path:
    parsed_artifact = load_parsed_artifact(parsed_artifact_path)
    retrieval_artifact = await _maybe_await(
        rebuild_index_func(
            parsed_artifact,
            config=config,
            parsed_artifact_path=parsed_artifact_path,
        )
    )
    output_root = (
        Path(config.artifact_root) / "index" / parsed_artifact.announcement_id
    )
    return write_retrieval_artifact(retrieval_artifact, output_root)


async def _maybe_await(value: object) -> object:
    if inspect.isawaitable(value):
        return await value  # type: ignore[misc]
    return value
