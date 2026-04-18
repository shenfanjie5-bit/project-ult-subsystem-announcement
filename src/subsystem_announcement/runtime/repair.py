"""Offline repair helpers for parsed announcement artifacts."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, model_validator

from subsystem_announcement.config import AnnouncementConfig
from subsystem_announcement.discovery.cache import load_document_artifact
from subsystem_announcement.discovery.document import AnnouncementDocumentArtifact
from subsystem_announcement.index import (
    AnnouncementRetrievalArtifact,
    build_retrieval_artifact,
    write_retrieval_artifact,
)
from subsystem_announcement.parse import ParsedAnnouncementArtifact, parse_announcement
from subsystem_announcement.parse.artifact import write_parsed_artifact

from .replay import load_cached_document_for_replay
from .trace import TraceStore


class RepairError(RuntimeError):
    """Raised when a repair request cannot be completed."""


class RepairReason(str, Enum):
    """Supported offline parse repair reasons."""

    PARSE_FAILURE = "parse_failure"
    DOCLING_VERSION_UPGRADE = "docling_version_upgrade"


class RepairRequest(BaseModel):
    """Request to rebuild a parsed artifact from cached document bytes."""

    model_config = ConfigDict(extra="forbid")

    announcement_id: str | None = None
    trace_path: Path | None = None
    document_path: Path | None = None
    reason: RepairReason
    rebuild_index: bool = True

    @model_validator(mode="after")
    def require_document_locator(self) -> "RepairRequest":
        """Require at least one source from which a document can be recovered."""

        if (
            self.announcement_id is None
            and self.trace_path is None
            and self.document_path is None
        ):
            raise ValueError(
                "repair requires one of announcement_id, trace_path, or document_path"
            )
        return self


class RepairResult(BaseModel):
    """Result of a parse repair operation."""

    model_config = ConfigDict(extra="forbid")

    announcement_id: str = Field(min_length=1)
    parsed_artifact_path: Path
    parser_version: str = Field(min_length=1)
    retrieval_artifact_path: Path | None
    repaired_at: datetime


ParseFunc = Callable[
    [AnnouncementDocumentArtifact, AnnouncementConfig],
    ParsedAnnouncementArtifact,
]
BuildRetrievalFunc = Callable[..., AnnouncementRetrievalArtifact]


def repair_parsed_artifact(
    request: RepairRequest,
    config: AnnouncementConfig,
    *,
    parse_func: ParseFunc = parse_announcement,
    rebuild_index_func: BuildRetrievalFunc = build_retrieval_artifact,
) -> RepairResult:
    """Reparse a cached announcement document and optionally rebuild retrieval."""

    document = _load_document_for_repair(request, config)
    parsed_artifact = parse_func(document, config)
    if parsed_artifact.announcement_id != document.announcement_id:
        raise RepairError(
            "Repair parse returned an artifact for a different announcement: "
            f"document={document.announcement_id} "
            f"parsed={parsed_artifact.announcement_id}"
        )
    if (
        request.reason is RepairReason.DOCLING_VERSION_UPGRADE
        and parsed_artifact.parser_version != config.docling_version
    ):
        raise RepairError(
            "Docling version upgrade repair produced unexpected parser_version: "
            f"expected={config.docling_version} actual={parsed_artifact.parser_version}"
        )

    parsed_artifact_path = write_parsed_artifact(parsed_artifact, config.artifact_root)
    retrieval_artifact_path = None
    if request.rebuild_index:
        retrieval_artifact = rebuild_index_func(
            parsed_artifact,
            config=config,
            parsed_artifact_path=parsed_artifact_path,
        )
        output_root = (
            Path(config.artifact_root) / "index" / parsed_artifact.announcement_id
        )
        retrieval_artifact_path = write_retrieval_artifact(
            retrieval_artifact,
            output_root,
        )

    return RepairResult(
        announcement_id=parsed_artifact.announcement_id,
        parsed_artifact_path=parsed_artifact_path,
        parser_version=parsed_artifact.parser_version,
        retrieval_artifact_path=retrieval_artifact_path,
        repaired_at=datetime.now(timezone.utc),
    )


def _load_document_for_repair(
    request: RepairRequest,
    config: AnnouncementConfig,
) -> AnnouncementDocumentArtifact:
    if request.document_path is not None:
        return load_document_artifact(request.document_path)
    if request.trace_path is not None:
        run = TraceStore(config).load(request.trace_path)
        if run.document_artifact_path is None:
            raise RepairError(
                "Run trace does not include document_artifact_path: "
                f"trace_path={request.trace_path}"
            )
        return load_document_artifact(run.document_artifact_path)
    if request.announcement_id is None:
        raise RepairError("repair request did not include a document locator")
    return load_cached_document_for_replay(request.announcement_id, config=config)
