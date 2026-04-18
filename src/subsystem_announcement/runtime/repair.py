"""Offline repair helpers for parsed announcement artifacts."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
from enum import Enum
import json
import re
from pathlib import Path
from uuid import uuid4

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

    if request.reason is RepairReason.DOCLING_VERSION_UPGRADE:
        parsed_artifact_path = _write_versioned_upgrade_artifact(
            parsed_artifact,
            config,
        )
    else:
        parsed_artifact_path = write_parsed_artifact(
            parsed_artifact,
            config.artifact_root,
        )

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

    repaired_at = datetime.now(timezone.utc)
    if request.reason is RepairReason.DOCLING_VERSION_UPGRADE:
        _write_latest_upgrade_pointer(
            parsed_artifact,
            parsed_artifact_path,
            config,
            repaired_at=repaired_at,
        )

    return RepairResult(
        announcement_id=parsed_artifact.announcement_id,
        parsed_artifact_path=parsed_artifact_path,
        parser_version=parsed_artifact.parser_version,
        retrieval_artifact_path=retrieval_artifact_path,
        repaired_at=repaired_at,
    )


def _load_document_for_repair(
    request: RepairRequest,
    config: AnnouncementConfig,
) -> AnnouncementDocumentArtifact:
    documents: list[tuple[str, AnnouncementDocumentArtifact]] = []
    if request.document_path is not None:
        documents.append(
            (
                f"document_path={request.document_path}",
                load_document_artifact(request.document_path),
            )
        )
    if request.trace_path is not None:
        run = TraceStore(config).load(request.trace_path)
        if run.document_artifact_path is None:
            raise RepairError(
                "Run trace does not include document_artifact_path: "
                f"trace_path={request.trace_path}"
            )
        documents.append(
            (
                f"trace_path={request.trace_path}",
                load_document_artifact(run.document_artifact_path),
            )
        )
    if request.announcement_id is not None:
        documents.append(
            (
                f"announcement_id={request.announcement_id}",
                load_cached_document_for_replay(
                    request.announcement_id,
                    config=config,
                ),
            )
        )
    if not documents:
        raise RepairError("repair request did not include a document locator")

    expected = documents[0][1]
    conflicts: list[str] = []
    for locator, document in documents[1:]:
        if document.announcement_id != expected.announcement_id:
            conflicts.append(
                f"{locator} announcement_id={document.announcement_id!r}"
            )
        if document.content_hash != expected.content_hash:
            conflicts.append(
                f"{locator} content_hash={document.content_hash!r}"
            )
    if conflicts:
        first_locator, first_document = documents[0]
        raise RepairError(
            "Repair request locators resolve to different documents: "
            f"{first_locator} announcement_id={first_document.announcement_id!r} "
            f"content_hash={first_document.content_hash!r}; "
            + "; ".join(conflicts)
        )
    return expected


def _write_versioned_upgrade_artifact(
    artifact: ParsedAnnouncementArtifact,
    config: AnnouncementConfig,
) -> Path:
    """Write an immutable Docling-upgrade parse without replacing rollback data."""

    upgrade_root = _upgrade_artifact_root(artifact, config)
    repair_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    path = upgrade_root / f"{repair_id}-{uuid4().hex}-{artifact.content_hash}.json"
    _write_json_atomic(path, artifact.model_dump_json(indent=2))
    return path


def _write_latest_upgrade_pointer(
    artifact: ParsedAnnouncementArtifact,
    artifact_path: Path,
    config: AnnouncementConfig,
    *,
    repaired_at: datetime,
) -> Path:
    announcement_root = _parsed_announcement_root(
        config,
        artifact.announcement_id,
    )
    pointer_path = announcement_root / "latest.json"
    payload = {
        "announcement_id": artifact.announcement_id,
        "content_hash": artifact.content_hash,
        "parser_version": artifact.parser_version,
        "parsed_artifact_path": str(artifact_path),
        "repaired_at": repaired_at.isoformat(),
    }
    _write_json_atomic(pointer_path, _json_dumps(payload))
    return pointer_path


def _upgrade_artifact_root(
    artifact: ParsedAnnouncementArtifact,
    config: AnnouncementConfig,
) -> Path:
    return (
        _parsed_announcement_root(config, artifact.announcement_id)
        / "upgrades"
        / _safe_path_component(artifact.parser_version, field_name="parser_version")
    )


def _parsed_announcement_root(
    config: AnnouncementConfig,
    announcement_id: str,
) -> Path:
    root = Path(config.artifact_root) / "parsed"
    announcement_root = root / _safe_path_component(
        announcement_id,
        field_name="announcement_id",
    )
    return announcement_root


def _write_json_atomic(path: Path, content: str) -> None:
    target = Path(path)
    parent = target.parent
    parent.mkdir(parents=True, exist_ok=True)
    if parent.is_symlink():
        raise RepairError(f"Repair artifact directory is a symlink: path={parent}")
    if target.exists() and target.is_symlink():
        raise RepairError(f"Repair artifact path is a symlink: path={target}")
    temp_path = parent / f".{target.name}.{uuid4().hex}.tmp"
    try:
        temp_path.write_text(content, encoding="utf-8")
        temp_path.replace(target)
    except OSError as exc:
        raise RepairError(f"Unable to write repair artifact: path={target}") from exc
    finally:
        try:
            if temp_path.exists():
                temp_path.unlink()
        except OSError:
            pass


def _safe_path_component(value: str, *, field_name: str) -> str:
    if (
        not isinstance(value, str)
        or value in {"", ".", ".."}
        or "/" in value
        or "\\" in value
        or "\x00" in value
        or Path(value).is_absolute()
    ):
        raise RepairError(f"Unsafe {field_name} for repair path: {value!r}")
    return re.sub(r"[^A-Za-z0-9._=-]+", "_", value)


def _json_dumps(payload: dict[str, object]) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=2)
