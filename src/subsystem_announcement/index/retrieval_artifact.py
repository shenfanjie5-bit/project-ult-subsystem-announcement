"""Retrieval artifact models and persistence helpers."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from subsystem_announcement.config import AnnouncementConfig
from subsystem_announcement.parse.artifact import ParsedAnnouncementArtifact


class AnnouncementChunk(BaseModel):
    """One searchable retrieval unit derived from a parsed announcement."""

    model_config = ConfigDict(extra="forbid")

    chunk_id: str = Field(min_length=1)
    announcement_id: str = Field(min_length=1)
    chunk_type: Literal["section", "table", "clause"]
    section_id: str = Field(min_length=1)
    table_ref: str | None = None
    text: str = Field(min_length=1)
    start_offset: int = Field(ge=0)
    end_offset: int = Field(ge=0)
    title_path: list[str] = Field(default_factory=list)
    source_reference: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_chunk(self) -> "AnnouncementChunk":
        """Keep chunk spans and source provenance usable for replay."""

        if self.end_offset <= self.start_offset:
            raise ValueError("end_offset must be greater than start_offset")
        if self.chunk_type == "table" and self.table_ref is None:
            raise ValueError("table chunks must include table_ref")
        official_url = self.source_reference.get("official_url")
        if not isinstance(official_url, str) or not official_url.strip():
            raise ValueError("source_reference.official_url is required")
        return self


class AnnouncementRetrievalArtifact(BaseModel):
    """Local retrieval index reference for one parsed announcement."""

    model_config = ConfigDict(extra="forbid")

    announcement_id: str = Field(min_length=1)
    chunk_refs: list[str] = Field(min_length=1)
    index_ref: str = Field(min_length=1)
    parser_version: str = Field(min_length=1)
    llama_index_version: str = Field(min_length=1)
    chunk_count: int = Field(ge=1)
    built_at: datetime
    source_parsed_artifact_path: Path | None = None
    chunks: list[AnnouncementChunk] = Field(default_factory=list)

    @field_validator("built_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        """Reject naive build timestamps."""

        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("built_at must include timezone information")
        return value

    @field_validator("llama_index_version")
    @classmethod
    def reject_unconfigured_llama_index(cls, value: str) -> str:
        """Retrieval artifacts must preserve a concrete LlamaIndex pin."""

        if value == "not-configured":
            raise ValueError("llama_index_version must not be not-configured")
        return value

    @model_validator(mode="after")
    def validate_chunk_refs(self) -> "AnnouncementRetrievalArtifact":
        """Keep chunk metadata internally consistent with artifact refs."""

        if self.chunk_count != len(self.chunk_refs):
            raise ValueError("chunk_count must match chunk_refs length")
        if not self.chunks:
            return self
        if self.chunk_count != len(self.chunks):
            raise ValueError("chunk_count must match chunks length")
        chunk_ids = [chunk.chunk_id for chunk in self.chunks]
        if self.chunk_refs != chunk_ids:
            raise ValueError("chunk_refs must match chunk ids in order")
        if any(chunk.announcement_id != self.announcement_id for chunk in self.chunks):
            raise ValueError("chunk announcement_id must match artifact")
        return self


class AnnouncementRetrievalHit(BaseModel):
    """One retrieval result returned for an announcement query."""

    model_config = ConfigDict(extra="forbid")

    chunk_id: str = Field(min_length=1)
    announcement_id: str = Field(min_length=1)
    section_id: str = Field(min_length=1)
    table_ref: str | None = None
    score: float = Field(ge=0.0)
    quote: str = Field(min_length=1)
    metadata: dict[str, Any] = Field(default_factory=dict)


def build_retrieval_artifact(
    parsed_artifact: ParsedAnnouncementArtifact,
    *,
    config: AnnouncementConfig,
    parsed_artifact_path: Path | None = None,
    output_root: Path | None = None,
) -> AnnouncementRetrievalArtifact:
    """Build chunks, a local vector index, and the retrieval artifact object."""

    from .chunker import chunk_parsed_artifact
    from .vector_store import build_vector_index

    root = (
        Path(output_root)
        if output_root is not None
        else Path(config.artifact_root) / "index" / parsed_artifact.announcement_id
    )
    chunks = chunk_parsed_artifact(parsed_artifact)
    vector_ref = build_vector_index(
        chunks,
        persist_dir=root / "vector_store",
        config=config,
    )
    return AnnouncementRetrievalArtifact(
        announcement_id=parsed_artifact.announcement_id,
        chunk_refs=[chunk.chunk_id for chunk in chunks],
        index_ref=vector_ref.index_ref,
        parser_version=parsed_artifact.parser_version,
        llama_index_version=vector_ref.llama_index_version,
        chunk_count=len(chunks),
        built_at=vector_ref.built_at,
        source_parsed_artifact_path=parsed_artifact_path,
        chunks=chunks,
    )


def write_retrieval_artifact(
    artifact: AnnouncementRetrievalArtifact,
    root: Path,
) -> Path:
    """Persist a retrieval artifact as ``root/retrieval_artifact.json``."""

    output_root = Path(root)
    path = output_root / "retrieval_artifact.json"
    try:
        output_root.mkdir(parents=True, exist_ok=True)
        path.write_text(artifact.model_dump_json(indent=2), encoding="utf-8")
    except OSError as exc:
        raise RuntimeError(f"Unable to write retrieval artifact: path={path}") from exc
    return path


def load_retrieval_artifact(path: Path) -> AnnouncementRetrievalArtifact:
    """Load a retrieval artifact JSON file."""

    artifact_path = Path(path)
    try:
        return AnnouncementRetrievalArtifact.model_validate_json(
            artifact_path.read_text(encoding="utf-8")
        )
    except (OSError, ValueError) as exc:
        raise RuntimeError(
            f"Unable to load retrieval artifact: path={artifact_path}"
        ) from exc
