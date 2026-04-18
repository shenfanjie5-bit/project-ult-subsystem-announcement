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


class AnnouncementEmbeddingStrategy(BaseModel):
    """Embedding identity used to build and query a persisted vector index."""

    model_config = ConfigDict(extra="forbid")

    strategy_type: Literal["adapter", "injected", "test_mock"]
    adapter_ref: str | None = None
    model_ref: str = Field(min_length=1)
    model_version: str | None = None
    model_dimension: int | None = Field(default=None, ge=1)
    model_fingerprint: str = Field(min_length=1)

    @model_validator(mode="after")
    def validate_strategy(self) -> "AnnouncementEmbeddingStrategy":
        """Keep adapter-built indexes tied to the configured adapter ref."""

        if self.strategy_type == "adapter":
            if not self.adapter_ref:
                raise ValueError("adapter embedding strategy requires adapter_ref")
        elif self.adapter_ref is not None:
            raise ValueError("adapter_ref is only valid for adapter strategy")
        return self


class AnnouncementRetrievalArtifact(BaseModel):
    """Local retrieval index reference for one parsed announcement."""

    model_config = ConfigDict(extra="forbid")

    announcement_id: str = Field(min_length=1)
    chunk_refs: list[str] = Field(min_length=1)
    index_ref: str = Field(min_length=1)
    parser_version: str = Field(min_length=1)
    llama_index_version: str = Field(min_length=1)
    embedding_strategy: AnnouncementEmbeddingStrategy
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

    if output_root is None:
        root, vector_store_dir = _prepare_default_output_paths(
            config,
            parsed_artifact.announcement_id,
        )
    else:
        root = Path(output_root)
        vector_store_dir = root / "vector_store"

    chunks = chunk_parsed_artifact(parsed_artifact)
    vector_ref = build_vector_index(
        chunks,
        persist_dir=vector_store_dir,
        config=config,
    )
    return AnnouncementRetrievalArtifact(
        announcement_id=parsed_artifact.announcement_id,
        chunk_refs=[chunk.chunk_id for chunk in chunks],
        index_ref=vector_ref.index_ref,
        parser_version=parsed_artifact.parser_version,
        llama_index_version=vector_ref.llama_index_version,
        embedding_strategy=vector_ref.embedding_strategy,
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


def _prepare_default_output_paths(
    config: AnnouncementConfig,
    announcement_id: str,
) -> tuple[Path, Path]:
    safe_announcement_id = _safe_path_component(
        announcement_id,
        field_name="announcement_id",
    )
    index_root = Path(config.artifact_root) / "index"
    output_root = index_root / safe_announcement_id
    output_root, resolved_index_root = _prepare_directory_under_root(
        index_root,
        output_root,
        description="retrieval announcement directory",
        announcement_id=safe_announcement_id,
    )
    vector_store_dir = output_root / "vector_store"
    vector_store_dir, _ = _prepare_directory_under_root(
        index_root,
        vector_store_dir,
        description="retrieval vector store directory",
        announcement_id=safe_announcement_id,
        resolved_root=resolved_index_root,
    )
    return output_root, vector_store_dir


def _safe_path_component(value: str, *, field_name: str) -> str:
    if (
        not isinstance(value, str)
        or value in {"", ".", ".."}
        or "/" in value
        or "\\" in value
        or "\x00" in value
        or Path(value).is_absolute()
    ):
        raise ValueError(f"Unsafe {field_name} for retrieval index path: {value!r}")
    return value


def _prepare_directory_under_root(
    root: Path,
    path: Path,
    *,
    description: str,
    announcement_id: str,
    resolved_root: Path | None = None,
) -> tuple[Path, Path]:
    if root.is_symlink():
        raise ValueError(
            "Retrieval index root is a symlink: "
            f"announcement_id={announcement_id} path={root}"
        )
    root.mkdir(parents=True, exist_ok=True)
    if root.is_symlink():
        raise ValueError(
            "Retrieval index root is a symlink: "
            f"announcement_id={announcement_id} path={root}"
        )
    if not root.is_dir():
        raise ValueError(
            "Retrieval index root is not a directory: "
            f"announcement_id={announcement_id} path={root}"
        )

    resolved_root = resolved_root or root.resolve()
    if path.is_symlink():
        raise ValueError(
            f"{description.title()} is a symlink: "
            f"announcement_id={announcement_id} path={path}"
        )
    path.mkdir(exist_ok=True)
    if path.is_symlink():
        raise ValueError(
            f"{description.title()} is a symlink: "
            f"announcement_id={announcement_id} path={path}"
        )
    if not path.is_dir():
        raise ValueError(
            f"{description.title()} is not a directory: "
            f"announcement_id={announcement_id} path={path}"
        )
    _ensure_under_resolved_root(path, resolved_root)
    return path, resolved_root


def _ensure_under_resolved_root(path: Path, resolved_root: Path) -> None:
    try:
        path.resolve().relative_to(resolved_root)
    except ValueError as exc:
        raise ValueError(
            "Retrieval index path escaped retrieval index root: "
            f"path={path} root={resolved_root}"
        ) from exc
