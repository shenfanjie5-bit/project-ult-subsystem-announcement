"""Parsed announcement artifact models and persistence helpers."""

from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from subsystem_announcement.discovery.document import AnnouncementDocumentArtifact

from .errors import ParseNormalizationError


_SHA256_HEX_RE = re.compile(r"^[0-9a-fA-F]{64}$")


class AnnouncementSection(BaseModel):
    """Normalized text section extracted from an announcement document."""

    model_config = ConfigDict(extra="forbid")

    section_id: str = Field(min_length=1)
    title: str | None = None
    level: int = Field(ge=0)
    text: str = Field(min_length=1)
    start_offset: int = Field(ge=0)
    end_offset: int = Field(ge=0)
    parent_id: str | None = None

    @model_validator(mode="after")
    def validate_offsets(self) -> "AnnouncementSection":
        """Ensure section offsets form a valid half-open range."""

        if self.end_offset < self.start_offset:
            raise ValueError("end_offset must be greater than or equal to start_offset")
        return self


class AnnouncementTable(BaseModel):
    """Normalized table extracted from an announcement document."""

    model_config = ConfigDict(extra="forbid")

    table_id: str = Field(min_length=1)
    section_id: str = Field(min_length=1)
    caption: str | None = None
    headers: list[str] = Field(default_factory=list)
    rows: list[list[str]] = Field(default_factory=list)
    start_offset: int = Field(ge=0)
    end_offset: int = Field(ge=0)

    @model_validator(mode="after")
    def validate_offsets(self) -> "AnnouncementTable":
        """Ensure table offsets form a valid half-open range."""

        if self.end_offset < self.start_offset:
            raise ValueError("end_offset must be greater than or equal to start_offset")
        return self


class ParsedAnnouncementArtifact(BaseModel):
    """Reusable Docling parse output for one official announcement document."""

    model_config = ConfigDict(extra="forbid")

    announcement_id: str = Field(min_length=1)
    content_hash: str = Field(min_length=64, max_length=64)
    parser_version: str = Field(min_length=1)
    title_hierarchy: list[str] = Field(default_factory=list)
    sections: list[AnnouncementSection] = Field(min_length=1)
    tables: list[AnnouncementTable] = Field(default_factory=list)
    extracted_text: str = Field(min_length=1)
    parsed_at: datetime
    source_document: AnnouncementDocumentArtifact

    @field_validator("content_hash")
    @classmethod
    def validate_content_hash(cls, value: str) -> str:
        """Require a path-safe SHA-256 digest for persistence provenance."""

        if not _is_sha256_hex_digest(value):
            raise ValueError("content_hash must be a 64-character SHA-256 hex digest")
        return value

    @field_validator("parser_version")
    @classmethod
    def reject_unconfigured_parser_version(cls, value: str) -> str:
        """Artifacts must preserve a concrete Docling parser provenance."""

        if value == "not-configured":
            raise ValueError("parser_version must not be not-configured")
        return value

    @model_validator(mode="after")
    def validate_artifact_consistency(self) -> "ParsedAnnouncementArtifact":
        """Keep parse artifact identity and offsets aligned with the source."""

        if self.announcement_id != self.source_document.announcement_id:
            raise ValueError("announcement_id must match source_document")
        if self.content_hash != self.source_document.content_hash:
            raise ValueError("content_hash must match source_document")

        text_length = len(self.extracted_text)
        for section in self.sections:
            if section.end_offset > text_length:
                raise ValueError("section offset exceeds extracted_text length")
        section_ids = {section.section_id for section in self.sections}
        for table in self.tables:
            if table.end_offset > text_length:
                raise ValueError("table offset exceeds extracted_text length")
            if table.section_id not in section_ids:
                raise ValueError("table section_id must reference a known section")
        return self


def write_parsed_artifact(artifact: ParsedAnnouncementArtifact, root: Path) -> Path:
    """Persist a parsed announcement artifact under ``root/parsed``."""

    root = Path(root)
    announcement_id = _safe_path_component(
        artifact.announcement_id,
        field_name="announcement_id",
    )
    content_hash = _safe_sha256_content_hash(artifact.content_hash)
    parsed_announcement_root = root / "parsed" / announcement_id
    path = parsed_announcement_root / f"{content_hash}.json"
    _ensure_under_root(path, parsed_announcement_root)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(artifact.model_dump_json(indent=2), encoding="utf-8")
    except OSError as exc:
        raise ParseNormalizationError(
            "Unable to write parsed announcement artifact: "
            f"announcement_id={artifact.announcement_id} path={path}"
        ) from exc
    return path


def load_parsed_artifact(path: Path) -> ParsedAnnouncementArtifact:
    """Load a parsed announcement artifact from disk."""

    try:
        return ParsedAnnouncementArtifact.model_validate_json(
            Path(path).read_text(encoding="utf-8")
        )
    except (OSError, ValueError) as exc:
        raise ParseNormalizationError(
            f"Unable to load parsed announcement artifact: path={path}"
        ) from exc


def _safe_path_component(value: str, *, field_name: str) -> str:
    if (
        value in {"", ".", ".."}
        or "/" in value
        or "\\" in value
        or "\x00" in value
        or Path(value).is_absolute()
    ):
        raise ParseNormalizationError(
            f"Unsafe {field_name} for parsed artifact path: {value!r}"
        )
    return value


def _safe_sha256_content_hash(value: str) -> str:
    if not _is_sha256_hex_digest(value):
        raise ParseNormalizationError(
            "Unsafe content_hash for parsed artifact path: "
            "expected 64-character SHA-256 hex digest"
        )
    return value


def _is_sha256_hex_digest(value: str) -> bool:
    return bool(_SHA256_HEX_RE.fullmatch(value))


def _ensure_under_root(path: Path, root: Path) -> None:
    try:
        path.resolve().relative_to(root.resolve())
    except ValueError as exc:
        raise ParseNormalizationError(
            f"Parsed artifact path escaped parsed announcement root: path={path} root={root}"
        ) from exc
