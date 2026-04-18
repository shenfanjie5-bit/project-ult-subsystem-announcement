"""Discovery artifacts produced from official announcement documents."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Literal

from pydantic import AnyUrl, BaseModel, ConfigDict, Field, field_validator


class AnnouncementDocumentArtifact(BaseModel):
    """Replayable local reference to official announcement document bytes."""

    model_config = ConfigDict(extra="forbid")

    announcement_id: str = Field(min_length=1)
    ts_code: str | None = Field(default=None, min_length=1)
    title: str | None = Field(default=None, min_length=1)
    publish_time: datetime | None = None
    content_hash: str = Field(min_length=64, max_length=64)
    official_url: AnyUrl
    source_exchange: str = Field(min_length=1)
    attachment_type: Literal["pdf", "html", "word"]
    local_path: Path
    content_type: str = Field(min_length=1)
    byte_size: int = Field(ge=0)
    fetched_at: datetime

    @field_validator("ts_code", "title")
    @classmethod
    def strip_optional_text(cls, value: str | None) -> str | None:
        """Normalize optional envelope provenance without inventing values."""

        if value is None:
            return None
        stripped = value.strip()
        if not stripped:
            raise ValueError("optional envelope provenance must not be empty")
        return stripped

    @field_validator("publish_time", "fetched_at")
    @classmethod
    def require_timezone(cls, value: datetime | None) -> datetime | None:
        """Reject naive datetimes so replay provenance is unambiguous."""

        if value is None:
            return None
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("announcement document datetimes must include timezone")
        return value


class AnnouncementDiscoveryResult(BaseModel):
    """Result of consuming one announcement reference."""

    model_config = ConfigDict(extra="forbid")

    status: Literal["fetched", "duplicate"]
    document: AnnouncementDocumentArtifact
