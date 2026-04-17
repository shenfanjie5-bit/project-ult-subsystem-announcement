"""Discovery artifacts produced from official announcement documents."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Literal

from pydantic import AnyUrl, BaseModel, ConfigDict, Field


class AnnouncementDocumentArtifact(BaseModel):
    """Replayable local reference to official announcement document bytes."""

    model_config = ConfigDict(extra="forbid")

    announcement_id: str = Field(min_length=1)
    content_hash: str = Field(min_length=64, max_length=64)
    official_url: AnyUrl
    source_exchange: str = Field(min_length=1)
    attachment_type: Literal["pdf", "html", "word"]
    local_path: Path
    content_type: str = Field(min_length=1)
    byte_size: int = Field(ge=0)
    fetched_at: datetime


class AnnouncementDiscoveryResult(BaseModel):
    """Result of consuming one announcement reference."""

    model_config = ConfigDict(extra="forbid")

    status: Literal["fetched", "duplicate"]
    document: AnnouncementDocumentArtifact
