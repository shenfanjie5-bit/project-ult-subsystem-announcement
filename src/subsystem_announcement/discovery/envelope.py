"""Announcement metadata references accepted by discovery."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal

from pydantic import AnyUrl, BaseModel, ConfigDict, Field, field_validator
from pydantic.types import StringConstraints

NonEmptyStr = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


class AnnouncementEnvelope(BaseModel):
    """Upstream announcement metadata needed to fetch an official document."""

    model_config = ConfigDict(extra="forbid")

    announcement_id: NonEmptyStr
    ts_code: NonEmptyStr
    title: NonEmptyStr
    publish_time: datetime
    official_url: AnyUrl
    source_exchange: NonEmptyStr
    attachment_type: Literal["pdf", "html", "word"] = Field(...)

    @field_validator("publish_time")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        """Reject naive datetimes so replay ordering is unambiguous."""

        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("publish_time must include timezone information")
        return value
