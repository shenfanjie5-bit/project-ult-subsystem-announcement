"""Announcement document parsing public API."""

from __future__ import annotations

from .artifact import (
    AnnouncementSection,
    AnnouncementTable,
    ParsedAnnouncementArtifact,
    load_parsed_artifact,
    write_parsed_artifact,
)
from .docling_client import DoclingAnnouncementParser, parse_announcement
from .errors import (
    DoclingParseError,
    ParseError,
    ParseNormalizationError,
    UnsupportedAttachmentTypeError,
)
from .normalize import normalize_docling_result

__all__ = [
    "AnnouncementSection",
    "AnnouncementTable",
    "DoclingAnnouncementParser",
    "DoclingParseError",
    "ParseError",
    "ParseNormalizationError",
    "ParsedAnnouncementArtifact",
    "UnsupportedAttachmentTypeError",
    "load_parsed_artifact",
    "normalize_docling_result",
    "parse_announcement",
    "write_parsed_artifact",
]
