"""Docling boundary for announcement parsing."""

from __future__ import annotations

import re
from importlib import metadata

from subsystem_announcement.config import AnnouncementConfig
from subsystem_announcement.discovery.document import AnnouncementDocumentArtifact

from .artifact import ParsedAnnouncementArtifact
from .errors import DoclingParseError, UnsupportedAttachmentTypeError
from .normalize import normalize_docling_result

_SUPPORTED_ATTACHMENT_TYPES = frozenset({"pdf", "html", "word"})
_PINNED_DOCLING_VERSION = re.compile(r"^docling==[A-Za-z0-9][A-Za-z0-9._!+-]*$")


class DoclingAnnouncementParser:
    """Parse official announcement document bytes through Docling only."""

    def parse(
        self,
        document_ref: AnnouncementDocumentArtifact,
        config: AnnouncementConfig,
    ) -> ParsedAnnouncementArtifact:
        """Parse one cached announcement document into a normalized artifact."""

        if document_ref.attachment_type not in _SUPPORTED_ATTACHMENT_TYPES:
            raise UnsupportedAttachmentTypeError(
                "Unsupported announcement attachment type: "
                f"announcement_id={document_ref.announcement_id} "
                f"attachment_type={document_ref.attachment_type}"
            )

        parser_version = resolve_docling_version(config)
        if not document_ref.local_path.exists():
            raise DoclingParseError(
                "Announcement document is not available for Docling parse: "
                f"announcement_id={document_ref.announcement_id} "
                f"path={document_ref.local_path}"
            )

        try:
            from docling.document_converter import DocumentConverter
        except ModuleNotFoundError as exc:
            raise DoclingParseError(
                "Docling is required to parse announcement documents. "
                "Install the exact docling pin from pyproject.toml or provide "
                "a test double at the Docling boundary."
            ) from exc

        try:
            raw_result = DocumentConverter().convert(str(document_ref.local_path))
        except Exception as exc:
            raise DoclingParseError(
                "Docling failed to parse announcement document: "
                f"announcement_id={document_ref.announcement_id} "
                f"path={document_ref.local_path}"
            ) from exc

        return normalize_docling_result(raw_result, document_ref, parser_version)


def resolve_docling_version(config: AnnouncementConfig) -> str:
    """Resolve concrete Docling parser provenance for parse artifacts."""

    try:
        return f"docling=={metadata.version('docling')}"
    except metadata.PackageNotFoundError:
        configured = config.docling_version.strip()
        if configured != "not-configured" and _PINNED_DOCLING_VERSION.fullmatch(
            configured
        ):
            return configured
        raise DoclingParseError(
            "Docling parser version is not configured and the docling package "
            "is not installed; parser_version cannot be not-configured."
        ) from None


def parse_announcement(
    document_ref: AnnouncementDocumentArtifact,
    config: AnnouncementConfig,
) -> ParsedAnnouncementArtifact:
    """Parse one cached announcement document using the default Docling parser."""

    return DoclingAnnouncementParser().parse(document_ref, config)
