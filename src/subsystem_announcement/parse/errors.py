"""Parse-stage errors for announcement document artifacts."""

from __future__ import annotations


class ParseError(RuntimeError):
    """Base error for announcement parsing failures."""


class UnsupportedAttachmentTypeError(ParseError):
    """Raised when the parse stage receives an unsupported attachment type."""


class DoclingParseError(ParseError):
    """Raised when Docling cannot parse an official announcement document."""


class ParseNormalizationError(ParseError):
    """Raised when a Docling result cannot be normalized into an artifact."""
