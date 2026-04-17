"""Discovery-stage errors with traceable identifiers."""

from __future__ import annotations


class DiscoveryError(RuntimeError):
    """Base error for announcement discovery failures."""


class InvalidAnnouncementEnvelopeError(DiscoveryError):
    """Raised when an upstream announcement envelope cannot be consumed."""


class NonOfficialSourceError(DiscoveryError):
    """Raised before fetching when an announcement URL is not official."""


class DocumentFetchError(DiscoveryError):
    """Raised when official document bytes cannot be fetched."""


class DocumentCacheError(DiscoveryError):
    """Raised when document bytes or metadata cannot be cached."""
