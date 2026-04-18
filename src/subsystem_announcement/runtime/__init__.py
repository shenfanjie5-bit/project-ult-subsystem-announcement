"""Runtime public API for announcement processing."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .pipeline import AnnouncementPipeline
    from .submit import submit_candidates
    from .trace import AnnouncementExtractionRun

__all__ = [
    "AnnouncementExtractionRun",
    "AnnouncementPipeline",
    "submit_candidates",
]


def __getattr__(name: str) -> object:
    if name == "AnnouncementPipeline":
        from .pipeline import AnnouncementPipeline

        return AnnouncementPipeline
    if name == "submit_candidates":
        from .submit import submit_candidates

        return submit_candidates
    if name == "AnnouncementExtractionRun":
        from .trace import AnnouncementExtractionRun

        return AnnouncementExtractionRun
    raise AttributeError(name)
