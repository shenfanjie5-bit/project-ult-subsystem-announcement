"""Deterministic extraction for equity pledge facts."""

from __future__ import annotations

import re

from subsystem_announcement.parse.artifact import ParsedAnnouncementArtifact

from subsystem_announcement.extract.candidates import (
    AnnouncementFactCandidate,
    ExtractionContext,
    FactType,
)

from . import build_single_evidence_candidate
from subsystem_announcement.extract.evidence import find_evidence_span


_PATTERNS = (
    re.compile(r"股份质押|股权质押|解除质押|质押.*股份"),
)
_RELATED_PATTERNS = (
    re.compile(r"(?:股东|质押人)(?P<name>[^，。；;]{2,40})"),
)


def extract(
    parsed_artifact: ParsedAnnouncementArtifact,
    context: ExtractionContext,
) -> list[AnnouncementFactCandidate]:
    """Extract equity pledge facts."""

    span = find_evidence_span(parsed_artifact, _PATTERNS)
    if span is None:
        return []
    pledge_action = "release" if "解除质押" in span.quote else "pledge"
    return build_single_evidence_candidate(
        parsed_artifact,
        context,
        fact_type=FactType.EQUITY_PLEDGE,
        span=span,
        fact_content={"pledge_action": pledge_action},
        related_mention_patterns=_RELATED_PATTERNS,
        confidence=0.88,
    )
