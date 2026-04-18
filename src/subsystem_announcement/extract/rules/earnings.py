"""Deterministic extraction for earnings preannouncement facts."""

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
    re.compile(r"业绩预告|预计.*(?:净利润|盈利|亏损)|净利润.*同比"),
)


def extract(
    parsed_artifact: ParsedAnnouncementArtifact,
    context: ExtractionContext,
) -> list[AnnouncementFactCandidate]:
    """Extract earnings preannouncement facts."""

    span = find_evidence_span(parsed_artifact, _PATTERNS)
    if span is None:
        return []
    direction = "uncertain"
    if "亏损" in span.quote or "下降" in span.quote or "减少" in span.quote:
        direction = "negative"
    elif "增长" in span.quote or "增加" in span.quote or "盈利" in span.quote:
        direction = "positive"
    return build_single_evidence_candidate(
        parsed_artifact,
        context,
        fact_type=FactType.EARNINGS_PREANNOUNCE,
        span=span,
        fact_content={"performance_direction": direction},
        confidence=0.88,
    )
