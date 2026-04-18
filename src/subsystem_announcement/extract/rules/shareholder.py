"""Deterministic extraction for shareholder change facts."""

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
    re.compile(r"权益变动|持股比例|增持|减持|控股股东|实际控制人|股份变动"),
)
_RELATED_PATTERNS = (
    re.compile(r"(?:股东|控股股东|实际控制人)(?P<name>[^，。；;]{2,40})"),
)


def extract(
    parsed_artifact: ParsedAnnouncementArtifact,
    context: ExtractionContext,
) -> list[AnnouncementFactCandidate]:
    """Extract shareholder change facts."""

    span = find_evidence_span(parsed_artifact, _PATTERNS)
    if span is None:
        return []
    change_type = "change"
    if "减持" in span.quote:
        change_type = "decrease"
    elif "增持" in span.quote:
        change_type = "increase"
    elif "控股股东" in span.quote or "实际控制人" in span.quote:
        change_type = "control_change"
    return build_single_evidence_candidate(
        parsed_artifact,
        context,
        fact_type=FactType.SHAREHOLDER_CHANGE,
        span=span,
        fact_content={"shareholder_change_type": change_type},
        related_mention_patterns=_RELATED_PATTERNS,
        confidence=0.87,
    )
