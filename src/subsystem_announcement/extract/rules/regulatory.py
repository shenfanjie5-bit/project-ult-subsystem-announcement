"""Deterministic extraction for regulatory action facts."""

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
    re.compile(r"行政处罚|监管函|纪律处分|立案调查|监管措施|问询函"),
)
_RELATED_PATTERNS = (
    re.compile(r"收到(?P<name>[^，。；;]{2,40}?)(?:出具|下发|送达|发出)"),
)


def extract(
    parsed_artifact: ParsedAnnouncementArtifact,
    context: ExtractionContext,
) -> list[AnnouncementFactCandidate]:
    """Extract regulatory action facts."""

    span = find_evidence_span(parsed_artifact, _PATTERNS)
    if span is None:
        return []
    if "行政处罚" in span.quote:
        action_type = "administrative_penalty"
    elif "立案调查" in span.quote:
        action_type = "investigation"
    else:
        action_type = "regulatory_notice"
    return build_single_evidence_candidate(
        parsed_artifact,
        context,
        fact_type=FactType.REGULATORY_ACTION,
        span=span,
        fact_content={"regulatory_action_type": action_type},
        related_mention_patterns=_RELATED_PATTERNS,
        confidence=0.87,
    )
