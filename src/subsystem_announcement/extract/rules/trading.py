"""Deterministic extraction for trading halt/resume facts."""

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
    re.compile(r"停牌|复牌|恢复交易|暂停交易"),
)


def extract(
    parsed_artifact: ParsedAnnouncementArtifact,
    context: ExtractionContext,
) -> list[AnnouncementFactCandidate]:
    """Extract trading halt/resume facts."""

    span = find_evidence_span(parsed_artifact, _PATTERNS)
    if span is None:
        return []
    trading_status = "resume" if "复牌" in span.quote or "恢复交易" in span.quote else "halt"
    return build_single_evidence_candidate(
        parsed_artifact,
        context,
        fact_type=FactType.TRADING_HALT_RESUME,
        span=span,
        fact_content={"trading_status": trading_status},
        confidence=0.89,
    )
