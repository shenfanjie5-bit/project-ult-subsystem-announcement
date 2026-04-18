"""Deterministic extraction for fundraising change facts."""

from __future__ import annotations

import re

from subsystem_announcement.parse.artifact import ParsedAnnouncementArtifact

from subsystem_announcement.extract.candidates import (
    AnnouncementFactCandidate,
    ExtractionContext,
    FactType,
)

from . import build_single_evidence_candidate


_PATTERNS = (
    re.compile(r"募集资金.*变更|变更.*募集资金|募投项目.*变更|发行方案.*调整"),
)


def extract(
    parsed_artifact: ParsedAnnouncementArtifact,
    context: ExtractionContext,
) -> list[AnnouncementFactCandidate]:
    """Extract fundraising change facts."""

    return build_single_evidence_candidate(
        parsed_artifact,
        context,
        fact_type=FactType.FUNDRAISING_CHANGE,
        evidence_patterns=_PATTERNS,
        fact_content={"fundraising_change_type": "use_or_plan_change"},
        confidence=0.86,
    )
