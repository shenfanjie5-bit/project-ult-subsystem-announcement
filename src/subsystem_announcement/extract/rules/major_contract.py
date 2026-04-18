"""Deterministic extraction for major contract facts."""

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
    re.compile(r"重大合同|签订.*合同|合同金额|中标.*项目"),
)
_RELATED_PATTERNS = (
    re.compile(r"与(?P<name>[^，。；;]{2,40}?)(?:签订|签署|订立)"),
    re.compile(r"中标(?P<name>[^，。；;]{2,40}?)(?:项目|工程)"),
)


def extract(
    parsed_artifact: ParsedAnnouncementArtifact,
    context: ExtractionContext,
) -> list[AnnouncementFactCandidate]:
    """Extract major contract facts."""

    return build_single_evidence_candidate(
        parsed_artifact,
        context,
        fact_type=FactType.MAJOR_CONTRACT,
        evidence_patterns=_PATTERNS,
        fact_content={"event": "major_contract"},
        related_mention_patterns=_RELATED_PATTERNS,
        confidence=0.9,
    )
