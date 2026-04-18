"""Shared helpers for deterministic announcement fact extractors."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any, Pattern

from subsystem_announcement.parse.artifact import ParsedAnnouncementArtifact

from subsystem_announcement.extract.candidates import (
    AnnouncementFactCandidate,
    ExtractionContext,
    FactType,
    build_fact_candidate,
)
from subsystem_announcement.extract.entity_anchor import EntityMention
from subsystem_announcement.extract.evidence import EvidenceSpan, find_evidence_span


PatternLike = str | Pattern[str]


def build_single_evidence_candidate(
    parsed_artifact: ParsedAnnouncementArtifact,
    context: ExtractionContext,
    *,
    fact_type: FactType,
    evidence_patterns: Sequence[PatternLike] = (),
    span: EvidenceSpan | None = None,
    fact_content: Mapping[str, Any],
    related_mention_patterns: Sequence[Pattern[str]] = (),
    confidence: float = 0.86,
) -> list[AnnouncementFactCandidate]:
    """Build one deterministic candidate when body evidence is present."""

    evidence_span = span or find_evidence_span(parsed_artifact, evidence_patterns)
    if evidence_span is None:
        return []
    related_mentions = _related_mentions(evidence_span, related_mention_patterns)
    related_entities = context.entity_anchorer.resolve_related_mentions(related_mentions)
    content = {
        "disclosure_type": fact_type.value,
        "summary": evidence_span.quote,
        **dict(fact_content),
    }
    return [
        build_fact_candidate(
            parsed_artifact,
            context,
            fact_type=fact_type,
            fact_content=content,
            evidence_spans=[evidence_span],
            related_entities=related_entities,
            confidence=confidence,
        )
    ]


def keyword_present(span: EvidenceSpan, keyword: str) -> bool:
    """Return whether a quote contains a keyword."""

    return keyword in span.quote


def _related_mentions(
    span: EvidenceSpan,
    patterns: Sequence[Pattern[str]],
) -> list[EntityMention]:
    mentions: list[EntityMention] = []
    seen: set[str] = set()
    for pattern in patterns:
        for match in pattern.finditer(span.quote):
            name = (match.groupdict().get("name") or match.group(1)).strip()
            name = re.sub(r"^(?:公司|与|收到|股东)", "", name).strip(" ：:，,")
            if not name or name in seen:
                continue
            seen.add(name)
            mentions.append(EntityMention(name=name, role="related_entity"))
    return mentions


__all__ = ["build_single_evidence_candidate", "keyword_present"]
