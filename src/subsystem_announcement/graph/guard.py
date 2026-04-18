"""High-threshold guardrails for Ex-3 graph delta generation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from subsystem_announcement.extract import AnnouncementFactCandidate

if TYPE_CHECKING:
    from .rules import GraphDeltaIntent


AMBIGUOUS_GRAPH_TERMS = (
    "战略合作意向",
    "框架协议",
    "意向",
    "拟",
    "可能",
)


@dataclass(frozen=True)
class GraphDeltaGuardResult:
    """Decision returned by graph delta guard checks."""

    allow: bool
    reasons: tuple[str, ...] = ()


@dataclass(frozen=True)
class GraphDeltaGuard:
    """Conservative threshold checks before materializing Ex-3 candidates."""

    min_evidence_spans: int = 2
    min_fact_confidence: float = 0.90
    require_resolved_entity_ids: bool = True

    def check(
        self,
        fact: AnnouncementFactCandidate,
        intent: "GraphDeltaIntent",
    ) -> GraphDeltaGuardResult:
        """Return whether a fact and classified intent may produce Ex-3."""

        reasons: list[str] = []
        if len(fact.evidence_spans) < self.min_evidence_spans:
            reasons.append("insufficient_evidence_spans")
        if fact.confidence < self.min_fact_confidence:
            reasons.append("low_fact_confidence")
        if self.require_resolved_entity_ids:
            if not is_resolved_entity_id(intent.source_node):
                reasons.append("unresolved_source_node")
            if not is_resolved_entity_id(intent.target_node):
                reasons.append("unresolved_target_node")
        official_url = fact.source_reference.get("official_url")
        if not isinstance(official_url, str) or not official_url.strip():
            reasons.append("missing_source_reference")
        if _contains_ambiguous_language(fact):
            reasons.append("ambiguous_language")
        return GraphDeltaGuardResult(allow=not reasons, reasons=tuple(reasons))


def is_resolved_entity_id(value: str) -> bool:
    """Return whether a reference is safe to use as a graph node id."""

    text = value.strip()
    if not text:
        return False
    lowered = text.lower()
    if lowered == "unresolved" or lowered.startswith("unresolved:"):
        return False
    if lowered.startswith("mention:"):
        return False
    return not lowered.startswith("unresolved")


def has_ambiguous_graph_language(text: str) -> bool:
    """Return whether text contains terms too weak for Ex-3 graph updates."""

    return any(term in text for term in AMBIGUOUS_GRAPH_TERMS)


def _contains_ambiguous_language(fact: AnnouncementFactCandidate) -> bool:
    return any(has_ambiguous_graph_language(span.quote) for span in fact.evidence_spans)
