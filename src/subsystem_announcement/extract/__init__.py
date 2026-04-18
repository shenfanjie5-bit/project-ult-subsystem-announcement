"""Announcement Ex-1 extraction public API."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any

from subsystem_announcement.parse.artifact import ParsedAnnouncementArtifact

from .candidates import (
    AnnouncementFactCandidate,
    ExtractionContext,
    FactType,
    build_extraction_context,
    build_fact_candidate,
)
from .classifier import classify_disclosure_types
from .entity_anchor import (
    EntityAnchor,
    EntityAnchorer,
    EntityMention,
    EntityRegistryClient,
    EntityResolution,
)
from .evidence import (
    EvidenceSpan,
    build_evidence_span,
    build_table_evidence_span,
    iter_evidence_sources,
)
from .reasoner_bridge import (
    ReasonerRuntimeBridge,
    StructuredExtractionRequest,
    StructuredExtractionSegment,
    StructuredReasoner,
    bounded_segments,
    ex1_reasoner_schema,
)
from .rules import (
    earnings,
    equity_pledge,
    fundraising,
    major_contract,
    regulatory,
    shareholder,
    trading,
)


_FACT_TYPE_ORDER = (
    FactType.EARNINGS_PREANNOUNCE,
    FactType.MAJOR_CONTRACT,
    FactType.SHAREHOLDER_CHANGE,
    FactType.EQUITY_PLEDGE,
    FactType.REGULATORY_ACTION,
    FactType.TRADING_HALT_RESUME,
    FactType.FUNDRAISING_CHANGE,
)

_RULE_EXTRACTORS = {
    FactType.EARNINGS_PREANNOUNCE: earnings.extract,
    FactType.MAJOR_CONTRACT: major_contract.extract,
    FactType.SHAREHOLDER_CHANGE: shareholder.extract,
    FactType.EQUITY_PLEDGE: equity_pledge.extract,
    FactType.REGULATORY_ACTION: regulatory.extract,
    FactType.TRADING_HALT_RESUME: trading.extract,
    FactType.FUNDRAISING_CHANGE: fundraising.extract,
}

_REASONER_HINTS: dict[FactType, tuple[re.Pattern[str], ...]] = {
    FactType.EARNINGS_PREANNOUNCE: (
        re.compile(r"业绩|净利润|盈利|亏损"),
    ),
    FactType.MAJOR_CONTRACT: (re.compile(r"合同|中标|项目|合作"),),
    FactType.SHAREHOLDER_CHANGE: (re.compile(r"股东|持股|增持|减持|控制"),),
    FactType.EQUITY_PLEDGE: (re.compile(r"质押"),),
    FactType.REGULATORY_ACTION: (re.compile(r"监管|处罚|调查|问询"),),
    FactType.TRADING_HALT_RESUME: (re.compile(r"停牌|复牌|交易"),),
    FactType.FUNDRAISING_CHANGE: (re.compile(r"募集资金|募投|发行方案"),),
}


def extract_fact_candidates(
    parsed_artifact: ParsedAnnouncementArtifact,
    *,
    entity_registry: EntityRegistryClient | None = None,
    reasoner: StructuredReasoner | None = None,
) -> list[AnnouncementFactCandidate]:
    """Extract evidence-backed Ex-1 fact candidates from a parsed artifact."""

    context = build_extraction_context(
        parsed_artifact,
        entity_registry=entity_registry,
        reasoner=reasoner,
    )
    classified_types = classify_disclosure_types(parsed_artifact)
    candidates: list[AnnouncementFactCandidate] = []
    for fact_type in _FACT_TYPE_ORDER:
        if fact_type not in classified_types:
            continue
        extracted = _RULE_EXTRACTORS[fact_type](parsed_artifact, context)
        if not extracted and reasoner is not None:
            extracted = _extract_with_reasoner(parsed_artifact, context, fact_type)
        candidates.extend(extracted)
    return _dedupe_candidates(candidates)


def _extract_with_reasoner(
    parsed_artifact: ParsedAnnouncementArtifact,
    context: ExtractionContext,
    fact_type: FactType,
) -> list[AnnouncementFactCandidate]:
    if context.reasoner is None:
        return []
    request = _build_reasoner_request(parsed_artifact, fact_type)
    if request is None:
        return []
    result = context.reasoner.generate_structured(request)
    raw_facts = result.get("facts") if isinstance(result, Mapping) else None
    if not isinstance(raw_facts, Sequence) or isinstance(raw_facts, str):
        return []

    candidates: list[AnnouncementFactCandidate] = []
    for raw_fact in raw_facts:
        if not isinstance(raw_fact, Mapping):
            continue
        quote = raw_fact.get("quote")
        fact_content = raw_fact.get("fact_content")
        if not isinstance(quote, str) or not isinstance(fact_content, Mapping):
            continue
        span = _span_for_quote(parsed_artifact, quote)
        if span is None:
            continue
        related_mentions = [
            EntityMention(name=name, role="related_entity")
            for name in raw_fact.get("related_mentions", [])
            if isinstance(name, str) and name.strip()
        ]
        related_entities = context.entity_anchorer.resolve_related_mentions(
            related_mentions
        )
        confidence = raw_fact.get("confidence", 0.65)
        candidates.append(
            build_fact_candidate(
                parsed_artifact,
                context,
                fact_type=fact_type,
                fact_content={
                    "disclosure_type": fact_type.value,
                    "summary": quote,
                    "reasoner_fact_content": dict(fact_content),
                },
                evidence_spans=[span],
                related_entities=related_entities,
                confidence=max(0.0, min(1.0, float(confidence))),
            )
        )
    return candidates


def _build_reasoner_request(
    parsed_artifact: ParsedAnnouncementArtifact,
    fact_type: FactType,
) -> StructuredExtractionRequest | None:
    hints = _REASONER_HINTS[fact_type]
    raw_segments: list[tuple[str, str, str, str | None]] = []
    for index, source in enumerate(iter_evidence_sources(parsed_artifact), start=1):
        if any(pattern.search(source.text) for pattern in hints):
            raw_segments.append(
                (
                    f"segment-{index:04d}",
                    source.section_id,
                    source.text,
                    source.table_ref,
                )
            )
    segments = bounded_segments(raw_segments)
    if not segments:
        return None
    return StructuredExtractionRequest(
        announcement_id=parsed_artifact.announcement_id,
        fact_type=fact_type.value,
        segments=segments,
        schema=ex1_reasoner_schema(fact_type.value),
    )


def _span_for_quote(
    parsed_artifact: ParsedAnnouncementArtifact,
    quote: str,
) -> EvidenceSpan | None:
    normalized_quote = " ".join(quote.split())
    if not normalized_quote:
        return None
    for source in iter_evidence_sources(parsed_artifact):
        offset = source.text.find(quote)
        if offset == -1:
            continue
        return EvidenceSpan(
            section_id=source.section_id,
            start_offset=offset,
            end_offset=offset + len(quote),
            quote=quote,
            table_ref=source.table_ref,
        )
    return None


def _dedupe_candidates(
    candidates: Sequence[AnnouncementFactCandidate],
) -> list[AnnouncementFactCandidate]:
    deduped: list[AnnouncementFactCandidate] = []
    seen: set[str] = set()
    for candidate in candidates:
        if candidate.fact_id in seen:
            continue
        seen.add(candidate.fact_id)
        deduped.append(candidate)
    return deduped


__all__ = [
    "AnnouncementFactCandidate",
    "EntityAnchor",
    "EntityAnchorer",
    "EntityMention",
    "EntityRegistryClient",
    "EntityResolution",
    "EvidenceSpan",
    "ExtractionContext",
    "FactType",
    "ReasonerRuntimeBridge",
    "StructuredExtractionRequest",
    "StructuredExtractionSegment",
    "StructuredReasoner",
    "build_evidence_span",
    "build_table_evidence_span",
    "classify_disclosure_types",
    "extract_fact_candidates",
]
