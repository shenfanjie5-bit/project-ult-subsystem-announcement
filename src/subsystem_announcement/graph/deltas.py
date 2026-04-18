"""Materialize conservative Ex-3 graph delta candidates from Ex-1 facts."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from datetime import datetime, timezone

from subsystem_announcement.extract import AnnouncementFactCandidate

from .candidates import AnnouncementGraphDeltaCandidate, make_delta_id
from .guard import GraphDeltaGuard
from .rules import classify_graph_delta_intent


GraphFunc = Callable[
    [Sequence[AnnouncementFactCandidate]],
    Sequence[AnnouncementGraphDeltaCandidate]
    | Awaitable[Sequence[AnnouncementGraphDeltaCandidate]],
]


def derive_graph_delta_candidates(
    facts: Sequence[AnnouncementFactCandidate],
    *,
    generated_at: datetime | None = None,
    guard: GraphDeltaGuard | None = None,
) -> list[AnnouncementGraphDeltaCandidate]:
    """Derive deterministic high-threshold Ex-3 candidates from Ex-1 facts."""

    if not facts:
        return []

    timestamp = generated_at or datetime.now(timezone.utc)
    active_guard = guard or GraphDeltaGuard()
    graph_deltas: list[AnnouncementGraphDeltaCandidate] = []
    seen_delta_ids: set[str] = set()

    for fact in facts:
        intent = classify_graph_delta_intent(fact)
        if intent is None:
            continue
        guard_result = active_guard.check(fact, intent)
        if not guard_result.allow:
            continue

        source_fact_ids = [fact.fact_id]
        properties = dict(intent.properties)
        delta_id = make_delta_id(
            fact.announcement_id,
            intent.relation_type,
            intent.source_node,
            intent.target_node,
            source_fact_ids,
            fact.evidence_spans,
            properties,
        )
        if delta_id in seen_delta_ids:
            continue

        graph_deltas.append(
            AnnouncementGraphDeltaCandidate(
                delta_id=delta_id,
                announcement_id=fact.announcement_id,
                delta_type=intent.delta_type,
                source_node=intent.source_node,
                target_node=intent.target_node,
                relation_type=intent.relation_type,
                properties=properties,
                source_fact_ids=source_fact_ids,
                source_reference=dict(fact.source_reference),
                evidence_spans=list(fact.evidence_spans),
                confidence=fact.confidence,
                generated_at=timestamp,
            )
        )
        seen_delta_ids.add(delta_id)

    return graph_deltas
