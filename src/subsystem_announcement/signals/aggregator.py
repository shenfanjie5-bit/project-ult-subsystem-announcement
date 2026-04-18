"""Aggregate Ex-1 facts into conservative Ex-2 signal candidates."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from datetime import datetime, timezone
from typing import Any

from subsystem_announcement.extract import AnnouncementFactCandidate

from .candidates import AnnouncementSignalCandidate, make_signal_id
from .classifier import classify_signal_for_fact
from .templates import SIGNAL_TEMPLATES


SignalFunc = Callable[
    [Sequence[AnnouncementFactCandidate]],
    Sequence[AnnouncementSignalCandidate] | Awaitable[Sequence[AnnouncementSignalCandidate]],
]


def derive_signal_candidates(
    facts: Sequence[AnnouncementFactCandidate],
    *,
    generated_at: datetime | None = None,
) -> list[AnnouncementSignalCandidate]:
    """Derive deterministic Ex-2 signal candidates from Ex-1 facts."""

    if not facts:
        return []

    timestamp = generated_at or datetime.now(timezone.utc)
    signals: list[AnnouncementSignalCandidate] = []
    seen_signal_ids: set[str] = set()

    for fact in facts:
        template = SIGNAL_TEMPLATES.get(fact.fact_type)
        if template is None:
            continue
        decision = classify_signal_for_fact(fact, template)
        if decision is None:
            continue
        affected_entities = _affected_entities(fact)
        if not affected_entities:
            continue

        source_fact_ids = [fact.fact_id]
        identity_payload: dict[str, Any] = {
            "direction": decision.direction,
            "magnitude": decision.magnitude,
            "affected_entities": affected_entities,
            "time_horizon": decision.time_horizon,
            "confidence": decision.confidence,
        }
        signal_id = make_signal_id(
            fact.announcement_id,
            decision.signal_type,
            source_fact_ids,
            fact.evidence_spans,
            identity_payload,
        )
        if signal_id in seen_signal_ids:
            continue

        signals.append(
            AnnouncementSignalCandidate(
                signal_id=signal_id,
                announcement_id=fact.announcement_id,
                signal_type=decision.signal_type,
                direction=decision.direction,
                magnitude=decision.magnitude,
                affected_entities=affected_entities,
                time_horizon=decision.time_horizon,
                source_fact_ids=source_fact_ids,
                source_reference=dict(fact.source_reference),
                evidence_spans=list(fact.evidence_spans),
                confidence=decision.confidence,
                generated_at=timestamp,
            )
        )
        seen_signal_ids.add(signal_id)

    return signals


def _affected_entities(fact: AnnouncementFactCandidate) -> list[str]:
    entities: list[str] = []
    seen: set[str] = set()
    for entity_id in (fact.primary_entity_id, *fact.related_entity_ids):
        if not entity_id or entity_id in seen:
            continue
        seen.add(entity_id)
        entities.append(entity_id)
    return entities
