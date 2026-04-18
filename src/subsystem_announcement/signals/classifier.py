"""Rule-based Ex-2 signal decisions from stable Ex-1 facts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from subsystem_announcement.extract import AnnouncementFactCandidate, FactType

from .candidates import SignalDirection, SignalTimeHorizon
from .templates import SignalTemplate


@dataclass(frozen=True)
class SignalDecision:
    """A classified signal interpretation before candidate materialization."""

    signal_type: str
    direction: SignalDirection
    magnitude: float
    time_horizon: SignalTimeHorizon
    confidence: float


def classify_signal_for_fact(
    fact: AnnouncementFactCandidate,
    template: SignalTemplate,
) -> SignalDecision | None:
    """Classify one fact into an Ex-2 signal decision, or return ``None``."""

    if fact.fact_type != template.fact_type:
        return None
    if not fact.evidence_spans:
        return None
    official_url = fact.source_reference.get("official_url")
    if not isinstance(official_url, str) or not official_url.strip():
        return None
    if not fact.primary_entity_id:
        return None
    if fact.confidence < template.min_confidence:
        return None
    if any(key not in fact.fact_content for key in template.required_content_keys):
        return None

    direction = _direction_for_fact(fact)
    if direction is None:
        return None

    magnitude = template.base_magnitude
    if direction is SignalDirection.NEUTRAL:
        magnitude = min(magnitude, 0.5)

    return SignalDecision(
        signal_type=template.signal_type,
        direction=direction,
        magnitude=magnitude,
        time_horizon=template.time_horizon,
        confidence=fact.confidence,
    )


def _direction_for_fact(fact: AnnouncementFactCandidate) -> SignalDirection | None:
    content = fact.fact_content
    if fact.fact_type is FactType.EARNINGS_PREANNOUNCE:
        return _map_value(
            content.get("performance_direction"),
            {
                "positive": SignalDirection.POSITIVE,
                "negative": SignalDirection.NEGATIVE,
                "uncertain": SignalDirection.NEUTRAL,
            },
        )
    if fact.fact_type is FactType.MAJOR_CONTRACT:
        return _map_value(
            content.get("event"),
            {
                "major_contract": SignalDirection.POSITIVE,
            },
        )
    if fact.fact_type is FactType.SHAREHOLDER_CHANGE:
        return _map_value(
            content.get("shareholder_change_type"),
            {
                "increase": SignalDirection.POSITIVE,
                "decrease": SignalDirection.NEGATIVE,
                "control_change": SignalDirection.NEUTRAL,
                "change": SignalDirection.NEUTRAL,
            },
        )
    if fact.fact_type is FactType.EQUITY_PLEDGE:
        return _map_value(
            content.get("pledge_action"),
            {
                "pledge": SignalDirection.NEGATIVE,
                "release": SignalDirection.POSITIVE,
            },
        )
    if fact.fact_type is FactType.REGULATORY_ACTION:
        return _map_value(
            content.get("regulatory_action_type"),
            {
                "administrative_penalty": SignalDirection.NEGATIVE,
                "investigation": SignalDirection.NEGATIVE,
                "disciplinary_action": SignalDirection.NEGATIVE,
                "regulatory_measure": SignalDirection.NEGATIVE,
                "regulatory_notice": SignalDirection.NEUTRAL,
                "inquiry_letter": SignalDirection.NEUTRAL,
            },
        )
    if fact.fact_type is FactType.TRADING_HALT_RESUME:
        return _map_value(
            content.get("trading_status"),
            {
                "resume": SignalDirection.POSITIVE,
                "halt": SignalDirection.NEGATIVE,
            },
        )
    if fact.fact_type is FactType.FUNDRAISING_CHANGE:
        return _map_value(
            content.get("fundraising_change_type"),
            {
                "use_or_plan_change": SignalDirection.NEUTRAL,
                "scale_increase": SignalDirection.POSITIVE,
                "scale_decrease": SignalDirection.NEGATIVE,
                "termination": SignalDirection.NEGATIVE,
            },
        )
    return None


def _map_value(
    raw_value: Any,
    mapping: dict[str, SignalDirection],
) -> SignalDirection | None:
    if not isinstance(raw_value, str):
        return None
    return mapping.get(raw_value.strip().lower())
