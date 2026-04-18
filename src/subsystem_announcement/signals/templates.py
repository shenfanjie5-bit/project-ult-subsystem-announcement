"""Conservative Ex-2 signal templates keyed by Ex-1 fact type."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from subsystem_announcement.extract import FactType

from .candidates import SignalTimeHorizon


@dataclass(frozen=True)
class SignalTemplate:
    """Configuration for converting one fact type into an Ex-2 signal."""

    fact_type: FactType
    signal_type: str
    base_magnitude: float
    time_horizon: SignalTimeHorizon
    required_content_keys: tuple[str, ...]
    min_confidence: float = 0.75


SIGNAL_TEMPLATES: Mapping[FactType, SignalTemplate] = {
    FactType.EARNINGS_PREANNOUNCE: SignalTemplate(
        fact_type=FactType.EARNINGS_PREANNOUNCE,
        signal_type="earnings_preannounce_outlook",
        base_magnitude=0.72,
        time_horizon=SignalTimeHorizon.SHORT_TERM,
        required_content_keys=("performance_direction",),
    ),
    FactType.MAJOR_CONTRACT: SignalTemplate(
        fact_type=FactType.MAJOR_CONTRACT,
        signal_type="major_contract_award",
        base_magnitude=0.68,
        time_horizon=SignalTimeHorizon.MEDIUM_TERM,
        required_content_keys=("event",),
    ),
    FactType.SHAREHOLDER_CHANGE: SignalTemplate(
        fact_type=FactType.SHAREHOLDER_CHANGE,
        signal_type="shareholder_position_change",
        base_magnitude=0.62,
        time_horizon=SignalTimeHorizon.SHORT_TERM,
        required_content_keys=("shareholder_change_type",),
    ),
    FactType.EQUITY_PLEDGE: SignalTemplate(
        fact_type=FactType.EQUITY_PLEDGE,
        signal_type="equity_pledge_status",
        base_magnitude=0.66,
        time_horizon=SignalTimeHorizon.SHORT_TERM,
        required_content_keys=("pledge_action",),
    ),
    FactType.REGULATORY_ACTION: SignalTemplate(
        fact_type=FactType.REGULATORY_ACTION,
        signal_type="regulatory_action_risk",
        base_magnitude=0.74,
        time_horizon=SignalTimeHorizon.IMMEDIATE,
        required_content_keys=("regulatory_action_type",),
    ),
    FactType.TRADING_HALT_RESUME: SignalTemplate(
        fact_type=FactType.TRADING_HALT_RESUME,
        signal_type="trading_status_change",
        base_magnitude=0.58,
        time_horizon=SignalTimeHorizon.IMMEDIATE,
        required_content_keys=("trading_status",),
    ),
    FactType.FUNDRAISING_CHANGE: SignalTemplate(
        fact_type=FactType.FUNDRAISING_CHANGE,
        signal_type="fundraising_plan_change",
        base_magnitude=0.55,
        time_horizon=SignalTimeHorizon.MEDIUM_TERM,
        required_content_keys=("fundraising_change_type",),
    ),
}
