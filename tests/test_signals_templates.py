from __future__ import annotations

from subsystem_announcement.extract import FactType, extract_fact_candidates
from subsystem_announcement.signals import (
    SIGNAL_TEMPLATES,
    SignalDirection,
    classify_signal_for_fact,
)

from .extract_fixtures import make_artifact


def test_signal_templates_cover_all_fact_types() -> None:
    assert set(SIGNAL_TEMPLATES) == set(FactType)
    assert all(template.fact_type == fact_type for fact_type, template in SIGNAL_TEMPLATES.items())


def test_earnings_performance_direction_mapping() -> None:
    template = SIGNAL_TEMPLATES[FactType.EARNINGS_PREANNOUNCE]

    assert _decision(
        FactType.EARNINGS_PREANNOUNCE,
        {"performance_direction": "positive"},
        template,
    ).direction is SignalDirection.POSITIVE
    assert _decision(
        FactType.EARNINGS_PREANNOUNCE,
        {"performance_direction": "negative"},
        template,
    ).direction is SignalDirection.NEGATIVE
    assert _decision(
        FactType.EARNINGS_PREANNOUNCE,
        {"performance_direction": "uncertain"},
        template,
    ).direction is SignalDirection.NEUTRAL


def test_equity_pledge_action_mapping() -> None:
    template = SIGNAL_TEMPLATES[FactType.EQUITY_PLEDGE]

    assert _decision(
        FactType.EQUITY_PLEDGE,
        {"pledge_action": "pledge"},
        template,
    ).direction is SignalDirection.NEGATIVE
    assert _decision(
        FactType.EQUITY_PLEDGE,
        {"pledge_action": "release"},
        template,
    ).direction is SignalDirection.POSITIVE


def test_regulatory_action_never_maps_positive() -> None:
    template = SIGNAL_TEMPLATES[FactType.REGULATORY_ACTION]

    directions = [
        _decision(
            FactType.REGULATORY_ACTION,
            {"regulatory_action_type": action_type},
            template,
        ).direction
        for action_type in (
            "administrative_penalty",
            "investigation",
            "regulatory_notice",
        )
    ]

    assert directions == [
        SignalDirection.NEGATIVE,
        SignalDirection.NEGATIVE,
        SignalDirection.NEUTRAL,
    ]
    assert SignalDirection.POSITIVE not in directions


def test_classifier_returns_none_for_insufficient_fact_inputs() -> None:
    template = SIGNAL_TEMPLATES[FactType.MAJOR_CONTRACT]
    fact = _fact(FactType.MAJOR_CONTRACT, {"event": "major_contract"})

    assert classify_signal_for_fact(
        fact.model_copy(update={"evidence_spans": []}),
        template,
    ) is None
    assert classify_signal_for_fact(
        fact.model_copy(update={"fact_content": {}}),
        template,
    ) is None
    assert classify_signal_for_fact(
        fact.model_copy(update={"primary_entity_id": ""}),
        template,
    ) is None
    assert classify_signal_for_fact(
        fact.model_copy(update={"confidence": template.min_confidence - 0.01}),
        template,
    ) is None
    assert classify_signal_for_fact(
        fact.model_copy(update={"source_reference": {"official_url": ""}}),
        template,
    ) is None


def test_unknown_direction_returns_none() -> None:
    template = SIGNAL_TEMPLATES[FactType.TRADING_HALT_RESUME]

    assert (
        classify_signal_for_fact(
            _fact(FactType.TRADING_HALT_RESUME, {"trading_status": "unknown"}),
            template,
        )
        is None
    )


def _decision(fact_type: FactType, content: dict[str, str], template):
    decision = classify_signal_for_fact(_fact(fact_type, content), template)
    assert decision is not None
    return decision


def _fact(fact_type: FactType, content: dict[str, str]):
    artifact = make_artifact(
        "证券代码：600000\n证券简称：测试公司\n"
        "公司与华东能源签订重大合同，合同金额为1000万元。",
        announcement_id=f"ann-{fact_type.value}",
    )
    fact = extract_fact_candidates(artifact)[0]
    return fact.model_copy(
        update={
            "fact_type": fact_type,
            "fact_content": content,
            "confidence": 0.9,
        }
    )
