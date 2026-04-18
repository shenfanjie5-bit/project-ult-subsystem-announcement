from __future__ import annotations

from datetime import datetime, timezone

from subsystem_announcement.extract import AnnouncementFactCandidate, EvidenceSpan, FactType
from subsystem_announcement.graph import (
    GraphDeltaType,
    GraphRelationType,
    classify_graph_delta_intent,
    derive_graph_delta_candidates,
)


GENERATED_AT = datetime(2026, 4, 18, 10, 0, tzinfo=timezone.utc)


def test_unsupported_fact_type_does_not_produce_graph_intent() -> None:
    fact = _fact(
        fact_type=FactType.EARNINGS_PREANNOUNCE,
        fact_content={"performance_direction": "positive"},
        quotes=["公司预计2026年净利润同比增长50%。", "本公告为业绩预告。"],
    )

    assert classify_graph_delta_intent(fact) is None
    assert derive_graph_delta_candidates([fact]) == []


def test_shareholder_control_change_intent_requires_resolved_related_entity() -> None:
    fact = _fact(
        fact_type=FactType.SHAREHOLDER_CHANGE,
        related_entity_ids=["entity:new-controller"],
        fact_content={"shareholder_change_type": "control_change"},
        quotes=[
            "本次权益变动将导致公司控股股东变更为明德投资。",
            "公司实际控制人将发生变更。",
        ],
    )

    intent = classify_graph_delta_intent(fact)

    assert intent is not None
    assert intent.delta_type is GraphDeltaType.UPDATE_EDGE
    assert intent.relation_type is GraphRelationType.CONTROL
    assert intent.source_node == "entity:new-controller"
    assert intent.target_node == "ts_code:600000.SH"
    assert intent.properties == {"change_type": "control_change"}


def test_shareholder_ratio_intent_records_explicit_ratio_fields() -> None:
    fact = _fact(
        fact_type=FactType.SHAREHOLDER_CHANGE,
        related_entity_ids=["entity:shareholder"],
        fact_content={"shareholder_change_type": "decrease"},
        quotes=[
            "股东明德投资减持后持股比例降至5%。",
            "本次权益变动后明德投资持股比例为5%。",
        ],
    )

    delta = derive_graph_delta_candidates([fact], generated_at=GENERATED_AT)[0]

    assert delta.relation_type is GraphRelationType.SHAREHOLDING
    assert delta.properties["after_ratio"] == "5%"
    assert delta.source_fact_ids == [fact.fact_id]
    assert delta.source_reference == fact.source_reference
    assert delta.evidence_spans == fact.evidence_spans


def test_shareholder_change_without_ratio_does_not_produce_shareholding_delta() -> None:
    fact = _fact(
        fact_type=FactType.SHAREHOLDER_CHANGE,
        related_entity_ids=["entity:shareholder"],
        fact_content={"shareholder_change_type": "decrease"},
        quotes=["股东明德投资减持公司股份。", "本次权益变动不会导致控制权变化。"],
    )

    assert classify_graph_delta_intent(fact) is None
    assert derive_graph_delta_candidates([fact]) == []


def test_major_contract_strong_action_produces_supply_contract_delta() -> None:
    fact = _fact(
        fact_type=FactType.MAJOR_CONTRACT,
        related_entity_ids=["entity:huadong-energy"],
        fact_content={"event": "major_contract"},
        quotes=["公司与华东能源签订重大合同。", "双方合同金额为1000万元。"],
    )

    delta = derive_graph_delta_candidates([fact], generated_at=GENERATED_AT)[0]

    assert delta.relation_type is GraphRelationType.SUPPLY_CONTRACT
    assert delta.delta_type is GraphDeltaType.ADD_EDGE
    assert delta.source_node == "ts_code:600000.SH"
    assert delta.target_node == "entity:huadong-energy"
    assert delta.confidence == fact.confidence


def test_major_contract_cooperation_action_produces_cooperation_delta() -> None:
    fact = _fact(
        fact_type=FactType.MAJOR_CONTRACT,
        related_entity_ids=["entity:research-partner"],
        fact_content={"event": "major_contract"},
        quotes=[
            "公司与研究院签署战略合作协议。",
            "双方将在新能源材料领域开展合作。",
        ],
    )

    intent = classify_graph_delta_intent(fact)

    assert intent is not None
    assert intent.relation_type is GraphRelationType.COOPERATION


def test_major_contract_ambiguous_wording_does_not_produce_graph_delta() -> None:
    fact = _fact(
        fact_type=FactType.MAJOR_CONTRACT,
        related_entity_ids=["entity:potential-partner"],
        fact_content={"event": "major_contract"},
        quotes=[
            "公司拟与潜在合作方签署战略合作意向。",
            "双方目前仅签订框架协议，后续存在不确定性。",
        ],
    )

    assert classify_graph_delta_intent(fact) is None
    assert derive_graph_delta_candidates([fact]) == []


def test_major_contract_requires_resolved_related_entity_and_two_evidence_spans() -> None:
    unresolved = _fact(
        fact_type=FactType.MAJOR_CONTRACT,
        related_entity_ids=["unresolved:counterparty"],
        fact_content={"event": "major_contract"},
        quotes=["公司与华东能源签订重大合同。", "双方合同金额为1000万元。"],
    )
    single_span = _fact(
        fact_type=FactType.MAJOR_CONTRACT,
        related_entity_ids=["entity:huadong-energy"],
        fact_content={"event": "major_contract"},
        quotes=["公司与华东能源签订重大合同。"],
    )

    assert classify_graph_delta_intent(unresolved) is None
    assert derive_graph_delta_candidates([unresolved]) == []
    assert classify_graph_delta_intent(single_span) is not None
    assert derive_graph_delta_candidates([single_span]) == []


def test_low_confidence_fact_does_not_materialize_graph_delta() -> None:
    fact = _fact(
        fact_type=FactType.MAJOR_CONTRACT,
        related_entity_ids=["entity:huadong-energy"],
        fact_content={"event": "major_contract"},
        confidence=0.89,
        quotes=["公司与华东能源签订重大合同。", "双方合同金额为1000万元。"],
    )

    assert classify_graph_delta_intent(fact) is not None
    assert derive_graph_delta_candidates([fact]) == []


def test_duplicate_graph_delta_ids_are_deduped_in_input_order() -> None:
    fact = _fact(
        fact_type=FactType.MAJOR_CONTRACT,
        related_entity_ids=["entity:huadong-energy"],
        fact_content={"event": "major_contract"},
        quotes=["公司与华东能源签订重大合同。", "双方合同金额为1000万元。"],
    )

    deltas = derive_graph_delta_candidates([fact, fact], generated_at=GENERATED_AT)

    assert len(deltas) == 1
    assert deltas[0].source_fact_ids == [fact.fact_id]


def _fact(
    *,
    fact_type: FactType,
    fact_content: dict[str, object],
    quotes: list[str],
    related_entity_ids: list[str] | None = None,
    confidence: float = 0.93,
) -> AnnouncementFactCandidate:
    announcement_id = "ann-graph-rules"
    return AnnouncementFactCandidate(
        fact_id=f"fact:{announcement_id}:{fact_type.value}:1",
        announcement_id=announcement_id,
        fact_type=fact_type,
        primary_entity_id="ts_code:600000.SH",
        related_entity_ids=related_entity_ids or [],
        fact_content=fact_content,
        confidence=confidence,
        source_reference=_source_reference(announcement_id),
        evidence_spans=[
            _span(quote, f"sec-{index}")
            for index, quote in enumerate(quotes, start=1)
        ],
        extracted_at=datetime(2026, 4, 18, 9, 30, tzinfo=timezone.utc),
    )


def _source_reference(announcement_id: str) -> dict[str, str]:
    return {
        "announcement_id": announcement_id,
        "official_url": f"https://static.sse.com.cn/disclosure/{announcement_id}.pdf",
        "source_exchange": "sse",
        "attachment_type": "pdf",
    }


def _span(quote: str, section_id: str) -> EvidenceSpan:
    return EvidenceSpan(
        section_id=section_id,
        start_offset=0,
        end_offset=len(quote),
        quote=quote,
    )
