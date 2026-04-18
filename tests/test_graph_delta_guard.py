from __future__ import annotations

from datetime import datetime, timezone

import pytest

from subsystem_announcement.extract import AnnouncementFactCandidate, EvidenceSpan, FactType
from subsystem_announcement.graph import (
    GraphDeltaGuard,
    GraphDeltaIntent,
    GraphDeltaType,
    GraphRelationType,
    is_resolved_entity_id,
)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("ts_code:600000.SH", True),
        ("entity:shareholder", True),
        ("", False),
        ("   ", False),
        ("unresolved", False),
        ("unresolved:abc", False),
        ("unresolved_related:abc", False),
        ("mention:华东能源", False),
    ],
)
def test_is_resolved_entity_id(value: str, expected: bool) -> None:
    assert is_resolved_entity_id(value) is expected


def test_graph_delta_guard_allows_resolved_fact_with_two_evidence_spans() -> None:
    result = GraphDeltaGuard().check(_fact(), _intent())

    assert result.allow is True
    assert result.reasons == ()


def test_graph_delta_guard_rejects_single_evidence_span() -> None:
    fact = _fact(quotes=["公司与华东能源签订重大合同。"])

    result = GraphDeltaGuard().check(fact, _intent())

    assert result.allow is False
    assert "insufficient_evidence_spans" in result.reasons


def test_graph_delta_guard_rejects_unresolved_endpoints() -> None:
    fact = _fact()
    intent = _intent(source_node="unresolved:shareholder", target_node="mention:测试公司")

    result = GraphDeltaGuard().check(fact, intent)

    assert result.allow is False
    assert "unresolved_source_node" in result.reasons
    assert "unresolved_target_node" in result.reasons


def test_graph_delta_guard_rejects_low_confidence_fact() -> None:
    fact = _fact(confidence=0.89)

    result = GraphDeltaGuard().check(fact, _intent())

    assert result.allow is False
    assert "low_fact_confidence" in result.reasons


def test_graph_delta_guard_rejects_ambiguous_wording() -> None:
    fact = _fact(
        quotes=[
            "公司拟与华东能源签订重大合同。",
            "双方可能后续签署正式协议。",
        ]
    )

    result = GraphDeltaGuard().check(fact, _intent())

    assert result.allow is False
    assert "ambiguous_language" in result.reasons


def _intent(
    *,
    source_node: str = "ts_code:600000.SH",
    target_node: str = "entity:huadong-energy",
) -> GraphDeltaIntent:
    return GraphDeltaIntent(
        delta_type=GraphDeltaType.ADD_EDGE,
        relation_type=GraphRelationType.SUPPLY_CONTRACT,
        source_node=source_node,
        target_node=target_node,
        properties={"event": "major_contract"},
        reason="test",
    )


def _fact(
    *,
    confidence: float = 0.93,
    quotes: list[str] | None = None,
) -> AnnouncementFactCandidate:
    spans = [
        _span(quote, f"sec-{index}")
        for index, quote in enumerate(
            quotes
            or ["公司与华东能源签订重大合同。", "双方合同金额为1000万元。"],
            start=1,
        )
    ]
    return AnnouncementFactCandidate(
        fact_id="fact:ann-guard:major_contract:1",
        announcement_id="ann-guard",
        fact_type=FactType.MAJOR_CONTRACT,
        primary_entity_id="ts_code:600000.SH",
        related_entity_ids=["entity:huadong-energy"],
        fact_content={"event": "major_contract"},
        confidence=confidence,
        source_reference=_source_reference("ann-guard"),
        evidence_spans=spans,
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
