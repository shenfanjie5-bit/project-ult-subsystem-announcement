from __future__ import annotations

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from subsystem_announcement.extract import EvidenceSpan
from subsystem_announcement.graph import (
    AnnouncementGraphDeltaCandidate,
    GraphDeltaType,
    GraphRelationType,
    make_delta_id,
)


GENERATED_AT = datetime(2026, 4, 18, 10, 0, tzinfo=timezone.utc)


def test_graph_delta_candidate_payload_shape_and_validation() -> None:
    candidate = _candidate()

    payload = candidate.to_ex_payload()

    assert payload["ex_type"] == "Ex-3"
    assert payload["delta_id"] == candidate.delta_id
    assert payload["delta_type"] == "add_edge"
    assert payload["relation_type"] == "supply_contract"
    assert payload["source_fact_ids"] == ["fact:ann-graph-schema:major_contract:1"]
    assert payload["source_reference"]["official_url"].startswith("https://")
    assert len(payload["evidence_spans"]) == 2
    AnnouncementGraphDeltaCandidate.model_validate(payload)


def test_graph_delta_candidate_rejects_extra_fields_and_missing_required_fields() -> None:
    payload = _candidate().model_dump(mode="python")

    with pytest.raises(ValidationError):
        AnnouncementGraphDeltaCandidate.model_validate({**payload, "local_path": "/tmp/a.pdf"})

    for field_name in ("source_fact_ids", "source_reference"):
        invalid = dict(payload)
        invalid.pop(field_name)
        with pytest.raises(ValidationError):
            AnnouncementGraphDeltaCandidate.model_validate(invalid)


def test_graph_delta_candidate_requires_two_evidence_spans_and_source_fact() -> None:
    payload = _candidate().model_dump(mode="python")

    with pytest.raises(ValidationError):
        AnnouncementGraphDeltaCandidate.model_validate({**payload, "evidence_spans": payload["evidence_spans"][:1]})

    with pytest.raises(ValidationError):
        AnnouncementGraphDeltaCandidate.model_validate({**payload, "source_fact_ids": []})


def test_graph_delta_type_and_relation_type_are_whitelisted() -> None:
    payload = _candidate().model_dump(mode="python")

    with pytest.raises(ValidationError):
        AnnouncementGraphDeltaCandidate.model_validate({**payload, "delta_type": "delete_edge"})

    with pytest.raises(ValidationError):
        AnnouncementGraphDeltaCandidate.model_validate({**payload, "relation_type": "rumor"})


def test_make_delta_id_is_stable_and_sensitive_to_core_fields() -> None:
    spans = [_span("公司与华东能源签订重大合同。"), _span("双方合同金额为1000万元。", "sec-2")]
    source_fact_ids = ["fact:ann-delta-id:major_contract:1"]
    properties = {"event": "major_contract"}

    first = make_delta_id(
        "ann-delta-id",
        GraphRelationType.SUPPLY_CONTRACT,
        "ts_code:600000.SH",
        "entity:huadong-energy",
        source_fact_ids,
        spans,
        properties,
    )
    second = make_delta_id(
        "ann-delta-id",
        GraphRelationType.SUPPLY_CONTRACT,
        "ts_code:600000.SH",
        "entity:huadong-energy",
        source_fact_ids,
        spans,
        properties,
    )
    changed_target = make_delta_id(
        "ann-delta-id",
        GraphRelationType.SUPPLY_CONTRACT,
        "ts_code:600000.SH",
        "entity:other",
        source_fact_ids,
        spans,
        properties,
    )
    changed_properties = make_delta_id(
        "ann-delta-id",
        GraphRelationType.SUPPLY_CONTRACT,
        "ts_code:600000.SH",
        "entity:huadong-energy",
        source_fact_ids,
        spans,
        {"event": "major_contract", "contract_status": "terminated"},
    )
    changed_evidence = make_delta_id(
        "ann-delta-id",
        GraphRelationType.SUPPLY_CONTRACT,
        "ts_code:600000.SH",
        "entity:huadong-energy",
        source_fact_ids,
        [spans[0], _span("双方合同金额为2000万元。", "sec-2")],
        properties,
    )

    assert first == second
    assert first.startswith("graph_delta:ann-delta-id:supply_contract:")
    assert changed_target != first
    assert changed_properties != first
    assert changed_evidence != first


def _candidate() -> AnnouncementGraphDeltaCandidate:
    spans = [_span("公司与华东能源签订重大合同。"), _span("双方合同金额为1000万元。", "sec-2")]
    properties = {"event": "major_contract"}
    source_fact_ids = ["fact:ann-graph-schema:major_contract:1"]
    delta_id = make_delta_id(
        "ann-graph-schema",
        GraphRelationType.SUPPLY_CONTRACT,
        "ts_code:600000.SH",
        "entity:huadong-energy",
        source_fact_ids,
        spans,
        properties,
    )
    return AnnouncementGraphDeltaCandidate(
        delta_id=delta_id,
        announcement_id="ann-graph-schema",
        delta_type=GraphDeltaType.ADD_EDGE,
        source_node="ts_code:600000.SH",
        target_node="entity:huadong-energy",
        relation_type=GraphRelationType.SUPPLY_CONTRACT,
        properties=properties,
        source_fact_ids=source_fact_ids,
        source_reference=_source_reference("ann-graph-schema"),
        evidence_spans=spans,
        confidence=0.93,
        generated_at=GENERATED_AT,
    )


def _source_reference(announcement_id: str) -> dict[str, str]:
    return {
        "announcement_id": announcement_id,
        "official_url": f"https://static.sse.com.cn/disclosure/{announcement_id}.pdf",
        "source_exchange": "sse",
        "attachment_type": "pdf",
    }


def _span(quote: str, section_id: str = "sec-1") -> EvidenceSpan:
    return EvidenceSpan(
        section_id=section_id,
        start_offset=0,
        end_offset=len(quote),
        quote=quote,
    )
