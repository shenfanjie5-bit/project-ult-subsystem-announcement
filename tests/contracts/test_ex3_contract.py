from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from subsystem_announcement.extract import AnnouncementFactCandidate, EvidenceSpan, FactType
from subsystem_announcement.graph import (
    AnnouncementGraphDeltaCandidate,
    derive_graph_delta_candidates,
)
from subsystem_announcement.runtime.submit import submit_candidates


FORBIDDEN_KEYS = {"submitted_at", "ingest_seq", "layer_b_receipt_id", "local_path"}
GENERATED_AT = datetime(2026, 4, 18, 10, 0, tzinfo=timezone.utc)


class RecordingSubsystem:
    def __init__(self) -> None:
        self.submissions: list[dict[str, Any]] = []

    def submit(self, candidate: dict[str, Any]) -> dict[str, Any]:
        self.submissions.append(candidate)
        return {
            "accepted": True,
            "receipt_id": f"receipt-{len(self.submissions)}",
            "warnings": (),
            "errors": (),
        }


def test_ex3_payload_shape_and_forbidden_metadata() -> None:
    fact = _major_contract_fact()
    delta = derive_graph_delta_candidates([fact], generated_at=GENERATED_AT)[0]

    payload = delta.to_ex_payload()

    assert payload["ex_type"] == "Ex-3"
    assert payload["delta_id"]
    assert payload["announcement_id"] == fact.announcement_id
    assert payload["delta_type"] == "add_edge"
    assert payload["relation_type"] == "supply_contract"
    assert payload["source_node"] == fact.primary_entity_id
    assert payload["target_node"] == fact.related_entity_ids[0]
    assert payload["source_fact_ids"] == [fact.fact_id]
    assert payload["source_reference"] == fact.source_reference
    assert payload["evidence_spans"] == [
        span.model_dump(mode="json") for span in fact.evidence_spans
    ]
    AnnouncementGraphDeltaCandidate.model_validate(payload)
    _assert_no_forbidden_keys(payload)


def test_ex3_submit_path_validates_contract_payload_without_runtime_metadata() -> None:
    fact = _major_contract_fact("ann-ex3-submit-contract")
    delta = derive_graph_delta_candidates([fact], generated_at=GENERATED_AT)[0]
    subsystem = RecordingSubsystem()

    result = submit_candidates([delta], subsystem)  # type: ignore[arg-type]

    assert result.submitted == 1
    assert result.failed == 0
    assert subsystem.submissions == [delta.to_ex_payload()]
    _assert_no_forbidden_keys(subsystem.submissions[0])


def test_ex3_source_reference_is_not_forged_when_missing() -> None:
    fact = _major_contract_fact("ann-ex3-missing-source").model_copy(
        update={"source_reference": {}}
    )

    assert derive_graph_delta_candidates([fact], generated_at=GENERATED_AT) == []


def _major_contract_fact(announcement_id: str = "ann-ex3-contract") -> AnnouncementFactCandidate:
    return AnnouncementFactCandidate(
        fact_id=f"fact:{announcement_id}:major_contract:1",
        announcement_id=announcement_id,
        fact_type=FactType.MAJOR_CONTRACT,
        primary_entity_id="ts_code:600000.SH",
        related_entity_ids=["entity:huadong-energy"],
        fact_content={"event": "major_contract"},
        confidence=0.93,
        source_reference={
            "announcement_id": announcement_id,
            "official_url": f"https://static.sse.com.cn/disclosure/{announcement_id}.pdf",
            "source_exchange": "sse",
            "attachment_type": "pdf",
        },
        evidence_spans=[
            _span("公司与华东能源签订重大合同。", "sec-1"),
            _span("双方合同金额为1000万元。", "sec-2"),
        ],
        extracted_at=datetime(2026, 4, 18, 9, 30, tzinfo=timezone.utc),
    )


def _span(quote: str, section_id: str) -> EvidenceSpan:
    return EvidenceSpan(
        section_id=section_id,
        start_offset=0,
        end_offset=len(quote),
        quote=quote,
    )


def _assert_no_forbidden_keys(value: Any) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            assert key not in FORBIDDEN_KEYS
            _assert_no_forbidden_keys(item)
    elif isinstance(value, list):
        for item in value:
            _assert_no_forbidden_keys(item)
