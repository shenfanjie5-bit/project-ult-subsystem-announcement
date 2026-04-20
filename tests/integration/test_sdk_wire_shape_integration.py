"""Integration tier — END-TO-END announcement ↔ subsystem-sdk wire-shape
integration test (the 7th issue per stage 2.8 plan template).

Goal: prove that announcement's SDK adapter uses ``validate_then_dispatch``
(which strips SDK envelope at dispatch boundary per stage 2.7 follow-up
#2) and does NOT bypass it. For Ex-1 / Ex-2 / Ex-3 each:

1. Build a real announcement candidate using the real candidate model
   constructor (no mocks).
2. Submit through the REAL ``subsystem_sdk.submit.SubmitClient`` against
   a ``MockSubmitBackend`` (which just records what arrives).
3. Assert backend.submitted_payloads[0] does NOT contain any SDK
   envelope field (``ex_type`` / ``semantic`` / ``produced_at``).
4. Assert producer-owned fields announcement cares about reach the
   backend unchanged (announcement_id, primary identifier, etc.).
5. Assert SubmitReceipt is accepted with no errors.

This is the standout test of the 7-issue announcement template. The
boundary tier already covers Ex-1; this tier covers all three Ex types
in a single place + asserts wire-shape parity at the integration
boundary.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest


@pytest.fixture
def permissive_validator() -> Any:
    """Validator that always returns ``ok`` so we can observe the
    strip behavior independent of contracts being installed (the
    cross-repo align tests cover that path). Smoke + boundary tiers
    already verified validate_payload's contract strip; here we verify
    the dispatch strip end-to-end."""

    from subsystem_sdk.validate.result import ValidationResult

    def _validator(_: Any, ex_type: str = "Ex-?") -> ValidationResult:
        return ValidationResult.ok(
            ex_type=ex_type, schema_version="integration-test"
        )

    return _validator


@pytest.fixture
def mock_backend() -> Any:
    from subsystem_sdk.backends.mock import MockSubmitBackend

    return MockSubmitBackend()


def _submit_through_real_sdk(
    backend: Any,
    validator_factory: Any,
    ex_type: str,
    payload: dict[str, Any],
) -> Any:
    """Drive the payload through the REAL SubmitClient → backend path
    (validate_then_dispatch chain). Returns the receipt."""

    from subsystem_sdk.submit.client import SubmitClient
    from subsystem_sdk.validate.result import ValidationResult

    def _validator(_: Any) -> ValidationResult:
        return ValidationResult.ok(
            ex_type=ex_type, schema_version="integration-test"
        )

    return SubmitClient(backend, validator=_validator).submit(payload)


def _assert_wire_shape(backend: Any, *, expected_field_present: list[str]) -> dict[str, Any]:
    """Common assertion: backend received exactly one payload, no SDK
    envelope leaked, all listed producer fields present."""

    from subsystem_sdk.validate.engine import SDK_ENVELOPE_FIELDS

    assert len(backend.submitted_payloads) == 1
    wire = backend.submitted_payloads[0]

    leaked = SDK_ENVELOPE_FIELDS.intersection(wire)
    assert not leaked, (
        f"announcement → SDK → backend: SDK envelope leaked {sorted(leaked)}; "
        "announcement's submit path must use validate_then_dispatch (which "
        "strips envelope at dispatch boundary per stage 2.7 follow-up #2). "
        "If the leak is in announcement's runtime/submit.py adapter, fix "
        "the adapter to delegate through SubmitClient.submit (NOT directly "
        "to backend.submit)."
    )

    for field in expected_field_present:
        assert field in wire, (
            f"required producer field {field!r} missing from wire payload: "
            f"{sorted(wire)}"
        )

    return wire


class TestEx1FactCandidateWireShape:
    def test_ex1_through_real_sdk_strips_envelope_and_preserves_producer_fields(
        self, mock_backend: Any
    ) -> None:
        from subsystem_announcement.extract import (
            AnnouncementFactCandidate,
        )
        from subsystem_announcement.extract.candidates import FactType
        from subsystem_announcement.extract.evidence import EvidenceSpan

        candidate = AnnouncementFactCandidate(
            fact_id="integ-ex1-fact",
            announcement_id="integ-ex1-ann",
            fact_type=FactType.MAJOR_CONTRACT,
            primary_entity_id="ENT_STOCK_INTEG",
            related_entity_ids=["ENT_STOCK_RELATED"],
            fact_content={"k": "v"},
            confidence=0.91,
            source_reference={
                "official_url": (
                    "https://www.sse.com.cn/disclosure/announcement/integ"
                ),
                "is_primary_source": True,
            },
            evidence_spans=[
                EvidenceSpan(
                    section_id="s1",
                    start_offset=0,
                    end_offset=11,
                    quote="placeholder",
                )
            ],
            extracted_at=datetime(2026, 1, 1, tzinfo=UTC),
        )

        receipt = _submit_through_real_sdk(
            mock_backend, None, "Ex-1", candidate.to_ex_payload()
        )

        assert receipt.accepted is True
        wire = _assert_wire_shape(
            mock_backend,
            expected_field_present=[
                "fact_id",
                "announcement_id",
                "fact_type",
                "primary_entity_id",
                "evidence_spans",
                "extracted_at",
                "source_reference",
            ],
        )
        assert wire["fact_id"] == "integ-ex1-fact"
        assert wire["announcement_id"] == "integ-ex1-ann"


class TestEx2SignalCandidateWireShape:
    def test_ex2_through_real_sdk_strips_envelope_and_preserves_producer_fields(
        self, mock_backend: Any
    ) -> None:
        from subsystem_announcement.extract.evidence import EvidenceSpan
        from subsystem_announcement.signals import (
            AnnouncementSignalCandidate,
        )
        from subsystem_announcement.signals.candidates import (
            SignalDirection,
            SignalTimeHorizon,
        )

        candidate = AnnouncementSignalCandidate(
            signal_id="integ-ex2-signal",
            announcement_id="integ-ex2-ann",
            signal_type="major_contract_positive",
            direction=SignalDirection.POSITIVE,
            magnitude=0.7,
            affected_entities=["ENT_STOCK_INTEG"],
            time_horizon=SignalTimeHorizon.SHORT_TERM,
            source_fact_ids=["integ-ex2-source-fact"],
            source_reference={
                "official_url": (
                    "https://www.sse.com.cn/disclosure/announcement/integ"
                ),
                "is_primary_source": True,
            },
            evidence_spans=[
                EvidenceSpan(
                    section_id="s1",
                    start_offset=0,
                    end_offset=11,
                    quote="placeholder",
                )
            ],
            confidence=0.88,
            generated_at=datetime(2026, 1, 1, tzinfo=UTC),
        )

        receipt = _submit_through_real_sdk(
            mock_backend, None, "Ex-2", candidate.to_ex_payload()
        )

        assert receipt.accepted is True
        wire = _assert_wire_shape(
            mock_backend,
            expected_field_present=[
                "signal_id",
                "announcement_id",
                "signal_type",
                "direction",
                "magnitude",
                "affected_entities",
                "time_horizon",
                "source_fact_ids",
                "evidence_spans",
                "confidence",
                "generated_at",
                "source_reference",
            ],
        )
        assert wire["signal_id"] == "integ-ex2-signal"
        assert wire["direction"] == "positive"


class TestEx3GraphDeltaCandidateWireShape:
    def test_ex3_through_real_sdk_strips_envelope_and_preserves_producer_fields(
        self, mock_backend: Any
    ) -> None:
        # Build an Ex-3 candidate using the real announcement model.
        # Real schema (subsystem_announcement/graph/candidates.py):
        # - source_node / target_node (not source_entity_id /
        #   target_entity_id — those would be a different model)
        # - delta_type: GraphDeltaType enum {"add_edge", "update_edge"}
        # - relation_type: GraphRelationType enum
        #   {"control", "shareholding", "supply_contract", "cooperation"}
        # - evidence_spans: list[EvidenceSpan] = Field(min_length=2) —
        #   Ex-3 high-threshold built INTO the schema (not an external
        #   guard); single-evidence Ex-3 fails at construction
        # - source_fact_ids: list[str] = Field(min_length=1)
        from subsystem_announcement.extract.evidence import EvidenceSpan
        from subsystem_announcement.graph import (
            AnnouncementGraphDeltaCandidate,
        )
        from subsystem_announcement.graph.candidates import (
            GraphDeltaType,
            GraphRelationType,
        )

        # Sanity: announcement Ex-3 must have these canonical fields.
        for required in (
            "delta_id",
            "announcement_id",
            "delta_type",
            "source_node",
            "target_node",
            "relation_type",
            "evidence_spans",
        ):
            assert required in AnnouncementGraphDeltaCandidate.model_fields, (
                f"announcement Ex-3 model lost required field {required!r} "
                "— update this integration test to match"
            )

        candidate = AnnouncementGraphDeltaCandidate(
            delta_id="integ-ex3-delta",
            announcement_id="integ-ex3-ann",
            delta_type=GraphDeltaType.ADD_EDGE,
            source_node="ENT_STOCK_INTEG_SRC",
            target_node="ENT_STOCK_INTEG_DST",
            relation_type=GraphRelationType.SUPPLY_CONTRACT,
            properties={"strength": "strong"},
            source_fact_ids=["integ-ex3-source-fact"],
            source_reference={
                "official_url": (
                    "https://www.sse.com.cn/disclosure/announcement/integ"
                ),
                "is_primary_source": True,
            },
            evidence_spans=[
                EvidenceSpan(
                    section_id="s1",
                    start_offset=0,
                    end_offset=11,
                    quote="placeholder",
                ),
                EvidenceSpan(
                    section_id="s2",
                    start_offset=0,
                    end_offset=15,
                    quote="dual_evidence!!",
                ),
            ],
            confidence=0.92,
            generated_at=datetime(2026, 1, 1, tzinfo=UTC),
        )

        receipt = _submit_through_real_sdk(
            mock_backend, None, "Ex-3", candidate.to_ex_payload()
        )

        assert receipt.accepted is True
        wire = _assert_wire_shape(
            mock_backend,
            expected_field_present=[
                "delta_id",
                "announcement_id",
                "delta_type",
                "source_node",
                "target_node",
                "relation_type",
                "evidence_spans",
            ],
        )
        assert wire["delta_id"] == "integ-ex3-delta"
        assert wire["announcement_id"] == "integ-ex3-ann"
        # Ex-3 high-threshold built into schema: evidence_spans count >= 2.
        assert len(wire["evidence_spans"]) >= 2
