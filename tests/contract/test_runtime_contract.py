"""Contract tier — announcement runtime public API signature stability.

Per CLAUDE.md §16: announcement's public surface is the
``subsystem_announcement.runtime`` module — submit_candidates,
replay_announcement, repair_parsed_artifact, compute_metrics_for_manifest,
plus the candidate models exposed by extract / signals / graph.

This tier locks the signature SHAPE so a future refactor can't silently
break consumers (subsystem-sdk integration tests + assembly e2e). Pure
unit-tier tests live in tests/test_runtime_*.py — they exercise BEHAVIOUR.
This tier is contract-shape-only.

Cross-repo alignment with ``contracts.schemas.ex_payloads`` is in the
sibling ``test_contracts_alignment.py`` (gated by ``importorskip``).
"""

from __future__ import annotations

import inspect

import subsystem_announcement
from subsystem_announcement.extract import AnnouncementFactCandidate
from subsystem_announcement.extract.candidates import (
    FORBIDDEN_PAYLOAD_KEYS,
    FactType,
)
from subsystem_announcement.extract.evidence import EvidenceSpan
from subsystem_announcement.graph import AnnouncementGraphDeltaCandidate
from subsystem_announcement.signals import AnnouncementSignalCandidate


class TestPackageVersion:
    def test_version_string_present_and_bumped(self) -> None:
        # Stage 2.8: 0.1.0 -> 0.1.1 baseline bump.
        assert isinstance(subsystem_announcement.__version__, str)
        assert subsystem_announcement.__version__ == "0.1.1"

    def test_package_name_constant(self) -> None:
        assert subsystem_announcement.PACKAGE_NAME == "subsystem-announcement"


class TestForbiddenPayloadKeys:
    """CLAUDE.md §5.4: ingest-metadata fields must not appear in producer
    payloads. The SDK already enforces this on the validate side; the
    announcement candidate models add their own forbidden-keys check
    (``_reject_forbidden_keys`` in extract/candidates.py).
    """

    def test_forbidden_payload_keys_includes_layer_b_metadata(self) -> None:
        for forbidden in (
            "submitted_at",
            "ingest_seq",
            "layer_b_receipt_id",
            "local_path",
        ):
            assert forbidden in FORBIDDEN_PAYLOAD_KEYS, (
                f"{forbidden!r} missing from FORBIDDEN_PAYLOAD_KEYS; "
                "this guard must reject Layer B / local-runtime fields"
            )


class TestEx1FactCandidateContract:
    def test_to_ex_payload_method_exists(self) -> None:
        assert hasattr(AnnouncementFactCandidate, "to_ex_payload")
        assert callable(AnnouncementFactCandidate.to_ex_payload)

    def test_required_fields_set(self) -> None:
        # CLAUDE.md §3 + §16: producer-owned required field set for Ex-1.
        # If any of these go missing, downstream consumers (signals,
        # graph delta extraction) break.
        required = {
            "fact_id",
            "announcement_id",
            "fact_type",
            "primary_entity_id",
            "evidence_spans",
            "extracted_at",
            "source_reference",
        }
        actual = set(AnnouncementFactCandidate.model_fields)
        missing = required - actual
        assert not missing, f"Ex-1 candidate lost required fields: {missing}"

    def test_ex_type_locked_to_ex1(self) -> None:
        # The ex_type field must default to "Ex-1" — it is the SDK
        # routing marker stripped at dispatch (per stage 2.7 follow-up
        # #2). Drift = wrong routing.
        ex_type_field = AnnouncementFactCandidate.model_fields["ex_type"]
        assert ex_type_field.default == "Ex-1"

    def test_extra_forbidden_at_construction(self) -> None:
        assert (
            AnnouncementFactCandidate.model_config.get("extra") == "forbid"
        )


class TestEx2SignalCandidateContract:
    def test_to_ex_payload_method_exists(self) -> None:
        assert hasattr(AnnouncementSignalCandidate, "to_ex_payload")
        assert callable(AnnouncementSignalCandidate.to_ex_payload)

    def test_required_fields_set(self) -> None:
        required = {
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
        }
        actual = set(AnnouncementSignalCandidate.model_fields)
        missing = required - actual
        assert not missing, f"Ex-2 candidate lost required fields: {missing}"

    def test_ex_type_locked_to_ex2(self) -> None:
        ex_type_field = AnnouncementSignalCandidate.model_fields["ex_type"]
        assert ex_type_field.default == "Ex-2"

    def test_extra_forbidden_at_construction(self) -> None:
        assert (
            AnnouncementSignalCandidate.model_config.get("extra") == "forbid"
        )


class TestEx3GraphDeltaCandidateContract:
    def test_to_ex_payload_method_exists(self) -> None:
        assert hasattr(AnnouncementGraphDeltaCandidate, "to_ex_payload")
        assert callable(AnnouncementGraphDeltaCandidate.to_ex_payload)

    def test_ex_type_locked_to_ex3(self) -> None:
        ex_type_field = AnnouncementGraphDeltaCandidate.model_fields[
            "ex_type"
        ]
        assert ex_type_field.default == "Ex-3"

    def test_extra_forbidden_at_construction(self) -> None:
        assert (
            AnnouncementGraphDeltaCandidate.model_config.get("extra")
            == "forbid"
        )


class TestEvidenceSpanContract:
    """CLAUDE.md §19: 100% Ex-1 coverage of EvidenceSpan."""

    def test_evidence_span_required_fields(self) -> None:
        required = {"section_id", "start_offset", "end_offset", "quote"}
        actual = set(EvidenceSpan.model_fields)
        missing = required - actual
        assert not missing

    def test_evidence_span_extra_forbidden(self) -> None:
        assert EvidenceSpan.model_config.get("extra") == "forbid"


class TestFactTypeEnum:
    """CLAUDE.md §-mapped fact taxonomy must stay stable; adding a new
    variant is fine but renaming/dropping breaks consumers."""

    def test_fact_type_enum_includes_canonical_set(self) -> None:
        canonical = {
            "EARNINGS_PREANNOUNCE",
            "MAJOR_CONTRACT",
            "SHAREHOLDER_CHANGE",
            "EQUITY_PLEDGE",
            "REGULATORY_ACTION",
            "TRADING_HALT_RESUME",
            "FUNDRAISING_CHANGE",
        }
        actual = {member.name for member in FactType}
        missing = canonical - actual
        assert not missing, (
            f"FactType lost canonical members: {missing}"
        )


class TestRuntimeSubmitContract:
    """``submit_candidates`` is the announcement → SDK boundary. Its
    signature must stay stable so the runtime pipeline can keep handing
    candidates over without a downstream refactor."""

    def test_submit_candidates_importable(self) -> None:
        from subsystem_announcement.runtime.submit import submit_candidates

        sig = inspect.signature(submit_candidates)
        assert "candidates" in sig.parameters or "submit_client" in sig.parameters
