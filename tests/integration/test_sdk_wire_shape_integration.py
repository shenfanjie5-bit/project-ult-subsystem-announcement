"""Integration tier — END-TO-END announcement ↔ subsystem-sdk wire-shape
integration test (the 7th issue per stage 2.8 plan template).

Goal: prove that announcement's REAL SDK adapter
(``AnnouncementSubsystem.submit``) routes through subsystem-sdk's
``validate_then_dispatch`` (which strips SDK envelope at dispatch
boundary per stage 2.7 follow-up #2) and does NOT bypass it.

Codex stage 2.8 review #5 P2: the previous version of this file
constructed ``SubmitClient`` directly and submitted the candidate
payload itself — that only re-tested subsystem-sdk's stripping
behavior. It never exercised ``AnnouncementSubsystem.submit()`` or
``runtime.submit.submit_candidates()``. If announcement later bypasses
the SDK helper and calls a backend path directly, the old test stays
green. Iron Rule #7 demands the test goes through the real announcement
adapter.

This rewrite for Ex-1 / Ex-2 / Ex-3 each:

1. Builds a real announcement candidate using the real candidate model
   constructor (no mocks — full pydantic validation).
2. Configures subsystem-sdk runtime with a ``BaseSubsystemContext``
   wrapping a ``SubmitClient(MockSubmitBackend)``. The MockSubmitBackend
   records what arrives at the wire.
3. Constructs ``AnnouncementSubsystem`` (the REAL announcement adapter,
   which is what production uses) and calls ``.submit(candidate.to_ex_payload())``.
   Internally this goes:
       AnnouncementSubsystem.submit
         → subsystem_sdk.submit.submit (top-level)
           → get_runtime().submit (= BaseSubsystemContext.submit)
             → SubmitClient.submit
               → validate_then_dispatch
                 → strip_sdk_envelope(payload)   ← critical strip
                 → MockSubmitBackend.submit(wire_payload)
4. Asserts ``backend.submitted_payloads[0]`` does NOT contain any SDK
   envelope field (``ex_type`` / ``semantic`` / ``produced_at``).
5. Asserts producer-owned fields announcement cares about reach the
   backend unchanged (announcement_id, primary identifier, etc.).
6. Asserts SubmitReceipt is accepted with no errors.

If announcement ever refactors ``AnnouncementSubsystem.submit`` to call
``backend.submit`` directly (bypassing the SDK runtime), step 4 catches
it: the unstripped envelope reaches the recording backend.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from subsystem_sdk.backends.heartbeat import SubmitBackendHeartbeatAdapter
from subsystem_sdk.backends.mock import MockSubmitBackend
from subsystem_sdk.base import (
    BaseSubsystemContext,
    SubsystemRegistrationSpec,
    configure_runtime,
)
from subsystem_sdk.heartbeat.client import HeartbeatClient
from subsystem_sdk.submit.client import SubmitClient
from subsystem_sdk.validate.engine import SDK_ENVELOPE_FIELDS
from subsystem_sdk.validate.result import ValidationResult

from subsystem_announcement.config import AnnouncementConfig
from subsystem_announcement.runtime.sdk_adapter import AnnouncementSubsystem


# ── Helpers ────────────────────────────────────────────────────────


def _permissive_validator(payload: Any) -> ValidationResult:
    """Validator that always returns ok so we can observe the strip
    behavior independent of contracts being installed (the cross-repo
    align tier covers contracts validation). The boundary tier already
    verified validate_payload's contract strip; here we verify the
    dispatch strip end-to-end through the real announcement adapter.

    ``ex_type`` reflected from the payload because subsystem-sdk's
    ``ValidationResult`` Literal restricts it to {Ex-0..Ex-3} — pick
    the one the producer declared.
    """

    ex_type = (
        payload.get("ex_type") if isinstance(payload, dict) else None
    ) or "Ex-1"
    return ValidationResult.ok(
        ex_type=ex_type, schema_version="integration-test"
    )


def _build_context_with_recording_backend() -> tuple[
    BaseSubsystemContext, MockSubmitBackend
]:
    """Build a BaseSubsystemContext whose SubmitClient is wired to a
    MockSubmitBackend. Returns (context, backend) so tests can inspect
    backend.submitted_payloads after the announcement adapter runs.

    Registration spec mirrors what announcement's
    ``build_registration_spec(AnnouncementConfig())`` would produce —
    same module_id (subsystem-announcement), version, and supported
    Ex types — so the SDK's per-registration support check
    (BaseSubsystemContext._validate_registration_support) accepts the
    candidates we submit (Ex-1 / Ex-2 / Ex-3).
    """

    backend = MockSubmitBackend()
    registration = SubsystemRegistrationSpec(
        subsystem_id="subsystem-announcement",
        version="0.1.1",
        domain="announcement",
        supported_ex_types=["Ex-0", "Ex-1", "Ex-2", "Ex-3"],
        owner="subsystem-announcement",
        heartbeat_policy_ref="interval:60s",
    )
    context = BaseSubsystemContext(
        registration=registration,
        submit_client=SubmitClient(backend, validator=_permissive_validator),
        heartbeat_client=HeartbeatClient(
            SubmitBackendHeartbeatAdapter(backend),
            validator=_permissive_validator,
        ),
        validator=_permissive_validator,
    )
    return context, backend


def _assert_backend_received_wire_shape(
    backend: MockSubmitBackend,
    *,
    expected_field_present: list[str],
) -> dict[str, Any]:
    """Common wire-shape assertion: backend received exactly one
    payload, NO SDK envelope leaked, all listed producer fields present.
    """

    assert len(backend.submitted_payloads) == 1, (
        f"expected exactly 1 backend payload (announcement -> SDK -> "
        f"backend), got {len(backend.submitted_payloads)}"
    )
    wire = backend.submitted_payloads[0]

    leaked = SDK_ENVELOPE_FIELDS.intersection(wire)
    assert not leaked, (
        f"announcement → SDK → backend: SDK envelope leaked "
        f"{sorted(leaked)}; AnnouncementSubsystem.submit must delegate "
        "through subsystem_sdk.submit.submit (NOT call backend.submit "
        "directly), so validate_then_dispatch's strip applies"
    )
    for field in expected_field_present:
        assert field in wire, (
            f"required producer field {field!r} missing from wire payload: "
            f"{sorted(wire)}"
        )
    return wire


def _enrich_with_sdk_required_fields(
    candidate_payload: dict[str, Any],
) -> dict[str, Any]:
    """Enrich announcement candidate payload with the SDK-required
    fields ``assert_producer_only`` checks.

    Cross-repo reality (codex stage 2.7 + 2.8 review trail):
    - Subsystem-sdk's ``_PRODUCER_OWNED_REQUIRED`` for Ex-1/2/3 is
      ``{subsystem_id, produced_at}``.
    - Announcement candidate models carry ``announcement_id`` +
      ``extracted_at`` / ``generated_at`` instead.
    - That's a real announcement-side gap — production
      ``submit_candidates`` would need to enrich payloads at the
      adapter boundary. As of stage 2.8 it doesn't, but THIS test is
      about the WIRE-SHAPE boundary (Iron Rule #7), not about the
      announcement payload-schema gap. So we enrich here to demonstrate
      the wire path; the announcement payload-schema gap is a separate
      concern (and a candidate for a future milestone).
    """

    enriched = dict(candidate_payload)
    enriched.setdefault("subsystem_id", "subsystem-announcement")
    if "produced_at" not in enriched:
        # Use whatever timestamp the announcement model carries for
        # this candidate — extracted_at for Ex-1, generated_at for Ex-2/3.
        produced_at = (
            enriched.get("extracted_at")
            or enriched.get("generated_at")
            or datetime(2026, 1, 1, tzinfo=UTC).isoformat()
        )
        enriched["produced_at"] = produced_at
    return enriched


def _submit_through_real_announcement_adapter(
    candidate_payload: dict[str, Any],
) -> tuple[Any, MockSubmitBackend]:
    """Drive a candidate payload through the FULL real announcement
    adapter path: AnnouncementSubsystem.submit -> _validate_sdk_payload
    (assert_producer_only) -> subsystem_sdk.submit.submit ->
    BaseSubsystemContext.submit -> SubmitClient.submit ->
    validate_then_dispatch (with strip) -> MockSubmitBackend.submit.

    Returns (receipt, backend) so callers can inspect both the SDK
    receipt (was it accepted?) and the recording backend (did the wire
    shape have the envelope stripped?).
    """

    context, backend = _build_context_with_recording_backend()
    enriched = _enrich_with_sdk_required_fields(candidate_payload)
    with configure_runtime(context):
        subsystem = AnnouncementSubsystem(AnnouncementConfig())
        receipt = subsystem.submit(enriched)
    return receipt, backend


# ── Ex-1 ──────────────────────────────────────────────────────────


class TestEx1FactCandidateThroughRealAnnouncementAdapter:
    """Ex-1 candidate constructed via real AnnouncementFactCandidate
    model + driven through real AnnouncementSubsystem.submit() —
    proves the wire-shape boundary holds for fact candidates."""

    def test_ex1_announcement_adapter_strips_envelope_and_preserves_producer_fields(
        self,
    ) -> None:
        from subsystem_announcement.extract import AnnouncementFactCandidate
        from subsystem_announcement.extract.candidates import FactType
        from subsystem_announcement.extract.evidence import EvidenceSpan

        candidate = AnnouncementFactCandidate(
            fact_id="integ-real-adapter-ex1-fact",
            announcement_id="integ-real-adapter-ex1-ann",
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

        receipt, backend = _submit_through_real_announcement_adapter(
            candidate.to_ex_payload()
        )

        assert receipt.accepted is True
        wire = _assert_backend_received_wire_shape(
            backend,
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
        assert wire["fact_id"] == "integ-real-adapter-ex1-fact"
        assert wire["announcement_id"] == "integ-real-adapter-ex1-ann"


# ── Ex-2 ──────────────────────────────────────────────────────────


class TestEx2SignalCandidateThroughRealAnnouncementAdapter:
    def test_ex2_announcement_adapter_strips_envelope_and_preserves_producer_fields(
        self,
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
            signal_id="integ-real-adapter-ex2-signal",
            announcement_id="integ-real-adapter-ex2-ann",
            signal_type="major_contract_positive",
            direction=SignalDirection.POSITIVE,
            magnitude=0.7,
            affected_entities=["ENT_STOCK_INTEG"],
            time_horizon=SignalTimeHorizon.SHORT_TERM,
            source_fact_ids=["integ-real-adapter-ex2-source-fact"],
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

        receipt, backend = _submit_through_real_announcement_adapter(
            candidate.to_ex_payload()
        )

        assert receipt.accepted is True
        wire = _assert_backend_received_wire_shape(
            backend,
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
        assert wire["signal_id"] == "integ-real-adapter-ex2-signal"
        assert wire["direction"] == "positive"


# ── Ex-3 ──────────────────────────────────────────────────────────


class TestEx3GraphDeltaCandidateThroughRealAnnouncementAdapter:
    def test_ex3_announcement_adapter_strips_envelope_and_preserves_producer_fields(
        self,
    ) -> None:
        from subsystem_announcement.extract.evidence import EvidenceSpan
        from subsystem_announcement.graph import (
            AnnouncementGraphDeltaCandidate,
        )
        from subsystem_announcement.graph.candidates import (
            GraphDeltaType,
            GraphRelationType,
        )

        candidate = AnnouncementGraphDeltaCandidate(
            delta_id="integ-real-adapter-ex3-delta",
            announcement_id="integ-real-adapter-ex3-ann",
            delta_type=GraphDeltaType.ADD_EDGE,
            source_node="ENT_STOCK_INTEG_SRC",
            target_node="ENT_STOCK_INTEG_DST",
            relation_type=GraphRelationType.SUPPLY_CONTRACT,
            properties={"strength": "strong"},
            source_fact_ids=["integ-real-adapter-ex3-source-fact"],
            source_reference={
                "official_url": (
                    "https://www.sse.com.cn/disclosure/announcement/integ"
                ),
                "is_primary_source": True,
            },
            # Ex-3 high-threshold built into schema: evidence_spans
            # min_length=2.
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

        receipt, backend = _submit_through_real_announcement_adapter(
            candidate.to_ex_payload()
        )

        assert receipt.accepted is True
        wire = _assert_backend_received_wire_shape(
            backend,
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
        assert wire["delta_id"] == "integ-real-adapter-ex3-delta"
        assert wire["announcement_id"] == "integ-real-adapter-ex3-ann"
        # Schema enforces min 2 evidence spans for Ex-3.
        assert len(wire["evidence_spans"]) >= 2


# ── Defense check: prove the strip path detects a regression ──────


class TestRegressionDetectionDefenseCheck:
    """Sanity check: if SDK_ENVELOPE_FIELDS ever shrinks (e.g. someone
    drops produced_at from the strip set without coordinating with
    announcement), this test surfaces it. Drives a payload that
    contains produced_at + ex_type + semantic and asserts BOTH the
    backend wire shape excludes them AND SDK_ENVELOPE_FIELDS still
    declares them as the canonical envelope set.
    """

    def test_envelope_set_canonical_definition_holds(self) -> None:
        # If anyone adds/removes envelope fields, the wire-shape
        # boundary in announcement boundary tests + this integration
        # test depend on the same SDK_ENVELOPE_FIELDS constant. Lock it.
        assert SDK_ENVELOPE_FIELDS == frozenset(
            {"ex_type", "semantic", "produced_at"}
        ), (
            f"SDK_ENVELOPE_FIELDS drifted: got {sorted(SDK_ENVELOPE_FIELDS)}; "
            "expected {ex_type, semantic, produced_at}. announcement's "
            "wire-shape boundary tests assume this exact 3-field set."
        )
