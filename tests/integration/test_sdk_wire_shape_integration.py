"""Integration tier — END-TO-END announcement ↔ subsystem-sdk wire-shape
integration test (the 7th issue per stage 2.8 plan template).

Goal: prove that announcement's REAL SDK adapter
(``AnnouncementSubsystem.submit``) routes through subsystem-sdk's
``validate_then_dispatch`` (which strips SDK envelope at dispatch
boundary per stage 2.7 follow-up #2) and does NOT bypass it.

Stage 2.8 follow-up #3 cross-repo reconciliation:
The previous version of this file injected a permissive validator into
SubmitClient/HeartbeatClient so the wire payload bypassed real
``contracts.Ex*.model_validate()``. That hid the canonical-shape gap
codex review #7 found. After contracts v0.1.3 added
``producer_context`` + ``Ex1.evidence`` + relaxed
``Ex2.affected_sectors`` AND announcement's ``_normalize_for_sdk`` was
rewritten as a full canonical mapper, the test can now use the SDK's
default ``validate_payload`` (real ``contracts.Ex*.model_validate``).
That makes this test catch any regression in either
(a) the announcement normalizer or (b) contracts' canonical wire shape.

For each Ex-1 / Ex-2 / Ex-3 we:

1. Build a real announcement candidate using the real candidate model
   constructor (no mocks — full pydantic validation).
2. Configure subsystem-sdk runtime with a ``BaseSubsystemContext``
   wrapping a ``SubmitClient(MockSubmitBackend)`` — using the SDK's
   default validator (NO permissive bypass).
3. Construct ``AnnouncementSubsystem`` (the REAL announcement adapter)
   and call ``.submit(_validated_payload(candidate))``. Internally:
       AnnouncementSubsystem.submit
         → subsystem_sdk.submit.submit (top-level)
           → get_runtime().submit (= BaseSubsystemContext.submit)
             → SubmitClient.submit
               → validate_then_dispatch
                 → validate_payload (REAL contracts validation)
                 → strip_sdk_envelope(payload)   ← critical strip
                 → MockSubmitBackend.submit(wire_payload)
4. Assert ``backend.submitted_payloads[0]`` does NOT contain any SDK
   envelope field (``ex_type`` / ``semantic`` / ``produced_at``).
5. Assert producer-owned canonical fields reach the backend (entity_id
   for Ex-1, direction for Ex-2, source_node/target_node for Ex-3,
   producer_context for all 3).
6. Assert the wire payload itself round-trips through real
   ``contracts.Ex*.model_validate()`` — defense in depth: if SDK strip
   ever drops too many fields, contracts will reject, this test fails.
7. Assert SubmitReceipt is accepted with no errors.

If announcement ever refactors ``AnnouncementSubsystem.submit`` to call
``backend.submit`` directly (bypassing the SDK runtime), step 4 catches
it: the unstripped envelope reaches the recording backend.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from typing import Any

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

from subsystem_announcement.config import AnnouncementConfig
from subsystem_announcement.runtime.sdk_adapter import AnnouncementSubsystem


# ── Helpers ────────────────────────────────────────────────────────


def _build_context_with_recording_backend(
    *,
    entity_lookup: Any | None = None,
    preflight_policy: str = "skip",
) -> tuple[
    BaseSubsystemContext, MockSubmitBackend
]:
    """Build a BaseSubsystemContext whose SubmitClient is wired to a
    MockSubmitBackend.

    Stage 2.8 follow-up #3: the SubmitClient + HeartbeatClient use the
    SDK's DEFAULT validator (real ``validate_payload`` against
    ``contracts.Ex*.model_validate``). No permissive validator bypass.
    Returns (context, backend) so tests can inspect
    ``backend.submitted_payloads`` after the announcement adapter runs.

    Registration spec mirrors what announcement's
    ``build_registration_spec(AnnouncementConfig())`` produces so the
    SDK's per-registration support check accepts the candidates we
    submit (Ex-0 + Ex-1 + Ex-2 + Ex-3).
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
    # No validator= override — uses subsystem_sdk.validate.engine.validate_payload
    # by default, which calls contracts.Ex*.model_validate on the
    # canonical wire payload (after SDK envelope strip).
    context = BaseSubsystemContext(
        registration=registration,
        submit_client=SubmitClient(
            backend,
            entity_lookup=entity_lookup,
            preflight_policy=preflight_policy,
        ),
        heartbeat_client=HeartbeatClient(SubmitBackendHeartbeatAdapter(backend)),
    )
    return context, backend


class RecordingLookup:
    def __init__(self, resolved_refs: Iterable[str] = ()) -> None:
        self._resolved_refs = set(resolved_refs)
        self.calls: list[tuple[str, ...]] = []

    def lookup(self, refs: Iterable[str]) -> Mapping[str, bool]:
        refs_tuple = tuple(refs)
        self.calls.append(refs_tuple)
        return {ref: ref in self._resolved_refs for ref in refs_tuple}


def _assert_backend_received_canonical_wire(
    backend: MockSubmitBackend,
    *,
    expected_top_level_fields: list[str],
    expected_producer_context_keys: list[str],
) -> dict[str, Any]:
    """Common wire-shape assertion: backend received exactly one
    payload, NO SDK envelope leaked, all listed canonical top-level
    fields present, and producer_context contains the expected keys.
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
    for field in expected_top_level_fields:
        assert field in wire, (
            f"required canonical top-level field {field!r} missing from "
            f"wire payload: {sorted(wire)}"
        )
    producer_context = wire.get("producer_context") or {}
    assert isinstance(producer_context, dict), (
        f"producer_context must be a dict, got {type(producer_context)!r}"
    )
    for key in expected_producer_context_keys:
        assert key in producer_context, (
            f"required producer_context key {key!r} missing: "
            f"{sorted(producer_context)}"
        )
    return wire


def _submit_candidate_through_real_announcement_pipeline(
    candidate: Any,
) -> tuple[Any, MockSubmitBackend]:
    """Drive a candidate through the FULL real announcement pipeline:
    1. ``runtime.submit._validated_payload(candidate)`` — production
       canonical mapper (re-validates the model AND maps announcement-
       local fields to the contracts.Ex* canonical wire shape via
       ``_normalize_for_sdk``).
    2. ``AnnouncementSubsystem.submit(wire_payload)`` — production
       SDK adapter.
    3. ``subsystem_sdk.submit.submit(sdk_payload)`` (top-level) →
       ``BaseSubsystemContext.submit`` → ``SubmitClient.submit`` →
       ``validate_then_dispatch`` → REAL ``validate_payload`` →
       ``strip_sdk_envelope`` → ``MockSubmitBackend.submit(wire)``.
    """

    from subsystem_announcement.runtime.submit import _validated_payload

    context, backend = _build_context_with_recording_backend()
    wire_payload = _validated_payload(candidate)
    with configure_runtime(context):
        subsystem = AnnouncementSubsystem(AnnouncementConfig())
        receipt = subsystem.submit(wire_payload)
    return receipt, backend


# ── Ex-0 ──────────────────────────────────────────────────────────


class TestHeartbeatThroughRealAnnouncementAdapter:
    def test_heartbeat_uses_sdk_status_boundary_and_reaches_backend(self) -> None:
        from contracts.schemas import Ex0Metadata

        context, backend = _build_context_with_recording_backend()

        with configure_runtime(context):
            subsystem = AnnouncementSubsystem(AnnouncementConfig())
            heartbeat = subsystem.on_heartbeat()

        assert heartbeat.status == "ok"
        assert len(backend.submitted_payloads) == 1
        wire = backend.submitted_payloads[0]
        leaked = SDK_ENVELOPE_FIELDS.intersection(wire)
        assert not leaked, (
            f"announcement heartbeat leaked SDK envelope fields {sorted(leaked)}"
        )
        assert wire["subsystem_id"] == "subsystem-announcement"
        assert wire["version"] == "0.1.1"
        assert wire["status"] == "ok"
        assert wire["pending_count"] == 0

        Ex0Metadata.model_validate(wire)


# ── Ex-1 ──────────────────────────────────────────────────────────


class TestEx1FactCandidateThroughRealAnnouncementAdapter:
    """Ex-1 candidate constructed via real AnnouncementFactCandidate
    model + driven through real AnnouncementSubsystem.submit() —
    proves the wire-shape boundary holds AND the canonical mapper
    output passes real ``contracts.Ex1CandidateFact.model_validate``."""

    def test_ex1_announcement_adapter_strips_envelope_and_maps_to_canonical_shape(
        self,
    ) -> None:
        from contracts.schemas import Ex1CandidateFact

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

        receipt, backend = _submit_candidate_through_real_announcement_pipeline(
            candidate
        )

        assert receipt.accepted is True, (
            f"announcement → SDK → backend should produce accepted receipt; "
            f"got errors={list(receipt.errors)}"
        )
        wire = _assert_backend_received_canonical_wire(
            backend,
            expected_top_level_fields=[
                "subsystem_id",
                "fact_id",
                "entity_id",  # renamed from primary_entity_id
                "fact_type",
                "fact_content",
                "confidence",
                "source_reference",  # Ex-1 keeps it top-level
                "extracted_at",
                "evidence",
            ],
            expected_producer_context_keys=[
                "announcement_id",
                "related_entity_ids",
                "evidence_spans_detail",
            ],
        )
        # Canonical rename: primary_entity_id → entity_id.
        assert wire["entity_id"] == "ENT_STOCK_INTEG"
        # Defense in depth: round-trip the wire through real contracts.
        Ex1CandidateFact.model_validate(wire)

    def test_ex1_announcement_adapter_honors_sdk_block_preflight_before_backend(
        self,
    ) -> None:
        from subsystem_announcement.extract import AnnouncementFactCandidate
        from subsystem_announcement.extract.candidates import FactType
        from subsystem_announcement.extract.evidence import EvidenceSpan
        from subsystem_announcement.runtime.submit import _validated_payload

        candidate = AnnouncementFactCandidate(
            fact_id="integ-preflight-ex1-fact",
            announcement_id="integ-preflight-ex1-ann",
            fact_type=FactType.MAJOR_CONTRACT,
            primary_entity_id="ENT_STOCK_PREFLIGHT",
            related_entity_ids=[],
            fact_content={"k": "v"},
            confidence=0.91,
            source_reference={
                "official_url": (
                    "https://www.sse.com.cn/disclosure/announcement/preflight"
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
        lookup = RecordingLookup()
        context, backend = _build_context_with_recording_backend(
            entity_lookup=lookup,
            preflight_policy="block",
        )

        with configure_runtime(context):
            receipt = AnnouncementSubsystem(AnnouncementConfig()).submit(
                _validated_payload(candidate)
            )

        assert lookup.calls == [("ENT_STOCK_PREFLIGHT",)]
        assert receipt.accepted is False
        assert receipt.errors == (
            "entity preflight blocked unresolved reference(s): ENT_STOCK_PREFLIGHT",
        )
        assert backend.submitted_payloads == ()


# ── Ex-2 ──────────────────────────────────────────────────────────


class TestEx2SignalCandidateThroughRealAnnouncementAdapter:
    def test_ex2_announcement_adapter_strips_envelope_and_maps_to_canonical_shape(
        self,
    ) -> None:
        from contracts.schemas import Ex2CandidateSignal

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

        receipt, backend = _submit_candidate_through_real_announcement_pipeline(
            candidate
        )

        assert receipt.accepted is True, (
            f"got errors={list(receipt.errors)}"
        )
        wire = _assert_backend_received_canonical_wire(
            backend,
            expected_top_level_fields=[
                "subsystem_id",
                "signal_id",
                "signal_type",
                "direction",
                "magnitude",
                "affected_entities",
                "affected_sectors",  # contracts v0.1.3: empty list valid
                "time_horizon",
                "evidence",
                "confidence",
            ],
            expected_producer_context_keys=[
                "announcement_id",
                "source_fact_ids",
                "source_reference",
                "evidence_spans_detail",
            ],
        )
        # SignalDirection.POSITIVE → contracts.Direction.bullish.
        assert wire["direction"] == "bullish"
        # contracts v0.1.3 allows empty affected_sectors; announcement
        # has no sector data so it emits [].
        assert wire["affected_sectors"] == []
        # generated_at MUST NOT leak to top-level (renamed to produced_at).
        assert "generated_at" not in wire, (
            f"Ex-2 wire payload must not contain top-level generated_at "
            f"(SDK strip doesn't cover it; contracts.Ex2 would reject as "
            f"extra). Wire keys: {sorted(wire)}"
        )
        # Defense in depth: real contracts validation.
        Ex2CandidateSignal.model_validate(wire)

    def test_ex2_announcement_adapter_honors_sdk_block_preflight_before_backend(
        self,
    ) -> None:
        from subsystem_announcement.extract.evidence import EvidenceSpan
        from subsystem_announcement.runtime.submit import _validated_payload
        from subsystem_announcement.signals import AnnouncementSignalCandidate
        from subsystem_announcement.signals.candidates import (
            SignalDirection,
            SignalTimeHorizon,
        )

        candidate = AnnouncementSignalCandidate(
            signal_id="integ-preflight-ex2-signal",
            announcement_id="integ-preflight-ex2-ann",
            signal_type="major_contract_positive",
            direction=SignalDirection.POSITIVE,
            magnitude=0.7,
            affected_entities=["ENT_STOCK_UNRESOLVED"],
            time_horizon=SignalTimeHorizon.SHORT_TERM,
            source_fact_ids=["integ-preflight-ex2-source-fact"],
            source_reference={
                "official_url": (
                    "https://www.sse.com.cn/disclosure/announcement/preflight"
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
        lookup = RecordingLookup()
        context, backend = _build_context_with_recording_backend(
            entity_lookup=lookup,
            preflight_policy="block",
        )

        with configure_runtime(context):
            receipt = AnnouncementSubsystem(AnnouncementConfig()).submit(
                _validated_payload(candidate)
            )

        assert lookup.calls == [("ENT_STOCK_UNRESOLVED",)]
        assert receipt.accepted is False
        assert receipt.errors == (
            "entity preflight blocked unresolved reference(s): ENT_STOCK_UNRESOLVED",
        )
        assert backend.submitted_payloads == ()


# ── Ex-3 ──────────────────────────────────────────────────────────


class TestEx3GraphDeltaCandidateThroughRealAnnouncementAdapter:
    def test_ex3_announcement_adapter_strips_envelope_and_maps_to_canonical_shape(
        self,
    ) -> None:
        from contracts.schemas import Ex3CandidateGraphDelta

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

        receipt, backend = _submit_candidate_through_real_announcement_pipeline(
            candidate
        )

        assert receipt.accepted is True, (
            f"got errors={list(receipt.errors)}"
        )
        wire = _assert_backend_received_canonical_wire(
            backend,
            expected_top_level_fields=[
                "subsystem_id",
                "delta_id",
                "delta_type",
                "source_node",
                "target_node",
                "relation_type",
                "properties",
                "evidence",
            ],
            expected_producer_context_keys=[
                "announcement_id",
                "source_fact_ids",
                "source_reference",
                "evidence_spans_detail",
                "confidence",  # Ex-3 contracts has no canonical confidence
            ],
        )
        # Enums lowered to canonical lowercase strings.
        assert wire["delta_type"] == "add_edge"
        assert wire["relation_type"] == "supply_contract"
        # Schema enforces min 2 evidence refs for Ex-3.
        assert len(wire["evidence"]) >= 2
        # generated_at MUST NOT leak to top-level (renamed to produced_at).
        assert "generated_at" not in wire
        # Defense in depth: real contracts validation.
        Ex3CandidateGraphDelta.model_validate(wire)

    def test_ex3_announcement_adapter_honors_sdk_block_preflight_before_backend(
        self,
    ) -> None:
        from subsystem_announcement.extract.evidence import EvidenceSpan
        from subsystem_announcement.graph import AnnouncementGraphDeltaCandidate
        from subsystem_announcement.graph.candidates import (
            GraphDeltaType,
            GraphRelationType,
        )
        from subsystem_announcement.runtime.submit import _validated_payload

        candidate = AnnouncementGraphDeltaCandidate(
            delta_id="integ-preflight-ex3-delta",
            announcement_id="integ-preflight-ex3-ann",
            delta_type=GraphDeltaType.ADD_EDGE,
            source_node="ENT_STOCK_RESOLVED_SRC",
            target_node="ENT_STOCK_UNRESOLVED_DST",
            relation_type=GraphRelationType.SUPPLY_CONTRACT,
            properties={"strength": "strong"},
            source_fact_ids=["integ-preflight-ex3-source-fact"],
            source_reference={
                "official_url": (
                    "https://www.sse.com.cn/disclosure/announcement/preflight"
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
        lookup = RecordingLookup(resolved_refs={"ENT_STOCK_RESOLVED_SRC"})
        context, backend = _build_context_with_recording_backend(
            entity_lookup=lookup,
            preflight_policy="block",
        )

        with configure_runtime(context):
            receipt = AnnouncementSubsystem(AnnouncementConfig()).submit(
                _validated_payload(candidate)
            )

        assert lookup.calls == [
            ("ENT_STOCK_RESOLVED_SRC", "ENT_STOCK_UNRESOLVED_DST")
        ]
        assert receipt.accepted is False
        assert receipt.errors == (
            "entity preflight blocked unresolved reference(s): "
            "ENT_STOCK_UNRESOLVED_DST",
        )
        assert backend.submitted_payloads == ()


# ── Defense check: prove the strip path detects a regression ──────


class TestRegressionDetectionDefenseCheck:
    """Sanity check: if SDK_ENVELOPE_FIELDS ever shrinks (e.g. someone
    drops produced_at from the strip set without coordinating with
    announcement), this test surfaces it. The wire-shape boundary in
    announcement boundary tests + this integration test depend on the
    same SDK_ENVELOPE_FIELDS constant — lock it.
    """

    def test_envelope_set_canonical_definition_holds(self) -> None:
        assert SDK_ENVELOPE_FIELDS == frozenset(
            {"ex_type", "semantic", "produced_at"}
        ), (
            f"SDK_ENVELOPE_FIELDS drifted: got {sorted(SDK_ENVELOPE_FIELDS)}; "
            "expected {ex_type, semantic, produced_at}. announcement's "
            "wire-shape boundary tests assume this exact 3-field set."
        )
