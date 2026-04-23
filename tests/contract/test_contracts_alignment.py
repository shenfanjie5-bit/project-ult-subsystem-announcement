"""Cross-repo alignment: subsystem-announcement candidate models ↔
contracts.schemas Ex payload models.

CLAUDE.md (announcement + contracts both): Ex schemas are defined ONLY
in ``contracts``; announcement's local Ex-1/Ex-2/Ex-3 candidate models
must produce wire payloads that contracts accepts unchanged.

Module-level skip on missing dep — install [contracts-schemas] extra
to run this lane:

    pip install -e ".[dev,contracts-schemas]"
    pytest tests/contract/test_contracts_alignment.py

Two layers of cross-repo verification (codex stage 2.7+2.8 review trail):

**Layer 1 (PRODUCTION FIX VERIFIED — codex stage 2.8 follow-up #2):**
the production wire normalizer in ``runtime/submit.py:_normalize_for_sdk``
adds the SDK-required ``subsystem_id`` + ``produced_at`` fields. Tests
in this file invoke the REAL production normalizer
(``runtime.submit._validated_payload``) — NOT a test-side workaround.
These assertions are unconditional: any drift between announcement's
production output and the SDK-required field set is a P1.

**Layer 2 (REAL ROUND-TRIP through contracts v0.1.3 canonical schema —
codex stage 2.8 follow-up #3 closed the gap):** the cross-repo schema
gap that earlier xfail-strict tests documented (primary_entity_id↔
entity_id rename, evidence_spans not in canonical wire, generated_at
extra-rejected for Ex-2/3, SignalDirection vs Direction enum, Ex-1
missing canonical evidence slot) is now reconciled by:

  - ``contracts v0.1.3`` adds optional ``producer_context`` extension
    slot (Ex1/2/3) + optional ``Ex1.evidence`` + relaxes
    ``Ex2.affected_sectors`` list min_length=1 (field stays required;
    list-level constraint removed; element ``SectorId min_length=1``
    still applies).
  - announcement's ``runtime/submit.py:_normalize_for_sdk`` rewritten
    as a full canonical mapper that produces the contracts-valid wire
    shape directly (rename, enum mapping, evidence-ref serialization,
    generated_at→produced_at rename+drop for Ex-2/3, pack non-canonical
    fields into ``producer_context``).

Tests in the ``TestProductionWirePayloadPassesRealContractsValidation``
class assert the production normalizer's output ROUND TRIPS through
``contracts.Ex1/2/3.model_validate()`` end-to-end — no xfail, no
permissive validator bypass. Any drift in either the announcement
mapper or the contracts canonical wire shape now fails this lane
loudly.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

contracts_schemas = pytest.importorskip(
    "contracts.schemas",
    reason=(
        "contracts package not installed; install [contracts-schemas] "
        "extra to run cross-repo alignment tests"
    ),
)


# ── Layer 1: PRODUCTION NORMALIZER VERIFIED (unconditional) ─────────


class TestProductionNormalizerAddsSdkRequiredFields:
    """Stage 2.8 follow-up #2 (codex review #6 P1) production fix:
    ``runtime/submit.py:_normalize_for_sdk`` adds ``subsystem_id``
    (from MODULE_ID) + ``produced_at`` (from extracted_at / generated_at)
    to the wire payload. These are what subsystem-sdk's
    ``assert_producer_only`` requires for Ex-1/2/3.

    These assertions are unconditional — any drift in the production
    normalizer is a P1 (real production submit path would break).
    """

    def test_ex1_production_payload_includes_subsystem_id_and_produced_at(
        self,
    ) -> None:
        from subsystem_announcement.extract import (
            AnnouncementFactCandidate,
        )
        from subsystem_announcement.extract.candidates import FactType
        from subsystem_announcement.extract.evidence import EvidenceSpan
        from subsystem_announcement.runtime.submit import _validated_payload

        candidate = AnnouncementFactCandidate(
            fact_id="prod-norm-ex1",
            announcement_id="prod-norm-ann",
            fact_type=FactType.MAJOR_CONTRACT,
            primary_entity_id="ENT_STOCK_300750.SZ",
            related_entity_ids=["ENT_STOCK_PARTNER"],
            fact_content={"k": "v"},
            confidence=0.92,
            source_reference={
                "official_url": "https://www.sse.com.cn/disclosure/announcement/prod-norm",
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

        # REAL production normalizer.
        wire = _validated_payload(candidate)

        assert wire["subsystem_id"] == "subsystem-announcement", (
            "production _normalize_for_sdk lost subsystem_id; "
            "AnnouncementSubsystem.submit -> assert_producer_only would "
            "raise MissingProducerFieldError"
        )
        assert "produced_at" in wire, (
            "production _normalize_for_sdk lost produced_at; SDK assert_producer_only "
            "would raise MissingProducerFieldError"
        )
        # Ex-1: produced_at maps from extracted_at.
        assert wire["produced_at"] == wire["extracted_at"]

    def test_ex2_production_payload_includes_subsystem_id_and_produced_at(
        self,
    ) -> None:
        from subsystem_announcement.extract.evidence import EvidenceSpan
        from subsystem_announcement.runtime.submit import _validated_payload
        from subsystem_announcement.signals import (
            AnnouncementSignalCandidate,
        )
        from subsystem_announcement.signals.candidates import (
            SignalDirection,
            SignalTimeHorizon,
        )

        candidate = AnnouncementSignalCandidate(
            signal_id="prod-norm-ex2",
            announcement_id="prod-norm-ann",
            signal_type="major_contract_positive",
            direction=SignalDirection.POSITIVE,
            magnitude=0.7,
            affected_entities=["ENT_STOCK_300750.SZ"],
            time_horizon=SignalTimeHorizon.SHORT_TERM,
            source_fact_ids=["prod-norm-source-fact"],
            source_reference={
                "official_url": "https://www.sse.com.cn/disclosure/announcement/prod-norm",
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

        wire = _validated_payload(candidate)

        assert wire["subsystem_id"] == "subsystem-announcement"
        assert "produced_at" in wire
        # Stage 2.8 follow-up #3: Ex-2 generated_at is RENAMED to
        # produced_at and DROPPED from top-level (contracts.Ex2 has no
        # generated_at field; SDK strip doesn't cover it; leaving it
        # would be rejected as extra). produced_at now carries the
        # original generated_at value.
        assert "generated_at" not in wire, (
            f"Ex-2 wire payload must not contain top-level generated_at "
            f"(renamed to produced_at). Wire keys: {sorted(wire)}"
        )
        # produced_at value matches the source generated_at on the
        # original candidate. Pydantic mode="json" serializes datetimes
        # as ISO strings — compare by parsing back to a tz-aware datetime
        # rather than relying on a specific UTC suffix ("Z" vs "+00:00").
        from datetime import datetime as _dt

        produced_at_str = wire["produced_at"]
        assert isinstance(produced_at_str, str)
        produced_at_dt = _dt.fromisoformat(produced_at_str.replace("Z", "+00:00"))
        assert produced_at_dt == _dt(2026, 1, 1, tzinfo=UTC)

    def test_ex3_production_payload_includes_subsystem_id_and_produced_at(
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
        from subsystem_announcement.runtime.submit import _validated_payload

        candidate = AnnouncementGraphDeltaCandidate(
            delta_id="prod-norm-ex3",
            announcement_id="prod-norm-ann",
            delta_type=GraphDeltaType.ADD_EDGE,
            source_node="ENT_STOCK_INTEG_SRC",
            target_node="ENT_STOCK_INTEG_DST",
            relation_type=GraphRelationType.SUPPLY_CONTRACT,
            properties={"strength": "strong"},
            source_fact_ids=["prod-norm-source-fact"],
            source_reference={
                "official_url": "https://www.sse.com.cn/disclosure/announcement/prod-norm",
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

        wire = _validated_payload(candidate)

        assert wire["subsystem_id"] == "subsystem-announcement"
        assert "produced_at" in wire
        # Stage 2.8 follow-up #3: same as Ex-2 — generated_at renamed +
        # dropped from top-level.
        assert "generated_at" not in wire, (
            f"Ex-3 wire payload must not contain top-level generated_at "
            f"(renamed to produced_at). Wire keys: {sorted(wire)}"
        )
        # See Ex-2 test for the parse-instead-of-string-compare rationale.
        from datetime import datetime as _dt

        produced_at_str = wire["produced_at"]
        assert isinstance(produced_at_str, str)
        produced_at_dt = _dt.fromisoformat(produced_at_str.replace("Z", "+00:00"))
        assert produced_at_dt == _dt(2026, 1, 1, tzinfo=UTC)


class TestForbiddenPayloadKeysAlignedWithContracts:
    """Iron rule: announcement's FORBIDDEN_PAYLOAD_KEYS must be a
    superset of contracts' FORBIDDEN_INGEST_METADATA_FIELDS. If contracts
    expands the forbidden set, announcement's guard must keep up.
    """

    def test_announcement_forbidden_keys_is_superset_of_contracts(self) -> None:
        from contracts.schemas import FORBIDDEN_INGEST_METADATA_FIELDS

        from subsystem_announcement.extract.candidates import (
            FORBIDDEN_PAYLOAD_KEYS,
        )

        missing = set(FORBIDDEN_INGEST_METADATA_FIELDS) - set(
            FORBIDDEN_PAYLOAD_KEYS
        )
        assert not missing, (
            f"announcement FORBIDDEN_PAYLOAD_KEYS missing fields contracts "
            f"already forbids: {sorted(missing)}; announcement's guard would "
            "let these through to the contracts model where they'd be "
            "rejected — drift creates a confusing two-layer error"
        )


# ── Layer 2: REAL ROUND-TRIP through contracts v0.1.3 canonical schema ─
#
# Stage 2.8 follow-up #3 cross-repo reconciliation: the previous
# `TestKnownAnnouncementContractsSchemaGap` class held three xfail strict
# tests documenting that announcement's wire payload could not pass
# `contracts.Ex*.model_validate()`. That gap is now closed by:
#
#   - contracts v0.1.3: added optional `producer_context` (Ex1/2/3) +
#     optional `Ex1.evidence`; relaxed `Ex2.affected_sectors` list
#     min_length=1.
#   - subsystem-announcement `_normalize_for_sdk` rewrite: maps
#     announcement-local candidate fields to the canonical wire shape
#     (rename `primary_entity_id` -> `entity_id`; serialize
#     `evidence_spans` -> canonical `evidence` ref strings; map
#     `SignalDirection` -> `Direction`; lower delta_type/relation_type
#     enums; pack non-canonical provenance into `producer_context`).
#
# These positive Layer 2 tests assert that the production
# `_validated_payload` output is what `contracts.Ex*.model_validate()`
# accepts directly — no xfail, no permissive validator bypass.


class TestProductionWirePayloadPassesRealContractsValidation:
    """End-to-end round trip: announcement candidate -> production
    `_validated_payload` -> `contracts.schemas.Ex*.model_validate()`.

    The previous follow-up ran the wire payload through a permissive
    fake validator; this Layer 2 class hits the REAL contracts validator
    so any regression in either (a) the announcement normalizer or (b)
    contracts' canonical wire shape will fail loudly here.

    Per plan-review #4 P1 (top-level vs producer_context):
    - Ex-1 ``source_reference`` MUST stay at top-level.
    - Ex-2 / Ex-3 ``source_reference`` lives in ``producer_context``
      (contracts.Ex2/Ex3 have no canonical slot).
    - Ex-2 / Ex-3 ``generated_at`` MUST be renamed to ``produced_at`` and
      dropped from top-level (SDK doesn't strip ``generated_at``;
      contracts would reject it as extra).
    """

    def test_ex1_wire_round_trip_through_real_contracts(self) -> None:
        from contracts.schemas import Ex1CandidateFact

        from subsystem_announcement.extract import (
            AnnouncementFactCandidate,
        )
        from subsystem_announcement.extract.candidates import FactType
        from subsystem_announcement.extract.evidence import EvidenceSpan
        from subsystem_announcement.runtime.submit import _validated_payload

        candidate = AnnouncementFactCandidate(
            fact_id="follow-up-3-ex1",
            announcement_id="ANN-2026-FU3-001",
            fact_type=FactType.MAJOR_CONTRACT,
            primary_entity_id="ENT_STOCK_300750.SZ",
            related_entity_ids=["ENT_STOCK_002594.SZ"],
            fact_content={"contract_value_cny": 1_200_000_000},
            confidence=0.93,
            source_reference={
                "official_url": (
                    "https://www.sse.com.cn/disclosure/announcement/fu3"
                ),
            },
            evidence_spans=[
                EvidenceSpan(
                    section_id="s1",
                    start_offset=0,
                    end_offset=11,
                    quote="placeholder",
                ),
            ],
            extracted_at=datetime(2026, 1, 1, tzinfo=UTC),
        )

        wire = _validated_payload(candidate)
        # SDK envelope (ex_type, semantic, produced_at) is stripped by
        # subsystem-sdk before contracts validation in the real submit
        # path. Apply the same strip here so the assertion exactly
        # mirrors what contracts.Ex* sees.
        from subsystem_sdk.validate.engine import strip_sdk_envelope

        stripped_wire = dict(strip_sdk_envelope(wire))
        model = Ex1CandidateFact.model_validate(stripped_wire)

        # Top-level canonical fields, including the rename.
        assert model.subsystem_id == "subsystem-announcement"
        assert model.entity_id == candidate.primary_entity_id
        assert model.fact_id == candidate.fact_id
        assert model.fact_type == candidate.fact_type.value
        # Ex-1 source_reference MUST stay at top-level (contracts.Ex1
        # requires it). plan-review #4 P1.
        assert model.source_reference == candidate.source_reference
        # Canonical evidence refs are deterministic from announcement_id +
        # section_id + offsets.
        assert model.evidence == [
            f"{candidate.announcement_id}#{span.section_id}:"
            f"{span.start_offset}-{span.end_offset}"
            for span in candidate.evidence_spans
        ]
        # producer_context holds the announcement-local provenance.
        assert model.producer_context is not None
        assert model.producer_context["announcement_id"] == candidate.announcement_id
        assert model.producer_context["related_entity_ids"] == list(
            candidate.related_entity_ids
        )
        assert "evidence_spans_detail" in model.producer_context

    def test_ex2_wire_round_trip_through_real_contracts(self) -> None:
        from contracts.schemas import Ex2CandidateSignal

        from subsystem_announcement.extract.evidence import EvidenceSpan
        from subsystem_announcement.runtime.submit import _validated_payload
        from subsystem_announcement.signals import (
            AnnouncementSignalCandidate,
        )
        from subsystem_announcement.signals.candidates import (
            SignalDirection,
            SignalTimeHorizon,
        )

        candidate = AnnouncementSignalCandidate(
            signal_id="follow-up-3-ex2",
            announcement_id="ANN-2026-FU3-001",
            signal_type="major_contract_positive",
            direction=SignalDirection.POSITIVE,
            magnitude=0.72,
            affected_entities=["ENT_STOCK_300750.SZ"],
            time_horizon=SignalTimeHorizon.SHORT_TERM,
            source_fact_ids=["follow-up-3-ex1"],
            source_reference={
                "official_url": (
                    "https://www.sse.com.cn/disclosure/announcement/fu3"
                ),
            },
            evidence_spans=[
                EvidenceSpan(
                    section_id="s1",
                    start_offset=0,
                    end_offset=11,
                    quote="placeholder",
                ),
            ],
            confidence=0.88,
            generated_at=datetime(2026, 1, 1, tzinfo=UTC),
        )

        wire = _validated_payload(candidate)
        from subsystem_sdk.validate.engine import strip_sdk_envelope

        stripped_wire = dict(strip_sdk_envelope(wire))
        model = Ex2CandidateSignal.model_validate(stripped_wire)

        assert model.subsystem_id == "subsystem-announcement"
        assert model.signal_id == candidate.signal_id
        # SignalDirection.POSITIVE -> contracts.Direction.bullish (enum
        # mapping in _SIGNAL_DIRECTION_TO_CONTRACTS_DIRECTION).
        assert model.direction.value == "bullish"
        # contracts v0.1.3 allows empty affected_sectors; announcement
        # has no sector data so it emits []. graph-engine downstream
        # is responsible for sector enrichment.
        assert model.affected_sectors == []
        assert model.affected_entities == list(candidate.affected_entities)
        assert model.time_horizon == candidate.time_horizon.value
        # Canonical evidence refs derived from evidence_spans.
        assert model.evidence == [
            f"{candidate.announcement_id}#{span.section_id}:"
            f"{span.start_offset}-{span.end_offset}"
            for span in candidate.evidence_spans
        ]
        # Ex-2 source_reference goes into producer_context (Ex-2
        # contracts has no canonical slot for it).
        assert model.producer_context is not None
        assert model.producer_context["announcement_id"] == candidate.announcement_id
        assert model.producer_context["source_fact_ids"] == list(
            candidate.source_fact_ids
        )
        assert (
            model.producer_context["source_reference"]
            == candidate.source_reference
        )

        # No top-level generated_at on the wire (it was renamed to
        # produced_at). Same defense as the boundary deny-scan: missing
        # this would mean contracts extra='forbid' rejected the payload
        # and we'd never reach this assertion.
        assert "generated_at" not in wire

    def test_ex3_wire_round_trip_through_real_contracts(self) -> None:
        from contracts.schemas import Ex3CandidateGraphDelta

        from subsystem_announcement.extract.evidence import EvidenceSpan
        from subsystem_announcement.graph import (
            AnnouncementGraphDeltaCandidate,
        )
        from subsystem_announcement.graph.candidates import (
            GraphDeltaType,
            GraphRelationType,
        )
        from subsystem_announcement.runtime.submit import _validated_payload

        candidate = AnnouncementGraphDeltaCandidate(
            delta_id="follow-up-3-ex3",
            announcement_id="ANN-2026-FU3-001",
            delta_type=GraphDeltaType.ADD_EDGE,
            source_node="ENT_STOCK_INTEG_SRC",
            target_node="ENT_STOCK_INTEG_DST",
            relation_type=GraphRelationType.SUPPLY_CONTRACT,
            properties={"contract_value_cny": 1_200_000_000},
            source_fact_ids=["follow-up-3-ex1"],
            source_reference={
                "official_url": (
                    "https://www.sse.com.cn/disclosure/announcement/fu3"
                ),
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

        wire = _validated_payload(candidate)
        from subsystem_sdk.validate.engine import strip_sdk_envelope

        stripped_wire = dict(strip_sdk_envelope(wire))
        model = Ex3CandidateGraphDelta.model_validate(stripped_wire)

        assert model.subsystem_id == "subsystem-announcement"
        assert model.delta_id == candidate.delta_id
        # Enum lowered to canonical lowercase strings.
        assert model.delta_type == "add_edge"
        assert model.relation_type == "supply_contract"
        assert model.source_node == candidate.source_node
        assert model.target_node == candidate.target_node
        # Two evidence refs serialized from announcement's two
        # EvidenceSpans (Ex-3 announcement min_length=2 invariant).
        assert len(model.evidence) == 2
        assert model.evidence == [
            f"{candidate.announcement_id}#{span.section_id}:"
            f"{span.start_offset}-{span.end_offset}"
            for span in candidate.evidence_spans
        ]
        # Ex-3 source_reference + announcement-local confidence both go
        # into producer_context (contracts.Ex3 has no canonical slot for
        # either — confidence is announcement-side metadata).
        assert model.producer_context is not None
        assert model.producer_context["announcement_id"] == candidate.announcement_id
        assert model.producer_context["source_fact_ids"] == list(
            candidate.source_fact_ids
        )
        assert (
            model.producer_context["source_reference"]
            == candidate.source_reference
        )
        assert model.producer_context["confidence"] == candidate.confidence

        # Same generated_at-not-on-wire defense as Ex-2.
        assert "generated_at" not in wire
