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

**Layer 2 (KNOWN SCHEMA GAP — DOCUMENTED, NOT YET FIXED):** announcement
candidate models declare fields that ``contracts.schemas.Ex*CandidateFact /
Signal / GraphDelta`` doesn't accept (announcement_id, evidence_spans,
related_entity_ids, primary_entity_id-vs-entity_id rename, etc.). This
is a deeper cross-repo schema decision (either announcement renames /
drops local fields, or contracts.Ex* extends to accept them). Tests
in the ``TestKnownAnnouncementContractsSchemaGap`` class are marked
``xfail strict=True`` — when a future cross-repo migration closes the
gap, those tests start passing and yell at us to remove the xfail (and
the announcement-side workarounds that document the gap today).
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
            primary_entity_id="ENT_STOCK_300750_SZ",
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
            affected_entities=["ENT_STOCK_300750_SZ"],
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
        # Ex-2: produced_at maps from generated_at.
        assert wire["produced_at"] == wire["generated_at"]

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
        # Ex-3: produced_at maps from generated_at.
        assert wire["produced_at"] == wire["generated_at"]


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


# ── Layer 2: KNOWN SCHEMA GAP (xfail strict, document the gap) ─────


class TestKnownAnnouncementContractsSchemaGap:
    """Document the cross-repo schema mismatch between announcement
    candidate models and contracts.schemas.Ex*. These tests are
    ``xfail strict=True`` — when a future cross-repo migration closes
    the gap, they start passing and signal it's time to remove the
    xfail and the workarounds in announcement.

    The gap (codex stage 2.8 review #6 P1 documented this honestly):

    Announcement candidate models declare fields contracts.Ex* doesn't:
    - announcement.AnnouncementFactCandidate has ``primary_entity_id`` —
      contracts.Ex1CandidateFact has ``entity_id`` (rename gap)
    - announcement has ``announcement_id`` — contracts has no
      announcement_id field (drop or extend gap)
    - announcement has ``evidence_spans: list[EvidenceSpan]`` —
      contracts.Ex1CandidateFact has no evidence field at all
    - announcement has ``related_entity_ids`` — contracts has no
      analog
    - announcement.AnnouncementSignalCandidate's SignalDirection is
      ``{positive,negative,neutral}`` — contracts.Direction is
      ``{bullish,bearish,neutral}`` (enum mapping gap)
    - contracts.Ex2CandidateSignal requires ``affected_sectors`` and
      ``evidence: list[EvidenceRef]`` — announcement has neither

    The fix path is one of:
    (a) Refactor announcement candidate models to mirror contracts.Ex*
        exactly (rename + drop fields).
    (b) Extend contracts.Ex* with optional fields covering announcement's
        local concepts.
    (c) Define a translation layer that maps announcement → contracts at
        the wire boundary (in addition to ``_normalize_for_sdk``'s
        subsystem_id/produced_at addition).

    None of those are in stage 2.8 scope. They need a separate
    cross-repo schema decision milestone.
    """

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "Announcement Ex-1 candidate has primary_entity_id (not "
            "entity_id), announcement_id, evidence_spans, "
            "related_entity_ids — none accepted by contracts.Ex1CandidateFact "
            "(extra='forbid'). Cross-repo schema reconciliation needed; see "
            "TestKnownAnnouncementContractsSchemaGap docstring."
        ),
    )
    def test_announcement_ex1_wire_validates_against_contracts_unchanged(
        self,
    ) -> None:
        from contracts.schemas import Ex1CandidateFact

        from subsystem_announcement.extract import (
            AnnouncementFactCandidate,
        )
        from subsystem_announcement.extract.candidates import FactType
        from subsystem_announcement.extract.evidence import EvidenceSpan
        from subsystem_announcement.runtime.submit import _validated_payload

        candidate = AnnouncementFactCandidate(
            fact_id="gap-ex1",
            announcement_id="gap-ann",
            fact_type=FactType.MAJOR_CONTRACT,
            primary_entity_id="ENT_STOCK_300750_SZ",
            fact_content={"k": "v"},
            confidence=0.92,
            source_reference={
                "official_url": "https://www.sse.com.cn/disclosure/announcement/gap",
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
        wire = _validated_payload(candidate)
        # This will fail today: contracts.Ex1CandidateFact has
        # extra='forbid' and the wire payload includes announcement-
        # specific fields (announcement_id, evidence_spans, etc.).
        Ex1CandidateFact.model_validate(wire)

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "Announcement Ex-2 SignalDirection enum is "
            "{positive,negative,neutral}; contracts.Direction is "
            "{bullish,bearish,neutral}. Plus contracts.Ex2CandidateSignal "
            "requires affected_sectors + evidence (announcement has neither). "
            "Cross-repo schema reconciliation needed."
        ),
    )
    def test_announcement_ex2_wire_validates_against_contracts_unchanged(
        self,
    ) -> None:
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
            signal_id="gap-ex2",
            announcement_id="gap-ann",
            signal_type="major_contract_positive",
            direction=SignalDirection.POSITIVE,
            magnitude=0.7,
            affected_entities=["ENT_STOCK_300750_SZ"],
            time_horizon=SignalTimeHorizon.SHORT_TERM,
            source_fact_ids=["gap-source-fact"],
            source_reference={
                "official_url": "https://www.sse.com.cn/disclosure/announcement/gap",
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
        # Will fail: enum mismatch + missing affected_sectors/evidence.
        Ex2CandidateSignal.model_validate(wire)

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "Announcement Ex-3 has source_node/target_node + delta_type "
            "GraphDeltaType {add_edge,update_edge}; contracts.Ex3CandidateGraphDelta "
            "expects same. But announcement also has source_fact_ids + "
            "evidence_spans which contracts doesn't accept (extra='forbid'); "
            "and contracts requires `evidence: list[EvidenceRef]` which "
            "announcement doesn't emit. Cross-repo schema reconciliation needed."
        ),
    )
    def test_announcement_ex3_wire_validates_against_contracts_unchanged(
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
        from subsystem_announcement.runtime.submit import _validated_payload

        candidate = AnnouncementGraphDeltaCandidate(
            delta_id="gap-ex3",
            announcement_id="gap-ann",
            delta_type=GraphDeltaType.ADD_EDGE,
            source_node="ENT_STOCK_INTEG_SRC",
            target_node="ENT_STOCK_INTEG_DST",
            relation_type=GraphRelationType.SUPPLY_CONTRACT,
            properties={"k": "v"},
            source_fact_ids=["gap-source-fact"],
            source_reference={
                "official_url": "https://www.sse.com.cn/disclosure/announcement/gap",
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
        # Will fail: announcement has source_fact_ids + evidence_spans,
        # contracts has neither and requires `evidence`.
        Ex3CandidateGraphDelta.model_validate(wire)
