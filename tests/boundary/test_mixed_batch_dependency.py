"""Boundary tier — mixed-batch Ex-2/Ex-3 → Ex-1 dependency gate
regression test (codex plan-review #4 P1 + #5 P2).

Why this test exists
====================

Stage 2.8 follow-up #3 moved ``source_fact_ids`` from the wire payload
top-level into ``producer_context``. The mixed-batch dependency gate
``_missing_unaccepted_batch_fact_ids`` previously read from the wire
payload via ``payload.get("source_fact_ids")``. That call would now
silently return ``()`` because the key is no longer top-level — and
Ex-2 / Ex-3 candidates would slip past the gate even when their
referenced Ex-1 fact never made it into ``accepted_fact_ids`` (a
correctness bug, not a performance issue).

The fix in ``runtime/submit.py``:
- Refactor ``_missing_unaccepted_batch_fact_ids(payload, ...)`` →
  ``_missing_unaccepted_batch_fact_ids(candidate, ...)``.
- Read from ``candidate.source_fact_ids`` (announcement-local Pydantic
  model attribute), not from the post-``_normalize_for_sdk`` wire
  payload.

This boundary test is the regression guard. If anyone later refactors
the gate back to reading from the wire payload, it fails immediately
with the wrong-condition message:

  "skipped Ex-2 candidate because source_fact_ids are not accepted Ex-1 facts"

Test condition (codex plan-review #5 P2)
=========================================

The correct way to exercise the gate is **NOT** "put Ex-1 later in the
input list" — ``submit_candidates`` partitions candidates by ex_type
and runs Ex-1s first, so input order doesn't matter for the gate. The
gate fires when:

  - an Ex-1 fact_id IS in the batch (so ``batch_fact_ids`` contains it)
  - but that Ex-1 ends up FAILING acceptance (so ``accepted_fact_ids``
    does NOT contain it)
  - and an Ex-2/Ex-3 in the same batch references it via
    ``source_fact_ids``

We synthesize this with a ``RejectingFactsSubsystem`` that accepts
every Ex-1 submission as ``accepted=False`` — the fact is in the batch
but never enters ``accepted_fact_ids``. The dependent Ex-2 then fires
``_dependency_failure_trace``.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from subsystem_announcement.extract import AnnouncementFactCandidate
from subsystem_announcement.extract.candidates import FactType
from subsystem_announcement.extract.evidence import EvidenceSpan
from subsystem_announcement.runtime.submit import (
    SubmitIdempotencyStore,
    submit_candidates,
)
from subsystem_announcement.signals import AnnouncementSignalCandidate
from subsystem_announcement.signals.candidates import (
    SignalDirection,
    SignalTimeHorizon,
)


# ── Test stub ─────────────────────────────────────────────────────


class RejectingFactsSubsystem:
    """Accepts Ex-1 submissions but marks them as ``accepted=False``.

    Fact IDs go into ``batch_fact_ids`` (computed from input list before
    submission) but never enter ``accepted_fact_ids`` because every Ex-1
    receipt comes back rejected. This is the exact scenario the
    dependency gate must catch (codex plan-review #5 P2: "Ex-1 in batch
    but doesn't make it into accepted_fact_ids").
    """

    def __init__(self) -> None:
        self.submissions: list[dict[str, Any]] = []

    def submit(self, candidate: dict[str, Any]) -> dict[str, Any]:
        self.submissions.append(candidate)
        if candidate.get("ex_type") == "Ex-1":
            return {
                "accepted": False,
                "receipt_id": f"rejected-{len(self.submissions)}",
                "warnings": (),
                "errors": ("fact rejected by stub for boundary test",),
            }
        return {
            "accepted": True,
            "receipt_id": f"receipt-{len(self.submissions)}",
            "warnings": (),
            "errors": (),
        }


def _ex1_candidate(
    *, fact_id: str, announcement_id: str
) -> AnnouncementFactCandidate:
    return AnnouncementFactCandidate(
        fact_id=fact_id,
        announcement_id=announcement_id,
        fact_type=FactType.MAJOR_CONTRACT,
        primary_entity_id="ENT_STOCK_BOUNDARY",
        fact_content={"k": "v"},
        confidence=0.9,
        source_reference={
            "official_url": (
                "https://www.sse.com.cn/disclosure/announcement/boundary"
            ),
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


def _ex2_candidate(
    *, signal_id: str, announcement_id: str, source_fact_ids: Iterable[str]
) -> AnnouncementSignalCandidate:
    return AnnouncementSignalCandidate(
        signal_id=signal_id,
        announcement_id=announcement_id,
        signal_type="major_contract_positive",
        direction=SignalDirection.POSITIVE,
        magnitude=0.7,
        affected_entities=["ENT_STOCK_BOUNDARY"],
        time_horizon=SignalTimeHorizon.SHORT_TERM,
        source_fact_ids=list(source_fact_ids),
        source_reference={
            "official_url": (
                "https://www.sse.com.cn/disclosure/announcement/boundary"
            ),
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


# ── Tests ─────────────────────────────────────────────────────────


class TestMixedBatchDependencyGateUsesCandidate:
    """Mixed-batch dependency gate must read ``source_fact_ids`` from
    the candidate (announcement-local Pydantic model attribute), NOT
    from the post-``_normalize_for_sdk`` wire payload (where it lives
    in ``producer_context`` after stage 2.8 follow-up #3).
    """

    def test_ex2_dependency_fails_when_referenced_ex1_is_in_batch_but_not_accepted(
        self, tmp_path: Path
    ) -> None:
        """The plan-review #5 P2 test condition: Ex-1 IS in batch
        (batch_fact_ids contains it) but ends up failing acceptance
        (accepted_fact_ids does NOT contain it). Ex-2 referencing it
        via source_fact_ids must fail with the dependency-gate error.

        If the gate ever regresses to reading source_fact_ids from the
        post-normalize wire payload, this test fails immediately —
        because the wire payload no longer has source_fact_ids at top
        level (it's in producer_context).
        """

        ann_id = "ANN-BOUNDARY-DEP-001"
        ex1_fact_id = "boundary-ex1-rejected"
        ex2_signal_id = "boundary-ex2-depends-on-rejected"

        candidates: list[Any] = [
            _ex1_candidate(fact_id=ex1_fact_id, announcement_id=ann_id),
            _ex2_candidate(
                signal_id=ex2_signal_id,
                announcement_id=ann_id,
                source_fact_ids=[ex1_fact_id],
            ),
        ]

        subsystem = RejectingFactsSubsystem()
        store = SubmitIdempotencyStore(tmp_path / "submit_idempotency.json")

        result = submit_candidates(
            candidates,
            subsystem=subsystem,  # type: ignore[arg-type]
            idempotency_store=store,
        )

        # Ex-1 was submitted (and rejected; submit_candidates retries up
        # to max_attempts so we may see >=1 Ex-1 entries). Ex-2 was NOT
        # submitted — the dependency gate caught it before it ever
        # reached the subsystem.
        ex_types_submitted = [
            payload.get("ex_type") for payload in subsystem.submissions
        ]
        assert "Ex-2" not in ex_types_submitted, (
            f"Ex-2 should NOT reach the subsystem because the dependency "
            f"gate caught it. Got submissions: {ex_types_submitted}"
        )
        assert ex_types_submitted, (
            "Expected at least one Ex-1 submission attempt before the "
            "dependency-gated Ex-2 was rejected; got zero submissions, "
            "which means submit_candidates short-circuited before "
            "exercising the gate."
        )
        assert all(
            ex_type == "Ex-1" for ex_type in ex_types_submitted
        ), (
            f"All recorded submissions should be Ex-1 retries; got "
            f"{ex_types_submitted}"
        )

        # Inspect Ex-2's failure trace explicitly.
        ex2_traces = [
            trace
            for trace in result.traces
            if trace.ex_type == "Ex-2"
        ]
        assert len(ex2_traces) == 1, (
            f"expected exactly one Ex-2 trace, got: {ex2_traces}"
        )
        ex2_trace = ex2_traces[0]
        assert ex2_trace.status == "failed"
        assert ex2_trace.attempts == 0, (
            "Ex-2 should fail at the gate (attempts=0), not after a real "
            "submit attempt"
        )
        assert any(
            "source_fact_ids are not accepted Ex-1 facts" in error
            for error in ex2_trace.errors
        ), (
            f"Ex-2 trace must carry the dependency-gate error. Got "
            f"errors={list(ex2_trace.errors)}. If this test fails because "
            "the gate is silently passing, the regression is that "
            "_missing_unaccepted_batch_fact_ids is reading source_fact_ids "
            "from the post-normalize wire payload again — it must read "
            "from candidate.source_fact_ids (announcement-local model "
            "attribute) since stage 2.8 follow-up #3 moved source_fact_ids "
            "into producer_context."
        )
        assert ex1_fact_id in ex2_trace.errors[0], (
            f"dependency-gate error message must name the missing Ex-1 "
            f"fact_id; got {ex2_trace.errors[0]!r}"
        )

    def test_normalized_wire_payload_does_not_carry_source_fact_ids_top_level(
        self,
    ) -> None:
        """Direct white-box check that the gate's regression vector is
        real: the post-``_normalize_for_sdk`` wire payload for Ex-2 has
        NO top-level ``source_fact_ids`` (it's in ``producer_context``
        instead). A future maintainer who adds ``source_fact_ids`` to
        the canonical wire shape would also need to revisit this gate
        decision.
        """

        from subsystem_announcement.runtime.submit import _validated_payload

        candidate = _ex2_candidate(
            signal_id="boundary-ex2-shape-check",
            announcement_id="ANN-BOUNDARY-DEP-SHAPE",
            source_fact_ids=["some-fact-id"],
        )

        wire = _validated_payload(candidate)

        assert "source_fact_ids" not in wire, (
            "Ex-2 wire payload must NOT contain top-level "
            f"source_fact_ids (got keys: {sorted(wire)}). If you intend "
            "to add it back to the canonical wire shape, update "
            "_missing_unaccepted_batch_fact_ids and the regression test "
            "above accordingly."
        )
        producer_context = wire.get("producer_context") or {}
        assert producer_context.get("source_fact_ids") == ["some-fact-id"], (
            f"source_fact_ids must live in producer_context after "
            f"_normalize_for_sdk; got producer_context={producer_context}"
        )
