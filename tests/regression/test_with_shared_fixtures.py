"""Regression tier — real consumption of audit_eval_fixtures via the
announcement runtime.

Iron rule #1: hard-import `audit_eval_fixtures` (no
`pytest.skip(allow_module_level=True)`). If the [shared-fixtures] extra
isn't installed, this module ImportErrors at collection — the
regression lane in CI stays honest about whether the dependency is
really there.

Iron rule #5 + main-core sub-rule (codex stage 2.8 review #5 P2):
must really call announcement runtime AND have at least one
fixture-derived business expectation. We use
`event_cases.case_ex3_negative` (added in audit-eval v0.2.2 — stage
2.8 pre-step 2) which is the announcement-targeted Ex-3 high-threshold
negative sample.

This regression invokes THREE real announcement runtime paths from the
fixture (codex stage 2.8 review #5 P2 fix):

1. ``_validate_official_url_text(case.input.source_reference.url)`` →
   the discovery-layer guard that blocks non-official sources BEFORE
   any candidate is built. The fixture's source_url is a forum
   redistribution URL; expected: ``NonOfficialSourceError``.

2. ``derive_graph_delta_candidates([fact])`` (the REAL Ex-3 derivation
   entry point, including ``GraphDeltaGuard.check`` and
   ``classify_graph_delta_intent``) → assert it returns ``[]``
   (zero Ex-3 candidates emitted), matching
   ``case.expected.ex3_candidates_emitted == 0`` exactly.

3. ``GraphDeltaGuard.check(fact, intent)`` directly → assert it returns
   ``allow=False`` and the reasons list overlaps the fixture's
   declared ``case.expected.guard_reasons`` (after mapping fixture
   labels {non_official_source / single_weak_evidence /
   unresolved_target_entity_anchor} to the SDK guard's actual reason
   tokens {missing_source_reference / low_fact_confidence /
   insufficient_evidence_spans / unresolved_source_node /
   unresolved_target_node / ambiguous_language}).

Constructing the real fact: the fixture describes a SCENARIO (a weak
Ex-3 attempt that should be rejected). To exercise the real Ex-3
runtime, we build a real ``AnnouncementFactCandidate`` from that
scenario. Note: the candidate model itself REQUIRES official_url
(it would refuse to construct without one), so the
"non_official_source" guard reason is provable via path #1
(`_validate_official_url_text`), not via path #3 (the in-model guard
runs against a model that already passed construction). Both reasons
end up exercised, just at different layers.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

# Iron rule #1 — bare import, no allow_module_level skip.
from audit_eval_fixtures import (  # noqa: F401  (load_case below proves use)
    fixture_root,
    iter_cases,
    load_case,
)


_PACK = "event_cases"
_CASE = "case_ex3_negative"


# Mapping from fixture-side guard reason labels (announcement §10
# vocabulary) to subsystem_announcement.graph.guard reason tokens.
# fixture vocab is human-readable; runtime guard tokens are stable
# names in graph/guard.py:GraphDeltaGuard.check.
_FIXTURE_TO_RUNTIME_REASON = {
    "non_official_source": ["missing_source_reference"],
    "single_weak_evidence": [
        "insufficient_evidence_spans",
        "low_fact_confidence",
    ],
    "unresolved_target_entity_anchor": [
        "unresolved_source_node",
        "unresolved_target_node",
    ],
}


# ── Fixture presence + metadata ──────────────────────────────────


class TestEx3NegativeCasePresent:
    def test_event_cases_pack_includes_ex3_negative(self) -> None:
        case_ids = sorted(c.case_id for c in iter_cases(_PACK))
        assert _CASE in case_ids, (
            f"audit_eval_fixtures.event_cases is missing {_CASE!r}; "
            f"got {case_ids}. announcement regression depends on this case "
            "(added in audit-eval v0.2.2 stage 2.8 pre-step 2)."
        )

    def test_case_metadata_marks_announcement_as_primary_consumer(
        self,
    ) -> None:
        case = load_case(_PACK, _CASE)
        assert case.metadata["fixture_kind"] == "ex3_high_threshold_negative"
        assert (
            case.metadata["primary_consumer"] == "subsystem-announcement"
        )


# ── Real announcement runtime: discovery-layer official-source guard ──


class TestDiscoveryLayerRejectsNonOfficialSourceFromFixture:
    """Real call to ``_validate_official_url_text`` (announcement
    discovery-layer guard) using the fixture's source URL. This is
    what blocks non-official announcements BEFORE any candidate model
    is built — drives the fixture's ``non_official_source``
    guard_reason directly through real runtime.
    """

    def test_fixture_source_url_rejected_by_discovery_official_validator(
        self,
    ) -> None:
        from subsystem_announcement.discovery.errors import (
            NonOfficialSourceError,
        )
        from subsystem_announcement.discovery.fetcher import (
            _validate_official_url_text,
        )

        case = load_case(_PACK, _CASE)
        source_url = case.input["source_reference"]["url"]

        assert (
            "non_official_source" in case.expected["guard_reasons"]
        ), (
            "fixture's expected.guard_reasons no longer lists "
            "'non_official_source'; this regression's premise has "
            "changed and needs re-derivation"
        )

        with pytest.raises(NonOfficialSourceError):
            _validate_official_url_text(
                source_url,
                announcement_id=case.input["announcement_id"],
            )


# ── Real announcement runtime: Ex-3 derivation pipeline ──


def _build_fact_from_fixture(
    case_input: dict,
) -> "object":
    """Build a real ``AnnouncementFactCandidate`` from the fixture's
    scenario. The fact is constructed with weak-evidence properties
    (single evidence span + confidence below GraphDeltaGuard threshold
    + unresolved target entity) so that the Ex-3 derivation pipeline
    rejects it for the right reasons. ``official_url`` IS provided
    (otherwise the model would refuse to construct) — the
    non_official_source rejection is exercised separately at the
    discovery layer (TestDiscoveryLayerRejectsNonOfficialSourceFromFixture).
    """

    from subsystem_announcement.extract import AnnouncementFactCandidate
    from subsystem_announcement.extract.candidates import FactType
    from subsystem_announcement.extract.evidence import EvidenceSpan

    attempt = case_input["candidate_graph_delta_attempt"]
    extracted = case_input["extracted_facts_so_far"][0]
    evidence_text = extracted["evidence_span"]["exact_text"]

    return AnnouncementFactCandidate(
        # Map fixture's scenario into the announcement model.
        fact_id=extracted["fact_id"],
        announcement_id=case_input["announcement_id"],
        # FactType.MAJOR_CONTRACT classifier path (rule:
        # graph/rules.py:_major_contract_intent) — this is the codepath
        # the fixture's "rumored_contract_change" scenario maps to;
        # MAJOR_CONTRACT is what the announcement runtime would emit
        # for a "contract terminated" classification.
        fact_type=FactType.MAJOR_CONTRACT,
        # Source = fixture's primary entity (resolved); target = the
        # unresolved downstream partner (mapped to a string the runtime
        # guard recognizes as unresolved via is_resolved_entity_id).
        primary_entity_id=attempt["source_node"],
        related_entity_ids=["unresolved:downstream-partner"],
        fact_content={"event": "contract_terminated"},
        # Below GraphDeltaGuard.min_fact_confidence (0.90) — drives
        # low_fact_confidence guard reason.
        confidence=extracted["confidence"],
        # Force official_url so the model accepts construction; the
        # discovery layer would have rejected this URL before reaching
        # this point in the production flow.
        source_reference={
            "official_url": "https://www.sse.com.cn/disclosure/announcement/synthetic-for-guard-test",
            "is_primary_source": True,
        },
        # Single evidence span — below GraphDeltaGuard.min_evidence_spans
        # (2) — drives insufficient_evidence_spans guard reason.
        evidence_spans=[
            EvidenceSpan(
                section_id=extracted["evidence_span"].get(
                    "section_id", "fixture-section"
                ),
                start_offset=extracted["evidence_span"].get(
                    "start_offset", extracted["evidence_span"].get("start", 0)
                ),
                end_offset=extracted["evidence_span"].get(
                    "end_offset",
                    extracted["evidence_span"].get(
                        "start", 0
                    )
                    + len(evidence_text),
                ),
                quote=evidence_text,
            ),
        ],
        extracted_at=datetime(2026, 4, 18, tzinfo=UTC),
    )


class TestRealEx3DerivationProducesZeroCandidatesForFixture:
    """Real call to ``derive_graph_delta_candidates`` (the entry point
    that production announcement runtime calls). Asserts the EXACT
    fixture-derived business expectation: 0 Ex-3 candidates.

    This exercises the FULL Ex-3 derivation chain:
    - classify_graph_delta_intent (graph/rules.py)
    - GraphDeltaGuard.check (graph/guard.py)
    - AnnouncementGraphDeltaCandidate construction (graph/candidates.py)
    """

    def test_derive_graph_delta_candidates_emits_zero_for_fixture_input(
        self,
    ) -> None:
        from subsystem_announcement.graph.deltas import (
            derive_graph_delta_candidates,
        )

        case = load_case(_PACK, _CASE)
        fact = _build_fact_from_fixture(case.input)

        deltas = derive_graph_delta_candidates([fact])

        # THE fixture-derived business expectation per main-core
        # sub-rule: not a generic invariant ("<= 0"), but the EXACT
        # number announcement runtime should produce when fed this
        # specific weak-evidence input.
        expected_count = case.expected["ex3_candidates_emitted"]
        assert expected_count == 0, (
            f"fixture's expected.ex3_candidates_emitted changed from 0 "
            f"to {expected_count}; this regression's premise is no longer "
            "an Ex-3 high-threshold *negative* case"
        )
        assert len(deltas) == expected_count, (
            f"REAL announcement runtime emitted {len(deltas)} Ex-3 "
            f"candidates for fixture input that fixture says must "
            f"produce {expected_count}; either GraphDeltaGuard regressed "
            "(let weak evidence through) or _build_fact_from_fixture "
            "stopped reflecting the fixture's scenario shape"
        )


class TestRuleLayerRejectsFixtureFact:
    """Real call to ``classify_graph_delta_intent`` (rule-layer). The
    fixture's target_node is unresolved (ENT_UNRESOLVED_DOWNSTREAM_PARTNER →
    mapped to "unresolved:downstream-partner" via _build_fact_from_fixture
    so the runtime's ``is_resolved_entity_id`` recognizes it as
    unresolved). The rule layer (graph/rules.py:_major_contract_intent)
    short-circuits to None when target_node isn't resolved — that IS
    a valid Ex-3 rejection path corresponding to the fixture's
    ``unresolved_target_entity_anchor`` guard_reason.
    """

    def test_rule_layer_returns_none_for_unresolved_target(self) -> None:
        from subsystem_announcement.graph.rules import (
            classify_graph_delta_intent,
        )

        case = load_case(_PACK, _CASE)
        fact = _build_fact_from_fixture(case.input)

        intent = classify_graph_delta_intent(fact)
        assert intent is None, (
            "rule-layer accepted a fact with unresolved target_node — "
            "graph/rules.py:_major_contract_intent or _shareholder_change_intent "
            "regressed; the fixture's unresolved_target_entity_anchor "
            "guard_reason is no longer enforced at the rule layer"
        )
        # Cross-check: fixture explicitly declares this reason.
        assert (
            "unresolved_target_entity_anchor"
            in case.expected["guard_reasons"]
        )


def _build_fact_with_resolved_target_for_guard_test(case_input: dict) -> "object":
    """Variant of _build_fact_from_fixture that:
    (a) resolves the target entity so classify_graph_delta_intent
        produces a real intent (not rejected at rule layer);
    (b) uses an evidence quote that matches
        ``_MAJOR_CONTRACT_ACTION_RE`` (specifically '终止合同' so
        classify reaches the guard);
    (c) keeps OTHER weak-evidence properties (single span, low
        confidence) so the guard rejects on those — exercising the
        runtime guard's {insufficient_evidence_spans,
        low_fact_confidence} reason tokens.

    The fixture's exact_text "终止现有供货合同" is interrupted by
    "现有供货" between '终止' and '合同' so the regex doesn't match;
    the synthetic quote restores the contiguous "终止合同" token.
    The fixture INPUT remains unchanged — we adapt the synthetic
    fact construction to make the rule layer pass so the guard runs.
    """

    from subsystem_announcement.extract import AnnouncementFactCandidate
    from subsystem_announcement.extract.candidates import FactType
    from subsystem_announcement.extract.evidence import EvidenceSpan

    attempt = case_input["candidate_graph_delta_attempt"]
    extracted = case_input["extracted_facts_so_far"][0]
    # Use an evidence quote that matches _MAJOR_CONTRACT_ACTION_RE
    # ('终止合同' contiguous). Keep the substring of the fixture quote
    # that actually contains the trigger token, so the assertion stays
    # tied to fixture-derived semantics (the "contract termination"
    # rumor from input.candidate_graph_delta_attempt.properties).
    rule_triggering_quote = "终止合同"

    return AnnouncementFactCandidate(
        fact_id=extracted["fact_id"],
        announcement_id=case_input["announcement_id"],
        fact_type=FactType.MAJOR_CONTRACT,
        primary_entity_id=attempt["source_node"],
        # Resolved target so rule-layer passes — guard takes over rejection.
        related_entity_ids=["ENT_STOCK_RESOLVED_PARTNER"],
        fact_content={"event": "contract_terminated"},
        confidence=extracted["confidence"],  # 0.35 < guard min 0.90
        source_reference={
            "official_url": "https://www.sse.com.cn/disclosure/announcement/synthetic-for-guard-test",
            "is_primary_source": True,
        },
        evidence_spans=[
            EvidenceSpan(
                section_id="fixture-section",
                start_offset=0,
                end_offset=len(rule_triggering_quote),
                quote=rule_triggering_quote,
            ),
        ],  # Single span < guard min 2
        extracted_at=datetime(2026, 4, 18, tzinfo=UTC),
    )


class TestRealGraphDeltaGuardRejectsForExpectedReasons:
    """Real call to ``GraphDeltaGuard.check`` directly with a resolved-
    target variant of the fixture's fact, so the rule layer does NOT
    short-circuit and the guard actually runs. Asserts the runtime
    guard's rejection reasons cover the fixture's
    ``single_weak_evidence`` declared reason (which maps to runtime
    tokens ``insufficient_evidence_spans`` and ``low_fact_confidence``).
    The other fixture reasons (non_official_source / unresolved_target)
    are exercised in the sibling test classes above —
    TestDiscoveryLayerRejectsNonOfficialSourceFromFixture and
    TestRuleLayerRejectsFixtureFact respectively.
    """

    def test_real_guard_rejects_resolved_target_fact_with_weak_evidence(
        self,
    ) -> None:
        from subsystem_announcement.graph.guard import GraphDeltaGuard
        from subsystem_announcement.graph.rules import (
            classify_graph_delta_intent,
        )

        case = load_case(_PACK, _CASE)
        fact = _build_fact_with_resolved_target_for_guard_test(case.input)

        intent = classify_graph_delta_intent(fact)
        assert intent is not None, (
            "Rule-layer rejected a fact even with resolved target — the "
            "guard test scaffold no longer exercises GraphDeltaGuard.check; "
            "either rule layer regressed or the fact construction needs "
            "another field tweak to make it past classify"
        )

        guard_result = GraphDeltaGuard().check(fact, intent)

        # Guard must reject (allow=False).
        assert guard_result.allow is False, (
            f"REAL GraphDeltaGuard accepted weak-evidence fact "
            f"(confidence={fact.confidence}, "
            f"evidence_spans={len(fact.evidence_spans)}); guard regressed"
        )

        # The fixture's "single_weak_evidence" reason maps to runtime
        # tokens {insufficient_evidence_spans, low_fact_confidence};
        # both should trigger because the synthetic fact carries 1
        # evidence span at confidence 0.35.
        runtime_reasons = set(guard_result.reasons)
        for runtime_token in (
            "insufficient_evidence_spans",
            "low_fact_confidence",
        ):
            assert runtime_token in runtime_reasons, (
                f"fixture single_weak_evidence reason maps to runtime token "
                f"{runtime_token!r}, but real GraphDeltaGuard.check did not "
                f"emit it; got {sorted(runtime_reasons)}. Either threshold "
                "constants in graph/guard.py drifted or the synthetic fact "
                "no longer triggers both subchecks."
            )

        # Cross-repo bridge: assert the fixture's declared
        # single_weak_evidence reason is what we just exercised.
        assert (
            "single_weak_evidence" in case.expected["guard_reasons"]
        ), (
            "fixture's expected.guard_reasons no longer lists "
            "'single_weak_evidence'; this test's premise has changed"
        )


# ── audit_eval_fixtures package surface ──────────────────────────


class TestFixturesPackageRoundTrip:
    """Smoke for the audit_eval_fixtures import surface — proves
    `fixture_root` + `iter_cases` work for the pack we depend on."""

    def test_fixture_root_returns_a_real_directory(self) -> None:
        root = fixture_root(_PACK)
        assert root.exists() and root.is_dir(), root

    def test_iter_cases_yields_at_least_two_cases(self) -> None:
        # event_cases now has both case_fuzzy_alias_simple
        # (entity-registry's) AND case_ex3_negative (announcement's,
        # added in v0.2.2).
        cases = list(iter_cases(_PACK))
        assert len(cases) >= 2
        case_ids = sorted(c.case_id for c in cases)
        assert "case_ex3_negative" in case_ids
        assert "case_fuzzy_alias_simple" in case_ids
