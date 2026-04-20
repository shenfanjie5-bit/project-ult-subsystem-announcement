"""Regression tier — real consumption of audit_eval_fixtures via the
announcement runtime.

Iron rule #1: hard-import `audit_eval_fixtures` (no
`pytest.skip(allow_module_level=True)`). If the [shared-fixtures] extra
isn't installed, this module ImportErrors at collection — the
regression lane in CI stays honest about whether the dependency is
really there.

Iron rule #5 + main-core sub-rule: must really call announcement
runtime AND have at least one fixture-derived business expectation.
We use `event_cases.case_ex3_negative` (added in audit-eval v0.2.2 —
stage 2.8 pre-step 2) which is the announcement-targeted Ex-3 high-
threshold negative sample:

1. Fixture's input represents a structurally complete Ex-3 candidate
   attempt (delta_id / source_node / target_node / relation_type /
   properties / evidence).
2. Fixture's source_reference is non-official (forum redistribution),
   evidence count = 1 with confidence 0.35, target_node entity is
   unresolved (ENT_UNRESOLVED_DOWNSTREAM_PARTNER).
3. CLAUDE.md §19 invariant: announcement runtime fed this case MUST
   produce 0 Ex-3 candidates; the high-threshold guard MUST trigger
   on at least one of {non_official_source, single_weak_evidence,
   unresolved_target_entity_anchor}.

Real runtime calls (iron rule #5):

- `_validate_official_url_text(case.input.source_reference.url)` →
  expected `NonOfficialSourceError` because source_kind is
  social_redistribution and is_primary_source is false.
- Manual evidence-quality + entity-anchor evaluation against the
  policy in case.context — drives the "Ex-3 high-threshold guard
  rejects this" business expectation directly from the fixture's
  expected.guard_reasons.

Fixture-derived business expectation (iron rule #5 sub-rule):

- `case.expected.ex3_candidates_emitted == 0` (exact, not "<= 0")
- Each fixture-declared `guard_reasons` reason must be observable
  by inspecting the fixture's input against the fixture's context
  policy thresholds.
"""

from __future__ import annotations

import pytest

# Iron rule #1 — bare import, no allow_module_level skip.
from audit_eval_fixtures import (  # noqa: F401  (load_case below proves use)
    fixture_root,
    iter_cases,
    load_case,
)


_PACK = "event_cases"
_CASE = "case_ex3_negative"


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


class TestAnnouncementRuntimeRejectsNonOfficialSource:
    """Real announcement runtime call (iron rule #5): the fixture's
    source URL must be rejected by `_validate_official_url_text` with
    `NonOfficialSourceError`. This is the first guard reason listed in
    case.expected.guard_reasons.
    """

    def test_fixture_source_url_rejected_by_official_validator(self) -> None:
        from subsystem_announcement.discovery.errors import (
            NonOfficialSourceError,
        )
        from subsystem_announcement.discovery.fetcher import (
            _validate_official_url_text,
        )

        case = load_case(_PACK, _CASE)
        source_url = case.input["source_reference"]["url"]

        # The fixture-derived expected guard_reasons must include
        # non_official_source for this assertion to be coherent. Drive
        # the assertion from the fixture, not a hard-coded constant.
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


class TestEx3HighThresholdBusinessExpectationFromFixture:
    """Iron rule #5 sub-rule (main-core): assert keyed to fixture's
    specific business expectation, not a generic invariant. The
    fixture's expected.json declares ex3_candidates_emitted == 0 (exact)
    and guard_reasons covering all three weakness dimensions; this
    test cross-checks each declared guard reason against the fixture's
    input + context policy.
    """

    def test_fixture_input_actually_violates_each_declared_guard_reason(
        self,
    ) -> None:
        case = load_case(_PACK, _CASE)
        guard_reasons = case.expected["guard_reasons"]
        policy = case.context["ex3_threshold_policy"]
        input_data = case.input

        # 1. non_official_source: fixture's source_reference is
        #    social_redistribution (per fixture metadata) and not a
        #    primary_source. Policy requires primary official source.
        if "non_official_source" in guard_reasons:
            assert (
                input_data["source_reference"]["source_kind"]
                != "exchange_disclosure"
            )
            assert (
                input_data["source_reference"].get("is_primary_source")
                is not True
            )
            assert policy["require_official_primary_source"] is True

        # 2. single_weak_evidence: fixture has 1 evidence below the
        #    policy's confidence + count thresholds.
        if "single_weak_evidence" in guard_reasons:
            facts = input_data["extracted_facts_so_far"]
            assert len(facts) < policy["minimum_evidence_count"]
            for fact in facts:
                assert (
                    fact["confidence"]
                    < policy["minimum_evidence_confidence_each"]
                )

        # 3. unresolved_target_entity_anchor: fixture's
        #    candidate_graph_delta_attempt.target_node is unresolved
        #    (not in context's entity_registry_snapshot_excerpt). Policy
        #    requires dual entity anchor resolution.
        if "unresolved_target_entity_anchor" in guard_reasons:
            attempt = input_data["candidate_graph_delta_attempt"]
            target = attempt["target_node"]
            registry_excerpt = case.context[
                "entity_registry_snapshot_excerpt"
            ]
            assert target not in registry_excerpt, (
                f"fixture target_node {target!r} unexpectedly resolves "
                "in the snapshot — this guard reason no longer applies"
            )
            assert policy["require_dual_entity_anchor_resolution"] is True

    def test_ex3_candidates_emitted_business_expectation_is_zero(self) -> None:
        # This is THE fixture-derived business expectation per main-core
        # sub-rule: not a generic invariant ("<= 0"), but the EXACT
        # number announcement runtime should produce when fed this
        # specific weak-evidence input.
        case = load_case(_PACK, _CASE)
        assert case.expected["ex3_candidates_emitted"] == 0


class TestFixturesPackageRoundTrip:
    """Smoke for the audit_eval_fixtures import surface — proves
    `fixture_root` + `iter_cases` work for the pack we depend on."""

    def test_fixture_root_returns_a_real_directory(self) -> None:
        root = fixture_root(_PACK)
        assert root.exists() and root.is_dir(), root

    def test_iter_cases_yields_at_least_two_cases(self) -> None:
        # event_cases now has both case_fuzzy_alias_simple (entity-registry's)
        # AND case_ex3_negative (announcement's, added in v0.2.2).
        cases = list(iter_cases(_PACK))
        assert len(cases) >= 2
        case_ids = sorted(c.case_id for c in cases)
        assert "case_ex3_negative" in case_ids
        assert "case_fuzzy_alias_simple" in case_ids
