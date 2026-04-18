from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from subsystem_announcement.extract import extract_fact_candidates
from subsystem_announcement.signals import AnnouncementSignalCandidate, derive_signal_candidates

from tests.extract_fixtures import make_artifact


FORBIDDEN_KEYS = {"submitted_at", "ingest_seq", "layer_b_receipt_id", "local_path"}


def test_ex2_payload_shape_and_forbidden_metadata() -> None:
    fact = _earnings_fact()
    signal = derive_signal_candidates(
        [fact],
        generated_at=datetime(2026, 4, 18, 10, 0, tzinfo=timezone.utc),
    )[0]

    payload = signal.to_ex_payload()

    assert payload["ex_type"] == "Ex-2"
    assert payload["signal_id"]
    assert payload["announcement_id"] == fact.announcement_id
    assert payload["signal_type"] == "earnings_preannounce_outlook"
    assert payload["direction"] == "positive"
    assert payload["affected_entities"] == [fact.primary_entity_id]
    assert payload["source_fact_ids"] == [fact.fact_id]
    assert payload["source_reference"] == fact.source_reference
    assert payload["evidence_spans"] == [span.model_dump(mode="json") for span in fact.evidence_spans]
    AnnouncementSignalCandidate.model_validate(payload)
    _assert_no_forbidden_keys(payload)


def test_ex2_source_reference_is_not_forged_when_missing() -> None:
    fact = _earnings_fact().model_copy(update={"source_reference": {}})

    assert derive_signal_candidates([fact]) == []


def _earnings_fact():
    artifact = make_artifact(
        "证券代码：600000\n证券简称：测试公司\n"
        "公司预计2026年净利润同比增长50%，本公告为业绩预告。",
        announcement_id="ann-ex2-contract",
    )
    return extract_fact_candidates(artifact)[0]


def _assert_no_forbidden_keys(value: Any) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            assert key not in FORBIDDEN_KEYS
            _assert_no_forbidden_keys(item)
    elif isinstance(value, list):
        for item in value:
            _assert_no_forbidden_keys(item)
