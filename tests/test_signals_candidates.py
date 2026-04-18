from __future__ import annotations

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from subsystem_announcement.extract import extract_fact_candidates
from subsystem_announcement.signals import (
    AnnouncementSignalCandidate,
    SignalDirection,
    derive_signal_candidates,
    make_signal_id,
)

from .extract_fixtures import make_artifact


GENERATED_AT = datetime(2026, 4, 18, 10, 0, tzinfo=timezone.utc)


def test_signal_candidate_rejects_extra_and_requires_sources() -> None:
    fact = _earnings_fact()
    signal = derive_signal_candidates([fact], generated_at=GENERATED_AT)[0]

    with pytest.raises(ValidationError):
        AnnouncementSignalCandidate.model_validate(
            {**signal.model_dump(mode="python"), "local_path": "/tmp/cache.pdf"}
        )

    with pytest.raises(ValidationError):
        AnnouncementSignalCandidate.model_validate(
            {**signal.model_dump(mode="python"), "source_fact_ids": []}
        )

    with pytest.raises(ValidationError):
        AnnouncementSignalCandidate.model_validate(
            {**signal.model_dump(mode="python"), "evidence_spans": []}
        )


def test_make_signal_id_is_stable_and_sensitive_to_evidence_and_direction() -> None:
    fact = _earnings_fact()
    payload = {
        "direction": SignalDirection.POSITIVE,
        "magnitude": 0.72,
        "affected_entities": [fact.primary_entity_id],
    }

    first = make_signal_id(
        fact.announcement_id,
        "earnings_preannounce_outlook",
        [fact.fact_id],
        fact.evidence_spans,
        payload,
    )
    second = make_signal_id(
        fact.announcement_id,
        "earnings_preannounce_outlook",
        [fact.fact_id],
        fact.evidence_spans,
        payload,
    )
    changed_direction = make_signal_id(
        fact.announcement_id,
        "earnings_preannounce_outlook",
        [fact.fact_id],
        fact.evidence_spans,
        {**payload, "direction": SignalDirection.NEGATIVE},
    )
    changed_span = fact.evidence_spans[0].model_copy(
        update={
            "end_offset": fact.evidence_spans[0].end_offset + 1,
            "quote": f"{fact.evidence_spans[0].quote}x",
        }
    )
    changed_evidence = make_signal_id(
        fact.announcement_id,
        "earnings_preannounce_outlook",
        [fact.fact_id],
        [changed_span],
        payload,
    )

    assert first == second
    assert first.startswith(f"signal:{fact.announcement_id}:earnings_preannounce_outlook:")
    assert changed_direction != first
    assert changed_evidence != first


def test_derive_signal_candidates_preserves_fact_provenance_and_entities() -> None:
    fact = _earnings_fact().model_copy(
        update={"related_entity_ids": ["entity-counterparty", "entity-counterparty"]}
    )

    signal = derive_signal_candidates([fact], generated_at=GENERATED_AT)[0]

    assert signal.source_fact_ids == [fact.fact_id]
    assert signal.source_reference == fact.source_reference
    assert signal.evidence_spans == fact.evidence_spans
    assert signal.affected_entities == [
        fact.primary_entity_id,
        "entity-counterparty",
    ]
    assert signal.generated_at == GENERATED_AT


def test_derive_signal_candidates_returns_empty_for_empty_input() -> None:
    assert derive_signal_candidates([]) == []


def _earnings_fact():
    artifact = make_artifact(
        "证券代码：600000\n证券简称：测试公司\n"
        "公司预计2026年净利润同比增长50%，本公告为业绩预告。",
        announcement_id="ann-signal-candidate",
    )
    return extract_fact_candidates(artifact)[0]
