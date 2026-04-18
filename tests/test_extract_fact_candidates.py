from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import pytest

import subsystem_announcement.extract as extract_module
from subsystem_announcement.extract import (
    FactType,
    StructuredExtractionRequest,
    extract_fact_candidates,
)
from subsystem_announcement.extract.evidence import evidence_matches_artifact

from .extract_fixtures import make_artifact


PREFIX = "证券代码：600000\n证券简称：测试公司\n"


@pytest.mark.parametrize(
    ("body", "expected"),
    [
        (
            "公司预计2026年净利润同比增长50%，本公告为业绩预告。",
            FactType.EARNINGS_PREANNOUNCE,
        ),
        ("公司与华东能源签订重大合同，合同金额为1000万元。", FactType.MAJOR_CONTRACT),
        ("公司股东明德投资减持股份后持股比例降至5%。", FactType.SHAREHOLDER_CHANGE),
        ("公司股东明德投资将其持有的1000万股股份质押。", FactType.EQUITY_PLEDGE),
        ("公司收到上海证券交易所出具的监管函。", FactType.REGULATORY_ACTION),
        ("公司股票将于2026年4月20日开市起复牌并恢复交易。", FactType.TRADING_HALT_RESUME),
        ("公司拟变更募集资金用途，调整募投项目。", FactType.FUNDRAISING_CHANGE),
    ],
)
def test_extract_fact_candidates_covers_all_fact_types(
    body: str,
    expected: FactType,
) -> None:
    artifact = make_artifact(PREFIX + body, announcement_id=f"ann-{expected.value}")

    facts = extract_fact_candidates(artifact)

    assert [fact.fact_type for fact in facts] == [expected]
    fact = facts[0]
    assert fact.ex_type == "Ex-1"
    assert fact.evidence_spans
    assert evidence_matches_artifact(artifact, fact.evidence_spans[0])
    assert fact.primary_entity_id == "ts_code:600000.SH"
    assert fact.source_reference["official_url"].startswith("https://")


def test_fact_id_is_deterministic_and_duplicate_free() -> None:
    artifact = make_artifact(
        PREFIX + "公司与华东能源签订重大合同，合同金额为1000万元。",
        announcement_id="ann-dedupe",
    )

    first = extract_fact_candidates(artifact)
    second = extract_fact_candidates(artifact)

    assert [fact.fact_id for fact in first] == [fact.fact_id for fact in second]
    assert len({fact.fact_id for fact in first}) == len(first)


def test_title_only_match_does_not_produce_ex1() -> None:
    artifact = make_artifact(
        PREFIX + "公司日常经营情况正常。",
        title="重大合同公告",
        announcement_id="ann-title-only",
    )

    assert extract_fact_candidates(artifact) == []


def test_unresolved_primary_entity_is_retained_without_guessing() -> None:
    artifact = make_artifact(
        "公司与华东能源签订重大合同，合同金额为1000万元。",
        announcement_id="ann-unresolved-primary",
    )

    fact = extract_fact_candidates(artifact)[0]

    assert fact.primary_entity_id.startswith("unresolved:")
    assert fact.fact_content["primary_entity"]["unresolved_ref"].startswith(
        "unresolved:"
    )


class RecordingReasoner:
    def __init__(self) -> None:
        self.requests: list[StructuredExtractionRequest] = []

    def generate_structured(
        self,
        request: StructuredExtractionRequest,
    ) -> Mapping[str, Any]:
        self.requests.append(request)
        return {
            "facts": [
                {
                    "quote": "合同金额为1000万元。",
                    "fact_content": {"amount": "1000万元"},
                    "confidence": 0.7,
                    "related_mentions": ["华东能源"],
                }
            ]
        }


def test_reasoner_fallback_uses_bounded_segments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifact = make_artifact(
        PREFIX + "合同金额为1000万元。" + "补充说明" * 900,
        announcement_id="ann-reasoner",
    )
    reasoner = RecordingReasoner()
    monkeypatch.setitem(
        extract_module._RULE_EXTRACTORS,
        FactType.MAJOR_CONTRACT,
        lambda parsed_artifact, context: [],
    )

    facts = extract_fact_candidates(artifact, reasoner=reasoner)

    assert [fact.fact_type for fact in facts] == [FactType.MAJOR_CONTRACT]
    assert reasoner.requests
    request = reasoner.requests[0]
    assert request.fact_type == FactType.MAJOR_CONTRACT.value
    assert len(request.segments) == 1
    assert len(request.segments[0].text) <= 2_000
    assert request.schema["x-fact-type"] == FactType.MAJOR_CONTRACT.value
    assert "schema" in request.model_dump(by_alias=True)


def test_reasoner_is_not_called_when_classifier_finds_no_body_evidence() -> None:
    artifact = make_artifact(PREFIX + "公司日常经营情况正常。", title="重大合同公告")
    reasoner = RecordingReasoner()

    assert extract_fact_candidates(artifact, reasoner=reasoner) == []
    assert reasoner.requests == []
