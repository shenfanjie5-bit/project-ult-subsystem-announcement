from __future__ import annotations

import inspect
import re
from typing import Any

import pytest
from pydantic import ValidationError

import subsystem_announcement.extract.reasoner_bridge as reasoner_bridge
from subsystem_announcement.extract import AnnouncementFactCandidate, extract_fact_candidates

from tests.extract_fixtures import make_artifact


FORBIDDEN_KEYS = {"submitted_at", "ingest_seq", "layer_b_receipt_id", "local_path"}


def test_ex1_payload_shape_and_forbidden_metadata() -> None:
    artifact = make_artifact(
        "证券代码：600000\n证券简称：测试公司\n公司与华东能源签订重大合同，合同金额为1000万元。",
        announcement_id="ann-contract",
    )
    fact = extract_fact_candidates(artifact)[0]

    payload = fact.to_ex_payload()

    assert payload["ex_type"] == "Ex-1"
    assert payload["fact_id"]
    assert payload["announcement_id"] == "ann-contract"
    assert payload["fact_type"] == "major_contract"
    assert payload["primary_entity_id"] == "ts_code:600000.SH"
    assert payload["confidence"] > 0
    assert payload["source_reference"]["official_url"].startswith("https://")
    assert payload["evidence_spans"]
    _assert_no_forbidden_keys(payload)


def test_ex1_candidate_requires_official_source_reference() -> None:
    artifact = make_artifact(
        "证券代码：600000\n证券简称：测试公司\n公司与华东能源签订重大合同。",
    )
    data = extract_fact_candidates(artifact)[0].model_dump()
    data["source_reference"] = {}

    with pytest.raises(ValidationError, match="official_url"):
        AnnouncementFactCandidate.model_validate(data)


def test_reasoner_bridge_does_not_import_provider_sdks() -> None:
    source = inspect.getsource(reasoner_bridge)

    assert re.search(r"\b(openai|anthropic|dashscope)\b", source) is None
    assert "reasoner_runtime" in source


def _assert_no_forbidden_keys(value: Any) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            assert key not in FORBIDDEN_KEYS
            _assert_no_forbidden_keys(item)
    elif isinstance(value, list):
        for item in value:
            _assert_no_forbidden_keys(item)
