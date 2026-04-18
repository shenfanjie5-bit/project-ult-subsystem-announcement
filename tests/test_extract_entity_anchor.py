from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from subsystem_announcement.extract.entity_anchor import (
    EntityAnchorer,
    EntityMention,
    EntityResolution,
)

from .extract_fixtures import make_artifact


class FakeEntityRegistry:
    def __init__(self) -> None:
        self.lookup_calls: list[str] = []
        self.resolve_calls: list[list[str]] = []
        self.alias_results: dict[str, Mapping[str, Any] | None] = {}
        self.fuzzy_results: dict[str, Mapping[str, Any] | None] = {}

    def lookup_alias(self, name: str) -> Mapping[str, Any] | None:
        self.lookup_calls.append(name)
        return self.alias_results.get(name)

    def resolve_mentions(
        self,
        mentions: Sequence[EntityMention],
    ) -> list[Mapping[str, Any]]:
        self.resolve_calls.append([mention.name for mention in mentions])
        return [
            self.fuzzy_results.get(mention.name)
            or {
                "mention": mention,
                "unresolved_ref": f"unresolved:{mention.name}",
                "resolution_method": "resolve_mentions",
            }
            for mention in mentions
        ]


def test_ts_code_primary_anchor_is_deterministic_without_registry_calls() -> None:
    registry = FakeEntityRegistry()
    artifact = make_artifact("证券代码：600000\n证券简称：浦发银行\n公司预计净利润增长。")

    anchor = EntityAnchorer(registry).anchor_primary_entity(artifact)

    assert anchor.entity_id == "ts_code:600000.SH"
    assert anchor.entity_name == "浦发银行"
    assert registry.lookup_calls == []
    assert registry.resolve_calls == []


def test_lookup_alias_runs_after_deterministic_primary_failure() -> None:
    registry = FakeEntityRegistry()
    registry.alias_results["测试股份"] = {
        "mention": "测试股份",
        "entity_id": "entity-001",
        "entity_name": "测试股份有限公司",
        "confidence": 0.95,
        "resolution_method": "lookup_alias",
    }
    artifact = make_artifact("公司名称：测试股份\n公司与客户签订重大合同。")

    anchor = EntityAnchorer(registry).anchor_primary_entity(artifact)

    assert anchor.entity_id == "entity-001"
    assert registry.lookup_calls == ["测试股份"]
    assert registry.resolve_calls == []


def test_fuzzy_resolver_runs_for_unresolved_related_mentions() -> None:
    registry = FakeEntityRegistry()
    registry.fuzzy_results["模糊客户"] = {
        "mention": "模糊客户",
        "entity_id": "entity-fuzzy",
        "entity_name": "模糊客户有限公司",
        "confidence": 0.72,
        "resolution_method": "resolve_mentions",
    }

    anchors = EntityAnchorer(registry).resolve_related_mentions(
        [EntityMention(name="模糊客户", role="counterparty")]
    )

    assert anchors[0].entity_id == "entity-fuzzy"
    assert registry.lookup_calls == ["模糊客户"]
    assert registry.resolve_calls == [["模糊客户"]]


def test_unresolved_mentions_keep_explicit_unresolved_reference() -> None:
    anchor = EntityAnchorer().resolve_related_mentions(["未知客户"])[0]

    assert anchor.entity_id is None
    assert anchor.identifier.startswith("unresolved:")


def test_entity_resolution_model_accepts_unresolved_ref() -> None:
    resolution = EntityResolution(
        mention=EntityMention(name="未知客户"),
        unresolved_ref="unresolved:abc",
        resolution_method="resolve_mentions",
    )

    assert resolution.entity_id is None
    assert resolution.unresolved_ref == "unresolved:abc"
