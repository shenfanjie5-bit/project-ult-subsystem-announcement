"""Conservative Ex-3 graph delta intent rules from Ex-1 facts."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from subsystem_announcement.extract import AnnouncementFactCandidate, FactType

from .candidates import GraphDeltaType, GraphRelationType
from .guard import has_ambiguous_graph_language, is_resolved_entity_id


@dataclass(frozen=True)
class GraphDeltaIntent:
    """A classified graph delta before threshold checks and materialization."""

    delta_type: GraphDeltaType
    relation_type: GraphRelationType
    source_node: str
    target_node: str
    properties: dict[str, Any] = field(default_factory=dict)
    reason: str = ""


_PERCENT = r"\d+(?:\.\d+)?%"
_RATIO_CHANGE_RE = re.compile(
    rf"持股比例(?:由|从)?\s*(?P<before>{_PERCENT})"
    rf"[^。；;，,]{{0,24}}(?:增至|增加至|升至|上升至|降至|下降至|减至|减少至|变更为|变为|至|到)"
    rf"\s*(?P<after>{_PERCENT})"
)
_AFTER_RATIO_RE = re.compile(
    rf"(?:变动后|权益变动后|本次权益变动后|完成后|减持后|增持后)"
    rf"[^。；;，,]{{0,30}}(?:持股比例)?(?:为|至|达到|降至|增至)?\s*(?P<after>{_PERCENT})"
)
_HOLDING_RATIO_RE = re.compile(
    rf"持股比例(?:为|至|达到|降至|增至)\s*(?P<after>{_PERCENT})"
)
_CHANGE_RATIO_RE = re.compile(
    rf"(?P<direction>增持|减持)比例(?:为|达到)?\s*(?P<change>{_PERCENT})"
)

_MAJOR_CONTRACT_ACTION_RE = re.compile(
    r"签订|签署|订立|签约|中标|合作协议|终止合同|解除合同|合同终止"
)
_COOPERATION_RE = re.compile(r"合作协议|战略合作协议")
_TERMINATION_RE = re.compile(r"终止合同|解除合同|合同终止")


def classify_graph_delta_intent(
    fact: AnnouncementFactCandidate,
) -> GraphDeltaIntent | None:
    """Classify one Ex-1 fact into an Ex-3 graph intent, when safe."""

    if fact.fact_type is FactType.SHAREHOLDER_CHANGE:
        return _shareholder_change_intent(fact)
    if fact.fact_type is FactType.MAJOR_CONTRACT:
        return _major_contract_intent(fact)
    return None


def _shareholder_change_intent(
    fact: AnnouncementFactCandidate,
) -> GraphDeltaIntent | None:
    if _evidence_has_ambiguous_language(fact):
        return None
    source_node = _first_resolved_related_entity(fact)
    if source_node is None or not is_resolved_entity_id(fact.primary_entity_id):
        return None

    if fact.fact_content.get("shareholder_change_type") == "control_change":
        return GraphDeltaIntent(
            delta_type=GraphDeltaType.UPDATE_EDGE,
            relation_type=GraphRelationType.CONTROL,
            source_node=source_node,
            target_node=fact.primary_entity_id,
            properties={"change_type": "control_change"},
            reason="explicit_control_change",
        )

    ratio_properties = _shareholding_ratio_properties(fact)
    if not ratio_properties:
        return None
    return GraphDeltaIntent(
        delta_type=GraphDeltaType.UPDATE_EDGE,
        relation_type=GraphRelationType.SHAREHOLDING,
        source_node=source_node,
        target_node=fact.primary_entity_id,
        properties=ratio_properties,
        reason="explicit_shareholding_ratio",
    )


def _major_contract_intent(
    fact: AnnouncementFactCandidate,
) -> GraphDeltaIntent | None:
    if _evidence_has_ambiguous_language(fact):
        return None
    evidence_text = _evidence_text(fact)
    if _MAJOR_CONTRACT_ACTION_RE.search(evidence_text) is None:
        return None
    target_node = _first_resolved_related_entity(fact)
    if target_node is None or not is_resolved_entity_id(fact.primary_entity_id):
        return None

    relation_type = (
        GraphRelationType.COOPERATION
        if _COOPERATION_RE.search(evidence_text)
        else GraphRelationType.SUPPLY_CONTRACT
    )
    properties: dict[str, Any] = {"event": fact.fact_content.get("event", "major_contract")}
    if _TERMINATION_RE.search(evidence_text):
        properties["contract_status"] = "terminated"

    return GraphDeltaIntent(
        delta_type=GraphDeltaType.ADD_EDGE,
        relation_type=relation_type,
        source_node=fact.primary_entity_id,
        target_node=target_node,
        properties=properties,
        reason="explicit_major_contract_action",
    )


def _first_resolved_related_entity(fact: AnnouncementFactCandidate) -> str | None:
    for entity_id in fact.related_entity_ids:
        if is_resolved_entity_id(entity_id):
            return entity_id
    return None


def _shareholding_ratio_properties(
    fact: AnnouncementFactCandidate,
) -> dict[str, Any]:
    text = _evidence_text(fact).replace("％", "%")
    properties: dict[str, Any] = {}

    change_match = _RATIO_CHANGE_RE.search(text)
    if change_match is not None:
        properties["before_ratio"] = change_match.group("before")
        properties["after_ratio"] = change_match.group("after")
        return properties

    after_match = _AFTER_RATIO_RE.search(text) or _HOLDING_RATIO_RE.search(text)
    if after_match is not None:
        properties["after_ratio"] = after_match.group("after")

    change_ratio_match = _CHANGE_RATIO_RE.search(text)
    if change_ratio_match is not None:
        properties["direction"] = (
            "increase"
            if change_ratio_match.group("direction") == "增持"
            else "decrease"
        )
        properties["change_ratio"] = change_ratio_match.group("change")

    return properties


def _evidence_has_ambiguous_language(fact: AnnouncementFactCandidate) -> bool:
    return any(has_ambiguous_graph_language(span.quote) for span in fact.evidence_spans)


def _evidence_text(fact: AnnouncementFactCandidate) -> str:
    return "\n".join(span.quote for span in fact.evidence_spans)
