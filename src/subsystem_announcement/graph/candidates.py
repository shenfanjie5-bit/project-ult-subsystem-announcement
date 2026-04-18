"""Ex-3 graph delta candidate models and deterministic identity helpers."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from datetime import datetime
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from subsystem_announcement.extract.candidates import FORBIDDEN_PAYLOAD_KEYS
from subsystem_announcement.extract.evidence import EvidenceSpan


class GraphDeltaType(str, Enum):
    """Allowed graph delta operations."""

    ADD_EDGE = "add_edge"
    UPDATE_EDGE = "update_edge"


class GraphRelationType(str, Enum):
    """Allowed announcement-derived graph relations."""

    CONTROL = "control"
    SHAREHOLDING = "shareholding"
    SUPPLY_CONTRACT = "supply_contract"
    COOPERATION = "cooperation"


class AnnouncementGraphDeltaCandidate(BaseModel):
    """Contract-ready Ex-3 graph delta candidate."""

    model_config = ConfigDict(extra="forbid")

    ex_type: Literal["Ex-3"] = "Ex-3"
    delta_id: str = Field(min_length=1)
    announcement_id: str = Field(min_length=1)
    delta_type: GraphDeltaType
    source_node: str = Field(min_length=1)
    target_node: str = Field(min_length=1)
    relation_type: GraphRelationType
    properties: dict[str, Any] = Field(default_factory=dict)
    source_fact_ids: list[str] = Field(min_length=1)
    source_reference: dict[str, Any]
    evidence_spans: list[EvidenceSpan] = Field(min_length=2)
    confidence: float = Field(ge=0.0, le=1.0)
    generated_at: datetime

    @field_validator("generated_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        """Reject naive generation timestamps."""

        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("generated_at must include timezone information")
        return value

    @field_validator("source_node", "target_node")
    @classmethod
    def require_non_blank_node(cls, value: str) -> str:
        """Reject blank graph endpoints."""

        if not value.strip():
            raise ValueError("graph node references must be non-empty strings")
        return value

    @field_validator("source_fact_ids")
    @classmethod
    def require_non_empty_fact_ids(cls, value: list[str]) -> list[str]:
        """Reject blank source fact references."""

        if any(not item.strip() for item in value):
            raise ValueError("source_fact_ids must contain non-empty strings")
        return value

    @model_validator(mode="after")
    def validate_source_reference(self) -> "AnnouncementGraphDeltaCandidate":
        """Require official source provenance and reject runtime metadata."""

        official_url = self.source_reference.get("official_url")
        if not isinstance(official_url, str) or not official_url.strip():
            raise ValueError("source_reference.official_url is required")
        _reject_forbidden_keys(self.model_dump(mode="python"))
        return self

    def to_ex_payload(self) -> dict[str, Any]:
        """Return the Ex-3 payload for subsystem-sdk submission."""

        payload = self.model_dump(mode="json")
        _reject_forbidden_keys(payload)
        return payload


def make_delta_id(
    announcement_id: str,
    relation_type: GraphRelationType,
    source_node: str,
    target_node: str,
    source_fact_ids: Sequence[str],
    evidence_spans: Sequence[EvidenceSpan],
    properties: Mapping[str, Any],
) -> str:
    """Generate a deterministic graph delta id for dedupe/replay."""

    identity_payload = {
        "announcement_id": announcement_id,
        "relation_type": relation_type.value,
        "source_node": source_node,
        "target_node": target_node,
        "source_fact_ids": list(source_fact_ids),
        "evidence_spans": [
            _stable_jsonable(span.model_dump(mode="python"))
            for span in evidence_spans
        ],
        "properties": _stable_jsonable(properties),
    }
    digest = hashlib.sha256(
        json.dumps(
            identity_payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()[:24]
    return f"graph_delta:{announcement_id}:{relation_type.value}:{digest}"


def _stable_jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): _stable_jsonable(item)
            for key, item in sorted(value.items(), key=lambda item: str(item[0]))
        }
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        return [_stable_jsonable(item) for item in value]
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    return value


def _reject_forbidden_keys(value: Any, path: str = "") -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            key_text = str(key)
            if key_text in FORBIDDEN_PAYLOAD_KEYS:
                raise ValueError(f"Ex-3 payload contains forbidden key: {path}{key_text}")
            _reject_forbidden_keys(item, f"{path}{key_text}.")
    elif isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        for index, item in enumerate(value):
            _reject_forbidden_keys(item, f"{path}{index}.")
