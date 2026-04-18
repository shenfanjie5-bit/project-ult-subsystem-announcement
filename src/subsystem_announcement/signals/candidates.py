"""Ex-2 signal candidate models and deterministic identity helpers."""

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


class SignalDirection(str, Enum):
    """Directional interpretation of an announcement signal."""

    POSITIVE = "positive"
    NEGATIVE = "negative"
    NEUTRAL = "neutral"


class SignalTimeHorizon(str, Enum):
    """Expected signal horizon used by downstream consumers."""

    IMMEDIATE = "immediate"
    SHORT_TERM = "short_term"
    MEDIUM_TERM = "medium_term"


class AnnouncementSignalCandidate(BaseModel):
    """Contract-ready Ex-2 announcement signal candidate."""

    model_config = ConfigDict(extra="forbid")

    ex_type: Literal["Ex-2"] = "Ex-2"
    signal_id: str = Field(min_length=1)
    announcement_id: str = Field(min_length=1)
    signal_type: str = Field(min_length=1)
    direction: SignalDirection
    magnitude: float = Field(ge=0.0, le=1.0)
    affected_entities: list[str] = Field(min_length=1)
    time_horizon: SignalTimeHorizon
    source_fact_ids: list[str] = Field(min_length=1)
    source_reference: dict[str, Any] = Field(default_factory=dict)
    evidence_spans: list[EvidenceSpan] = Field(min_length=1)
    confidence: float = Field(ge=0.0, le=1.0)
    generated_at: datetime

    @field_validator("generated_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        """Reject naive generation timestamps."""

        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("generated_at must include timezone information")
        return value

    @field_validator("affected_entities", "source_fact_ids")
    @classmethod
    def require_non_empty_items(cls, value: list[str]) -> list[str]:
        """Reject blank entity and source fact references."""

        if any(not item.strip() for item in value):
            raise ValueError("references must be non-empty strings")
        return value

    @model_validator(mode="after")
    def validate_source_reference(self) -> "AnnouncementSignalCandidate":
        """Require official source provenance and reject runtime metadata."""

        official_url = self.source_reference.get("official_url")
        if not isinstance(official_url, str) or not official_url.strip():
            raise ValueError("source_reference.official_url is required")
        _reject_forbidden_keys(self.model_dump(mode="python"))
        return self

    def to_ex_payload(self) -> dict[str, Any]:
        """Return the Ex-2 payload for subsystem-sdk submission."""

        payload = self.model_dump(mode="json")
        _reject_forbidden_keys(payload)
        return payload


def make_signal_id(
    announcement_id: str,
    signal_type: str,
    source_fact_ids: Sequence[str],
    evidence_spans: Sequence[EvidenceSpan],
    payload: Mapping[str, Any],
) -> str:
    """Generate a deterministic signal id for replay/dedupe."""

    evidence_quotes = [
        " ".join(span.quote.split()) for span in evidence_spans if span.quote.strip()
    ]
    identity_payload = {
        "announcement_id": announcement_id,
        "signal_type": signal_type,
        "source_fact_ids": list(source_fact_ids),
        "evidence_quotes": evidence_quotes,
        "payload": _stable_jsonable(payload),
    }
    digest = hashlib.sha256(
        json.dumps(
            identity_payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()[:24]
    return f"signal:{announcement_id}:{signal_type}:{digest}"


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
                raise ValueError(f"Ex-2 payload contains forbidden key: {path}{key_text}")
            _reject_forbidden_keys(item, f"{path}{key_text}.")
    elif isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        for index, item in enumerate(value):
            _reject_forbidden_keys(item, f"{path}{index}.")
