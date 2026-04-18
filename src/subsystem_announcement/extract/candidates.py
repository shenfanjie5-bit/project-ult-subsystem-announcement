"""Ex-1 fact candidate models and construction helpers."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from subsystem_announcement.parse.artifact import ParsedAnnouncementArtifact

from .entity_anchor import EntityAnchor, EntityAnchorer
from .evidence import EvidenceSpan


class FactType(str, Enum):
    """Supported Ex-1 announcement fact types."""

    EARNINGS_PREANNOUNCE = "earnings_preannounce"
    MAJOR_CONTRACT = "major_contract"
    SHAREHOLDER_CHANGE = "shareholder_change"
    EQUITY_PLEDGE = "equity_pledge"
    REGULATORY_ACTION = "regulatory_action"
    TRADING_HALT_RESUME = "trading_halt_resume"
    FUNDRAISING_CHANGE = "fundraising_change"


FORBIDDEN_PAYLOAD_KEYS = {
    "submitted_at",
    "ingest_seq",
    "layer_b_receipt_id",
    "local_path",
}


class AnnouncementFactCandidate(BaseModel):
    """Contract-ready Ex-1 announcement fact candidate."""

    model_config = ConfigDict(extra="forbid")

    ex_type: Literal["Ex-1"] = "Ex-1"
    fact_id: str = Field(min_length=1)
    announcement_id: str = Field(min_length=1)
    fact_type: FactType
    primary_entity_id: str = Field(min_length=1)
    related_entity_ids: list[str] = Field(default_factory=list)
    fact_content: dict[str, Any] = Field(default_factory=dict)
    confidence: float = Field(ge=0.0, le=1.0)
    source_reference: dict[str, Any] = Field(default_factory=dict)
    evidence_spans: list[EvidenceSpan] = Field(min_length=1)
    extracted_at: datetime

    @field_validator("extracted_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        """Reject naive extraction timestamps."""

        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("extracted_at must include timezone information")
        return value

    @model_validator(mode="after")
    def validate_source_reference(self) -> "AnnouncementFactCandidate":
        """Require official source provenance without local ingest metadata."""

        official_url = self.source_reference.get("official_url")
        if not isinstance(official_url, str) or not official_url.strip():
            raise ValueError("source_reference.official_url is required")
        _reject_forbidden_keys(self.model_dump(mode="python"))
        return self

    def to_ex_payload(self) -> dict[str, Any]:
        """Return the Ex-1 payload for subsystem-sdk submission."""

        payload = self.model_dump(mode="json")
        _reject_forbidden_keys(payload)
        return payload


class ExtractionContext(BaseModel):
    """Shared state used by deterministic and reasoner extractors."""

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")

    primary_entity: EntityAnchor
    source_reference: dict[str, Any]
    entity_anchorer: EntityAnchorer = Field(exclude=True)
    entity_registry: Any = Field(default=None, exclude=True)
    reasoner: Any = Field(default=None, exclude=True)


def build_extraction_context(
    parsed_artifact: ParsedAnnouncementArtifact,
    *,
    entity_registry: Any = None,
    reasoner: Any = None,
) -> ExtractionContext:
    """Build shared extraction context from a parsed artifact."""

    anchorer = EntityAnchorer(entity_registry)
    source_reference = build_source_reference(parsed_artifact)
    return ExtractionContext(
        primary_entity=anchorer.anchor_primary_entity(parsed_artifact),
        source_reference=source_reference,
        entity_anchorer=anchorer,
        entity_registry=entity_registry,
        reasoner=reasoner,
    )


def build_source_reference(
    parsed_artifact: ParsedAnnouncementArtifact,
) -> dict[str, Any]:
    """Build official-source reference without local filesystem metadata."""

    official_url = str(parsed_artifact.source_document.official_url).strip()
    if not official_url:
        raise ValueError("official source reference is required")
    return {
        "announcement_id": parsed_artifact.announcement_id,
        "official_url": official_url,
        "source_exchange": parsed_artifact.source_document.source_exchange,
        "attachment_type": parsed_artifact.source_document.attachment_type,
        "content_hash": parsed_artifact.content_hash,
        "parser_version": parsed_artifact.parser_version,
    }


def build_fact_candidate(
    parsed_artifact: ParsedAnnouncementArtifact,
    context: ExtractionContext,
    *,
    fact_type: FactType,
    fact_content: Mapping[str, Any],
    evidence_spans: Sequence[EvidenceSpan],
    related_entities: Sequence[EntityAnchor] = (),
    confidence: float,
    extracted_at: datetime | None = None,
) -> AnnouncementFactCandidate:
    """Create a fact candidate with deterministic identity."""

    related_entity_ids = [entity.identifier for entity in related_entities]
    content = dict(fact_content)
    content.setdefault("primary_entity", context.primary_entity.to_payload())
    if related_entities:
        content.setdefault(
            "related_entities",
            [entity.to_payload() for entity in related_entities],
        )
    fact_id = make_fact_id(
        parsed_artifact.announcement_id,
        fact_type,
        evidence_spans,
        content,
    )
    return AnnouncementFactCandidate(
        fact_id=fact_id,
        announcement_id=parsed_artifact.announcement_id,
        fact_type=fact_type,
        primary_entity_id=context.primary_entity.identifier,
        related_entity_ids=related_entity_ids,
        fact_content=content,
        confidence=confidence,
        source_reference=context.source_reference,
        evidence_spans=list(evidence_spans),
        extracted_at=extracted_at or datetime.now(timezone.utc),
    )


def make_fact_id(
    announcement_id: str,
    fact_type: FactType,
    evidence_spans: Sequence[EvidenceSpan],
    fact_content: Mapping[str, Any],
) -> str:
    """Generate a deterministic fact id for dedupe/replay."""

    evidence_quotes = [
        " ".join(span.quote.split()) for span in evidence_spans if span.quote.strip()
    ]
    payload = {
        "announcement_id": announcement_id,
        "fact_type": fact_type.value,
        "evidence_quotes": evidence_quotes,
        "fact_content": _stable_jsonable(fact_content),
    }
    digest = hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()[:24]
    return f"fact:{announcement_id}:{fact_type.value}:{digest}"


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
                raise ValueError(f"Ex-1 payload contains forbidden key: {path}{key_text}")
            _reject_forbidden_keys(item, f"{path}{key_text}.")
    elif isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        for index, item in enumerate(value):
            _reject_forbidden_keys(item, f"{path}{index}.")
