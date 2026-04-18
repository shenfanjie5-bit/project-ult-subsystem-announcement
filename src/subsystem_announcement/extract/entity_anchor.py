"""Entity anchoring coordination for announcement extraction."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping, Sequence
from typing import Any, Protocol

from pydantic import BaseModel, ConfigDict, Field

from subsystem_announcement.parse.artifact import ParsedAnnouncementArtifact


class EntityMention(BaseModel):
    """A raw entity mention found in announcement text."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1)
    role: str | None = None


class EntityResolution(BaseModel):
    """Resolution returned by an entity registry client."""

    model_config = ConfigDict(extra="forbid")

    mention: EntityMention
    entity_id: str | None = None
    entity_name: str | None = None
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    unresolved_ref: str | None = None
    resolution_method: str = "entity_registry"


class EntityAnchor(BaseModel):
    """Entity anchor attached to an extraction candidate."""

    model_config = ConfigDict(extra="forbid")

    mention_text: str = Field(min_length=1)
    entity_id: str | None = None
    entity_name: str | None = None
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    resolution_method: str
    unresolved_ref: str | None = None

    @property
    def identifier(self) -> str:
        """Return a resolved id or an explicit unresolved reference."""

        if self.entity_id:
            return self.entity_id
        if self.unresolved_ref:
            return self.unresolved_ref
        return _unresolved_ref(self.mention_text)

    def to_payload(self) -> dict[str, Any]:
        """Serialize anchor details without inventing missing ids."""

        payload: dict[str, Any] = {
            "mention_text": self.mention_text,
            "identifier": self.identifier,
            "resolution_method": self.resolution_method,
            "confidence": self.confidence,
        }
        if self.entity_id is not None:
            payload["entity_id"] = self.entity_id
        if self.entity_name is not None:
            payload["entity_name"] = self.entity_name
        if self.unresolved_ref is not None:
            payload["unresolved_ref"] = self.unresolved_ref
        return payload


class EntityRegistryClient(Protocol):
    """Entity registry surface used by extraction code."""

    def lookup_alias(self, name: str) -> EntityResolution | Mapping[str, Any] | None:
        """Resolve a deterministic alias quickly."""

    def resolve_mentions(
        self,
        mentions: Sequence[EntityMention],
    ) -> Sequence[EntityResolution | Mapping[str, Any]]:
        """Resolve fuzzy mentions in batch."""


class EntityAnchorer:
    """Resolve entities in the required deterministic-to-fuzzy order."""

    def __init__(self, registry: EntityRegistryClient | None = None) -> None:
        self._registry = registry

    def anchor_primary_entity(
        self,
        parsed_artifact: ParsedAnnouncementArtifact,
    ) -> EntityAnchor:
        """Anchor the announcing company from code/short-name before registry use."""

        envelope_ts_code = _optional_str(parsed_artifact.source_document.ts_code)
        if envelope_ts_code is not None:
            normalized_code = _normalize_ts_code(
                envelope_ts_code,
                parsed_artifact.source_document.source_exchange,
            )
            return EntityAnchor(
                mention_text=normalized_code,
                entity_id=f"ts_code:{normalized_code}",
                confidence=1.0,
                resolution_method="ts_code",
            )

        text = _artifact_body_text(parsed_artifact)
        ts_code = _extract_ts_code(text)
        short_name = _extract_short_name(text)
        if ts_code is not None:
            normalized_code = _normalize_ts_code(
                ts_code,
                parsed_artifact.source_document.source_exchange,
            )
            return EntityAnchor(
                mention_text=short_name or normalized_code,
                entity_id=f"ts_code:{normalized_code}",
                entity_name=short_name,
                confidence=1.0,
                resolution_method="ts_code",
            )

        if short_name is not None:
            return EntityAnchor(
                mention_text=short_name,
                confidence=0.8,
                resolution_method="company_short_name",
                unresolved_ref=_unresolved_ref(f"company_short_name:{short_name}"),
            )

        mention = _primary_name_candidate(parsed_artifact)
        if mention is not None and self._registry is not None:
            alias_anchor = self._lookup_alias_anchor(mention)
            if alias_anchor.entity_id is not None:
                return alias_anchor

            fuzzy_anchors = self.resolve_related_mentions(
                [EntityMention(name=mention.name, role="primary_entity")]
            )
            if fuzzy_anchors:
                return fuzzy_anchors[0]

        if mention is None:
            mention = EntityMention(
                name=parsed_artifact.announcement_id,
                role="primary_entity",
            )
        return _unresolved_anchor(mention, method="unresolved_primary")

    def resolve_related_mentions(
        self,
        mentions: Sequence[EntityMention | str],
    ) -> list[EntityAnchor]:
        """Resolve related mentions with alias lookup before fuzzy resolution."""

        normalized_mentions = [_coerce_mention(mention) for mention in mentions]
        if not normalized_mentions:
            return []
        if self._registry is None:
            return [
                _unresolved_anchor(mention, method="unresolved_related")
                for mention in normalized_mentions
            ]

        anchors_by_name: dict[str, EntityAnchor] = {}
        unresolved_mentions: list[EntityMention] = []
        for mention in normalized_mentions:
            anchor = self._lookup_alias_anchor(mention)
            if anchor.entity_id is None:
                unresolved_mentions.append(mention)
            else:
                anchors_by_name[mention.name] = anchor

        if unresolved_mentions:
            fuzzy_results = self._registry.resolve_mentions(unresolved_mentions)
            for mention, raw_result in zip(unresolved_mentions, fuzzy_results, strict=False):
                resolution = _coerce_resolution(raw_result, mention)
                anchor = _anchor_from_resolution(resolution)
                if anchor.entity_id is None:
                    anchor = _unresolved_anchor(mention, method="unresolved_related")
                anchors_by_name[mention.name] = anchor

        return [
            anchors_by_name.get(mention.name)
            or _unresolved_anchor(mention, method="unresolved_related")
            for mention in normalized_mentions
        ]

    def _lookup_alias_anchor(self, mention: EntityMention) -> EntityAnchor:
        if self._registry is None:
            return _unresolved_anchor(mention, method="unresolved_related")
        raw_result = self._registry.lookup_alias(mention.name)
        resolution = _coerce_resolution(raw_result, mention)
        if resolution is None:
            return _unresolved_anchor(mention, method="lookup_alias")
        return _anchor_from_resolution(resolution)


def _anchor_from_resolution(resolution: EntityResolution) -> EntityAnchor:
    return EntityAnchor(
        mention_text=resolution.mention.name,
        entity_id=resolution.entity_id,
        entity_name=resolution.entity_name,
        confidence=resolution.confidence,
        resolution_method=resolution.resolution_method,
        unresolved_ref=resolution.unresolved_ref
        or (None if resolution.entity_id else _unresolved_ref(resolution.mention.name)),
    )


def _coerce_resolution(
    raw_result: EntityResolution | Mapping[str, Any] | None,
    mention: EntityMention,
) -> EntityResolution | None:
    if raw_result is None:
        return None
    if isinstance(raw_result, EntityResolution):
        return raw_result
    raw = dict(raw_result)
    raw_mention = raw.get("mention")
    if isinstance(raw_mention, EntityMention):
        coerced_mention = raw_mention
    elif isinstance(raw_mention, Mapping):
        coerced_mention = EntityMention.model_validate(raw_mention)
    elif isinstance(raw_mention, str):
        coerced_mention = EntityMention(name=raw_mention, role=mention.role)
    else:
        coerced_mention = mention
    return EntityResolution(
        mention=coerced_mention,
        entity_id=_optional_str(raw.get("entity_id")),
        entity_name=_optional_str(raw.get("entity_name")),
        confidence=float(raw.get("confidence") or 0.0),
        unresolved_ref=_optional_str(raw.get("unresolved_ref")),
        resolution_method=_optional_str(raw.get("resolution_method"))
        or "entity_registry",
    )


def _coerce_mention(mention: EntityMention | str) -> EntityMention:
    if isinstance(mention, EntityMention):
        return mention
    return EntityMention(name=mention)


def _unresolved_anchor(mention: EntityMention, *, method: str) -> EntityAnchor:
    return EntityAnchor(
        mention_text=mention.name,
        confidence=0.0,
        resolution_method=method,
        unresolved_ref=_unresolved_ref(mention.name),
    )


def _extract_ts_code(text: str) -> str | None:
    match = re.search(
        r"(?:证券代码|股票代码|A股代码)\s*[:：]\s*(?P<code>\d{6}(?:\.(?:SH|SZ|BJ))?)",
        text,
        re.IGNORECASE,
    )
    return match.group("code") if match else None


def _extract_short_name(text: str) -> str | None:
    match = re.search(
        r"(?:证券简称|股票简称|公司简称)\s*[:：]\s*(?P<name>[^\s，,；;。]{2,20})",
        text,
    )
    return match.group("name") if match else None


def _primary_name_candidate(
    parsed_artifact: ParsedAnnouncementArtifact,
) -> EntityMention | None:
    text = _artifact_body_text(parsed_artifact)
    match = re.search(r"(?:公司名称|发行人)\s*[:：]\s*(?P<name>[^\s，,；;。]{2,40})", text)
    if match:
        return EntityMention(name=match.group("name"), role="primary_entity")
    for title in parsed_artifact.title_hierarchy:
        cleaned = re.sub(r"(公告|报告|提示性公告)$", "", title).strip()
        if 2 <= len(cleaned) <= 40:
            return EntityMention(name=cleaned, role="primary_entity")
    return None


def _artifact_body_text(parsed_artifact: ParsedAnnouncementArtifact) -> str:
    return "\n".join(section.text for section in parsed_artifact.sections)


def _normalize_ts_code(code: str, source_exchange: str) -> str:
    code = code.upper()
    if "." in code:
        return code
    exchange = source_exchange.lower()
    suffix = {
        "sse": "SH",
        "sh": "SH",
        "szse": "SZ",
        "sz": "SZ",
        "bse": "BJ",
        "neeq": "BJ",
    }.get(exchange)
    return f"{code}.{suffix}" if suffix else code


def _unresolved_ref(value: str) -> str:
    digest = hashlib.sha256(" ".join(value.split()).encode("utf-8")).hexdigest()[:12]
    return f"unresolved:{digest}"


def _optional_str(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None
