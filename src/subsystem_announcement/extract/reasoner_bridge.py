"""Bridge to reasoner-runtime for difficult structured extraction."""

from __future__ import annotations

import importlib
from collections.abc import Mapping, Sequence
from typing import Any, Protocol

from pydantic import BaseModel, ConfigDict, Field


class StructuredExtractionSegment(BaseModel):
    """Bounded text/table segment sent to reasoner-runtime."""

    model_config = ConfigDict(extra="forbid")

    segment_id: str = Field(min_length=1)
    section_id: str = Field(min_length=1)
    text: str = Field(min_length=1, max_length=2_000)
    table_ref: str | None = None


class StructuredExtractionRequest(BaseModel):
    """Reasoner request that avoids sending unbounded full documents."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    announcement_id: str = Field(min_length=1)
    fact_type: str = Field(min_length=1)
    segments: list[StructuredExtractionSegment] = Field(min_length=1, max_length=5)
    extraction_schema: dict[str, Any] = Field(alias="schema")

    @property
    def schema(self) -> dict[str, Any]:
        """Expose the request schema under the contract-facing name."""

        return self.extraction_schema


class StructuredReasoner(Protocol):
    """Narrow structured generation surface used by this subsystem."""

    def generate_structured(
        self,
        request: StructuredExtractionRequest,
    ) -> Mapping[str, Any]:
        """Generate structured extraction output."""


class ReasonerRuntimeBridge:
    """Adapter around ``reasoner_runtime.generate_structured``."""

    def __init__(self, *, endpoint: str | None = None) -> None:
        self.endpoint = endpoint

    def generate_structured(
        self,
        request: StructuredExtractionRequest,
    ) -> Mapping[str, Any]:
        """Call reasoner-runtime without importing provider SDKs."""

        reasoner_runtime = importlib.import_module("reasoner_runtime")
        payload = request.model_dump(mode="json", by_alias=True)
        if self.endpoint is not None:
            payload["endpoint"] = self.endpoint
        result = reasoner_runtime.generate_structured(payload)
        if not isinstance(result, Mapping):
            raise TypeError("reasoner_runtime.generate_structured returned non-mapping")
        return result


def ex1_reasoner_schema(fact_type: str) -> dict[str, Any]:
    """Return the minimal schema requested from the reasoner."""

    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "facts": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": True,
                    "required": ["quote", "fact_content"],
                    "properties": {
                        "quote": {"type": "string"},
                        "fact_content": {"type": "object"},
                        "confidence": {"type": "number"},
                        "related_mentions": {
                            "type": "array",
                            "items": {"type": "string"},
                        },
                    },
                },
            }
        },
        "required": ["facts"],
        "x-fact-type": fact_type,
    }


def bounded_segments(
    segments: Sequence[tuple[str, str, str, str | None]],
) -> list[StructuredExtractionSegment]:
    """Coerce raw segment tuples into the bounded request model."""

    bounded: list[StructuredExtractionSegment] = []
    for segment_id, section_id, text, table_ref in segments[:5]:
        stripped = text.strip()
        if not stripped:
            continue
        bounded.append(
            StructuredExtractionSegment(
                segment_id=segment_id,
                section_id=section_id,
                text=stripped[:2_000],
                table_ref=table_ref,
            )
        )
    return bounded
