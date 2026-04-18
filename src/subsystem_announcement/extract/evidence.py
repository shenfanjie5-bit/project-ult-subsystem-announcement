"""Evidence span helpers for announcement extraction."""

from __future__ import annotations

import re
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Pattern

from pydantic import BaseModel, ConfigDict, Field, model_validator

from subsystem_announcement.parse.artifact import (
    AnnouncementSection,
    AnnouncementTable,
    ParsedAnnouncementArtifact,
)


class EvidenceSpan(BaseModel):
    """A reproducible quote inside a parsed announcement section or table."""

    model_config = ConfigDict(extra="forbid")

    section_id: str = Field(min_length=1)
    start_offset: int = Field(ge=0)
    end_offset: int = Field(ge=0)
    quote: str = Field(min_length=1)
    table_ref: str | None = None

    @model_validator(mode="after")
    def validate_offsets_match_quote(self) -> "EvidenceSpan":
        """Keep span offsets and quote length internally consistent."""

        if self.end_offset <= self.start_offset:
            raise ValueError("end_offset must be greater than start_offset")
        if self.end_offset - self.start_offset != len(self.quote):
            raise ValueError("quote length must match end_offset - start_offset")
        if not self.quote.strip():
            raise ValueError("quote must contain non-whitespace evidence")
        return self


@dataclass(frozen=True)
class EvidenceSource:
    """Searchable text source backed by a section or rendered table."""

    section_id: str
    text: str
    table_ref: str | None = None


PatternLike = str | Pattern[str]


def build_evidence_span(
    section: AnnouncementSection,
    start_offset: int,
    end_offset: int,
    *,
    table_ref: str | None = None,
) -> EvidenceSpan:
    """Build an evidence span using offsets relative to ``section.text``."""

    quote = _slice_text(section.text, start_offset, end_offset)
    return EvidenceSpan(
        section_id=section.section_id,
        start_offset=start_offset,
        end_offset=end_offset,
        quote=quote,
        table_ref=table_ref,
    )


def build_table_evidence_span(
    table: AnnouncementTable,
    start_offset: int,
    end_offset: int,
) -> EvidenceSpan:
    """Build an evidence span using offsets relative to rendered table text."""

    text = render_table_text(table)
    quote = _slice_text(text, start_offset, end_offset)
    return EvidenceSpan(
        section_id=table.section_id,
        start_offset=start_offset,
        end_offset=end_offset,
        quote=quote,
        table_ref=table.table_id,
    )


def iter_evidence_sources(
    parsed_artifact: ParsedAnnouncementArtifact,
) -> Iterable[EvidenceSource]:
    """Yield section text and rendered table text for classification/extraction."""

    for section in parsed_artifact.sections:
        yield EvidenceSource(section_id=section.section_id, text=section.text)
    for table in parsed_artifact.tables:
        text = render_table_text(table)
        if text:
            yield EvidenceSource(
                section_id=table.section_id,
                text=text,
                table_ref=table.table_id,
            )


def render_table_text(table: AnnouncementTable) -> str:
    """Render a normalized table the same way parse normalization does."""

    lines: list[str] = []
    if table.caption:
        lines.append(_clean_text(table.caption))
    if table.headers:
        lines.append("\t".join(_clean_cell(header) for header in table.headers))
    for row in table.rows:
        lines.append("\t".join(_clean_cell(cell) for cell in row))
    return _clean_text("\n".join(lines))


def find_evidence_span(
    parsed_artifact: ParsedAnnouncementArtifact,
    patterns: Sequence[PatternLike],
) -> EvidenceSpan | None:
    """Find the first sentence-level evidence span matching any pattern."""

    for source in iter_evidence_sources(parsed_artifact):
        for pattern in patterns:
            match = _search(pattern, source.text)
            if match is None:
                continue
            start_offset, end_offset = _sentence_bounds(
                source.text,
                match.start(),
                match.end(),
            )
            quote = source.text[start_offset:end_offset]
            return EvidenceSpan(
                section_id=source.section_id,
                start_offset=start_offset,
                end_offset=end_offset,
                quote=quote,
                table_ref=source.table_ref,
            )
    return None


def quote_from_artifact(
    parsed_artifact: ParsedAnnouncementArtifact,
    span: EvidenceSpan,
) -> str:
    """Reconstruct a span quote from its parsed artifact source."""

    if span.table_ref is not None:
        for table in parsed_artifact.tables:
            if table.table_id == span.table_ref:
                return render_table_text(table)[span.start_offset : span.end_offset]
        raise ValueError(f"unknown table_ref: {span.table_ref}")

    for section in parsed_artifact.sections:
        if section.section_id == span.section_id:
            return section.text[span.start_offset : span.end_offset]
    raise ValueError(f"unknown section_id: {span.section_id}")


def evidence_matches_artifact(
    parsed_artifact: ParsedAnnouncementArtifact,
    span: EvidenceSpan,
) -> bool:
    """Return whether a span can be reconstructed exactly from the artifact."""

    try:
        return quote_from_artifact(parsed_artifact, span) == span.quote
    except ValueError:
        return False


def _slice_text(text: str, start_offset: int, end_offset: int) -> str:
    if start_offset < 0 or end_offset > len(text) or end_offset <= start_offset:
        raise ValueError("evidence offsets are outside source text")
    return text[start_offset:end_offset]


def _search(pattern: PatternLike, text: str) -> re.Match[str] | None:
    if isinstance(pattern, str):
        return re.search(re.escape(pattern), text)
    return pattern.search(text)


def _sentence_bounds(text: str, start: int, end: int) -> tuple[int, int]:
    sentence_start = max(
        text.rfind("。", 0, start),
        text.rfind("！", 0, start),
        text.rfind("？", 0, start),
        text.rfind("\n", 0, start),
    )
    sentence_start = 0 if sentence_start == -1 else sentence_start + 1

    sentence_end_candidates = [
        index
        for marker in ("。", "！", "？", "\n")
        if (index := text.find(marker, end)) != -1
    ]
    sentence_end = min(sentence_end_candidates) + 1 if sentence_end_candidates else len(text)

    if sentence_end - sentence_start > 320:
        window_start = max(0, start - 120)
        window_end = min(len(text), end + 180)
        return window_start, window_end
    return sentence_start, sentence_end


def _clean_cell(value: object) -> str:
    return " ".join(str(value).split())


def _clean_text(value: str) -> str:
    lines = [" ".join(line.split()) for line in value.splitlines()]
    return "\n".join(line for line in lines if line).strip()
