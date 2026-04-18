"""Chunk parsed announcement artifacts for offline retrieval."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator, Mapping, Sequence
from datetime import datetime
from enum import Enum
from typing import Any, Literal

from subsystem_announcement.extract.evidence import render_table_text
from subsystem_announcement.parse.artifact import (
    AnnouncementSection,
    AnnouncementTable,
    ParsedAnnouncementArtifact,
)

from .retrieval_artifact import AnnouncementChunk


ChunkType = Literal["section", "table", "clause"]


def chunk_parsed_artifact(
    parsed_artifact: ParsedAnnouncementArtifact,
    *,
    max_chars: int = 1800,
    overlap_chars: int = 120,
) -> list[AnnouncementChunk]:
    """Build stable section/table retrieval chunks from a parse artifact."""

    if max_chars <= 0:
        raise ValueError("max_chars must be greater than 0")
    if overlap_chars < 0:
        raise ValueError("overlap_chars must be greater than or equal to 0")
    if overlap_chars >= max_chars:
        raise ValueError("overlap_chars must be smaller than max_chars")

    source_reference = build_source_reference(parsed_artifact)
    sections_by_id = {section.section_id: section for section in parsed_artifact.sections}
    chunks: list[AnnouncementChunk] = []

    for section in parsed_artifact.sections:
        chunks.extend(
            _chunk_section(
                parsed_artifact,
                section,
                sections_by_id,
                source_reference=source_reference,
                max_chars=max_chars,
                overlap_chars=overlap_chars,
            )
        )

    for table in parsed_artifact.tables:
        chunks.extend(
            _chunk_table(
                parsed_artifact,
                table,
                sections_by_id,
                source_reference=source_reference,
                max_chars=max_chars,
                overlap_chars=overlap_chars,
            )
        )

    return chunks


def make_chunk_id(
    announcement_id: str,
    section_id: str,
    chunk_type: ChunkType,
    start_offset: int,
    end_offset: int,
    text: str,
) -> str:
    """Generate a deterministic chunk id for replay and index rebuilds."""

    payload = {
        "announcement_id": announcement_id,
        "section_id": section_id,
        "chunk_type": chunk_type,
        "start_offset": start_offset,
        "end_offset": end_offset,
        "text": text,
    }
    digest = hashlib.sha256(
        json.dumps(
            _stable_jsonable(payload),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()[:24]
    return f"chunk:{announcement_id}:{section_id}:{chunk_type}:{digest}"


def build_source_reference(
    parsed_artifact: ParsedAnnouncementArtifact,
) -> dict[str, Any]:
    """Build official-source provenance for retrieval chunks."""

    source_document = parsed_artifact.source_document
    official_url = str(source_document.official_url).strip()
    if not official_url:
        raise ValueError("official source reference is required")

    source_reference: dict[str, Any] = {
        "announcement_id": parsed_artifact.announcement_id,
        "official_url": official_url,
        "source_exchange": source_document.source_exchange,
        "attachment_type": source_document.attachment_type,
        "content_hash": parsed_artifact.content_hash,
        "parser_version": parsed_artifact.parser_version,
    }
    if source_document.ts_code is not None:
        source_reference["ts_code"] = source_document.ts_code
    if source_document.title is not None:
        source_reference["title"] = source_document.title
    if source_document.publish_time is not None:
        source_reference["publish_time"] = source_document.publish_time.isoformat()
    return source_reference


def _chunk_section(
    parsed_artifact: ParsedAnnouncementArtifact,
    section: AnnouncementSection,
    sections_by_id: Mapping[str, AnnouncementSection],
    *,
    source_reference: Mapping[str, Any],
    max_chars: int,
    overlap_chars: int,
) -> list[AnnouncementChunk]:
    title_path = _title_path_for_section(parsed_artifact, section, sections_by_id)
    if len(section.text) <= max_chars:
        return [
            _make_chunk(
                parsed_artifact.announcement_id,
                chunk_type="section",
                section_id=section.section_id,
                table_ref=None,
                text=section.text,
                start_offset=section.start_offset,
                end_offset=section.end_offset,
                title_path=title_path,
                source_reference=source_reference,
            )
        ]

    chunks: list[AnnouncementChunk] = []
    for local_start, local_end in _split_text_ranges(
        section.text,
        max_chars=max_chars,
        overlap_chars=overlap_chars,
    ):
        text = section.text[local_start:local_end]
        chunks.append(
            _make_chunk(
                parsed_artifact.announcement_id,
                chunk_type="clause",
                section_id=section.section_id,
                table_ref=None,
                text=text,
                start_offset=section.start_offset + local_start,
                end_offset=section.start_offset + local_end,
                title_path=title_path,
                source_reference=source_reference,
            )
        )
    return chunks


def _chunk_table(
    parsed_artifact: ParsedAnnouncementArtifact,
    table: AnnouncementTable,
    sections_by_id: Mapping[str, AnnouncementSection],
    *,
    source_reference: Mapping[str, Any],
    max_chars: int,
    overlap_chars: int,
) -> list[AnnouncementChunk]:
    text = render_table_text(table)
    if not text.strip():
        return []

    section = sections_by_id.get(table.section_id)
    title_path = (
        _title_path_for_section(parsed_artifact, section, sections_by_id)
        if section is not None
        else list(parsed_artifact.title_hierarchy)
    )
    if table.caption and table.caption not in title_path:
        title_path = [*title_path, table.caption]

    if len(text) <= max_chars:
        return [
            _make_chunk(
                parsed_artifact.announcement_id,
                chunk_type="table",
                section_id=table.section_id,
                table_ref=table.table_id,
                text=text,
                start_offset=table.start_offset,
                end_offset=table.end_offset,
                title_path=title_path,
                source_reference=source_reference,
            )
        ]

    chunks: list[AnnouncementChunk] = []
    for local_start, local_end in _split_text_ranges(
        text,
        max_chars=max_chars,
        overlap_chars=overlap_chars,
    ):
        chunks.append(
            _make_chunk(
                parsed_artifact.announcement_id,
                chunk_type="table",
                section_id=table.section_id,
                table_ref=table.table_id,
                text=text[local_start:local_end],
                start_offset=table.start_offset + local_start,
                end_offset=table.start_offset + local_end,
                title_path=title_path,
                source_reference=source_reference,
            )
        )
    return chunks


def _make_chunk(
    announcement_id: str,
    *,
    chunk_type: ChunkType,
    section_id: str,
    table_ref: str | None,
    text: str,
    start_offset: int,
    end_offset: int,
    title_path: Sequence[str],
    source_reference: Mapping[str, Any],
) -> AnnouncementChunk:
    chunk_id = make_chunk_id(
        announcement_id,
        section_id,
        chunk_type,
        start_offset,
        end_offset,
        text,
    )
    return AnnouncementChunk(
        chunk_id=chunk_id,
        announcement_id=announcement_id,
        chunk_type=chunk_type,
        section_id=section_id,
        table_ref=table_ref,
        text=text,
        start_offset=start_offset,
        end_offset=end_offset,
        title_path=list(title_path),
        source_reference=dict(source_reference),
    )


def _split_text_ranges(
    text: str,
    *,
    max_chars: int,
    overlap_chars: int,
) -> Iterator[tuple[int, int]]:
    start = 0
    text_length = len(text)
    while start < text_length:
        end = min(text_length, start + max_chars)
        if end < text_length:
            end = _choose_split_end(text, start, end, max_chars=max_chars)
        trimmed_start, trimmed_end = _trim_range(text, start, end)
        if trimmed_end > trimmed_start:
            yield trimmed_start, trimmed_end
        if end >= text_length:
            break
        next_start = max(0, end - overlap_chars)
        if next_start <= start:
            next_start = end
        start = next_start


def _choose_split_end(text: str, start: int, end: int, *, max_chars: int) -> int:
    minimum = start + max(1, max_chars // 2)
    candidates = [
        text.rfind(marker, minimum, end)
        for marker in ("。", "！", "？", "\n", "；", ";", " ")
    ]
    split_at = max(candidates)
    if split_at == -1:
        return end
    return split_at + 1


def _trim_range(text: str, start: int, end: int) -> tuple[int, int]:
    while start < end and text[start].isspace():
        start += 1
    while end > start and text[end - 1].isspace():
        end -= 1
    return start, end


def _title_path_for_section(
    parsed_artifact: ParsedAnnouncementArtifact,
    section: AnnouncementSection,
    sections_by_id: Mapping[str, AnnouncementSection],
) -> list[str]:
    titles = [title for title in parsed_artifact.title_hierarchy if title.strip()]
    ancestors: list[AnnouncementSection] = []
    current = section
    seen = {current.section_id}
    while current.parent_id and current.parent_id in sections_by_id:
        parent = sections_by_id[current.parent_id]
        if parent.section_id in seen:
            break
        ancestors.append(parent)
        seen.add(parent.section_id)
        current = parent

    for ancestor in reversed(ancestors):
        _append_title(titles, ancestor.title)
    _append_title(titles, section.title)
    return titles


def _append_title(titles: list[str], title: str | None) -> None:
    if title is None:
        return
    stripped = title.strip()
    if stripped and (not titles or titles[-1] != stripped):
        titles.append(stripped)


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
