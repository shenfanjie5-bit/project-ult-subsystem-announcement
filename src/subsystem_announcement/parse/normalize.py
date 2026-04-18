"""Normalize Docling conversion results into parsed announcement artifacts."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import datetime, timezone

from subsystem_announcement.discovery.document import AnnouncementDocumentArtifact

from .artifact import (
    AnnouncementSection,
    AnnouncementTable,
    ParsedAnnouncementArtifact,
)
from .errors import ParseNormalizationError


def normalize_docling_result(
    raw_result: object,
    document_ref: AnnouncementDocumentArtifact,
    parser_version: str,
    parser_core_version: str = "not-configured",
) -> ParsedAnnouncementArtifact:
    """Convert a Docling result object into the persisted artifact schema."""

    try:
        return _normalize_docling_result(
            raw_result,
            document_ref,
            parser_version,
            parser_core_version,
        )
    except ParseNormalizationError:
        raise
    except Exception as exc:
        raise ParseNormalizationError(
            "Unable to normalize Docling result: "
            f"announcement_id={document_ref.announcement_id}"
        ) from exc


def _normalize_docling_result(
    raw_result: object,
    document_ref: AnnouncementDocumentArtifact,
    parser_version: str,
    parser_core_version: str,
) -> ParsedAnnouncementArtifact:
    document = _get_value(raw_result, "document") or raw_result
    section_inputs = list(_iter_section_inputs(document))
    table_inputs = list(_iter_table_inputs(document))

    section_blocks = _build_section_blocks(section_inputs, document)
    if not section_blocks:
        text = _export_text(document) or _export_text(raw_result)
        if text:
            section_blocks = [
                {
                    "title": _optional_str(_get_value(document, "name")),
                    "level": 0,
                    "text": text,
                    "parent_id": None,
                }
            ]

    if not section_blocks:
        raise ParseNormalizationError(
            "Docling result did not contain extractable text: "
            f"announcement_id={document_ref.announcement_id}"
        )

    pieces: list[str] = []
    sections: list[AnnouncementSection] = []
    title_hierarchy: list[str] = []
    parent_by_level: dict[int, str] = {}

    for index, block in enumerate(section_blocks, start=1):
        if pieces:
            pieces.append("\n\n")
        start_offset = sum(len(piece) for piece in pieces)
        text = _clean_text(block["text"])
        pieces.append(text)
        end_offset = sum(len(piece) for piece in pieces)

        section_id = f"sec-{index:04d}"
        level = max(0, int(block.get("level") or 0))
        parent_id = _optional_str(block.get("parent_id"))
        if parent_id is None and level > 0:
            parent_id = parent_by_level.get(level - 1)
        if level > 0:
            parent_by_level[level] = section_id
            for child_level in [key for key in parent_by_level if key > level]:
                del parent_by_level[child_level]

        title = _optional_str(block.get("title"))
        if title:
            title_hierarchy.append(title)

        sections.append(
            AnnouncementSection(
                section_id=section_id,
                title=title,
                level=level,
                text=text,
                start_offset=start_offset,
                end_offset=end_offset,
                parent_id=parent_id,
            )
        )

    tables: list[AnnouncementTable] = []
    fallback_section_id = sections[0].section_id
    section_ids = {section.section_id for section in sections}
    for index, table_input in enumerate(table_inputs, start=1):
        table_block = _table_block(table_input)
        if table_block is None:
            continue
        table_text = _render_table_text(table_block)
        if not table_text:
            continue
        if pieces:
            pieces.append("\n\n")
        start_offset = sum(len(piece) for piece in pieces)
        pieces.append(table_text)
        end_offset = sum(len(piece) for piece in pieces)

        section_id = _optional_str(table_block.get("section_id")) or fallback_section_id
        if section_id not in section_ids:
            section_id = fallback_section_id
        tables.append(
            AnnouncementTable(
                table_id=f"tbl-{index:04d}",
                section_id=section_id,
                caption=_optional_str(table_block.get("caption")),
                headers=list(table_block["headers"]),
                rows=[list(row) for row in table_block["rows"]],
                start_offset=start_offset,
                end_offset=end_offset,
            )
        )

    extracted_text = "".join(pieces).strip()
    if not extracted_text:
        raise ParseNormalizationError(
            "Normalized Docling text is empty: "
            f"announcement_id={document_ref.announcement_id}"
        )

    return ParsedAnnouncementArtifact(
        announcement_id=document_ref.announcement_id,
        content_hash=document_ref.content_hash,
        parser_version=parser_version,
        parser_core_version=parser_core_version,
        title_hierarchy=title_hierarchy,
        sections=sections,
        tables=tables,
        extracted_text=extracted_text,
        parsed_at=datetime.now(timezone.utc),
        source_document=document_ref,
    )


def _iter_section_inputs(document: object) -> Iterable[object]:
    for key in ("sections", "texts"):
        values = _get_value(document, key)
        if _is_sequence(values):
            yield from values


def _iter_table_inputs(document: object) -> Iterable[object]:
    values = _get_value(document, "tables")
    if _is_sequence(values):
        yield from values


def _build_section_blocks(
    section_inputs: list[object],
    document: object,
) -> list[dict[str, object]]:
    blocks: list[dict[str, object]] = []
    for item in section_inputs:
        text = _clean_text(_first_text(item, ("text", "content", "orig")))
        if not text:
            continue
        title = _optional_str(_first_text(item, ("title", "heading", "name")))
        level = _level_for_item(item)
        blocks.append(
            {
                "title": title,
                "level": level,
                "text": text,
                "parent_id": _optional_str(_get_value(item, "parent_id")),
            }
        )
    if blocks:
        return blocks

    exported = _export_text(document)
    return _sections_from_markdown(exported) if exported else []


def _sections_from_markdown(text: str) -> list[dict[str, object]]:
    blocks: list[dict[str, object]] = []
    current_title: str | None = None
    current_level = 0
    current_lines: list[str] = []

    def flush() -> None:
        cleaned = _clean_text("\n".join(current_lines))
        if cleaned:
            blocks.append(
                {
                    "title": current_title,
                    "level": current_level,
                    "text": cleaned,
                    "parent_id": None,
                }
            )

    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("#"):
            marker = stripped.split(" ", 1)[0]
            if marker and set(marker) == {"#"}:
                flush()
                current_level = len(marker)
                current_title = stripped[len(marker) :].strip() or None
                current_lines = []
                continue
        current_lines.append(line)
    flush()
    if blocks:
        return blocks
    cleaned = _clean_text(text)
    if not cleaned:
        return []
    return [{"title": None, "level": 0, "text": cleaned, "parent_id": None}]


def _table_block(table: object) -> dict[str, object] | None:
    headers = _string_list(_get_value(table, "headers"))
    rows = _string_rows(_get_value(table, "rows"))

    if not headers and not rows:
        dataframe = _call_no_arg(table, "export_to_dataframe")
        if dataframe is not None:
            headers = _dataframe_headers(dataframe)
            rows = _dataframe_rows(dataframe)

    if not headers and not rows:
        return None

    return {
        "caption": _optional_str(_first_text(table, ("caption", "title", "name"))),
        "section_id": _optional_str(_get_value(table, "section_id")),
        "headers": headers,
        "rows": rows,
    }


def _render_table_text(table: Mapping[str, object]) -> str:
    lines: list[str] = []
    caption = _optional_str(table.get("caption"))
    if caption:
        lines.append(caption)
    headers = _string_list(table.get("headers"))
    if headers:
        lines.append("\t".join(headers))
    rows = _string_rows(table.get("rows"))
    lines.extend("\t".join(row) for row in rows)
    return _clean_text("\n".join(lines))


def _export_text(value: object) -> str:
    for method_name in ("export_to_markdown", "export_to_text"):
        exported = _call_no_arg(value, method_name)
        if isinstance(exported, str):
            return _clean_text(exported)
    text = _first_text(value, ("text", "content", "body"))
    return _clean_text(text)


def _first_text(value: object, keys: tuple[str, ...]) -> str:
    for key in keys:
        item = _get_value(value, key)
        if isinstance(item, str):
            return item
    return ""


def _level_for_item(value: object) -> int:
    level = _get_value(value, "level")
    if isinstance(level, int):
        return max(0, level)
    label = _optional_str(_get_value(value, "label"))
    if label:
        lowered = label.lower()
        if "section_header" in lowered or "heading" in lowered:
            return 1
    return 0


def _get_value(value: object, key: str) -> object | None:
    if isinstance(value, Mapping):
        return value.get(key)
    return getattr(value, key, None)


def _call_no_arg(value: object, method_name: str) -> object | None:
    method = getattr(value, method_name, None)
    if callable(method):
        return method()
    return None


def _dataframe_headers(dataframe: object) -> list[str]:
    columns = getattr(dataframe, "columns", None)
    if columns is None:
        return []
    return [str(column) for column in columns]


def _dataframe_rows(dataframe: object) -> list[list[str]]:
    values = getattr(dataframe, "values", None)
    if values is None:
        return []
    tolist = getattr(values, "tolist", None)
    raw_rows = tolist() if callable(tolist) else values
    return _string_rows(raw_rows)


def _is_sequence(value: object) -> bool:
    return isinstance(value, list | tuple)


def _string_list(value: object) -> list[str]:
    if not _is_sequence(value):
        return []
    return [_clean_cell(item) for item in value]


def _string_rows(value: object) -> list[list[str]]:
    if not _is_sequence(value):
        return []
    rows: list[list[str]] = []
    for row in value:
        if _is_sequence(row):
            rows.append([_clean_cell(cell) for cell in row])
        else:
            rows.append([_clean_cell(row)])
    return rows


def _clean_cell(value: object) -> str:
    return " ".join(str(value).split())


def _optional_str(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def _clean_text(value: object) -> str:
    if not isinstance(value, str):
        return ""
    lines = [" ".join(line.split()) for line in value.splitlines()]
    return "\n".join(line for line in lines if line).strip()
