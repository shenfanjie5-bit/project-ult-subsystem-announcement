from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from subsystem_announcement.discovery.document import AnnouncementDocumentArtifact
from subsystem_announcement.parse.artifact import (
    AnnouncementSection,
    AnnouncementTable,
    ParsedAnnouncementArtifact,
)


def make_artifact(
    text: str,
    *,
    announcement_id: str = "ann-extract-1",
    title: str = "测试公告",
    source_ts_code: str | None = None,
    source_title: str | None = None,
    source_publish_time: datetime | None = None,
    source_exchange: str = "sse",
    tables: list[AnnouncementTable] | None = None,
) -> ParsedAnnouncementArtifact:
    table_text = ""
    if tables:
        rendered_tables = []
        for table in tables:
            lines = []
            if table.caption:
                lines.append(table.caption)
            if table.headers:
                lines.append("\t".join(table.headers))
            lines.extend("\t".join(row) for row in table.rows)
            rendered_tables.append("\n".join(lines))
        table_text = "\n\n" + "\n\n".join(rendered_tables)
    extracted_text = text + table_text
    return ParsedAnnouncementArtifact(
        announcement_id=announcement_id,
        content_hash="a" * 64,
        parser_version="docling==2.15.1",
        title_hierarchy=[title],
        sections=[
            AnnouncementSection(
                section_id="sec-0001",
                title=title,
                level=1,
                text=text,
                start_offset=0,
                end_offset=len(text),
                parent_id=None,
            )
        ],
        tables=tables or [],
        extracted_text=extracted_text,
        parsed_at=datetime(2026, 4, 18, 9, 31, tzinfo=timezone.utc),
        source_document=AnnouncementDocumentArtifact(
            announcement_id=announcement_id,
            ts_code=source_ts_code,
            title=source_title,
            publish_time=source_publish_time,
            content_hash="a" * 64,
            official_url=f"https://static.sse.com.cn/disclosure/{announcement_id}.pdf",
            source_exchange=source_exchange,
            attachment_type="pdf",
            local_path=Path("fixtures") / f"{announcement_id}.pdf",
            content_type="application/pdf",
            byte_size=100,
            fetched_at=datetime(2026, 4, 18, 9, 30, tzinfo=timezone.utc),
        ),
    )
