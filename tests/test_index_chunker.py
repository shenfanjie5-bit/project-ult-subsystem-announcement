from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from subsystem_announcement.discovery.document import AnnouncementDocumentArtifact
from subsystem_announcement.index import chunk_parsed_artifact
from subsystem_announcement.index.chunker import make_chunk_id
from subsystem_announcement.parse.artifact import (
    AnnouncementSection,
    AnnouncementTable,
    ParsedAnnouncementArtifact,
)


def make_index_artifact(tmp_path: Path) -> ParsedAnnouncementArtifact:
    section_1 = "公司与客户签署重大合同。合同金额1000万元。"
    table_text = "项目\t金额\n重大合同\t1000万元"
    section_2 = "本合同履行存在不确定性。"
    extracted_text = f"{section_1}\n\n{table_text}\n\n{section_2}"
    table_start = len(section_1) + 2
    section_2_start = table_start + len(table_text) + 2
    return ParsedAnnouncementArtifact(
        announcement_id="ann-index-1",
        content_hash="b" * 64,
        parser_version="docling==2.15.1",
        title_hierarchy=["重大合同公告"],
        sections=[
            AnnouncementSection(
                section_id="sec-0001",
                title="重大合同",
                level=1,
                text=section_1,
                start_offset=0,
                end_offset=len(section_1),
                parent_id=None,
            ),
            AnnouncementSection(
                section_id="sec-0002",
                title="风险提示",
                level=1,
                text=section_2,
                start_offset=section_2_start,
                end_offset=section_2_start + len(section_2),
                parent_id=None,
            ),
        ],
        tables=[
            AnnouncementTable(
                table_id="tbl-0001",
                section_id="sec-0001",
                caption=None,
                headers=["项目", "金额"],
                rows=[["重大合同", "1000万元"]],
                start_offset=table_start,
                end_offset=table_start + len(table_text),
            )
        ],
        extracted_text=extracted_text,
        parsed_at=datetime(2026, 4, 18, 9, 31, tzinfo=timezone.utc),
        source_document=AnnouncementDocumentArtifact(
            announcement_id="ann-index-1",
            ts_code="600000.SH",
            title="重大合同公告",
            publish_time=datetime(2026, 4, 18, 9, 0, tzinfo=timezone.utc),
            content_hash="b" * 64,
            official_url="https://static.sse.com.cn/disclosure/ann-index-1.pdf",
            source_exchange="sse",
            attachment_type="pdf",
            local_path=tmp_path / "ann-index-1.pdf",
            content_type="application/pdf",
            byte_size=100,
            fetched_at=datetime(2026, 4, 18, 9, 30, tzinfo=timezone.utc),
        ),
    )


def test_chunk_parsed_artifact_builds_section_and_table_chunks(
    tmp_path: Path,
) -> None:
    artifact = make_index_artifact(tmp_path)

    chunks = chunk_parsed_artifact(artifact)

    assert len(chunks) == 3
    assert [chunk.chunk_type for chunk in chunks] == ["section", "section", "table"]
    assert all(chunk.source_reference for chunk in chunks)
    assert all("local_path" not in chunk.source_reference for chunk in chunks)
    assert chunks[0].start_offset == artifact.sections[0].start_offset
    assert chunks[0].end_offset == artifact.sections[0].end_offset
    assert chunks[1].start_offset == artifact.sections[1].start_offset
    assert chunks[1].end_offset == artifact.sections[1].end_offset
    assert chunks[2].table_ref == artifact.tables[0].table_id
    assert chunks[2].source_reference["official_url"].startswith("https://")


def test_chunk_ids_and_order_are_stable(tmp_path: Path) -> None:
    artifact = make_index_artifact(tmp_path)

    first = chunk_parsed_artifact(artifact)
    second = chunk_parsed_artifact(artifact)

    assert [chunk.chunk_id for chunk in first] == [chunk.chunk_id for chunk in second]
    assert first == second
    assert make_chunk_id(
        first[0].announcement_id,
        first[0].section_id,
        first[0].chunk_type,
        first[0].start_offset,
        first[0].end_offset,
        first[0].text,
    ) == first[0].chunk_id


def test_long_section_is_split_into_clause_chunks(tmp_path: Path) -> None:
    artifact = make_index_artifact(tmp_path)
    long_text = "。".join([f"第{index}项合同安排" for index in range(20)]) + "。"
    artifact = artifact.model_copy(
        update={
            "sections": [
                artifact.sections[0].model_copy(
                    update={
                        "text": long_text,
                        "start_offset": 0,
                        "end_offset": len(long_text),
                    }
                )
            ],
            "tables": [],
            "extracted_text": long_text,
        }
    )

    chunks = chunk_parsed_artifact(artifact, max_chars=30, overlap_chars=5)

    assert len(chunks) > 1
    assert {chunk.chunk_type for chunk in chunks} == {"clause"}
    assert chunks[0].start_offset == 0
    assert all(chunk.end_offset > chunk.start_offset for chunk in chunks)


@pytest.mark.parametrize(
    ("max_chars", "overlap_chars", "message"),
    [
        (0, 0, "max_chars"),
        (10, -1, "overlap_chars"),
        (10, 10, "overlap_chars"),
    ],
)
def test_chunker_rejects_invalid_split_settings(
    tmp_path: Path,
    max_chars: int,
    overlap_chars: int,
    message: str,
) -> None:
    artifact = make_index_artifact(tmp_path)

    with pytest.raises(ValueError, match=message):
        chunk_parsed_artifact(
            artifact,
            max_chars=max_chars,
            overlap_chars=overlap_chars,
        )
