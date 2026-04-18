from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from subsystem_announcement.discovery.document import AnnouncementDocumentArtifact
from subsystem_announcement.parse.errors import ParseNormalizationError
from subsystem_announcement.parse.normalize import normalize_docling_result


def _document(local_path: Path) -> AnnouncementDocumentArtifact:
    return AnnouncementDocumentArtifact(
        announcement_id="ann-1",
        content_hash="b" * 64,
        official_url="https://static.sse.com.cn/disclosure/ann-1.html",
        source_exchange="sse",
        attachment_type="html",
        local_path=local_path,
        content_type="text/html",
        byte_size=32,
        fetched_at=datetime(2026, 4, 18, 9, 30, tzinfo=timezone.utc),
    )


def test_normalize_docling_result_builds_sections_tables_and_offsets(
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "ann-1.html"
    source_path.write_text("<html>fixture</html>", encoding="utf-8")
    raw_result = {
        "document": {
            "sections": [
                {
                    "title": "重大合同公告",
                    "level": 1,
                    "text": "公司与客户签署重大合同。",
                },
                {
                    "title": "合同金额",
                    "level": 2,
                    "text": "合同金额以正式协议为准。",
                },
            ],
            "tables": [
                {
                    "caption": "合同明细",
                    "headers": ["项目", "金额"],
                    "rows": [["设备销售", "1000万元"]],
                }
            ],
        }
    }

    artifact = normalize_docling_result(
        raw_result,
        _document(source_path),
        "docling==2.15.1",
    )

    assert artifact.announcement_id == "ann-1"
    assert artifact.content_hash == "b" * 64
    assert artifact.parser_version == "docling==2.15.1"
    assert [section.section_id for section in artifact.sections] == [
        "sec-0001",
        "sec-0002",
    ]
    assert artifact.sections[1].parent_id == "sec-0001"
    assert artifact.title_hierarchy == ["重大合同公告", "合同金额"]
    assert artifact.tables[0].table_id == "tbl-0001"
    assert artifact.tables[0].section_id == "sec-0001"
    assert artifact.tables[0].rows == [["设备销售", "1000万元"]]
    assert artifact.extracted_text
    for section in artifact.sections:
        assert 0 <= section.start_offset <= section.end_offset <= len(
            artifact.extracted_text
        )
    for table in artifact.tables:
        assert (
            0
            <= table.start_offset
            <= table.end_offset
            <= len(artifact.extracted_text)
        )


def test_normalize_docling_result_falls_back_to_exported_markdown(
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "ann-1.html"
    source_path.write_text("<html>fixture</html>", encoding="utf-8")

    class FakeDocument:
        def export_to_markdown(self) -> str:
            return (
                "# 第一节\n公司披露业绩预告。\n\n"
                "## 第二节\n预计净利润增长。"
            )

    raw_result = {"document": FakeDocument()}

    artifact = normalize_docling_result(
        raw_result,
        _document(source_path),
        "docling==2.15.1",
    )

    assert len(artifact.sections) == 2
    assert artifact.sections[0].title == "第一节"
    assert artifact.sections[1].title == "第二节"
    assert "预计净利润增长" in artifact.extracted_text


def test_normalize_docling_result_rejects_empty_text(tmp_path: Path) -> None:
    source_path = tmp_path / "ann-1.html"
    source_path.write_text("<html>fixture</html>", encoding="utf-8")

    with pytest.raises(ParseNormalizationError, match="extractable text"):
        normalize_docling_result(
            {"document": {"sections": []}},
            _document(source_path),
            "docling==2.15.1",
        )
