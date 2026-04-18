from __future__ import annotations

import tomllib
from datetime import datetime, timezone
from pathlib import Path

import pytest
from pydantic import ValidationError

from subsystem_announcement.discovery.document import AnnouncementDocumentArtifact
from subsystem_announcement.parse.artifact import (
    AnnouncementSection,
    AnnouncementTable,
    ParsedAnnouncementArtifact,
    load_parsed_artifact,
    write_parsed_artifact,
)
from subsystem_announcement.parse.errors import ParseNormalizationError

ROOT = Path(__file__).resolve().parents[1]


def _document(local_path: Path) -> AnnouncementDocumentArtifact:
    return AnnouncementDocumentArtifact(
        announcement_id="ann-1",
        content_hash="a" * 64,
        official_url="https://static.sse.com.cn/disclosure/ann-1.pdf",
        source_exchange="sse",
        attachment_type="pdf",
        local_path=local_path,
        content_type="application/pdf",
        byte_size=12,
        fetched_at=datetime(2026, 4, 18, 9, 30, tzinfo=timezone.utc),
    )


def _artifact(local_path: Path) -> ParsedAnnouncementArtifact:
    text = "公司签署重大合同。\n\n项目\t金额\n合同\t1000万元"
    return ParsedAnnouncementArtifact(
        announcement_id="ann-1",
        content_hash="a" * 64,
        parser_version="docling==2.15.1",
        title_hierarchy=["重大合同公告"],
        sections=[
            AnnouncementSection(
                section_id="sec-0001",
                title="重大合同公告",
                level=1,
                text="公司签署重大合同。",
                start_offset=0,
                end_offset=9,
                parent_id=None,
            )
        ],
        tables=[
            AnnouncementTable(
                table_id="tbl-0001",
                section_id="sec-0001",
                caption=None,
                headers=["项目", "金额"],
                rows=[["合同", "1000万元"]],
                start_offset=11,
                end_offset=len(text),
            )
        ],
        extracted_text=text,
        parsed_at=datetime(2026, 4, 18, 9, 31, tzinfo=timezone.utc),
        source_document=_document(local_path),
    )


def test_docling_dependency_is_exactly_pinned() -> None:
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    dependencies = pyproject["project"]["dependencies"]
    docling_dependencies = [
        dependency for dependency in dependencies if dependency.startswith("docling")
    ]

    assert docling_dependencies == ["docling==2.15.1"]
    assert not any(dependency.startswith("docling>=") for dependency in dependencies)
    assert not any(
        dependency.startswith("llama-index-node-parser-docling")
        for dependency in dependencies
    )


def test_parsed_artifact_round_trips_to_disk(tmp_path: Path) -> None:
    source_path = tmp_path / "ann-1.pdf"
    source_path.write_bytes(b"%PDF fixture")
    artifact = _artifact(source_path)

    artifact_path = write_parsed_artifact(artifact, tmp_path)
    loaded = load_parsed_artifact(artifact_path)

    assert artifact_path == tmp_path / "parsed" / "ann-1" / f"{'a' * 64}.json"
    assert loaded == artifact
    assert loaded.announcement_id == loaded.source_document.announcement_id
    assert loaded.content_hash == loaded.source_document.content_hash


def test_parsed_artifact_rejects_unconfigured_parser_version(tmp_path: Path) -> None:
    source_path = tmp_path / "ann-1.pdf"
    source_path.write_bytes(b"%PDF fixture")
    data = _artifact(source_path).model_dump()
    data["parser_version"] = "not-configured"

    with pytest.raises(ValidationError, match="parser_version"):
        ParsedAnnouncementArtifact.model_validate(data)


def test_parsed_artifact_rejects_non_hex_content_hash(tmp_path: Path) -> None:
    source_path = tmp_path / "ann-1.pdf"
    source_path.write_bytes(b"%PDF fixture")
    data = _artifact(source_path).model_dump()
    data["content_hash"] = "g" * 64
    data["source_document"]["content_hash"] = "g" * 64

    with pytest.raises(ValidationError, match="SHA-256 hex digest"):
        ParsedAnnouncementArtifact.model_validate(data)


def test_parsed_artifact_validates_offsets_and_table_section_ids(
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "ann-1.pdf"
    source_path.write_bytes(b"%PDF fixture")
    data = _artifact(source_path).model_dump()
    data["tables"][0]["section_id"] = "missing-section"

    with pytest.raises(ValidationError, match="table section_id"):
        ParsedAnnouncementArtifact.model_validate(data)

    data = _artifact(source_path).model_dump()
    data["sections"][0]["end_offset"] = len(data["extracted_text"]) + 1
    with pytest.raises(ValidationError, match="section offset"):
        ParsedAnnouncementArtifact.model_validate(data)


def test_write_parsed_artifact_rejects_unsafe_announcement_id(tmp_path: Path) -> None:
    source_path = tmp_path / "ann-1.pdf"
    source_path.write_bytes(b"%PDF fixture")
    artifact = _artifact(source_path)
    unsafe = artifact.model_copy(update={"announcement_id": "../ann-1"})

    with pytest.raises(ParseNormalizationError, match="Unsafe announcement_id"):
        write_parsed_artifact(unsafe, tmp_path)


def test_write_parsed_artifact_rejects_symlinked_announcement_directory(
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "ann-1.pdf"
    source_path.write_bytes(b"%PDF fixture")
    artifact = _artifact(source_path)
    outside_root = tmp_path / "outside"
    outside_root.mkdir()
    parsed_root = tmp_path / "parsed"
    parsed_root.mkdir()
    (parsed_root / "ann-1").symlink_to(outside_root, target_is_directory=True)

    with pytest.raises(ParseNormalizationError, match="symlink"):
        write_parsed_artifact(artifact, tmp_path)

    assert not (outside_root / f"{'a' * 64}.json").exists()


def test_write_parsed_artifact_rejects_symlinked_parsed_root(
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "ann-1.pdf"
    source_path.write_bytes(b"%PDF fixture")
    artifact = _artifact(source_path)
    outside_root = tmp_path / "outside"
    outside_root.mkdir()
    (tmp_path / "parsed").symlink_to(outside_root, target_is_directory=True)

    with pytest.raises(ParseNormalizationError, match="symlink"):
        write_parsed_artifact(artifact, tmp_path)

    assert not (outside_root / "ann-1" / f"{'a' * 64}.json").exists()


@pytest.mark.parametrize(
    "unsafe_hash",
    [
        "../" + ("a" * 61),
        "a/" + ("b" * 62),
        "a\\" + ("c" * 62),
    ],
)
def test_write_parsed_artifact_rejects_unsafe_content_hash(
    tmp_path: Path,
    unsafe_hash: str,
) -> None:
    source_path = tmp_path / "ann-1.pdf"
    source_path.write_bytes(b"%PDF fixture")
    artifact = _artifact(source_path)
    unsafe = artifact.model_copy(update={"content_hash": unsafe_hash})

    with pytest.raises(ParseNormalizationError, match="Unsafe content_hash"):
        write_parsed_artifact(unsafe, tmp_path)
