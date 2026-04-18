from __future__ import annotations

import ast
import json
import sys
import time
import types
from datetime import datetime, timezone
from pathlib import Path

import pytest

import subsystem_announcement.parse.docling_client as docling_client
from subsystem_announcement.config import AnnouncementConfig
from subsystem_announcement.discovery.document import AnnouncementDocumentArtifact
from subsystem_announcement.parse import ParsedAnnouncementArtifact, parse_announcement
from subsystem_announcement.parse.docling_client import (
    DoclingAnnouncementParser,
    resolve_docling_version,
)
from subsystem_announcement.parse.errors import (
    DoclingParseError,
    UnsupportedAttachmentTypeError,
)

ROOT = Path(__file__).resolve().parents[1]


def _document(
    local_path: Path,
    *,
    attachment_type: str = "pdf",
) -> AnnouncementDocumentArtifact:
    return AnnouncementDocumentArtifact(
        announcement_id="ann-1",
        content_hash="c" * 64,
        official_url="https://static.sse.com.cn/disclosure/ann-1.pdf",
        source_exchange="sse",
        attachment_type=attachment_type,  # type: ignore[arg-type]
        local_path=local_path,
        content_type="application/pdf",
        byte_size=32,
        fetched_at=datetime(2026, 4, 18, 9, 30, tzinfo=timezone.utc),
    )


def test_resolve_docling_version_prefers_installed_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(docling_client.metadata, "version", lambda name: "2.99.0")

    version = resolve_docling_version(
        AnnouncementConfig(docling_version="docling==2.15.1")
    )

    assert version == "docling==2.99.0"


def test_resolve_docling_version_uses_validated_config_when_package_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def missing_version(name: str) -> str:
        raise docling_client.metadata.PackageNotFoundError(name)

    monkeypatch.setattr(docling_client.metadata, "version", missing_version)

    assert (
        resolve_docling_version(AnnouncementConfig(docling_version="docling==2.15.1"))
        == "docling==2.15.1"
    )


def test_resolve_docling_version_rejects_unconfigured_parser_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def missing_version(name: str) -> str:
        raise docling_client.metadata.PackageNotFoundError(name)

    monkeypatch.setattr(docling_client.metadata, "version", missing_version)

    with pytest.raises(DoclingParseError, match="not-configured"):
        resolve_docling_version(AnnouncementConfig())


def test_parse_announcement_uses_docling_boundary(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "ann-1.pdf"
    source_path.write_bytes(b"%PDF fixture")
    converted_paths: list[str] = []

    class FakeDocumentConverter:
        def convert(self, path: str) -> object:
            converted_paths.append(path)
            return {
                "document": {
                    "sections": [
                        {
                            "title": "重大合同公告",
                            "level": 1,
                            "text": "公司签署重大合同。",
                        }
                    ]
                }
            }

    _install_fake_docling(monkeypatch, FakeDocumentConverter)

    artifact = parse_announcement(
        _document(source_path),
        AnnouncementConfig(docling_version="docling==2.15.1"),
    )

    assert isinstance(artifact, ParsedAnnouncementArtifact)
    assert converted_paths == [str(source_path)]
    assert artifact.sections[0].text == "公司签署重大合同。"


def test_parse_announcement_rejects_unsupported_attachment_before_docling_import(
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "ann-1.txt"
    source_path.write_text("fixture", encoding="utf-8")
    data = _document(source_path).model_dump()
    data["attachment_type"] = "txt"
    document = AnnouncementDocumentArtifact.model_construct(**data)

    with pytest.raises(UnsupportedAttachmentTypeError, match="attachment_type=txt"):
        DoclingAnnouncementParser().parse(
            document,
            AnnouncementConfig(docling_version="docling==2.15.1"),
        )


def test_parse_announcement_wraps_docling_failures(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "ann-1.pdf"
    source_path.write_bytes(b"%PDF fixture")

    class FailingDocumentConverter:
        def convert(self, path: str) -> object:
            raise ValueError("bad document")

    _install_fake_docling(monkeypatch, FailingDocumentConverter)

    with pytest.raises(DoclingParseError, match="Docling failed"):
        parse_announcement(
            _document(source_path),
            AnnouncementConfig(docling_version="docling==2.15.1"),
        )


def test_docling_imports_are_isolated_to_docling_client() -> None:
    offenders: list[Path] = []
    for path in (ROOT / "src" / "subsystem_announcement").rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                names = [node.module or ""]
            else:
                continue
            if any(name == "docling" or name.startswith("docling.") for name in names):
                if path.name != "docling_client.py":
                    offenders.append(path)

    assert offenders == []


def test_source_does_not_import_second_parser() -> None:
    forbidden_modules = {
        "bs4",
        "docx",
        "pdfplumber",
        "pypdf",
    }
    offenders: list[tuple[Path, str]] = []
    for path in (ROOT / "src" / "subsystem_announcement").rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                names = [node.module or ""]
            else:
                continue
            for name in names:
                top_level = name.split(".", 1)[0]
                if top_level in forbidden_modules:
                    offenders.append((path, name))

    assert offenders == []


def test_announcement_fixture_manifest_covers_supported_attachment_types() -> None:
    fixture_root = ROOT / "tests" / "fixtures" / "announcements"
    manifest = json.loads((fixture_root / "manifest.json").read_text(encoding="utf-8"))
    samples = manifest["samples"]

    assert 10 <= len(samples) <= 20
    assert {sample["attachment_type"] for sample in samples}.issubset(
        {"pdf", "html", "word"}
    )
    assert {"pdf", "html", "word"}.issubset(
        {sample["attachment_type"] for sample in samples}
    )
    assert any(sample["expected_success"] is False for sample in samples)
    for sample in samples:
        sample_path = fixture_root / sample["file"]
        assert sample_path.exists(), sample_path
        assert isinstance(sample["expected_min_sections"], int)
        assert isinstance(sample["expected_min_tables"], int)


def test_manifest_samples_parse_through_docling_boundary_smoke(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture_root = ROOT / "tests" / "fixtures" / "announcements"
    manifest = json.loads((fixture_root / "manifest.json").read_text(encoding="utf-8"))

    class ManifestDocumentConverter:
        def convert(self, path: str) -> object:
            content = Path(path).read_text(encoding="utf-8")
            if not content.strip():
                raise ValueError("empty fixture")
            tables = []
            if "\t" in content or "<table" in content:
                tables.append(
                    {
                        "caption": "fixture table",
                        "headers": ["项目", "值"],
                        "rows": [["样本", Path(path).name]],
                    }
                )
            return {
                "document": {
                    "sections": [
                        {
                            "title": Path(path).stem,
                            "level": 1,
                            "text": content,
                        }
                    ],
                    "tables": tables,
                }
            }

    _install_fake_docling(monkeypatch, ManifestDocumentConverter)
    config = AnnouncementConfig(docling_version="docling==2.15.1")

    for sample in manifest["samples"]:
        sample_path = fixture_root / sample["file"]
        document = _document(
            sample_path,
            attachment_type=sample["attachment_type"],
        )
        start = time.perf_counter()
        if sample["expected_success"]:
            artifact = parse_announcement(document, config)
            elapsed_seconds = time.perf_counter() - start
            assert len(artifact.sections) >= sample["expected_min_sections"]
            assert len(artifact.tables) >= sample["expected_min_tables"]
            assert elapsed_seconds < 180
        else:
            with pytest.raises(DoclingParseError):
                parse_announcement(document, config)


def _install_fake_docling(
    monkeypatch: pytest.MonkeyPatch,
    converter_class: type,
) -> None:
    def missing_version(name: str) -> str:
        raise docling_client.metadata.PackageNotFoundError(name)

    monkeypatch.setattr(docling_client.metadata, "version", missing_version)
    docling_module = types.ModuleType("docling")
    converter_module = types.ModuleType("docling.document_converter")
    converter_module.DocumentConverter = converter_class
    monkeypatch.setitem(sys.modules, "docling", docling_module)
    monkeypatch.setitem(sys.modules, "docling.document_converter", converter_module)
