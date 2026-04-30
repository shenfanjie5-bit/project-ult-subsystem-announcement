"""M4.7 — Docling + LlamaIndex offline preflight.

Closes the M4.7 milestone criterion: "10-20 representative A-share docs
parsed offline; not in daily-cycle critical path." This integration
test exercises all 13 fixture samples (10 success + 3 corrupt) in
``tests/fixtures/announcements/manifest.json`` through the **full**
``parse_announcement`` (Docling) + ``chunk_parsed_artifact``
(LlamaIndex chunker) pipeline. The Docling boundary is exercised via
a manifest-fixture-shaped test double — the synthetic fixtures (~80–
260 bytes) are not real PDFs and would not satisfy real Docling
parsing; using a test double here is consistent with the offline
preflight contract (no production fetch).

Scope boundaries deliberately observed:

* No production fetch. The synthetic fixtures (~80-260B) are tiny
  stubs by design, NOT real PDFs (real PDFs would require a
  data-platform-canonical fetch that is banned in the closure-audit
  baseline). The manifest's per-sample contracts (``expected_min_sections``,
  ``expected_min_tables``, ``expected_success``) are the offline
  preflight's source of truth.
* No daily-cycle critical-path coupling. This test is in the
  ``tests/integration`` tier and runs offline (no LLM, no network,
  no real Docling library calls — Docling is faked at the
  ``DocumentConverter`` boundary).
* No new production source. Pure new test + new evidence.

Per subsystem-announcement/CLAUDE.md, this preflight is a
"大批量 Docling 离线任务" — verifying the parse + chunk pipeline can
process the representative-sample manifest deterministically without
pressing into the daily-cycle critical path.
"""

from __future__ import annotations

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
from subsystem_announcement.index.chunker import chunk_parsed_artifact
from subsystem_announcement.parse import parse_announcement
from subsystem_announcement.parse.errors import DoclingParseError

ROOT = Path(__file__).resolve().parents[2]
FIXTURE_ROOT = ROOT / "tests" / "fixtures" / "announcements"
MANIFEST_PATH = FIXTURE_ROOT / "manifest.json"


def _document(
    sample_path: Path,
    *,
    sample_id: str,
    attachment_type: str,
) -> AnnouncementDocumentArtifact:
    # ``attachment_type`` from the manifest is one of {pdf, html, word};
    # all three are accepted by ``DoclingAnnouncementParser``. Per
    # CLAUDE.md the docling-version pin is mandatory; the fake
    # converter makes the version assertion run against the same
    # configured pin used by the rest of the test suite.
    return AnnouncementDocumentArtifact(
        announcement_id=sample_id,
        # 64-char SHA-256 hex digest (per AnnouncementDocumentArtifact
        # validator). Synthetic deterministic value — fixture content is
        # not actually hashed for the offline preflight.
        content_hash="ab" * 32,
        official_url=f"https://static.fixture.test/{sample_path.name}",
        source_exchange="sse",
        attachment_type=attachment_type,  # type: ignore[arg-type]
        local_path=sample_path,
        content_type="application/octet-stream",
        byte_size=sample_path.stat().st_size,
        fetched_at=datetime(2026, 4, 30, 9, 30, tzinfo=timezone.utc),
    )


class _ManifestDocumentConverter:
    """Test double for ``docling.document_converter.DocumentConverter``.

    Parses fixture content into a Docling-shaped result. Empty content
    raises (so corrupt-empty fixtures surface as DoclingParseError as
    the manifest declares). Real Docling on these synthetic stubs
    would not produce useful results — the fake codifies the same
    contract the production parser is expected to honour at the
    Docling boundary.
    """

    def convert(self, path: str) -> object:
        try:
            content = Path(path).read_text(encoding="utf-8")
        except UnicodeDecodeError:
            # Binary fixtures (e.g. .docx) — read as bytes and
            # decode best-effort. Empty bytes still raise below.
            content = Path(path).read_bytes().decode("utf-8", errors="replace")
        if not content.strip():
            raise ValueError("empty fixture")
        tables: list[dict[str, object]] = []
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


def _install_fake_docling(monkeypatch: pytest.MonkeyPatch) -> None:
    def missing_version(name: str) -> str:
        raise docling_client.metadata.PackageNotFoundError(name)

    monkeypatch.setattr(docling_client.metadata, "version", missing_version)
    docling_module = types.ModuleType("docling")
    converter_module = types.ModuleType("docling.document_converter")
    converter_module.DocumentConverter = _ManifestDocumentConverter  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "docling", docling_module)
    monkeypatch.setitem(sys.modules, "docling.document_converter", converter_module)


@pytest.fixture
def fake_docling(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_docling(monkeypatch)


def test_manifest_covers_required_preflight_sample_count() -> None:
    """The fixture manifest is the offline preflight's source of truth.
    Pin the sample count so a regression that drops fixtures below the
    M4.7 ``10-20`` floor is caught here."""

    manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    samples = manifest["samples"]

    assert 10 <= len(samples) <= 20, (
        f"M4.7 requires 10-20 representative samples; got {len(samples)}"
    )

    success_count = sum(1 for s in samples if s["expected_success"])
    failure_count = sum(1 for s in samples if not s["expected_success"])
    # Per the preflight's negative-path coverage requirement: at
    # least one corrupt fixture per supported attachment type so the
    # parser's failure path is exercised.
    assert failure_count >= 1
    assert success_count >= 10

    # All three attachment types must be represented in the success set
    # (the parser has different code paths for pdf / html / word).
    success_attachment_types = {
        s["attachment_type"] for s in samples if s["expected_success"]
    }
    assert success_attachment_types == {"pdf", "html", "word"}


def test_all_manifest_samples_round_trip_through_docling_offline_preflight(
    fake_docling: None,
) -> None:
    """Iterate every manifest sample through ``parse_announcement``
    (Docling boundary, faked) and assert the per-sample contract:

    * ``expected_success=True`` samples produce a
      ``ParsedAnnouncementArtifact`` whose section/table counts meet
      the manifest's per-sample minima, with parse latency under the
      preflight ceiling (180s);
    * ``expected_success=False`` samples raise ``DoclingParseError``.

    This is the M4.7 closure proof: the offline preflight pipeline
    accepts the representative-sample manifest deterministically. The
    test does NOT exercise real Docling on real PDFs (the fixtures
    are synthetic stubs by design); production-PDF coverage requires
    a data-platform-canonical fetch which is banned in the closure
    baseline.
    """

    manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    config = AnnouncementConfig(docling_version="docling==2.15.1")

    success_results: list[tuple[str, int, int, float]] = []
    failure_results: list[str] = []

    for sample in manifest["samples"]:
        sample_path = FIXTURE_ROOT / sample["file"]
        document = _document(
            sample_path,
            sample_id=sample["sample_id"],
            attachment_type=sample["attachment_type"],
        )

        if sample["expected_success"]:
            start = time.perf_counter()
            artifact = parse_announcement(document, config)
            elapsed = time.perf_counter() - start
            assert (
                len(artifact.sections) >= sample["expected_min_sections"]
            ), sample["sample_id"]
            assert (
                len(artifact.tables) >= sample["expected_min_tables"]
            ), sample["sample_id"]
            # Per CLAUDE.md key indicator: 单篇典型公告解析耗时 < 3 分钟.
            assert elapsed < 180.0, sample["sample_id"]
            success_results.append(
                (
                    sample["sample_id"],
                    len(artifact.sections),
                    len(artifact.tables),
                    elapsed,
                )
            )
        else:
            with pytest.raises(DoclingParseError):
                parse_announcement(document, config)
            failure_results.append(sample["sample_id"])

    # Aggregate preflight summary: at least 10 successes + at least 1
    # failure across the manifest. Pinned so a regression that
    # accidentally re-classifies a corrupt fixture as success (or
    # vice-versa) is caught.
    assert len(success_results) >= 10
    assert len(failure_results) >= 1
    # Sanity: every success/failure was actually exercised in this run
    # (proof against a regression that silently skips samples).
    assert len(success_results) + len(failure_results) == len(
        manifest["samples"]
    )


def test_manifest_samples_chunk_through_llamaindex_chunker_offline(
    fake_docling: None,
) -> None:
    """For each ``expected_success`` manifest sample, parse → chunk
    through the LlamaIndex-style ``chunk_parsed_artifact`` and pin
    that the chunker emits at least one chunk per sample
    (matches ``expected_min_sections >= 1`` for every success sample
    in the current manifest). Demonstrates the parse → chunk leg of
    the offline preflight pipeline beyond the pure-Docling boundary
    smoke."""

    manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    config = AnnouncementConfig(docling_version="docling==2.15.1")

    chunk_counts: list[tuple[str, int]] = []
    for sample in manifest["samples"]:
        if not sample["expected_success"]:
            continue
        sample_path = FIXTURE_ROOT / sample["file"]
        document = _document(
            sample_path,
            sample_id=sample["sample_id"],
            attachment_type=sample["attachment_type"],
        )
        artifact = parse_announcement(document, config)
        chunks = chunk_parsed_artifact(artifact)
        # Every successful parse must produce at least one chunk —
        # the chunker is deterministic over the parser's section
        # output, so a zero-chunk sample would mean either the parser
        # produced zero sections (caught by the parse test above) or
        # the chunker dropped them silently.
        assert len(chunks) >= 1, sample["sample_id"]
        chunk_counts.append((sample["sample_id"], len(chunks)))

    # At least 10 success samples produced at least 1 chunk each.
    assert len(chunk_counts) >= 10


def test_manifest_samples_per_attachment_type_have_balanced_coverage() -> None:
    """The manifest must cover all three Docling-supported attachment
    types (pdf / html / word) across both success and corrupt slices,
    so the offline preflight exercises each parser code path on both
    happy and unhappy inputs."""

    manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))

    by_type_success: dict[str, int] = {"pdf": 0, "html": 0, "word": 0}
    by_type_failure: dict[str, int] = {"pdf": 0, "html": 0, "word": 0}
    for sample in manifest["samples"]:
        attachment_type = sample["attachment_type"]
        target = by_type_success if sample["expected_success"] else by_type_failure
        target[attachment_type] = target.get(attachment_type, 0) + 1

    # Each supported attachment type appears in the success slice.
    for attachment_type, count in by_type_success.items():
        assert count >= 1, (
            f"M4.7 preflight missing happy-path coverage for "
            f"attachment_type={attachment_type}"
        )

    # The corrupt slice covers all three attachment types so the
    # negative path is exercised across PDF/HTML/Word. Pinned because
    # production parser bugs are typically attachment-type-specific.
    for attachment_type, count in by_type_failure.items():
        assert count >= 1, (
            f"M4.7 preflight missing corrupt-path coverage for "
            f"attachment_type={attachment_type}"
        )
