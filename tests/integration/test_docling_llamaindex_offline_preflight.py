"""M4.7 — Docling + LlamaIndex offline preflight artifact.

This is a PARTIAL/PREFLIGHT artifact for the M4.7 milestone criterion:
"10-20 representative A-share docs parsed offline; not in daily-cycle
critical path." It does not close the milestone. This integration test
exercises all 13 fixture samples (10 success + 3 corrupt) in
``tests/fixtures/announcements/manifest.json`` through the
``parse_announcement`` boundary plus LlamaIndex chunk/vector-index code.
The fixture parse path uses a manifest-fixture-shaped Docling test double:
the synthetic fixtures (~80-260 bytes) are not representative production
documents and do not prove real Docling parsing.

Scope boundaries deliberately observed:

* No production fetch. The synthetic fixtures (~80-260B) are tiny
  stubs by design, NOT representative PDFs/HTML/Word documents. The
  manifest's per-sample contracts (``expected_min_sections``,
  ``expected_min_tables``, ``expected_success``) are a synthetic
  manifest-contract smoke, not representative-document proof.
* No daily-cycle critical-path coupling. This test is in the
  ``tests/integration`` tier and runs offline (no LLM, no network,
  no production fetch). Real Docling availability is checked only when
  the package is installed; in lightweight envs it is explicitly skipped.
* No new production source. Pure new test + new evidence.

Per subsystem-announcement/CLAUDE.md, this preflight is a
"大批量 Docling 离线任务" — verifying the parse + chunk pipeline can
process the manifest contract deterministically without pressing into
the daily-cycle critical path.
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
from subsystem_announcement.index.vector_store import build_vector_index
from subsystem_announcement.parse import parse_announcement
from subsystem_announcement.parse.errors import DoclingParseError

ROOT = Path(__file__).resolve().parents[2]
FIXTURE_ROOT = ROOT / "tests" / "fixtures" / "announcements"
MANIFEST_PATH = FIXTURE_ROOT / "manifest.json"

# Single point of truth for the Docling pin used in this preflight.
# Mirrors the value in pyproject.toml and is also the value the
# existing smoke test in tests/test_parse_docling_client.py uses.
# (review-fold-1 P2: python-reviewer P2-A.)
_DOCLING_VERSION_PIN = "docling==2.15.1"


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

    **Note on intentional duplication with
    ``tests/test_parse_docling_client.py:225``** — the existing smoke
    test has a structurally identical converter. The duplication is
    deliberate so the M4.7 preflight is independently audit-able as
    a standalone preflight artifact: changes to the smoke test's converter
    must not silently propagate into the M4.7 evidence trail. If a
    third copy materialises, both should be lifted into a shared
    ``tests/_docling_fixture_helpers.py`` module
    (review-fold-1 P1: code-reviewer note).
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
    """Install a faked ``docling.document_converter`` module +
    selectively shadow ``importlib.metadata.version("docling")`` so the
    Docling boundary is exercised offline.

    **Selectively** shadows so the real LlamaIndex package can still
    resolve its own version (the prior implementation faked all
    metadata.version() calls, which broke the LlamaIndex retrieval
    leg below — codex review-fold-1 P2).
    """

    real_version = docling_client.metadata.version

    def selective_missing_version(name: str) -> str:
        # Only "docling" is faked-missing so the resolver falls back
        # to the configured pin in ``AnnouncementConfig``. Every other
        # package name passes through to the real lookup.
        if name == "docling":
            raise docling_client.metadata.PackageNotFoundError(name)
        return real_version(name)

    monkeypatch.setattr(
        docling_client.metadata, "version", selective_missing_version
    )
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
        f"M4.7 preflight manifest requires 10-20 samples; got {len(samples)}"
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

    This is a partial M4.7 preflight artifact: the offline preflight
    pipeline accepts the synthetic manifest contract deterministically.
    The test does NOT exercise real Docling on representative PDFs/HTML/
    Word docs (the fixtures are synthetic stubs by design).

    **Vacuous-pass guard (review-fold-1):** the fake's table-emission
    heuristic (`"\\t" in content or "<table" in content`) is verified
    against the manifest's per-sample ``expected_min_tables`` — every
    fixture file with ``expected_min_tables >= 1``
    (ANN-SAMPLE-002 / 003 / 007 / 008) contains a tab character, so
    the ``len(artifact.tables) >= expected_min_tables`` assertion is
    meaningful and not silently passing.
    """

    manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    config = AnnouncementConfig(docling_version=_DOCLING_VERSION_PIN)

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
    config = AnnouncementConfig(docling_version=_DOCLING_VERSION_PIN)

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
        # Assert message includes sample_id so a CI failure points at
        # the offending fixture (review-fold-1 P2: code-reviewer).
        assert len(chunks) >= 1, (
            f"zero chunks emitted for sample={sample['sample_id']} "
            f"file={sample['file']}; chunker dropped a non-empty parse"
        )
        chunk_counts.append((sample["sample_id"], len(chunks)))

    # At least 10 success samples produced at least 1 chunk each.
    assert len(chunk_counts) >= 10


def test_real_docling_package_is_installed_and_version_resolves() -> None:
    """Verifies the real ``docling`` package is installed and that
    ``DocumentConverter`` is importable. Does NOT call ``.convert()``
    on the synthetic fixtures (they are not real PDFs); the goal of
    this test is to fail loudly if the Docling pin is missing or
    version-mismatched in the venv where M4.7 is run, rather than
    silently passing because the test double absorbs the failure
    (codex review-fold-1 P2: real Docling boundary).

    **CI-tier contract:** if ``docling`` is not installed in the venv
    (e.g. a dev iteration that did not pip-install the heavy
    ``deepsearch-glm`` build dep yet), this test is **skipped** with
    an explicit message. CI lanes that need to gate M4.7 on the
    Docling version pin must install the heavy dep set; lighter
    dev lanes get a fast no-op rather than a hard failure. The
    skip reason makes the deferral visible in the CI summary so a
    silently-missing Docling cannot be mistaken for real-Docling
    evidence.

    Pairs with the mocked ``test_all_manifest_samples_round_trip_*``
    above — together they cover both "real package available (when
    installed) + version pin matched" + "the test fixtures parse
    against the boundary contract".
    """

    try:
        from docling.document_converter import DocumentConverter
    except ModuleNotFoundError:
        pytest.skip(
            "docling package not installed in this venv (typically "
            "blocked by the heavy `deepsearch-glm` build dep). M4.7 "
            "real-Docling availability check skipped; this run "
            "produces no real Docling parse proof. Install the full "
            "Docling dep set in the CI lane that gates this preflight."
        )

    assert DocumentConverter is not None

    # Version pin alignment: the AnnouncementConfig's resolver must be
    # able to read the installed docling version via importlib.metadata
    # (no fake here — the config defaults to "not-configured" and the
    # resolver falls back to the installed package's version).
    from importlib import metadata

    installed_version = metadata.version("docling")
    expected_version = _DOCLING_VERSION_PIN.split("==", 1)[1]
    assert installed_version == expected_version, (
        f"Docling pin drift: pyproject pins {_DOCLING_VERSION_PIN}, "
        f"venv installed docling=={installed_version}"
    )


def test_real_llama_index_package_is_installed_and_version_resolves() -> None:
    """Companion to ``test_real_docling_package_is_installed_*`` — pin
    the LlamaIndex package availability + version. The downstream
    LlamaIndex integration test below (``test_manifest_samples_*_vector_index_*``)
    exercises the actual code path; this test catches a missing /
    wrong-version package up-front so the failure mode is "M4.7
    blocked on missing dep" rather than "M4.7 silently fakes the
    LlamaIndex leg" (codex review-fold-1 P2: LlamaIndex boundary).
    """

    from importlib import metadata

    # llama-index-core is the M4.7 pin per pyproject.toml.
    installed_version = metadata.version("llama-index-core")
    # Pinned floor: the version used in subsystem-announcement's
    # pyproject.toml is "llama-index-core==0.10.0". Pin equality so a
    # silent bump is caught.
    assert installed_version == "0.10.0", (
        f"llama-index-core pin drift: pyproject pins 0.10.0, "
        f"venv installed llama-index-core=={installed_version}"
    )

    # The vector-store loader must be able to import the LlamaIndex
    # API surface used by build_vector_index.
    from subsystem_announcement.index.vector_store import (
        _load_llama_index_api,
    )

    api = _load_llama_index_api()
    assert api is not None
    assert hasattr(api, "TextNode")
    assert hasattr(api, "SimpleVectorStore")
    assert hasattr(api, "StorageContext")


def test_manifest_samples_build_real_llama_index_vector_index_offline(
    fake_docling: None,
    tmp_path: Path,
) -> None:
    """Build a real LlamaIndex SimpleVectorStore from the chunks of one
    synthetic manifest success sample, with mock embeddings (no LLM
    network calls). Persists the vector store to ``tmp_path`` and
    asserts the persistence side-effects landed.

    This addresses codex review-fold-1 P2's second concern: the M4.7
    preflight previously stopped at the local ``chunk_parsed_artifact``
    helper without exercising the LlamaIndex retrieval/index path.
    This test calls ``build_vector_index`` (which loads LlamaIndex
    proper) with mock embeddings so the integration is offline and
    network-free, and asserts the produced ``AnnouncementVectorIndexRef``
    + on-disk persistence."""

    manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    config = AnnouncementConfig(
        docling_version=_DOCLING_VERSION_PIN,
        llama_index_version="llama-index-core==0.10.0",
        # Switch on the test-only mock-embedding path so
        # ``_resolve_embed_model`` does not require a configured
        # adapter or live embedding API.
        allow_test_mock_embeddings=True,
    )

    # Pick the first success sample — one is enough to prove the
    # LlamaIndex leg integrates; running every sample would multiply
    # the index-persistence cost without adding signal beyond the
    # parse/chunk preflight tests above.
    sample = next(s for s in manifest["samples"] if s["expected_success"])
    sample_path = FIXTURE_ROOT / sample["file"]
    document = _document(
        sample_path,
        sample_id=sample["sample_id"],
        attachment_type=sample["attachment_type"],
    )

    parsed_artifact = parse_announcement(document, config)
    chunks = chunk_parsed_artifact(parsed_artifact)
    assert len(chunks) >= 1

    vector_store_dir = tmp_path / "vector_store"
    vector_ref = build_vector_index(
        chunks,
        persist_dir=vector_store_dir,
        config=config,
    )

    # The vector ref points at the persisted directory, the
    # llama-index-core version is recorded, and the on-disk artifact
    # has at least one of the expected LlamaIndex persistence files.
    assert vector_ref.index_ref == str(vector_store_dir)
    assert vector_ref.llama_index_version == "llama-index-core==0.10.0"
    assert vector_store_dir.is_dir()
    persisted_files = list(vector_store_dir.iterdir())
    assert len(persisted_files) >= 1, (
        f"build_vector_index did not persist any files to "
        f"{vector_store_dir}; LlamaIndex SimpleVectorStore.persist() "
        "may be silently skipped"
    )


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
