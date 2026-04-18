from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path

import pytest

from subsystem_announcement.config import AnnouncementConfig
from subsystem_announcement.discovery import (
    AnnouncementDiscoveryResult,
    AnnouncementEnvelope,
)
from subsystem_announcement.discovery.document import AnnouncementDocumentArtifact
from subsystem_announcement.index.retrieval_artifact import (
    AnnouncementRetrievalArtifact,
    build_retrieval_artifact,
    load_retrieval_artifact,
    write_retrieval_artifact,
)
from subsystem_announcement.index.vector_store import AnnouncementVectorIndexRef
from subsystem_announcement.parse import ParsedAnnouncementArtifact
from subsystem_announcement.runtime.pipeline import AnnouncementPipeline
from subsystem_announcement.runtime.trace import TraceStore

from .test_index_chunker import make_index_artifact


def test_retrieval_artifact_builds_and_round_trips(
    tmp_path: Path,
    monkeypatch,
) -> None:
    parsed_artifact = make_index_artifact(tmp_path)

    def fake_build_vector_index(chunks, *, persist_dir, config, embed_model=None):
        persist_dir.mkdir(parents=True, exist_ok=True)
        return AnnouncementVectorIndexRef(
            index_ref=str(persist_dir),
            llama_index_version=config.llama_index_version,
            chunk_ids=[chunk.chunk_id for chunk in chunks],
            built_at=datetime(2026, 4, 18, 10, 0, tzinfo=timezone.utc),
        )

    monkeypatch.setattr(
        "subsystem_announcement.index.vector_store.build_vector_index",
        fake_build_vector_index,
    )
    config = AnnouncementConfig(
        artifact_root=tmp_path / "artifacts",
        llama_index_version="llama-index-core==0.10.0",
    )

    artifact = build_retrieval_artifact(
        parsed_artifact,
        config=config,
        parsed_artifact_path=tmp_path / "parsed.json",
        output_root=tmp_path / "index-output",
    )
    path = write_retrieval_artifact(artifact, tmp_path / "index-output")
    loaded = load_retrieval_artifact(path)

    assert path == tmp_path / "index-output" / "retrieval_artifact.json"
    assert loaded == artifact
    assert loaded.chunk_count == 3
    assert loaded.chunk_refs == [chunk.chunk_id for chunk in loaded.chunks]
    assert loaded.index_ref == str(tmp_path / "index-output" / "vector_store")


@pytest.mark.parametrize(
    "announcement_id",
    ["../x", "/abs/path", "a/b", ".", "..", "ann\x00index"],
)
def test_retrieval_artifact_rejects_unsafe_default_announcement_id(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    announcement_id: str,
) -> None:
    parsed_artifact = make_index_artifact(tmp_path)
    unsafe_artifact = parsed_artifact.model_copy(
        update={
            "announcement_id": announcement_id,
            "source_document": parsed_artifact.source_document.model_copy(
                update={"announcement_id": announcement_id}
            ),
        }
    )
    config = AnnouncementConfig(
        artifact_root=tmp_path / "artifacts",
        llama_index_version="llama-index-core==0.10.0",
    )

    def fail_build_vector_index(*args, **kwargs):
        raise AssertionError("unsafe announcement_id reached vector index build")

    monkeypatch.setattr(
        "subsystem_announcement.index.vector_store.build_vector_index",
        fail_build_vector_index,
    )

    with pytest.raises(ValueError, match="Unsafe announcement_id"):
        build_retrieval_artifact(unsafe_artifact, config=config)


def test_retrieval_artifact_rejects_default_vector_store_symlink_escape(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parsed_artifact = make_index_artifact(tmp_path)
    config = AnnouncementConfig(
        artifact_root=tmp_path / "artifacts",
        llama_index_version="llama-index-core==0.10.0",
    )
    outside = tmp_path / "outside"
    outside.mkdir()
    vector_store_dir = (
        tmp_path
        / "artifacts"
        / "index"
        / parsed_artifact.announcement_id
        / "vector_store"
    )
    vector_store_dir.parent.mkdir(parents=True)
    vector_store_dir.symlink_to(outside, target_is_directory=True)

    def fail_build_vector_index(*args, **kwargs):
        raise AssertionError("unsafe vector_store path reached vector index build")

    monkeypatch.setattr(
        "subsystem_announcement.index.vector_store.build_vector_index",
        fail_build_vector_index,
    )

    with pytest.raises(ValueError, match="Vector Store Directory is a symlink"):
        build_retrieval_artifact(parsed_artifact, config=config)


def test_retrieval_artifact_rejects_inconsistent_chunk_refs(
    tmp_path: Path,
) -> None:
    parsed_artifact = make_index_artifact(tmp_path)
    from subsystem_announcement.index import chunk_parsed_artifact

    chunks = chunk_parsed_artifact(parsed_artifact)

    try:
        AnnouncementRetrievalArtifact(
            announcement_id=parsed_artifact.announcement_id,
            chunk_refs=["wrong"],
            index_ref=str(tmp_path / "index"),
            parser_version=parsed_artifact.parser_version,
            llama_index_version="llama-index-core==0.10.0",
            chunk_count=1,
            built_at=datetime(2026, 4, 18, 10, 0, tzinfo=timezone.utc),
            source_parsed_artifact_path=None,
            chunks=chunks,
        )
    except ValueError as exc:
        assert "chunk_count" in str(exc) or "chunk_refs" in str(exc)
    else:
        raise AssertionError("inconsistent chunk refs were accepted")


def test_retrieval_artifact_accepts_external_chunk_refs_without_inline_chunks(
    tmp_path: Path,
) -> None:
    artifact = AnnouncementRetrievalArtifact(
        announcement_id="ann-index-1",
        chunk_refs=["chunk:ann-index-1:sec-0001:section:abc"],
        index_ref=str(tmp_path / "index"),
        parser_version="docling==2.15.1",
        llama_index_version="llama-index-core==0.10.0",
        chunk_count=1,
        built_at=datetime(2026, 4, 18, 10, 0, tzinfo=timezone.utc),
        source_parsed_artifact_path=None,
    )

    assert artifact.chunks == []


def test_pipeline_process_envelope_records_retrieval_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    vector_calls: list[list[str]] = []

    def fake_build_vector_index(chunks, *, persist_dir, config, embed_model=None):
        persist_dir.mkdir(parents=True, exist_ok=True)
        vector_calls.append([chunk.chunk_id for chunk in chunks])
        return AnnouncementVectorIndexRef(
            index_ref=str(persist_dir),
            llama_index_version=config.llama_index_version,
            chunk_ids=[chunk.chunk_id for chunk in chunks],
            built_at=datetime(2026, 4, 18, 10, 0, tzinfo=timezone.utc),
        )

    monkeypatch.setattr(
        "subsystem_announcement.index.vector_store.build_vector_index",
        fake_build_vector_index,
    )
    config = AnnouncementConfig(
        artifact_root=tmp_path / "artifacts",
        docling_version="docling==2.15.1",
        llama_index_version="llama-index-core==0.10.0",
    )
    pipeline = AnnouncementPipeline(
        config,
        discovery_func=_fake_discovery,
        parse_func=_fake_parse,
        extract_func=lambda artifact: [],
    )

    run = asyncio.run(pipeline.process_envelope(_envelope()))

    assert run.status == "succeeded"
    assert run.parsed_artifact_path is not None
    assert run.parsed_artifact_path.exists()
    assert run.index_build_status == "succeeded"
    assert run.retrieval_artifact_path == (
        tmp_path / "artifacts" / "index" / "ann-index-1" / "retrieval_artifact.json"
    )
    assert run.retrieval_artifact_path.exists()
    assert vector_calls

    retrieval_artifact = load_retrieval_artifact(run.retrieval_artifact_path)
    assert retrieval_artifact.announcement_id == "ann-index-1"
    assert retrieval_artifact.source_parsed_artifact_path == run.parsed_artifact_path
    assert retrieval_artifact.chunk_count == 3
    assert retrieval_artifact.chunk_refs == vector_calls[0]

    assert run.trace_path is not None
    loaded_trace = TraceStore(config).load(run.trace_path)
    assert loaded_trace.retrieval_artifact_path == run.retrieval_artifact_path
    assert loaded_trace.index_build_status == "succeeded"


def test_pipeline_marks_retrieval_artifact_pending_when_index_build_is_offline(
    tmp_path: Path,
) -> None:
    config = AnnouncementConfig(
        artifact_root=tmp_path / "artifacts",
        docling_version="docling==2.15.1",
        llama_index_version="llama-index-core==0.10.0",
    )

    def unavailable_index(*args, **kwargs):
        raise RuntimeError("LlamaIndex dependency is unavailable")

    pipeline = AnnouncementPipeline(
        config,
        discovery_func=_fake_discovery,
        parse_func=_fake_parse,
        build_retrieval_func=unavailable_index,
        extract_func=lambda artifact: [],
    )

    run = asyncio.run(pipeline.process_envelope(_envelope()))

    assert run.status == "succeeded"
    assert run.parsed_artifact_path is not None
    assert run.retrieval_artifact_path is None
    assert run.index_build_status == "pending"
    assert run.trace_path is not None
    loaded_trace = TraceStore(config).load(run.trace_path)
    assert loaded_trace.index_build_status == "pending"
    assert loaded_trace.retrieval_artifact_path is None


def _envelope() -> AnnouncementEnvelope:
    return AnnouncementEnvelope(
        announcement_id="ann-index-1",
        ts_code="600000.SH",
        title="重大合同公告",
        publish_time=datetime(2026, 4, 18, 9, 0, tzinfo=timezone.utc),
        official_url="https://static.sse.com.cn/disclosure/ann-index-1.pdf",
        source_exchange="sse",
        attachment_type="pdf",
    )


async def _fake_discovery(
    envelope: AnnouncementEnvelope,
    config: AnnouncementConfig,
) -> AnnouncementDiscoveryResult:
    document_path = Path(config.artifact_root) / "documents" / "ann-index-1.pdf"
    document_path.parent.mkdir(parents=True, exist_ok=True)
    document_path.write_bytes(b"%PDF mocked fixture")
    document = AnnouncementDocumentArtifact(
        announcement_id=envelope.announcement_id,
        ts_code=envelope.ts_code,
        title=envelope.title,
        publish_time=envelope.publish_time,
        content_hash="b" * 64,
        official_url=envelope.official_url,
        source_exchange=envelope.source_exchange,
        attachment_type=envelope.attachment_type,
        local_path=document_path,
        content_type="application/pdf",
        byte_size=document_path.stat().st_size,
        fetched_at=datetime(2026, 4, 18, 9, 30, tzinfo=timezone.utc),
    )
    return AnnouncementDiscoveryResult(status="fetched", document=document)


def _fake_parse(
    document: AnnouncementDocumentArtifact,
    config: AnnouncementConfig,
) -> ParsedAnnouncementArtifact:
    artifact = make_index_artifact(Path(config.artifact_root))
    return artifact.model_copy(
        update={
            "content_hash": document.content_hash,
            "parser_version": config.docling_version,
            "source_document": document,
        }
    )
