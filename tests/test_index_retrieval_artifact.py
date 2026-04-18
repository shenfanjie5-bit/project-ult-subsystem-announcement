from __future__ import annotations

import inspect
from datetime import datetime, timezone
from pathlib import Path

from subsystem_announcement.config import AnnouncementConfig
from subsystem_announcement.index.retrieval_artifact import (
    AnnouncementRetrievalArtifact,
    build_retrieval_artifact,
    load_retrieval_artifact,
    write_retrieval_artifact,
)
from subsystem_announcement.index.vector_store import AnnouncementVectorIndexRef
from subsystem_announcement.runtime.pipeline import AnnouncementPipeline

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


def test_pipeline_process_envelope_does_not_build_retrieval_index() -> None:
    source = inspect.getsource(AnnouncementPipeline.process_envelope)

    assert "build_retrieval_artifact" not in source
