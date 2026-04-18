from __future__ import annotations

import json
import sys
import types
from pathlib import Path
from typing import Any

import pytest

import subsystem_announcement.index.vector_store as vector_store
from subsystem_announcement.config import AnnouncementConfig
from subsystem_announcement.index import chunk_parsed_artifact
from subsystem_announcement.index.retrieval_artifact import AnnouncementRetrievalArtifact
from subsystem_announcement.index.sample_query import query
from subsystem_announcement.index.vector_store import build_vector_index

from .test_index_chunker import make_index_artifact


def test_build_vector_index_persists_simple_vector_store(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    installed = _install_fake_llama_index(monkeypatch)
    chunks = chunk_parsed_artifact(make_index_artifact(tmp_path))
    config = AnnouncementConfig(llama_index_version="llama-index-core==0.10.0")

    ref = build_vector_index(
        chunks,
        persist_dir=tmp_path / "vector-store",
        config=config,
    )

    assert ref.index_ref == str(tmp_path / "vector-store")
    assert ref.llama_index_version == "llama-index-core==0.10.0"
    assert ref.chunk_ids == [chunk.chunk_id for chunk in chunks]
    assert (tmp_path / "vector-store" / "fake_vector_index.json").exists()
    assert installed["docling_parser_calls"] == 1


def test_query_returns_section_and_table_keyword_hits(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_llama_index(monkeypatch)
    chunks = chunk_parsed_artifact(make_index_artifact(tmp_path))
    config = AnnouncementConfig(llama_index_version="llama-index-core==0.10.0")
    index_ref = build_vector_index(
        chunks,
        persist_dir=tmp_path / "vector-store",
        config=config,
    )
    artifact = AnnouncementRetrievalArtifact(
        announcement_id="ann-index-1",
        chunk_refs=[chunk.chunk_id for chunk in chunks],
        index_ref=index_ref.index_ref,
        parser_version="docling==2.15.1",
        llama_index_version=index_ref.llama_index_version,
        chunk_count=len(chunks),
        built_at=index_ref.built_at,
        source_parsed_artifact_path=None,
        chunks=chunks,
    )

    section_hits = query("风险提示", artifact, top_k=1)
    table_hits = query("1000万元", artifact, top_k=1)

    assert section_hits[0].section_id == "sec-0002"
    assert section_hits[0].table_ref is None
    assert table_hits[0].table_ref == "tbl-0001"
    assert table_hits[0].metadata["chunk_type"] == "table"


def test_build_vector_index_rejects_unconfigured_llama_index(
    tmp_path: Path,
) -> None:
    chunks = chunk_parsed_artifact(make_index_artifact(tmp_path))

    with pytest.raises(RuntimeError, match="LlamaIndex version is not configured"):
        build_vector_index(
            chunks,
            persist_dir=tmp_path / "vector-store",
            config=AnnouncementConfig(),
        )


def test_build_vector_index_reports_missing_llama_index_dependency(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    chunks = chunk_parsed_artifact(make_index_artifact(tmp_path))
    config = AnnouncementConfig(llama_index_version="llama-index-core==0.10.0")
    monkeypatch.setattr(
        vector_store.metadata,
        "version",
        lambda package_name: "0.10.0",
    )
    monkeypatch.setitem(sys.modules, "llama_index", None)

    with pytest.raises(RuntimeError, match="LlamaIndex core"):
        build_vector_index(
            chunks,
            persist_dir=tmp_path / "vector-store",
            config=config,
        )


def _install_fake_llama_index(monkeypatch: pytest.MonkeyPatch) -> dict[str, int]:
    calls = {"docling_parser_calls": 0}

    class FakeDocument:
        def __init__(self, *, text: str, metadata: dict[str, Any]) -> None:
            self.text = text
            self.metadata = metadata

        def get_content(self) -> str:
            return self.text

    class FakeSimpleVectorStore:
        pass

    class FakeStorageContext:
        def __init__(
            self,
            *,
            vector_store: Any | None = None,
            persist_dir: str | None = None,
        ) -> None:
            self.vector_store = vector_store
            self.persist_dir = persist_dir
            self.documents: list[FakeDocument] = []
            if persist_dir is not None:
                index_path = Path(persist_dir) / "fake_vector_index.json"
                data = json.loads(index_path.read_text(encoding="utf-8"))
                self.documents = [
                    FakeDocument(text=item["text"], metadata=item["metadata"])
                    for item in data["documents"]
                ]

        @classmethod
        def from_defaults(cls, **kwargs):
            return cls(**kwargs)

        def persist(self, *, persist_dir: str) -> None:
            output_dir = Path(persist_dir)
            output_dir.mkdir(parents=True, exist_ok=True)
            payload = {
                "documents": [
                    {"text": document.text, "metadata": document.metadata}
                    for document in self.documents
                ]
            }
            (output_dir / "fake_vector_index.json").write_text(
                json.dumps(payload, ensure_ascii=False),
                encoding="utf-8",
            )

    class FakeDoclingNodeParser:
        def __init__(self) -> None:
            calls["docling_parser_calls"] += 1

    class FakeVectorStoreIndex:
        def __init__(
            self,
            documents: list[FakeDocument],
            storage_context: FakeStorageContext,
        ) -> None:
            self.documents = documents
            self.storage_context = storage_context
            self.storage_context.documents = documents

        @classmethod
        def from_documents(
            cls,
            documents,
            *,
            storage_context,
            transformations,
            embed_model=None,
        ):
            assert transformations
            assert isinstance(transformations[0], FakeDoclingNodeParser)
            assert embed_model is not None
            return cls(list(documents), storage_context)

        def as_retriever(self, *, similarity_top_k: int):
            return FakeRetriever(self.documents, similarity_top_k)

    class FakeRetriever:
        def __init__(self, documents, top_k: int) -> None:
            self.documents = documents
            self.top_k = top_k

        def retrieve(self, text: str):
            ranked = sorted(
                self.documents,
                key=lambda document: text in document.text,
                reverse=True,
            )
            return [
                types.SimpleNamespace(
                    node=document,
                    score=1.0 if text in document.text else 0.01,
                )
                for document in ranked[: self.top_k]
            ]

    class FakeMockEmbedding:
        def __init__(self, *, embed_dim: int) -> None:
            self.embed_dim = embed_dim

    def fake_load_index_from_storage(storage_context, embed_model=None):
        assert embed_model is not None
        return FakeVectorStoreIndex(storage_context.documents, storage_context)

    llama_index_module = types.ModuleType("llama_index")
    llama_index_module.__path__ = []
    core_module = types.ModuleType("llama_index.core")
    core_module.Document = FakeDocument
    core_module.StorageContext = FakeStorageContext
    core_module.VectorStoreIndex = FakeVectorStoreIndex
    core_module.load_index_from_storage = fake_load_index_from_storage
    vector_stores_module = types.ModuleType("llama_index.core.vector_stores")
    vector_stores_module.SimpleVectorStore = FakeSimpleVectorStore
    node_parser_module = types.ModuleType("llama_index.node_parser")
    node_parser_module.__path__ = []
    docling_module = types.ModuleType("llama_index.node_parser.docling")
    docling_module.DoclingNodeParser = FakeDoclingNodeParser
    embeddings_module = types.ModuleType("llama_index.core.embeddings")
    embeddings_module.__path__ = []
    mock_embedding_module = types.ModuleType(
        "llama_index.core.embeddings.mock_embed_model"
    )
    mock_embedding_module.MockEmbedding = FakeMockEmbedding

    monkeypatch.setitem(sys.modules, "llama_index", llama_index_module)
    monkeypatch.setitem(sys.modules, "llama_index.core", core_module)
    monkeypatch.setitem(
        sys.modules,
        "llama_index.core.vector_stores",
        vector_stores_module,
    )
    monkeypatch.setitem(sys.modules, "llama_index.node_parser", node_parser_module)
    monkeypatch.setitem(
        sys.modules,
        "llama_index.node_parser.docling",
        docling_module,
    )
    monkeypatch.setitem(sys.modules, "llama_index.core.embeddings", embeddings_module)
    monkeypatch.setitem(
        sys.modules,
        "llama_index.core.embeddings.mock_embed_model",
        mock_embedding_module,
    )
    monkeypatch.setattr(
        vector_store.metadata,
        "version",
        lambda package_name: "0.10.0",
    )
    return calls
