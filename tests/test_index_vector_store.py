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
from subsystem_announcement.index.retrieval_artifact import (
    AnnouncementChunk,
    AnnouncementEmbeddingStrategy,
    AnnouncementRetrievalArtifact,
)
from subsystem_announcement.index.sample_query import query
from subsystem_announcement.index.vector_store import build_vector_index

from .test_index_chunker import make_index_artifact


def test_build_vector_index_persists_simple_vector_store(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    installed = _install_fake_llama_index(monkeypatch)
    chunks = chunk_parsed_artifact(make_index_artifact(tmp_path))
    config = AnnouncementConfig(
        llama_index_version="llama-index-core==0.10.0",
        allow_test_mock_embeddings=True,
    )

    ref = build_vector_index(
        chunks,
        persist_dir=tmp_path / "vector-store",
        config=config,
    )

    assert ref.index_ref == str(tmp_path / "vector-store")
    assert ref.llama_index_version == "llama-index-core==0.10.0"
    assert ref.embedding_strategy.strategy_type == "test_mock"
    assert ref.embedding_strategy.model_dimension == 384
    assert ref.chunk_ids == [chunk.chunk_id for chunk in chunks]
    assert (tmp_path / "vector-store" / "fake_vector_index.json").exists()
    assert installed["build_node_ids"] == [chunk.chunk_id for chunk in chunks]
    assert installed["build_node_metadata"] == [
        {
            "chunk_id": chunk.chunk_id,
            "announcement_id": chunk.announcement_id,
            "chunk_type": chunk.chunk_type,
            "section_id": chunk.section_id,
            "table_ref": chunk.table_ref,
            "start_offset": chunk.start_offset,
            "end_offset": chunk.end_offset,
            "title_path": chunk.title_path,
            "source_reference": chunk.source_reference,
        }
        for chunk in chunks
    ]


def test_query_returns_section_and_table_keyword_hits(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_llama_index(monkeypatch)
    chunks = chunk_parsed_artifact(make_index_artifact(tmp_path))
    config = AnnouncementConfig(
        llama_index_version="llama-index-core==0.10.0",
        allow_test_mock_embeddings=True,
    )
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
        embedding_strategy=index_ref.embedding_strategy,
        chunk_count=len(chunks),
        built_at=index_ref.built_at,
        source_parsed_artifact_path=None,
        chunks=chunks,
    )

    section_hits = query("风险提示", artifact, top_k=1, config=config)
    table_hits = query("1000万元", artifact, top_k=1, config=config)

    assert section_hits[0].section_id == "sec-0002"
    assert section_hits[0].table_ref is None
    assert table_hits[0].table_ref == "tbl-0001"
    assert table_hits[0].metadata["chunk_type"] == "table"


def test_build_vector_index_rejects_implicit_mock_embeddings(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_llama_index(monkeypatch)
    chunks = chunk_parsed_artifact(make_index_artifact(tmp_path))
    config = AnnouncementConfig(llama_index_version="llama-index-core==0.10.0")

    with pytest.raises(RuntimeError, match="retrieval_embedding_adapter"):
        build_vector_index(
            chunks,
            persist_dir=tmp_path / "vector-store",
            config=config,
        )


def test_build_vector_index_uses_configured_embedding_adapter(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    installed = _install_fake_llama_index(monkeypatch)
    chunks = chunk_parsed_artifact(make_index_artifact(tmp_path))
    embedding = FakeSemanticEmbedding()
    adapter_module = types.ModuleType("fake_embedding_adapter")
    adapter_module.build_embedding = lambda: embedding
    monkeypatch.setitem(sys.modules, "fake_embedding_adapter", adapter_module)
    config = AnnouncementConfig(
        llama_index_version="llama-index-core==0.10.0",
        retrieval_embedding_adapter="fake_embedding_adapter:build_embedding",
    )

    build_vector_index(
        chunks,
        persist_dir=tmp_path / "vector-store",
        config=config,
    )

    assert installed["build_embed_models"] == [embedding]


def test_build_vector_index_records_configured_embedding_strategy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_llama_index(monkeypatch)
    chunks = chunk_parsed_artifact(make_index_artifact(tmp_path))
    embedding = FakeSemanticEmbedding()
    embedding.model_version = "semantic-fixture-v1"
    adapter_module = types.ModuleType("versioned_embedding_adapter")
    adapter_module.build_embedding = lambda: embedding
    monkeypatch.setitem(sys.modules, "versioned_embedding_adapter", adapter_module)
    config = AnnouncementConfig(
        llama_index_version="llama-index-core==0.10.0",
        retrieval_embedding_adapter="versioned_embedding_adapter:build_embedding",
    )

    ref = build_vector_index(
        chunks,
        persist_dir=tmp_path / "vector-store",
        config=config,
    )

    assert ref.embedding_strategy == AnnouncementEmbeddingStrategy(
        strategy_type="adapter",
        adapter_ref="versioned_embedding_adapter:build_embedding",
        model_ref=(
            f"{FakeSemanticEmbedding.__module__}."
            f"{FakeSemanticEmbedding.__qualname__}"
        ),
        model_version="semantic-fixture-v1",
        model_dimension=None,
        model_fingerprint=ref.embedding_strategy.model_fingerprint,
    )
    assert len(ref.embedding_strategy.model_fingerprint) == 64


def test_query_uses_semantic_vectors_without_exact_word_overlap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_llama_index(monkeypatch)
    embedding = FakeSemanticEmbedding()
    config = AnnouncementConfig(llama_index_version="llama-index-core==0.10.0")
    chunks = [
        _chunk(
            "chunk:ann-semantic:sec-0001:section:dividend",
            "sec-0001",
            "公司完成现金分红方案实施，权益分派已办理完毕。",
        ),
        _chunk(
            "chunk:ann-semantic:sec-0002:section:inquiry",
            "sec-0002",
            "公司收到上海证券交易所监管问询函，"
            "将按要求及时回复。",
        ),
    ]
    index_ref = build_vector_index(
        chunks,
        persist_dir=tmp_path / "vector-store",
        config=config,
        embed_model=embedding,
    )
    artifact = AnnouncementRetrievalArtifact(
        announcement_id="ann-semantic",
        chunk_refs=[chunk.chunk_id for chunk in chunks],
        index_ref=index_ref.index_ref,
        parser_version="docling==2.15.1",
        llama_index_version=index_ref.llama_index_version,
        embedding_strategy=index_ref.embedding_strategy,
        chunk_count=len(chunks),
        built_at=index_ref.built_at,
        source_parsed_artifact_path=None,
        chunks=chunks,
    )

    with pytest.raises(
        RuntimeError,
        match="Retrieval embedding model is not configured",
    ):
        query("交易所关注函", artifact, top_k=1)

    assert "交易所关注函" not in chunks[1].text
    hits = query("交易所关注函", artifact, top_k=1, embed_model=embedding)
    assert hits[0].chunk_id == chunks[1].chunk_id


def test_build_vector_index_pins_chunk_identity_through_load_and_query(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    installed = _install_fake_llama_index(monkeypatch)
    embedding = FakeSemanticEmbedding()
    config = AnnouncementConfig(llama_index_version="llama-index-core==0.10.0")
    chunks = [
        _chunk(
            "chunk:ann-semantic:sec-0001:section:inquiry",
            "sec-0001",
            "公司收到上海证券交易所监管问询函，将按要求及时回复。",
        ),
        _chunk(
            "chunk:ann-semantic:sec-0002:section:dividend",
            "sec-0002",
            "公司完成现金分红方案实施，权益分派已办理完毕。",
        ),
    ]
    index_ref = build_vector_index(
        chunks,
        persist_dir=tmp_path / "vector-store",
        config=config,
        embed_model=embedding,
    )
    artifact = AnnouncementRetrievalArtifact(
        announcement_id="ann-semantic",
        chunk_refs=[chunk.chunk_id for chunk in chunks],
        index_ref=index_ref.index_ref,
        parser_version="docling==2.15.1",
        llama_index_version=index_ref.llama_index_version,
        embedding_strategy=index_ref.embedding_strategy,
        chunk_count=len(chunks),
        built_at=index_ref.built_at,
        source_parsed_artifact_path=None,
        chunks=chunks,
    )

    persisted = json.loads(
        (tmp_path / "vector-store" / "fake_vector_index.json").read_text(
            encoding="utf-8"
        )
    )
    hits = query("交易所关注函", artifact, top_k=1, embed_model=embedding)

    assert [node["node_id"] for node in persisted["nodes"]] == [
        chunk.chunk_id for chunk in chunks
    ]
    assert [node["metadata"]["chunk_id"] for node in persisted["nodes"]] == [
        chunk.chunk_id for chunk in chunks
    ]
    assert installed["load_node_ids"] == [chunk.chunk_id for chunk in chunks]
    assert hits[0].chunk_id == chunks[0].chunk_id


def test_query_semantic_hit_beats_lexical_only_distractor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_llama_index(monkeypatch)
    embedding = FakeSemanticEmbedding()
    config = AnnouncementConfig(llama_index_version="llama-index-core==0.10.0")
    chunks = [
        _chunk(
            "chunk:ann-semantic:sec-0001:section:lexical-distractor",
            "sec-0001",
            "术语说明：交易所关注函一词出现在历史背景介绍中。",
        ),
        _chunk(
            "chunk:ann-semantic:sec-0002:section:semantic-match",
            "sec-0002",
            "公司收到上海证券交易所监管问询函，"
            "将按要求及时回复。",
        ),
    ]
    index_ref = build_vector_index(
        chunks,
        persist_dir=tmp_path / "vector-store",
        config=config,
        embed_model=embedding,
    )
    artifact = AnnouncementRetrievalArtifact(
        announcement_id="ann-semantic",
        chunk_refs=[chunk.chunk_id for chunk in chunks],
        index_ref=index_ref.index_ref,
        parser_version="docling==2.15.1",
        llama_index_version=index_ref.llama_index_version,
        embedding_strategy=index_ref.embedding_strategy,
        chunk_count=len(chunks),
        built_at=index_ref.built_at,
        source_parsed_artifact_path=None,
        chunks=chunks,
    )

    hits = query("交易所关注函", artifact, top_k=1, embed_model=embedding)

    assert hits[0].chunk_id == chunks[1].chunk_id
    assert "交易所关注函" not in chunks[1].text


def test_query_rejects_embedding_strategy_mismatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_llama_index(monkeypatch)
    embedding = FakeSemanticEmbedding()
    config = AnnouncementConfig(llama_index_version="llama-index-core==0.10.0")
    chunks = [
        _chunk(
            "chunk:ann-semantic:sec-0001:section:inquiry",
            "sec-0001",
            "公司收到上海证券交易所监管问询函，"
            "将按要求及时回复。",
        )
    ]
    index_ref = build_vector_index(
        chunks,
        persist_dir=tmp_path / "vector-store",
        config=config,
        embed_model=embedding,
    )
    artifact = AnnouncementRetrievalArtifact(
        announcement_id="ann-semantic",
        chunk_refs=[chunk.chunk_id for chunk in chunks],
        index_ref=index_ref.index_ref,
        parser_version="docling==2.15.1",
        llama_index_version=index_ref.llama_index_version,
        embedding_strategy=index_ref.embedding_strategy,
        chunk_count=len(chunks),
        built_at=index_ref.built_at,
        source_parsed_artifact_path=None,
        chunks=chunks,
    )

    with pytest.raises(RuntimeError, match="embedding strategy mismatch"):
        query(
            "交易所关注函",
            artifact,
            top_k=1,
            config=AnnouncementConfig(
                llama_index_version="llama-index-core==0.10.0",
                allow_test_mock_embeddings=True,
            ),
        )


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
    config = AnnouncementConfig(
        llama_index_version="llama-index-core==0.10.0",
        allow_test_mock_embeddings=True,
    )
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


class FakeSemanticEmbedding:
    def embed(self, text: str) -> list[float]:
        if "术语说明" in text:
            return [0.0, 1.0]
        if "交易所关注函" in text or "监管问询函" in text:
            return [1.0, 0.0]
        if "分红" in text:
            return [0.0, 1.0]
        return [0.0, 0.0]


def _chunk(chunk_id: str, section_id: str, text: str) -> AnnouncementChunk:
    return AnnouncementChunk(
        chunk_id=chunk_id,
        announcement_id="ann-semantic",
        chunk_type="section",
        section_id=section_id,
        table_ref=None,
        text=text,
        start_offset=0,
        end_offset=len(text),
        title_path=["测试公告"],
        source_reference={"official_url": "https://example.test/ann-semantic.pdf"},
    )


def _install_fake_llama_index(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    calls: dict[str, Any] = {
        "build_node_ids": [],
        "build_node_metadata": [],
        "load_node_ids": [],
        "build_embed_models": [],
        "load_embed_models": [],
    }

    class FakeTextNode:
        def __init__(
            self,
            *,
            text: str,
            metadata: dict[str, Any],
            id_: str | None = None,
        ) -> None:
            self.text = text
            self.metadata = metadata
            self.id_ = id_ or "generated-node-id"

        @property
        def node_id(self) -> str:
            return self.id_

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
            self.nodes: list[FakeTextNode] = []
            self.embeddings: list[list[float]] = []
            if persist_dir is not None:
                index_path = Path(persist_dir) / "fake_vector_index.json"
                data = json.loads(index_path.read_text(encoding="utf-8"))
                self.nodes = [
                    FakeTextNode(
                        text=item["text"],
                        metadata=item["metadata"],
                        id_=item["node_id"],
                    )
                    for item in data["nodes"]
                ]
                self.embeddings = data.get("embeddings", [])

        @classmethod
        def from_defaults(cls, **kwargs):
            return cls(**kwargs)

        def persist(self, *, persist_dir: str) -> None:
            output_dir = Path(persist_dir)
            output_dir.mkdir(parents=True, exist_ok=True)
            payload = {
                "nodes": [
                    {
                        "node_id": node.node_id,
                        "text": node.text,
                        "metadata": node.metadata,
                    }
                    for node in self.nodes
                ],
                "embeddings": self.embeddings,
            }
            (output_dir / "fake_vector_index.json").write_text(
                json.dumps(payload, ensure_ascii=False),
                encoding="utf-8",
            )

    class FakeVectorStoreIndex:
        def __init__(
            self,
            *,
            nodes: list[FakeTextNode],
            storage_context: FakeStorageContext,
            embed_model: Any,
            embeddings: list[list[float]] | None = None,
        ) -> None:
            if embeddings is None:
                calls["build_node_ids"] = [node.node_id for node in nodes]
                calls["build_node_metadata"] = [node.metadata for node in nodes]
                calls["build_embed_models"].append(embed_model)
            self.nodes = nodes
            self.storage_context = storage_context
            self.storage_context.nodes = nodes
            self.embed_model = embed_model
            self.embeddings = embeddings or [
                _embed(embed_model, node.text) for node in nodes
            ]
            self.storage_context.embeddings = self.embeddings

        def as_retriever(self, *, similarity_top_k: int):
            return FakeRetriever(
                self.nodes,
                self.embeddings,
                self.embed_model,
                similarity_top_k,
            )

    class FakeRetriever:
        def __init__(
            self,
            nodes,
            embeddings: list[list[float]],
            embed_model: Any,
            top_k: int,
        ) -> None:
            self.nodes = nodes
            self.embeddings = embeddings
            self.embed_model = embed_model
            self.top_k = top_k

        def retrieve(self, text: str):
            query_embedding = _embed(self.embed_model, text)
            if query_embedding and any(self.embeddings):
                ranked_with_scores = sorted(
                    zip(self.nodes, self.embeddings, strict=True),
                    key=lambda item: _dot(query_embedding, item[1]),
                    reverse=True,
                )
                return [
                    types.SimpleNamespace(
                        node=node,
                        score=_dot(query_embedding, embedding),
                    )
                    for node, embedding in ranked_with_scores[: self.top_k]
                ]

            ranked = sorted(
                self.nodes,
                key=lambda node: text in node.text,
                reverse=True,
            )
            return [
                types.SimpleNamespace(
                    node=node,
                    score=1.0 if text in node.text else 0.01,
                )
                for node in ranked[: self.top_k]
            ]

    class FakeMockEmbedding:
        def __init__(self, *, embed_dim: int) -> None:
            self.embed_dim = embed_dim

    def fake_load_index_from_storage(storage_context, embed_model=None):
        assert embed_model is not None
        calls["load_embed_models"].append(embed_model)
        calls["load_node_ids"] = [node.node_id for node in storage_context.nodes]
        return FakeVectorStoreIndex(
            nodes=storage_context.nodes,
            storage_context=storage_context,
            embed_model=embed_model,
            embeddings=storage_context.embeddings,
        )

    llama_index_module = types.ModuleType("llama_index")
    llama_index_module.__path__ = []
    core_module = types.ModuleType("llama_index.core")
    core_module.StorageContext = FakeStorageContext
    core_module.VectorStoreIndex = FakeVectorStoreIndex
    core_module.load_index_from_storage = fake_load_index_from_storage
    schema_module = types.ModuleType("llama_index.core.schema")
    schema_module.TextNode = FakeTextNode
    vector_stores_module = types.ModuleType("llama_index.core.vector_stores")
    vector_stores_module.SimpleVectorStore = FakeSimpleVectorStore
    embeddings_module = types.ModuleType("llama_index.core.embeddings")
    embeddings_module.__path__ = []
    mock_embedding_module = types.ModuleType(
        "llama_index.core.embeddings.mock_embed_model"
    )
    mock_embedding_module.MockEmbedding = FakeMockEmbedding

    monkeypatch.setitem(sys.modules, "llama_index", llama_index_module)
    monkeypatch.setitem(sys.modules, "llama_index.core", core_module)
    monkeypatch.setitem(sys.modules, "llama_index.core.schema", schema_module)
    monkeypatch.setitem(
        sys.modules,
        "llama_index.core.vector_stores",
        vector_stores_module,
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


def _embed(embed_model: Any, text: str) -> list[float]:
    embed = getattr(embed_model, "embed", None)
    if embed is None:
        return []
    return list(embed(text))


def _dot(left: list[float], right: list[float]) -> float:
    return sum(a * b for a, b in zip(left, right, strict=False))
