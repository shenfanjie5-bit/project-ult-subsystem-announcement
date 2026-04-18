"""LlamaIndex vector store integration for announcement chunks."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from importlib import metadata
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from subsystem_announcement.config import AnnouncementConfig

from .retrieval_artifact import AnnouncementChunk


class AnnouncementVectorIndexRef(BaseModel):
    """Local reference to a persisted vector index."""

    model_config = ConfigDict(extra="forbid")

    index_ref: str = Field(min_length=1)
    llama_index_version: str = Field(min_length=1)
    chunk_ids: list[str] = Field(default_factory=list)
    built_at: datetime

    @field_validator("built_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        """Reject naive index build timestamps."""

        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("built_at must include timezone information")
        return value


@dataclass(frozen=True)
class _LlamaIndexApi:
    Document: Any
    StorageContext: Any
    VectorStoreIndex: Any
    SimpleVectorStore: Any
    DoclingNodeParser: Any
    load_index_from_storage: Any


def build_vector_index(
    chunks: Sequence[AnnouncementChunk],
    *,
    persist_dir: Path,
    config: AnnouncementConfig,
    embed_model: Any | None = None,
) -> AnnouncementVectorIndexRef:
    """Build and persist a LlamaIndex SimpleVectorStore for retrieval chunks."""

    if not chunks:
        raise ValueError("chunks must contain at least one retrieval chunk")

    llama_index_version = resolve_llama_index_version(config.llama_index_version)
    api = _load_llama_index_api()
    embedding = embed_model if embed_model is not None else _default_embed_model()
    documents = [_document_from_chunk(api.Document, chunk) for chunk in chunks]
    vector_store = api.SimpleVectorStore()
    storage_context = api.StorageContext.from_defaults(vector_store=vector_store)
    node_parser = api.DoclingNodeParser()

    index = _index_from_documents(
        api,
        documents,
        storage_context=storage_context,
        node_parser=node_parser,
        embed_model=embedding,
    )

    output_dir = Path(persist_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    index_storage_context = getattr(index, "storage_context", storage_context)
    index_storage_context.persist(persist_dir=str(output_dir))
    return AnnouncementVectorIndexRef(
        index_ref=str(output_dir),
        llama_index_version=llama_index_version,
        chunk_ids=[chunk.chunk_id for chunk in chunks],
        built_at=datetime.now(timezone.utc),
    )


def load_vector_index(
    *,
    persist_dir: Path,
    llama_index_version: str,
    embed_model: Any | None = None,
) -> Any:
    """Load a persisted LlamaIndex vector index for retrieval."""

    resolve_llama_index_version(llama_index_version)
    api = _load_llama_index_api()
    embedding = embed_model if embed_model is not None else _default_embed_model()
    storage_context = api.StorageContext.from_defaults(persist_dir=str(persist_dir))
    try:
        return api.load_index_from_storage(
            storage_context,
            embed_model=embedding,
        )
    except TypeError:
        settings = _try_load_settings()
        if settings is None:
            return api.load_index_from_storage(storage_context)
        previous_embed_model = getattr(settings, "embed_model", None)
        settings.embed_model = embedding
        try:
            return api.load_index_from_storage(storage_context)
        finally:
            settings.embed_model = previous_embed_model


def resolve_llama_index_version(version_pin: str) -> str:
    """Validate and resolve the exact installed LlamaIndex core version."""

    configured = version_pin.strip()
    if configured == "not-configured":
        raise RuntimeError(
            "LlamaIndex version is not configured. Set "
            "AnnouncementConfig.llama_index_version to an exact pin such as "
            "llama-index-core==0.10.0 before building retrieval indexes."
        )
    if "==" not in configured:
        raise RuntimeError(
            "LlamaIndex version must be an exact package pin, got "
            f"{configured!r}."
        )
    package_name, expected_version = configured.split("==", 1)
    try:
        installed_version = metadata.version(package_name)
    except metadata.PackageNotFoundError as exc:
        raise RuntimeError(
            "LlamaIndex dependency is not installed for the configured exact "
            f"pin {configured!r}. Install the locked LlamaIndex and "
            "DoclingNodeParser dependencies before building retrieval indexes."
        ) from exc
    if installed_version != expected_version:
        raise RuntimeError(
            "LlamaIndex version mismatch: configured "
            f"{configured!r}, installed {package_name}=={installed_version!r}."
        )
    return f"{package_name}=={installed_version}"


def _document_from_chunk(Document: Any, chunk: AnnouncementChunk) -> Any:
    metadata_payload = {
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
    return Document(text=chunk.text, metadata=metadata_payload)


def _index_from_documents(
    api: _LlamaIndexApi,
    documents: Sequence[Any],
    *,
    storage_context: Any,
    node_parser: Any,
    embed_model: Any,
) -> Any:
    try:
        return api.VectorStoreIndex.from_documents(
            documents,
            storage_context=storage_context,
            transformations=[node_parser],
            embed_model=embed_model,
        )
    except TypeError:
        settings = _try_load_settings()
        if settings is None:
            return api.VectorStoreIndex.from_documents(
                documents,
                storage_context=storage_context,
                transformations=[node_parser],
            )
        previous_embed_model = getattr(settings, "embed_model", None)
        settings.embed_model = embed_model
        try:
            return api.VectorStoreIndex.from_documents(
                documents,
                storage_context=storage_context,
                transformations=[node_parser],
            )
        finally:
            settings.embed_model = previous_embed_model


def _load_llama_index_api() -> _LlamaIndexApi:
    try:
        from llama_index.core import (  # type: ignore[import-not-found]
            Document,
            StorageContext,
            VectorStoreIndex,
            load_index_from_storage,
        )
    except (ImportError, ModuleNotFoundError) as exc:
        raise RuntimeError(
            "LlamaIndex core is required for announcement retrieval indexes. "
            "Install the exact llama-index-core pin."
        ) from exc

    try:
        from llama_index.core.vector_stores import (  # type: ignore[import-not-found]
            SimpleVectorStore,
        )
    except (ImportError, ModuleNotFoundError):
        try:
            from llama_index.core.vector_stores.simple import (  # type: ignore[import-not-found]
                SimpleVectorStore,
            )
        except (ImportError, ModuleNotFoundError) as exc:
            raise RuntimeError(
                "LlamaIndex SimpleVectorStore is required for announcement "
                "retrieval indexes. Install the exact llama-index-core pin."
            ) from exc

    try:
        from llama_index.node_parser.docling import (  # type: ignore[import-not-found]
            DoclingNodeParser,
        )
    except (ImportError, ModuleNotFoundError):
        try:
            from llama_index.node_parser.docling.base import (  # type: ignore[import-not-found]
                DoclingNodeParser,
            )
        except (ImportError, ModuleNotFoundError) as exc:
            raise RuntimeError(
                "LlamaIndex DoclingNodeParser is required for announcement "
                "retrieval indexes. Install the exact "
                "llama-index-node-parser-docling pin."
            ) from exc
    return _LlamaIndexApi(
        Document=Document,
        StorageContext=StorageContext,
        VectorStoreIndex=VectorStoreIndex,
        SimpleVectorStore=SimpleVectorStore,
        DoclingNodeParser=DoclingNodeParser,
        load_index_from_storage=load_index_from_storage,
    )


def _default_embed_model() -> Any:
    try:
        from llama_index.core.embeddings.mock_embed_model import (  # type: ignore[import-not-found]
            MockEmbedding,
        )
    except (ImportError, ModuleNotFoundError):
        try:
            from llama_index.core.embeddings import MockEmbedding  # type: ignore[import-not-found]
        except (ImportError, ModuleNotFoundError) as exc:
            raise RuntimeError(
                "LlamaIndex MockEmbedding is required for offline retrieval index "
                "builds when no embed_model is explicitly provided."
            ) from exc
    return MockEmbedding(embed_dim=384)


def _try_load_settings() -> Any | None:
    try:
        from llama_index.core import Settings  # type: ignore[import-not-found]
    except (ImportError, ModuleNotFoundError):
        return None
    return Settings
