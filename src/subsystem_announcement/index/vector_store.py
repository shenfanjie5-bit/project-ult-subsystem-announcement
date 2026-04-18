"""LlamaIndex vector store integration for announcement chunks."""

from __future__ import annotations

import hashlib
import importlib
import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from importlib import metadata
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from subsystem_announcement.config import AnnouncementConfig

from .retrieval_artifact import AnnouncementChunk, AnnouncementEmbeddingStrategy


class AnnouncementVectorIndexRef(BaseModel):
    """Local reference to a persisted vector index."""

    model_config = ConfigDict(extra="forbid")

    index_ref: str = Field(min_length=1)
    llama_index_version: str = Field(min_length=1)
    embedding_strategy: AnnouncementEmbeddingStrategy
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
    embedding = _resolve_embed_model(config=config, embed_model=embed_model)
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
        embedding_strategy=_embedding_strategy_from_model(
            config=config,
            embed_model=embed_model,
            embedding=embedding,
        ),
        chunk_ids=[chunk.chunk_id for chunk in chunks],
        built_at=datetime.now(timezone.utc),
    )


def load_vector_index(
    *,
    persist_dir: Path,
    llama_index_version: str,
    config: AnnouncementConfig | None = None,
    embed_model: Any | None = None,
    embedding_strategy: AnnouncementEmbeddingStrategy | None = None,
) -> Any:
    """Load a persisted LlamaIndex vector index for retrieval."""

    resolve_llama_index_version(llama_index_version)
    api = _load_llama_index_api()
    embedding = _resolve_embed_model(config=config, embed_model=embed_model)
    if embedding_strategy is not None:
        _validate_embedding_strategy(
            expected=embedding_strategy,
            actual=_embedding_strategy_from_model(
                config=config,
                embed_model=embed_model,
                embedding=embedding,
            ),
        )
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


def _resolve_embed_model(
    *,
    config: AnnouncementConfig | None,
    embed_model: Any | None,
) -> Any:
    if embed_model is not None:
        return embed_model
    if config is not None and config.retrieval_embedding_adapter is not None:
        return _load_embedding_adapter(config.retrieval_embedding_adapter)
    if config is not None and config.allow_test_mock_embeddings:
        return _mock_embed_model()
    raise RuntimeError(
        "Retrieval embedding model is not configured. Set "
        "AnnouncementConfig.retrieval_embedding_adapter to a module:attribute "
        "adapter, pass embed_model explicitly, or set "
        "allow_test_mock_embeddings=True only in tests."
    )


def _load_embedding_adapter(adapter_ref: str) -> Any:
    module_name, object_path = adapter_ref.split(":", 1)
    try:
        module = importlib.import_module(module_name)
    except (ImportError, ModuleNotFoundError) as exc:
        raise RuntimeError(
            "Unable to import retrieval embedding adapter module "
            f"{module_name!r}."
        ) from exc

    adapter: Any = module
    try:
        for name in object_path.split("."):
            adapter = getattr(adapter, name)
    except AttributeError as exc:
        raise RuntimeError(
            "Unable to resolve retrieval embedding adapter object "
            f"{adapter_ref!r}."
        ) from exc

    if isinstance(adapter, type) or (
        callable(adapter) and not _looks_like_embedding_model(adapter)
    ):
        try:
            adapter = adapter()
        except TypeError as exc:
            raise RuntimeError(
                "Retrieval embedding adapter factory must be callable without "
                f"arguments: {adapter_ref!r}."
            ) from exc

    if adapter is None:
        raise RuntimeError(
            "Retrieval embedding adapter resolved to None: "
            f"{adapter_ref!r}."
        )
    return adapter


def _looks_like_embedding_model(value: Any) -> bool:
    return any(
        hasattr(value, name)
        for name in (
            "get_text_embedding",
            "get_query_embedding",
            "_get_text_embedding",
            "_get_query_embedding",
        )
    )


def _mock_embed_model() -> Any:
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


def _embedding_strategy_from_model(
    *,
    config: AnnouncementConfig | None,
    embed_model: Any | None,
    embedding: Any,
) -> AnnouncementEmbeddingStrategy:
    strategy_type = _embedding_strategy_type(config=config, embed_model=embed_model)
    adapter_ref = (
        config.retrieval_embedding_adapter
        if strategy_type == "adapter" and config is not None
        else None
    )
    model_ref = _model_ref(embedding)
    model_version = _string_identity_attr(
        embedding,
        ("model_version", "version", "revision", "__version__"),
    )
    model_dimension = _int_identity_attr(
        embedding,
        ("embed_dim", "embedding_dim", "dimensions", "dimension"),
    )
    identity_payload = {
        "strategy_type": strategy_type,
        "adapter_ref": adapter_ref,
        "model_ref": model_ref,
        "model_version": model_version,
        "model_dimension": model_dimension,
        "identity_attributes": _embedding_identity_attrs(embedding),
    }
    fingerprint = hashlib.sha256(
        json.dumps(
            identity_payload,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    return AnnouncementEmbeddingStrategy(
        strategy_type=strategy_type,
        adapter_ref=adapter_ref,
        model_ref=model_ref,
        model_version=model_version,
        model_dimension=model_dimension,
        model_fingerprint=fingerprint,
    )


def _embedding_strategy_type(
    *,
    config: AnnouncementConfig | None,
    embed_model: Any | None,
) -> str:
    if embed_model is not None:
        return "injected"
    if config is not None and config.retrieval_embedding_adapter is not None:
        return "adapter"
    return "test_mock"


def _validate_embedding_strategy(
    *,
    expected: AnnouncementEmbeddingStrategy,
    actual: AnnouncementEmbeddingStrategy,
) -> None:
    if expected == actual:
        return
    raise RuntimeError(
        "Retrieval embedding strategy mismatch: index was built with "
        f"{_describe_embedding_strategy(expected)}, but query is configured with "
        f"{_describe_embedding_strategy(actual)}. Rebuild the retrieval artifact "
        "or query it with the same embedding adapter/model."
    )


def _describe_embedding_strategy(strategy: AnnouncementEmbeddingStrategy) -> str:
    adapter = f" adapter={strategy.adapter_ref!r}" if strategy.adapter_ref else ""
    version = f" version={strategy.model_version!r}" if strategy.model_version else ""
    dimension = (
        f" dimension={strategy.model_dimension}"
        if strategy.model_dimension is not None
        else ""
    )
    return (
        f"type={strategy.strategy_type!r}{adapter} model={strategy.model_ref!r}"
        f"{version}{dimension} fingerprint={strategy.model_fingerprint[:12]}"
    )


def _model_ref(value: Any) -> str:
    model_type = type(value)
    return f"{model_type.__module__}.{model_type.__qualname__}"


def _embedding_identity_attrs(value: Any) -> dict[str, str | int | float | bool | None]:
    attrs: dict[str, str | int | float | bool | None] = {}
    for name in (
        "model_name",
        "model",
        "model_id",
        "model_version",
        "version",
        "revision",
        "__version__",
        "embed_dim",
        "embedding_dim",
        "dimensions",
        "dimension",
    ):
        attr_value = _safe_getattr(value, name)
        if isinstance(attr_value, str | int | float | bool) or attr_value is None:
            attrs[name] = attr_value
    return attrs


def _string_identity_attr(value: Any, names: tuple[str, ...]) -> str | None:
    for name in names:
        attr_value = _safe_getattr(value, name)
        if isinstance(attr_value, str) and attr_value.strip():
            return attr_value.strip()
    return None


def _int_identity_attr(value: Any, names: tuple[str, ...]) -> int | None:
    for name in names:
        attr_value = _safe_getattr(value, name)
        if isinstance(attr_value, bool):
            continue
        if isinstance(attr_value, int) and attr_value > 0:
            return attr_value
    return None


def _safe_getattr(value: Any, name: str) -> Any:
    try:
        return getattr(value, name)
    except Exception:
        return None


def _try_load_settings() -> Any | None:
    try:
        from llama_index.core import Settings  # type: ignore[import-not-found]
    except (ImportError, ModuleNotFoundError):
        return None
    return Settings
