"""Sample retrieval query helper for offline announcement indexes."""

from __future__ import annotations

import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from .retrieval_artifact import (
    AnnouncementChunk,
    AnnouncementRetrievalArtifact,
    AnnouncementRetrievalHit,
)
from .vector_store import load_vector_index


def query(
    text: str,
    artifact: AnnouncementRetrievalArtifact,
    *,
    top_k: int = 5,
    embed_model: Any | None = None,
) -> list[AnnouncementRetrievalHit]:
    """Query a retrieval artifact and return chunk-level hits only."""

    query_text = text.strip()
    if not query_text:
        raise ValueError("query text must not be empty")
    if top_k <= 0:
        raise ValueError("top_k must be greater than 0")
    if not artifact.chunks:
        raise ValueError("retrieval artifact must include chunk metadata for query")

    index = load_vector_index(
        persist_dir=Path(artifact.index_ref),
        llama_index_version=artifact.llama_index_version,
        embed_model=embed_model,
    )
    vector_scores = _vector_scores(index, query_text, top_k=top_k)
    lexical_scores = {
        chunk.chunk_id: score
        for chunk in artifact.chunks
        if (score := _lexical_score(query_text, _chunk_search_text(chunk))) > 0.0
    }

    chunks_by_id = {chunk.chunk_id: chunk for chunk in artifact.chunks}
    candidate_ids = [
        chunk.chunk_id
        for chunk in artifact.chunks
        if chunk.chunk_id in lexical_scores or chunk.chunk_id in vector_scores
    ]
    ranked_ids = sorted(
        candidate_ids,
        key=lambda chunk_id: (
            max(lexical_scores.get(chunk_id, 0.0), vector_scores.get(chunk_id, 0.0)),
            -artifact.chunk_refs.index(chunk_id),
        ),
        reverse=True,
    )

    hits: list[AnnouncementRetrievalHit] = []
    for chunk_id in ranked_ids[:top_k]:
        chunk = chunks_by_id[chunk_id]
        score = max(lexical_scores.get(chunk_id, 0.0), vector_scores.get(chunk_id, 0.0))
        hits.append(_hit_from_chunk(chunk, query_text=query_text, score=score))
    return hits


def _vector_scores(index: Any, query_text: str, *, top_k: int) -> dict[str, float]:
    retriever = index.as_retriever(similarity_top_k=top_k)
    results = retriever.retrieve(query_text)
    scores: dict[str, float] = {}
    for result in results:
        node = getattr(result, "node", result)
        metadata = getattr(node, "metadata", {}) or {}
        chunk_id = metadata.get("chunk_id") if isinstance(metadata, Mapping) else None
        if not isinstance(chunk_id, str) or not chunk_id:
            continue
        raw_score = getattr(result, "score", 0.0)
        score = 0.0 if raw_score is None else max(float(raw_score), 0.0)
        scores[chunk_id] = max(scores.get(chunk_id, 0.0), score)
    return scores


def _lexical_score(query_text: str, chunk_text: str) -> float:
    terms = _query_terms(query_text)
    if not terms:
        return 0.0
    score = 0.0
    lowered_chunk = chunk_text.lower()
    for term in terms:
        lowered_term = term.lower()
        count = lowered_chunk.count(lowered_term)
        if count:
            score += count * max(len(term), 1)
    if score == 0.0:
        return 0.0
    return 1.0 + score / max(len(chunk_text), 1)


def _chunk_search_text(chunk: AnnouncementChunk) -> str:
    return "\n".join([chunk.text, *chunk.title_path])


def _query_terms(query_text: str) -> list[str]:
    terms = [query_text]
    terms.extend(term for term in re.split(r"\s+", query_text) if term)
    seen: set[str] = set()
    unique_terms: list[str] = []
    for term in terms:
        if term not in seen:
            seen.add(term)
            unique_terms.append(term)
    return unique_terms


def _hit_from_chunk(
    chunk: AnnouncementChunk,
    *,
    query_text: str,
    score: float,
) -> AnnouncementRetrievalHit:
    return AnnouncementRetrievalHit(
        chunk_id=chunk.chunk_id,
        announcement_id=chunk.announcement_id,
        section_id=chunk.section_id,
        table_ref=chunk.table_ref,
        score=score,
        quote=_quote(chunk.text, query_text),
        metadata={
            "chunk_type": chunk.chunk_type,
            "start_offset": chunk.start_offset,
            "end_offset": chunk.end_offset,
            "title_path": chunk.title_path,
            "source_reference": chunk.source_reference,
        },
    )


def _quote(chunk_text: str, query_text: str) -> str:
    index = chunk_text.lower().find(query_text.lower())
    if index == -1:
        return chunk_text[:320].strip() or chunk_text
    start = max(0, index - 80)
    end = min(len(chunk_text), index + len(query_text) + 160)
    return chunk_text[start:end].strip() or chunk_text[index:end]
