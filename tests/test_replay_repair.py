from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from subsystem_announcement.config import AnnouncementConfig
from subsystem_announcement.discovery.cache import AnnouncementDocumentCache
from subsystem_announcement.discovery.dedupe import AnnouncementDedupeStore
from subsystem_announcement.discovery.document import AnnouncementDocumentArtifact
from subsystem_announcement.discovery.envelope import AnnouncementEnvelope
from subsystem_announcement.extract import (
    AnnouncementFactCandidate,
    EvidenceSpan,
    FactType,
)
from subsystem_announcement.index import AnnouncementRetrievalArtifact
from subsystem_announcement.index.retrieval_artifact import (
    AnnouncementEmbeddingStrategy,
)
from subsystem_announcement.parse.artifact import (
    ParsedAnnouncementArtifact,
    load_parsed_artifact,
    write_parsed_artifact,
)
from subsystem_announcement.runtime.repair import (
    RepairError,
    RepairReason,
    RepairRequest,
    repair_parsed_artifact,
)
from subsystem_announcement.runtime.replay import (
    ReplayError,
    ReplayRequest,
    envelope_from_document_artifact,
    load_cached_document_for_replay,
    replay_announcement,
)
from subsystem_announcement.runtime.submit import SubmitIdempotencyStore
from subsystem_announcement.runtime.trace import (
    AnnouncementExtractionRun,
    RunTraceError,
    TraceStore,
)
from subsystem_announcement.graph import derive_graph_delta_candidates
from subsystem_announcement.signals import derive_signal_candidates

from .extract_fixtures import make_artifact


class RecordingSubsystem:
    def __init__(self, *, accept_facts: bool = True) -> None:
        self.accept_facts = accept_facts
        self.submissions: list[dict[str, Any]] = []

    def submit(self, candidate: dict[str, Any]) -> dict[str, Any]:
        self.submissions.append(candidate)
        if candidate["ex_type"] == "Ex-1" and not self.accept_facts:
            return {
                "accepted": False,
                "receipt_id": f"rejected-{len(self.submissions)}",
                "warnings": (),
                "errors": ("fact rejected",),
            }
        return {
            "accepted": True,
            "receipt_id": f"receipt-{len(self.submissions)}",
            "warnings": (),
            "errors": (),
        }


def test_envelope_from_document_artifact_rebuilds_required_metadata(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    document = _cache_document(config, "ANN-REPLAY-001")

    envelope = envelope_from_document_artifact(document)

    assert envelope.announcement_id == document.announcement_id
    assert envelope.ts_code == "600000.SH"
    assert envelope.title == "测试公司重大合同公告"
    assert envelope.official_url == document.official_url


def test_load_cached_document_for_replay_requires_dedupe_hit(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)

    try:
        load_cached_document_for_replay("missing-ann", config=config)
    except ReplayError as exc:
        assert "missing-ann" in str(exc)
    else:
        raise AssertionError("missing cached document was accepted")


def test_replay_uses_cached_duplicate_discovery_and_skips_network(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    document = _cache_document(config, "ANN-REPLAY-002")
    subsystem = RecordingSubsystem()

    run = asyncio.run(
        replay_announcement(
            ReplayRequest(announcement_id=document.announcement_id),
            config,
            subsystem=subsystem,  # type: ignore[arg-type]
            parse_func=_fake_parse,
        )
    ).run

    assert run.status == "succeeded"
    assert run.document_artifact_path == document.local_path
    assert run.submit_success_count == run.candidate_count
    assert subsystem.submissions
    # Stage 2.8 follow-up #3: announcement_id moved from top-level into
    # producer_context (no canonical slot in contracts.Ex*).
    assert {
        payload["producer_context"]["announcement_id"]
        for payload in subsystem.submissions
    } == {document.announcement_id}


def test_replay_is_idempotent_for_repeated_cached_document(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    document = _cache_document(config, "ANN-REPLAY-003")
    subsystem = RecordingSubsystem()
    store = SubmitIdempotencyStore(tmp_path / "runs" / "submit_idempotency.json")

    first = asyncio.run(
        replay_announcement(
            ReplayRequest(announcement_id=document.announcement_id),
            config,
            subsystem=subsystem,  # type: ignore[arg-type]
            idempotency_store=store,
            parse_func=_fake_parse,
        )
    ).run
    second = asyncio.run(
        replay_announcement(
            ReplayRequest(announcement_id=document.announcement_id),
            config,
            subsystem=subsystem,  # type: ignore[arg-type]
            idempotency_store=store,
            parse_func=_fake_parse,
        )
    ).run

    assert first.submit_success_count == first.candidate_count
    assert second.submit_success_count == 0
    assert second.submit_duplicate_count == second.candidate_count
    assert second.submit_failure_count == 0
    assert all(trace.status == "duplicate" for trace in second.candidate_traces)
    assert len(subsystem.submissions) == first.candidate_count


def test_replay_preserves_ex1_dependency_gating_when_fact_rejected(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    document = _cache_document(config, "ANN-REPLAY-004")
    subsystem = RecordingSubsystem(accept_facts=False)

    run = asyncio.run(
        replay_announcement(
            ReplayRequest(announcement_id=document.announcement_id),
            config,
            subsystem=subsystem,  # type: ignore[arg-type]
            parse_func=_fake_parse,
            extract_func=lambda artifact: [_graph_fact(artifact.announcement_id)],
            signal_func=lambda facts: derive_signal_candidates(
                facts,
                generated_at=_timestamp(),
            ),
            graph_func=lambda facts: derive_graph_delta_candidates(
                facts,
                generated_at=_timestamp(),
            ),
        )
    ).run

    assert run.status == "failed"
    assert run.candidate_count == 3
    assert run.submit_success_count == 0
    assert run.submit_failure_count == 3
    assert [payload["ex_type"] for payload in subsystem.submissions] == [
        "Ex-1",
        "Ex-1",
        "Ex-1",
    ]
    assert [trace.ex_type for trace in run.candidate_traces] == [
        "Ex-1",
        "Ex-2",
        "Ex-3",
    ]
    assert run.candidate_traces[1].attempts == 0
    assert run.candidate_traces[2].attempts == 0
    assert "source_fact_ids are not accepted Ex-1 facts" in (
        run.candidate_traces[1].errors[0]
    )


def test_repair_parse_failure_from_trace_rewrites_parsed_artifact(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    document = _cache_document(config, "ANN-REPAIR-001")
    trace_path = _failed_trace(config, document)

    result = repair_parsed_artifact(
        RepairRequest(trace_path=trace_path, reason=RepairReason.PARSE_FAILURE),
        config,
        parse_func=_fake_parse,
        rebuild_index_func=_fake_rebuild_index,
    )

    assert result.announcement_id == document.announcement_id
    assert result.parsed_artifact_path.exists()
    assert result.retrieval_artifact_path is not None
    assert result.retrieval_artifact_path.exists()


def test_repair_docling_version_upgrade_requires_configured_parser_version(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    document = _cache_document(config, "ANN-REPAIR-002")

    result = repair_parsed_artifact(
        RepairRequest(
            document_path=document.local_path,
            reason=RepairReason.DOCLING_VERSION_UPGRADE,
            rebuild_index=False,
        ),
        config,
        parse_func=_fake_parse,
    )

    assert result.parser_version == config.docling_version
    assert result.retrieval_artifact_path is None
    assert "upgrades" in result.parsed_artifact_path.parts
    latest_pointer = (
        Path(config.artifact_root)
        / "parsed"
        / document.announcement_id
        / "latest.json"
    )
    latest = json.loads(latest_pointer.read_text(encoding="utf-8"))
    assert latest["parsed_artifact_path"] == str(result.parsed_artifact_path)


def test_docling_upgrade_repair_preserves_previous_parse_when_index_fails(
    tmp_path: Path,
) -> None:
    config = AnnouncementConfig(
        artifact_root=tmp_path,
        docling_version="docling==2.16.0",
        llama_index_version="llama-index-core==0.10.0",
    )
    document = _cache_document(config, "ANN-REPAIR-003")
    previous = _fake_parse(document, config).model_copy(
        update={"parser_version": "docling==2.15.1"}
    )
    previous_path = write_parsed_artifact(previous, config.artifact_root)

    def fail_rebuild(
        parsed_artifact: ParsedAnnouncementArtifact,
        *,
        config: AnnouncementConfig,
        parsed_artifact_path: Path | None = None,
    ) -> AnnouncementRetrievalArtifact:
        raise RuntimeError("index rebuild failed")

    try:
        repair_parsed_artifact(
            RepairRequest(
                document_path=document.local_path,
                reason=RepairReason.DOCLING_VERSION_UPGRADE,
            ),
            config,
            parse_func=_fake_parse,
            rebuild_index_func=fail_rebuild,
        )
    except RuntimeError as exc:
        assert "index rebuild failed" in str(exc)
    else:
        raise AssertionError("failing index rebuild was accepted")

    preserved = load_parsed_artifact(previous_path)
    assert preserved.parser_version == "docling==2.15.1"
    assert preserved.extracted_text == previous.extracted_text
    assert not (previous_path.parent / "latest.json").exists()


def test_docling_upgrade_repair_rejects_symlinked_announcement_directory(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    document = _cache_document(config, "ANN-REPAIR-SYMLINK")
    parsed_root = Path(config.artifact_root) / "parsed"
    parsed_root.mkdir()
    escaped_root = tmp_path / "escaped-parsed-root"
    escaped_root.mkdir()
    (parsed_root / document.announcement_id).symlink_to(
        escaped_root,
        target_is_directory=True,
    )

    try:
        repair_parsed_artifact(
            RepairRequest(
                document_path=document.local_path,
                reason=RepairReason.DOCLING_VERSION_UPGRADE,
                rebuild_index=False,
            ),
            config,
            parse_func=_fake_parse,
        )
    except RepairError as exc:
        assert "symlink" in str(exc)
        assert document.announcement_id in str(exc)
    else:
        raise AssertionError("symlinked repair artifact directory was accepted")

    assert not (escaped_root / "latest.json").exists()
    assert not (escaped_root / "upgrades").exists()


def test_repair_rejects_conflicting_document_locators(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    traced_document = _cache_document(config, "ANN-REPAIR-004")
    cache = AnnouncementDocumentCache(config)
    conflicting_document = cache.put(
        _envelope("ANN-REPAIR-005"),
        b"%PDF conflicting repair fixture",
        content_type="application/pdf",
    )
    AnnouncementDedupeStore(config.artifact_root).record(conflicting_document)
    trace_path = _failed_trace(config, traced_document)

    try:
        repair_parsed_artifact(
            RepairRequest(
                trace_path=trace_path,
                document_path=conflicting_document.local_path,
                reason=RepairReason.PARSE_FAILURE,
            ),
            config,
            parse_func=_fake_parse,
            rebuild_index_func=_fake_rebuild_index,
        )
    except RepairError as exc:
        assert "resolve to different documents" in str(exc)
        assert traced_document.announcement_id in str(exc)
        assert conflicting_document.announcement_id in str(exc)
    else:
        raise AssertionError("conflicting repair locators were accepted")


def _config(tmp_path: Path) -> AnnouncementConfig:
    return AnnouncementConfig(
        artifact_root=tmp_path,
        docling_version="docling==2.15.1",
        llama_index_version="llama-index-core==0.10.0",
    )


def _envelope(announcement_id: str) -> AnnouncementEnvelope:
    return AnnouncementEnvelope(
        announcement_id=announcement_id,
        ts_code="600000.SH",
        title="测试公司重大合同公告",
        publish_time=datetime(2026, 4, 18, 9, 0, tzinfo=timezone.utc),
        official_url=f"https://static.sse.com.cn/disclosure/{announcement_id}.pdf",
        source_exchange="sse",
        attachment_type="pdf",
    )


def _cache_document(
    config: AnnouncementConfig,
    announcement_id: str,
) -> AnnouncementDocumentArtifact:
    cache = AnnouncementDocumentCache(config)
    document = cache.put(
        _envelope(announcement_id),
        b"%PDF cached replay fixture",
        content_type="application/pdf",
    )
    AnnouncementDedupeStore(config.artifact_root).record(document)
    return document


def _fake_parse(
    document: AnnouncementDocumentArtifact,
    config: AnnouncementConfig,
) -> ParsedAnnouncementArtifact:
    artifact = make_artifact(
        "证券代码：600000\n证券简称：测试公司\n"
        "公司与华东能源签订重大合同，合同金额为1000万元。",
        announcement_id=document.announcement_id,
        title=document.title or "重大合同公告",
    )
    return artifact.model_copy(
        update={
            "content_hash": document.content_hash,
            "parser_version": config.docling_version,
            "source_document": document,
        }
    )


def _fake_rebuild_index(
    parsed_artifact: ParsedAnnouncementArtifact,
    *,
    config: AnnouncementConfig,
    parsed_artifact_path: Path | None = None,
) -> AnnouncementRetrievalArtifact:
    return AnnouncementRetrievalArtifact(
        announcement_id=parsed_artifact.announcement_id,
        chunk_refs=["chunk-1"],
        index_ref=str(Path(config.artifact_root) / "index" / "fake"),
        parser_version=parsed_artifact.parser_version,
        llama_index_version=config.llama_index_version,
        embedding_strategy=_embedding_strategy(),
        chunk_count=1,
        built_at=_timestamp(),
        source_parsed_artifact_path=parsed_artifact_path,
    )


def _embedding_strategy() -> AnnouncementEmbeddingStrategy:
    return AnnouncementEmbeddingStrategy(
        strategy_type="adapter",
        adapter_ref="tests.fixtures:embedding",
        model_ref="tests.fixtures.Embedding",
        model_version="fixture-v1",
        model_dimension=2,
        model_fingerprint="fixture-fingerprint",
    )


def _failed_trace(
    config: AnnouncementConfig,
    document: AnnouncementDocumentArtifact,
) -> Path:
    run = AnnouncementExtractionRun(
        run_id=f"failed-{document.announcement_id}",
        announcement_id=document.announcement_id,
        status="failed",
        started_at=_timestamp(),
        finished_at=_timestamp(),
        document_artifact_path=document.local_path,
        errors=[RunTraceError(stage="parse", message="Docling failed")],
    )
    return TraceStore(config).write(run)


def _graph_fact(announcement_id: str) -> AnnouncementFactCandidate:
    return AnnouncementFactCandidate(
        fact_id=f"fact:{announcement_id}:major_contract:graph",
        announcement_id=announcement_id,
        fact_type=FactType.MAJOR_CONTRACT,
        primary_entity_id="ts_code:600000.SH",
        related_entity_ids=["entity:huadong-energy"],
        fact_content={"event": "major_contract"},
        confidence=0.93,
        source_reference={
            "announcement_id": announcement_id,
            "official_url": f"https://static.sse.com.cn/disclosure/{announcement_id}.pdf",
            "source_exchange": "sse",
            "attachment_type": "pdf",
        },
        evidence_spans=[
            _span("公司与华东能源签订重大合同。", "sec-1"),
            _span("双方合同金额为1000万元。", "sec-2"),
        ],
        extracted_at=_timestamp(),
    )


def _span(quote: str, section_id: str) -> EvidenceSpan:
    return EvidenceSpan(
        section_id=section_id,
        start_offset=0,
        end_offset=len(quote),
        quote=quote,
    )


def _timestamp() -> datetime:
    return datetime(2026, 4, 18, 10, 0, tzinfo=timezone.utc)
