from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from subsystem_announcement.config import AnnouncementConfig
from subsystem_announcement.discovery import (
    AnnouncementDiscoveryResult,
    AnnouncementEnvelope,
)
from subsystem_announcement.discovery.document import AnnouncementDocumentArtifact
from subsystem_announcement.extract import AnnouncementFactCandidate, EvidenceSpan, FactType
from subsystem_announcement.graph import derive_graph_delta_candidates
from subsystem_announcement.parse import ParsedAnnouncementArtifact
from subsystem_announcement.runtime.pipeline import AnnouncementPipeline
from subsystem_announcement.runtime.submit import SubmitIdempotencyStore, submit_candidates
from subsystem_announcement.signals import derive_signal_candidates

from .extract_fixtures import make_artifact


GENERATED_AT = datetime(2026, 4, 18, 10, 30, tzinfo=timezone.utc)


class RecordingSubsystem:
    def __init__(self) -> None:
        self.submissions: list[dict[str, Any]] = []

    def submit(self, candidate: dict[str, Any]) -> dict[str, Any]:
        self.submissions.append(candidate)
        return {
            "accepted": True,
            "receipt_id": f"receipt-{len(self.submissions)}",
            "warnings": (),
            "errors": (),
        }


def test_pipeline_generates_graph_after_signals_and_submits_ex3_last(
    tmp_path: Path,
) -> None:
    subsystem = RecordingSubsystem()
    calls: list[str] = []

    def extract_func(artifact: ParsedAnnouncementArtifact) -> list[AnnouncementFactCandidate]:
        calls.append("extract")
        return [_major_contract_fact(artifact.announcement_id)]

    def signal_func(facts: list[AnnouncementFactCandidate]):
        assert calls == ["extract"]
        calls.append("signals")
        return derive_signal_candidates(facts, generated_at=GENERATED_AT)

    def graph_func(facts: list[AnnouncementFactCandidate]):
        assert calls == ["extract", "signals"]
        calls.append("graph")
        return derive_graph_delta_candidates(facts, generated_at=GENERATED_AT)

    pipeline = AnnouncementPipeline(
        _config(tmp_path),
        subsystem=subsystem,  # type: ignore[arg-type]
        discovery_func=_fake_discovery,
        parse_func=_fake_parse,
        extract_func=extract_func,
        signal_func=signal_func,
        graph_func=graph_func,
    )

    run = asyncio.run(pipeline.process_envelope(_envelope()))

    assert calls == ["extract", "signals", "graph"]
    assert run.status == "succeeded"
    assert run.candidate_count == 3
    assert [payload["ex_type"] for payload in subsystem.submissions] == [
        "Ex-1",
        "Ex-2",
        "Ex-3",
    ]
    assert subsystem.submissions[1]["source_fact_ids"] == [
        subsystem.submissions[0]["fact_id"]
    ]
    assert subsystem.submissions[2]["source_fact_ids"] == [
        subsystem.submissions[0]["fact_id"]
    ]
    assert [receipt["ex_type"] for receipt in run.submit_receipts] == [
        "Ex-1",
        "Ex-2",
        "Ex-3",
    ]
    assert [trace.ex_type for trace in run.candidate_traces] == [
        "Ex-1",
        "Ex-2",
        "Ex-3",
    ]


def test_ex3_idempotency_is_isolated_from_ex1_and_ex2_candidate_ids() -> None:
    fact = _major_contract_fact("ann-graph-idempotency")
    signal = derive_signal_candidates([fact], generated_at=GENERATED_AT)[0].model_copy(
        update={"signal_id": fact.fact_id}
    )
    delta = derive_graph_delta_candidates([fact], generated_at=GENERATED_AT)[0].model_copy(
        update={"delta_id": fact.fact_id}
    )
    subsystem = RecordingSubsystem()
    store = SubmitIdempotencyStore()

    first = submit_candidates([fact, signal, delta], subsystem, idempotency_store=store)  # type: ignore[arg-type]
    second = submit_candidates([fact, signal, delta], subsystem, idempotency_store=store)  # type: ignore[arg-type]

    assert first.submitted == 3
    assert first.skipped_duplicates == 0
    assert second.submitted == 0
    assert second.skipped_duplicates == 3
    assert len(subsystem.submissions) == 3
    assert [trace.ex_type for trace in second.traces] == ["Ex-1", "Ex-2", "Ex-3"]
    assert all(trace.status == "duplicate" for trace in second.traces)


async def _fake_discovery(
    envelope: AnnouncementEnvelope,
    config: AnnouncementConfig,
) -> AnnouncementDiscoveryResult:
    path = Path(config.artifact_root) / "documents" / f"{envelope.announcement_id}.pdf"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"%PDF mocked")
    document = AnnouncementDocumentArtifact(
        announcement_id=envelope.announcement_id,
        ts_code=envelope.ts_code,
        title=envelope.title,
        publish_time=envelope.publish_time,
        content_hash="b" * 64,
        official_url=envelope.official_url,
        source_exchange=envelope.source_exchange,
        attachment_type=envelope.attachment_type,
        local_path=path,
        content_type="application/pdf",
        byte_size=path.stat().st_size,
        fetched_at=datetime(2026, 4, 18, 9, 30, tzinfo=timezone.utc),
    )
    return AnnouncementDiscoveryResult(status="fetched", document=document)


def _fake_parse(
    document: AnnouncementDocumentArtifact,
    config: AnnouncementConfig,
) -> ParsedAnnouncementArtifact:
    artifact = make_artifact(
        "证券代码：600000\n证券简称：测试公司\n"
        "公司与华东能源签订重大合同，合同金额为1000万元。",
        announcement_id=document.announcement_id,
    )
    return artifact.model_copy(
        update={
            "content_hash": document.content_hash,
            "source_document": document,
        }
    )


def _major_contract_fact(announcement_id: str) -> AnnouncementFactCandidate:
    return AnnouncementFactCandidate(
        fact_id=f"fact:{announcement_id}:major_contract:1",
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
        extracted_at=datetime(2026, 4, 18, 9, 30, tzinfo=timezone.utc),
    )


def _span(quote: str, section_id: str) -> EvidenceSpan:
    return EvidenceSpan(
        section_id=section_id,
        start_offset=0,
        end_offset=len(quote),
        quote=quote,
    )


def _envelope() -> AnnouncementEnvelope:
    return AnnouncementEnvelope(
        announcement_id="ann-graph-pipeline",
        ts_code="600000.SH",
        title="测试公司重大合同公告",
        publish_time=datetime(2026, 4, 18, 9, 0, tzinfo=timezone.utc),
        official_url="https://static.sse.com.cn/disclosure/ann-graph-pipeline.pdf",
        source_exchange="sse",
        attachment_type="pdf",
    )


def _config(tmp_path: Path) -> AnnouncementConfig:
    return AnnouncementConfig(
        artifact_root=tmp_path,
        docling_version="docling==2.15.1",
        llama_index_version="llama-index-core==0.10.0",
    )
