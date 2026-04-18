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
from subsystem_announcement.extract import AnnouncementFactCandidate, extract_fact_candidates
from subsystem_announcement.parse import ParsedAnnouncementArtifact
from subsystem_announcement.runtime.pipeline import AnnouncementPipeline
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


def test_pipeline_extracts_facts_then_generates_signals_then_submits_in_order(
    tmp_path: Path,
) -> None:
    subsystem = RecordingSubsystem()
    signal_calls: list[list[str]] = []

    def signal_func(facts: list[AnnouncementFactCandidate]):
        signal_calls.append([fact.fact_id for fact in facts])
        return derive_signal_candidates(facts, generated_at=GENERATED_AT)

    pipeline = AnnouncementPipeline(
        _config(tmp_path),
        subsystem=subsystem,  # type: ignore[arg-type]
        discovery_func=_fake_discovery,
        parse_func=_fake_parse,
        extract_func=_fake_extract,
        signal_func=signal_func,
    )

    run = asyncio.run(pipeline.process_envelope(_envelope()))

    assert run.status == "succeeded"
    assert run.candidate_count == 2
    assert signal_calls == [[subsystem.submissions[0]["fact_id"]]]
    assert [payload["ex_type"] for payload in subsystem.submissions] == [
        "Ex-1",
        "Ex-2",
    ]
    assert subsystem.submissions[1]["source_fact_ids"] == [
        subsystem.submissions[0]["fact_id"]
    ]
    assert [receipt["ex_type"] for receipt in run.submit_receipts] == [
        "Ex-1",
        "Ex-2",
    ]
    assert [trace.ex_type for trace in run.candidate_traces] == ["Ex-1", "Ex-2"]


def test_pipeline_empty_signals_preserves_ex1_submit_behavior(tmp_path: Path) -> None:
    subsystem = RecordingSubsystem()

    pipeline = AnnouncementPipeline(
        _config(tmp_path),
        subsystem=subsystem,  # type: ignore[arg-type]
        discovery_func=_fake_discovery,
        parse_func=_fake_parse,
        extract_func=_fake_extract,
        signal_func=lambda facts: [],
    )

    run = asyncio.run(pipeline.process_envelope(_envelope()))

    assert run.status == "succeeded"
    assert run.candidate_count == 1
    assert [payload["ex_type"] for payload in subsystem.submissions] == ["Ex-1"]


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
        "公司预计2026年净利润同比增长50%，本公告为业绩预告。",
        announcement_id=document.announcement_id,
    )
    return artifact.model_copy(
        update={
            "content_hash": document.content_hash,
            "source_document": document,
        }
    )


def _fake_extract(
    artifact: ParsedAnnouncementArtifact,
) -> list[AnnouncementFactCandidate]:
    return extract_fact_candidates(artifact)


def _envelope() -> AnnouncementEnvelope:
    return AnnouncementEnvelope(
        announcement_id="ann-signal-pipeline",
        ts_code="600000.SH",
        title="测试公司业绩预告",
        publish_time=datetime(2026, 4, 18, 9, 0, tzinfo=timezone.utc),
        official_url="https://static.sse.com.cn/disclosure/ann-signal-pipeline.pdf",
        source_exchange="sse",
        attachment_type="pdf",
    )


def _config(tmp_path: Path) -> AnnouncementConfig:
    return AnnouncementConfig(
        artifact_root=tmp_path,
        docling_version="docling==2.15.1",
        llama_index_version="llama-index-core==0.10.0",
    )
