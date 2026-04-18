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
from subsystem_announcement.extract import extract_fact_candidates
from subsystem_announcement.parse import ParsedAnnouncementArtifact
from subsystem_announcement.runtime.pipeline import AnnouncementPipeline
from subsystem_announcement.runtime.trace import TraceStore

from .extract_fixtures import make_artifact

ROOT = Path(__file__).resolve().parents[1]


class RecordingSubsystem:
    def __init__(self, *, accepted: bool = True) -> None:
        self.accepted = accepted
        self.submissions: list[dict[str, Any]] = []

    def submit(self, candidate: dict[str, Any]) -> dict[str, Any]:
        self.submissions.append(candidate)
        return {
            "accepted": self.accepted,
            "receipt_id": f"receipt-{len(self.submissions)}",
            "warnings": (),
            "errors": () if self.accepted else ("sdk rejected",),
        }


def test_process_envelope_discovers_parses_extracts_and_submits_ex1(
    tmp_path: Path,
) -> None:
    envelope = _fixture_envelope()
    subsystem = RecordingSubsystem()
    config = _config(tmp_path)

    pipeline = AnnouncementPipeline(
        config,
        subsystem=subsystem,  # type: ignore[arg-type]
        discovery_func=_fake_discovery,
        parse_func=_fake_parse,
        extract_func=extract_fact_candidates,
    )

    run = asyncio.run(pipeline.process_envelope(envelope))

    assert run.status == "succeeded"
    assert run.candidate_count >= 1
    assert run.submit_success_count == run.candidate_count
    assert run.submit_failure_count == 0
    assert run.document_artifact_path == tmp_path / "documents" / "ann-e2e.pdf"
    assert run.parsed_artifact_path is not None
    assert run.parsed_artifact_path.exists()
    assert run.trace_path is not None
    assert run.trace_path.exists()
    assert subsystem.submissions
    assert all(payload["ex_type"] == "Ex-1" for payload in subsystem.submissions)
    assert all(payload["evidence_spans"] for payload in subsystem.submissions)
    assert all(
        payload["source_reference"]["official_url"].startswith("https://")
        for payload in subsystem.submissions
    )

    loaded = TraceStore(config).load(run.trace_path)
    assert loaded == run


def test_process_envelope_records_submit_rejection_as_failed_trace(
    tmp_path: Path,
) -> None:
    envelope = _fixture_envelope()
    subsystem = RecordingSubsystem(accepted=False)
    pipeline = AnnouncementPipeline(
        _config(tmp_path),
        subsystem=subsystem,  # type: ignore[arg-type]
        discovery_func=_fake_discovery,
        parse_func=_fake_parse,
        extract_func=extract_fact_candidates,
    )

    run = asyncio.run(pipeline.process_envelope(envelope))

    assert run.status == "failed"
    assert run.submit_failure_count == run.candidate_count
    assert run.errors
    assert run.errors[0].stage == "submit"
    assert "sdk rejected" in run.errors[0].message
    assert run.trace_path is not None
    assert run.trace_path.exists()


def _fixture_envelope() -> AnnouncementEnvelope:
    path = ROOT / "tests" / "fixtures" / "announcements" / "earnings_envelope.json"
    return AnnouncementEnvelope.model_validate_json(path.read_text(encoding="utf-8"))


async def _fake_discovery(
    envelope: AnnouncementEnvelope,
    config: AnnouncementConfig,
) -> AnnouncementDiscoveryResult:
    document_path = Path(config.artifact_root) / "documents" / "ann-e2e.pdf"
    document_path.parent.mkdir(parents=True, exist_ok=True)
    document_path.write_bytes(b"%PDF mocked fixture")
    document = AnnouncementDocumentArtifact(
        announcement_id=envelope.announcement_id,
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


def _config(tmp_path: Path) -> AnnouncementConfig:
    return AnnouncementConfig(
        artifact_root=tmp_path,
        docling_version="docling==2.15.1",
        llama_index_version="llama-index-core==0.10.0",
    )
