from __future__ import annotations

import asyncio
import sys
import types
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
    assert [payload["ex_type"] for payload in subsystem.submissions] == ["Ex-1", "Ex-2"]
    assert subsystem.submissions[1]["source_fact_ids"] == [
        subsystem.submissions[0]["fact_id"]
    ]
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


def test_process_envelope_skips_reextracted_duplicate_with_file_backed_idempotency(
    tmp_path: Path,
) -> None:
    envelope = _fixture_envelope()
    subsystem = RecordingSubsystem()
    config = _config(tmp_path)
    parsed_artifact = make_artifact(
        "证券代码：600000\n证券简称：测试公司\n"
        "公司预计2026年净利润同比增长50%，本公告为业绩预告。",
        announcement_id=envelope.announcement_id,
    )
    extraction_times = [
        datetime(2026, 4, 18, 10, 0, tzinfo=timezone.utc),
        datetime(2026, 4, 18, 10, 1, tzinfo=timezone.utc),
    ]

    def parse_same_artifact(
        _document: AnnouncementDocumentArtifact,
        _config: AnnouncementConfig,
    ) -> ParsedAnnouncementArtifact:
        return parsed_artifact

    def extract_with_replay_timestamp(artifact: ParsedAnnouncementArtifact) -> Any:
        assert artifact is parsed_artifact
        timestamp = extraction_times.pop(0)
        return [
            candidate.model_copy(update={"extracted_at": timestamp})
            for candidate in extract_fact_candidates(artifact)
        ]

    first_pipeline = AnnouncementPipeline(
        config,
        subsystem=subsystem,  # type: ignore[arg-type]
        discovery_func=_fake_discovery,
        parse_func=parse_same_artifact,
        extract_func=extract_with_replay_timestamp,
    )
    second_pipeline = AnnouncementPipeline(
        config,
        subsystem=subsystem,  # type: ignore[arg-type]
        discovery_func=_fake_discovery,
        parse_func=parse_same_artifact,
        extract_func=extract_with_replay_timestamp,
    )

    first_run = asyncio.run(first_pipeline.process_envelope(envelope))
    second_run = asyncio.run(second_pipeline.process_envelope(envelope))

    assert first_run.candidate_count >= 1
    assert first_run.submit_success_count == first_run.candidate_count
    assert second_run.candidate_count == first_run.candidate_count
    assert second_run.submit_success_count == 0
    assert second_run.submit_duplicate_count == second_run.candidate_count
    assert second_run.submit_failure_count == 0
    assert len(subsystem.submissions) == first_run.candidate_count
    assert all(trace.status == "duplicate" for trace in second_run.candidate_traces)


def test_process_envelope_wires_configured_registry_and_reasoner(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    envelope = _fixture_envelope()
    subsystem = RecordingSubsystem()
    registry_endpoint = "entity-registry://fake"
    reasoner_endpoint = "reasoner-runtime://fake"
    registry_calls: list[tuple[str, Any, str | None]] = []
    reasoner_payloads: list[dict[str, Any]] = []

    def lookup_alias(name: str, *, endpoint: str | None = None) -> dict[str, Any] | None:
        registry_calls.append(("lookup_alias", name, endpoint))
        if name == "测试股份":
            return {
                "mention": name,
                "entity_id": "entity-primary",
                "entity_name": "测试股份有限公司",
                "confidence": 0.96,
                "resolution_method": "lookup_alias",
            }
        return None

    def resolve_mentions(
        mentions: list[dict[str, Any]],
        *,
        endpoint: str | None = None,
    ) -> list[dict[str, Any]]:
        registry_calls.append(
            ("resolve_mentions", [mention["name"] for mention in mentions], endpoint)
        )
        return [
            {
                "mention": mention,
                "entity_id": "entity-counterparty",
                "entity_name": "银河资本有限公司",
                "confidence": 0.82,
                "resolution_method": "resolve_mentions",
            }
            for mention in mentions
        ]

    def generate_structured(payload: dict[str, Any]) -> dict[str, Any]:
        reasoner_payloads.append(payload)
        return {
            "facts": [
                {
                    "quote": "公司与银河资本签署战略合作协议。",
                    "fact_content": {"agreement_type": "strategic_cooperation"},
                    "confidence": 0.72,
                    "related_mentions": ["银河资本"],
                }
            ]
        }

    monkeypatch.setitem(
        sys.modules,
        "entity_registry",
        types.SimpleNamespace(
            lookup_alias=lookup_alias,
            resolve_mentions=resolve_mentions,
        ),
    )
    monkeypatch.setitem(
        sys.modules,
        "reasoner_runtime",
        types.SimpleNamespace(generate_structured=generate_structured),
    )

    config = _config(tmp_path).model_copy(
        update={
            "entity_registry_endpoint": registry_endpoint,
            "reasoner_endpoint": reasoner_endpoint,
        }
    )

    pipeline = AnnouncementPipeline(
        config,
        subsystem=subsystem,  # type: ignore[arg-type]
        discovery_func=_fake_discovery,
        parse_func=_parse_reasoner_only_major_contract,
    )

    run = asyncio.run(pipeline.process_envelope(envelope))

    assert run.status == "succeeded"
    assert run.candidate_count == 1
    assert reasoner_payloads
    assert reasoner_payloads[0]["endpoint"] == reasoner_endpoint
    assert registry_calls == [
        ("lookup_alias", "银河资本", registry_endpoint),
        ("resolve_mentions", ["银河资本"], registry_endpoint),
    ]
    assert subsystem.submissions[0]["primary_entity_id"] == "ts_code:600000.SH"
    assert subsystem.submissions[0]["related_entity_ids"] == ["entity-counterparty"]
    assert (
        subsystem.submissions[0]["fact_content"]["reasoner_fact_content"][
            "agreement_type"
        ]
        == "strategic_cooperation"
    )


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
        ts_code=envelope.ts_code,
        title=envelope.title,
        publish_time=envelope.publish_time,
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


def _parse_reasoner_only_major_contract(
    document: AnnouncementDocumentArtifact,
    config: AnnouncementConfig,
) -> ParsedAnnouncementArtifact:
    artifact = make_artifact(
        "公司名称：测试股份\n公司与银河资本签署战略合作协议。",
        announcement_id=document.announcement_id,
        title="战略合作公告",
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
