from __future__ import annotations

import asyncio
import json
from pathlib import Path

from subsystem_announcement.config import AnnouncementConfig
from subsystem_announcement.extract import extract_fact_candidates
from subsystem_announcement.graph import derive_graph_delta_candidates
from subsystem_announcement.parse.artifact import ParsedAnnouncementArtifact
from subsystem_announcement.runtime.metrics import (
    MetricThresholds,
    assert_metrics_within_thresholds,
    compute_metrics_for_manifest,
)
from subsystem_announcement.runtime.replay import ReplayRequest, replay_announcement
from subsystem_announcement.runtime.submit import SubmitIdempotencyStore

from .test_replay_repair import RecordingSubsystem, _cache_document, _fake_parse


ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "tests" / "fixtures" / "announcements" / "manifest.json"


def test_manifest_has_stage3_sample_baseline() -> None:
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    samples = manifest["samples"]
    successful = [sample for sample in samples if sample["expected_success"]]

    assert 10 <= len(samples) <= 20
    assert 10 <= len(successful) <= 20
    assert {
        "earnings_preannounce",
        "major_contract",
        "shareholder_change",
        "equity_pledge",
        "regulatory_action",
        "trading_halt_resume",
        "fundraising_change",
    }.issubset(
        {
            fact_type
            for sample in successful
            for fact_type in sample["fact_types"]
        }
    )
    for sample in samples:
        assert sample["sample_id"]
        assert sample["announcement_id"]
        assert isinstance(sample["fixture_paths"], dict)
        assert sample["source_exchange"]
        assert sample["attachment_type"] in {"pdf", "html", "word"}


def test_metrics_report_satisfies_stage3_thresholds() -> None:
    report = compute_metrics_for_manifest(MANIFEST, config=_config())

    assert report.sample_count == 13
    assert report.evaluated_sample_count == 10
    assert report.official_source_coverage == 1.0
    assert report.ex1_evidence_coverage == 1.0
    assert report.ex3_false_positive_rate == 0.0
    assert report.deterministic_anchor_rate >= 0.9
    assert report.parse_seconds_max <= 180
    assert report.discovery_to_ex1_seconds_max <= 300
    assert report.index_seconds_max <= 120
    assert report.diagnostics == []
    assert_metrics_within_thresholds(report)


def test_metrics_timings_are_measured_not_read_from_manifest(
    tmp_path: Path,
) -> None:
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    for sample in manifest["samples"]:
        sample["metrics"] = {
            "parse_seconds": 999,
            "discovery_to_ex1_seconds": 999,
            "index_seconds": 999,
        }
        parsed_fixture = sample.get("fixture_paths", {}).get("parsed_artifact")
        if parsed_fixture:
            sample["fixture_paths"]["parsed_artifact"] = str(
                MANIFEST.parent / parsed_fixture
            )
    copied_manifest = tmp_path / "manifest.json"
    copied_manifest.write_text(
        json.dumps(manifest, ensure_ascii=False),
        encoding="utf-8",
    )

    report = compute_metrics_for_manifest(copied_manifest, config=_config())

    assert report.parse_seconds_max < 999
    assert report.discovery_to_ex1_seconds_max < 999
    assert report.index_seconds_max < 999
    assert report.diagnostics == []


def test_assert_metrics_checks_all_stage3_thresholds() -> None:
    report = compute_metrics_for_manifest(MANIFEST, config=_config())
    bad_report = report.model_copy(
        update={
            "parse_seconds_max": 181,
            "discovery_to_ex1_seconds_max": 301,
            "index_seconds_max": 121,
            "official_source_coverage": 0.99,
            "ex1_evidence_coverage": 0.99,
            "ex3_false_positive_rate": 0.02,
            "deterministic_anchor_rate": 0.89,
        }
    )

    try:
        assert_metrics_within_thresholds(bad_report, MetricThresholds())
    except AssertionError as exc:
        message = str(exc)
    else:
        raise AssertionError("bad metrics report passed thresholds")

    assert "parse_seconds_max" in message
    assert "discovery_to_ex1_seconds_max" in message
    assert "index_seconds_max" in message
    assert "official_source_coverage" in message
    assert "ex1_evidence_coverage" in message
    assert "ex3_false_positive_rate" in message
    assert "deterministic_anchor_rate" in message


def test_title_only_regression_does_not_extract_from_title_keyword() -> None:
    artifact = _manifest_artifact("ANN-SAMPLE-010")

    facts = extract_fact_candidates(artifact)

    assert artifact.title_hierarchy == ["重大合同公告"]
    assert facts == []


def test_ambiguous_relation_sample_produces_no_ex3_with_diagnostic() -> None:
    artifact = _manifest_artifact("ANN-SAMPLE-009")
    facts = extract_fact_candidates(artifact)
    graph_deltas = derive_graph_delta_candidates(facts)
    report = compute_metrics_for_manifest(MANIFEST, config=_config())

    assert graph_deltas == []
    assert report.graph_guard_rejection_counts["ambiguous_language"] >= 1


def test_repeated_replay_does_not_duplicate_submit_receipts(tmp_path: Path) -> None:
    config = AnnouncementConfig(
        artifact_root=tmp_path,
        docling_version="docling==2.15.1",
        llama_index_version="llama-index-core==0.10.0",
    )
    document = _cache_document(config, "ANN-METRICS-REPLAY")
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


def _config() -> AnnouncementConfig:
    return AnnouncementConfig(
        artifact_root=ROOT / "tests" / "fixtures" / "announcements" / "artifacts",
        docling_version="docling==2.15.1",
        llama_index_version="llama-index-core==0.10.0",
    )


def _manifest_artifact(sample_id: str) -> ParsedAnnouncementArtifact:
    fixture = json.loads(
        (
            ROOT
            / "tests"
            / "fixtures"
            / "announcements"
            / "parsed_stage3_samples.json"
        ).read_text(encoding="utf-8")
    )
    return ParsedAnnouncementArtifact.model_validate(fixture[sample_id])
