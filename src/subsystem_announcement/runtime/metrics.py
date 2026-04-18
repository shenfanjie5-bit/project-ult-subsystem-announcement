"""Fixture-backed quality regression metrics for announcement runs."""

from __future__ import annotations

import json
import tempfile
import time
from collections import Counter
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from subsystem_announcement.config import AnnouncementConfig
from subsystem_announcement.discovery.dedupe import compute_content_hash
from subsystem_announcement.discovery.document import AnnouncementDocumentArtifact
from subsystem_announcement.extract import (
    AnnouncementFactCandidate,
    FactType,
    extract_fact_candidates,
)
from subsystem_announcement.graph import derive_graph_delta_candidates
from subsystem_announcement.graph.guard import (
    GraphDeltaGuard,
    has_ambiguous_graph_language,
)
from subsystem_announcement.graph.rules import classify_graph_delta_intent
from subsystem_announcement.index import (
    AnnouncementRetrievalArtifact,
    build_retrieval_artifact,
)
from subsystem_announcement.parse import parse_announcement
from subsystem_announcement.parse.artifact import (
    ParsedAnnouncementArtifact,
    load_parsed_artifact,
)
from subsystem_announcement.parse.errors import ParseNormalizationError


ParseFunc = Callable[
    [AnnouncementDocumentArtifact, AnnouncementConfig],
    ParsedAnnouncementArtifact,
]
BuildRetrievalFunc = Callable[..., AnnouncementRetrievalArtifact]


class MetricThresholds(BaseModel):
    """Stage-3 regression thresholds from project document §19."""

    model_config = ConfigDict(extra="forbid")

    parse_seconds_max: float = 180
    discovery_to_ex1_seconds_max: float = 300
    index_seconds_max: float = 120
    official_source_coverage_min: float = 1.0
    ex1_evidence_coverage_min: float = 1.0
    ex3_false_positive_rate_max: float = 0.01
    deterministic_anchor_rate_min: float = 0.9


class MetricsRegressionReport(BaseModel):
    """Computed quality report for a fixture manifest."""

    model_config = ConfigDict(extra="forbid")

    manifest_path: Path
    sample_count: int = Field(ge=0)
    evaluated_sample_count: int = Field(ge=0)
    fact_count: int = Field(ge=0)
    ex3_count: int = Field(ge=0)
    parse_seconds_max: float = Field(ge=0)
    discovery_to_ex1_seconds_max: float = Field(ge=0)
    index_seconds_max: float = Field(ge=0)
    official_source_coverage: float = Field(ge=0, le=1)
    ex1_evidence_coverage: float = Field(ge=0, le=1)
    ex3_false_positive_rate: float = Field(ge=0)
    deterministic_anchor_rate: float = Field(ge=0, le=1)
    unresolved_ref_count: int = Field(ge=0)
    fact_type_counts: dict[str, int] = Field(default_factory=dict)
    graph_guard_rejection_counts: dict[str, int] = Field(default_factory=dict)
    diagnostics: list[str] = Field(default_factory=list)


def compute_metrics_for_manifest(
    manifest_path: Path,
    *,
    config: AnnouncementConfig,
    parse_func: ParseFunc = parse_announcement,
    build_retrieval_func: BuildRetrievalFunc = build_retrieval_artifact,
) -> MetricsRegressionReport:
    """Compute regression metrics from fixture documents in a manifest."""

    manifest_file = Path(manifest_path)
    manifest = _load_manifest(manifest_file)
    samples = manifest.get("samples")
    if not isinstance(samples, list):
        raise ValueError(f"manifest samples must be a list: path={manifest_file}")

    diagnostics: list[str] = []
    if not 10 <= len(samples) <= 20:
        diagnostics.append(f"manifest sample count outside 10-20: count={len(samples)}")

    manifest_root = manifest_file.parent
    evaluated_count = 0
    official_source_count = 0
    facts_with_evidence = 0
    fact_count = 0
    deterministic_anchor_count = 0
    anchor_count = 0
    unresolved_ref_count = 0
    ex3_count = 0
    ex3_false_positive_count = 0
    parse_seconds_max = 0.0
    discovery_to_ex1_seconds_max = 0.0
    index_seconds_max = 0.0
    fact_type_counts: Counter[str] = Counter()
    guard_rejection_counts: Counter[str] = Counter()
    seen_candidate_ids: set[str] = set()

    for raw_sample in samples:
        if not isinstance(raw_sample, dict):
            diagnostics.append("manifest contains a non-object sample entry")
            continue
        sample_id = str(raw_sample.get("sample_id") or raw_sample.get("file") or "")
        _validate_manifest_sample(raw_sample, diagnostics)
        if raw_sample.get("expected_success") is False:
            continue

        evaluated_count += 1
        parsed_path = _fixture_path(raw_sample, manifest_root, "parsed_artifact")
        if parsed_path is None:
            diagnostics.append(f"{sample_id}: missing fixture_paths.parsed_artifact")
            continue
        document_path = _fixture_path(raw_sample, manifest_root, "document")
        if document_path is None:
            diagnostics.append(f"{sample_id}: missing fixture_paths.document")
            continue
        try:
            reference_artifact = _load_sample_parsed_artifact(
                parsed_path,
                sample_key=sample_id,
                announcement_id=str(raw_sample.get("announcement_id") or ""),
            )
            document_artifact = _document_artifact_from_sample(
                raw_sample,
                document_path,
                reference_artifact,
            )
        except Exception as exc:
            diagnostics.append(f"{sample_id}: unable to load fixture document: {exc}")
            continue

        try:
            parse_started = time.perf_counter()
            parsed_artifact = parse_func(document_artifact, config)
            parse_seconds_max = max(
                parse_seconds_max,
                time.perf_counter() - parse_started,
            )
        except Exception as exc:
            diagnostics.append(f"{sample_id}: unable to parse fixture document: {exc}")
            continue

        index_started = time.perf_counter()
        try:
            with tempfile.TemporaryDirectory(
                prefix=f"announcement-metrics-{sample_id}-"
            ) as tmp_dir:
                build_retrieval_func(
                    parsed_artifact,
                    config=config,
                    parsed_artifact_path=None,
                    output_root=Path(tmp_dir) / "index",
                )
        except Exception as exc:
            diagnostics.append(f"{sample_id}: unable to build retrieval index: {exc}")
        else:
            index_seconds_max = max(
                index_seconds_max,
                time.perf_counter() - index_started,
            )

        if parsed_artifact.parser_version != config.docling_version:
            diagnostics.append(
                f"{sample_id}: parser_version {parsed_artifact.parser_version!r} "
                f"does not match config {config.docling_version!r}"
            )
        if parsed_artifact.content_hash != document_artifact.content_hash:
            diagnostics.append(
                f"{sample_id}: parsed content_hash does not match fixture document"
            )

        if _has_official_source(parsed_artifact):
            official_source_count += 1
        else:
            diagnostics.append(f"{sample_id}: missing official source_reference")

        started = time.perf_counter()
        facts = extract_fact_candidates(parsed_artifact)
        discovery_to_ex1_seconds_max = max(
            discovery_to_ex1_seconds_max,
            time.perf_counter() - started,
        )
        graph_deltas = derive_graph_delta_candidates(facts)
        ex3_count += len(graph_deltas)
        _record_guard_diagnostics(
            raw_sample,
            parsed_artifact,
            facts,
            guard_rejection_counts,
        )

        expected_min_ex1 = _int_field(raw_sample, "expected_min_ex1", default=0)
        if len(facts) < expected_min_ex1:
            diagnostics.append(
                f"{sample_id}: expected at least {expected_min_ex1} Ex-1 "
                f"candidates, got {len(facts)}"
            )
        expected_max_ex3 = _int_field(raw_sample, "expected_max_ex3", default=0)
        if len(graph_deltas) > expected_max_ex3:
            ex3_false_positive_count += len(graph_deltas) - expected_max_ex3
            diagnostics.append(
                f"{sample_id}: expected at most {expected_max_ex3} Ex-3 "
                f"candidates, got {len(graph_deltas)}"
            )

        expected_fact_types = {
            str(value)
            for value in raw_sample.get("fact_types", [])
            if isinstance(value, str)
        }
        actual_fact_types = {fact.fact_type.value for fact in facts}
        missing_fact_types = expected_fact_types - actual_fact_types
        if expected_min_ex1 > 0 and missing_fact_types:
            diagnostics.append(
                f"{sample_id}: missing expected fact_types "
                f"{sorted(missing_fact_types)}"
            )
        expected_primary_entity_id = raw_sample.get("expected_primary_entity_id")
        if isinstance(expected_primary_entity_id, str) and expected_primary_entity_id:
            for fact in facts:
                if fact.primary_entity_id != expected_primary_entity_id:
                    diagnostics.append(
                        f"{sample_id}: unexpected primary_entity_id "
                        f"{fact.primary_entity_id!r}, expected "
                        f"{expected_primary_entity_id!r}"
                    )

        for candidate in [*facts, *graph_deltas]:
            candidate_id = getattr(candidate, "fact_id", None) or getattr(
                candidate,
                "delta_id",
                None,
            )
            if isinstance(candidate_id, str):
                if candidate_id in seen_candidate_ids:
                    diagnostics.append(
                        f"{sample_id}: duplicate candidate_id={candidate_id}"
                    )
                seen_candidate_ids.add(candidate_id)

        for fact in facts:
            fact_count += 1
            fact_type_counts[fact.fact_type.value] += 1
            if fact.evidence_spans:
                facts_with_evidence += 1
            else:
                diagnostics.append(
                    f"{sample_id}: Ex-1 lacks evidence fact_id={fact.fact_id}"
                )
            anchor_count += 1
            if _is_deterministic_anchor(fact):
                deterministic_anchor_count += 1
            if _has_unresolved_ref(fact):
                unresolved_ref_count += 1

    def _coverage(numerator: int, denominator: int) -> float:
        # Missing required artifacts are recorded in diagnostics before coverage
        # is computed, so an empty denominator is not treated as a silent pass.
        return 1.0 if denominator == 0 else numerator / denominator

    return MetricsRegressionReport(
        manifest_path=manifest_file,
        sample_count=len(samples),
        evaluated_sample_count=evaluated_count,
        fact_count=fact_count,
        ex3_count=ex3_count,
        parse_seconds_max=parse_seconds_max,
        discovery_to_ex1_seconds_max=discovery_to_ex1_seconds_max,
        index_seconds_max=index_seconds_max,
        official_source_coverage=_coverage(official_source_count, evaluated_count),
        ex1_evidence_coverage=_coverage(facts_with_evidence, fact_count),
        ex3_false_positive_rate=(
            ex3_false_positive_count / max(1, ex3_count)
        ),
        deterministic_anchor_rate=_coverage(
            deterministic_anchor_count,
            anchor_count,
        ),
        unresolved_ref_count=unresolved_ref_count,
        fact_type_counts=dict(sorted(fact_type_counts.items())),
        graph_guard_rejection_counts=dict(sorted(guard_rejection_counts.items())),
        diagnostics=diagnostics,
    )


def assert_metrics_within_thresholds(
    report: MetricsRegressionReport,
    thresholds: MetricThresholds | None = None,
) -> None:
    """Raise AssertionError when a metrics report violates stage-3 gates."""

    active_thresholds = thresholds or MetricThresholds()
    failures: list[str] = []
    if report.diagnostics:
        failures.extend(report.diagnostics)
    if report.parse_seconds_max > active_thresholds.parse_seconds_max:
        failures.append(
            "parse_seconds_max exceeded: "
            f"{report.parse_seconds_max} > {active_thresholds.parse_seconds_max}"
        )
    if (
        report.discovery_to_ex1_seconds_max
        > active_thresholds.discovery_to_ex1_seconds_max
    ):
        failures.append(
            "discovery_to_ex1_seconds_max exceeded: "
            f"{report.discovery_to_ex1_seconds_max} > "
            f"{active_thresholds.discovery_to_ex1_seconds_max}"
        )
    if report.index_seconds_max > active_thresholds.index_seconds_max:
        failures.append(
            "index_seconds_max exceeded: "
            f"{report.index_seconds_max} > {active_thresholds.index_seconds_max}"
        )
    if report.official_source_coverage < active_thresholds.official_source_coverage_min:
        failures.append(
            "official_source_coverage below threshold: "
            f"{report.official_source_coverage} < "
            f"{active_thresholds.official_source_coverage_min}"
        )
    if report.ex1_evidence_coverage < active_thresholds.ex1_evidence_coverage_min:
        failures.append(
            "ex1_evidence_coverage below threshold: "
            f"{report.ex1_evidence_coverage} < "
            f"{active_thresholds.ex1_evidence_coverage_min}"
        )
    if report.ex3_false_positive_rate > active_thresholds.ex3_false_positive_rate_max:
        failures.append(
            "ex3_false_positive_rate exceeded: "
            f"{report.ex3_false_positive_rate} > "
            f"{active_thresholds.ex3_false_positive_rate_max}"
        )
    if (
        report.deterministic_anchor_rate
        < active_thresholds.deterministic_anchor_rate_min
    ):
        failures.append(
            "deterministic_anchor_rate below threshold: "
            f"{report.deterministic_anchor_rate} < "
            f"{active_thresholds.deterministic_anchor_rate_min}"
        )
    if failures:
        raise AssertionError("; ".join(failures))


def _load_manifest(path: Path) -> dict[str, Any]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Unable to load metrics manifest: path={path}") from exc
    if not isinstance(raw, dict):
        raise ValueError(f"manifest root must be an object: path={path}")
    return raw


def _load_sample_parsed_artifact(
    path: Path,
    *,
    sample_key: str,
    announcement_id: str,
) -> ParsedAnnouncementArtifact:
    try:
        return load_parsed_artifact(path)
    except ParseNormalizationError:
        pass
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(
            f"Unable to load parsed artifact fixture: path={path}"
        ) from exc
    if not isinstance(raw, dict):
        raise ValueError(f"parsed artifact fixture must be an object: path={path}")
    candidates = []
    if sample_key:
        candidates.append(sample_key)
    if announcement_id:
        candidates.append(announcement_id)
    for key in candidates:
        value = raw.get(key)
        if isinstance(value, dict):
            try:
                return ParsedAnnouncementArtifact.model_validate(value)
            except ValidationError as exc:
                raise ValueError(
                    "parsed artifact fixture failed schema validation: "
                    f"path={path} sample_key={key}"
                ) from exc
    raise ValueError(
        "parsed artifact fixture does not contain the requested sample: "
        f"path={path} sample_key={sample_key} announcement_id={announcement_id}"
    )


def _document_artifact_from_sample(
    sample: dict[str, Any],
    document_path: Path,
    reference_artifact: ParsedAnnouncementArtifact,
) -> AnnouncementDocumentArtifact:
    try:
        content = Path(document_path).read_bytes()
    except OSError as exc:
        raise RuntimeError(
            f"Unable to read fixture document: path={document_path}"
        ) from exc

    source_document = reference_artifact.source_document
    source_exchange = sample.get("source_exchange")
    attachment_type = sample.get("attachment_type")
    fetched_at = source_document.fetched_at or datetime.now(timezone.utc)
    return AnnouncementDocumentArtifact(
        announcement_id=str(sample.get("announcement_id") or ""),
        ts_code=source_document.ts_code,
        title=source_document.title,
        publish_time=source_document.publish_time,
        content_hash=compute_content_hash(content),
        official_url=source_document.official_url,
        source_exchange=(
            source_exchange
            if isinstance(source_exchange, str)
            else source_document.source_exchange
        ),
        attachment_type=(
            attachment_type
            if attachment_type in {"pdf", "html", "word"}
            else source_document.attachment_type
        ),
        local_path=Path(document_path),
        content_type=source_document.content_type,
        byte_size=len(content),
        fetched_at=fetched_at,
    )


def _validate_manifest_sample(
    sample: dict[str, Any],
    diagnostics: list[str],
) -> None:
    sample_id = str(sample.get("sample_id") or sample.get("file") or "<unknown>")
    required_fields = (
        "sample_id",
        "announcement_id",
        "fact_types",
        "expected_min_ex1",
        "expected_max_ex3",
        "expected_primary_entity_id",
        "source_exchange",
        "attachment_type",
        "fixture_paths",
    )
    for field_name in required_fields:
        if field_name not in sample:
            diagnostics.append(f"{sample_id}: missing manifest field {field_name}")
    fact_types = sample.get("fact_types")
    if not isinstance(fact_types, list) or any(
        fact_type not in {item.value for item in FactType}
        for fact_type in fact_types
        if isinstance(fact_type, str)
    ):
        diagnostics.append(
            f"{sample_id}: fact_types must list supported FactType values"
        )
    fixture_paths = sample.get("fixture_paths")
    if not isinstance(fixture_paths, dict):
        diagnostics.append(f"{sample_id}: fixture_paths must be an object")


def _fixture_path(
    sample: dict[str, Any],
    manifest_root: Path,
    key: str,
) -> Path | None:
    fixture_paths = sample.get("fixture_paths")
    if not isinstance(fixture_paths, dict):
        return None
    value = fixture_paths.get(key)
    if not isinstance(value, str) or not value.strip():
        return None
    path = Path(value)
    if path.is_absolute():
        return path
    return manifest_root / path


def _has_official_source(parsed_artifact: ParsedAnnouncementArtifact) -> bool:
    official_url = str(parsed_artifact.source_document.official_url).strip()
    return bool(official_url)


def _record_guard_diagnostics(
    sample: dict[str, Any],
    parsed_artifact: ParsedAnnouncementArtifact,
    facts: list[AnnouncementFactCandidate],
    guard_rejection_counts: Counter[str],
) -> None:
    guard = GraphDeltaGuard()
    for fact in facts:
        intent = classify_graph_delta_intent(fact)
        if intent is None:
            if _artifact_has_ambiguous_language(parsed_artifact):
                guard_rejection_counts["ambiguous_language"] += 1
            continue
        result = guard.check(fact, intent)
        if not result.allow:
            guard_rejection_counts.update(result.reasons)
    if (
        not facts
        and _int_field(sample, "expected_max_ex3", default=0) == 0
        and _artifact_has_ambiguous_language(parsed_artifact)
    ):
        guard_rejection_counts["ambiguous_language"] += 1


def _artifact_has_ambiguous_language(
    parsed_artifact: ParsedAnnouncementArtifact,
) -> bool:
    return any(
        has_ambiguous_graph_language(section.text)
        for section in parsed_artifact.sections
    )


def _is_deterministic_anchor(fact: AnnouncementFactCandidate) -> bool:
    primary_entity = fact.fact_content.get("primary_entity")
    if isinstance(primary_entity, dict):
        if primary_entity.get("resolution_method") == "ts_code":
            return True
    return fact.primary_entity_id.startswith("ts_code:")


def _has_unresolved_ref(fact: AnnouncementFactCandidate) -> bool:
    candidate_ids = [fact.primary_entity_id, *fact.related_entity_ids]
    if any(_is_unresolved_ref(value) for value in candidate_ids):
        return True
    primary_entity = fact.fact_content.get("primary_entity")
    if isinstance(primary_entity, dict):
        if primary_entity.get("unresolved_ref"):
            return True
        resolution_method = primary_entity.get("resolution_method")
        if isinstance(resolution_method, str) and resolution_method.startswith(
            "unresolved"
        ):
            return True
    return False


def _is_unresolved_ref(value: str) -> bool:
    lowered = value.strip().lower()
    return lowered == "unresolved" or lowered.startswith("unresolved:")


def _int_field(sample: dict[str, Any], key: str, *, default: int) -> int:
    value = sample.get(key, default)
    try:
        return int(value)
    except (TypeError, ValueError):
        return default
