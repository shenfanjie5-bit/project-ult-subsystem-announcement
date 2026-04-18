"""Runtime public API for announcement processing."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .metrics import (
        MetricThresholds,
        MetricsRegressionReport,
        assert_metrics_within_thresholds,
        compute_metrics_for_manifest,
    )
    from .pipeline import AnnouncementPipeline
    from .repair import (
        RepairReason,
        RepairRequest,
        RepairResult,
        repair_parsed_artifact,
    )
    from .replay import (
        ReplayRequest,
        ReplayResult,
        envelope_from_document_artifact,
        load_cached_document_for_replay,
        replay_announcement,
    )
    from .submit import submit_candidates
    from .trace import AnnouncementExtractionRun

__all__ = [
    "AnnouncementExtractionRun",
    "AnnouncementPipeline",
    "MetricThresholds",
    "MetricsRegressionReport",
    "RepairReason",
    "RepairRequest",
    "RepairResult",
    "ReplayRequest",
    "ReplayResult",
    "assert_metrics_within_thresholds",
    "compute_metrics_for_manifest",
    "envelope_from_document_artifact",
    "load_cached_document_for_replay",
    "repair_parsed_artifact",
    "replay_announcement",
    "submit_candidates",
]


def __getattr__(name: str) -> object:
    if name == "AnnouncementPipeline":
        from .pipeline import AnnouncementPipeline

        return AnnouncementPipeline
    if name == "submit_candidates":
        from .submit import submit_candidates

        return submit_candidates
    if name == "AnnouncementExtractionRun":
        from .trace import AnnouncementExtractionRun

        return AnnouncementExtractionRun
    if name in {
        "ReplayRequest",
        "ReplayResult",
        "envelope_from_document_artifact",
        "load_cached_document_for_replay",
        "replay_announcement",
    }:
        from . import replay as replay_module

        return getattr(replay_module, name)
    if name in {
        "RepairReason",
        "RepairRequest",
        "RepairResult",
        "repair_parsed_artifact",
    }:
        from . import repair as repair_module

        return getattr(repair_module, name)
    if name in {
        "MetricThresholds",
        "MetricsRegressionReport",
        "assert_metrics_within_thresholds",
        "compute_metrics_for_manifest",
    }:
        from . import metrics as metrics_module

        return getattr(metrics_module, name)
    raise AttributeError(name)
