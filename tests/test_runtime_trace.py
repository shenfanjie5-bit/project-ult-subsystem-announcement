from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from subsystem_announcement.config import AnnouncementConfig
from subsystem_announcement.runtime.trace import (
    AnnouncementExtractionRun,
    CandidateSubmitTrace,
    RunTraceError,
    TraceStore,
)


def test_trace_store_round_trips_run_json(tmp_path: Path) -> None:
    store = TraceStore(AnnouncementConfig(artifact_root=tmp_path))
    run = AnnouncementExtractionRun(
        run_id="run-1",
        announcement_id="ann-1",
        status="partial_failed",
        started_at=datetime(2026, 4, 18, 9, 0, tzinfo=timezone.utc),
        finished_at=datetime(2026, 4, 18, 9, 1, tzinfo=timezone.utc),
        document_artifact_path=tmp_path / "documents" / "ann-1.pdf",
        parsed_artifact_path=tmp_path / "parsed" / "ann-1.json",
        candidate_count=2,
        submit_success_count=1,
        submit_failure_count=1,
        candidate_traces=[
            CandidateSubmitTrace(
                fact_id="fact-1",
                status="accepted",
                receipt_id="receipt-1",
                attempts=1,
            ),
            CandidateSubmitTrace(
                fact_id="fact-2",
                status="failed",
                attempts=3,
                errors=["rejected"],
            ),
        ],
        errors=[RunTraceError(stage="submit", fact_id="fact-2", message="rejected")],
    )

    trace_path = store.write(run)
    loaded = store.load(trace_path)

    assert trace_path == tmp_path / "runs" / "run-1.json"
    assert loaded == run
    assert loaded.trace_path == trace_path
    assert loaded.candidate_traces[0].receipt_id == "receipt-1"
    assert loaded.errors[0].stage == "submit"


def test_trace_store_rejects_unsafe_run_id(tmp_path: Path) -> None:
    store = TraceStore(tmp_path)
    run = AnnouncementExtractionRun(
        run_id="../outside",
        announcement_id="ann-1",
        started_at=datetime.now(timezone.utc),
    )

    with pytest.raises(ValueError, match="Unsafe run_id"):
        store.write(run)
