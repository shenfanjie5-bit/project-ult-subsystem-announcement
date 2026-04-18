"""Auditable run trace models and persistence."""

from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from subsystem_announcement.config import AnnouncementConfig


_RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")


class CandidateSubmitTrace(BaseModel):
    """Per-candidate submit trace for one Ex candidate."""

    model_config = ConfigDict(extra="forbid")

    fact_id: str = Field(min_length=1)
    candidate_id: str | None = None
    ex_type: Literal["Ex-1", "Ex-2", "Ex-3"] | None = None
    status: Literal["accepted", "duplicate", "failed"]
    receipt_id: str | None = None
    attempts: int = Field(ge=0)
    errors: list[str] = Field(default_factory=list)
    requires_reconciliation: bool = False


class RunTraceError(BaseModel):
    """Stage-localized error recorded in an extraction run trace."""

    model_config = ConfigDict(extra="forbid")

    stage: str = Field(min_length=1)
    message: str = Field(min_length=1)
    fact_id: str | None = None


class AnnouncementExtractionRun(BaseModel):
    """Audit trace for one announcement envelope processing run."""

    model_config = ConfigDict(extra="forbid")

    run_id: str = Field(min_length=1)
    announcement_id: str = Field(min_length=1)
    status: Literal["running", "succeeded", "partial_failed", "failed"] = "running"
    started_at: datetime
    finished_at: datetime | None = None
    document_artifact_path: Path | None = None
    parsed_artifact_path: Path | None = None
    candidate_count: int = Field(default=0, ge=0)
    submit_success_count: int = Field(default=0, ge=0)
    submit_duplicate_count: int = Field(default=0, ge=0)
    submit_failure_count: int = Field(default=0, ge=0)
    submit_receipts: list[dict[str, object]] = Field(default_factory=list)
    candidate_traces: list[CandidateSubmitTrace] = Field(default_factory=list)
    errors: list[RunTraceError] = Field(default_factory=list)
    trace_path: Path | None = None

    @field_validator("started_at", "finished_at")
    @classmethod
    def require_timezone(cls, value: datetime | None) -> datetime | None:
        """Reject naive timestamps so trace ordering is replayable."""

        if value is None:
            return value
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("trace timestamps must include timezone information")
        return value


class TraceStore:
    """Read and write run traces under ``artifact_root/runs``."""

    def __init__(self, config_or_root: AnnouncementConfig | Path) -> None:
        if isinstance(config_or_root, AnnouncementConfig):
            artifact_root = config_or_root.artifact_root
        else:
            artifact_root = config_or_root
        self.run_root = Path(artifact_root) / "runs"

    def write(self, run: AnnouncementExtractionRun) -> Path:
        """Persist a run trace and return the JSON path."""

        run_id = _safe_run_id(run.run_id)
        path = self.run_root / f"{run_id}.json"
        try:
            self.run_root.mkdir(parents=True, exist_ok=True)
            run.trace_path = path
            path.write_text(run.model_dump_json(indent=2), encoding="utf-8")
        except OSError as exc:
            raise RuntimeError(f"Unable to write run trace: path={path}") from exc
        return path

    def load(self, path: Path) -> AnnouncementExtractionRun:
        """Load a run trace JSON file."""

        trace_path = Path(path)
        try:
            return AnnouncementExtractionRun.model_validate_json(
                trace_path.read_text(encoding="utf-8")
            )
        except (OSError, ValueError) as exc:
            raise RuntimeError(f"Unable to load run trace: path={trace_path}") from exc


def _safe_run_id(value: str) -> str:
    if (
        not _RUN_ID_RE.fullmatch(value)
        or "/" in value
        or "\\" in value
        or value in {".", ".."}
        or Path(value).is_absolute()
    ):
        raise ValueError(f"Unsafe run_id for trace path: {value!r}")
    return value
