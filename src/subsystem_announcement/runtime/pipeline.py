"""Single-envelope announcement processing pipeline."""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from subsystem_announcement.config import AnnouncementConfig
from subsystem_announcement.discovery import (
    AnnouncementDiscoveryResult,
    AnnouncementEnvelope,
    consume_announcement_ref,
)
from subsystem_announcement.discovery.document import AnnouncementDocumentArtifact
from subsystem_announcement.extract import (
    AnnouncementFactCandidate,
    extract_fact_candidates,
)
from subsystem_announcement.parse import ParsedAnnouncementArtifact, parse_announcement
from subsystem_announcement.parse.artifact import write_parsed_artifact

from .sdk_adapter import AnnouncementSubsystem
from .submit import SubmitBatchResult, SubmitIdempotencyStore, submit_candidates
from .trace import AnnouncementExtractionRun, RunTraceError, TraceStore


DiscoveryFunc = Callable[
    [AnnouncementEnvelope, AnnouncementConfig],
    AnnouncementDiscoveryResult | Awaitable[AnnouncementDiscoveryResult],
]
ParseFunc = Callable[
    [AnnouncementDocumentArtifact, AnnouncementConfig],
    ParsedAnnouncementArtifact | Awaitable[ParsedAnnouncementArtifact],
]
ExtractFunc = Callable[
    [ParsedAnnouncementArtifact],
    Sequence[AnnouncementFactCandidate] | Awaitable[Sequence[AnnouncementFactCandidate]],
]


class AnnouncementPipeline:
    """Process one announcement envelope through Ex-1 submission."""

    def __init__(
        self,
        config: AnnouncementConfig,
        *,
        subsystem: AnnouncementSubsystem | None = None,
        discovery_func: DiscoveryFunc = consume_announcement_ref,
        parse_func: ParseFunc = parse_announcement,
        extract_func: ExtractFunc = extract_fact_candidates,
        idempotency_store: SubmitIdempotencyStore | None = None,
        trace_store: TraceStore | None = None,
    ) -> None:
        self.config = config
        self._subsystem = subsystem
        self._discovery_func = discovery_func
        self._parse_func = parse_func
        self._extract_func = extract_func
        self._idempotency_store = idempotency_store or SubmitIdempotencyStore(
            Path(config.artifact_root) / "runs" / "submit_idempotency.json"
        )
        self._trace_store = trace_store or TraceStore(config)

    async def process_envelope(
        self,
        envelope: AnnouncementEnvelope,
    ) -> AnnouncementExtractionRun:
        """Run discovery, parse, Ex-1 extraction, submit, and trace persistence."""

        run = AnnouncementExtractionRun(
            run_id=str(uuid4()),
            announcement_id=envelope.announcement_id,
            started_at=datetime.now(timezone.utc),
        )
        try:
            discovery_result = await self._call_discovery(envelope)
            run.document_artifact_path = discovery_result.document.local_path

            parsed_artifact = await self._call_parse(discovery_result.document)
            run.parsed_artifact_path = write_parsed_artifact(
                parsed_artifact,
                self.config.artifact_root,
            )

            candidates = list(await self._call_extract(parsed_artifact))
            run.candidate_count = len(candidates)

            if candidates:
                submit_result = submit_candidates(
                    candidates,
                    self._get_subsystem(),
                    idempotency_store=self._idempotency_store,
                )
            else:
                submit_result = SubmitBatchResult(
                    submitted=0,
                    skipped_duplicates=0,
                    failed=0,
                )
            _apply_submit_result(run, submit_result)
            run.status = _status_from_submit_result(submit_result, len(candidates))
        except Exception as exc:
            run.status = "failed"
            run.errors.append(
                RunTraceError(stage=_stage_for_run(run), message=str(exc))
            )
        finally:
            run.finished_at = datetime.now(timezone.utc)
            self._trace_store.write(run)
        return run

    async def _call_discovery(
        self,
        envelope: AnnouncementEnvelope,
    ) -> AnnouncementDiscoveryResult:
        return await _maybe_await(self._discovery_func(envelope, self.config))

    async def _call_parse(
        self,
        document: AnnouncementDocumentArtifact,
    ) -> ParsedAnnouncementArtifact:
        return await _maybe_await(self._parse_func(document, self.config))

    async def _call_extract(
        self,
        artifact: ParsedAnnouncementArtifact,
    ) -> Sequence[AnnouncementFactCandidate]:
        return await _maybe_await(self._extract_func(artifact))

    def _get_subsystem(self) -> AnnouncementSubsystem:
        if self._subsystem is None:
            self._subsystem = AnnouncementSubsystem(self.config)
        return self._subsystem


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


def _apply_submit_result(
    run: AnnouncementExtractionRun,
    result: SubmitBatchResult,
) -> None:
    run.submit_success_count = result.submitted
    run.submit_duplicate_count = result.skipped_duplicates
    run.submit_failure_count = result.failed
    run.submit_receipts = [
        receipt.model_dump(mode="json") for receipt in result.receipts
    ]
    run.candidate_traces = result.traces
    for failure in result.failures:
        message = "; ".join(failure.errors) if failure.errors else "submit failed"
        run.errors.append(
            RunTraceError(
                stage="submit",
                fact_id=failure.fact_id,
                message=message,
            )
        )


def _status_from_submit_result(
    result: SubmitBatchResult,
    candidate_count: int,
) -> str:
    if result.failed == 0:
        return "succeeded"
    if candidate_count == 0 or result.failed == candidate_count:
        return "failed"
    return "partial_failed"


def _stage_for_run(run: AnnouncementExtractionRun) -> str:
    if run.document_artifact_path is None:
        return "discovery"
    if run.parsed_artifact_path is None:
        return "parse"
    if run.candidate_count == 0 and not run.candidate_traces:
        return "extract"
    return "submit"
