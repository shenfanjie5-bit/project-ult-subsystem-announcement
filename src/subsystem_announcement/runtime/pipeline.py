"""Single-envelope announcement processing pipeline."""

from __future__ import annotations

import inspect
import importlib
from collections.abc import Awaitable, Callable, Mapping, Sequence
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
    EntityMention,
    EntityRegistryClient,
    EntityResolution,
    ReasonerRuntimeBridge,
    StructuredReasoner,
    extract_fact_candidates,
)
from subsystem_announcement.graph import (
    AnnouncementGraphDeltaCandidate,
    GraphFunc,
    derive_graph_delta_candidates,
)
from subsystem_announcement.parse import ParsedAnnouncementArtifact, parse_announcement
from subsystem_announcement.parse.artifact import write_parsed_artifact
from subsystem_announcement.signals import (
    AnnouncementSignalCandidate,
    SignalFunc,
    derive_signal_candidates,
)

from .sdk_adapter import AnnouncementSubsystem
from .submit import (
    CandidatePayload,
    SubmitBatchResult,
    SubmitIdempotencyStore,
    candidate_id_for,
    ex_type_for,
    submit_candidates,
)
from .trace import (
    AnnouncementExtractionRun,
    CandidateSubmitTrace,
    RunTraceError,
    TraceStore,
)


DiscoveryFunc = Callable[
    [AnnouncementEnvelope, AnnouncementConfig],
    AnnouncementDiscoveryResult | Awaitable[AnnouncementDiscoveryResult],
]
ParseFunc = Callable[
    [AnnouncementDocumentArtifact, AnnouncementConfig],
    ParsedAnnouncementArtifact | Awaitable[ParsedAnnouncementArtifact],
]
ExtractFunc = Callable[
    ...,
    Sequence[AnnouncementFactCandidate] | Awaitable[Sequence[AnnouncementFactCandidate]],
]


class AnnouncementPipeline:
    """Process one announcement envelope through Ex candidate submission."""

    def __init__(
        self,
        config: AnnouncementConfig,
        *,
        subsystem: AnnouncementSubsystem | None = None,
        discovery_func: DiscoveryFunc = consume_announcement_ref,
        parse_func: ParseFunc = parse_announcement,
        extract_func: ExtractFunc = extract_fact_candidates,
        signal_func: SignalFunc = derive_signal_candidates,
        graph_func: GraphFunc = derive_graph_delta_candidates,
        idempotency_store: SubmitIdempotencyStore | None = None,
        trace_store: TraceStore | None = None,
    ) -> None:
        self.config = config
        self._subsystem = subsystem
        self._discovery_func = discovery_func
        self._parse_func = parse_func
        self._extract_func = extract_func
        self._signal_func = signal_func
        self._graph_func = graph_func
        self._entity_registry = _build_entity_registry(config)
        self._reasoner = _build_reasoner(config)
        self._idempotency_store = idempotency_store or SubmitIdempotencyStore(
            Path(config.artifact_root) / "runs" / "submit_idempotency.json"
        )
        self._trace_store = trace_store or TraceStore(config)

    async def process_envelope(
        self,
        envelope: AnnouncementEnvelope,
    ) -> AnnouncementExtractionRun:
        """Run discovery, parse, Ex extraction, submit, and trace."""

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

            facts = list(await self._call_extract(parsed_artifact))
            signals = list(await self._call_signals(facts))
            graph_deltas = list(await self._call_graph(facts))
            downstream_candidates: list[CandidatePayload] = [*signals, *graph_deltas]
            run.candidate_count = len(facts) + len(downstream_candidates)

            if run.candidate_count:
                submit_result = _submit_candidate_phases(
                    facts,
                    downstream_candidates,
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
            run.status = _status_from_submit_result(
                submit_result,
                run.candidate_count,
            )
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
        kwargs: dict[str, Any] = {}
        if self._entity_registry is not None:
            kwargs["entity_registry"] = self._entity_registry
        if self._reasoner is not None:
            kwargs["reasoner"] = self._reasoner
        supported_kwargs = _supported_extract_kwargs(self._extract_func, kwargs)
        return await _maybe_await(self._extract_func(artifact, **supported_kwargs))

    async def _call_signals(
        self,
        facts: Sequence[AnnouncementFactCandidate],
    ) -> Sequence[AnnouncementSignalCandidate]:
        return await _maybe_await(self._signal_func(facts))

    async def _call_graph(
        self,
        facts: Sequence[AnnouncementFactCandidate],
    ) -> Sequence[AnnouncementGraphDeltaCandidate]:
        return await _maybe_await(self._graph_func(facts))

    def _get_subsystem(self) -> AnnouncementSubsystem:
        if self._subsystem is None:
            self._subsystem = AnnouncementSubsystem(self.config)
        return self._subsystem


class EntityRegistryRuntimeAdapter:
    """Adapter around the entity-registry runtime module."""

    def __init__(self, *, endpoint: str | None = None) -> None:
        self.endpoint = endpoint

    def lookup_alias(self, name: str) -> EntityResolution | Mapping[str, Any] | None:
        """Resolve a deterministic alias through entity-registry."""

        return self._call_runtime("lookup_alias", name)

    def resolve_mentions(
        self,
        mentions: Sequence[EntityMention],
    ) -> Sequence[EntityResolution | Mapping[str, Any]]:
        """Resolve fuzzy mentions through entity-registry."""

        payload = [mention.model_dump(mode="json") for mention in mentions]
        result = self._call_runtime("resolve_mentions", payload)
        if result is None:
            return []
        if isinstance(result, Sequence) and not isinstance(
            result,
            str | bytes | bytearray,
        ):
            return result
        raise TypeError("entity_registry.resolve_mentions returned non-sequence")

    def _call_runtime(self, function_name: str, *args: Any) -> Any:
        try:
            entity_registry = importlib.import_module("entity_registry")
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "entity-registry endpoint is configured but "
                "the entity_registry runtime module is not importable"
            ) from exc
        runtime_func = getattr(entity_registry, function_name, None)
        if not callable(runtime_func):
            raise RuntimeError(f"entity_registry.{function_name} is not callable")
        if _accepts_keyword(runtime_func, "endpoint") and self.endpoint is not None:
            return runtime_func(*args, endpoint=self.endpoint)
        return runtime_func(*args)


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


def _build_entity_registry(
    config: AnnouncementConfig,
) -> EntityRegistryClient | None:
    if config.entity_registry_endpoint is None:
        return None
    return EntityRegistryRuntimeAdapter(endpoint=config.entity_registry_endpoint)


def _build_reasoner(config: AnnouncementConfig) -> StructuredReasoner | None:
    if config.reasoner_endpoint is None:
        return None
    return ReasonerRuntimeBridge(endpoint=config.reasoner_endpoint)


def _supported_extract_kwargs(
    extract_func: ExtractFunc,
    kwargs: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        name: value
        for name, value in kwargs.items()
        if _accepts_keyword(extract_func, name)
    }


def _accepts_keyword(func: Callable[..., Any], name: str) -> bool:
    try:
        signature = inspect.signature(func)
    except (TypeError, ValueError):
        return True
    parameters = signature.parameters
    if any(
        parameter.kind == inspect.Parameter.VAR_KEYWORD
        for parameter in parameters.values()
    ):
        return True
    parameter = parameters.get(name)
    if parameter is None:
        return False
    return parameter.kind in {
        inspect.Parameter.KEYWORD_ONLY,
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
    }


def _submit_candidate_phases(
    facts: Sequence[AnnouncementFactCandidate],
    downstream_candidates: Sequence[CandidatePayload],
    subsystem: AnnouncementSubsystem,
    *,
    idempotency_store: SubmitIdempotencyStore,
) -> SubmitBatchResult:
    """Submit Ex-1 first, then gate Ex-2/Ex-3 on accepted Ex-1 ids."""

    results: list[SubmitBatchResult] = []
    if facts:
        fact_result = submit_candidates(
            facts,
            subsystem,
            idempotency_store=idempotency_store,
        )
        results.append(fact_result)
    else:
        fact_result = SubmitBatchResult(submitted=0, skipped_duplicates=0, failed=0)

    accepted_fact_ids = _accepted_fact_ids(fact_result)
    for candidate in downstream_candidates:
        missing_fact_ids = _missing_source_fact_ids(candidate, accepted_fact_ids)
        if missing_fact_ids:
            results.append(_failed_dependency_result(candidate, missing_fact_ids))
            continue
        results.append(
            submit_candidates(
                [candidate],
                subsystem,
                idempotency_store=idempotency_store,
            )
        )

    return _merge_submit_results(results)


def _accepted_fact_ids(result: SubmitBatchResult) -> set[str]:
    return {
        trace.candidate_id or trace.fact_id
        for trace in result.traces
        if trace.ex_type == "Ex-1" and trace.status in {"accepted", "duplicate"}
    }


def _missing_source_fact_ids(
    candidate: CandidatePayload,
    accepted_fact_ids: set[str],
) -> tuple[str, ...]:
    source_fact_ids = getattr(candidate, "source_fact_ids", ())
    return tuple(
        fact_id
        for fact_id in source_fact_ids
        if fact_id not in accepted_fact_ids
    )


def _failed_dependency_result(
    candidate: CandidatePayload,
    missing_fact_ids: Sequence[str],
) -> SubmitBatchResult:
    candidate_id = candidate_id_for(candidate)
    ex_type = ex_type_for(candidate)
    trace = CandidateSubmitTrace(
        fact_id=candidate_id,
        candidate_id=candidate_id,
        ex_type=ex_type,
        status="failed",
        attempts=0,
        errors=[
            f"skipped {ex_type} candidate because source_fact_ids are not "
            f"accepted Ex-1 facts: {', '.join(missing_fact_ids)}"
        ],
    )
    return SubmitBatchResult(
        submitted=0,
        skipped_duplicates=0,
        failed=1,
        failures=[trace],
        traces=[trace],
    )


def _merge_submit_results(
    results: Sequence[SubmitBatchResult],
) -> SubmitBatchResult:
    return SubmitBatchResult(
        submitted=sum(result.submitted for result in results),
        skipped_duplicates=sum(result.skipped_duplicates for result in results),
        failed=sum(result.failed for result in results),
        receipts=[
            receipt
            for result in results
            for receipt in result.receipts
        ],
        failures=[
            failure
            for result in results
            for failure in result.failures
        ],
        traces=[
            trace
            for result in results
            for trace in result.traces
        ],
    )


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
