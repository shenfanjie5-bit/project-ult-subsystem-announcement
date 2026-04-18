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
    ...,
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
        kwargs: dict[str, Any] = {}
        if self._entity_registry is not None:
            kwargs["entity_registry"] = self._entity_registry
        if self._reasoner is not None:
            kwargs["reasoner"] = self._reasoner
        supported_kwargs = _supported_extract_kwargs(self._extract_func, kwargs)
        return await _maybe_await(self._extract_func(artifact, **supported_kwargs))

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
