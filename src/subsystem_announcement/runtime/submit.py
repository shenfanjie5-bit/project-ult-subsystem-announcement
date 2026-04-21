"""Batch Ex candidate submission through the announcement SDK adapter."""

from __future__ import annotations

import hashlib
import json
import threading
from collections.abc import Mapping, Sequence
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal, TypeAlias
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field

from subsystem_announcement.discovery.errors import NonOfficialSourceError
from subsystem_announcement.discovery.fetcher import _validate_official_url_text
from subsystem_announcement.extract import AnnouncementFactCandidate
from subsystem_announcement.extract.candidates import FORBIDDEN_PAYLOAD_KEYS
from subsystem_announcement.graph import AnnouncementGraphDeltaCandidate
from subsystem_announcement.signals import AnnouncementSignalCandidate

from .sdk_adapter import AnnouncementSubsystem
from .trace import CandidateSubmitTrace

try:
    import fcntl
except ImportError:  # pragma: no cover - fcntl is available on supported CI hosts.
    fcntl = None  # type: ignore[assignment]


_FORBIDDEN_RUNTIME_KEYS = FORBIDDEN_PAYLOAD_KEYS | {
    "cache_path",
    "artifact_path",
    "document_path",
    "parsed_path",
    "document_artifact_path",
    "parsed_artifact_path",
}
# Stage 2.8 follow-up #2: ``produced_at`` is added by
# ``_normalize_for_sdk`` as a copy of extracted_at/generated_at — it
# inherits the same volatility (changes per construction) so it must
# be excluded from idempotency hashes alongside its source fields.
#
# Stage 2.8 follow-up #3: ``producer_context`` deliberately STAYS OUT of
# this set — its contents (announcement_id, source_fact_ids,
# source_reference, evidence_spans_detail, confidence) are stable
# discriminating provenance that two different logical events would
# differ on. Dropping the whole ``producer_context`` from the hash would
# collapse them into one idempotency key. The volatile timestamps
# (extracted_at / generated_at) are kept at top-level (Ex-1) or renamed
# to produced_at (Ex-2/3); generated_at is also dropped from
# producer_context for the same reason. See the plan's Part C section.
_VOLATILE_IDEMPOTENCY_PAYLOAD_KEYS = {
    "extracted_at",
    "generated_at",
    "produced_at",
}

# Stage 2.8 follow-up #3 — full canonical mapper. SignalDirection (local)
# values map to contracts.core.types.Direction (canonical) values.
# Plain value→value rename: positive market signal == bullish, etc.
_SIGNAL_DIRECTION_TO_CONTRACTS_DIRECTION: dict[str, str] = {
    "positive": "bullish",
    "negative": "bearish",
    "neutral": "neutral",
}

CandidatePayload: TypeAlias = (
    AnnouncementFactCandidate
    | AnnouncementSignalCandidate
    | AnnouncementGraphDeltaCandidate
)
ExType: TypeAlias = Literal["Ex-1", "Ex-2", "Ex-3"]


class CandidateSubmitReceipt(BaseModel):
    """Stable receipt recorded after a candidate submit is accepted."""

    model_config = ConfigDict(extra="forbid")

    fact_id: str = Field(min_length=1)
    candidate_id: str | None = None
    payload_hash: str = Field(min_length=64, max_length=64)
    receipt_id: str = Field(min_length=1)
    ex_type: ExType = "Ex-1"
    attempts: int = Field(ge=1)
    accepted_at: datetime
    warnings: tuple[str, ...] = Field(default_factory=tuple)
    errors: tuple[str, ...] = Field(default_factory=tuple)
    requires_reconciliation: bool = False


class SubmitBatchResult(BaseModel):
    """Summary of an ordered Ex candidate batch submission."""

    model_config = ConfigDict(extra="forbid")

    submitted: int = Field(ge=0)
    skipped_duplicates: int = Field(ge=0)
    failed: int = Field(ge=0)
    receipts: list[CandidateSubmitReceipt] = Field(default_factory=list)
    failures: list[CandidateSubmitTrace] = Field(default_factory=list)
    traces: list[CandidateSubmitTrace] = Field(default_factory=list)


class SubmitIdempotencyStore:
    """Idempotency records keyed by ``ex_type``, candidate id, and payload hash."""

    _path_locks: dict[Path, threading.RLock] = {}
    _path_locks_guard = threading.Lock()

    def __init__(self, path: Path | None = None) -> None:
        self.path = Path(path) if path is not None else None
        self._lock_path = (
            self.path.expanduser().resolve(strict=False)
            if self.path is not None
            else None
        )
        self._thread_lock = self._get_thread_lock(self._lock_path)
        self._records: dict[str, CandidateSubmitReceipt] = {}
        if self.path is not None and self.path.exists():
            with self.locked():
                pass

    def seen(
        self,
        candidate_id: str,
        payload_hash: str,
        ex_type: str = "Ex-1",
    ) -> CandidateSubmitReceipt | None:
        """Return the previous receipt for an identical candidate payload."""

        with self.locked():
            return self._seen_loaded(ex_type, candidate_id, payload_hash)

    def record(
        self,
        candidate_id: str,
        payload_hash: str,
        receipt: CandidateSubmitReceipt,
        ex_type: str | None = None,
    ) -> None:
        """Record an accepted candidate receipt."""

        with self.locked():
            self._record_loaded(
                ex_type or receipt.ex_type,
                candidate_id,
                payload_hash,
                receipt,
            )

    @contextmanager
    def locked(self) -> Any:
        """Hold the in-process and file-backed idempotency locks."""

        with self._thread_lock:
            with self._file_lock():
                if self.path is not None:
                    if self.path.exists():
                        self._load()
                yield

    @classmethod
    def _get_thread_lock(cls, lock_path: Path | None) -> threading.RLock:
        if lock_path is None:
            return threading.RLock()
        with cls._path_locks_guard:
            lock = cls._path_locks.get(lock_path)
            if lock is None:
                lock = threading.RLock()
                cls._path_locks[lock_path] = lock
            return lock

    @contextmanager
    def _file_lock(self) -> Any:
        if self.path is None:
            yield
            return
        if fcntl is None:
            raise RuntimeError(
                "File-backed submit idempotency requires fcntl locking support"
            )

        lock_path = self.path.with_name(f"{self.path.name}.lock")
        try:
            lock_path.parent.mkdir(parents=True, exist_ok=True)
            with lock_path.open("a+", encoding="utf-8") as lock_file:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
                try:
                    yield
                finally:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
        except OSError as exc:
            raise RuntimeError(
                f"Unable to lock submit idempotency store: path={self.path}"
            ) from exc

    def _seen_loaded(
        self,
        ex_type: str,
        candidate_id: str,
        payload_hash: str,
    ) -> CandidateSubmitReceipt | None:
        receipt = self._records.get(
            _idempotency_key(ex_type, candidate_id, payload_hash)
        )
        if receipt is not None:
            return receipt
        if ex_type == "Ex-1":
            return self._records.get(_legacy_idempotency_key(candidate_id, payload_hash))
        return None

    def _record_loaded(
        self,
        ex_type: str,
        candidate_id: str,
        payload_hash: str,
        receipt: CandidateSubmitReceipt,
    ) -> None:
        self._records[_idempotency_key(ex_type, candidate_id, payload_hash)] = receipt
        self._persist()

    def _load(self) -> None:
        if self.path is None:
            return
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError(
                f"Unable to load submit idempotency store: path={self.path}"
            ) from exc
        if not isinstance(raw, Mapping):
            raise RuntimeError(
                f"Invalid submit idempotency store: path={self.path}"
            )
        self._records = {
            str(key): CandidateSubmitReceipt.model_validate(value)
            for key, value in raw.items()
        }

    def _persist(self) -> None:
        if self.path is None:
            return
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = self.path.with_name(f"{self.path.name}.tmp")
            tmp_path.write_text(
                json.dumps(
                    {
                        key: receipt.model_dump(mode="json")
                        for key, receipt in self._records.items()
                    },
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=True,
                ),
                encoding="utf-8",
            )
            tmp_path.replace(self.path)
        except OSError as exc:
            raise RuntimeError(
                f"Unable to persist submit idempotency store: path={self.path}"
            ) from exc


def submit_candidates(
    candidates: Sequence[CandidatePayload],
    subsystem: AnnouncementSubsystem,
    *,
    idempotency_store: SubmitIdempotencyStore | None = None,
    max_attempts: int = 3,
) -> SubmitBatchResult:
    """Submit Ex candidates with retry, idempotency, and same-batch Ex gating."""

    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")

    store = idempotency_store or SubmitIdempotencyStore()
    fact_candidates, downstream_candidates = _partition_candidate_phases(candidates)
    batch_fact_ids = _batch_fact_ids(fact_candidates)
    accepted_fact_ids: set[str] = set()
    submitted = 0
    skipped_duplicates = 0
    failed = 0
    receipts: list[CandidateSubmitReceipt] = []
    failures: list[CandidateSubmitTrace] = []
    traces: list[CandidateSubmitTrace] = []

    def process_candidate(candidate: CandidatePayload) -> CandidateSubmitTrace:
        nonlocal failed, skipped_duplicates, submitted

        try:
            payload = _validated_payload(candidate)
            payload_hash = _payload_hash(payload)
            candidate_id = candidate_id_for(candidate)
            ex_type = ex_type_for(candidate)
        except Exception as exc:
            failed += 1
            candidate_id = _best_effort_candidate_id(candidate)
            ex_type = _best_effort_ex_type(candidate)
            trace = CandidateSubmitTrace(
                fact_id=candidate_id,
                candidate_id=candidate_id,
                ex_type=ex_type,
                status="failed",
                attempts=0,
                errors=[str(exc)],
            )
            failures.append(trace)
            traces.append(trace)
            return trace

        # Stage 2.8 follow-up #3: gate reads from candidate (announcement-
        # local model attribute) NOT from post-_normalize_for_sdk wire
        # payload, because source_fact_ids is no longer at top-level on
        # the wire (it lives in producer_context now).
        missing_fact_ids = _missing_unaccepted_batch_fact_ids(
            candidate,
            accepted_fact_ids=accepted_fact_ids,
            batch_fact_ids=batch_fact_ids,
        )
        if ex_type in {"Ex-2", "Ex-3"} and missing_fact_ids:
            failed += 1
            trace = _dependency_failure_trace(
                candidate_id,
                ex_type,
                missing_fact_ids,
            )
            failures.append(trace)
            traces.append(trace)
            return trace

        with store.locked():
            previous_receipt = store._seen_loaded(ex_type, candidate_id, payload_hash)
            if previous_receipt is not None:
                skipped_duplicates += 1
                trace = CandidateSubmitTrace(
                    fact_id=candidate_id,
                    candidate_id=candidate_id,
                    ex_type=ex_type,
                    status="duplicate",
                    receipt_id=previous_receipt.receipt_id,
                    attempts=0,
                    errors=[],
                )
                receipts.append(previous_receipt)
                traces.append(trace)
                return trace

            receipt, trace = _submit_one(
                candidate_id,
                ex_type,
                payload_hash,
                payload,
                subsystem,
                max_attempts=max_attempts,
            )
            traces.append(trace)
            if receipt is None:
                failed += 1
                failures.append(trace)
                return trace

            submitted += 1
            try:
                store._record_loaded(ex_type, candidate_id, payload_hash, receipt)
            except Exception as exc:
                message = f"idempotency receipt persistence failed: {exc}"
                receipt = receipt.model_copy(
                    update={
                        "warnings": (*receipt.warnings, message),
                        "requires_reconciliation": True,
                    }
                )
                trace = trace.model_copy(
                    update={
                        "errors": [*trace.errors, message],
                        "requires_reconciliation": True,
                    }
                )
                store._records[
                    _idempotency_key(ex_type, candidate_id, payload_hash)
                ] = receipt
                traces[-1] = trace
            receipts.append(receipt)
            return trace

    for candidate in fact_candidates:
        trace = process_candidate(candidate)
        if trace.ex_type == "Ex-1" and trace.status in {"accepted", "duplicate"}:
            accepted_fact_ids.add(trace.candidate_id or trace.fact_id)

    for candidate in downstream_candidates:
        process_candidate(candidate)

    return SubmitBatchResult(
        submitted=submitted,
        skipped_duplicates=skipped_duplicates,
        failed=failed,
        receipts=receipts,
        failures=failures,
        traces=traces,
    )


def _partition_candidate_phases(
    candidates: Sequence[CandidatePayload],
) -> tuple[list[CandidatePayload], list[CandidatePayload]]:
    fact_candidates: list[CandidatePayload] = []
    downstream_candidates: list[CandidatePayload] = []
    for candidate in candidates:
        if _best_effort_ex_type(candidate) == "Ex-1":
            fact_candidates.append(candidate)
        else:
            downstream_candidates.append(candidate)
    return fact_candidates, downstream_candidates


def _batch_fact_ids(candidates: Sequence[CandidatePayload]) -> set[str]:
    fact_ids: set[str] = set()
    for candidate in candidates:
        fact_id = getattr(candidate, "fact_id", None)
        if isinstance(fact_id, str) and fact_id:
            fact_ids.add(fact_id)
    return fact_ids


def _missing_unaccepted_batch_fact_ids(
    candidate: CandidatePayload,
    *,
    accepted_fact_ids: set[str],
    batch_fact_ids: set[str],
) -> tuple[str, ...]:
    """Mixed-batch dependency gate for Ex-2/Ex-3 candidates.

    Reads ``candidate.source_fact_ids`` directly off the announcement-
    local Pydantic model (Signal / GraphDelta candidates). Stage 2.8
    follow-up #3 moved ``source_fact_ids`` from the wire payload into
    ``producer_context`` (since contracts.Ex2/Ex3 have no canonical
    ``source_fact_ids`` field), so the gate must NOT read from the
    post-``_normalize_for_sdk`` wire payload anymore — that would
    silently return ``()`` and let Ex-2/Ex-3 candidates skip past their
    Ex-1 dependency check (codex plan-review #4 P1: this would be a
    correctness bug, not just a performance issue).

    Returns the tuple of source fact ids that are present in the same
    submit batch (``batch_fact_ids``) but have not yet been accepted
    (``accepted_fact_ids``). Ex-1 candidates have no ``source_fact_ids``
    attribute — for them this returns ``()``.
    """

    source_fact_ids = getattr(candidate, "source_fact_ids", None)
    if not isinstance(source_fact_ids, Sequence) or isinstance(
        source_fact_ids,
        str | bytes | bytearray,
    ):
        return ()
    return tuple(
        fact_id
        for fact_id in source_fact_ids
        if (
            isinstance(fact_id, str)
            and fact_id in batch_fact_ids
            and fact_id not in accepted_fact_ids
        )
    )


def _dependency_failure_trace(
    candidate_id: str,
    ex_type: ExType,
    missing_fact_ids: Sequence[str],
) -> CandidateSubmitTrace:
    return CandidateSubmitTrace(
        fact_id=candidate_id,
        candidate_id=candidate_id,
        ex_type=ex_type,
        status="failed",
        attempts=0,
        errors=[
            f"skipped {ex_type} candidate because source_fact_ids are not "
            f"accepted Ex-1 facts in this submission: {', '.join(missing_fact_ids)}"
        ],
    )


def _submit_one(
    candidate_id: str,
    ex_type: ExType,
    payload_hash: str,
    payload: dict[str, Any],
    subsystem: AnnouncementSubsystem,
    *,
    max_attempts: int,
) -> tuple[CandidateSubmitReceipt | None, CandidateSubmitTrace]:
    errors: list[str] = []
    last_receipt_id: str | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            result = subsystem.submit(payload)
        except Exception as exc:
            errors.append(str(exc))
            continue

        last_receipt_id = _result_receipt_id(result)
        if _result_accepted(result):
            if last_receipt_id is None:
                errors.append("subsystem-sdk accepted payload without receipt_id")
                continue
            receipt = CandidateSubmitReceipt(
                fact_id=candidate_id,
                candidate_id=candidate_id,
                payload_hash=payload_hash,
                receipt_id=last_receipt_id,
                ex_type=ex_type,
                attempts=attempt,
                accepted_at=datetime.now(timezone.utc),
                warnings=_string_tuple(_result_field(result, "warnings")),
                errors=_string_tuple(_result_field(result, "errors")),
            )
            trace = CandidateSubmitTrace(
                fact_id=candidate_id,
                candidate_id=candidate_id,
                ex_type=ex_type,
                status="accepted",
                receipt_id=receipt.receipt_id,
                attempts=attempt,
                errors=[],
            )
            return receipt, trace

        rejection_errors = _string_tuple(_result_field(result, "errors"))
        rejection_warnings = _string_tuple(_result_field(result, "warnings"))
        detail = ", ".join(
            part
            for part in (
                f"errors={list(rejection_errors)}" if rejection_errors else "",
                f"warnings={list(rejection_warnings)}" if rejection_warnings else "",
            )
            if part
        )
        errors.append(
            f"subsystem-sdk rejected {ex_type} payload"
            + (f": {detail}" if detail else "")
        )

    return None, CandidateSubmitTrace(
        fact_id=candidate_id,
        candidate_id=candidate_id,
        ex_type=ex_type,
        status="failed",
        receipt_id=last_receipt_id,
        attempts=max_attempts,
        errors=errors,
    )


def _validated_payload(candidate: CandidatePayload) -> dict[str, Any]:
    payload = candidate.to_ex_payload()
    ex_type = payload.get("ex_type")
    if ex_type not in {"Ex-1", "Ex-2", "Ex-3"}:
        raise ValueError("submit_candidates only accepts Ex-1/Ex-2/Ex-3 payloads")
    if not payload.get("evidence_spans"):
        raise ValueError(f"{ex_type} payload requires at least one evidence span")
    source_reference = payload.get("source_reference")
    if not isinstance(source_reference, Mapping):
        raise ValueError(f"{ex_type} payload requires source_reference")
    _validate_source_reference(payload, source_reference)
    _reject_forbidden_runtime_keys(payload)
    if ex_type == "Ex-1":
        validated = AnnouncementFactCandidate.model_validate(payload)
    elif ex_type == "Ex-2":
        validated = AnnouncementSignalCandidate.model_validate(payload)
    else:
        validated = AnnouncementGraphDeltaCandidate.model_validate(payload)
    wire_payload = validated.model_dump(mode="json")
    return _normalize_for_sdk(wire_payload, ex_type)


def _serialize_evidence_ref(span: Mapping[str, Any], announcement_id: str) -> str:
    """Deterministic wire-ref string for an EvidenceSpan.

    Format: ``"{announcement_id}#{section_id}:{start_offset}-{end_offset}"``
    Each ref is self-contained (Layer B can correlate back to
    ``producer_context.evidence_spans_detail`` for quote + table_ref
    detail without a side-channel lookup). Each ref is min_length=1 (well
    above contracts.EvidenceRef requirement; announcement_id +
    section_id are themselves min_length=1).
    """

    section_id = str(span.get("section_id", ""))
    start_offset = span.get("start_offset", 0)
    end_offset = span.get("end_offset", 0)
    return f"{announcement_id}#{section_id}:{start_offset}-{end_offset}"


def _normalize_for_sdk(
    local_payload: dict[str, Any], ex_type: str
) -> dict[str, Any]:
    """Map an announcement-local candidate wire dict to the contracts
    canonical Ex-1 / Ex-2 / Ex-3 wire shape.

    Background (codex stage 2.8 review #7 P1, follow-up #3 cross-repo):
    follow-up #2 added the SDK-required ``subsystem_id`` + ``produced_at``
    fields but kept all other announcement-local fields at top-level. The
    real SDK link
    (``AnnouncementSubsystem.submit`` -> ``BaseSubsystemContext.submit``
    -> ``SubmitClient.submit`` -> ``validate_then_dispatch`` ->
    ``validate_payload`` -> ``contracts.schemas.Ex*.model_validate``)
    then rejected those payloads because of 5 schema mismatches:

      1. Field rename: announcement ``primary_entity_id`` vs
         contracts.Ex1 ``entity_id``.
      2. Extras rejected by ``extra="forbid"``: ``announcement_id``,
         ``related_entity_ids``, ``evidence_spans``, ``source_fact_ids``,
         ``source_reference`` (Ex-2/3 only — Ex-1 contracts has it),
         ``generated_at``, ``confidence`` (Ex-3 only).
      3. Enum value mismatch: ``SignalDirection
         {positive,negative,neutral}`` vs ``contracts.Direction
         {bullish,bearish,neutral}``.
      4. Ex-2 hard requirement announcement cannot satisfy:
         ``affected_sectors`` was ``min_length=1`` in v0.1.2; now relaxed
         to allow ``[]`` in v0.1.3.
      5. Ex-1 had no canonical evidence slot; v0.1.3 adds optional
         ``evidence: list[EvidenceRef] | None``.

    This normalizer (paired with contracts v0.1.3) closes the gap. The
    output is what ``contracts.schemas.Ex1CandidateFact.model_validate``
    (or Ex2/Ex3) accepts directly.

    Critical mapping notes (codex plan-review #4 P1, plan Part C):
    - **Ex-1 ``source_reference`` STAYS at top-level** (contracts.Ex1
      requires it). It is NOT moved into ``producer_context``. Ex-2/3
      contracts have NO ``source_reference`` slot, so it goes into
      ``producer_context`` for those.
    - **Ex-2 / Ex-3 ``generated_at`` is RENAMED to ``produced_at`` and
      DROPPED from top-level** (contracts.Ex2/3 have no ``generated_at``
      field; SDK ``_strip_sdk_envelope`` only strips
      ``{ex_type, semantic, produced_at}`` and would NOT strip
      ``generated_at``, so leaving it at top-level would be rejected by
      contracts ``extra="forbid"``).
    - ``producer_context`` deliberately holds **stable** provenance only
      (no timestamps); the whole dict goes into ``_payload_hash`` for
      idempotency discrimination. See ``_VOLATILE_IDEMPOTENCY_PAYLOAD_KEYS``
      docstring.
    - ``evidence_spans`` (full ``EvidenceSpan.model_dump`` list) is
      preserved in ``producer_context["evidence_spans_detail"]`` for
      Layer B replay/audit. Canonical wire ``evidence`` is a list of
      deterministic ref strings (announcement_id#section:start-end).
    """

    from .registration import MODULE_ID

    # All Ex types share these.
    announcement_id = str(local_payload.get("announcement_id", ""))
    evidence_spans_local = list(local_payload.get("evidence_spans") or [])
    evidence_refs = [
        _serialize_evidence_ref(span, announcement_id) for span in evidence_spans_local
    ]

    if ex_type == "Ex-1":
        producer_context: dict[str, Any] = {
            "announcement_id": announcement_id,
            "related_entity_ids": list(local_payload.get("related_entity_ids", [])),
            # Full EvidenceSpan dumps (with quote + table_ref) for Layer
            # B replay/audit; canonical wire `evidence` is just refs.
            "evidence_spans_detail": evidence_spans_local,
        }
        produced_at = local_payload.get("extracted_at")
        return {
            # SDK envelope routing field — subsystem-sdk's
            # ``_identify_ex_type`` requires ``ex_type`` on dict payloads
            # (announcement passes dicts, not BaseModel instances).
            # Stripped by ``_strip_sdk_envelope`` before
            # ``contracts.Ex1.model_validate`` and again before backend
            # dispatch — does not reach Layer B.
            "ex_type": "Ex-1",
            "subsystem_id": MODULE_ID,
            "fact_id": local_payload["fact_id"],
            "entity_id": local_payload["primary_entity_id"],  # rename
            "fact_type": str(local_payload["fact_type"]),  # FactType.value
            "fact_content": dict(local_payload.get("fact_content") or {}),
            "confidence": float(local_payload["confidence"]),
            # contracts.Ex1.source_reference is REQUIRED top-level.
            "source_reference": dict(local_payload["source_reference"]),
            "extracted_at": local_payload["extracted_at"],
            "evidence": evidence_refs,  # contracts v0.1.3 optional canonical
            "producer_context": producer_context,
            "produced_at": produced_at,
        }

    if ex_type == "Ex-2":
        producer_context = {
            "announcement_id": announcement_id,
            "source_fact_ids": list(local_payload.get("source_fact_ids", [])),
            "source_reference": dict(local_payload.get("source_reference") or {}),
            "evidence_spans_detail": evidence_spans_local,
        }
        produced_at = local_payload.get("generated_at")
        direction_local = str(local_payload.get("direction", ""))
        try:
            direction_canonical = _SIGNAL_DIRECTION_TO_CONTRACTS_DIRECTION[
                direction_local
            ]
        except KeyError as exc:
            raise ValueError(
                f"unknown SignalDirection value {direction_local!r}; expected one of "
                f"{sorted(_SIGNAL_DIRECTION_TO_CONTRACTS_DIRECTION)}"
            ) from exc
        return {
            "ex_type": "Ex-2",  # SDK envelope routing — see Ex-1 above
            "subsystem_id": MODULE_ID,
            "signal_id": local_payload["signal_id"],
            "signal_type": local_payload["signal_type"],
            "direction": direction_canonical,  # bullish/bearish/neutral
            "magnitude": float(local_payload["magnitude"]),
            "affected_entities": list(local_payload["affected_entities"]),
            # contracts v0.1.3 allows []. Sector enrichment happens
            # downstream at graph-engine; announcement has no sector data.
            "affected_sectors": [],
            "time_horizon": str(local_payload["time_horizon"]),  # enum.value
            "evidence": evidence_refs,
            "confidence": float(local_payload["confidence"]),
            "producer_context": producer_context,
            "produced_at": produced_at,
            # NOTE: ``generated_at`` deliberately NOT included at
            # top-level — renamed to ``produced_at`` above. SDK strip
            # does not cover ``generated_at``; contracts.Ex2 would
            # reject it as extra.
        }

    # Ex-3
    if ex_type != "Ex-3":
        raise ValueError(
            f"_normalize_for_sdk only supports Ex-1/Ex-2/Ex-3; got {ex_type!r}"
        )
    producer_context = {
        "announcement_id": announcement_id,
        "source_fact_ids": list(local_payload.get("source_fact_ids", [])),
        "source_reference": dict(local_payload.get("source_reference") or {}),
        "evidence_spans_detail": evidence_spans_local,
        # contracts.Ex3 has no ``confidence`` field — preserve here for
        # downstream Layer B replay/audit (announcement-local concept).
        "confidence": float(local_payload["confidence"]),
    }
    produced_at = local_payload.get("generated_at")
    return {
        "ex_type": "Ex-3",  # SDK envelope routing — see Ex-1 above
        "subsystem_id": MODULE_ID,
        "delta_id": local_payload["delta_id"],
        "delta_type": str(local_payload["delta_type"]),  # GraphDeltaType.value
        "source_node": local_payload["source_node"],
        "target_node": local_payload["target_node"],
        "relation_type": str(local_payload["relation_type"]),  # GraphRelationType.value
        "properties": dict(local_payload.get("properties") or {}),
        "evidence": evidence_refs,  # min_length=2 upstream guarantees ≥2
        "producer_context": producer_context,
        "produced_at": produced_at,
        # Same as Ex-2: ``generated_at`` deliberately NOT at top-level.
    }


def candidate_id_for(candidate: CandidatePayload) -> str:
    """Return the stable candidate id regardless of Ex type."""

    ex_type = ex_type_for(candidate)
    if ex_type == "Ex-1":
        return candidate.fact_id
    if ex_type == "Ex-2":
        return candidate.signal_id
    return candidate.delta_id


def ex_type_for(candidate: CandidatePayload) -> ExType:
    """Return the candidate Ex type."""

    ex_type = getattr(candidate, "ex_type", None)
    if ex_type not in {"Ex-1", "Ex-2", "Ex-3"}:
        raise ValueError("candidate must be Ex-1, Ex-2, or Ex-3")
    return ex_type


def _best_effort_candidate_id(candidate: object) -> str:
    for attribute in ("fact_id", "signal_id", "delta_id"):
        value = getattr(candidate, attribute, None)
        if isinstance(value, str) and value:
            return value
    return "unknown"


def _best_effort_ex_type(candidate: object) -> ExType | None:
    value = getattr(candidate, "ex_type", None)
    if value in {"Ex-1", "Ex-2", "Ex-3"}:
        return value
    return None


def _validate_source_reference(
    payload: Mapping[str, Any],
    source_reference: Mapping[str, Any],
) -> None:
    official_url = source_reference.get("official_url")
    if not isinstance(official_url, str) or not official_url.strip():
        raise ValueError("source_reference.official_url is required")
    parsed_url = urlsplit(official_url)
    if parsed_url.scheme not in {"http", "https"} or not parsed_url.netloc:
        raise ValueError("source_reference.official_url must be an official HTTP URL")
    try:
        _validate_official_url_text(
            official_url,
            announcement_id=str(payload.get("announcement_id") or "unknown"),
        )
    except NonOfficialSourceError as exc:
        raise ValueError(
            "source_reference.official_url must use an official disclosure domain"
        ) from exc
    source_announcement_id = source_reference.get("announcement_id")
    if (
        source_announcement_id is not None
        and source_announcement_id != payload.get("announcement_id")
    ):
        raise ValueError("source_reference.announcement_id must match candidate")


def _reject_forbidden_runtime_keys(value: Any, path: str = "") -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            key_text = str(key)
            if key_text in _FORBIDDEN_RUNTIME_KEYS:
                raise ValueError(
                    f"candidate payload contains forbidden runtime key: {path}{key_text}"
                )
            _reject_forbidden_runtime_keys(item, f"{path}{key_text}.")
    elif isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        for index, item in enumerate(value):
            _reject_forbidden_runtime_keys(item, f"{path}{index}.")


def _payload_hash(payload: Mapping[str, Any]) -> str:
    stable_payload = {
        key: value
        for key, value in payload.items()
        if key not in _VOLATILE_IDEMPOTENCY_PAYLOAD_KEYS
    }
    return hashlib.sha256(
        json.dumps(
            stable_payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _idempotency_key(ex_type: str, candidate_id: str, payload_hash: str) -> str:
    return f"{ex_type}:{candidate_id}:{payload_hash}"


def _legacy_idempotency_key(candidate_id: str, payload_hash: str) -> str:
    return f"{candidate_id}:{payload_hash}"


def _result_accepted(result: Any) -> bool:
    return bool(_result_field(result, "accepted"))


def _result_receipt_id(result: Any) -> str | None:
    value = _result_field(result, "receipt_id")
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _result_field(result: Any, field: str) -> Any:
    if isinstance(result, Mapping):
        return result.get(field)
    if hasattr(result, "model_dump"):
        return result.model_dump().get(field)
    return getattr(result, field, None)


def _string_tuple(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (value,)
    if isinstance(value, Sequence):
        return tuple(str(item) for item in value)
    return (str(value),)
