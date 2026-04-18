"""Batch Ex-1 submission through the announcement SDK adapter."""

from __future__ import annotations

import hashlib
import json
import threading
from collections.abc import Mapping, Sequence
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field

from subsystem_announcement.discovery.errors import NonOfficialSourceError
from subsystem_announcement.discovery.fetcher import _validate_official_url_text
from subsystem_announcement.extract import AnnouncementFactCandidate
from subsystem_announcement.extract.candidates import FORBIDDEN_PAYLOAD_KEYS

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
_VOLATILE_IDEMPOTENCY_PAYLOAD_KEYS = {"extracted_at"}


class CandidateSubmitReceipt(BaseModel):
    """Stable receipt recorded after a candidate submit is accepted."""

    model_config = ConfigDict(extra="forbid")

    fact_id: str = Field(min_length=1)
    payload_hash: str = Field(min_length=64, max_length=64)
    receipt_id: str = Field(min_length=1)
    ex_type: Literal["Ex-1"] = "Ex-1"
    attempts: int = Field(ge=1)
    accepted_at: datetime
    warnings: tuple[str, ...] = Field(default_factory=tuple)
    errors: tuple[str, ...] = Field(default_factory=tuple)


class SubmitBatchResult(BaseModel):
    """Summary of an ordered Ex-1 batch submission."""

    model_config = ConfigDict(extra="forbid")

    submitted: int = Field(ge=0)
    skipped_duplicates: int = Field(ge=0)
    failed: int = Field(ge=0)
    receipts: list[CandidateSubmitReceipt] = Field(default_factory=list)
    failures: list[CandidateSubmitTrace] = Field(default_factory=list)
    traces: list[CandidateSubmitTrace] = Field(default_factory=list)


class SubmitIdempotencyStore:
    """Idempotency records keyed by ``fact_id`` and payload hash."""

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
    ) -> CandidateSubmitReceipt | None:
        """Return the previous receipt for an identical candidate payload."""

        with self.locked():
            return self._seen_loaded(candidate_id, payload_hash)

    def record(
        self,
        candidate_id: str,
        payload_hash: str,
        receipt: CandidateSubmitReceipt,
    ) -> None:
        """Record an accepted candidate receipt."""

        with self.locked():
            self._record_loaded(candidate_id, payload_hash, receipt)

    @contextmanager
    def locked(self) -> Any:
        """Hold the in-process and file-backed idempotency locks."""

        with self._thread_lock:
            with self._file_lock():
                if self.path is not None:
                    if self.path.exists():
                        self._load()
                    else:
                        self._records = {}
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
        candidate_id: str,
        payload_hash: str,
    ) -> CandidateSubmitReceipt | None:
        return self._records.get(_idempotency_key(candidate_id, payload_hash))

    def _record_loaded(
        self,
        candidate_id: str,
        payload_hash: str,
        receipt: CandidateSubmitReceipt,
    ) -> None:
        self._records[_idempotency_key(candidate_id, payload_hash)] = receipt
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
    candidates: Sequence[AnnouncementFactCandidate],
    subsystem: AnnouncementSubsystem,
    *,
    idempotency_store: SubmitIdempotencyStore | None = None,
    max_attempts: int = 3,
) -> SubmitBatchResult:
    """Submit Ex-1 fact candidates in order with retry and idempotency."""

    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")

    store = idempotency_store or SubmitIdempotencyStore()
    submitted = 0
    skipped_duplicates = 0
    failed = 0
    receipts: list[CandidateSubmitReceipt] = []
    failures: list[CandidateSubmitTrace] = []
    traces: list[CandidateSubmitTrace] = []

    for candidate in candidates:
        try:
            payload = _validated_payload(candidate)
            payload_hash = _payload_hash(payload)
        except Exception as exc:
            failed += 1
            fact_id = getattr(candidate, "fact_id", "unknown")
            trace = CandidateSubmitTrace(
                fact_id=str(fact_id),
                status="failed",
                attempts=0,
                errors=[str(exc)],
            )
            failures.append(trace)
            traces.append(trace)
            continue

        with store.locked():
            previous_receipt = store._seen_loaded(candidate.fact_id, payload_hash)
            if previous_receipt is not None:
                skipped_duplicates += 1
                trace = CandidateSubmitTrace(
                    fact_id=candidate.fact_id,
                    status="duplicate",
                    receipt_id=previous_receipt.receipt_id,
                    attempts=0,
                    errors=[],
                )
                receipts.append(previous_receipt)
                traces.append(trace)
                continue

            receipt, trace = _submit_one(
                candidate.fact_id,
                payload_hash,
                payload,
                subsystem,
                max_attempts=max_attempts,
            )
            traces.append(trace)
            if receipt is None:
                failed += 1
                failures.append(trace)
                continue

            submitted += 1
            receipts.append(receipt)
            store._record_loaded(candidate.fact_id, payload_hash, receipt)

    return SubmitBatchResult(
        submitted=submitted,
        skipped_duplicates=skipped_duplicates,
        failed=failed,
        receipts=receipts,
        failures=failures,
        traces=traces,
    )


def _submit_one(
    fact_id: str,
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
                fact_id=fact_id,
                payload_hash=payload_hash,
                receipt_id=last_receipt_id,
                attempts=attempt,
                accepted_at=datetime.now(timezone.utc),
                warnings=_string_tuple(_result_field(result, "warnings")),
                errors=_string_tuple(_result_field(result, "errors")),
            )
            trace = CandidateSubmitTrace(
                fact_id=fact_id,
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
            "subsystem-sdk rejected Ex-1 payload"
            + (f": {detail}" if detail else "")
        )

    return None, CandidateSubmitTrace(
        fact_id=fact_id,
        status="failed",
        receipt_id=last_receipt_id,
        attempts=max_attempts,
        errors=errors,
    )


def _validated_payload(candidate: AnnouncementFactCandidate) -> dict[str, Any]:
    payload = candidate.to_ex_payload()
    if payload.get("ex_type") != "Ex-1":
        raise ValueError("submit_candidates only accepts Ex-1 payloads")
    if not payload.get("evidence_spans"):
        raise ValueError("Ex-1 payload requires at least one evidence span")
    source_reference = payload.get("source_reference")
    if not isinstance(source_reference, Mapping):
        raise ValueError("Ex-1 payload requires source_reference")
    _validate_source_reference(payload, source_reference)
    _reject_forbidden_runtime_keys(payload)
    validated = AnnouncementFactCandidate.model_validate(payload)
    return validated.model_dump(mode="json")


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
                    f"Ex-1 payload contains forbidden runtime key: {path}{key_text}"
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


def _idempotency_key(candidate_id: str, payload_hash: str) -> str:
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
