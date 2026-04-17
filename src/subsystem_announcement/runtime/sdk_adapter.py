"""SDK adapter for registration, heartbeat, and Ex payload submission."""

from __future__ import annotations

import importlib
import importlib.util
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Final, TypeAlias
from uuid import uuid4

from subsystem_announcement import PACKAGE_NAME, __version__
from subsystem_announcement.config import AnnouncementConfig

from ._sdk_stub import (
    Ex0Payload,
    ExPayload,
    HeartbeatPayload,
    RegistrationSpec,
    SubmitResult as StubSubmitResult,
    SubsystemBaseInterface as StubSubsystemBaseInterface,
)

TEST_STUB_ENV: Final[str] = "SUBSYSTEM_ANNOUNCEMENT_TEST_SDK_STUB"
_SDK_PACKAGE: Final[str] = "subsystem_sdk"

PayloadLike: TypeAlias = ExPayload | dict[str, Any]


class SubsystemSdkUnavailableError(RuntimeError):
    """Raised when runtime code starts without the required subsystem-sdk."""


class UnsupportedSubsystemSdkError(RuntimeError):
    """Raised when subsystem-sdk is installed but lacks the pinned API."""


@dataclass(frozen=True)
class _SdkApi:
    subsystem_base_interface: type[Any]
    registration_spec: type[Any]
    submit_receipt: type[Any]
    register_subsystem: Any
    send_heartbeat: Any
    submit: Any
    assert_producer_only: Any


def _load_official_sdk_api() -> _SdkApi | None:
    if importlib.util.find_spec(_SDK_PACKAGE) is None:
        return None

    try:
        base_module = importlib.import_module("subsystem_sdk.base")
        submit_module = importlib.import_module("subsystem_sdk.submit")
        heartbeat_module = importlib.import_module("subsystem_sdk.heartbeat")
        validate_module = importlib.import_module("subsystem_sdk.validate")
        return _SdkApi(
            subsystem_base_interface=_required_symbol(
                base_module,
                "SubsystemBaseInterface",
            ),
            registration_spec=_required_symbol(
                base_module,
                "SubsystemRegistrationSpec",
            ),
            submit_receipt=_required_symbol(submit_module, "SubmitReceipt"),
            register_subsystem=_required_symbol(base_module, "register_subsystem"),
            send_heartbeat=_required_symbol(heartbeat_module, "send_heartbeat"),
            submit=_required_symbol(submit_module, "submit"),
            assert_producer_only=_required_symbol(
                validate_module,
                "assert_producer_only",
            ),
        )
    except Exception as exc:
        raise UnsupportedSubsystemSdkError(
            "Unsupported subsystem-sdk version: expected pinned public API "
            "subsystem_sdk.base.{SubsystemBaseInterface,"
            "SubsystemRegistrationSpec,register_subsystem}, "
            "subsystem_sdk.submit.{SubmitReceipt,submit}, "
            "subsystem_sdk.heartbeat.send_heartbeat, and "
            "subsystem_sdk.validate.assert_producer_only."
        ) from exc


def _required_symbol(module: Any, name: str) -> Any:
    symbol = getattr(module, name, None)
    if symbol is None:
        raise AttributeError(f"{module.__name__}.{name} is required")
    return symbol


_SDK_API = _load_official_sdk_api()
SDK_AVAILABLE = _SDK_API is not None
SubsystemBaseInterface = (
    _SDK_API.subsystem_base_interface
    if _SDK_API is not None
    else StubSubsystemBaseInterface
)
SubmitResult = _SDK_API.submit_receipt if _SDK_API is not None else StubSubmitResult


class AnnouncementSubsystem(SubsystemBaseInterface):
    """Announcement runtime implementation of the SDK subsystem interface."""

    def __init__(
        self,
        config: AnnouncementConfig,
        *,
        allow_sdk_stub: bool = False,
    ) -> None:
        if not SDK_AVAILABLE and not _stub_explicitly_allowed(allow_sdk_stub):
            raise SubsystemSdkUnavailableError(
                "subsystem-sdk is required for announcement runtime startup. "
                f"Install the pinned dependency or set {TEST_STUB_ENV}=1 only "
                "inside tests that intentionally use the local SDK stub."
            )
        self.config = config
        self.run_id = str(uuid4())
        self.last_ex_id: str | None = None
        self.last_submit_failed = False
        self._submit_seq = 0

    def on_register(self) -> RegistrationSpec:
        """Return this subsystem's registration spec."""

        from .registration import build_registration_spec

        spec = build_registration_spec(self.config)
        if SDK_AVAILABLE:
            _require_sdk_api().register_subsystem(
                _to_sdk_registration_spec(spec, self.config),
            )
        return spec

    def on_heartbeat(self) -> HeartbeatPayload:
        """Return this subsystem's current heartbeat payload."""

        from .heartbeat import build_heartbeat

        payload = build_heartbeat(
            datetime.now(timezone.utc),
            self.run_id,
            interval_seconds=self.config.heartbeat_interval_seconds,
            last_ex_id=self.last_ex_id,
            status="degraded" if self.last_submit_failed else "ok",
        )
        if SDK_AVAILABLE:
            sdk_payload = _to_sdk_heartbeat_payload(payload)
            _validate_sdk_payload("Ex-0", sdk_payload)
            _ensure_accepted(
                _require_sdk_api().send_heartbeat(sdk_payload),
                "heartbeat",
            )
        return payload

    def submit(self, candidate: ExPayload) -> SubmitResult:
        """Submit a candidate through the SDK surface."""

        ex_type = _payload_ex_type(candidate)
        if SDK_AVAILABLE:
            sdk_payload = _to_sdk_submit_payload(candidate)
            _validate_sdk_payload(ex_type, sdk_payload)
            result = _coerce_submit_result(
                _require_sdk_api().submit(sdk_payload),
                ex_type,
            )
            if getattr(result, "accepted", False):
                self.last_ex_id = getattr(result, "receipt_id", None)
            return result

        self._submit_seq += 1
        receipt_id = f"{self.run_id}:{self._submit_seq}"
        self.last_ex_id = receipt_id
        return SubmitResult(
            accepted=True,
            receipt_id=receipt_id,
            ex_type=ex_type,
            warnings=(),
            errors=(),
        )


def _stub_explicitly_allowed(allow_sdk_stub: bool) -> bool:
    return allow_sdk_stub or os.environ.get(TEST_STUB_ENV) == "1"


def _require_sdk_api() -> _SdkApi:
    if _SDK_API is None:
        raise SubsystemSdkUnavailableError("subsystem-sdk API is unavailable")
    return _SDK_API


def _payload_ex_type(candidate: PayloadLike) -> str:
    if isinstance(candidate, Mapping):
        ex_type = candidate.get("ex_type")
    else:
        ex_type = getattr(candidate, "ex_type", None)
        if ex_type is None and hasattr(candidate, "model_dump"):
            ex_type = candidate.model_dump().get("ex_type")

    if not isinstance(ex_type, str) or not ex_type:
        raise ValueError("Ex payload must include a non-empty ex_type")
    return ex_type


def _payload_to_mapping(candidate: PayloadLike) -> dict[str, Any]:
    if isinstance(candidate, Mapping):
        return dict(candidate)
    if hasattr(candidate, "model_dump"):
        return candidate.model_dump()
    payload = {
        key: value
        for key, value in vars(candidate).items()
        if not key.startswith("_")
    }
    if not payload:
        raise TypeError("Ex payload must be mapping-like or expose model_dump()")
    return payload


def _to_sdk_registration_spec(
    spec: RegistrationSpec,
    config: AnnouncementConfig,
) -> Any:
    sdk_api = _require_sdk_api()
    return sdk_api.registration_spec(
        subsystem_id=spec.module_id,
        version=__version__,
        domain="announcement",
        supported_ex_types=spec.owned_ex_types,
        owner=PACKAGE_NAME,
        heartbeat_policy_ref=f"interval:{config.heartbeat_interval_seconds}s",
        capabilities={
            "parser_version": spec.parser_version,
            "registration_ttl_seconds": spec.registration_ttl_seconds,
            "sdk_endpoint": spec.sdk_endpoint,
        },
    )


def _to_sdk_heartbeat_payload(payload: HeartbeatPayload) -> dict[str, Any]:
    return {
        "subsystem_id": PACKAGE_NAME,
        "version": __version__,
        "heartbeat_at": payload.timestamp,
        "status": payload.status,
        "last_output_at": None,
        "pending_count": 0,
    }


def _to_sdk_submit_payload(candidate: PayloadLike) -> dict[str, Any]:
    payload = _payload_to_mapping(candidate)
    if payload.get("ex_type") != "Ex-0":
        return payload

    heartbeat_at = payload.get("emitted_at")
    if not isinstance(heartbeat_at, datetime):
        heartbeat_at = datetime.now(timezone.utc)
    return {
        "ex_type": "Ex-0",
        "subsystem_id": PACKAGE_NAME,
        "version": __version__,
        "heartbeat_at": heartbeat_at,
        "status": "ok",
        "last_output_at": heartbeat_at,
        "pending_count": 0,
    }


def _validate_sdk_payload(ex_type: str, payload: Mapping[str, Any]) -> None:
    _require_sdk_api().assert_producer_only(ex_type, payload)


def _ensure_accepted(raw_result: Any, operation: str) -> None:
    if raw_result is None:
        return
    accepted = (
        raw_result.get("accepted")
        if isinstance(raw_result, Mapping)
        else getattr(raw_result, "accepted", None)
    )
    if accepted is None:
        return
    if bool(accepted):
        return
    raise RuntimeError(_rejection_message(raw_result, operation))


def _coerce_submit_result(raw_result: Any, ex_type: str) -> SubmitResult:
    result_model = _submit_result_model()
    if isinstance(raw_result, result_model):
        return raw_result

    if isinstance(raw_result, Mapping):
        result_data = dict(raw_result)
    elif hasattr(raw_result, "model_dump"):
        result_data = raw_result.model_dump()
    else:
        result_data = {
            field: getattr(raw_result, field)
            for field in (
                "accepted",
                "receipt_id",
                "backend_kind",
                "transport_ref",
                "validator_version",
                "warnings",
                "errors",
            )
            if hasattr(raw_result, field)
        }

    result_data.setdefault("warnings", ())
    result_data.setdefault("errors", ())
    if not SDK_AVAILABLE:
        result_data.setdefault("ex_type", ex_type)
    return result_model(**result_data)


def _submit_result_model() -> type[Any]:
    if SDK_AVAILABLE and _SDK_API is not None:
        return _SDK_API.submit_receipt
    return StubSubmitResult


def _rejection_message(result: Any, operation: str) -> str:
    if isinstance(result, Mapping):
        errors = _string_sequence(result.get("errors", ()))
        warnings = _string_sequence(result.get("warnings", ()))
    else:
        errors = _string_sequence(getattr(result, "errors", ()))
        warnings = _string_sequence(getattr(result, "warnings", ()))
    detail = ", ".join(
        part
        for part in (
            f"errors={errors}" if errors else "",
            f"warnings={warnings}" if warnings else "",
        )
        if part
    )
    return f"subsystem-sdk {operation} rejected payload" + (
        f": {detail}" if detail else ""
    )


def _string_sequence(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (value,)
    if isinstance(value, Sequence):
        return tuple(str(item) for item in value)
    return (str(value),)
