"""SDK adapter for registration, heartbeat, and Ex payload submission."""

from __future__ import annotations

import importlib
import importlib.util
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from threading import RLock
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

_SDK_PACKAGE: Final[str] = "subsystem_sdk"

PayloadLike: TypeAlias = ExPayload | dict[str, Any]


class SubsystemSdkUnavailableError(RuntimeError):
    """Raised when runtime code starts without the required subsystem-sdk."""


class UnsupportedSubsystemSdkError(RuntimeError):
    """Raised when subsystem-sdk is installed but lacks the pinned API."""


@dataclass(frozen=True)
class _SdkApi:
    subsystem_base_interface: type[Any]
    base_subsystem_context: type[Any]
    registration_spec: type[Any]
    submit_receipt: type[Any]
    submit_client: type[Any]
    heartbeat_client: type[Any]
    mock_submit_backend: type[Any]
    submit_backend_heartbeat_adapter: type[Any]
    register_subsystem: Any
    configure_runtime: Any
    send_heartbeat: Any
    submit: Any
    build_ex0_payload: Any
    validation_result: type[Any]
    assert_producer_only: Any


def _load_official_sdk_api() -> _SdkApi | None:
    if importlib.util.find_spec(_SDK_PACKAGE) is None:
        return None

    try:
        base_module = importlib.import_module("subsystem_sdk.base")
        backends_module = importlib.import_module("subsystem_sdk.backends")
        submit_module = importlib.import_module("subsystem_sdk.submit")
        heartbeat_module = importlib.import_module("subsystem_sdk.heartbeat")
        validate_module = importlib.import_module("subsystem_sdk.validate")
        return _SdkApi(
            subsystem_base_interface=_required_symbol(
                base_module,
                "SubsystemBaseInterface",
            ),
            base_subsystem_context=_required_symbol(
                base_module,
                "BaseSubsystemContext",
            ),
            registration_spec=_required_symbol(
                base_module,
                "SubsystemRegistrationSpec",
            ),
            submit_receipt=_required_symbol(submit_module, "SubmitReceipt"),
            submit_client=_required_symbol(submit_module, "SubmitClient"),
            heartbeat_client=_required_symbol(heartbeat_module, "HeartbeatClient"),
            mock_submit_backend=_required_symbol(backends_module, "MockSubmitBackend"),
            submit_backend_heartbeat_adapter=_required_symbol(
                backends_module,
                "SubmitBackendHeartbeatAdapter",
            ),
            register_subsystem=_required_symbol(base_module, "register_subsystem"),
            configure_runtime=_required_symbol(base_module, "configure_runtime"),
            send_heartbeat=_required_symbol(heartbeat_module, "send_heartbeat"),
            submit=_required_symbol(submit_module, "submit"),
            build_ex0_payload=_required_symbol(heartbeat_module, "build_ex0_payload"),
            validation_result=_required_symbol(validate_module, "ValidationResult"),
            assert_producer_only=_required_symbol(
                validate_module,
                "assert_producer_only",
            ),
        )
    except Exception as exc:
        raise UnsupportedSubsystemSdkError(
            "Unsupported subsystem-sdk version: expected pinned public API "
            "subsystem_sdk.base.{SubsystemBaseInterface,"
            "BaseSubsystemContext,SubsystemRegistrationSpec,"
            "configure_runtime,register_subsystem}, "
            "subsystem_sdk.backends.{MockSubmitBackend,"
            "SubmitBackendHeartbeatAdapter}, "
            "subsystem_sdk.submit.{SubmitClient,SubmitReceipt,submit}, "
            "subsystem_sdk.heartbeat.{HeartbeatClient,build_ex0_payload,"
            "send_heartbeat}, and subsystem_sdk.validate."
            "{ValidationResult,assert_producer_only}."
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
                "Install the pinned dependency or inject the local SDK stub "
                "from test-only code."
            )
        self._use_sdk = SDK_AVAILABLE and not _stub_explicitly_allowed(allow_sdk_stub)
        self.config = config
        self.run_id = str(uuid4())
        self.last_ex_id: str | None = None
        self.last_submit_failed = False
        self._submit_seq = 0
        self._sdk_context: Any | None = None
        self._sdk_context_lock = RLock()
        self._sdk_registration_spec: Any | None = None

    def on_register(self) -> RegistrationSpec:
        """Return this subsystem's registration spec."""

        from .registration import build_registration_spec

        spec = build_registration_spec(self.config)
        if self._use_sdk:
            _require_sdk_api().register_subsystem(self._get_sdk_registration_spec())
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
        if self._use_sdk:
            sdk_status = _to_sdk_heartbeat_status(payload)
            with _require_sdk_api().configure_runtime(self._get_sdk_context()):
                _ensure_accepted(
                    _require_sdk_api().send_heartbeat(sdk_status),
                    "heartbeat",
                )
        return payload

    def submit(self, candidate: ExPayload) -> SubmitResult:
        """Submit a candidate through the SDK surface."""

        ex_type = _payload_ex_type(candidate)
        if self._use_sdk:
            sdk_payload = _to_sdk_submit_payload(candidate)
            _validate_sdk_payload(ex_type, sdk_payload)
            with _require_sdk_api().configure_runtime(self._get_sdk_context()):
                result = _coerce_submit_result(
                    _require_sdk_api().submit(sdk_payload),
                    ex_type,
                    use_sdk=True,
                )
            if getattr(result, "accepted", False):
                self.last_ex_id = getattr(result, "receipt_id", None)
            return result

        self._submit_seq += 1
        receipt_id = f"{self.run_id}:{self._submit_seq}"
        self.last_ex_id = receipt_id
        return StubSubmitResult(
            accepted=True,
            receipt_id=receipt_id,
            ex_type=ex_type,
            warnings=(),
            errors=(),
        )

    def _get_sdk_registration_spec(self) -> Any:
        if self._sdk_registration_spec is None:
            from .registration import build_registration_spec

            self._sdk_registration_spec = _to_sdk_registration_spec(
                build_registration_spec(self.config),
                self.config,
            )
        return self._sdk_registration_spec

    def _get_sdk_context(self) -> Any:
        if self._sdk_context is None:
            with self._sdk_context_lock:
                if self._sdk_context is None:
                    self._sdk_context = _build_sdk_runtime_context(
                        self._get_sdk_registration_spec()
                    )
        return self._sdk_context


def _stub_explicitly_allowed(allow_sdk_stub: bool) -> bool:
    return allow_sdk_stub


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
            "parser_core_version": config.docling_core_version,
            "registration_ttl_seconds": spec.registration_ttl_seconds,
            "sdk_endpoint": spec.sdk_endpoint,
        },
    )


def _to_sdk_heartbeat_status(payload: HeartbeatPayload) -> dict[str, Any]:
    return {
        "status": _sdk_heartbeat_state(payload.status),
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
    return _require_sdk_api().build_ex0_payload(
        PACKAGE_NAME,
        __version__,
        {
            "status": "healthy",
            "last_output_at": heartbeat_at,
            "pending_count": 0,
        },
        heartbeat_at=heartbeat_at,
    )


def _build_sdk_runtime_context(registration_spec: Any) -> Any:
    sdk_api = _require_sdk_api()
    submit_backend = sdk_api.mock_submit_backend()
    validator = _validate_sdk_payload_result
    return sdk_api.base_subsystem_context(
        registration=registration_spec,
        submit_client=sdk_api.submit_client(submit_backend, validator=validator),
        heartbeat_client=sdk_api.heartbeat_client(
            sdk_api.submit_backend_heartbeat_adapter(submit_backend),
            validator=validator,
        ),
        validator=validator,
    )


def _validate_sdk_payload_result(payload: Mapping[str, Any]) -> Any:
    sdk_api = _require_sdk_api()
    ex_type = payload.get("ex_type")
    if ex_type not in {"Ex-0", "Ex-1", "Ex-2", "Ex-3"}:
        ex_type = "Ex-0"
    try:
        sdk_api.assert_producer_only(payload)
    except Exception as exc:
        return sdk_api.validation_result.fail(
            ex_type=ex_type,
            schema_version="producer-semantics",
            field_errors=(str(exc),),
        )
    return sdk_api.validation_result.ok(
        ex_type=ex_type,
        schema_version="producer-semantics",
    )


def _sdk_heartbeat_state(status: str) -> str:
    if status == "ok":
        return "healthy"
    if status in {"healthy", "degraded", "unhealthy"}:
        return status
    return "degraded"


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


def _coerce_submit_result(raw_result: Any, ex_type: str, *, use_sdk: bool) -> Any:
    result_model = _submit_result_model(use_sdk=use_sdk)
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
    if not use_sdk:
        result_data.setdefault("ex_type", ex_type)
    return result_model(**result_data)


def _submit_result_model(*, use_sdk: bool) -> type[Any]:
    if use_sdk and _SDK_API is not None:
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
