"""SDK adapter for registration, heartbeat, and Ex payload submission."""

from __future__ import annotations

import importlib
import importlib.util
from collections.abc import Mapping, Sequence
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
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
    registration_spec: type[Any]
    submit_receipt: type[Any]
    register_subsystem: Any
    send_heartbeat: Any
    submit: Any
    assert_producer_only: Any
    base_subsystem_context: type[Any] | None = None
    configure_runtime: Any | None = None
    get_runtime: Any | None = None
    runtime_not_configured_error: type[Exception] | None = None
    submit_client: type[Any] | None = None
    heartbeat_client: type[Any] | None = None
    submit_backend_config: type[Any] | None = None
    build_submit_backend: Any | None = None
    load_submit_backend_config: Any | None = None
    submit_backend_heartbeat_adapter: type[Any] | None = None
    validation_result: type[Any] | None = None


def _load_official_sdk_api() -> _SdkApi | None:
    if importlib.util.find_spec(_SDK_PACKAGE) is None:
        return None

    try:
        base_module = importlib.import_module("subsystem_sdk.base")
        submit_module = importlib.import_module("subsystem_sdk.submit")
        heartbeat_module = importlib.import_module("subsystem_sdk.heartbeat")
        validate_module = importlib.import_module("subsystem_sdk.validate")
        backends_module = importlib.import_module("subsystem_sdk.backends")
        runtime_module = importlib.import_module("subsystem_sdk.base.runtime")
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
            base_subsystem_context=_required_symbol(
                base_module,
                "BaseSubsystemContext",
            ),
            configure_runtime=_required_symbol(base_module, "configure_runtime"),
            get_runtime=_required_symbol(runtime_module, "get_runtime"),
            runtime_not_configured_error=_required_symbol(
                base_module,
                "RuntimeNotConfiguredError",
            ),
            submit_client=_required_symbol(submit_module, "SubmitClient"),
            heartbeat_client=_required_symbol(heartbeat_module, "HeartbeatClient"),
            submit_backend_config=_required_symbol(
                backends_module,
                "SubmitBackendConfig",
            ),
            build_submit_backend=_required_symbol(
                backends_module,
                "build_submit_backend",
            ),
            load_submit_backend_config=_required_symbol(
                base_module,
                "load_submit_backend_config",
            ),
            submit_backend_heartbeat_adapter=_required_symbol(
                backends_module,
                "SubmitBackendHeartbeatAdapter",
            ),
            validation_result=_required_symbol(validate_module, "ValidationResult"),
        )
    except Exception as exc:
        raise UnsupportedSubsystemSdkError(
            "Unsupported subsystem-sdk version: expected pinned public API "
            "subsystem_sdk.base.{BaseSubsystemContext,SubsystemBaseInterface,"
            "SubsystemRegistrationSpec,RuntimeNotConfiguredError,"
            "configure_runtime,load_submit_backend_config,register_subsystem}, "
            "subsystem_sdk.base.runtime.get_runtime, "
            "subsystem_sdk.submit.{SubmitClient,SubmitReceipt,submit}, "
            "subsystem_sdk.heartbeat.{HeartbeatClient,send_heartbeat}, "
            "subsystem_sdk.backends.{SubmitBackendConfig,build_submit_backend,"
            "SubmitBackendHeartbeatAdapter}, and "
            "subsystem_sdk.validate.{ValidationResult,assert_producer_only}."
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
SubmitResult = StubSubmitResult


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
        self.config = config
        self._use_sdk = SDK_AVAILABLE and not allow_sdk_stub
        self._sdk_registration: Any | None = None
        self._sdk_context: Any | None = None
        self.run_id = str(uuid4())
        self.last_ex_id: str | None = None
        self.last_submit_failed = False
        self._submit_seq = 0

    def on_register(self) -> RegistrationSpec:
        """Return this subsystem's registration spec."""

        from .registration import build_registration_spec

        spec = build_registration_spec(self.config)
        if self._use_sdk:
            self._sdk_registration = _to_sdk_registration_spec(spec, self.config)
            _require_sdk_api().register_subsystem(self._sdk_registration)
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
            with self._sdk_runtime_scope():
                _ensure_accepted(
                    _require_sdk_api().send_heartbeat(
                        _to_sdk_heartbeat_status(payload)
                    ),
                    "heartbeat",
                )
        return payload

    def submit(self, candidate: ExPayload) -> SubmitResult:
        """Submit a candidate through the SDK surface."""

        ex_type = _payload_ex_type(candidate)
        if self._use_sdk:
            sdk_payload = _to_sdk_submit_payload(candidate)
            _validate_sdk_payload(ex_type, sdk_payload)
            with self._sdk_runtime_scope():
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
        return StubSubmitResult(
            accepted=True,
            receipt_id=receipt_id,
            ex_type=ex_type,
            warnings=(),
            errors=(),
        )

    def _sdk_runtime_scope(self) -> Any:
        api = _require_sdk_api()
        if _get_configured_sdk_runtime(api) is not None:
            return nullcontext()
        context = self._ensure_sdk_context()
        if context is None or api.configure_runtime is None:
            return nullcontext()
        return api.configure_runtime(context)

    def _ensure_sdk_context(self) -> Any | None:
        if self._sdk_context is not None:
            return self._sdk_context

        api = _require_sdk_api()
        required = (
            api.base_subsystem_context,
            api.submit_client,
            api.heartbeat_client,
            api.submit_backend_config,
            api.build_submit_backend,
            api.submit_backend_heartbeat_adapter,
            api.validation_result,
        )
        if any(symbol is None for symbol in required):
            return None

        if self._sdk_registration is None:
            from .registration import build_registration_spec

            self._sdk_registration = _to_sdk_registration_spec(
                build_registration_spec(self.config),
                self.config,
            )

        submit_backend = _build_sdk_submit_backend(api, self.config)
        submit_client = api.submit_client(
            submit_backend,
            validator=_sdk_validate_payload,
        )
        heartbeat_client = api.heartbeat_client(
            api.submit_backend_heartbeat_adapter(submit_backend),
            validator=_sdk_validate_payload,
        )
        self._sdk_context = api.base_subsystem_context(
            registration=self._sdk_registration,
            submit_client=submit_client,
            heartbeat_client=heartbeat_client,
            validator=_sdk_validate_payload,
        )
        return self._sdk_context


_BACKEND_CONFIG_SUFFIXES: Final[frozenset[str]] = frozenset(
    {".json", ".toml", ".yaml", ".yml"}
)


def _get_configured_sdk_runtime(api: _SdkApi) -> Any | None:
    if api.get_runtime is None:
        return None
    try:
        return api.get_runtime()
    except Exception as exc:
        runtime_error = api.runtime_not_configured_error
        if runtime_error is not None and isinstance(exc, runtime_error):
            return None
        raise


def _build_sdk_submit_backend(api: _SdkApi, config: AnnouncementConfig) -> Any:
    backend_config = _load_sdk_submit_backend_config(api, config)
    _reject_mock_sdk_backend(backend_config, "configured SDK submit backend")

    backend = api.build_submit_backend(backend_config)
    _reject_mock_sdk_backend(backend, "constructed SDK submit backend")
    return backend


def _load_sdk_submit_backend_config(
    api: _SdkApi,
    config: AnnouncementConfig,
) -> Any:
    endpoint = config.sdk_endpoint
    if endpoint is None or not endpoint.strip():
        raise SubsystemSdkUnavailableError(
            "subsystem-sdk runtime is not configured and sdk_endpoint is unset; "
            "wrap runtime calls in subsystem_sdk.base.configure_runtime(...) or "
            "set AnnouncementConfig.sdk_endpoint to a non-mock SDK backend "
            "configuration path or PostgreSQL DSN."
        )

    endpoint_text = endpoint.strip()
    if _looks_like_backend_config_path(endpoint_text):
        if api.load_submit_backend_config is None:
            raise UnsupportedSubsystemSdkError(
                "subsystem-sdk load_submit_backend_config is required for "
                "sdk_endpoint backend config paths"
            )
        return api.load_submit_backend_config(Path(endpoint_text))

    config_model = api.submit_backend_config
    if config_model is None:
        raise UnsupportedSubsystemSdkError(
            "subsystem-sdk SubmitBackendConfig is required for sdk_endpoint DSNs"
        )
    return config_model(backend_kind="lite_pg", dsn=endpoint_text)


def _looks_like_backend_config_path(endpoint: str) -> bool:
    if "://" in endpoint:
        return False
    return Path(endpoint).suffix.lower() in _BACKEND_CONFIG_SUFFIXES


def _reject_mock_sdk_backend(value: Any, label: str) -> None:
    if _backend_kind(value) != "mock":
        return
    raise UnsupportedSubsystemSdkError(
        f"{label} must not use backend_kind='mock' in production runtime"
    )


def _backend_kind(value: Any) -> Any:
    if isinstance(value, Mapping):
        return value.get("backend_kind")
    return getattr(value, "backend_kind", None)


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
        heartbeat_policy_ref="default",
        capabilities={
            "parser_version": spec.parser_version,
            "registration_ttl_seconds": spec.registration_ttl_seconds,
            "sdk_endpoint": spec.sdk_endpoint,
        },
    )


def _to_sdk_heartbeat_status(payload: HeartbeatPayload) -> dict[str, Any]:
    return {
        "status": _to_sdk_heartbeat_state(payload.status),
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
        "semantic": "metadata_or_heartbeat",
        "subsystem_id": PACKAGE_NAME,
        "version": __version__,
        "heartbeat_at": heartbeat_at,
        "status": "healthy",
        "last_output_at": heartbeat_at,
        "pending_count": 0,
    }


def _validate_sdk_payload(ex_type: str, payload: Mapping[str, Any]) -> None:
    _require_sdk_api().assert_producer_only(ex_type, payload)


def _sdk_validate_payload(payload: Mapping[str, Any]) -> Any:
    api = _require_sdk_api()
    result_model = api.validation_result
    if result_model is None:
        raise UnsupportedSubsystemSdkError(
            "subsystem-sdk ValidationResult is required for runtime context wiring"
        )

    ex_type = _payload_ex_type(payload)
    try:
        _validate_sdk_payload(ex_type, payload)
    except Exception as exc:
        return result_model.fail(
            ex_type=ex_type,
            schema_version="announcement-producer-only",
            field_errors=(str(exc),),
        )
    return result_model.ok(
        ex_type=ex_type,
        schema_version="announcement-producer-only",
    )


def _to_sdk_heartbeat_state(status: str) -> str:
    if status == "ok":
        return "healthy"
    if status == "degraded":
        return "degraded"
    return "unhealthy"


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
