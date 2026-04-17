"""SDK adapter for registration, heartbeat, and Ex payload submission."""

from __future__ import annotations

import importlib
from collections.abc import Callable, Mapping
from datetime import datetime, timezone
from typing import Any, TypeAlias
from uuid import uuid4

from subsystem_announcement.config import AnnouncementConfig

try:
    from subsystem_sdk import (  # type: ignore[import-not-found]
        Ex0Payload,
        ExPayload,
        HeartbeatPayload,
        RegistrationSpec,
        SubmitResult,
        SubsystemBaseInterface,
    )
except ModuleNotFoundError as exc:
    if exc.name != "subsystem_sdk":
        raise

    from ._sdk_stub import (
        Ex0Payload,
        ExPayload,
        HeartbeatPayload,
        RegistrationSpec,
        SubmitResult,
        SubsystemBaseInterface,
    )

    SDK_AVAILABLE = False
else:
    SDK_AVAILABLE = True

PayloadLike: TypeAlias = ExPayload | dict[str, Any]
SdkHook: TypeAlias = Callable[[Any], Any]


class AnnouncementSubsystem(SubsystemBaseInterface):
    """Announcement runtime implementation of the SDK subsystem interface."""

    def __init__(self, config: AnnouncementConfig) -> None:
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
            _call_sdk_hook(
                ("subsystem_sdk", "subsystem_sdk.base.registration"),
                ("register_subsystem",),
                spec,
                "registration",
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
            _call_sdk_hook(
                (
                    "subsystem_sdk.heartbeat",
                    "subsystem_sdk.heartbeat.client",
                    "subsystem_sdk",
                ),
                ("send_heartbeat", "heartbeat"),
                _payload_to_mapping(payload),
                "heartbeat",
            )
        return payload

    def submit(self, candidate: ExPayload) -> SubmitResult:
        """Submit a candidate through the SDK surface."""

        ex_type = _payload_ex_type(candidate)
        if SDK_AVAILABLE:
            result = _coerce_submit_result(
                _call_sdk_hook(
                    (
                        "subsystem_sdk.submit",
                        "subsystem_sdk.submit.client",
                        "subsystem_sdk",
                    ),
                    ("submit",),
                    _payload_to_mapping(candidate),
                    "submit",
                ),
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


def _call_sdk_hook(
    module_names: tuple[str, ...],
    hook_names: tuple[str, ...],
    payload: Any,
    hook_kind: str,
) -> Any:
    hook = _load_sdk_hook(module_names, hook_names)
    if hook is None:
        raise RuntimeError(f"subsystem-sdk {hook_kind} transport is unavailable")
    return hook(payload)


def _load_sdk_hook(
    module_names: tuple[str, ...],
    hook_names: tuple[str, ...],
) -> SdkHook | None:
    for module_name in module_names:
        try:
            module = importlib.import_module(module_name)
        except ModuleNotFoundError as exc:
            if exc.name is not None and (
                exc.name == module_name or module_name.startswith(f"{exc.name}.")
            ):
                continue
            raise

        for hook_name in hook_names:
            hook = getattr(module, hook_name, None)
            if callable(hook):
                return hook
    return None


def _coerce_submit_result(raw_result: Any, ex_type: str) -> SubmitResult:
    if isinstance(raw_result, SubmitResult):
        return raw_result

    if isinstance(raw_result, Mapping):
        result_data = dict(raw_result)
    elif hasattr(raw_result, "model_dump"):
        result_data = raw_result.model_dump()
    else:
        result_data = {
            field: getattr(raw_result, field)
            for field in ("accepted", "receipt_id", "warnings", "errors")
            if hasattr(raw_result, field)
        }

    result_data.setdefault("ex_type", ex_type)
    result_data.setdefault("warnings", ())
    result_data.setdefault("errors", ())
    return SubmitResult(**result_data)
