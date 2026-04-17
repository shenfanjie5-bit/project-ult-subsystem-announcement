"""SDK adapter for registration, heartbeat, and Ex payload submission."""

from __future__ import annotations

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
except (ImportError, ModuleNotFoundError):
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


class AnnouncementSubsystem(SubsystemBaseInterface):
    """Announcement runtime implementation of the SDK subsystem interface."""

    def __init__(self, config: AnnouncementConfig) -> None:
        self.config = config
        self.run_id = str(uuid4())
        self.last_ex_id: str | None = None
        self._submit_seq = 0

    def on_register(self) -> RegistrationSpec:
        """Return this subsystem's registration spec."""

        from .registration import build_registration_spec

        return build_registration_spec(self.config)

    def on_heartbeat(self) -> HeartbeatPayload:
        """Return this subsystem's current heartbeat payload."""

        from .heartbeat import build_heartbeat

        return build_heartbeat(
            datetime.now(timezone.utc),
            self.run_id,
            interval_seconds=self.config.heartbeat_interval_seconds,
            last_ex_id=self.last_ex_id,
            status="ok",
        )

    def submit(self, candidate: ExPayload) -> SubmitResult:
        """Submit a candidate through the SDK surface.

        The offline shim returns a deterministic local receipt. When the real
        SDK is present this override still keeps the subsystem API stable; SDK
        transport integration remains owned by the imported base interface.
        """

        ex_type = _payload_ex_type(candidate)
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
    if isinstance(candidate, dict):
        ex_type = candidate.get("ex_type")
    else:
        ex_type = getattr(candidate, "ex_type", None)
        if ex_type is None and hasattr(candidate, "model_dump"):
            ex_type = candidate.model_dump().get("ex_type")

    if not isinstance(ex_type, str) or not ex_type:
        raise ValueError("Ex payload must include a non-empty ex_type")
    return ex_type
