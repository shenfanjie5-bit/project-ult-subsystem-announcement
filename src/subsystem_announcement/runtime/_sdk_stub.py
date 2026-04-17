"""Protocol-compatible SDK placeholders used when subsystem-sdk is absent."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class RegistrationSpec(BaseModel):
    """Minimal registration metadata consumed by the announcement runtime."""

    model_config = ConfigDict(extra="forbid")

    module_id: str
    owned_ex_types: tuple[str, ...]
    parser_version: str
    registration_ttl_seconds: int
    sdk_endpoint: str | None = None


class HeartbeatPayload(BaseModel):
    """Minimal Ex-0 heartbeat shape for offline SDK-compatible execution."""

    model_config = ConfigDict(extra="forbid")

    run_id: str
    timestamp: datetime
    last_ex_id: str | None = None
    status: str
    interval_seconds: int


class ExPayload(BaseModel):
    """Base payload placeholder; concrete Ex payloads remain contract-owned."""

    model_config = ConfigDict(extra="forbid")

    ex_type: str


class Ex0Payload(ExPayload):
    """Stage-0 metadata heartbeat payload placeholder."""

    ex_type: Literal["Ex-0"] = "Ex-0"
    run_id: str
    reason: str
    emitted_at: datetime


class SubmitResult(BaseModel):
    """Normalized submit result used by the offline shim."""

    model_config = ConfigDict(extra="forbid")

    accepted: bool
    receipt_id: str
    ex_type: str
    warnings: tuple[str, ...] = Field(default_factory=tuple)
    errors: tuple[str, ...] = Field(default_factory=tuple)


class SubsystemBaseInterface(ABC):
    """Small abstract surface matching the SDK hooks used by this subsystem."""

    @abstractmethod
    def on_register(self) -> RegistrationSpec:
        """Return registration metadata for this subsystem."""

    @abstractmethod
    def on_heartbeat(self) -> HeartbeatPayload:
        """Return the current heartbeat payload."""

    @abstractmethod
    def submit(self, candidate: ExPayload) -> SubmitResult:
        """Submit one Ex payload through the SDK surface."""
