from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest


@dataclass
class FakeSDKRecorder:
    registrations: list[Any] = field(default_factory=list)
    heartbeats: list[Any] = field(default_factory=list)
    submissions: list[dict[str, Any]] = field(default_factory=list)
    submit_results: list[Any] = field(default_factory=list)
    raise_on_submit: bool = False


@pytest.fixture
def fake_sdk(monkeypatch: pytest.MonkeyPatch) -> FakeSDKRecorder:
    from subsystem_announcement.runtime import lifecycle
    from subsystem_announcement.runtime.sdk_adapter import AnnouncementSubsystem

    recorder = FakeSDKRecorder()

    class RecordingAnnouncementSubsystem(AnnouncementSubsystem):
        def on_register(self):  # type: ignore[no-untyped-def]
            spec = super().on_register()
            recorder.registrations.append(spec)
            return spec

        def on_heartbeat(self):  # type: ignore[no-untyped-def]
            payload = super().on_heartbeat()
            recorder.heartbeats.append(payload)
            return payload

        def submit(self, candidate):  # type: ignore[no-untyped-def]
            payload = (
                candidate.model_dump()
                if hasattr(candidate, "model_dump")
                else dict(candidate)
            )
            _validate_ex0_payload(payload)
            recorder.submissions.append(payload)
            if recorder.raise_on_submit:
                raise RuntimeError("fake submit failure")
            result = super().submit(candidate)
            recorder.submit_results.append(result)
            return result

    monkeypatch.setattr(lifecycle, "AnnouncementSubsystem", RecordingAnnouncementSubsystem)
    return recorder


def _validate_ex0_payload(payload: dict[str, Any]) -> None:
    assert payload["ex_type"] == "Ex-0"
    assert payload["run_id"]
    assert payload["reason"]
    assert payload["emitted_at"]
    assert "submitted_at" not in payload
    assert "ingest_seq" not in payload
    assert "layer_b_receipt_id" not in payload
