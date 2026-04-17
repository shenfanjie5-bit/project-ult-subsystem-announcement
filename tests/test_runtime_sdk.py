from __future__ import annotations

import asyncio
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest
from pydantic import ValidationError

from subsystem_announcement.config import AnnouncementConfig, load_config
from subsystem_announcement.runtime.ex0 import build_ex0_envelope
from subsystem_announcement.runtime.heartbeat import build_heartbeat
from subsystem_announcement.runtime.lifecycle import run
from subsystem_announcement.runtime import sdk_adapter
from subsystem_announcement.runtime.registration import build_registration_spec
from subsystem_announcement.runtime.sdk_adapter import (
    AnnouncementSubsystem,
    SDK_AVAILABLE,
    SubsystemSdkUnavailableError,
)

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"


def _cli_env() -> dict[str, str]:
    env = os.environ.copy()
    existing_pythonpath = env.get("PYTHONPATH")
    env["PYTHONPATH"] = (
        str(SRC)
        if not existing_pythonpath
        else os.pathsep.join([str(SRC), existing_pythonpath])
    )
    return env


def test_registration_spec_contains_module_ex_types_and_parser_version() -> None:
    config = AnnouncementConfig(
        docling_version="docling==1.2.3",
        sdk_endpoint="http://sdk.local",
        registration_ttl_seconds=321,
    )

    spec = build_registration_spec(config)

    assert spec.module_id == "subsystem-announcement"
    assert spec.owned_ex_types == ("Ex-0", "Ex-1", "Ex-2", "Ex-3")
    assert spec.parser_version == "docling==1.2.3"
    assert spec.registration_ttl_seconds == 321
    assert spec.sdk_endpoint == "http://sdk.local"


def test_heartbeat_payload_contains_required_fields_and_interval() -> None:
    now = datetime(2026, 4, 18, 1, 2, 3, tzinfo=timezone.utc)

    heartbeat = build_heartbeat(
        now,
        "run-1",
        interval_seconds=7,
        last_ex_id="ex-1",
    )

    assert heartbeat.run_id == "run-1"
    assert heartbeat.timestamp == now
    assert heartbeat.last_ex_id == "ex-1"
    assert heartbeat.status == "ok"
    assert heartbeat.interval_seconds == 7


def test_ex0_payload_has_stage0_placeholder_fields() -> None:
    payload = build_ex0_envelope("run-2", "stage0")

    assert payload.ex_type == "Ex-0"
    assert payload.run_id == "run-2"
    assert payload.reason == "stage0"
    assert payload.emitted_at.tzinfo is not None


def test_announcement_subsystem_overrides_sdk_methods() -> None:
    subsystem = AnnouncementSubsystem(
        AnnouncementConfig(heartbeat_interval_seconds=5),
        allow_sdk_stub=True,
    )

    registration = subsystem.on_register()
    heartbeat = subsystem.on_heartbeat()
    result = subsystem.submit(build_ex0_envelope(subsystem.run_id, "test"))

    assert registration.module_id == "subsystem-announcement"
    assert heartbeat.run_id == subsystem.run_id
    assert heartbeat.interval_seconds == 5
    assert result.accepted is True
    assert result.ex_type == "Ex-0"
    assert subsystem.last_ex_id == result.receipt_id


def test_lifecycle_ping_path_registers_heartbeats_and_submits_ex0(fake_sdk) -> None:
    asyncio.run(run(AnnouncementConfig(heartbeat_interval_seconds=1), once=True))

    assert len(fake_sdk.registrations) == 1
    assert len(fake_sdk.heartbeats) == 1
    assert len(fake_sdk.submissions) == 1
    assert fake_sdk.submissions[0]["ex_type"] == "Ex-0"


def test_run_exits_cleanly_when_stop_event_is_set(fake_sdk) -> None:
    async def scenario() -> None:
        stop_event = asyncio.Event()

        async def stop_soon() -> None:
            await asyncio.sleep(0.05)
            stop_event.set()

        await asyncio.gather(
            asyncio.wait_for(
                run(
                    AnnouncementConfig(heartbeat_interval_seconds=60),
                    stop_event=stop_event,
                ),
                timeout=2,
            ),
            stop_soon(),
        )

    asyncio.run(scenario())

    assert fake_sdk.submissions


def test_submit_exception_is_logged_and_does_not_crash_run(fake_sdk) -> None:
    fake_sdk.raise_on_submit = True

    async def scenario() -> None:
        stop_event = asyncio.Event()

        async def stop_soon() -> None:
            await asyncio.sleep(0.05)
            stop_event.set()

        await asyncio.gather(
            run(AnnouncementConfig(heartbeat_interval_seconds=1), stop_event=stop_event),
            stop_soon(),
        )

    asyncio.run(scenario())

    assert len(fake_sdk.heartbeats) >= 2
    assert fake_sdk.heartbeats[-1].status == "degraded"
    assert len(fake_sdk.submissions) == 1
    assert fake_sdk.submit_results == []


def test_once_submit_exception_raises_for_health_check(fake_sdk) -> None:
    fake_sdk.raise_on_submit = True

    with pytest.raises(RuntimeError, match="fake submit failure"):
        asyncio.run(run(AnnouncementConfig(heartbeat_interval_seconds=1), once=True))

    assert len(fake_sdk.submissions) == 1
    assert fake_sdk.submit_results == []


def test_once_rejected_submit_raises_for_health_check(fake_sdk) -> None:
    fake_sdk.reject_submit = True

    with pytest.raises(RuntimeError, match="submit rejected Ex-0 payload"):
        asyncio.run(run(AnnouncementConfig(heartbeat_interval_seconds=1), once=True))

    assert len(fake_sdk.submissions) == 1
    assert fake_sdk.submit_results[0].accepted is False


def test_concurrent_heartbeats_keep_same_run_id() -> None:
    subsystem = AnnouncementSubsystem(AnnouncementConfig(), allow_sdk_stub=True)

    async def gather_heartbeats():
        return await asyncio.gather(
            *(asyncio.to_thread(subsystem.on_heartbeat) for _ in range(8))
        )

    heartbeats = asyncio.run(gather_heartbeats())

    assert {heartbeat.run_id for heartbeat in heartbeats} == {subsystem.run_id}


def test_sdk_missing_fails_without_explicit_test_stub() -> None:
    assert SDK_AVAILABLE is False
    with pytest.raises(SubsystemSdkUnavailableError, match="subsystem-sdk is required"):
        AnnouncementSubsystem(AnnouncementConfig())


def test_broken_sdk_import_is_not_downgraded_to_stub(tmp_path: Path) -> None:
    sdk_dir = tmp_path / "subsystem_sdk"
    sdk_dir.mkdir()
    (sdk_dir / "__init__.py").write_text(
        "import definitely_missing_subsystem_dep\n",
        encoding="utf-8",
    )
    env = _cli_env()
    env["PYTHONPATH"] = os.pathsep.join([str(tmp_path), env["PYTHONPATH"]])

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import subsystem_announcement.runtime.sdk_adapter",
        ],
        cwd=ROOT,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert "definitely_missing_subsystem_dep" in result.stderr


def test_real_sdk_mode_delegates_official_registration_heartbeat_and_submit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, object]] = []

    class FakeBase:
        pass

    class FakeRegistrationSpec:
        def __init__(self, **kwargs):  # type: ignore[no-untyped-def]
            self.kwargs = kwargs

    class FakeSubmitReceipt:
        def __init__(self, **kwargs):  # type: ignore[no-untyped-def]
            self.accepted = kwargs["accepted"]
            self.receipt_id = kwargs["receipt_id"]
            self.backend_kind = kwargs["backend_kind"]
            self.transport_ref = kwargs["transport_ref"]
            self.validator_version = kwargs["validator_version"]
            self.warnings = tuple(kwargs.get("warnings", ()))
            self.errors = tuple(kwargs.get("errors", ()))

    def register_subsystem(spec):  # type: ignore[no-untyped-def]
        calls.append(("register", spec.kwargs))

    def send_heartbeat(payload):  # type: ignore[no-untyped-def]
        calls.append(("heartbeat", payload))
        return FakeSubmitReceipt(
            accepted=True,
            receipt_id="heartbeat-receipt",
            backend_kind="mock",
            transport_ref=None,
            validator_version="validator-v1",
        )

    def submit(payload):  # type: ignore[no-untyped-def]
        calls.append(("submit", payload))
        return FakeSubmitReceipt(
            accepted=True,
            receipt_id="sdk-receipt",
            backend_kind="mock",
            transport_ref=None,
            validator_version="validator-v1",
        )

    def assert_producer_only(ex_type, payload):  # type: ignore[no-untyped-def]
        calls.append(("validate", (ex_type, payload)))
        assert "submitted_at" not in payload
        assert "ingest_seq" not in payload
        assert "layer_b_receipt_id" not in payload

    fake_api = sdk_adapter._SdkApi(
        subsystem_base_interface=FakeBase,
        registration_spec=FakeRegistrationSpec,
        submit_receipt=FakeSubmitReceipt,
        register_subsystem=register_subsystem,
        send_heartbeat=send_heartbeat,
        submit=submit,
        assert_producer_only=assert_producer_only,
    )
    monkeypatch.setattr(sdk_adapter, "SDK_AVAILABLE", True)
    monkeypatch.setattr(sdk_adapter, "_SDK_API", fake_api)
    subsystem = AnnouncementSubsystem(AnnouncementConfig())

    registration = subsystem.on_register()
    heartbeat = subsystem.on_heartbeat()
    result = subsystem.submit(build_ex0_envelope(subsystem.run_id, "test"))

    assert result.accepted is True
    assert result.receipt_id == "sdk-receipt"
    assert subsystem.last_ex_id == "sdk-receipt"
    assert registration.module_id == "subsystem-announcement"
    assert heartbeat.run_id == subsystem.run_id
    assert calls[0] == (
        "register",
        {
            "subsystem_id": "subsystem-announcement",
            "version": "0.1.0",
            "domain": "announcement",
            "supported_ex_types": ("Ex-0", "Ex-1", "Ex-2", "Ex-3"),
            "owner": "subsystem-announcement",
            "heartbeat_policy_ref": "interval:60s",
            "capabilities": {
                "parser_version": "not-configured",
                "registration_ttl_seconds": 900,
                "sdk_endpoint": None,
            },
        },
    )
    assert (
        "heartbeat",
        {
            "subsystem_id": "subsystem-announcement",
            "version": "0.1.0",
            "heartbeat_at": heartbeat.timestamp,
            "status": "ok",
            "last_output_at": None,
            "pending_count": 0,
        },
    ) in calls
    submit_calls = [call for call in calls if call[0] == "submit"]
    assert len(submit_calls) == 1
    submit_payload = submit_calls[0][1]
    assert submit_payload["ex_type"] == "Ex-0"
    assert submit_payload["subsystem_id"] == "subsystem-announcement"
    assert submit_payload["version"] == "0.1.0"
    assert submit_payload["status"] == "ok"
    assert submit_payload["pending_count"] == 0
    assert "run_id" not in submit_payload
    assert "reason" not in submit_payload


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("heartbeat_interval_seconds", 0),
        ("heartbeat_interval_seconds", -1),
        ("registration_ttl_seconds", 0),
        ("registration_ttl_seconds", -1),
    ],
)
def test_load_config_rejects_non_positive_runtime_intervals(
    tmp_path: Path,
    field: str,
    value: int,
) -> None:
    config_path = tmp_path / "announcement.toml"
    config_path.write_text(f"{field} = {value}\n", encoding="utf-8")

    with pytest.raises(ValidationError):
        load_config(config_path)


def test_cli_ping_fails_when_sdk_is_unavailable() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "subsystem_announcement", "ping"],
        cwd=ROOT,
        env=_cli_env(),
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    assert result.stdout.strip() == ""
    assert "subsystem-sdk is required" in result.stderr


def test_cli_run_once_fails_when_sdk_is_unavailable() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "subsystem_announcement", "run", "--once"],
        cwd=ROOT,
        env=_cli_env(),
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    assert result.stdout.strip() == ""
    assert "subsystem-sdk is required" in result.stderr
