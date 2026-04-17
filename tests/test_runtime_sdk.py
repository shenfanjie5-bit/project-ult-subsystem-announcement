from __future__ import annotations

import asyncio
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from subsystem_announcement.config import AnnouncementConfig
from subsystem_announcement.runtime.ex0 import build_ex0_envelope
from subsystem_announcement.runtime.heartbeat import build_heartbeat
from subsystem_announcement.runtime.lifecycle import run
from subsystem_announcement.runtime.registration import build_registration_spec
from subsystem_announcement.runtime.sdk_adapter import (
    AnnouncementSubsystem,
    SDK_AVAILABLE,
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
    subsystem = AnnouncementSubsystem(AnnouncementConfig(heartbeat_interval_seconds=5))

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

    asyncio.run(run(AnnouncementConfig(heartbeat_interval_seconds=1), once=True))

    assert len(fake_sdk.submissions) == 1
    assert fake_sdk.submit_results == []


def test_concurrent_heartbeats_keep_same_run_id() -> None:
    subsystem = AnnouncementSubsystem(AnnouncementConfig())

    async def gather_heartbeats():
        return await asyncio.gather(
            *(asyncio.to_thread(subsystem.on_heartbeat) for _ in range(8))
        )

    heartbeats = asyncio.run(gather_heartbeats())

    assert {heartbeat.run_id for heartbeat in heartbeats} == {subsystem.run_id}


def test_sdk_missing_uses_local_stub() -> None:
    assert SDK_AVAILABLE is False


def test_cli_ping_returns_zero_in_offline_stub_mode() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "subsystem_announcement", "ping"],
        cwd=ROOT,
        env=_cli_env(),
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert result.stdout.strip() == "ok"
    assert "subsystem-sdk unavailable; using local SDK stub" in result.stderr


def test_cli_run_once_returns_zero() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "subsystem_announcement", "run", "--once"],
        cwd=ROOT,
        env=_cli_env(),
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert result.stdout.strip() == "ok"
