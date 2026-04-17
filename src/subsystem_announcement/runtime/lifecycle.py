"""Runtime lifecycle for the stage-0 announcement subsystem."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from subsystem_announcement.config import AnnouncementConfig

from .ex0 import build_ex0_envelope
from .sdk_adapter import AnnouncementSubsystem, SDK_AVAILABLE

try:
    import structlog
except ModuleNotFoundError:
    structlog = None

_STD_LOGGER = logging.getLogger(__name__)
_STRUCT_LOGGER = structlog.get_logger(__name__) if structlog is not None else None


async def run(
    config: AnnouncementConfig,
    *,
    stop_event: asyncio.Event | None = None,
    once: bool = False,
) -> None:
    """Run registration, heartbeat, and Ex-0 submission until stopped."""

    subsystem = AnnouncementSubsystem(config)
    subsystem.on_register()
    subsystem.on_heartbeat()
    if not SDK_AVAILABLE:
        _log_info("subsystem_sdk_degraded", sdk_available=False)

    await _submit_ex0_once(subsystem)
    if once:
        return

    active_stop_event = stop_event or asyncio.Event()
    while not active_stop_event.is_set():
        try:
            await asyncio.wait_for(
                active_stop_event.wait(),
                timeout=config.heartbeat_interval_seconds,
            )
        except TimeoutError:
            try:
                subsystem.on_heartbeat()
            except Exception as exc:
                _log_error("heartbeat_failed", exc, run_id=subsystem.run_id)
                continue
            await _submit_ex0_once(subsystem)


async def ping(config: AnnouncementConfig) -> None:
    """Run a one-shot offline health check."""

    await run(config, once=True)


async def _submit_ex0_once(subsystem: AnnouncementSubsystem) -> None:
    payload = build_ex0_envelope(subsystem.run_id, reason="stage0-placeholder")
    try:
        subsystem.submit(payload)
    except Exception as exc:
        _log_error("ex0_submit_failed", exc, run_id=subsystem.run_id)


def _log_error(event: str, exc: Exception, **fields: Any) -> None:
    payload = {"error": str(exc), **fields}
    if _STRUCT_LOGGER is not None:
        _STRUCT_LOGGER.error(event, **payload)
        return
    _STD_LOGGER.error("%s %s", event, payload)


def _log_info(event: str, **fields: Any) -> None:
    if _STRUCT_LOGGER is not None:
        _STRUCT_LOGGER.info(event, **fields)
        return
    _STD_LOGGER.info("%s %s", event, fields)
