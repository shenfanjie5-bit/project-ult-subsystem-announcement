"""Heartbeat payload builders."""

from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from .sdk_adapter import HeartbeatPayload


def build_heartbeat(
    now: datetime,
    last_run_id: str | None,
    *,
    interval_seconds: int,
    last_ex_id: str | None = None,
    status: str = "ok",
) -> HeartbeatPayload:
    """Build one heartbeat payload.

    ``last_run_id`` is retained for the interface named in the project plan.
    The lifecycle passes the active run id so repeated heartbeats are stable.
    """

    timestamp = now if now.tzinfo is not None else now.replace(tzinfo=timezone.utc)
    return HeartbeatPayload(
        run_id=last_run_id or str(uuid4()),
        timestamp=timestamp,
        last_ex_id=last_ex_id,
        status=status,
        interval_seconds=interval_seconds,
    )
