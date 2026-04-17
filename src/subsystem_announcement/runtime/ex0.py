"""Ex-0 placeholder payload builder."""

from __future__ import annotations

from datetime import datetime, timezone

from .sdk_adapter import Ex0Payload


def build_ex0_envelope(run_id: str, reason: str) -> Ex0Payload:
    """Build the stage-0 Ex-0 placeholder payload."""

    return Ex0Payload(
        run_id=run_id,
        reason=reason,
        emitted_at=datetime.now(timezone.utc),
    )
