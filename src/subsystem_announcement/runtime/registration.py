"""Registration metadata builders for the announcement subsystem."""

from __future__ import annotations

from subsystem_announcement.config import AnnouncementConfig

from .sdk_adapter import RegistrationSpec

MODULE_ID = "subsystem-announcement"
OWNED_EX_TYPES = ("Ex-0", "Ex-1", "Ex-2", "Ex-3")


def build_registration_spec(config: AnnouncementConfig) -> RegistrationSpec:
    """Build the SDK registration spec for the announcement subsystem."""

    return RegistrationSpec(
        module_id=MODULE_ID,
        owned_ex_types=OWNED_EX_TYPES,
        parser_version=config.docling_version,
        registration_ttl_seconds=config.registration_ttl_seconds,
        sdk_endpoint=config.sdk_endpoint,
    )
