"""Configuration loading for the announcement subsystem."""

from __future__ import annotations

import os
import re
import tomllib
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

DEFAULT_CONFIG_PATH = Path("config/announcement.toml")
_DOCLING_VERSION = re.compile(r"^docling==[A-Za-z0-9][A-Za-z0-9._!+-]*$")
_LLAMA_INDEX_VERSION = re.compile(
    r"^llama-index(?:-core)?==[A-Za-z0-9][A-Za-z0-9._!+-]*$"
)


class AnnouncementConfig(BaseModel):
    """Runtime configuration for the announcement subsystem scaffold."""

    model_config = ConfigDict(extra="forbid")

    artifact_root: Path = Path("artifacts/announcement")
    docling_version: str = "not-configured"
    llama_index_version: str = "not-configured"
    reasoner_endpoint: str | None = None
    entity_registry_endpoint: str | None = None
    sdk_endpoint: str | None = None
    heartbeat_interval_seconds: int = Field(60, gt=0, le=86_400)
    registration_ttl_seconds: int = Field(900, gt=0, le=604_800)

    @field_validator("docling_version")
    @classmethod
    def validate_docling_version(cls, value: str) -> str:
        """Allow only explicit Docling pins or the unset scaffold sentinel."""

        return _validate_version_field(
            value,
            pattern=_DOCLING_VERSION,
            field_name="docling_version",
        )

    @field_validator("llama_index_version")
    @classmethod
    def validate_llama_index_version(cls, value: str) -> str:
        """Allow only explicit LlamaIndex pins or the unset scaffold sentinel."""

        return _validate_version_field(
            value,
            pattern=_LLAMA_INDEX_VERSION,
            field_name="llama_index_version",
        )


def load_config(path: Path | None = None) -> AnnouncementConfig:
    """Load announcement subsystem configuration from TOML.

    Missing config files intentionally return defaults so a fresh checkout can
    run the scaffold commands before environment-specific config exists.
    """

    config_path = path
    if config_path is None:
        env_path = os.environ.get("ANNOUNCEMENT_CONFIG")
        config_path = Path(env_path) if env_path else DEFAULT_CONFIG_PATH

    if not config_path.exists():
        return AnnouncementConfig()

    try:
        config_text = config_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise FileNotFoundError(f"Unable to read config file: {config_path}") from exc

    raw_config: dict[str, Any] = tomllib.loads(config_text)
    return AnnouncementConfig.model_validate(raw_config)


def _validate_version_field(
    value: str,
    *,
    pattern: re.Pattern[str],
    field_name: str,
) -> str:
    stripped = value.strip()
    if stripped == "not-configured" or pattern.fullmatch(stripped):
        return stripped
    raise ValueError(f"{field_name} must be not-configured or an exact package pin")
