"""Configuration loading for the announcement subsystem."""

from __future__ import annotations

import os
import tomllib
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict

DEFAULT_CONFIG_PATH = Path("config/announcement.toml")


class AnnouncementConfig(BaseModel):
    """Runtime configuration for the announcement subsystem scaffold."""

    model_config = ConfigDict(extra="forbid")

    artifact_root: Path = Path("artifacts/announcement")
    docling_version: str = "not-configured"
    llama_index_version: str = "not-configured"
    reasoner_endpoint: str | None = None
    entity_registry_endpoint: str | None = None
    heartbeat_interval_seconds: int = 60


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
