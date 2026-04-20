from __future__ import annotations

import importlib
import os
import subprocess
import sys
from pathlib import Path

import pytest
from pydantic import ValidationError

from subsystem_announcement import PACKAGE_NAME, __version__
from subsystem_announcement.config import AnnouncementConfig, load_config
from subsystem_announcement.logging_setup import configure_logging

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
SUBPACKAGES = [
    "discovery",
    "parse",
    "extract",
    "signals",
    "graph",
    "index",
    "runtime",
]


def _cli_env() -> dict[str, str]:
    env = os.environ.copy()
    existing_pythonpath = env.get("PYTHONPATH")
    env["PYTHONPATH"] = (
        str(SRC)
        if not existing_pythonpath
        else os.pathsep.join([str(SRC), existing_pythonpath])
    )
    return env


def test_package_exports_version_and_name() -> None:
    # Bumped 0.1.0 -> 0.1.1 in stage 2.8 (milestone-test-baseline) when
    # public.py + 5 canonical tier dirs + 5-lane CI (3-step Plan A
    # install) + iron rules 1-7 enforced landed.
    assert __version__ == "0.1.1"
    assert PACKAGE_NAME == "subsystem-announcement"


@pytest.mark.parametrize("subpackage", SUBPACKAGES)
def test_subpackages_are_importable(subpackage: str) -> None:
    module = importlib.import_module(f"subsystem_announcement.{subpackage}")
    if subpackage == "discovery":
        assert set(module.__all__) == {
            "AnnouncementDiscoveryResult",
            "AnnouncementDocumentArtifact",
            "AnnouncementEnvelope",
            "consume_announcement_ref",
        }
    elif subpackage == "parse":
        assert {
            "AnnouncementSection",
            "AnnouncementTable",
            "ParsedAnnouncementArtifact",
            "parse_announcement",
        }.issubset(set(module.__all__))
    elif subpackage == "extract":
        assert {
            "AnnouncementFactCandidate",
            "EvidenceSpan",
            "FactType",
            "classify_disclosure_types",
            "extract_fact_candidates",
        }.issubset(set(module.__all__))
    elif subpackage == "runtime":
        assert {
            "AnnouncementExtractionRun",
            "AnnouncementPipeline",
            "submit_candidates",
        }.issubset(set(module.__all__))
    elif subpackage == "signals":
        assert {
            "AnnouncementSignalCandidate",
            "SignalDirection",
            "SignalTemplate",
            "SignalTimeHorizon",
            "derive_signal_candidates",
        }.issubset(set(module.__all__))
    elif subpackage == "graph":
        assert {
            "AnnouncementGraphDeltaCandidate",
            "GraphDeltaGuard",
            "GraphDeltaType",
            "GraphRelationType",
            "derive_graph_delta_candidates",
        }.issubset(set(module.__all__))
    else:
        assert {
            "AnnouncementChunk",
            "AnnouncementRetrievalArtifact",
            "AnnouncementRetrievalHit",
            "build_retrieval_artifact",
            "chunk_parsed_artifact",
            "load_retrieval_artifact",
            "query",
        }.issubset(set(module.__all__))


def test_config_defaults_are_instantiable() -> None:
    config = AnnouncementConfig()
    assert config.heartbeat_interval_seconds == 60
    assert config.reasoner_endpoint is None
    assert config.entity_registry_endpoint is None


def test_load_config_missing_file_returns_defaults(tmp_path: Path) -> None:
    config = load_config(tmp_path / "missing.toml")
    assert config == AnnouncementConfig()


def test_load_config_rejects_invalid_fields(tmp_path: Path) -> None:
    config_path = tmp_path / "announcement.toml"
    config_path.write_text('heartbeat_interval_seconds = "bad"\n', encoding="utf-8")

    with pytest.raises(ValidationError):
        load_config(config_path)


def test_config_rejects_unpinned_parser_and_index_versions() -> None:
    with pytest.raises(ValidationError, match="docling_version"):
        AnnouncementConfig(docling_version="docling>=2")

    with pytest.raises(ValidationError, match="llama_index_version"):
        AnnouncementConfig(llama_index_version="llama-index>=0.11")


def test_cli_version_returns_package_version() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "subsystem_announcement", "version"],
        cwd=ROOT,
        env=_cli_env(),
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert result.stdout.strip() == "0.1.1"


def test_cli_doctor_loads_default_config() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "subsystem_announcement", "doctor"],
        cwd=ROOT,
        env=_cli_env(),
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert "ok" in result.stdout.splitlines()
    assert "parser_version=not-configured (unset)" in result.stdout.splitlines()
    assert "index_version=not-configured (unset)" in result.stdout.splitlines()


def test_configure_logging_is_idempotent() -> None:
    configure_logging()
    configure_logging()
