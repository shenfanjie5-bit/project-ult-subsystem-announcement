"""Offline CLI for announcement retrieval indexes."""

from __future__ import annotations

import argparse
import json
import sys
import tomllib
from pathlib import Path

from subsystem_announcement.config import AnnouncementConfig, load_config
from subsystem_announcement.parse.artifact import load_parsed_artifact

from .retrieval_artifact import (
    build_retrieval_artifact,
    load_retrieval_artifact,
    write_retrieval_artifact,
)
from .sample_query import query


def main(argv: list[str] | None = None) -> int:
    """Run the offline index CLI."""

    parser = _build_parser()
    args = parser.parse_args(argv)
    if args.command is None:
        parser.print_help()
        return 0

    try:
        if args.command == "build":
            return _build_command(args)
        if args.command == "query":
            return _query_command(args)
    except Exception as exc:
        print(f"index {args.command} failed: {exc}", file=sys.stderr)
        return 1
    return 1


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m subsystem_announcement.index",
        description="Build and query offline announcement retrieval indexes.",
    )
    subparsers = parser.add_subparsers(dest="command")

    build_parser = subparsers.add_parser("build", help="Build a retrieval index.")
    build_parser.add_argument("--parsed-artifact", type=Path, required=True)
    build_parser.add_argument("--output", type=Path, required=True)
    build_parser.add_argument("--config", type=Path, default=None)

    query_parser = subparsers.add_parser("query", help="Query a retrieval artifact.")
    query_parser.add_argument("--artifact", type=Path, required=True)
    query_parser.add_argument("--text", required=True)
    query_parser.add_argument("--top-k", type=int, default=5)
    return parser


def _build_command(args: argparse.Namespace) -> int:
    config = _load_cli_config(args.config)
    parsed_artifact = load_parsed_artifact(args.parsed_artifact)
    artifact = build_retrieval_artifact(
        parsed_artifact,
        config=config,
        parsed_artifact_path=args.parsed_artifact,
        output_root=args.output,
    )
    artifact_path = write_retrieval_artifact(artifact, args.output)
    print(f"ok artifact={artifact_path}")
    return 0


def _query_command(args: argparse.Namespace) -> int:
    artifact = load_retrieval_artifact(args.artifact)
    hits = query(args.text, artifact, top_k=args.top_k)
    print(
        json.dumps(
            [hit.model_dump(mode="json") for hit in hits],
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def _load_cli_config(config_path: Path | None) -> AnnouncementConfig:
    config = load_config(config_path)
    if config.llama_index_version != "not-configured":
        return config
    pin = _pyproject_llama_index_pin(Path.cwd())
    if pin is None:
        return config
    return config.model_copy(update={"llama_index_version": pin})


def _pyproject_llama_index_pin(cwd: Path) -> str | None:
    for root in [cwd, *cwd.parents]:
        pyproject_path = root / "pyproject.toml"
        if not pyproject_path.exists():
            continue
        try:
            pyproject = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))
        except (OSError, tomllib.TOMLDecodeError):
            return None
        dependencies = pyproject.get("project", {}).get("dependencies", [])
        exact_pins = [
            dependency
            for dependency in dependencies
            if isinstance(dependency, str)
            and (
                dependency.startswith("llama-index-core==")
                or dependency.startswith("llama-index==")
            )
        ]
        return exact_pins[0] if exact_pins else None
    return None


if __name__ == "__main__":
    raise SystemExit(main())
