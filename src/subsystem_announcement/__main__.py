"""Command-line entry point for the announcement subsystem scaffold."""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

from . import __version__
from .config import AnnouncementConfig, load_config
from .logging_setup import configure_logging

try:
    import typer
except ModuleNotFoundError:
    typer = None


if typer is not None:
    app = typer.Typer(add_completion=False, help="Announcement subsystem CLI.")

    @app.command("version")
    def version_command() -> None:
        """Print package version."""

        typer.echo(__version__)

    @app.command("doctor")
    def doctor_command(
        config: Path | None = typer.Option(
            None,
            "--config",
            "-c",
            help="Optional announcement TOML config path.",
        ),
    ) -> None:
        """Validate that configuration can be loaded."""

        try:
            runtime_config = load_config(config)
        except Exception as exc:
            typer.echo(f"doctor failed: {exc}", err=True)
            raise typer.Exit(code=1) from exc
        typer.echo(_doctor_report(runtime_config))

    @app.command("ping")
    def ping_command(
        config: Path | None = typer.Option(
            None,
            "--config",
            "-c",
            help="Optional announcement TOML config path.",
        ),
    ) -> None:
        """Run registration, one heartbeat, and one Ex-0 submit."""

        try:
            configure_logging()
            runtime_config = load_config(config)
            from .runtime.lifecycle import ping
            asyncio.run(ping(runtime_config))
        except Exception as exc:
            typer.echo(f"ping failed: {exc}", err=True)
            raise typer.Exit(code=1) from exc
        typer.echo("ok")

    @app.command("run")
    def run_command(
        config: Path | None = typer.Option(
            None,
            "--config",
            "-c",
            help="Optional announcement TOML config path.",
        ),
        once: bool = typer.Option(
            False,
            "--once",
            help="Run one lifecycle iteration and exit.",
        ),
    ) -> None:
        """Run the announcement subsystem lifecycle."""

        try:
            configure_logging()
            runtime_config = load_config(config)
            from .runtime.lifecycle import run
            asyncio.run(run(runtime_config, once=once))
        except KeyboardInterrupt:
            raise typer.Exit(code=130) from None
        except Exception as exc:
            typer.echo(f"run failed: {exc}", err=True)
            raise typer.Exit(code=1) from exc
        typer.echo("ok")

    @app.command("process")
    def process_command(
        envelope: Path = typer.Option(
            ...,
            "--envelope",
            help="Announcement envelope JSON path.",
        ),
        config: Path = typer.Option(
            ...,
            "--config",
            "-c",
            help="Announcement TOML config path.",
        ),
        trace_output: Path | None = typer.Option(
            None,
            "--trace-output",
            help="Optional path for an additional trace JSON copy.",
        ),
    ) -> None:
        """Process one announcement envelope through Ex-1 submit."""

        try:
            configure_logging()
            runtime_config = load_config(config)
            announcement_envelope = _load_envelope(envelope)
            from .runtime.pipeline import AnnouncementPipeline

            run = asyncio.run(
                AnnouncementPipeline(runtime_config).process_envelope(
                    announcement_envelope
                )
            )
            _write_trace_copy(run, trace_output)
        except Exception as exc:
            typer.echo(f"process failed: {exc}", err=True)
            raise typer.Exit(code=1) from exc
        if run.status != "succeeded":
            typer.echo(
                f"process failed: status={run.status} trace={run.trace_path}",
                err=True,
            )
            raise typer.Exit(code=1)
        typer.echo(f"ok trace={run.trace_path}")

    @app.command("replay")
    def replay_command(
        announcement_id: str = typer.Option(
            ...,
            "--announcement-id",
            help="Cached announcement id to replay.",
        ),
        config: Path = typer.Option(
            ...,
            "--config",
            "-c",
            help="Announcement TOML config path.",
        ),
        rebuild_index: bool = typer.Option(
            False,
            "--rebuild-index",
            help="Rebuild retrieval artifact after replay parse.",
        ),
    ) -> None:
        """Replay one cached announcement without fetching its official URL."""

        try:
            configure_logging()
            runtime_config = load_config(config)
            from .runtime.replay import ReplayRequest, replay_announcement

            result = asyncio.run(
                replay_announcement(
                    ReplayRequest(
                        announcement_id=announcement_id,
                        rebuild_index=rebuild_index,
                    ),
                    runtime_config,
                )
            )
        except Exception as exc:
            typer.echo(f"replay failed: {exc}", err=True)
            raise typer.Exit(code=1) from exc
        if result.run.status != "succeeded":
            typer.echo(
                f"replay failed: status={result.run.status} "
                f"trace={result.run.trace_path}",
                err=True,
            )
            raise typer.Exit(code=1)
        typer.echo(f"ok trace={result.run.trace_path}")

    @app.command("repair")
    def repair_command(
        trace: Path | None = typer.Option(
            None,
            "--trace",
            help="Failed run trace JSON path.",
        ),
        document: Path | None = typer.Option(
            None,
            "--document",
            help="Cached document path or metadata JSON path.",
        ),
        announcement_id: str | None = typer.Option(
            None,
            "--announcement-id",
            help="Announcement id to resolve through the dedupe cache.",
        ),
        config: Path = typer.Option(
            ...,
            "--config",
            "-c",
            help="Announcement TOML config path.",
        ),
        reason: str = typer.Option(
            "parse_failure",
            "--reason",
            help="Repair reason: parse_failure or docling_version_upgrade.",
        ),
        rebuild_index: bool = typer.Option(
            True,
            "--rebuild-index/--no-rebuild-index",
            help="Rebuild retrieval artifact after reparsing.",
        ),
    ) -> None:
        """Repair a parsed artifact from cached document bytes."""

        try:
            configure_logging()
            runtime_config = load_config(config)
            from .runtime.repair import (
                RepairReason,
                RepairRequest,
                repair_parsed_artifact,
            )

            result = repair_parsed_artifact(
                RepairRequest(
                    announcement_id=announcement_id,
                    trace_path=trace,
                    document_path=document,
                    reason=RepairReason(reason),
                    rebuild_index=rebuild_index,
                ),
                runtime_config,
            )
        except Exception as exc:
            typer.echo(f"repair failed: {exc}", err=True)
            raise typer.Exit(code=1) from exc
        typer.echo(f"ok parsed={result.parsed_artifact_path}")

    @app.command("metrics")
    def metrics_command(
        manifest: Path = typer.Option(
            Path("tests/fixtures/announcements/manifest.json"),
            "--manifest",
            help="Announcement sample manifest path.",
        ),
        config: Path | None = typer.Option(
            None,
            "--config",
            "-c",
            help="Optional announcement TOML config path.",
        ),
    ) -> None:
        """Compute fixture-backed stage-3 regression metrics."""

        try:
            runtime_config = load_config(config)
            from .runtime.metrics import (
                assert_metrics_within_thresholds,
                compute_metrics_for_manifest,
            )

            report = compute_metrics_for_manifest(manifest, config=runtime_config)
            assert_metrics_within_thresholds(report)
        except Exception as exc:
            typer.echo(f"metrics failed: {exc}", err=True)
            raise typer.Exit(code=1) from exc
        typer.echo(report.model_dump_json(indent=2))
else:
    app = None


def _fallback_main(argv: list[str]) -> int:
    command = argv[0] if argv else ""
    if command == "version":
        print(__version__)
        return 0
    if command == "doctor":
        config_path: Path | None = None
        if len(argv) in {2, 3} and argv[1] in {"--config", "-c"}:
            config_path = Path(argv[2]) if len(argv) == 3 else None
        try:
            runtime_config = load_config(config_path)
        except Exception as exc:
            print(f"doctor failed: {exc}", file=sys.stderr)
            return 1
        print(_doctor_report(runtime_config))
        return 0
    if command in {"ping", "run"}:
        once = command == "ping" or "--once" in argv
        config_path: Path | None = None
        if "--config" in argv:
            index = argv.index("--config")
            if index + 1 < len(argv):
                config_path = Path(argv[index + 1])
        elif "-c" in argv:
            index = argv.index("-c")
            if index + 1 < len(argv):
                config_path = Path(argv[index + 1])
        try:
            configure_logging()
            runtime_config = load_config(config_path)
            from .runtime.lifecycle import ping, run
            if command == "ping":
                asyncio.run(ping(runtime_config))
            else:
                asyncio.run(run(runtime_config, once=once))
        except KeyboardInterrupt:
            return 130
        except Exception as exc:
            print(f"{command} failed: {exc}", file=sys.stderr)
            return 1
        print("ok")
        return 0
    if command == "process":
        envelope_path = _option_path(argv, "--envelope")
        config_path = _option_path(argv, "--config") or _option_path(argv, "-c")
        trace_output = _option_path(argv, "--trace-output")
        if envelope_path is None or config_path is None:
            print(
                "usage: python -m subsystem_announcement process "
                "--envelope PATH --config PATH [--trace-output PATH]",
                file=sys.stderr,
            )
            return 1
        try:
            configure_logging()
            runtime_config = load_config(config_path)
            announcement_envelope = _load_envelope(envelope_path)
            from .runtime.pipeline import AnnouncementPipeline

            run = asyncio.run(
                AnnouncementPipeline(runtime_config).process_envelope(
                    announcement_envelope
                )
            )
            _write_trace_copy(run, trace_output)
        except Exception as exc:
            print(f"process failed: {exc}", file=sys.stderr)
            return 1
        if run.status != "succeeded":
            print(
                f"process failed: status={run.status} trace={run.trace_path}",
                file=sys.stderr,
            )
            return 1
        print(f"ok trace={run.trace_path}")
        return 0
    if command == "replay":
        announcement_id = _option_value(argv, "--announcement-id")
        config_path = _option_path(argv, "--config") or _option_path(argv, "-c")
        if announcement_id is None or config_path is None:
            print(
                "usage: python -m subsystem_announcement replay "
                "--announcement-id ID --config PATH [--rebuild-index]",
                file=sys.stderr,
            )
            return 1
        try:
            configure_logging()
            runtime_config = load_config(config_path)
            from .runtime.replay import ReplayRequest, replay_announcement

            result = asyncio.run(
                replay_announcement(
                    ReplayRequest(
                        announcement_id=announcement_id,
                        rebuild_index="--rebuild-index" in argv,
                    ),
                    runtime_config,
                )
            )
        except Exception as exc:
            print(f"replay failed: {exc}", file=sys.stderr)
            return 1
        if result.run.status != "succeeded":
            print(
                f"replay failed: status={result.run.status} "
                f"trace={result.run.trace_path}",
                file=sys.stderr,
            )
            return 1
        print(f"ok trace={result.run.trace_path}")
        return 0
    if command == "repair":
        config_path = _option_path(argv, "--config") or _option_path(argv, "-c")
        trace_path = _option_path(argv, "--trace")
        document_path = _option_path(argv, "--document")
        announcement_id = _option_value(argv, "--announcement-id")
        reason = _option_value(argv, "--reason") or "parse_failure"
        if config_path is None or (
            trace_path is None and document_path is None and announcement_id is None
        ):
            print(
                "usage: python -m subsystem_announcement repair "
                "(--trace PATH | --document PATH | --announcement-id ID) "
                "--config PATH [--reason REASON] [--no-rebuild-index]",
                file=sys.stderr,
            )
            return 1
        try:
            configure_logging()
            runtime_config = load_config(config_path)
            from .runtime.repair import (
                RepairReason,
                RepairRequest,
                repair_parsed_artifact,
            )

            result = repair_parsed_artifact(
                RepairRequest(
                    announcement_id=announcement_id,
                    trace_path=trace_path,
                    document_path=document_path,
                    reason=RepairReason(reason),
                    rebuild_index="--no-rebuild-index" not in argv,
                ),
                runtime_config,
            )
        except Exception as exc:
            print(f"repair failed: {exc}", file=sys.stderr)
            return 1
        print(f"ok parsed={result.parsed_artifact_path}")
        return 0
    if command == "metrics":
        manifest_path = (
            _option_path(argv, "--manifest")
            or Path("tests/fixtures/announcements/manifest.json")
        )
        config_path = _option_path(argv, "--config") or _option_path(argv, "-c")
        try:
            runtime_config = load_config(config_path)
            from .runtime.metrics import (
                assert_metrics_within_thresholds,
                compute_metrics_for_manifest,
            )

            report = compute_metrics_for_manifest(manifest_path, config=runtime_config)
            assert_metrics_within_thresholds(report)
        except Exception as exc:
            print(f"metrics failed: {exc}", file=sys.stderr)
            return 1
        print(report.model_dump_json(indent=2))
        return 0

    print(
        "usage: python -m subsystem_announcement "
        "[version|doctor|ping|run|process|replay|repair|metrics]",
        file=sys.stderr,
    )
    return 1


def main() -> int:
    """Run the CLI and return a process exit code."""

    if app is None:
        return _fallback_main(sys.argv[1:])

    try:
        app()
    except SystemExit as exc:
        if isinstance(exc.code, int):
            return exc.code
        return 0 if exc.code is None else 1
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 1
    return 0


def _doctor_report(config: AnnouncementConfig) -> str:
    return "\n".join(
        [
            "ok",
            f"parser_version={_version_status(config.docling_version)}",
            f"index_version={_version_status(config.llama_index_version)}",
        ]
    )


def _version_status(value: str) -> str:
    if value == "not-configured":
        return "not-configured (unset)"
    return value


def _load_envelope(path: Path) -> object:
    from .discovery import AnnouncementEnvelope

    try:
        return AnnouncementEnvelope.model_validate_json(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise RuntimeError(f"Unable to load announcement envelope: path={path}") from exc


def _write_trace_copy(run: object, trace_output: Path | None) -> None:
    if trace_output is None:
        return
    if not hasattr(run, "model_dump_json"):
        raise RuntimeError("Trace object does not support JSON serialization")
    try:
        trace_output.parent.mkdir(parents=True, exist_ok=True)
        trace_output.write_text(run.model_dump_json(indent=2), encoding="utf-8")
    except OSError as exc:
        raise RuntimeError(f"Unable to write trace output: path={trace_output}") from exc


def _option_path(argv: list[str], option: str) -> Path | None:
    if option not in argv:
        return None
    index = argv.index(option)
    if index + 1 >= len(argv):
        return None
    return Path(argv[index + 1])


def _option_value(argv: list[str], option: str) -> str | None:
    if option not in argv:
        return None
    index = argv.index(option)
    if index + 1 >= len(argv):
        return None
    return argv[index + 1]


if __name__ == "__main__":
    raise SystemExit(main())
