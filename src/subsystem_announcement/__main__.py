"""Command-line entry point for the announcement subsystem scaffold."""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

from . import __version__
from .config import load_config
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
            load_config(config)
        except Exception as exc:
            typer.echo(f"doctor failed: {exc}", err=True)
            raise typer.Exit(code=1) from exc
        typer.echo("ok")

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
            load_config(config_path)
        except Exception as exc:
            print(f"doctor failed: {exc}", file=sys.stderr)
            return 1
        print("ok")
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

    print(
        "usage: python -m subsystem_announcement [version|doctor|ping|run]",
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


if __name__ == "__main__":
    raise SystemExit(main())
