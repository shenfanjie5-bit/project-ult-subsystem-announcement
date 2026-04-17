"""Command-line entry point for the announcement subsystem scaffold."""

from __future__ import annotations

import sys
from pathlib import Path

from . import __version__
from .config import load_config

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

    print("usage: python -m subsystem_announcement [version|doctor]", file=sys.stderr)
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
