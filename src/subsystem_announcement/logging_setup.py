"""Logging configuration for JSON-line subsystem logs."""

from __future__ import annotations

import json
import logging
import sys
from typing import Any

_CONFIGURED = False


class _JsonLineFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "level": record.levelname.lower(),
            "event": record.getMessage(),
            "logger": record.name,
        }
        return json.dumps(payload, ensure_ascii=False)


def configure_logging(level: str = "INFO") -> None:
    """Configure idempotent JSON-line logging.

    structlog is the preferred backend when installed. The stdlib fallback keeps
    the scaffold runnable in offline sandboxes before dependencies are synced.
    """

    global _CONFIGURED

    numeric_level = getattr(logging, level.upper(), logging.INFO)

    try:
        try:
            import structlog
        except ModuleNotFoundError:
            handler = logging.StreamHandler(sys.stderr)
            handler.setFormatter(_JsonLineFormatter())
            root_logger = logging.getLogger()
            if not _CONFIGURED:
                root_logger.handlers = [handler]
            root_logger.setLevel(numeric_level)
        else:
            logging.basicConfig(
                format="%(message)s",
                level=numeric_level,
                stream=sys.stderr,
                force=not _CONFIGURED,
            )
            structlog.configure(
                processors=[
                    structlog.processors.add_log_level,
                    structlog.processors.TimeStamper(fmt="iso"),
                    structlog.processors.JSONRenderer(),
                ],
                wrapper_class=structlog.make_filtering_bound_logger(numeric_level),
                logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
                cache_logger_on_first_use=True,
            )
        _CONFIGURED = True
    except Exception:
        if _CONFIGURED:
            return
        raise
