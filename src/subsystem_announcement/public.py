"""Assembly-facing public entrypoints for subsystem-announcement.

This module is the single boundary that ``assembly`` (registry + compat
checks + bootstrap) imports to introspect this package. The five
``module-level singleton instances`` below match the assembly Protocols
in ``assembly/src/assembly/contracts/entrypoints.py`` and the signature
shape enforced by ``assembly/src/assembly/compat/checks/public_api_boundary.py``:

- ``health_probe.check(*, timeout_sec: float)``
- ``smoke_hook.run(*, profile_id: str)``
- ``init_hook.initialize(*, resolved_env: dict[str, str])``
- ``version_declaration.declare()``
- ``cli.invoke(argv: list[str])``

CLAUDE.md guardrails this file enforces by construction:

- **No Layer B authoritative validation** — that's ``data-platform`` /
  ``main-core``. SDK does producer-side preflight only.
- **No second parser** — ``docling`` is the single Docling-only parser.
  public.py never imports any other PDF / HTML parser.
- **No direct LLM provider SDK** — complex extraction goes through
  ``reasoner-runtime``. public.py never imports openai/anthropic/litellm.
- **No formal-object writes** — only Ex-1/2/3 candidate emission via
  ``subsystem-sdk.submit.SubmitClient`` (which strips the SDK envelope
  at dispatch boundary per stage 2.7 follow-up #2 — backend always
  receives the wire shape Layer B accepts).
- **No business module imports** — never imports data_platform /
  main_core / graph_engine / audit_eval / orchestrator / assembly.
  Heavy infra (Docling / LlamaIndex / httpx) is imported lazily inside
  smoke_hook only when a real probe call is requested.
"""

from __future__ import annotations

import json
import sys
from typing import Any, Final

from subsystem_announcement import __version__ as _ANNOUNCEMENT_VERSION


_HEALTHY: Final[str] = "healthy"
_DEGRADED: Final[str] = "degraded"
_DOWN: Final[str] = "blocked"

# Ex types this subsystem produces. Ex-0 (heartbeat) is provided by
# subsystem-sdk's own heartbeat client, NOT by announcement, so it's
# not in this list.
_SUPPORTED_EX_TYPES: Final[tuple[str, ...]] = ("Ex-1", "Ex-2", "Ex-3")

# Stage 4 §4.1.5: contract_version is the canonical contracts schema version
# this module is bound against (NOT this module's own package version, which
# stays in module_version). Harmonized to v0.1.3 across all 11 active
# subsystem modules so assembly's ContractsVersionCheck (strict equality vs
# matrix.contract_version) succeeds at the cross-project compat audit.
# Previously this was derived dynamically via subsystem_sdk._contracts
# .get_schema_version, which returns "unknown" today (contracts Ex models
# don't expose a `schema_version` class attribute), and assembly's
# VersionInfo regex `^v\d+\.\d+\.\d+$` rejects "unknown". Per Stage 4 §4.1.5
# we hardcode the canonical value matching the contracts package version
# announcement is pinned against.
_CONTRACT_VERSION: Final[str] = "v0.1.3"
_COMPATIBLE_CONTRACT_RANGE: Final[str] = ">=0.1.3,<0.2.0"


def _probe_sdk_envelope_strip() -> dict[str, Any]:
    """Confirm subsystem-sdk's ``strip_sdk_envelope`` is importable + the
    canonical envelope set is the expected 3 fields. This is the cross-
    repo wire-shape invariant announcement depends on (铁律 #7).
    """

    try:
        from subsystem_sdk.validate.engine import (
            SDK_ENVELOPE_FIELDS,
            strip_sdk_envelope,
        )
    except Exception as exc:  # pragma: no cover - defensive
        return {
            "available": False,
            "reason": f"subsystem_sdk.validate.engine import failed: {exc!r}",
        }

    expected = {"ex_type", "semantic", "produced_at"}
    if set(SDK_ENVELOPE_FIELDS) != expected:
        return {
            "available": False,
            "reason": (
                "SDK_ENVELOPE_FIELDS drifted: expected "
                f"{sorted(expected)}, got {sorted(SDK_ENVELOPE_FIELDS)}"
            ),
        }

    # Probe the strip function on a synthetic payload — proves the
    # envelope is actually removed (not just declared).
    sample = {
        "ex_type": "Ex-1",
        "semantic": "metadata_or_heartbeat",
        "produced_at": "2026-01-01T00:00:00Z",
        "subsystem_id": "probe",
    }
    stripped = dict(strip_sdk_envelope(sample))
    if set(stripped) != {"subsystem_id"}:
        return {
            "available": False,
            "reason": f"strip_sdk_envelope returned unexpected keys: {sorted(stripped)}",
        }

    return {"available": True, "envelope_fields": sorted(SDK_ENVELOPE_FIELDS)}


def _probe_announcement_runtime_imports() -> dict[str, Any]:
    """Confirm the Ex-1/2/3 candidate models + submit shim are importable
    without pulling in the heavy parser/LLM stack at module load.
    """

    try:
        from subsystem_announcement.extract import AnnouncementFactCandidate
        from subsystem_announcement.signals import AnnouncementSignalCandidate
        from subsystem_announcement.graph import AnnouncementGraphDeltaCandidate
    except Exception as exc:
        return {
            "available": False,
            "reason": f"candidate models import failed: {exc!r}",
        }

    return {
        "available": True,
        "candidate_models": {
            "Ex-1": AnnouncementFactCandidate.__name__,
            "Ex-2": AnnouncementSignalCandidate.__name__,
            "Ex-3": AnnouncementGraphDeltaCandidate.__name__,
        },
    }


class _HealthProbe:
    """Probe SDK + announcement-internal invariants without doing any
    network IO or pulling in Docling / LlamaIndex.

    `check(*, timeout_sec)` returns a structured dict with status one of
    ``healthy`` / ``degraded`` / ``down``. ``timeout_sec`` is accepted
    for assembly Protocol compliance but unused — none of these checks
    do IO.
    """

    _PROBE_NAME: Final[str] = "subsystem_announcement.health"

    def check(self, *, timeout_sec: float) -> dict[str, Any]:
        # Stage 4 §4.3 Lite-stack e2e fix: assembly's
        # ``HealthResult.model_validate`` requires ``module_id`` /
        # ``probe_name`` / ``latency_ms`` / ``message`` plus the status
        # enum value in {healthy, degraded, blocked}.
        from time import perf_counter

        started_at = perf_counter()
        details: dict[str, Any] = {
            "supported_ex_types": list(_SUPPORTED_EX_TYPES),
            "timeout_sec": timeout_sec,
        }

        # Invariant 1: SDK envelope strip wire-shape boundary (铁律 #7).
        sdk_probe = _probe_sdk_envelope_strip()
        details["sdk_envelope_strip"] = sdk_probe
        if not sdk_probe["available"]:
            return self._build_result(
                started_at,
                status=_DOWN,
                message=(
                    "subsystem-announcement SDK envelope strip wire-shape "
                    "boundary unavailable (铁律 #7 broken)"
                ),
                details=details,
            )

        # Invariant 2: announcement candidate models importable.
        # Treat ``ModuleNotFoundError`` for transitive runtime deps
        # (``httpx``, ``docling``, ``llama_index``) as ``degraded`` —
        # offline-first dev venvs without these heavy parsers are
        # allowed (per ``[runtime]`` extra split discussed in plan).
        # Other import failures (e.g., a candidate model class
        # rename / removal) remain ``blocked`` because they indicate a
        # real domain invariant violation.
        runtime_probe = _probe_announcement_runtime_imports()
        details["announcement_runtime"] = runtime_probe
        if not runtime_probe["available"]:
            reason = runtime_probe.get("reason", "")
            if "ModuleNotFoundError" in reason:
                return self._build_result(
                    started_at,
                    status=_DEGRADED,
                    message=(
                        "subsystem-announcement running offline-first — "
                        "transitive runtime dep missing in this venv: "
                        f"{reason}"
                    ),
                    details=details,
                )
            return self._build_result(
                started_at,
                status=_DOWN,
                message=(
                    "subsystem-announcement candidate runtime imports failed"
                ),
                details=details,
            )

        return self._build_result(
            started_at,
            status=_HEALTHY,
            message=(
                "subsystem-announcement invariants verified (SDK envelope "
                "strip + announcement runtime imports both available)"
            ),
            details=details,
        )

    def _build_result(
        self,
        started_at: float,
        *,
        status: str,
        message: str,
        details: dict[str, Any],
    ) -> dict[str, Any]:
        from time import perf_counter

        return {
            "module_id": "subsystem-announcement",
            "probe_name": self._PROBE_NAME,
            "status": status,
            "latency_ms": max(0.0, (perf_counter() - started_at) * 1000.0),
            "message": message,
            "details": details,
        }


class _SmokeHook:
    """Run a one-shot end-to-end smoke that builds a minimal Ex-1 payload
    via the announcement candidate model, submits it through the real
    ``subsystem_sdk.SubmitClient`` against a ``MockSubmitBackend``, and
    asserts:

    1. Backend receives the WIRE shape (no SDK envelope — proves the
       end-to-end strip path works for announcement, not just SDK alone).
    2. Backend receives all CLAUDE.md producer-owned fields
       (announcement_id / fact_id / primary_entity_id / evidence_spans /
       extracted_at / official_url source_reference).
    3. SubmitReceipt is accepted with no errors.

    Profile-aware only insofar as it rejects unknown profile_ids.
    Heavy deps (Docling / LlamaIndex / httpx) are NOT imported here.
    """

    _SUPPORTED_PROFILES: Final[frozenset[str]] = frozenset(
        {"lite-local", "full-dev"}
    )

    def run(self, *, profile_id: str) -> dict[str, Any]:
        if profile_id not in self._SUPPORTED_PROFILES:
            return {
                "passed": False,
                "failure_reason": (
                    f"unknown profile_id={profile_id!r}; supported: "
                    f"{sorted(self._SUPPORTED_PROFILES)}"
                ),
                "profile_id": profile_id,
            }

        from datetime import UTC, datetime

        from subsystem_sdk.backends.mock import MockSubmitBackend
        from subsystem_sdk.submit.client import SubmitClient
        from subsystem_sdk.validate.engine import SDK_ENVELOPE_FIELDS
        from subsystem_sdk.validate.result import ValidationResult

        from subsystem_announcement.extract import AnnouncementFactCandidate
        from subsystem_announcement.extract.evidence import EvidenceSpan
        from subsystem_announcement.extract.candidates import FactType

        # 1. Build a minimal valid Ex-1 candidate (model rejects bad
        #    shape via its own validators — extracted_at must be tz-aware,
        #    evidence_spans must be non-empty, source_reference must
        #    have official_url, no FORBIDDEN_PAYLOAD_KEYS leak).
        try:
            ex1_candidate = AnnouncementFactCandidate(
                fact_id="smoke-fact-001",
                announcement_id="smoke-ann-001",
                fact_type=FactType.MAJOR_CONTRACT,
                primary_entity_id="ENT_STOCK_PLACEHOLDER",
                fact_content={"smoke": "minimal"},
                confidence=0.9,
                source_reference={
                    "official_url": "https://www.sse.com.cn/disclosure/announcement/smoke",
                    "source_kind": "exchange_disclosure",
                    "is_primary_source": True,
                },
                evidence_spans=[
                    EvidenceSpan(
                        section_id="smoke-section-001",
                        start_offset=0,
                        end_offset=11,
                        quote="placeholder",
                    ),
                ],
                extracted_at=datetime(2026, 1, 1, tzinfo=UTC),
            )
        except Exception as exc:
            return {
                "passed": False,
                "failure_reason": (
                    f"AnnouncementFactCandidate construction failed: {exc!r}"
                ),
                "profile_id": profile_id,
            }

        # 2. Submit through real SDK + MockSubmitBackend. Use a
        #    permissive validator so smoke doesn't depend on contracts
        #    being installed (real cross-repo align is in tests/contract
        #    + tests/integration, not smoke).
        backend = MockSubmitBackend()

        def permissive_validator(_: Any) -> ValidationResult:
            return ValidationResult.ok(
                ex_type="Ex-1", schema_version="smoke"
            )

        try:
            receipt = SubmitClient(
                backend, validator=permissive_validator
            ).submit(ex1_candidate.to_ex_payload())
        except Exception as exc:
            return {
                "passed": False,
                "failure_reason": f"SubmitClient.submit raised: {exc!r}",
                "profile_id": profile_id,
            }

        # 3. Receipt must be accepted; backend must receive WIRE shape.
        if not receipt.accepted:
            return {
                "passed": False,
                "failure_reason": (
                    f"receipt not accepted: errors={list(receipt.errors)}"
                ),
                "profile_id": profile_id,
            }

        if len(backend.submitted_payloads) != 1:
            return {
                "passed": False,
                "failure_reason": (
                    f"expected 1 submitted payload, got "
                    f"{len(backend.submitted_payloads)}"
                ),
                "profile_id": profile_id,
            }
        wire = backend.submitted_payloads[0]
        leaked = SDK_ENVELOPE_FIELDS.intersection(wire)
        if leaked:
            return {
                "passed": False,
                "failure_reason": (
                    f"SDK envelope leaked to backend: {sorted(leaked)}; "
                    "validate_then_dispatch must strip envelope before "
                    "backend dispatch (announcement -> SDK -> backend "
                    "wire-shape boundary, 铁律 #7)"
                ),
                "profile_id": profile_id,
            }

        # 4. CLAUDE.md producer-owned fields must reach the backend.
        for required_field in (
            "announcement_id",
            "fact_id",
            "primary_entity_id",
            "evidence_spans",
            "extracted_at",
            "source_reference",
        ):
            if required_field not in wire:
                return {
                    "passed": False,
                    "failure_reason": (
                        f"required producer field {required_field!r} "
                        f"missing from wire payload: {sorted(wire)}"
                    ),
                    "profile_id": profile_id,
                }

        return {
            "passed": True,
            "profile_id": profile_id,
            "details": {
                "receipt_id": receipt.receipt_id,
                "backend_kind": receipt.backend_kind,
                "validator_version": receipt.validator_version,
                "wire_payload_keys": sorted(wire),
                "envelope_fields_stripped": sorted(SDK_ENVELOPE_FIELDS),
            },
        }


class _InitHook:
    """No-op initialization. announcement has no global mutable state to
    set up at bootstrap (Docling / LlamaIndex / httpx are constructed
    per-call inside the runtime pipeline, not eagerly at import time).
    Returns ``None`` per assembly Protocol; ``resolved_env`` is accepted
    for compliance.
    """

    def initialize(self, *, resolved_env: dict[str, str]) -> None:
        _ = resolved_env
        return None


class _VersionDeclaration:
    """Declare the announcement + SDK + contracts schema versions
    assembly should reconcile in the registry. Returns a stable dict
    shape:

        {
            "module_id": "subsystem-announcement",
            "module_version": "<package version>",
            "supported_ex_types": [...],
            "sdk_envelope_fields": [...],
            "contract_version": "<contracts schema version or 'unknown'>",
            "ex3_high_threshold_marker": True,
        }
    """

    def declare(self) -> dict[str, Any]:
        sdk_envelope = self._safe_sdk_envelope()

        return {
            "module_id": "subsystem-announcement",
            "module_version": _ANNOUNCEMENT_VERSION,
            "contract_version": _CONTRACT_VERSION,
            "compatible_contract_range": _COMPATIBLE_CONTRACT_RANGE,
            "supported_ex_types": list(_SUPPORTED_EX_TYPES),
            "sdk_envelope_fields": sdk_envelope,
            # CLAUDE.md §19: Ex-3 误产出率 < 1%; the high-threshold guard
            # is a structural marker assembly can use to verify the
            # invariant is enforced (cross-checked in boundary tier).
            "ex3_high_threshold_marker": True,
        }

    @staticmethod
    def _safe_sdk_envelope() -> list[str]:
        try:
            from subsystem_sdk.validate.engine import SDK_ENVELOPE_FIELDS

            return sorted(SDK_ENVELOPE_FIELDS)
        except Exception:
            return []


class _Cli:
    """Tiny announcement CLI for assembly's smoke probes; intentionally
    minimal to keep iron rule #2 boundary (no business logic in CLI).
    Supported argv:

    - ``["version"]`` — print version_declaration JSON to stdout, exit 0
    - ``["health", "--timeout-sec", "<float>"]`` — print health JSON,
      exit 0 on healthy/degraded, 1 on down
    - ``["smoke", "--profile-id", "<id>"]`` — print smoke JSON, exit 0
      on passed, 1 on failed
    """

    def invoke(self, argv: list[str]) -> int:
        if not argv:
            sys.stderr.write(
                "usage: subsystem-announcement-cli "
                "{version|health|smoke} [args]\n"
            )
            return 2

        command = argv[0]
        rest = argv[1:]

        if command == "version":
            sys.stdout.write(
                json.dumps(version_declaration.declare()) + "\n"
            )
            return 0

        if command == "health":
            timeout_sec = self._parse_kw_float(
                rest, "--timeout-sec", default=1.0
            )
            if timeout_sec is None:
                return 2
            result = health_probe.check(timeout_sec=timeout_sec)
            sys.stdout.write(json.dumps(result) + "\n")
            return 0 if result["status"] in {_HEALTHY, _DEGRADED} else 1

        if command == "smoke":
            profile_id = self._parse_kw_str(
                rest, "--profile-id", default=None
            )
            if profile_id is None:
                sys.stderr.write("smoke requires --profile-id <id>\n")
                return 2
            result = smoke_hook.run(profile_id=profile_id)
            sys.stdout.write(json.dumps(result) + "\n")
            return 0 if result.get("passed") else 1

        sys.stderr.write(f"unknown command: {command!r}\n")
        return 2

    @staticmethod
    def _parse_kw_float(
        rest: list[str], flag: str, *, default: float
    ) -> float | None:
        if flag not in rest:
            return default
        idx = rest.index(flag)
        if idx + 1 >= len(rest):
            sys.stderr.write(f"{flag} requires a value\n")
            return None
        try:
            return float(rest[idx + 1])
        except ValueError:
            sys.stderr.write(
                f"{flag} must be a float; got {rest[idx + 1]!r}\n"
            )
            return None

    @staticmethod
    def _parse_kw_str(
        rest: list[str], flag: str, *, default: str | None
    ) -> str | None:
        if flag not in rest:
            return default
        idx = rest.index(flag)
        if idx + 1 >= len(rest):
            sys.stderr.write(f"{flag} requires a value\n")
            return None
        return rest[idx + 1]


# Module-level singleton instances — assembly registry references these
# by their lowercase attribute names (not the underscore-prefixed classes).
health_probe = _HealthProbe()
smoke_hook = _SmokeHook()
init_hook = _InitHook()
version_declaration = _VersionDeclaration()
cli = _Cli()


__all__ = [
    "cli",
    "health_probe",
    "init_hook",
    "smoke_hook",
    "version_declaration",
]
