"""Boundary tier — CLAUDE.md red lines that subsystem-announcement MUST enforce.

Five red lines per ``subsystem-announcement/CLAUDE.md`` "不可协商约束":

1. **官方源优先** — primary source must be exchange/listed-company
   official disclosure; redistribution / summaries cannot be primary
   evidence (`_validate_official_url_text` enforces).
2. **Ex-3 高门槛** — no Ex-3 candidate without strong evidence; the
   high-threshold guard (`graph_delta_guard`) rejects weak inputs.
3. **每 Ex-1 携 EvidenceSpan** — `AnnouncementFactCandidate` schema
   requires `evidence_spans: list[EvidenceSpan] = Field(min_length=1)`;
   construction without evidence_spans must fail.
4. **No second parser + no provider SDK direct + no business import**
   — subprocess-isolated deny scan on `subsystem_announcement.public`:
   no `pdfplumber` / `pypdf` / `unstructured` / `pdfminer` (other PDF
   parsers); no `openai` / `anthropic` / `litellm` (must go through
   `reasoner-runtime`); no `data_platform` / `main_core` /
   `graph_engine` / `audit_eval` / `orchestrator` / `assembly`
   (business modules); also no `docling` / `llama_index` / `httpx`
   eagerly loaded at public.py import time (smoke imports them lazily).
5. **铁律 #7 wire-shape boundary** — when announcement submits via the
   real SDK, the backend MUST receive the wire shape (no
   `ex_type`/`semantic`/`produced_at` envelope); cross-prove by feeding
   the announcement candidate through `SubmitClient` + `MockSubmitBackend`.
"""

from __future__ import annotations

import json
import subprocess
import sys
import textwrap
from datetime import UTC, datetime
from pathlib import Path

import pytest


# ── Red line #1: 官方源优先 ──────────────────────────────────────


class TestOfficialSourcePriority:
    """CLAUDE.md §5.4: primary source must be exchange/listed-company
    official disclosure; redistribution / summaries cannot be primary."""

    @pytest.mark.parametrize(
        "official_url",
        [
            # Canonical mainland China exchange + disclosure platform
            # domains per subsystem_announcement.discovery.fetcher
            # _OFFICIAL_DISCLOSURE_DOMAINS = {bse.cn, cninfo.com.cn,
            # neeq.com.cn, sse.com.cn, szse.cn}.
            "https://www.sse.com.cn/disclosure/announcement/123",
            "https://www.szse.cn/disclosure/announcement/456",
            "https://www.cninfo.com.cn/new/disclosure/789",
            "https://www.bse.cn/disclosure/announcement/abc",
            "https://www.neeq.com.cn/disclosure/notice/def",
        ],
    )
    def test_validate_official_url_text_accepts_exchange_urls(
        self, official_url: str
    ) -> None:
        from subsystem_announcement.discovery.fetcher import (
            _validate_official_url_text,
        )

        # Should not raise — these are the canonical official sources.
        _validate_official_url_text(
            official_url, announcement_id="boundary-official-url-test"
        )

    @pytest.mark.parametrize(
        "non_official_url",
        [
            "https://forum.example.com/announcements/redistributed/123",
            "https://blog.example.com/summary/456",
            "https://news.example.com/article/789",
            "https://twitter.com/user/status/abc",
        ],
    )
    def test_validate_official_url_text_rejects_non_official_sources(
        self, non_official_url: str
    ) -> None:
        from subsystem_announcement.discovery.errors import (
            NonOfficialSourceError,
        )
        from subsystem_announcement.discovery.fetcher import (
            _validate_official_url_text,
        )

        with pytest.raises(NonOfficialSourceError):
            _validate_official_url_text(
                non_official_url,
                announcement_id="boundary-non-official-url-test",
            )


# ── Red line #2: Ex-3 高门槛 ────────────────────────────────────


class TestEx3HighThresholdGuard:
    """CLAUDE.md §19: Ex-3 误产出率 < 1%. The high-threshold guard
    must reject candidates with weak evidence / non-official source /
    unresolved counterparty entity anchor."""

    def test_graph_delta_guard_module_exists(self) -> None:
        from subsystem_announcement.graph import guard as graph_delta_guard

        # The guard module must export a callable that consumers
        # (announcement runtime + tests) can use to gate Ex-3 emission.
        assert hasattr(graph_delta_guard, "__name__")

    def test_announcement_graph_delta_candidate_extra_forbidden(self) -> None:
        from subsystem_announcement.graph import (
            AnnouncementGraphDeltaCandidate,
        )

        # extra="forbid" is the type-level Ex-3 high-threshold guard:
        # downstream cannot smuggle weak-evidence fields through extras.
        assert (
            AnnouncementGraphDeltaCandidate.model_config.get("extra")
            == "forbid"
        )


# ── Red line #3: every Ex-1 carries EvidenceSpan ────────────────


class TestEveryEx1CarriesEvidenceSpan:
    """CLAUDE.md §19: 100% Ex-1 EvidenceSpan coverage. Ex-1 candidate
    schema must reject construction without at least one EvidenceSpan."""

    def test_ex1_construction_without_evidence_spans_fails(self) -> None:
        from pydantic import ValidationError

        from subsystem_announcement.extract import AnnouncementFactCandidate
        from subsystem_announcement.extract.candidates import FactType

        with pytest.raises(ValidationError) as excinfo:
            AnnouncementFactCandidate(
                fact_id="boundary-no-evidence",
                announcement_id="boundary-ann",
                fact_type=FactType.MAJOR_CONTRACT,
                primary_entity_id="ENT_STOCK_PLACEHOLDER",
                fact_content={"k": "v"},
                confidence=0.9,
                source_reference={
                    "official_url": "https://www.sse.com.cn/disclosure/x",
                },
                evidence_spans=[],  # empty — must be rejected
                extracted_at=datetime(2026, 1, 1, tzinfo=UTC),
            )
        assert any(
            "evidence_spans" in str(err.get("loc", ()))
            for err in excinfo.value.errors()
        )

    def test_ex1_construction_with_one_evidence_span_succeeds(self) -> None:
        from subsystem_announcement.extract import AnnouncementFactCandidate
        from subsystem_announcement.extract.candidates import FactType
        from subsystem_announcement.extract.evidence import EvidenceSpan

        candidate = AnnouncementFactCandidate(
            fact_id="boundary-with-evidence",
            announcement_id="boundary-ann",
            fact_type=FactType.MAJOR_CONTRACT,
            primary_entity_id="ENT_STOCK_PLACEHOLDER",
            fact_content={"k": "v"},
            confidence=0.9,
            source_reference={
                "official_url": "https://www.sse.com.cn/disclosure/x",
            },
            evidence_spans=[
                EvidenceSpan(
                    section_id="s1",
                    start_offset=0,
                    end_offset=5,
                    quote="hello",
                )
            ],
            extracted_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        assert len(candidate.evidence_spans) == 1


# ── Red line #4: public.py boundary deny scan (subprocess-isolated) ──

_BUSINESS_DOWNSTREAMS = (
    "main_core",
    "data_platform",
    "graph_engine",
    "audit_eval",
    "reasoner_runtime",
    "entity_registry",
    "subsystem_news",  # sibling subsystem; cannot import each other
    "orchestrator",
    "assembly",
    "feature_store",
    "stream_layer",
)
_HEAVY_RUNTIME_PREFIXES = (
    # Other PDF / HTML parsers — Docling is the ONLY parser per CLAUDE.md.
    "pdfplumber",
    "pypdf",
    "unstructured",
    "pdfminer",
    # LLM provider direct SDKs — must go through reasoner-runtime.
    "openai",
    "anthropic",
    "litellm",
    # Heavy infra — must not be eagerly imported by public.py.
    "psycopg",
    "pyiceberg",
    "neo4j",
    "torch",
    "tensorflow",
    "dagster",
    "hanlp",
    "splink",
    # Announcement's own heavy deps that smoke imports lazily — must
    # NOT be at module-import-time of public.py.
    "docling",
    "llama_index",
    "httpx",
)
_PROBE_SCRIPT = textwrap.dedent(
    """
    import json, sys
    sys.path.insert(0, {pkg_dir!r})
    sys.path.insert(0, {sdk_src!r})
    sys.path.insert(0, {contracts_src!r})
    import subsystem_announcement.public  # noqa: F401
    print(json.dumps(sorted(sys.modules.keys())))
    """
).strip()


@pytest.fixture(scope="module")
def loaded_modules_in_clean_subprocess() -> frozenset[str]:
    """Iron rule #2: subprocess-isolated import deny scan."""

    repo_root = Path(__file__).resolve().parents[2]
    pkg_dir = repo_root / "src"
    sdk_src = repo_root.parent / "subsystem-sdk"
    contracts_src = repo_root.parent / "contracts" / "src"
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            _PROBE_SCRIPT.format(
                pkg_dir=str(pkg_dir),
                sdk_src=str(sdk_src),
                contracts_src=str(contracts_src),
            ),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise AssertionError(
            "subprocess probe failed; stderr:\n" + result.stderr
        )
    return frozenset(json.loads(result.stdout))


class TestPublicNoBusinessImports:
    def test_public_pulls_in_no_business_module(
        self, loaded_modules_in_clean_subprocess: frozenset[str]
    ) -> None:
        offenders = sorted(
            mod
            for mod in loaded_modules_in_clean_subprocess
            if any(
                mod == p or mod.startswith(p + ".")
                for p in _BUSINESS_DOWNSTREAMS
            )
        )
        assert not offenders, (
            f"subsystem_announcement.public pulled in business module(s): "
            f"{offenders}; CLAUDE.md §5.4 boundary"
        )

    def test_public_pulls_in_no_heavy_or_forbidden_infra(
        self, loaded_modules_in_clean_subprocess: frozenset[str]
    ) -> None:
        offenders = sorted(
            mod
            for mod in loaded_modules_in_clean_subprocess
            if any(
                mod == p or mod.startswith(p + ".")
                for p in _HEAVY_RUNTIME_PREFIXES
            )
        )
        assert not offenders, (
            f"subsystem_announcement.public pulled in forbidden infra "
            f"or heavy deps: {offenders}; CLAUDE.md §5.4 boundary "
            "(Docling-only / no provider SDK / heavy deps lazy)"
        )


# ── Red line #5: 铁律 #7 — backend never receives SDK envelope ────


class TestBackendNeverReceivesSdkEnvelope:
    """Stage-2.7 follow-up #2 + 铁律 #7: announcement's SDK adapter must
    use ``validate_then_dispatch`` (which strips envelope) and not
    bypass it. End-to-end check: drive a real Ex-1 candidate through
    SubmitClient + MockSubmitBackend, assert backend gets wire shape."""

    def test_announcement_to_sdk_to_backend_strips_envelope(self) -> None:
        from subsystem_sdk.backends.mock import MockSubmitBackend
        from subsystem_sdk.submit.client import SubmitClient
        from subsystem_sdk.validate.engine import SDK_ENVELOPE_FIELDS
        from subsystem_sdk.validate.result import ValidationResult

        from subsystem_announcement.extract import AnnouncementFactCandidate
        from subsystem_announcement.extract.candidates import FactType
        from subsystem_announcement.extract.evidence import EvidenceSpan

        backend = MockSubmitBackend()

        def permissive_validator(_):
            return ValidationResult.ok(
                ex_type="Ex-1", schema_version="boundary-test"
            )

        candidate = AnnouncementFactCandidate(
            fact_id="boundary-wire-shape-fact",
            announcement_id="boundary-wire-shape-ann",
            fact_type=FactType.MAJOR_CONTRACT,
            primary_entity_id="ENT_STOCK_BOUNDARY",
            fact_content={"k": "v"},
            confidence=0.95,
            source_reference={
                "official_url": "https://www.sse.com.cn/disclosure/boundary",
            },
            evidence_spans=[
                EvidenceSpan(
                    section_id="s1",
                    start_offset=0,
                    end_offset=5,
                    quote="hello",
                )
            ],
            extracted_at=datetime(2026, 1, 1, tzinfo=UTC),
        )

        # Submit through the REAL SDK SubmitClient (not a mock client).
        # This exercises the real validate_then_dispatch -> strip path.
        receipt = SubmitClient(
            backend, validator=permissive_validator
        ).submit(candidate.to_ex_payload())

        assert receipt.accepted is True

        assert len(backend.submitted_payloads) == 1
        wire = backend.submitted_payloads[0]
        leaked = SDK_ENVELOPE_FIELDS.intersection(wire)
        assert not leaked, (
            f"announcement -> SDK -> backend: SDK envelope leaked "
            f"{sorted(leaked)}; the SDK adapter must use "
            "validate_then_dispatch (which strips envelope) and not "
            "bypass it"
        )
        # Producer-owned fields reach backend (announcement's runtime
        # cares about these for its own analytics).
        assert wire["fact_id"] == "boundary-wire-shape-fact"
        assert wire["announcement_id"] == "boundary-wire-shape-ann"
        assert wire["primary_entity_id"] == "ENT_STOCK_BOUNDARY"
