from __future__ import annotations

import pytest
from pydantic import ValidationError

from subsystem_announcement.extract.evidence import (
    EvidenceSpan,
    build_evidence_span,
    build_table_evidence_span,
    evidence_matches_artifact,
    quote_from_artifact,
)
from subsystem_announcement.parse.artifact import AnnouncementTable

from .extract_fixtures import make_artifact


def test_build_evidence_span_reconstructs_section_quote() -> None:
    artifact = make_artifact("公司签订重大合同，合同金额为1000万元。")
    section = artifact.sections[0]
    start = section.text.index("重大合同")
    end = start + len("重大合同")

    span = build_evidence_span(section, start, end)

    assert span.quote == "重大合同"
    assert quote_from_artifact(artifact, span) == "重大合同"
    assert evidence_matches_artifact(artifact, span)


def test_evidence_span_rejects_quote_offset_mismatch() -> None:
    with pytest.raises(ValidationError, match="quote length"):
        EvidenceSpan(
            section_id="sec-0001",
            start_offset=0,
            end_offset=3,
            quote="重大合同",
            table_ref=None,
        )


def test_build_table_evidence_span_reconstructs_table_quote() -> None:
    section_text = "公司签订重大合同。"
    table_text = "项目\t金额\n合同\t1000万元"
    table_start = len(section_text) + 2
    table = AnnouncementTable(
        table_id="tbl-0001",
        section_id="sec-0001",
        caption=None,
        headers=["项目", "金额"],
        rows=[["合同", "1000万元"]],
        start_offset=table_start,
        end_offset=table_start + len(table_text),
    )
    artifact = make_artifact(section_text, tables=[table])
    start = table_text.index("1000万元")
    end = start + len("1000万元")

    span = build_table_evidence_span(table, start, end)

    assert span.table_ref == "tbl-0001"
    assert quote_from_artifact(artifact, span) == "1000万元"
    assert evidence_matches_artifact(artifact, span)
