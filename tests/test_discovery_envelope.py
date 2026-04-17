from __future__ import annotations

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from subsystem_announcement.discovery.envelope import AnnouncementEnvelope


def _envelope_data() -> dict[str, object]:
    return {
        "announcement_id": "ann-1",
        "ts_code": "600000.SH",
        "title": "重大合同公告",
        "publish_time": datetime(2026, 4, 18, 9, 30, tzinfo=timezone.utc),
        "official_url": "https://static.sse.com.cn/disclosure/ann-1.pdf",
        "source_exchange": "sse",
        "attachment_type": "pdf",
    }


def test_announcement_envelope_accepts_required_fields_and_timezone() -> None:
    envelope = AnnouncementEnvelope.model_validate(_envelope_data())

    assert envelope.announcement_id == "ann-1"
    assert envelope.publish_time.tzinfo is not None
    assert envelope.publish_time.utcoffset() is not None


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("announcement_id", ""),
        ("title", ""),
        ("attachment_type", "image"),
    ],
)
def test_announcement_envelope_rejects_invalid_required_fields(
    field: str,
    value: object,
) -> None:
    data = _envelope_data()
    data[field] = value

    with pytest.raises(ValidationError):
        AnnouncementEnvelope.model_validate(data)


def test_announcement_envelope_rejects_extra_fields() -> None:
    data = _envelope_data()
    data["ingest_metadata"] = {"source": "crawler"}

    with pytest.raises(ValidationError):
        AnnouncementEnvelope.model_validate(data)


def test_announcement_envelope_rejects_naive_publish_time() -> None:
    data = _envelope_data()
    data["publish_time"] = datetime(2026, 4, 18, 9, 30)

    with pytest.raises(ValidationError, match="timezone"):
        AnnouncementEnvelope.model_validate(data)
