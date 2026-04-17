from __future__ import annotations

import asyncio
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import httpx

from subsystem_announcement.config import AnnouncementConfig
from subsystem_announcement.discovery import consume_announcement_ref
from subsystem_announcement.discovery.cache import (
    AnnouncementDocumentCache,
    load_document_artifact,
)
from subsystem_announcement.discovery.dedupe import (
    AnnouncementDedupeStore,
    compute_content_hash,
)
from subsystem_announcement.discovery.envelope import AnnouncementEnvelope


def _envelope(
    announcement_id: str = "ann-1",
    url: str | None = None,
) -> AnnouncementEnvelope:
    return AnnouncementEnvelope(
        announcement_id=announcement_id,
        ts_code="600000.SH",
        title="重大合同公告",
        publish_time=datetime(2026, 4, 18, 9, 30, tzinfo=timezone.utc),
        official_url=url
        or f"https://static.sse.com.cn/disclosure/{announcement_id}.pdf",
        source_exchange="sse",
        attachment_type="pdf",
    )


def test_compute_content_hash_is_stable_and_content_sensitive() -> None:
    content = b"official document bytes"

    assert compute_content_hash(content) == compute_content_hash(content)
    assert compute_content_hash(content) != compute_content_hash(b"other bytes")


def test_document_cache_put_writes_bytes_and_round_trips_metadata(
    tmp_path: Path,
) -> None:
    config = AnnouncementConfig(artifact_root=tmp_path)
    cache = AnnouncementDocumentCache(config)

    artifact = cache.put(_envelope(), b"pdf bytes", content_type="application/pdf")
    loaded = load_document_artifact(artifact.local_path)

    assert artifact.local_path.read_bytes() == b"pdf bytes"
    assert artifact.local_path.resolve().is_relative_to(tmp_path.resolve())
    assert loaded == artifact
    assert cache.load(artifact.local_path) == artifact
    assert artifact.byte_size == len(b"pdf bytes")
    assert artifact.content_type == "application/pdf"


def test_dedupe_store_finds_recorded_artifact_by_id_and_hash(
    tmp_path: Path,
) -> None:
    config = AnnouncementConfig(artifact_root=tmp_path)
    artifact = AnnouncementDocumentCache(config).put(_envelope(), b"pdf bytes")
    store = AnnouncementDedupeStore(tmp_path)

    store.record(artifact)

    assert store.find_by_announcement_id("ann-1") == artifact
    assert store.find_by_content_hash(artifact.content_hash) == artifact


def test_dedupe_store_records_concurrent_writers_without_lost_updates(
    tmp_path: Path,
) -> None:
    config = AnnouncementConfig(artifact_root=tmp_path)
    cache = AnnouncementDocumentCache(config)
    artifacts = [
        cache.put(_envelope(f"ann-{index}"), f"pdf bytes {index}".encode())
        for index in range(24)
    ]

    def record_artifact(artifact) -> None:
        AnnouncementDedupeStore(tmp_path).record(artifact)

    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(record_artifact, artifacts))

    store = AnnouncementDedupeStore(tmp_path)
    for artifact in artifacts:
        assert store.find_by_announcement_id(artifact.announcement_id) == artifact
        assert store.find_by_content_hash(artifact.content_hash) == artifact


def test_dedupe_store_uses_relative_paths_after_artifact_root_move(
    tmp_path: Path,
) -> None:
    original_root = tmp_path / "artifacts"
    moved_root = tmp_path / "moved-artifacts"
    config = AnnouncementConfig(artifact_root=original_root)
    artifact = AnnouncementDocumentCache(config).put(_envelope(), b"pdf bytes")
    store = AnnouncementDedupeStore(original_root)

    store.record(artifact)

    index = json.loads(
        (original_root / "documents" / ".dedupe_index.json").read_text(
            encoding="utf-8"
        )
    )
    assert not Path(index["announcement_id"]["ann-1"]).is_absolute()
    assert not Path(index["content_hash"][artifact.content_hash]).is_absolute()

    original_root.rename(moved_root)
    moved_store = AnnouncementDedupeStore(moved_root)
    moved_artifact = moved_store.find_by_announcement_id("ann-1")

    assert moved_artifact is not None
    assert moved_artifact.local_path == moved_root / artifact.local_path.relative_to(
        original_root
    )
    assert moved_artifact.local_path.exists()


def test_consume_announcement_ref_dedupes_repeated_announcement_id(
    tmp_path: Path,
) -> None:
    request_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal request_count
        request_count += 1
        return httpx.Response(200, content=b"pdf bytes")

    async def scenario():
        config = AnnouncementConfig(artifact_root=tmp_path)
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
        ) as client:
            first = await consume_announcement_ref(
                _envelope("ann-1"),
                config,
                client=client,
            )
            second = await consume_announcement_ref(
                _envelope("ann-1"),
                config,
                client=client,
            )
            return first, second

    first, second = asyncio.run(scenario())
    body_files = _document_body_files(tmp_path)

    assert first.status == "fetched"
    assert second.status == "duplicate"
    assert second.document == first.document
    assert request_count == 1
    assert len(body_files) == 1


def test_consume_announcement_ref_marks_same_content_hash_duplicate(
    tmp_path: Path,
) -> None:
    request_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal request_count
        request_count += 1
        return httpx.Response(200, content=b"same pdf bytes")

    async def scenario():
        config = AnnouncementConfig(artifact_root=tmp_path)
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
        ) as client:
            first = await consume_announcement_ref(
                _envelope("ann-1"),
                config,
                client=client,
            )
            second = await consume_announcement_ref(
                _envelope("ann-2"),
                config,
                client=client,
            )
            third = await consume_announcement_ref(
                _envelope("ann-2"),
                config,
                client=client,
            )
            return first, second, third

    first, second, third = asyncio.run(scenario())
    body_files = _document_body_files(tmp_path)

    assert first.status == "fetched"
    assert second.status == "duplicate"
    assert third.status == "duplicate"
    assert second.document.local_path == first.document.local_path
    assert third.document.local_path == first.document.local_path
    assert request_count == 2
    assert len(body_files) == 1


def _document_body_files(root: Path) -> list[Path]:
    return [
        path
        for path in (root / "documents").rglob("*")
        if path.is_file() and path.suffix in {".pdf", ".html", ".doc", ".docx"}
    ]
