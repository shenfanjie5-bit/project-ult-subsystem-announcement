"""Local document cache for official announcement bytes."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit

from subsystem_announcement.config import AnnouncementConfig

from .dedupe import compute_content_hash
from .document import AnnouncementDocumentArtifact
from .envelope import AnnouncementEnvelope
from .errors import DocumentCacheError


class AnnouncementDocumentCache:
    """Write and load official document bytes and sidecar metadata."""

    def __init__(self, config: AnnouncementConfig) -> None:
        self.artifact_root = Path(config.artifact_root)

    def put(
        self,
        envelope: AnnouncementEnvelope,
        content: bytes,
        content_type: str | None = None,
    ) -> AnnouncementDocumentArtifact:
        """Cache document bytes and return replayable artifact metadata."""

        content_hash = compute_content_hash(content)
        document_path = self._document_path(envelope, content_hash)
        metadata_path = _metadata_path_for_document(document_path)
        artifact = AnnouncementDocumentArtifact(
            announcement_id=envelope.announcement_id,
            ts_code=envelope.ts_code,
            title=envelope.title,
            publish_time=envelope.publish_time,
            content_hash=content_hash,
            official_url=envelope.official_url,
            source_exchange=envelope.source_exchange,
            attachment_type=envelope.attachment_type,
            local_path=document_path,
            content_type=content_type or _default_content_type(envelope),
            byte_size=len(content),
            fetched_at=datetime.now(timezone.utc),
        )
        self._ensure_under_artifact_root(document_path, envelope.announcement_id)

        try:
            document_path.parent.mkdir(parents=True, exist_ok=True)
            if not document_path.exists():
                document_path.write_bytes(content)
            metadata_path.write_text(
                artifact.model_dump_json(indent=2),
                encoding="utf-8",
            )
        except OSError as exc:
            raise DocumentCacheError(
                "Unable to cache announcement document: "
                f"announcement_id={envelope.announcement_id} path={document_path}"
            ) from exc
        return artifact

    def load(self, path: Path) -> AnnouncementDocumentArtifact:
        """Load artifact metadata from a JSON path or document path."""

        return load_document_artifact(path)

    def _document_path(
        self,
        envelope: AnnouncementEnvelope,
        content_hash: str,
    ) -> Path:
        suffix = _suffix_for_attachment(envelope)
        return (
            self.artifact_root
            / "documents"
            / envelope.source_exchange
            / envelope.announcement_id
            / f"{content_hash}.{suffix}"
        )

    def _ensure_under_artifact_root(
        self,
        document_path: Path,
        announcement_id: str,
    ) -> None:
        try:
            document_path.resolve().relative_to(self.artifact_root.resolve())
        except ValueError as exc:
            raise DocumentCacheError(
                "Document cache path escaped artifact_root: "
                f"announcement_id={announcement_id} path={document_path} "
                f"artifact_root={self.artifact_root}"
            ) from exc


def load_document_artifact(path: Path) -> AnnouncementDocumentArtifact:
    """Load a cached document artifact for parse/replay reuse."""

    metadata_path = path if path.suffix == ".json" else _metadata_path_for_document(path)
    try:
        return AnnouncementDocumentArtifact.model_validate_json(
            metadata_path.read_text(encoding="utf-8")
        )
    except (OSError, ValueError) as exc:
        raise DocumentCacheError(
            "Unable to load announcement document metadata: "
            f"announcement_id=unknown path={metadata_path}"
        ) from exc


def _metadata_path_for_document(local_path: Path) -> Path:
    return local_path.with_name(f"{local_path.stem}.metadata.json")


def _suffix_for_attachment(envelope: AnnouncementEnvelope) -> str:
    if envelope.attachment_type in {"pdf", "html"}:
        return envelope.attachment_type
    url_path = urlsplit(str(envelope.official_url)).path.lower()
    if url_path.endswith(".doc"):
        return "doc"
    return "docx"


def _default_content_type(envelope: AnnouncementEnvelope) -> str:
    if envelope.attachment_type == "pdf":
        return "application/pdf"
    if envelope.attachment_type == "html":
        return "text/html"
    return "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
