"""Content hashing and file-backed dedupe state for discovery."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from .document import AnnouncementDocumentArtifact
from .errors import DocumentCacheError


def compute_content_hash(content: bytes) -> str:
    """Return a stable sha256 hex digest for document bytes."""

    return hashlib.sha256(content).hexdigest()


class AnnouncementDedupeStore:
    """Small JSON index keyed by announcement id and content hash."""

    def __init__(self, artifact_root: Path) -> None:
        self.artifact_root = Path(artifact_root)
        self.index_path = self.artifact_root / "documents" / ".dedupe_index.json"

    def find_by_announcement_id(
        self,
        announcement_id: str,
    ) -> AnnouncementDocumentArtifact | None:
        """Return a previously recorded artifact for an announcement id."""

        metadata_path = self._read_index()["announcement_id"].get(announcement_id)
        if metadata_path is None:
            return None
        return self._load_artifact(Path(metadata_path))

    def find_by_content_hash(
        self,
        content_hash: str,
    ) -> AnnouncementDocumentArtifact | None:
        """Return a previously recorded artifact for identical bytes."""

        metadata_path = self._read_index()["content_hash"].get(content_hash)
        if metadata_path is None:
            return None
        return self._load_artifact(Path(metadata_path))

    def record(
        self,
        artifact: AnnouncementDocumentArtifact,
        *,
        announcement_id: str | None = None,
    ) -> None:
        """Record an artifact under its hash and an announcement id."""

        index = self._read_index()
        metadata_path = _metadata_path_for_document(artifact.local_path)
        metadata_path_text = str(metadata_path)
        index["announcement_id"][announcement_id or artifact.announcement_id] = (
            metadata_path_text
        )
        index["content_hash"].setdefault(artifact.content_hash, metadata_path_text)
        self._write_index(index)

    def _read_index(self) -> dict[str, dict[str, str]]:
        if not self.index_path.exists():
            return {"announcement_id": {}, "content_hash": {}}
        try:
            raw_index = json.loads(self.index_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise DocumentCacheError(
                "Unable to read discovery dedupe index: "
                f"announcement_id=unknown path={self.index_path}"
            ) from exc
        return {
            "announcement_id": dict(raw_index.get("announcement_id", {})),
            "content_hash": dict(raw_index.get("content_hash", {})),
        }

    def _write_index(self, index: dict[str, dict[str, str]]) -> None:
        try:
            self.index_path.parent.mkdir(parents=True, exist_ok=True)
            temp_path = self.index_path.with_suffix(".tmp")
            temp_path.write_text(
                json.dumps(index, ensure_ascii=False, indent=2, sort_keys=True),
                encoding="utf-8",
            )
            temp_path.replace(self.index_path)
        except OSError as exc:
            raise DocumentCacheError(
                "Unable to write discovery dedupe index: "
                f"announcement_id=unknown path={self.index_path}"
            ) from exc

    def _load_artifact(self, metadata_path: Path) -> AnnouncementDocumentArtifact:
        try:
            return AnnouncementDocumentArtifact.model_validate_json(
                metadata_path.read_text(encoding="utf-8")
            )
        except (OSError, ValueError) as exc:
            raise DocumentCacheError(
                "Unable to load dedupe artifact metadata: "
                f"announcement_id=unknown path={metadata_path}"
            ) from exc


def _metadata_path_for_document(local_path: Path) -> Path:
    return local_path.with_name(f"{local_path.stem}.metadata.json")
