"""Content hashing and file-backed dedupe state for discovery."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from fcntl import LOCK_EX, LOCK_UN, flock
from pathlib import Path
from typing import Literal

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
        self.lock_path = self.artifact_root / "documents" / ".dedupe_index.lock"

    def find_by_announcement_id(
        self,
        announcement_id: str,
    ) -> AnnouncementDocumentArtifact | None:
        """Return a previously recorded artifact for an announcement id."""

        metadata_path_text = self._read_index()["announcement_id"].get(announcement_id)
        if metadata_path_text is None:
            return None
        return self._load_artifact(self._metadata_path_from_index(metadata_path_text))

    def find_by_content_hash(
        self,
        content_hash: str,
    ) -> AnnouncementDocumentArtifact | None:
        """Return a previously recorded artifact for identical bytes."""

        metadata_path_text = self._read_index()["content_hash"].get(content_hash)
        if metadata_path_text is None:
            return None
        return self._load_artifact(self._metadata_path_from_index(metadata_path_text))

    def record(
        self,
        artifact: AnnouncementDocumentArtifact,
        *,
        announcement_id: str | None = None,
    ) -> None:
        """Record an artifact under its hash and an announcement id."""

        target_announcement_id = announcement_id or artifact.announcement_id
        metadata_path = _metadata_path_for_document(artifact.local_path)
        metadata_path_text = self._metadata_path_to_index(metadata_path)

        with self._exclusive_lock():
            index = self._read_index()
            existing_for_id = index["announcement_id"].get(target_announcement_id)
            if existing_for_id is not None:
                existing_artifact = self._load_artifact(
                    self._metadata_path_from_index(existing_for_id)
                )
                if existing_artifact.content_hash == artifact.content_hash:
                    indexed_hash_path = index["content_hash"].get(
                        artifact.content_hash
                    )
                    if indexed_hash_path != existing_for_id:
                        index["content_hash"][artifact.content_hash] = existing_for_id
                        self._write_index(index)
                    return
                raise DocumentCacheError(
                    "Conflicting announcement document content: "
                    f"announcement_id={target_announcement_id} "
                    f"existing_hash={existing_artifact.content_hash} "
                    f"new_hash={artifact.content_hash}"
                )

            canonical_metadata_path = index["content_hash"].setdefault(
                artifact.content_hash,
                metadata_path_text,
            )
            index["announcement_id"][target_announcement_id] = canonical_metadata_path
            self._write_index(index)

    def resolve_or_record(
        self,
        *,
        announcement_id: str,
        content_hash: str,
        create_artifact: Callable[[], AnnouncementDocumentArtifact],
    ) -> tuple[Literal["fetched", "duplicate"], AnnouncementDocumentArtifact]:
        """Atomically resolve duplicate state or create and index a new artifact."""

        with self._exclusive_lock():
            index = self._read_index()
            existing_for_id = index["announcement_id"].get(announcement_id)
            if existing_for_id is not None:
                existing_artifact = self._load_artifact(
                    self._metadata_path_from_index(existing_for_id)
                )
                if existing_artifact.content_hash != content_hash:
                    raise DocumentCacheError(
                        "Conflicting announcement document content: "
                        f"announcement_id={announcement_id} "
                        f"existing_hash={existing_artifact.content_hash} "
                        f"new_hash={content_hash}"
                    )
                if index["content_hash"].get(content_hash) != existing_for_id:
                    index["content_hash"][content_hash] = existing_for_id
                    self._write_index(index)
                return "duplicate", existing_artifact

            existing_for_hash = index["content_hash"].get(content_hash)
            if existing_for_hash is not None:
                existing_artifact = self._load_artifact(
                    self._metadata_path_from_index(existing_for_hash)
                )
                index["announcement_id"][announcement_id] = existing_for_hash
                self._write_index(index)
                return "duplicate", existing_artifact

            artifact = create_artifact()
            if artifact.announcement_id != announcement_id:
                raise DocumentCacheError(
                    "Cached artifact announcement_id mismatch: "
                    f"announcement_id={announcement_id} "
                    f"artifact_announcement_id={artifact.announcement_id}"
                )
            if artifact.content_hash != content_hash:
                raise DocumentCacheError(
                    "Cached artifact content_hash mismatch: "
                    f"announcement_id={announcement_id} "
                    f"expected_hash={content_hash} actual_hash={artifact.content_hash}"
                )
            metadata_path_text = self._metadata_path_to_index(
                _metadata_path_for_document(artifact.local_path)
            )
            index["content_hash"][content_hash] = metadata_path_text
            index["announcement_id"][announcement_id] = metadata_path_text
            self._write_index(index)
            return "fetched", artifact

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
        temp_path: Path | None = None
        try:
            self.index_path.parent.mkdir(parents=True, exist_ok=True)
            fd, temp_path_text = tempfile.mkstemp(
                prefix=f"{self.index_path.name}.",
                suffix=".tmp",
                dir=self.index_path.parent,
            )
            temp_path = Path(temp_path_text)
            with os.fdopen(fd, "w", encoding="utf-8") as temp_file:
                json.dump(index, temp_file, ensure_ascii=False, indent=2, sort_keys=True)
                temp_file.write("\n")
            temp_path.replace(self.index_path)
        except OSError as exc:
            if temp_path is not None:
                temp_path.unlink(missing_ok=True)
            raise DocumentCacheError(
                "Unable to write discovery dedupe index: "
                f"announcement_id=unknown path={self.index_path}"
            ) from exc

    def _load_artifact(self, metadata_path: Path) -> AnnouncementDocumentArtifact:
        try:
            artifact = AnnouncementDocumentArtifact.model_validate_json(
                metadata_path.read_text(encoding="utf-8")
            )
        except (OSError, ValueError) as exc:
            raise DocumentCacheError(
                "Unable to load dedupe artifact metadata: "
                f"announcement_id=unknown path={metadata_path}"
            ) from exc
        document_path = _document_path_for_metadata(metadata_path, artifact)
        if document_path != artifact.local_path:
            return artifact.model_copy(update={"local_path": document_path})
        return artifact

    def _metadata_path_to_index(self, metadata_path: Path) -> str:
        try:
            return str(metadata_path.resolve().relative_to(self.artifact_root.resolve()))
        except ValueError as exc:
            raise DocumentCacheError(
                "Dedupe metadata path escaped artifact_root: "
                f"announcement_id=unknown path={metadata_path} "
                f"artifact_root={self.artifact_root}"
            ) from exc

    def _metadata_path_from_index(self, metadata_path_text: str) -> Path:
        metadata_path = Path(metadata_path_text)
        if metadata_path.is_absolute():
            return metadata_path
        return self.artifact_root / metadata_path

    @contextmanager
    def _exclusive_lock(self) -> Iterator[None]:
        try:
            self.lock_path.parent.mkdir(parents=True, exist_ok=True)
            with self.lock_path.open("a+") as lock_file:
                flock(lock_file.fileno(), LOCK_EX)
                try:
                    yield
                finally:
                    flock(lock_file.fileno(), LOCK_UN)
        except OSError as exc:
            raise DocumentCacheError(
                "Unable to lock discovery dedupe index: "
                f"announcement_id=unknown path={self.lock_path}"
            ) from exc


def _metadata_path_for_document(local_path: Path) -> Path:
    return local_path.with_name(f"{local_path.stem}.metadata.json")


def _document_path_for_metadata(
    metadata_path: Path,
    artifact: AnnouncementDocumentArtifact,
) -> Path:
    metadata_suffix = ".metadata.json"
    if not metadata_path.name.endswith(metadata_suffix):
        return artifact.local_path
    content_stem = metadata_path.name[: -len(metadata_suffix)]
    return metadata_path.with_name(f"{content_stem}{artifact.local_path.suffix}")
