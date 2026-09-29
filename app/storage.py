"""Local filesystem content-addressable storage.

Layout under ``$REGISTRY_DATA_DIR``::

    blobs/sha256/<ab>/<rest>      global CAS blob store (hard-link origin)
    uploads/<uuid>.part           in-progress chunked uploads
    staging/<uuid>                single-shot request bodies pending commit

Blob content lives exactly once on disk: commits are ``os.link`` (atomic,
same-filesystem), so identical content pushed to any number of repositories
occupies one file. Inode links therefore never exceed one because blobs are
not linked again per repository — repository membership is a SQLite row.
"""
from __future__ import annotations

import errno
import os
from typing import BinaryIO, Iterator

from .digest import parse_digest, sha256_file

CHUNK_SIZE = 1024 * 1024


class BlobMissing(Exception):
    """Raised when a blob's file is absent (e.g. deleted mid-read)."""


class BlobCorrupt(Exception):
    """Raised when on-disk content does not match its claimed digest."""


class Storage:
    def __init__(self, data_dir: str) -> None:
        self.data_dir = data_dir
        self.blobs_dir = os.path.join(data_dir, "blobs", "sha256")
        self.uploads_dir = os.path.join(data_dir, "uploads")
        self.staging_dir = os.path.join(data_dir, "staging")
        for path in (self.blobs_dir, self.uploads_dir, self.staging_dir):
            os.makedirs(path, exist_ok=True)

    # ------------------------------------------------------------------ blobs

    def blob_abspath(self, digest: str) -> str:
        algo, hex_digest = parse_digest(digest)
        return os.path.join(self.blobs_dir, hex_digest[:2], hex_digest[2:])

    def blob_size(self, digest: str) -> int | None:
        try:
            return os.stat(self.blob_abspath(digest)).st_size
        except FileNotFoundError:
            return None

    def blob_exists(self, digest: str) -> bool:
        return os.path.exists(self.blob_abspath(digest))

    def open_blob(self, digest: str) -> BinaryIO:
        try:
            return open(self.blob_abspath(digest), "rb")
        except FileNotFoundError:
            raise BlobMissing(digest)

    def stream_blob(self, digest: str, offset: int = 0,
                    end: int | None = None) -> Iterator[bytes]:
        """Stream a blob. When *end* is given (inclusive), stop after that byte."""
        try:
            fh = open(self.blob_abspath(digest), "rb")
        except FileNotFoundError:
            raise BlobMissing(digest)
        with fh:
            if offset:
                fh.seek(offset)
            remaining = None if end is None else end - offset + 1
            while True:
                want = CHUNK_SIZE if remaining is None else min(CHUNK_SIZE, remaining)
                if want <= 0:
                    break
                chunk = fh.read(want)
                if not chunk:
                    break
                if remaining is not None:
                    remaining -= len(chunk)
                yield chunk

    def remove_blob(self, digest: str) -> bool:
        path = self.blob_abspath(digest)
        try:
            os.unlink(path)
            return True
        except FileNotFoundError:
            return False

    def list_blob_digests(self) -> set[str]:
        """Scan the sharded CAS directory (used by startup reconciliation)."""
        found: set[str] = set()
        for shard in sorted(os.listdir(self.blobs_dir)):
            shard_path = os.path.join(self.blobs_dir, shard)
            if not (len(shard) == 2 and os.path.isdir(shard_path)):
                continue
            for rest in os.listdir(shard_path):
                if len(rest) == 62:
                    found.add(f"sha256:{shard}{rest}")
        return found

    def commit_blob(self, source_path: str, digest: str, expected_size: int | None = None) -> bool:
        """Verify *source_path*, then atomically place it in the CAS store.

        Returns ``True`` if a new blob file was created, ``False`` if content
        with this digest already existed. The source file is always unlinked
        on success.
        """
        actual_digest, actual_size = sha256_file(source_path)
        if actual_digest != digest:
            raise BlobCorrupt(f"{digest} != {actual_digest}")
        if expected_size is not None and actual_size != expected_size:
            raise BlobCorrupt(f"size {actual_size} != {expected_size}")

        target = self.blob_abspath(digest)
        os.makedirs(os.path.dirname(target), exist_ok=True)
        created = True
        try:
            os.link(source_path, target)
            self._fsync_dir(os.path.dirname(target))
        except FileExistsError:
            created = False
        except OSError as exc:
            if exc.errno != errno.EEXIST:
                raise
            created = False
        os.unlink(source_path)
        return created

    # ---------------------------------------------------------------- staging

    def new_staging_path(self, token: str) -> str:
        return os.path.join(self.staging_dir, token)

    def discard_staging(self, path: str) -> None:
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass

    # ---------------------------------------------------------------- uploads

    def upload_path(self, uuid: str) -> str:
        return os.path.join(self.uploads_dir, f"{uuid}.part")

    def init_upload(self, uuid: str) -> None:
        with open(self.upload_path(uuid), "wb"):
            pass
        self._fsync_dir(self.uploads_dir)

    def upload_size(self, uuid: str) -> int:
        try:
            return os.stat(self.upload_path(uuid)).st_size
        except FileNotFoundError:
            return 0

    def upload_exists(self, uuid: str) -> bool:
        return os.path.exists(self.upload_path(uuid))

    def truncate_upload(self, uuid: str, size: int) -> None:
        """Drop any bytes beyond the last acknowledged offset (crash recovery)."""
        path = self.upload_path(uuid)
        if not os.path.exists(path):
            self.init_upload(uuid)
            return
        if size == 0:
            with open(path, "wb"):
                pass
        else:
            with open(path, "r+b") as fh:
                fh.truncate(size)

    def remove_upload(self, uuid: str) -> None:
        try:
            os.unlink(self.upload_path(uuid))
        except FileNotFoundError:
            pass

    async def append_async(self, uuid: str, stream) -> int:
        """Append an async stream (IterStream) to the upload file; fsync.

        Returns the new file size. Synchronous local writes are awaited
        cooperatively via the stream; fsync guarantees every acknowledged byte
        survives a restart.
        """
        path = self.upload_path(uuid)
        with open(path, "ab") as fh:
            while True:
                chunk = await stream.read(CHUNK_SIZE)
                if not chunk:
                    break
                fh.write(chunk)
            fh.flush()
            os.fsync(fh.fileno())
        return os.stat(path).st_size

    def list_upload_files(self) -> set[str]:
        result: set[str] = set()
        for name in os.listdir(self.uploads_dir):
            if name.endswith(".part"):
                result.add(name[: -len(".part")])
        return result

    # ----------------------------------------------------------------- helpers

    @staticmethod
    def _fsync_dir(path: str) -> None:
        try:
            fd = os.open(path, os.O_RDONLY)
        except OSError:
            return
        try:
            os.fsync(fd)
        except OSError:
            pass
        finally:
            os.close(fd)
