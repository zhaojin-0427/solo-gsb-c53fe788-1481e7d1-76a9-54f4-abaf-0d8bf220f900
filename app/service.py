"""Registry core logic: upload sessions, CAS commits, manifest CAS publish,
garbage collection reachability graph and crash recovery.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import threading
import time
import uuid as uuidlib
from typing import Any

from . import manifests as manifest_mod
from .config import Settings
from .db import Database
from .digest import is_digest, is_tag, parse_digest, sha256_file, valid_repo
from .errors import (
    OCIError,
    digest_invalid,
    gc_running,
    manifest_invalid,
    manifest_unknown,
    name_invalid,
    plan_stale,
    plan_unknown,
    range_invalid,
    size_invalid,
    tag_conflict,
    upload_unknown,
)
from .storage import BlobCorrupt, Storage

log = logging.getLogger("registry")

_RANGE_RE = re.compile(r"^bytes=(\d+)-(\d*)$")
_CONTENT_RANGE_RE = re.compile(r"^bytes (\d+)-(\d+)/(\d+|\*)$")


def _now() -> float:
    return time.time()


def _http_iso(ts: float) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))


class IterStream:
    """Adapt a Starlette async body chunk iterator to an async binary stream."""

    def __init__(self, iterator) -> None:
        self._iter = iterator.__aiter__()
        self._buf = b""
        self._exhausted = False

    async def read(self, size: int = -1) -> bytes:
        chunks: list[bytes] = []
        remaining = size if size is not None and size >= 0 else None
        while remaining is None or remaining > 0:
            if self._buf:
                if remaining is None:
                    chunks.append(self._buf)
                    self._buf = b""
                else:
                    take = min(remaining, len(self._buf))
                    chunks.append(self._buf[:take])
                    self._buf = self._buf[take:]
                    remaining -= take
                continue
            if self._exhausted:
                break
            try:
                self._buf = await self._iter.__anext__()
            except StopAsyncIteration:
                self._exhausted = True
                break
        return b"".join(chunks)


class UploadResult:
    def __init__(self, kind: str, **data: Any) -> None:
        self.kind = kind  # "session" | "created"
        self.data = data


class RegistryService:
    def __init__(self, settings: Settings, db: Database, storage: Storage) -> None:
        self.settings = settings
        self.db = db
        self.storage = storage
        self._upload_locks_guard = threading.Lock()
        self._upload_locks: dict[str, threading.Lock] = {}
        self._gc_lock = threading.Lock()

    # =============================================================== helpers

    def require_repo(self, repo: str) -> None:
        if not valid_repo(repo):
            raise name_invalid(repo)

    def _upload_lock(self, uuid: str) -> threading.Lock:
        with self._upload_locks_guard:
            lock = self._upload_locks.get(uuid)
            if lock is None:
                # RLock because PUT finalize may carry a final chunk and
                # re-enters append_upload while already holding the lock.
                lock = threading.RLock()
                self._upload_locks[uuid] = lock
            return lock

    def _expiry(self) -> float:
        return _now() + self.settings.upload_ttl_seconds

    def _load_upload(self, conn, repo: str, uuid: str):
        row = conn.execute(
            "SELECT * FROM uploads WHERE uuid = ? AND repo = ?", (uuid, repo)
        ).fetchone()
        if row is None:
            return None
        return row

    def _require_live_upload(self, conn, repo: str, uuid: str):
        """Fetch an upload usable for PATCH/PUT; lazily expire stale sessions."""
        row = self._load_upload(conn, repo, uuid)
        if row is None:
            raise upload_unknown(uuid)
        if row["status"] != "uploading" or row["expires_at"] <= _now():
            # Consumed (finalized) or expired: remove lazily.
            conn.execute("DELETE FROM uploads WHERE uuid = ?", (uuid,))
            self.storage.remove_upload(uuid)
            raise upload_unknown(uuid)
        return row

    # ======================================================= upload: initiate

    async def create_upload(
        self,
        repo: str,
        body: IterStream | None = None,
        mount: str | None = None,
        from_repo: str | None = None,
        digest: str | None = None,
    ) -> UploadResult:
        """POST /v2/{repo}/blobs/uploads/

        Handles three modes: cross/global content mount, monolithic single
        request upload, and plain (optionally seeded) chunked session.
        """
        self.require_repo(repo)
        now = _now()

        # --- cross-mount (or global dedup) ---------------------------------
        if mount:
            try:
                parse_digest(mount)
            except ValueError:
                raise digest_invalid(mount, "mount digest invalid")
            if self._blob_available(mount, from_repo):
                self._link_membership(repo, mount)
                return UploadResult("created", repo=repo, digest=mount)
            # Mount source unavailable: fall through to a normal session and
            # echo the source repository on the Location for retry.

        new_uuid = uuidlib.uuid4().hex
        self.storage.init_upload(new_uuid)
        offset = 0
        has_body = False
        expires = self._expiry()

        if body is not None:
            first = await body.read(1024 * 1024)
            if first:
                has_body = True
                with open(self.storage.upload_path(new_uuid), "ab") as fh:
                    fh.write(first)
                    while True:
                        chunk = await body.read(1024 * 1024)
                        if not chunk:
                            break
                        fh.write(chunk)
                    fh.flush()
                    os.fsync(fh.fileno())
                offset = self.storage.upload_size(new_uuid)

        with self.db.tx(immediate=True) as conn:
            conn.execute(
                "INSERT INTO uploads(uuid, repo, status, offset, updated_at, expires_at) "
                "VALUES (?, ?, 'uploading', ?, ?, ?)",
                (new_uuid, repo, offset, now, expires),
            )

        # Monolithic upload with digest: verify and finalize immediately.
        if has_body and digest:
            try:
                parse_digest(digest)
                self._commit_upload(repo, new_uuid, digest)
            except BlobCorrupt as exc:
                raise digest_invalid(digest, str(exc))
            return UploadResult("created", repo=repo, digest=digest)

        return UploadResult(
            "session",
            repo=repo,
            uuid=new_uuid,
            offset=offset,
            expires_at=expires,
            from_repo=from_repo if mount else None,
        )

    def _blob_available(self, digest: str, from_repo: str | None) -> bool:
        with self.db.ro() as conn:
            if not self.storage.blob_exists(digest):
                return False
            if from_repo:
                row = conn.execute(
                    "SELECT 1 FROM repo_blobs WHERE repo = ? AND digest = ?",
                    (from_repo, digest),
                ).fetchone()
                return row is not None
            return conn.execute(
                "SELECT 1 FROM blobs WHERE digest = ?", (digest,)
            ).fetchone() is not None

    def _link_membership(self, repo: str, digest: str) -> None:
        with self.db.tx(immediate=True) as conn:
            row = conn.execute("SELECT size FROM blobs WHERE digest = ?", (digest,)).fetchone()
            if row is None:
                # File exists without a row (orphan from a crashed commit):
                # reconcile handles it normally, but mount can validate now.
                actual_digest, size = sha256_file(self.storage.blob_abspath(digest))
                if actual_digest != digest:
                    raise digest_invalid(digest, "on-disk content corrupt")
                conn.execute(
                    "INSERT INTO blobs(digest, size, created_at) VALUES (?, ?, ?)",
                    (digest, size, _now()),
                )
            conn.execute(
                "INSERT OR IGNORE INTO repo_blobs(repo, digest, created_at) VALUES (?, ?, ?)",
                (repo, digest, _now()),
            )

    # ======================================================== upload: append

    async def append_upload(
        self,
        repo: str,
        uuid: str,
        body: IterStream,
        content_range: str | None,
    ) -> dict[str, Any]:
        self.require_repo(repo)
        start: int | None = None
        end: int | None = None
        total: int | None = None
        if content_range:
            m = _CONTENT_RANGE_RE.match(content_range.strip())
            if not m:
                raise OCIError(400, "RANGE_INVALID",
                               "Invalid Content-Range header, expected 'bytes <start>-<end>/<total>'.",
                               {"Header": content_range})
            start, end = int(m.group(1)), int(m.group(2))
            if m.group(3) != "*":
                total = int(m.group(3))
            if start > end:
                raise range_invalid(start, 0)
            if total is not None and (end >= total or (start and start >= total)):
                raise OCIError(416, "RANGE_INVALID",
                               "Content-Range exceeds the declared total size.",
                               {"Start": start, "End": end, "Total": total})

        lock = self._upload_lock(uuid)
        with lock:
            expires = self._expiry()
            with self.db.tx(immediate=True) as conn:
                row = self._require_live_upload(conn, repo, uuid)
                current = row["offset"]
                if start is not None and start != current:
                    raise range_invalid(start, current)
                # Drop bytes past the last acknowledged offset (aborted chunk).
                if self.storage.upload_size(uuid) > current:
                    self.storage.truncate_upload(uuid, current)
                new_size = await self.storage.append_async(uuid, body)
                if end is not None and end != new_size - 1:
                    # Roll the unacknowledged bytes back so resume is exact.
                    self.storage.truncate_upload(uuid, current)
                    raise range_invalid(start or current, current)
                conn.execute(
                    "UPDATE uploads SET offset = ?, updated_at = ?, expires_at = ? WHERE uuid = ?",
                    (new_size, _now(), expires, uuid),
                )
                return {"repo": repo, "uuid": uuid, "offset": new_size,
                        "expires_at": expires}

    # ====================================================== upload: status

    def upload_status(self, repo: str, uuid: str) -> dict[str, Any]:
        self.require_repo(repo)
        with self.db.tx(immediate=True) as conn:
            row = self._require_live_upload(conn, repo, uuid)
            return {"repo": repo, "uuid": uuid, "offset": row["offset"],
                    "expires_at": row["expires_at"]}

    # ===================================================== upload: finalize

    async def finalize_upload(
        self,
        repo: str,
        uuid: str,
        digest: str | None,
        body: IterStream | None = None,
        content_range: str | None = None,
    ) -> dict[str, Any]:
        self.require_repo(repo)
        if not digest:
            raise OCIError(400, "DIGEST_INVALID",
                           "A 'digest' query parameter is required to finalize an upload.",
                           {"Digest": None})
        try:
            parse_digest(digest)
        except ValueError:
            raise digest_invalid(digest)

        lock = self._upload_lock(uuid)
        with lock:
            # An optional final chunk may ride along on the PUT.
            if body is not None:
                await self.append_upload(repo, uuid, body, content_range)
            try:
                self._commit_upload(repo, uuid, digest)
            except BlobCorrupt as exc:
                raise digest_invalid(digest, str(exc))
        row = self.db.query_one(
            "SELECT size FROM blobs WHERE digest = ?", (digest,)
        )
        return {"repo": repo, "digest": digest, "size": row["size"]}

    def _commit_upload(self, repo: str, uuid: str, digest: str) -> None:
        """Verify the assembled upload, place it in CAS, record lease + membership.

        The file is hash-verified before linking; the DB row insert commits
        only after the durable CAS link exists. A crash between the two leaves
        an orphan CAS file that startup reconciliation reinserts.
        """
        path = self.storage.upload_path(uuid)
        size = self.storage.upload_size(uuid)
        created = self.storage.commit_blob(path, digest)
        now = _now()
        with self.db.tx(immediate=True) as conn:
            conn.execute(
                "INSERT INTO blobs(digest, size, created_at) VALUES (?, ?, ?) "
                "ON CONFLICT(digest) DO NOTHING",
                (digest, size, now),
            )
            conn.execute(
                "INSERT OR IGNORE INTO repo_blobs(repo, digest, created_at) VALUES (?, ?, ?)",
                (repo, digest, now),
            )
            # Finalized uploads act as short-lived GC roots ("push leases") so
            # a blob pushed moments ago cannot be reaped before its manifest
            # is published.
            conn.execute(
                "UPDATE uploads SET status='finalized', digest=?, offset=?, "
                "updated_at=?, expires_at=? WHERE uuid=?",
                (digest, size, now, now + self.settings.upload_ttl_seconds, uuid),
            )
        log.info("blob committed repo=%s digest=%s size=%d new=%s", repo, digest, size, created)

    # ================================================================ blobs

    def blob_stat(self, repo: str, digest: str) -> tuple[int, str] | None:
        self.require_repo(repo)
        try:
            parse_digest(digest)
        except ValueError:
            raise digest_invalid(digest)
        row = self.db.query_one(
            "SELECT b.size FROM blobs b JOIN repo_blobs r ON r.digest = b.digest "
            "WHERE r.repo = ? AND b.digest = ?",
            (repo, digest),
        )
        if row is None or not self.storage.blob_exists(digest):
            return None
        return row["size"], digest

    # ============================================================= manifests

    def put_manifest(
        self,
        repo: str,
        ref: str,
        media_type: str | None,
        body: bytes,
        if_match: str | None,
        if_none_match: str | None,
    ) -> dict[str, Any]:
        self.require_repo(repo)
        digest = "sha256:" + hashlib.sha256(body).hexdigest()
        size = len(body)

        if is_digest(ref):
            if ref != digest:
                raise digest_invalid(digest, "digest reference does not match manifest content")
            tag: str | None = None
        elif is_tag(ref):
            tag = ref
        else:
            raise OCIError(400, "TAG_INVALID", "Reference must be a tag or a digest.",
                           {"Reference": ref})

        doc = manifest_mod.parse_body(media_type, body)
        assert media_type is not None
        staging = self.storage.new_staging_path(uuidlib.uuid4().hex)
        with open(staging, "wb") as fh:
            fh.write(body)
            fh.flush()
            os.fsync(fh.fileno())
        try:
            # Durable CAS content first; manifest row follows in the tx.
            self.storage.commit_blob(staging, digest, size)
        except BlobCorrupt:
            self.storage.discard_staging(staging)
            raise digest_invalid(digest, "manifest content hashing failed")
        # commit_blob already unlinked staging.

        created = False
        tag_moved = False
        with self.db.tx(immediate=True) as conn:
            manifest_mod.check_references(conn, repo, media_type, doc)

            existing = conn.execute(
                "SELECT 1 FROM manifests WHERE repo = ? AND digest = ?",
                (repo, digest),
            ).fetchone()
            if existing is None:
                conn.execute(
                    "INSERT INTO manifests(repo, digest, size, media_type, created_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (repo, digest, size, media_type, _now()),
                )
                created = True

            if tag is not None:
                tag_row = conn.execute(
                    "SELECT digest FROM tags WHERE repo = ? AND tag = ?",
                    (repo, tag),
                ).fetchone()
                if tag_row is None:
                    if if_match is not None:
                        raise OCIError(
                            412, "PRECONDITION_FAILED",
                            "If-Match supplied but the tag does not exist.",
                            {"Name": repo, "Tag": tag},
                        )
                    conn.execute(
                        "INSERT INTO tags(repo, tag, digest, updated_at) VALUES (?, ?, ?, ?)",
                        (repo, tag, digest, _now()),
                    )
                else:
                    if if_none_match is not None and if_none_match.strip() == "*":
                        raise tag_conflict(tag_row["digest"], "<absent>", repo, tag)
                    current = tag_row["digest"]
                    if if_match is None:
                        raise OCIError(
                            409, "TAG_CONFLICT",
                            "Tag already exists; moving a tag requires If-Match with "
                            "the expected current digest (compare-and-swap).",
                            {"Name": repo, "Tag": tag, "CurrentDigest": current},
                            headers={
                                "Location": f"/v2/{repo}/manifests/{tag}",
                                "Docker-Content-Digest": current,
                                "ETag": f'"{current}"',
                            },
                        )
                    wanted = self._normalize_etag(if_match)
                    if wanted != current:
                        raise tag_conflict(current, wanted, repo, tag)
                    if current != digest:
                        tag_moved = True
                    conn.execute(
                        "UPDATE tags SET digest = ?, updated_at = ? WHERE repo = ? AND tag = ?",
                        (digest, _now(), repo, tag),
                    )

        return {"repo": repo, "digest": digest, "size": size,
                "media_type": media_type, "created": created,
                "tag_moved": tag_moved, "tag": tag}

    @staticmethod
    def _normalize_etag(value: str) -> str:
        # Support comma-separated lists of quoted or bare digests; clients
        # normally send exactly one.
        for item in value.split(","):
            item = item.strip().strip('"')
            if is_digest(item):
                return item
        return value.strip().strip('"')

    def head_manifest(self, repo: str, ref: str) -> dict[str, Any] | None:
        self.require_repo(repo)
        row = self._resolve_manifest(repo, ref)
        if row is None:
            return None
        if not self.storage.blob_exists(row["digest"]):
            return None
        return {"digest": row["digest"], "size": row["size"],
                "media_type": row["media_type"]}

    def get_manifest(self, repo: str, ref: str) -> dict[str, Any] | None:
        meta = self.head_manifest(repo, ref)
        if meta is None:
            return None
        with self.storage.open_blob(meta["digest"]) as fh:
            meta = dict(meta)
            meta["body"] = fh.read()
        return meta

    def _resolve_manifest(self, repo: str, ref: str):
        if is_digest(ref):
            return self.db.query_one(
                "SELECT digest, size, media_type FROM manifests WHERE repo = ? AND digest = ?",
                (repo, ref),
            )
        if is_tag(ref):
            return self.db.query_one(
                "SELECT m.digest, m.size, m.media_type FROM manifests m "
                "JOIN tags t ON t.repo = m.repo AND t.digest = m.digest "
                "WHERE t.repo = ? AND t.tag = ?",
                (repo, ref),
            )
        raise OCIError(400, "TAG_INVALID", "Reference must be a tag or a digest.",
                       {"Reference": ref})

    def list_tags(self, repo: str) -> list[str]:
        self.require_repo(repo)
        rows = self.db.query_all(
            "SELECT tag FROM tags WHERE repo = ? ORDER BY tag", (repo,)
        )
        return [r["tag"] for r in rows]

    # ========================================================= garbage collect

    def _build_graph(self, conn) -> dict[str, Any]:
        """Compute the reachability graph inside an open transaction.

        Roots: every tag target and every unexpired finalized upload lease.
        Edges: manifest -> blob descriptors, manifest -> manifest descriptors
        (index children, subject / referrers, artifact manifests).
        """
        now = _now()
        reachable_manifests: set[tuple[str, str]] = set()
        reachable_blobs: set[str] = set()
        leases: list[dict[str, Any]] = []
        warnings: list[str] = []
        tag_count = 0

        queue: list[tuple[str, str]] = []
        for t in conn.execute(
            "SELECT repo, digest FROM tags"
        ):
            tag_count += 1
            key = (t["repo"], t["digest"])
            if key not in reachable_manifests:
                reachable_manifests.add(key)
                queue.append(key)

        for u in conn.execute(
            "SELECT uuid, repo, digest, expires_at FROM uploads "
            "WHERE status='finalized' AND expires_at > ? AND digest IS NOT NULL",
            (now,),
        ):
            reachable_blobs.add(u["digest"])
            leases.append({"uuid": u["uuid"], "repo": u["repo"],
                           "digest": u["digest"], "expires_at": u["expires_at"]})

        while queue:
            repo, digest = queue.pop()
            mrow = conn.execute(
                "SELECT media_type FROM manifests WHERE repo=? AND digest=?",
                (repo, digest),
            ).fetchone()
            if mrow is None:
                # Tag/edge points at a missing manifest row: dangling ref.
                warnings.append(f"dangling manifest reference {repo}@{digest}")
                continue
            try:
                with self.storage.open_blob(digest) as fh:
                    doc = json.loads(fh.read())
            except Exception as exc:  # missing file / bad json
                warnings.append(f"unreadable manifest {repo}@{digest}: {exc}; "
                                f"its references are conservatively skipped")
                continue
            try:
                groups = manifest_mod.referenced_descriptors(mrow["media_type"], doc)
            except OCIError as exc:
                warnings.append(f"invalid manifest {repo}@{digest}: {exc.message}")
                continue
            for b_digest, _size in groups["blobs"]:
                reachable_blobs.add(b_digest)
            for m_digest, _size in groups["manifests"] + groups["subject"]:
                key = (repo, m_digest)
                if key not in reachable_manifests:
                    reachable_manifests.add(key)
                    queue.append(key)

        all_blobs = {r["digest"] for r in conn.execute("SELECT digest FROM blobs")}
        all_manifests = {(r["repo"], r["digest"])
                         for r in conn.execute("SELECT repo, digest FROM manifests")}
        expired_uploads = [r["uuid"] for r in conn.execute(
            "SELECT uuid FROM uploads WHERE expires_at <= ?", (now,)
        )]

        unreachable_blobs = all_blobs - reachable_blobs
        unreachable_manifests = all_manifests - reachable_manifests

        sizes = dict(conn.execute("SELECT digest, size FROM blobs").fetchall())
        man_sizes = {(r["repo"], r["digest"]): r["size"]
                     for r in conn.execute("SELECT repo, digest, size FROM manifests")}
        reclaimable = sum(sizes.get(d, 0) for d in unreachable_blobs)
        reclaimable += sum(man_sizes.get(k, 0) for k in unreachable_manifests)

        return {
            "generated_at": now,
            "roots": {"tags": tag_count, "uploadLeases": leases},
            "reachable": {"manifests": sorted(reachable_manifests),
                          "blobs": sorted(reachable_blobs)},
            "unreachable": {"blobs": sorted(unreachable_blobs),
                            "manifests": [list(x) for x in sorted(unreachable_manifests)],
                            "expiredUploads": sorted(expired_uploads)},
            "reclaimableBytes": reclaimable,
            "warnings": warnings,
        }

    def gc_dry_run(self) -> dict[str, Any]:
        with self.db.tx(immediate=False) as conn:
            graph = self._build_graph(conn)
        plan_id = uuidlib.uuid4().hex
        now = _now()
        expires = now + self.settings.plan_ttl_seconds
        plan = {
            "id": plan_id,
            "dry_run": True,
            "created_at": now,
            "expires_at": expires,
            **graph,
        }
        with self.db.tx(immediate=True) as conn:
            conn.execute(
                "INSERT INTO gc_plans(id, created_at, expires_at, payload_json) "
                "VALUES (?, ?, ?, ?)",
                (plan_id, now, expires, json.dumps(plan)),
            )
        return self._public_plan(plan)

    def gc_execute(self, plan_id: str, force: bool = False) -> dict[str, Any]:
        if not self._gc_lock.acquire(blocking=False):
            raise gc_running()
        try:
            return self._gc_execute_locked(plan_id, force)
        finally:
            self._gc_lock.release()

    def _gc_execute_locked(self, plan_id: str, force: bool) -> dict[str, Any]:
        with self.db.ro() as conn:
            prow = conn.execute("SELECT * FROM gc_plans WHERE id=?", (plan_id,)).fetchone()
            if prow is None:
                raise plan_unknown(plan_id)
            now = _now()
            if prow["expires_at"] <= now:
                raise plan_unknown(plan_id)
            if prow["run_id"] is not None and not force:
                raise plan_stale(plan_id)
            plan = json.loads(prow["payload_json"])
        # A plan may be executed more than once against a changing store: the
        # execute step recomputes reachability and intersects, so only content
        # still unreachable is ever deleted. The plan itself becomes invalid
        # only when consumed (run_id) or expired.

        run_id = uuidlib.uuid4().hex
        with self.db.tx(immediate=True) as conn:
            conn.execute(
                "INSERT INTO gc_runs(id, started_at, status, plan_id) VALUES (?, ?, 'running', ?)",
                (run_id, _now(), plan_id),
            )
            # Recompute the graph against the *current* snapshot, then only
            # delete what the reviewed plan AND the fresh graph agree is
            # unreachable. Publishing is serialized by the same IMMEDIATE lock,
            # so content referenced by a committed manifest can never match.
            fresh = self._build_graph(conn)

            plan_blobs = set(plan["unreachable"]["blobs"])
            fresh_blobs = set(fresh["unreachable"]["blobs"])
            plan_man = {tuple(x) for x in plan["unreachable"]["manifests"]}
            fresh_man = {tuple(x) for x in fresh["unreachable"]["manifests"]}
            plan_up = set(plan["unreachable"]["expiredUploads"])
            fresh_up = set(fresh["unreachable"]["expiredUploads"])

            blobs = sorted(plan_blobs & fresh_blobs)
            manifests = sorted(plan_man & fresh_man)
            uploads = sorted(plan_up & fresh_up)
            skipped = {
                "blobs": sorted(plan_blobs - fresh_blobs),
                "manifests": [list(x) for x in sorted(plan_man - fresh_man)],
                "uploads": sorted(plan_up - fresh_up),
            }

            sizes = dict(conn.execute("SELECT digest, size FROM blobs").fetchall())
            man_sizes = dict(((r["repo"], r["digest"]), r["size"])
                             for r in conn.execute("SELECT repo, digest, size FROM manifests"))
            reclaimable = sum(sizes.get(d, 0) for d in blobs)
            reclaimable += sum(man_sizes.get(k, 0) for k in manifests)

            # FK ON DELETE RESTRICT on tags -> manifests is the final guard:
            # deleting a tagged manifest would abort the whole transaction.
            for repo, digest in manifests:
                conn.execute("DELETE FROM manifests WHERE repo=? AND digest=?",
                             (repo, digest))
            if blobs:
                conn.executemany(
                    "DELETE FROM repo_blobs WHERE digest=?",
                    [(d,) for d in blobs],
                )
                conn.executemany("DELETE FROM blobs WHERE digest=?", [(d,) for d in blobs])
            conn.executemany("DELETE FROM uploads WHERE uuid=?", [(u,) for u in uploads])

            # Final invariant sweep: no surviving tag may dangle.
            dangling = conn.execute(
                "SELECT t.repo, t.tag FROM tags t LEFT JOIN manifests m "
                "ON m.repo=t.repo AND m.digest=t.digest WHERE m.digest IS NULL"
            ).fetchall()
            if dangling:
                raise OCIError(500, "GC_ABORTED",
                               "Invariant violation detected; transaction rolled back, "
                               "nothing was deleted.",
                               {"dangling": [dict(r) for r in dangling]})

            conn.execute("UPDATE gc_plans SET run_id=? WHERE id=?", (run_id, plan_id))
            result = {
                "run_id": run_id,
                "plan_id": plan_id,
                "deleted": {"blobs": blobs,
                            "manifests": [list(x) for x in manifests],
                            "uploads": uploads},
                "reclaimableBytes": reclaimable,
                "skippedBecauseReachable": skipped,
                "warnings": fresh["warnings"],
            }
            conn.execute(
                "UPDATE gc_runs SET status='completed', finished_at=?, result_json=? WHERE id=?",
                (_now(), json.dumps(result), run_id),
            )

        # Files are unlinked only after the DB commit. On POSIX, a concurrent
        # pull that already opened the file keeps reading its inode safely.
        for digest in blobs:
            self.storage.remove_blob(digest)
        for _repo, digest in manifests:
            self.storage.remove_blob(digest)
        for up in uploads:
            self.storage.remove_upload(up)
        log.info("gc complete run=%s blobs=%d manifests=%d uploads=%d bytes=%d",
                 run_id, len(blobs), len(manifests), len(uploads), reclaimable)
        return result

    def get_plan(self, plan_id: str) -> dict[str, Any]:
        row = self.db.query_one("SELECT payload_json FROM gc_plans WHERE id=?", (plan_id,))
        if row is None:
            raise plan_unknown(plan_id)
        return self._public_plan(json.loads(row["payload_json"]))

    @staticmethod
    def _public_plan(plan: dict[str, Any]) -> dict[str, Any]:
        out = dict(plan)
        out["review"] = (
            f"{len(plan['unreachable']['blobs'])} blob(s) and "
            f"{len(plan['unreachable']['manifests'])} manifest(s) unreachable; "
            f"{plan['reclaimableBytes']} byte(s) reclaimable. "
            f"POST /gc with {{\"plan_id\": \"{plan['id']}\"}} to execute."
        )
        return out

    # ============================================================ reconcile

    def reconcile_on_startup(self) -> None:
        """Recover state after a crash or an unclean shutdown.

        * CAS files without rows: verify digest and adopt, or remove corrupt.
        * Blob rows without files: drop rows; manifests without files: drop
          their tags and rows (already-broken refs).
        * In-progress uploads: truncate to the last acknowledged offset.
        * Expired/consumed sessions and stray part/staging files: remove.
        * Stuck gc_runs: mark failed.
        """
        now = _now()
        warnings: list[str] = []

        disk_blobs = self.storage.list_blob_digests()
        adopted: list[str] = []
        for digest in sorted(disk_blobs):
            row = self.db.query_one("SELECT 1 FROM blobs WHERE digest=?", (digest,))
            is_manifest = self.db.query_one(
                "SELECT 1 FROM manifests WHERE digest=? LIMIT 1", (digest,)
            )
            if row is not None or is_manifest is not None:
                continue
            actual, size = sha256_file(self.storage.blob_abspath(digest))
            if actual != digest:
                log.warning("removing corrupt orphan CAS file %s", digest)
                self.storage.remove_blob(digest)
                warnings.append(f"removed corrupt orphan {digest}")
                continue
            with self.db.tx(immediate=True) as conn:
                conn.execute(
                    "INSERT OR IGNORE INTO blobs(digest, size, created_at) VALUES (?, ?, ?)",
                    (digest, size, now),
                )
            adopted.append(digest)
        if adopted:
            log.info("adopted %d orphan CAS blob(s) after restart", len(adopted))

        disk_blobs = self.storage.list_blob_digests()
        with self.db.tx(immediate=True) as conn:
            missing_blobs = [r["digest"] for r in conn.execute("SELECT digest FROM blobs")
                             if r["digest"] not in disk_blobs]
            for digest in missing_blobs:
                log.warning("blob row with missing file removed: %s", digest)
                warnings.append(f"missing blob file {digest}")
                conn.execute("DELETE FROM blobs WHERE digest=?", (digest,))

            missing_man = [(r["repo"], r["digest"]) for r in
                           conn.execute("SELECT repo, digest FROM manifests")
                           if r["digest"] not in disk_blobs]
            for repo, digest in missing_man:
                log.warning("manifest %s@%s missing file; removing tags + row", repo, digest)
                warnings.append(f"missing manifest file {repo}@{digest}")
                conn.execute("DELETE FROM tags WHERE repo=? AND digest=?", (repo, digest))
                conn.execute("DELETE FROM manifests WHERE repo=? AND digest=?", (repo, digest))

            # Upload recovery.
            rows = conn.execute("SELECT uuid, status, offset, expires_at FROM uploads").fetchall()
            live_uuids = {r["uuid"] for r in rows}
            for r in rows:
                uuid = r["uuid"]
                if r["expires_at"] <= now:
                    conn.execute("DELETE FROM uploads WHERE uuid=?", (uuid,))
                    self.storage.remove_upload(uuid)
                    continue
                if r["status"] == "uploading":
                    self.storage.truncate_upload(uuid, r["offset"])
                elif r["status"] == "finalized":
                    # Part file should have been unlinked at finalize time.
                    self.storage.remove_upload(uuid)

            stuck = conn.execute(
                "SELECT id FROM gc_runs WHERE status='running'"
            ).fetchall()
            for r in stuck:
                conn.execute("UPDATE gc_runs SET status='failed', finished_at=? WHERE id=?",
                             (now, r["id"]))
                warnings.append(f"gc run {r['id']} interrupted by shutdown")

        # Stray upload part files with no DB row.
        for uuid in self.storage.list_upload_files() - live_uuids:
            log.warning("removing stray upload file %s", uuid)
            self.storage.remove_upload(uuid)

        # Staging files belong to in-process requests only.
        for name in os.listdir(self.storage.staging_dir):
            try:
                os.unlink(os.path.join(self.storage.staging_dir, name))
            except OSError:
                pass

        log.info("startup reconciliation complete: %s",
                 warnings if warnings else "no inconsistencies")
