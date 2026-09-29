"""SQLite persistence (WAL mode, foreign keys enforced).

Timestamps are unix epoch seconds (REAL).
"""
from __future__ import annotations

import os
import sqlite3
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

SCHEMA_VERSION = 1

SCHEMA = """
CREATE TABLE IF NOT EXISTS schema_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- Content-addressable blobs, globally deduplicated (one row per digest).
CREATE TABLE IF NOT EXISTS blobs (
    digest     TEXT PRIMARY KEY,
    size       INTEGER NOT NULL,
    created_at REAL NOT NULL
);

-- Repository membership of a blob. A digest must be a member of a repository
-- before it may be referenced by a manifest of that repository.
CREATE TABLE IF NOT EXISTS repo_blobs (
    repo       TEXT NOT NULL,
    digest     TEXT NOT NULL REFERENCES blobs(digest) ON DELETE CASCADE,
    created_at REAL NOT NULL,
    PRIMARY KEY (repo, digest)
);

CREATE TABLE IF NOT EXISTS uploads (
    uuid       TEXT PRIMARY KEY,
    repo       TEXT NOT NULL,
    status     TEXT NOT NULL CHECK (status IN ('uploading','committed','finalized','expired')),
    offset     INTEGER NOT NULL DEFAULT 0,
    -- Set once finalized: the resulting blob digest.
    digest     TEXT,
    -- Last activity: created, PATCH received or finalized.
    updated_at REAL NOT NULL,
    expires_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_uploads_expires ON uploads(expires_at);
CREATE INDEX IF NOT EXISTS idx_uploads_digest ON uploads(digest);

-- Manifests are themselves CAS content stored like blobs; this table carries
-- parsed metadata. (repo, digest) is the key, tags point at it.
CREATE TABLE IF NOT EXISTS manifests (
    repo       TEXT NOT NULL,
    digest     TEXT NOT NULL,
    size       INTEGER NOT NULL,
    media_type TEXT NOT NULL,
    created_at REAL NOT NULL,
    PRIMARY KEY (repo, digest)
);
CREATE INDEX IF NOT EXISTS idx_manifests_digest ON manifests(digest);

CREATE TABLE IF NOT EXISTS tags (
    repo       TEXT NOT NULL,
    tag        TEXT NOT NULL,
    digest     TEXT NOT NULL,
    updated_at REAL NOT NULL,
    PRIMARY KEY (repo, tag),
    FOREIGN KEY (repo, digest) REFERENCES manifests(repo, digest) ON DELETE RESTRICT
);
CREATE INDEX IF NOT EXISTS idx_tags_repo ON tags(repo);

-- Dry-run GC plans, reviewable before execution.
CREATE TABLE IF NOT EXISTS gc_plans (
    id           TEXT PRIMARY KEY,
    created_at   REAL NOT NULL,
    expires_at   REAL NOT NULL,
    run_id       TEXT,
    payload_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS gc_runs (
    id           TEXT PRIMARY KEY,
    started_at   REAL NOT NULL,
    finished_at  REAL,
    status       TEXT NOT NULL CHECK (status IN ('running','completed','failed')),
    plan_id      TEXT,
    result_json  TEXT
);
"""


class Database:
    """Thin sqlite3 wrapper.

    A new connection is opened per request (cheap with WAL). A module-level
    lock serializes writers using ``BEGIN IMMEDIATE`` so publish and GC cannot
    interleave their multi-statement transactions (SQLite itself would raise
    SQLITE_BUSY; the lock makes retries unnecessary).
    """

    def __init__(self, path: str) -> None:
        self.path = path
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self._writer_lock = threading.RLock()
        self.init()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def init(self) -> None:
        with self._connect() as conn:
            conn.executescript(SCHEMA)
            conn.execute(
                "INSERT OR IGNORE INTO schema_meta(key, value) VALUES ('version', ?)",
                (str(SCHEMA_VERSION),),
            )

    @contextmanager
    def tx(self, immediate: bool = False) -> Iterator[sqlite3.Connection]:
        """Transaction context manager.

        ``immediate=True`` acquires the write lock on BEGIN, giving a stable
        read-then-write snapshot for publish/GC transactions.
        """
        conn = self._connect()
        if immediate:
            self._writer_lock.acquire()
        try:
            conn.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
            try:
                yield conn
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        finally:
            if immediate:
                self._writer_lock.release()
            conn.close()

    @contextmanager
    def ro(self) -> Iterator[sqlite3.Connection]:
        """Autocommit read connection (WAL snapshot per statement)."""
        conn = self._connect()
        try:
            yield conn
        finally:
            conn.close()

    def query_one(self, sql: str, params: tuple[Any, ...] = ()) -> sqlite3.Row | None:
        with self.ro() as conn:
            return conn.execute(sql, params).fetchone()

    def query_all(self, sql: str, params: tuple[Any, ...] = ()) -> list[sqlite3.Row]:
        with self.ro() as conn:
            return list(conn.execute(sql, params).fetchall())
