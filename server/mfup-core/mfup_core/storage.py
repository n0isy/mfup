"""MFUP/2 SQLite storage layer.

Each session lives in:
    <base_dir>/.incoming.<session_id>/state.sqlite
    <base_dir>/.incoming.<session_id>/payload/...
"""

from __future__ import annotations

import os
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from .protocol import NodeKind, NodeStatus, SessionState, ROOT_NODE_ID


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_SCHEMA = """\
CREATE TABLE IF NOT EXISTS sessions (
    session_id   TEXT PRIMARY KEY,
    resume_token TEXT NOT NULL,
    epoch        INTEGER NOT NULL DEFAULT 1,
    state        TEXT NOT NULL DEFAULT 'active',
    target_dir   TEXT NOT NULL DEFAULT '.',
    expires_at   TEXT NOT NULL,
    created_at   TEXT NOT NULL,
    updated_at   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS nodes (
    node_id    INTEGER PRIMARY KEY,
    parent_id  INTEGER NOT NULL,
    kind       TEXT NOT NULL,
    name       TEXT NOT NULL,
    size       INTEGER,
    mtime_ms   INTEGER,
    status     TEXT NOT NULL DEFAULT 'open'
);

CREATE TABLE IF NOT EXISTS files (
    node_id          INTEGER PRIMARY KEY REFERENCES nodes(node_id),
    accepted_offset  INTEGER NOT NULL DEFAULT 0,
    final_size       INTEGER,
    checksum_state   BLOB,
    local_tmp_path   TEXT
);

CREATE TABLE IF NOT EXISTS pruned (
    node_id  INTEGER PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS rejected (
    node_id  INTEGER PRIMARY KEY,
    code     TEXT NOT NULL,
    reason   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS root_summary (
    id               INTEGER PRIMARY KEY CHECK (id = 1),
    scan_done_units  INTEGER NOT NULL DEFAULT 0,
    scan_est_units   INTEGER NOT NULL DEFAULT 0,
    body_done_bytes  INTEGER NOT NULL DEFAULT 0,
    body_est_bytes   INTEGER NOT NULL DEFAULT 0,
    sealed           INTEGER NOT NULL DEFAULT 0
);

INSERT OR IGNORE INTO root_summary (id) VALUES (1);
"""


class SessionDB:
    """Thin wrapper around a per-session SQLite database.

    By default every mutating method commits immediately. For hot paths
    (a data POST carrying thousands of frames) wrap the work in
    begin_batch()/end_batch() — mutations then accumulate in one SQLite
    transaction and commit at the batch boundary, which is 10–100× cheaper
    for many-small-files uploads (think node_modules).
    """

    def __init__(self, db_path: Path) -> None:
        self.path = db_path
        self._batch = False
        self._conn = sqlite3.connect(str(db_path), timeout=5.0)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=3000")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.executescript(_SCHEMA)
        # Lightweight migration: sessions are short-lived (TTL), so additive
        # columns are applied in place instead of versioned migrations.
        for col in ("meta_json", "auth_json"):
            try:
                self._conn.execute(f"ALTER TABLE sessions ADD COLUMN {col} TEXT")
            except sqlite3.OperationalError:
                pass  # column already exists
        self._conn.commit()

    @property
    def conn(self) -> sqlite3.Connection:
        return self._conn

    def close(self) -> None:
        self._conn.close()

    # -- batching -------------------------------------------------------------

    def begin_batch(self) -> None:
        """Enter deferred-commit mode (idempotent)."""
        self._batch = True

    def end_batch(self) -> None:
        """Commit accumulated work and leave deferred-commit mode."""
        self._batch = False
        self._conn.commit()

    def flush(self) -> None:
        """Commit accumulated work but stay in the current mode."""
        self._conn.commit()

    def _maybe_commit(self) -> None:
        if not self._batch:
            self._conn.commit()

    # -- session ------------------------------------------------------------

    def init_session(
        self,
        session_id: str,
        resume_token: str,
        expires_at: str,
        target_dir: str = ".",
        meta_json: Optional[str] = None,
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        # Explicit column list: the table gains additive columns over time
        # (ALTER migration above) — positional VALUES would break.
        self._conn.execute(
            "INSERT OR REPLACE INTO sessions "
            "(session_id, resume_token, epoch, state, target_dir, expires_at,"
            " created_at, updated_at, meta_json) "
            "VALUES (?,?,1,?,?,?,?,?,?)",
            (session_id, resume_token, SessionState.ACTIVE.value, target_dir,
             expires_at, now, now, meta_json),
        )
        self._maybe_commit()

    def set_auth_json(self, auth_json: str) -> None:
        """Persist authorize-hook constraints (quotas + JSON-able context) so
        they survive restarts / lazy-resume on another worker."""
        self._conn.execute("UPDATE sessions SET auth_json=?", (auth_json,))
        self._maybe_commit()

    def get_target_dir(self) -> str:
        cur = self._conn.execute("SELECT target_dir FROM sessions LIMIT 1")
        row = cur.fetchone()
        return row[0] if row else "."

    def get_session(self) -> Optional[sqlite3.Row]:
        self._conn.row_factory = sqlite3.Row
        cur = self._conn.execute("SELECT * FROM sessions LIMIT 1")
        return cur.fetchone()

    def set_state(self, state: SessionState) -> None:
        now = datetime.now(timezone.utc).isoformat()
        self._conn.execute(
            "UPDATE sessions SET state=?, updated_at=?", (state.value, now)
        )
        self._maybe_commit()

    def increment_epoch(self) -> int:
        self._conn.execute("UPDATE sessions SET epoch = epoch + 1, updated_at = ?",
                           (datetime.now(timezone.utc).isoformat(),))
        self._maybe_commit()
        cur = self._conn.execute("SELECT epoch FROM sessions LIMIT 1")
        return cur.fetchone()[0]

    def get_epoch(self) -> int:
        cur = self._conn.execute("SELECT epoch FROM sessions LIMIT 1")
        row = cur.fetchone()
        return row[0] if row else 0

    def get_state(self) -> SessionState:
        cur = self._conn.execute("SELECT state FROM sessions LIMIT 1")
        row = cur.fetchone()
        return SessionState(row[0]) if row else SessionState.ACTIVE

    def get_expires_at(self) -> str:
        cur = self._conn.execute("SELECT expires_at FROM sessions LIMIT 1")
        return cur.fetchone()[0]

    def set_expires_at(self, expires_at: str) -> None:
        self._conn.execute(
            "UPDATE sessions SET expires_at=?, updated_at=?",
            (expires_at, datetime.now(timezone.utc).isoformat()),
        )
        self._maybe_commit()

    # -- nodes --------------------------------------------------------------

    def upsert_node(
        self,
        node_id: int,
        parent_id: int,
        kind: NodeKind,
        name: str,
        size: Optional[int] = None,
        mtime_ms: Optional[int] = None,
    ) -> bool:
        """Upsert a node. Returns True iff a NEW files-row was created
        (i.e. this is the first time we see this FILE node) — used by the
        caller to maintain the file-count quota without SQL aggregates."""
        kind_str = "dir" if kind == NodeKind.DIR else "file"
        node = self._conn.execute(
            "INSERT INTO nodes VALUES (?,?,?,?,?,?,?) ON CONFLICT(node_id) DO UPDATE "
            "SET size=excluded.size, mtime_ms=excluded.mtime_ms "
            "WHERE nodes.parent_id=excluded.parent_id AND nodes.kind=excluded.kind AND nodes.name=excluded.name",
            (node_id, parent_id, kind_str, name, size, mtime_ms, NodeStatus.OPEN.value),
        )
        if node.rowcount == 0:
            raise ValueError("node_id cannot change its parent, kind or name")
        new_file = False
        if kind == NodeKind.FILE:
            cur = self._conn.execute(
                "INSERT OR IGNORE INTO files (node_id, accepted_offset) VALUES (?, 0)",
                (node_id,),
            )
            new_file = cur.rowcount == 1
        self._maybe_commit()
        return new_file

    def get_node(self, node_id: int) -> Optional[sqlite3.Row]:
        self._conn.row_factory = sqlite3.Row
        cur = self._conn.execute("SELECT * FROM nodes WHERE node_id=?", (node_id,))
        return cur.fetchone()

    def set_node_status(self, node_id: int, status: NodeStatus) -> None:
        self._conn.execute(
            "UPDATE nodes SET status=? WHERE node_id=?", (status.value, node_id)
        )
        self._maybe_commit()

    # -- files --------------------------------------------------------------

    def get_file(self, node_id: int) -> Optional[sqlite3.Row]:
        self._conn.row_factory = sqlite3.Row
        cur = self._conn.execute("SELECT * FROM files WHERE node_id=?", (node_id,))
        return cur.fetchone()

    def set_accepted_offset(self, node_id: int, offset: int) -> None:
        self._conn.execute(
            "UPDATE files SET accepted_offset=? WHERE node_id=?", (offset, node_id)
        )
        self._maybe_commit()

    def set_file_path(self, node_id: int, local_path: str) -> None:
        self._conn.execute("UPDATE files SET local_tmp_path=? WHERE node_id=?", (local_path, node_id))
        self._maybe_commit()

    def set_file_final(self, node_id: int, final_size: int, local_path: str) -> None:
        self._conn.execute(
            "UPDATE files SET final_size=?, local_tmp_path=? WHERE node_id=?",
            (final_size, local_path, node_id),
        )
        self._maybe_commit()

    def get_open_files(self) -> list[sqlite3.Row]:
        self._conn.row_factory = sqlite3.Row
        cur = self._conn.execute(
            "SELECT f.*, n.status FROM files f JOIN nodes n ON f.node_id = n.node_id "
            "WHERE n.status IN ('open')"
        )
        return cur.fetchall()

    def get_all_files(self) -> list[sqlite3.Row]:
        self._conn.row_factory = sqlite3.Row
        cur = self._conn.execute(
            "SELECT f.node_id, f.accepted_offset, n.status "
            "FROM files f JOIN nodes n ON f.node_id = n.node_id"
        )
        return cur.fetchall()

    # -- prune / reject -----------------------------------------------------

    def add_pruned(self, node_id: int) -> None:
        self._conn.execute("INSERT OR IGNORE INTO pruned VALUES (?)", (node_id,))
        self._maybe_commit()

    def is_pruned(self, node_id: int) -> bool:
        cur = self._conn.execute("SELECT 1 FROM pruned WHERE node_id=?", (node_id,))
        return cur.fetchone() is not None

    def get_pruned_nodes(self) -> list[int]:
        cur = self._conn.execute("SELECT node_id FROM pruned")
        return [r[0] for r in cur.fetchall()]

    def add_rejected(self, node_id: int, code: str, reason: str) -> None:
        self._conn.execute(
            "INSERT OR IGNORE INTO rejected VALUES (?,?,?)", (node_id, code, reason)
        )
        self.set_node_status(node_id, NodeStatus.REJECTED)
        self._maybe_commit()

    def is_rejected(self, node_id: int) -> bool:
        cur = self._conn.execute("SELECT 1 FROM rejected WHERE node_id=?", (node_id,))
        return cur.fetchone() is not None

    def get_rejected_files(self) -> list[int]:
        cur = self._conn.execute("SELECT node_id FROM rejected")
        return [r[0] for r in cur.fetchall()]

    # -- root summary -------------------------------------------------------

    def update_root_summary(
        self,
        scan_done: int,
        scan_est: int,
        body_done: int,
        body_est: int,
        sealed: bool,
    ) -> None:
        self._conn.execute(
            "UPDATE root_summary SET scan_done_units=?, scan_est_units=?, "
            "body_done_bytes=?, body_est_bytes=?, sealed=? WHERE id=1",
            (scan_done, scan_est, body_done, body_est, int(sealed)),
        )
        self._maybe_commit()

    def get_root_summary(self) -> dict:
        self._conn.row_factory = sqlite3.Row
        cur = self._conn.execute("SELECT * FROM root_summary WHERE id=1")
        row = cur.fetchone()
        if not row:
            return {
                "scan_done_units": 0,
                "scan_est_units": 0,
                "body_done_bytes": 0,
                "body_est_bytes": 0,
                "sealed": False,
            }
        return {
            "scan_done_units": row["scan_done_units"],
            "scan_est_units": row["scan_est_units"],
            "body_done_bytes": row["body_done_bytes"],
            "body_est_bytes": row["body_est_bytes"],
            "sealed": bool(row["sealed"]),
        }

    # -- aggregate stats for COMMIT_OK --------------------------------------

    def count_nodes(self) -> int:
        """Total nodes seen (dirs + files) — commit node-count invariant."""
        cur = self._conn.execute("SELECT COUNT(*) FROM nodes")
        return cur.fetchone()[0]

    def count_committed_files(self) -> tuple[int, int]:
        """Return (file_count, total_bytes) for all non-rejected files."""
        cur = self._conn.execute(
            "SELECT COUNT(*), COALESCE(SUM(accepted_offset), 0) "
            "FROM files f JOIN nodes n ON f.node_id = n.node_id "
            "WHERE n.status NOT IN ('rejected', 'pruned')"
        )
        row = cur.fetchone()
        return (row[0], row[1])

    def get_incomplete_files(self) -> list[dict]:
        """Return files where accepted_offset != final_size (incomplete transfers)."""
        self._conn.row_factory = sqlite3.Row
        cur = self._conn.execute(
            "SELECT f.node_id, f.accepted_offset, f.final_size "
            "FROM files f JOIN nodes n ON f.node_id = n.node_id "
            "WHERE n.status NOT IN ('rejected', 'pruned') "
            "  AND (f.final_size IS NULL OR f.accepted_offset != f.final_size)"
        )
        return [dict(row) for row in cur.fetchall()]


# ---------------------------------------------------------------------------
# Staging directory helpers
# ---------------------------------------------------------------------------

DEFAULT_STAGING_PREFIX = ".incoming"


def validate_session_id(session_id: object) -> None:
    if not isinstance(session_id, str) or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", session_id) is None:
        raise ValueError("session_id must be 1–128 ASCII letters, digits, dots, underscores or hyphens, starting with a letter or digit")


def staging_dir(base_dir: Path, session_id: str, prefix: str = DEFAULT_STAGING_PREFIX) -> Path:
    validate_session_id(session_id)
    return base_dir / f"{prefix}.{session_id}"


def ensure_staging(base_dir: Path, session_id: str, prefix: str = DEFAULT_STAGING_PREFIX) -> Path:
    sd = staging_dir(base_dir, session_id, prefix)
    sd.mkdir(parents=True, exist_ok=True)
    (sd / "payload").mkdir(exist_ok=True)
    return sd


def open_session_db(base_dir: Path, session_id: str, prefix: str = DEFAULT_STAGING_PREFIX) -> SessionDB:
    sd = ensure_staging(base_dir, session_id, prefix)
    return SessionDB(sd / "state.sqlite")


def validate_node_name(name: str) -> None:
    """Reject names that could escape the payload directory."""
    if not name:
        raise ValueError("empty node name")
    if name in (".", ".."):
        raise ValueError(f"illegal node name: {name!r}")
    if "/" in name or "\\" in name or "\x00" in name:
        raise ValueError(f"illegal characters in node name: {name!r}")


def resolve_payload_path(base_dir: Path, session_id: str, db: SessionDB, node_id: int, prefix: str = DEFAULT_STAGING_PREFIX) -> Path:
    """Build the filesystem path for a node inside the payload directory.

    Walks parent_id chain in the DB to reconstruct the relative path.
    Validates each name component to prevent path traversal.
    """
    parts: list[str] = []
    cur_id = node_id
    seen: set[int] = set()
    while cur_id != ROOT_NODE_ID:
        if cur_id in seen:
            raise ValueError(f"parent cycle detected at node {cur_id}")
        seen.add(cur_id)
        node = db.get_node(cur_id)
        if node is None:
            # A silent `break` here would resolve a SHORTENED path and write
            # the file into the wrong directory. Broken chains are an error.
            raise ValueError(f"broken parent chain: node {cur_id} unknown")
        validate_node_name(node["name"])
        parts.append(node["name"])
        cur_id = node["parent_id"]
    parts.reverse()
    sd = staging_dir(base_dir, session_id, prefix)
    result = sd / "payload" / Path(*parts) if parts else sd / "payload"
    # Defense in depth: resolved path must stay within payload dir
    payload_root = sd / "payload"
    if not result.resolve().is_relative_to(payload_root.resolve()):
        raise ValueError(f"path traversal detected: {result}")
    return result
