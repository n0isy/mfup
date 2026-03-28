"""MFUP/2 session manager — in-memory registry of active sessions backed by
per-session SQLite databases in staging directories.

One writer per file is enforced. Each session has at most one active leg.
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

from .protocol import (
    ChecksumKind,
    FileChunkFrame,
    FileCloseFrame,
    FileOpenFrame,
    Frame,
    FrameTag,
    NodeFrame,
    NodeKind,
    SessionEndFrame,
    SessionState,
    SummaryFrame,
    ClientAbortFrame,
    DirCloseFrame,
    NodeStatus,
    crc32c,
)
from .storage import DEFAULT_STAGING_PREFIX, SessionDB, ensure_staging, open_session_db, resolve_payload_path, staging_dir

logger = logging.getLogger("mfup.session")


# ---------------------------------------------------------------------------
# Per-session live state (in-memory, not persisted)
# ---------------------------------------------------------------------------

class FileWriter:
    """Wraps an open file handle for a single file being uploaded."""

    def __init__(self, path: Path, node_id: int, accepted_offset: int = 0) -> None:
        self.path = path
        self.node_id = node_id
        self.accepted_offset = accepted_offset
        # Open with 'ab' to append; if resuming, we truncate to accepted_offset first.
        path.parent.mkdir(parents=True, exist_ok=True)
        if accepted_offset == 0:
            self._fh = open(path, "wb")
        else:
            self._fh = open(path, "r+b")
            self._fh.seek(accepted_offset)
            self._fh.truncate()

    async def write(self, data: bytes, offset: int) -> int:
        """Write data at the expected offset. Returns new accepted_offset."""
        if offset != self.accepted_offset:
            raise ValueError(
                f"bad offset: expected {self.accepted_offset}, got {offset}"
            )
        await asyncio.to_thread(self._sync_write, data)
        self.accepted_offset += len(data)
        return self.accepted_offset

    def _sync_write(self, data: bytes) -> None:
        self._fh.write(data)
        self._fh.flush()

    def close(self) -> None:
        self._fh.close()


class LiveSession:
    """In-memory state for one active upload session."""

    def __init__(
        self,
        session_id: str,
        resume_token: str,
        base_dir: Path,
        db: SessionDB,
        *,
        target_dir: str = ".",
        staging_prefix: str = DEFAULT_STAGING_PREFIX,
        session_resume_ttl: int = 3600,
        leg_idle_timeout: int = 60,
        publish_timeout: int = 30,
    ) -> None:
        self.session_id = session_id
        self.resume_token = resume_token
        self.base_dir = base_dir
        self.db = db
        self.target_dir = target_dir
        self.staging_prefix = staging_prefix

        # Timers (seconds)
        self.session_resume_ttl = session_resume_ttl
        self.leg_idle_timeout = leg_idle_timeout
        self.publish_timeout = publish_timeout

        # Current leg
        self.leg_id: Optional[str] = None
        self.epoch: int = db.get_epoch()

        # File writers — one per open file, keyed by node_id
        self.writers: dict[int, FileWriter] = {}

        # WebSocket for control channel (set when control connects)
        self.ws: Any = None

        # Idle timer handle
        self._idle_task: Optional[asyncio.TimerHandle] = None

        # Sequence tracking for multi-POST data legs
        self.last_data_seq: int = -1

        # Conflict FSM: "clean" → "conflict_dir" → "conflict_files"
        self.conflict_state: str = "clean"

        # Publish action (set by client ACTION message)
        self.publish_action: Optional[str] = None

        # Callback for expiry changes (set by registry for Redis updates)
        self._on_expiry_change: Any = None  # async fn(session_id, expires_at_datetime)

        # Locks
        self._lock = asyncio.Lock()

    @property
    def state(self) -> SessionState:
        return self.db.get_state()

    @property
    def expires_at(self) -> str:
        return self.db.get_expires_at()

    # -- leg management -----------------------------------------------------

    def validate_and_advance_seq(self, seq: int) -> bool:
        """Validate that seq == last_data_seq + 1 and advance on success.

        Returns True if the sequence number is valid (next expected),
        False on gap or duplicate.
        """
        if seq == self.last_data_seq + 1:
            self.last_data_seq = seq
            return True
        return False

    def attach_leg(self, leg_id: str) -> int:
        """Attach a new leg. Returns the new epoch."""
        # Close any existing writers (stale leg)
        self._close_all_writers()
        self.leg_id = leg_id
        self.last_data_seq = -1
        self.epoch = self.db.increment_epoch()
        self.db.set_state(SessionState.ACTIVE)
        self._reset_idle_timer()
        return self.epoch

    def detach_leg(self) -> None:
        """Detach the current leg (disconnect)."""
        self._close_all_writers()
        self._cancel_idle_timer()
        if self.state in (SessionState.ACTIVE, SessionState.PAUSED_BY_SERVER):
            self.db.set_state(SessionState.WAITING_RESUME)
            # Update expires_at for resume TTL
            exp = datetime.now(timezone.utc) + timedelta(seconds=self.session_resume_ttl)
            self.db.set_expires_at(exp.isoformat())
            # Notify Redis index about new expiry
            if self._on_expiry_change:
                asyncio.ensure_future(self._on_expiry_change(self.session_id, exp))
        self.leg_id = None
        self.ws = None

    # -- frame processing ---------------------------------------------------

    async def process_frame(self, frame: Frame, leg_id: str) -> None:
        """Process a decoded data frame. Must match current leg_id."""
        if leg_id != self.leg_id:
            logger.warning("Ignoring frame from stale leg %s (current: %s)", leg_id, self.leg_id)
            return

        self._reset_idle_timer()

        if isinstance(frame, NodeFrame):
            await self._handle_node(frame)
        elif isinstance(frame, FileOpenFrame):
            await self._handle_file_open(frame)
        elif isinstance(frame, FileChunkFrame):
            await self._handle_file_chunk(frame)
        elif isinstance(frame, FileCloseFrame):
            await self._handle_file_close(frame)
        elif isinstance(frame, DirCloseFrame):
            await self._handle_dir_close(frame)
        elif isinstance(frame, SummaryFrame):
            await self._handle_summary(frame)
        elif isinstance(frame, SessionEndFrame):
            await self._handle_session_end(frame)
        elif isinstance(frame, ClientAbortFrame):
            await self._handle_client_abort(frame)

    def _resolve_target_path(self, node_id: int) -> Path | None:
        """Resolve where this node would land in target_dir after publish."""
        target = Path(self.target_dir)
        if not target.is_absolute():
            target = self.base_dir / target
        # Build relative path from payload root
        parts: list[str] = []
        cur = node_id
        while cur != 0:  # ROOT_NODE_ID
            node = self.db.get_node(cur)
            if node is None:
                return None
            parts.append(node["name"])
            cur = node["parent_id"]
        if not parts:
            return None
        parts.reverse()
        result = target / Path(*parts)
        # Guard against path traversal via crafted file names
        try:
            result.resolve().relative_to(self.base_dir.resolve())
        except ValueError:
            return None
        return result

    async def _check_conflict(self, node_id: int, is_dir: bool) -> None:
        """Advance conflict FSM by checking if target path exists."""
        if self.conflict_state == "conflict_files":
            return  # already at terminal state
        dest = self._resolve_target_path(node_id)
        if dest is None or not dest.exists():
            return
        if is_dir and dest.is_dir():
            # Dir-dir match: auto-accept, advance silently
            if self.conflict_state == "clean":
                self.conflict_state = "conflict_dir"
                logger.info("Session %s: conflict_dir (dir %s exists)", self.session_id, dest.name)
        else:
            # File-file or type mismatch: send ASK once
            self.conflict_state = "conflict_files"
            logger.info("Session %s: conflict_files (file %s exists)", self.session_id, dest.name)
            await self.send_control({"t": "ASK"})

    async def _handle_node(self, f: NodeFrame) -> None:
        if self.db.is_pruned(f.node_id):
            return
        self.db.upsert_node(
            f.node_id, f.parent_id, f.kind, f.name,
            size=f.size_hint, mtime_ms=f.mtime_ms,
        )
        if f.kind == NodeKind.DIR:
            # Create directory in payload
            path = resolve_payload_path(self.base_dir, self.session_id, self.db, f.node_id, self.staging_prefix)
            path.mkdir(parents=True, exist_ok=True)
            await self._check_conflict(f.node_id, is_dir=True)
        else:
            await self._check_conflict(f.node_id, is_dir=False)

    async def _handle_file_open(self, f: FileOpenFrame) -> None:
        if self.db.is_rejected(f.node_id) or self.db.is_pruned(f.node_id):
            return

        node = self.db.get_node(f.node_id)
        if node is None:
            await self._send_nack(f.node_id, 0, "bad_offset")
            return

        file_row = self.db.get_file(f.node_id)
        accepted = file_row["accepted_offset"] if file_row else 0

        path = resolve_payload_path(self.base_dir, self.session_id, self.db, f.node_id)
        writer = FileWriter(path, f.node_id, accepted)
        self.writers[f.node_id] = writer

    async def _handle_file_chunk(self, f: FileChunkFrame) -> None:
        if self.db.is_rejected(f.node_id) or self.db.is_pruned(f.node_id):
            return

        writer = self.writers.get(f.node_id)
        if writer is None:
            await self._send_nack(f.node_id, 0, "bad_offset")
            return

        # Verify checksum
        if f.checksum_kind == ChecksumKind.CRC32C:
            computed = crc32c(f.payload)
            if computed != f.checksum:
                await self._send_nack(f.node_id, writer.accepted_offset, "bad_checksum")
                return

        # Verify contiguous offset
        if f.offset != writer.accepted_offset:
            await self._send_nack(f.node_id, writer.accepted_offset, "bad_offset")
            return

        try:
            new_offset = await writer.write(f.payload, f.offset)
        except Exception as exc:
            logger.error("Write error for node %d: %s", f.node_id, exc)
            await self._send_nack(f.node_id, writer.accepted_offset, "server_policy")
            return

        self.db.set_accepted_offset(f.node_id, new_offset)

        # Update root summary body_done
        summary = self.db.get_root_summary()
        self.db.update_root_summary(
            summary["scan_done_units"],
            summary["scan_est_units"],
            summary["body_done_bytes"] + len(f.payload),
            summary["body_est_bytes"],
            summary["sealed"],
        )

        # Send cumulative FILE_ACK (not per-chunk mandatory, but we send periodically)
        # Send ACK every 256 KiB or more
        if new_offset % (256 * 1024) < f.length or f.length == 0:
            await self._send_file_ack(f.node_id, new_offset)

    async def _handle_file_close(self, f: FileCloseFrame) -> None:
        writer = self.writers.pop(f.node_id, None)
        if writer:
            final_offset = writer.accepted_offset
            writer.close()
            self.db.set_file_final(f.node_id, f.size_sent, str(writer.path))
            self.db.set_node_status(f.node_id, NodeStatus.CLOSED)
            # Send final ACK
            await self._send_file_ack(f.node_id, final_offset)

    async def _handle_dir_close(self, f: DirCloseFrame) -> None:
        self.db.set_node_status(f.node_id, NodeStatus.CLOSED)

    async def _handle_summary(self, f: SummaryFrame) -> None:
        cur = self.db.get_root_summary()
        self.db.update_root_summary(
            max(f.scan_done_units, cur["scan_done_units"]),
            max(f.scan_est_units, cur["scan_est_units"]),
            cur["body_done_bytes"],  # keep server-side body count
            max(f.body_est_bytes, cur["body_est_bytes"]),
            f.sealed or cur["sealed"],
        )

    async def _handle_session_end(self, f: SessionEndFrame) -> None:
        summary = self.db.get_root_summary()
        self.db.update_root_summary(
            f.scan_done_units,
            f.scan_est_units,
            summary["body_done_bytes"],
            f.body_est_bytes,
            True,
        )
        self._close_all_writers()
        self.db.set_state(SessionState.COMMITTING)
        logger.info("Session %s entering COMMITTING state", self.session_id)

    async def _handle_client_abort(self, f: ClientAbortFrame) -> None:
        self._close_all_writers()
        self.db.set_state(SessionState.ABORTED)
        logger.info("Session %s aborted by client: %s — %s", self.session_id, f.code, f.reason)

    # -- control message senders -------------------------------------------

    async def _send_file_ack(self, node_id: int, accepted_offset: int) -> None:
        if self.ws:
            try:
                await self.ws.send_json({
                    "t": "FILE_ACK",
                    "node_id": node_id,
                    "accepted_offset": accepted_offset,
                })
            except Exception as exc:
                logger.error("Failed to send FILE_ACK for node %d: %s", node_id, exc)

    async def _send_nack(self, node_id: int, expected_offset: int, reason: str) -> None:
        if self.ws:
            try:
                await self.ws.send_json({
                    "t": "NACK_CHUNK",
                    "node_id": node_id,
                    "expected_offset": expected_offset,
                    "reason": reason,
                })
            except Exception as exc:
                logger.error("Failed to send NACK for node %d: %s", node_id, exc)

    async def send_control(self, msg: dict) -> None:
        if self.ws:
            try:
                await self.ws.send_json(msg)
            except Exception as exc:
                logger.error("Failed to send control msg %s: %s", msg.get("t", "?"), exc)

    # -- prune / reject (server-initiated) ----------------------------------

    async def prune_node(self, node_id: int, code: str, reason: str) -> None:
        self.db.add_pruned(node_id)
        self.db.set_node_status(node_id, NodeStatus.PRUNED)
        # Close any active writer for files in this subtree
        writer = self.writers.pop(node_id, None)
        if writer:
            writer.close()
        await self.send_control({
            "t": "PRUNE_NODE", "node_id": node_id, "code": code, "reason": reason,
        })

    async def reject_file(self, node_id: int, code: str, reason: str) -> None:
        self.db.add_rejected(node_id, code, reason)
        writer = self.writers.pop(node_id, None)
        if writer:
            writer.close()
        await self.send_control({
            "t": "REJECT_FILE", "node_id": node_id, "code": code, "reason": reason,
        })

    # -- flow control -------------------------------------------------------

    async def send_flow(self, paused: bool, reason: str) -> None:
        if paused:
            self.db.set_state(SessionState.PAUSED_BY_SERVER)
        elif self.state == SessionState.PAUSED_BY_SERVER:
            self.db.set_state(SessionState.ACTIVE)
        await self.send_control({"t": "FLOW", "paused": paused, "reason": reason})

    # -- commit -------------------------------------------------------------

    async def try_commit(self) -> Optional[dict]:
        """Attempt to commit: returns COMMIT_OK payload or None if not ready."""
        state = self.db.get_state()
        if state != SessionState.COMMITTING:
            return None

        file_count, total_bytes = self.db.count_committed_files()
        self.db.set_state(SessionState.COMMITTED)

        result = {"t": "COMMIT_OK", "files": file_count, "bytes": total_bytes}
        await self.send_control(result)
        return result

    # -- internal -----------------------------------------------------------

    def _close_all_writers(self) -> None:
        for w in self.writers.values():
            try:
                w.close()
            except Exception:
                pass
        self.writers.clear()

    def _reset_idle_timer(self) -> None:
        self._cancel_idle_timer()
        loop = asyncio.get_event_loop()
        self._idle_task = loop.call_later(self.leg_idle_timeout, self._on_idle)

    def _cancel_idle_timer(self) -> None:
        if self._idle_task:
            self._idle_task.cancel()
            self._idle_task = None

    def _on_idle(self) -> None:
        logger.info("Session %s leg idle timeout", self.session_id)
        self.detach_leg()

    def build_resume_ok(self) -> dict:
        """Build a RESUME_OK message from persisted state."""
        summary = self.db.get_root_summary()
        files_rows = self.db.get_all_files()
        files = []
        for row in files_rows:
            status_str = row["status"]
            if status_str == NodeStatus.REJECTED.value:
                s = "rejected"
            elif status_str == NodeStatus.CLOSED.value:
                s = "closed"
            else:
                s = "open"
            files.append({
                "node_id": row["node_id"],
                "accepted_offset": row["accepted_offset"],
                "status": s,
            })
        return {
            "t": "RESUME_OK",
            "epoch": self.epoch,
            "expires_at": self.expires_at,
            "root_summary": summary,
            "files": files,
            "pruned_nodes": self.db.get_pruned_nodes(),
            "rejected_files": self.db.get_rejected_files(),
        }


# ---------------------------------------------------------------------------
# Global session registry
# ---------------------------------------------------------------------------

class SessionRegistry:
    """Global in-memory registry of live sessions.

    No startup scan — stale staging dirs are cleaned by the Redis-based sweeper.
    """

    def __init__(self, base_dir: Path, staging_prefix: str = DEFAULT_STAGING_PREFIX, **defaults: Any) -> None:
        self.base_dir = base_dir
        self.staging_prefix = staging_prefix
        self.defaults = defaults
        self._sessions: dict[str, LiveSession] = {}
        self._lock = asyncio.Lock()

    async def create(
        self,
        session_id: str,
        resume_token: str,
        leg_id: str,
        expires_at: str,
        target_dir: str = ".",
    ) -> LiveSession:
        async with self._lock:
            if session_id in self._sessions:
                raise ValueError(f"session {session_id} already exists")
            db = open_session_db(self.base_dir, session_id, self.staging_prefix)
            db.init_session(session_id, resume_token, expires_at, target_dir)
            session = LiveSession(session_id, resume_token, self.base_dir, db, target_dir=target_dir, staging_prefix=self.staging_prefix, **self.defaults)
            session.attach_leg(leg_id)
            self._sessions[session_id] = session
            return session

    async def resume(
        self,
        session_id: str,
        resume_token: str,
        leg_id: str,
    ) -> LiveSession:
        async with self._lock:
            session = self._sessions.get(session_id)
            if session is None:
                raise KeyError(f"session {session_id} not found")
            if session.resume_token != resume_token:
                raise PermissionError("invalid resume token")
            state = session.state
            if state not in (SessionState.WAITING_RESUME, SessionState.ACTIVE, SessionState.PAUSED_BY_SERVER):
                raise ValueError(f"cannot resume session in state {state.value}")
            session.attach_leg(leg_id)
            return session

    def get(self, session_id: str) -> Optional[LiveSession]:
        return self._sessions.get(session_id)

    async def remove(self, session_id: str) -> None:
        async with self._lock:
            session = self._sessions.pop(session_id, None)
            if session:
                session.detach_leg()
                session.db.close()

    async def recover_session(
        self,
        session_id: str,
        staging_path: Path,
    ) -> LiveSession | None:
        """Recover a session from its SQLite DB on disk.

        Opens the DB, reads session row, and registers a detached LiveSession
        (no leg, no WS). Returns None if the DB is missing or unreadable.
        """
        db_path = staging_path / "state.sqlite"
        if not db_path.exists():
            logger.warning("Recovery: DB missing for session %s at %s", session_id, db_path)
            return None

        try:
            db = SessionDB(db_path)
        except Exception:
            logger.exception("Recovery: failed to open DB for session %s", session_id)
            return None

        row = db.get_session()
        if row is None:
            logger.warning("Recovery: empty sessions table for %s", session_id)
            db.close()
            return None

        state = SessionState(row["state"])
        # Only recover sessions that can still accept a RESUME
        if state not in (
            SessionState.ACTIVE,
            SessionState.PAUSED_BY_SERVER,
            SessionState.WAITING_RESUME,
            SessionState.COMMITTING,
        ):
            logger.info(
                "Recovery: skipping session %s in terminal state %s",
                session_id, state.value,
            )
            db.close()
            return None

        resume_token = row["resume_token"]
        target_dir = row["target_dir"]

        # Force state to WAITING_RESUME — no leg is attached after restart
        if state in (SessionState.ACTIVE, SessionState.PAUSED_BY_SERVER):
            db.set_state(SessionState.WAITING_RESUME)

        async with self._lock:
            if session_id in self._sessions:
                db.close()
                return self._sessions[session_id]

            session = LiveSession(
                session_id, resume_token, self.base_dir, db,
                target_dir=target_dir,
                staging_prefix=self.staging_prefix,
                **self.defaults,
            )
            self._sessions[session_id] = session
            logger.info(
                "Recovery: restored session %s (state=%s, epoch=%d, target=%s)",
                session_id, state.value, session.epoch, target_dir,
            )
            return session

    def all_sessions(self) -> dict[str, LiveSession]:
        return dict(self._sessions)
