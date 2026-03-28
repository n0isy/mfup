"""MFUP/2 server — FastAPI application with WebSocket control and HTTP data endpoints."""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, Response, status
from fastapi.responses import JSONResponse

from .protocol import PROTOCOL_VERSION, FrameReader, SessionEndFrame, SessionState
from .session_manager import SessionRegistry, LiveSession
from .publish import publish_session, ConflictError, sweep

logger = logging.getLogger("mfup.app")

# ---------------------------------------------------------------------------
# Configuration (overridable via env or constructor)
# ---------------------------------------------------------------------------

DEFAULT_BASE_DIR = Path(
    __import__("os").environ.get("MFUP_BASE_DIR", "/tmp/mfup-uploads")
)
SESSION_RESUME_TTL = int(__import__("os").environ.get("MFUP_SESSION_RESUME_TTL", "3600"))
LEG_IDLE_TIMEOUT = int(__import__("os").environ.get("MFUP_LEG_IDLE_TIMEOUT", "60"))
MAX_CHUNK_BYTES = int(__import__("os").environ.get("MFUP_MAX_CHUNK_BYTES", "262144"))
MAX_OPEN_FILES = int(__import__("os").environ.get("MFUP_MAX_OPEN_FILES", "1"))
MAX_PENDING_FILES = int(__import__("os").environ.get("MFUP_MAX_PENDING_FILES", "64"))
SWEEP_INTERVAL = int(__import__("os").environ.get("MFUP_SWEEP_INTERVAL", "300"))
STAGING_PREFIX = __import__("os").environ.get("MFUP_STAGING_PREFIX", ".incoming")

# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------

def _is_safe_target(base_dir: Path, target_dir: str) -> bool:
    """Check that target_dir resolves within base_dir (no path traversal)."""
    target = Path(target_dir)
    if target.is_absolute():
        resolved = target.resolve()
    else:
        resolved = (base_dir / target).resolve()
    try:
        resolved.relative_to(base_dir.resolve())
        return True
    except ValueError:
        return False


_registry: SessionRegistry | None = None
_sweep_task: asyncio.Task | None = None


def get_registry() -> SessionRegistry:
    assert _registry is not None
    return _registry


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _registry, _sweep_task
    base = DEFAULT_BASE_DIR
    base.mkdir(parents=True, exist_ok=True)

    _registry = SessionRegistry(
        base,
        staging_prefix=STAGING_PREFIX,
        session_resume_ttl=SESSION_RESUME_TTL,
        leg_idle_timeout=LEG_IDLE_TIMEOUT,
    )
    await _registry.recover()

    # Periodic sweeper
    async def sweeper():
        while True:
            await asyncio.sleep(SWEEP_INTERVAL)
            try:
                removed = sweep(base, STAGING_PREFIX)
                if removed:
                    for sid in removed:
                        await _registry.remove(sid)
                    logger.info("Sweeper removed %d sessions", len(removed))
            except Exception:
                logger.exception("Sweeper error")

    _sweep_task = asyncio.create_task(sweeper())

    yield

    _sweep_task.cancel()
    try:
        await _sweep_task
    except asyncio.CancelledError:
        pass


app = FastAPI(title="MFUP/2 Server", lifespan=lifespan)


# ---------------------------------------------------------------------------
# WebSocket control endpoint: /mfup/control
# ---------------------------------------------------------------------------

@app.websocket("/mfup/control")
async def control_endpoint(ws: WebSocket):
    await ws.accept()
    registry = get_registry()
    session: LiveSession | None = None

    try:
        # First message must be HELLO or RESUME
        msg = await ws.receive_json()
        t = msg.get("t")

        if t == "HELLO":
            if msg.get("v") != PROTOCOL_VERSION:
                await ws.send_json({
                    "t": "SESSION_ABORT",
                    "code": "bad_version",
                    "reason": f"expected {PROTOCOL_VERSION}",
                })
                await ws.close()
                return

            session_id = msg["session_id"]
            resume_token = msg["resume_token"]
            leg_id = msg["leg_id"]
            target_dir = msg.get("target_dir", ".")

            # Validate target_dir stays within base_dir
            if not _is_safe_target(registry.base_dir, target_dir):
                await ws.send_json({
                    "t": "SESSION_ABORT",
                    "code": "bad_target_dir",
                    "reason": "target_dir escapes base directory",
                })
                await ws.close()
                return

            expires = datetime.now(timezone.utc) + timedelta(seconds=SESSION_RESUME_TTL)
            try:
                session = await registry.create(
                    session_id, resume_token, leg_id, expires.isoformat(),
                    target_dir=target_dir,
                )
            except ValueError:
                # Session already exists — treat as conflict
                await ws.send_json({
                    "t": "SESSION_ABORT",
                    "code": "conflict",
                    "reason": "session already exists",
                })
                await ws.close()
                return

            session.ws = ws
            await ws.send_json({
                "t": "HELLO_OK",
                "epoch": session.epoch,
                "expires_at": session.expires_at,
                "limits": {
                    "max_chunk_bytes": MAX_CHUNK_BYTES,
                    "max_open_files": MAX_OPEN_FILES,
                    "max_pending_files": MAX_PENDING_FILES,
                },
            })

        elif t == "RESUME":
            session_id = msg["session_id"]
            resume_token = msg["resume_token"]
            leg_id = msg["leg_id"]

            try:
                session = await registry.resume(session_id, resume_token, leg_id)
            except KeyError:
                await ws.send_json({
                    "t": "SESSION_ABORT",
                    "code": "not_found",
                    "reason": "session not found or expired",
                })
                await ws.close()
                return
            except PermissionError:
                await ws.send_json({
                    "t": "SESSION_ABORT",
                    "code": "auth_failed",
                    "reason": "invalid resume token",
                })
                await ws.close()
                return
            except ValueError as exc:
                await ws.send_json({
                    "t": "SESSION_ABORT",
                    "code": "invalid_state",
                    "reason": str(exc),
                })
                await ws.close()
                return

            session.ws = ws
            resume_ok = session.build_resume_ok()
            await ws.send_json(resume_ok)

        else:
            await ws.send_json({
                "t": "SESSION_ABORT",
                "code": "protocol_error",
                "reason": f"expected HELLO or RESUME, got {t}",
            })
            await ws.close()
            return

        # Main control loop — listen for CLIENT_ABORT or other client messages
        while True:
            msg = await ws.receive_json()
            t = msg.get("t")

            if t == "CLIENT_ABORT":
                if session:
                    session.db.set_state(SessionState.ABORTED)
                    session.detach_leg()
                break

            if t == "ACTION":
                action = msg.get("action")
                if session and action in ("merge_overwrite", "cancel"):
                    session.publish_action = action
                    logger.info("Session %s: ACTION=%s", session.session_id, action)
                    if action == "cancel":
                        session.db.set_state(SessionState.ABORTED)
                        session.detach_leg()
                        break

    except WebSocketDisconnect:
        logger.info("Control WS disconnected for session %s (state=%s)",
                     session.session_id if session else "unknown",
                     session.state.value if session else "n/a")
    except Exception:
        logger.exception("Control WS error for session %s",
                        session.session_id if session else "unknown")
    finally:
        if session:
            # If session is in COMMITTING state, attempt commit before detaching
            if session.state == SessionState.COMMITTING:
                await session.try_commit()
            if session.ws is ws:
                session.ws = None
            if session.leg_id and session.state not in (
                SessionState.COMMITTED, SessionState.ABORTED,
            ):
                session.detach_leg()


# ---------------------------------------------------------------------------
# HTTP data endpoint: POST /mfup/data/{session_id}/{leg_id}
# ---------------------------------------------------------------------------

@app.post("/mfup/data/{session_id}/{leg_id}")
async def data_endpoint(session_id: str, leg_id: str, request: Request, seq: int, final: int = 0):
    registry = get_registry()
    session = registry.get(session_id)

    if session is None:
        return JSONResponse(
            {"error": "session not found"},
            status_code=status.HTTP_410_GONE,
        )

    if session.leg_id != leg_id:
        return JSONResponse(
            {"error": "stale leg", "expected": session.leg_id},
            status_code=status.HTTP_409_CONFLICT,
        )

    state = session.state
    if state not in (SessionState.ACTIVE, SessionState.PAUSED_BY_SERVER):
        return JSONResponse(
            {"error": f"session in state {state.value}"},
            status_code=status.HTTP_409_CONFLICT,
        )

    # Validate sequence number
    if not session.validate_and_advance_seq(seq):
        logger.warning(
            "Seq gap/duplicate for session %s leg %s: got seq=%d, expected=%d",
            session_id, leg_id, seq, session.last_data_seq + 1,
        )
        return JSONResponse(
            {"error": "seq_mismatch", "got": seq, "expected": session.last_data_seq + 1},
            status_code=status.HTTP_409_CONFLICT,
        )

    reader = FrameReader()
    body_received = 0
    frame_count = 0
    session_end_seen = False
    error_detail: str | None = None

    try:
        async for chunk in request.stream():
            if session.leg_id != leg_id:
                logger.warning("Data stream for stale leg %s seq=%d, aborting read", leg_id, seq)
                break

            reader.feed(chunk)
            frames = reader.drain()
            for frame in frames:
                await session.process_frame(frame, leg_id)
                frame_count += 1
                if isinstance(frame, SessionEndFrame):
                    session_end_seen = True
            body_received += len(chunk)

    except Exception as exc:
        error_detail = f"{type(exc).__name__}: {exc}"
        logger.exception("Data stream error for session %s leg %s seq=%d", session_id, leg_id, seq)

    logger.info(
        "Data stream ended for session %s leg %s seq=%d: %d bytes, %d frames, final=%d%s",
        session_id, leg_id, seq, body_received, frame_count, final,
        f", error: {error_detail}" if error_detail else "",
    )

    # Only attempt commit when final=1 or SESSION_END was found in frames
    if (final == 1 or session_end_seen) and session.state == SessionState.COMMITTING:
        await session.try_commit()

    if error_detail:
        return JSONResponse(
            {"error": "data_stream_error", "detail": error_detail, "bytes_received": body_received},
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )

    return JSONResponse(
        {"ok": True, "bytes_received": body_received, "frames": frame_count},
        status_code=status.HTTP_200_OK,
    )


# ---------------------------------------------------------------------------
# HTTP probe endpoint: POST /mfup/probe/{session_id}
# ---------------------------------------------------------------------------

@app.post("/mfup/probe/{session_id}")
async def probe_endpoint(session_id: str, request: Request):
    registry = get_registry()
    session = registry.get(session_id)

    if session is None:
        return JSONResponse(
            {"error": "session not found"},
            status_code=status.HTTP_410_GONE,
        )

    state = session.state
    if state in (SessionState.COMMITTED, SessionState.ABORTED, SessionState.EXPIRED):
        return JSONResponse(
            {"error": f"session in terminal state {state.value}"},
            status_code=status.HTTP_410_GONE,
        )

    # Read the request body stream; send PROBE_ACK after the first chunk arrives.
    # Include first_chunk_bytes so the client can verify real binary data arrived
    # (Firefox stringifies ReadableStream to "[object ReadableStream]" = 23 bytes).
    probe_ack_sent = False
    total_bytes = 0
    async for chunk in request.stream():
        total_bytes += len(chunk)
        if not probe_ack_sent:
            if session.ws:
                try:
                    await session.ws.send_json({
                        "t": "PROBE_ACK",
                        "first_chunk_bytes": len(chunk),
                    })
                except Exception as exc:
                    logger.error("Failed to send PROBE_ACK for session %s: %s", session_id, exc)
            probe_ack_sent = True

    return JSONResponse(
        {"ok": True, "total_bytes": total_bytes},
        status_code=status.HTTP_200_OK,
    )


# ---------------------------------------------------------------------------
# Admin / status endpoints
# ---------------------------------------------------------------------------

@app.get("/mfup/sessions")
async def list_sessions():
    """List all active sessions (admin/debug)."""
    registry = get_registry()
    result = []
    for sid, session in registry.all_sessions().items():
        result.append({
            "session_id": sid,
            "state": session.state.value,
            "epoch": session.epoch,
            "leg_id": session.leg_id,
            "expires_at": session.expires_at,
        })
    return result


@app.get("/mfup/sessions/{session_id}")
async def get_session_status(session_id: str):
    """Get detailed status for a single session."""
    registry = get_registry()
    session = registry.get(session_id)
    if session is None:
        return JSONResponse({"error": "not found"}, status_code=404)

    summary = session.db.get_root_summary()
    return {
        "session_id": session_id,
        "state": session.state.value,
        "epoch": session.epoch,
        "leg_id": session.leg_id,
        "expires_at": session.expires_at,
        "root_summary": summary,
    }


@app.post("/mfup/sessions/{session_id}/publish")
async def publish_endpoint(session_id: str):
    """Publish a committed session — atomic rename payload entries to target_dir."""
    registry = get_registry()
    session = registry.get(session_id)
    if session is None:
        return JSONResponse({"error": "not found"}, status_code=404)

    if session.state != SessionState.COMMITTED:
        return JSONResponse(
            {"error": f"cannot publish session in state {session.state.value}"},
            status_code=status.HTTP_409_CONFLICT,
        )

    # Resolve target_dir: relative paths are under base_dir
    target = Path(session.target_dir)
    if not target.is_absolute():
        target = registry.base_dir / target

    # Defense in depth: verify target stays within base_dir
    if not _is_safe_target(registry.base_dir, session.target_dir):
        return JSONResponse(
            {"error": "target_dir escapes base directory"},
            status_code=status.HTTP_403_FORBIDDEN,
        )

    try:
        published = publish_session(
            registry.base_dir, session_id, target, STAGING_PREFIX,
            action=session.publish_action,
        )
    except ConflictError as exc:
        return JSONResponse(
            {"error": "conflict_files", "conflicting_files": exc.conflicting_files},
            status_code=status.HTTP_409_CONFLICT,
        )
    except FileNotFoundError as exc:
        return JSONResponse({"error": str(exc)}, status_code=404)

    await registry.remove(session_id)
    return {"published": published}


@app.post("/mfup/sweep")
async def sweep_endpoint():
    """Manually trigger a sweep."""
    registry = get_registry()
    removed = sweep(registry.base_dir, STAGING_PREFIX)
    for sid in removed:
        await registry.remove(sid)
    return {"removed": removed}


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

@app.get("/health")
async def health():
    return {"status": "ok", "protocol": PROTOCOL_VERSION}
