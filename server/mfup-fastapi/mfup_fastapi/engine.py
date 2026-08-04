"""MfupEngine — the FastAPI-facing shell around the mfup-core engine.

Embedding into an existing application:

    from fastapi import FastAPI
    from mfup_fastapi import MfupConfig, MfupEngine

    engine = MfupEngine(MfupConfig(
        base_dir=Path("/srv/uploads"),
        redis_url="redis://localhost:6379/0",
        authorize=my_authorize,          # a callable — or "pkg.mod:func"
    ))
    app = FastAPI(lifespan=engine.lifespan)
    app.include_router(engine.router, prefix="/api/uploads")

The router keeps the protocol's fixed "/mfup/*" namespace under whatever
prefix it is mounted at; point the browser client's serverUrl at the same
prefix ("https://host/api/uploads") and every path lines up. No module-level
state: two engines in one process are two independent instances.

The standalone server (``python -m mfup_fastapi`` / mfup_fastapi.app:app)
is a thin wrapper: ``create_app(MfupConfig.from_env())``.
"""

from __future__ import annotations

import asyncio
import logging
import json
import secrets
import shutil
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, AsyncIterator, Optional

from fastapi import APIRouter, FastAPI, Request, WebSocket, WebSocketDisconnect, status
from fastapi.responses import JSONResponse

from mfup_core.hooks import (
    AuthRequest,
    AuthorizeHook,
    CommitEvent,
    FileMapRequest,
    MapFileHook,
    OnCommittedHook,
    load_authorize_hook,
    resolve_hook,
)
from mfup_core.protocol import CRC32C_IMPL, PROTOCOL_VERSION, FrameReader, SessionEndFrame, SessionState
from mfup_core.session_manager import SessionRegistry, LiveSession
from mfup_core.publish import (
    ConflictError,
    MappingError,
    list_payload_files,
    publish_session,
    publish_session_mapped,
)
from mfup_core.redis_index import SessionIndex
from mfup_core.storage import staging_dir

from .config import MfupConfig

logger = logging.getLogger("mfup.engine")


# ---------------------------------------------------------------------------
# Typed publish errors — raised by MfupEngine.publish(), mapped to HTTP by
# the endpoint; consumer backends calling publish() directly catch them.
# ---------------------------------------------------------------------------

class PublishError(Exception):
    pass


class SessionNotFound(PublishError):
    pass


class NotCommitted(PublishError):
    def __init__(self, state: str) -> None:
        super().__init__(f"cannot publish session in state {state}")
        self.state = state


class TargetEscapes(PublishError):
    pass


class MapFileHookError(PublishError):
    def __init__(self, path: str) -> None:
        super().__init__(f"map_file hook raised for {path}")
        self.path = path


# ---------------------------------------------------------------------------
# Retention safety net (kept as a free function — tests exercise it directly)
# ---------------------------------------------------------------------------

async def reconcile_orphans(
    base: Path,
    registry: SessionRegistry,
    index: SessionIndex,
    prefix: str,
    grace_seconds: int = 600,
) -> list[str]:
    """Remove staging dirs that no live session and no Redis entry reference.

    The zset-driven sweeper can only clean sessions Redis still knows about.
    A staging dir becomes an unreachable orphan when cleanup is interrupted
    (crash between rmtree and ZREM), when rmtree silently failed
    (ignore_errors), or when Redis lost the entry (flush/eviction). Such a
    dir can never be resumed (resume needs the session in the registry, which
    at startup is populated only from Redis) nor swept — so it would
    accumulate forever. This scan is the retention safety net.

    An orphan is removed only when ALL hold, to avoid racing a live upload:
      - not in the in-memory registry,
      - not scored in the Redis session zset,
      - last modified at least `grace_seconds` ago.
    """
    removed: list[str] = []
    if not base.exists():
        return removed

    now = time.time()
    for entry in base.iterdir():
        name = entry.name
        if not entry.is_dir() or not name.startswith(f"{prefix}."):
            continue
        sid = name[len(prefix) + 1:]
        if registry.get(sid) is not None:
            continue  # live in this process
        try:
            if await index.is_registered(sid):
                continue  # Redis still tracks it — sweeper owns it
        except Exception:
            # Redis unreachable — do not delete anything we cannot verify.
            logger.warning("Reconcile: cannot verify session %s in Redis, skipping", sid)
            continue
        try:
            age = now - entry.stat().st_mtime
        except OSError:
            continue
        if age < grace_seconds:
            continue  # too fresh — might be a session mid-registration

        shutil.rmtree(str(entry), ignore_errors=True)
        # Belt and suspenders: drop any half-written Redis entry too.
        try:
            await index.remove(sid)
        except Exception:
            pass
        removed.append(sid)

    return removed


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


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------

class MfupEngine:
    """Owns the registry, the Redis index, the sweeper, the hooks, and an
    APIRouter with every MFUP endpoint. All state is instance state."""

    def __init__(self, config: MfupConfig) -> None:
        self.cfg = config

        # Hooks: callables pass through, dotted paths import loudly at
        # construction time — a misconfigured hook must not boot.
        if config.authorize is None:
            self.authorize: AuthorizeHook | None = load_authorize_hook(None)  # logs the allow-all warning
        else:
            self.authorize = resolve_hook(config.authorize)  # type: ignore[assignment]
        self.map_file: MapFileHook | None = resolve_hook(config.map_file)  # type: ignore[assignment]
        self.on_committed: OnCommittedHook | None = resolve_hook(config.on_committed)  # type: ignore[assignment]

        self.registry: SessionRegistry | None = None
        self.index: SessionIndex | None = None
        self._sweep_task: asyncio.Task | None = None

        self.router: APIRouter = self._build_router()

    # -- lifecycle -----------------------------------------------------------

    async def startup(self) -> None:
        cfg = self.cfg
        cfg.base_dir.mkdir(parents=True, exist_ok=True)

        self.index = SessionIndex(cfg.redis_url)
        self.registry = SessionRegistry(
            cfg.base_dir,
            staging_prefix=cfg.staging_prefix,
            session_resume_ttl=cfg.session_resume_ttl,
            leg_idle_timeout=cfg.leg_idle_timeout,
            max_chunk_bytes=cfg.max_chunk_bytes,
            # With a map_file hook the client's layout no longer predicts
            # final paths — the ingest-time conflict ASK would be noise;
            # publish-time conflict handling (409 → action) takes over.
            conflict_check=(self.map_file is None),
        )

        # Recover live sessions from Redis index → SQLite DBs
        try:
            alive_ids = await self.index.get_not_expired()
            expired_ids = await self.index.get_expired()
            logger.info(
                "Startup: Redis has %d alive + %d expired session(s)",
                len(alive_ids), len(expired_ids),
            )
            recovered = 0
            for sid in alive_ids:
                meta = await self.index.get_meta(sid)
                if not meta or not meta.staging_dir:
                    logger.warning("Startup: no meta for session %s, skipping", sid)
                    continue
                session = await self.registry.recover_session(sid, Path(meta.staging_dir))
                if session:
                    idx = self.index
                    session._on_expiry_change = lambda s, e, _idx=idx: _idx.update_expiry(s, e)
                    recovered += 1
            logger.info("Startup: recovered %d session(s) from disk", recovered)
        except Exception:
            logger.exception("Startup: session recovery failed")

        # One filesystem reconciliation at startup: catches staging dirs
        # orphaned by a crash between rmtree and Redis-remove, a silently-
        # failed rmtree, or a Redis flush — none of which the zset-driven
        # sweeper can ever find.
        try:
            removed = await reconcile_orphans(
                cfg.base_dir, self.registry, self.index, cfg.staging_prefix,
                cfg.orphan_grace_seconds,
            )
            if removed:
                logger.warning("Startup: reconciled %d orphaned staging dir(s): %s", len(removed), removed)
        except Exception:
            logger.exception("Startup: orphan reconciliation failed")

        self._sweep_task = asyncio.create_task(self._sweeper())

    async def shutdown(self) -> None:
        if self._sweep_task is not None:
            self._sweep_task.cancel()
            try:
                await self._sweep_task
            except asyncio.CancelledError:
                pass
            self._sweep_task = None
        if self.index is not None:
            await self.index.close()

    @asynccontextmanager
    async def lifespan(self, app: FastAPI) -> AsyncIterator[None]:
        """Drop-in FastAPI lifespan: ``FastAPI(lifespan=engine.lifespan)``.

        Consumers with their own lifespan call startup()/shutdown() from it
        instead.
        """
        await self.startup()
        try:
            yield
        finally:
            await self.shutdown()

    def _require_registry(self) -> SessionRegistry:
        assert self.registry is not None, "MfupEngine.startup() has not run"
        return self.registry

    def _require_index(self) -> SessionIndex:
        assert self.index is not None, "MfupEngine.startup() has not run"
        return self.index

    # -- periodic sweeper ------------------------------------------------------

    async def _sweeper(self) -> None:
        cfg = self.cfg
        sweeps = 0
        while True:
            await asyncio.sleep(cfg.sweep_interval)
            try:
                registry = self._require_registry()
                index = self._require_index()
                expired_ids = await index.get_expired()
                for sid in expired_ids:
                    await registry.remove(sid)
                    meta = await index.get_meta(sid)
                    if meta and meta.staging_dir:
                        sd = Path(meta.staging_dir)
                    else:
                        sd = staging_dir(cfg.base_dir, sid, cfg.staging_prefix)
                    if sd.exists():
                        shutil.rmtree(str(sd), ignore_errors=True)
                    await index.remove(sid)
                    logger.info("Sweeper cleaned session %s (staging=%s)", sid, sd)
                if expired_ids:
                    logger.info("Sweeper removed %d sessions", len(expired_ids))

                sweeps += 1
                if sweeps % cfg.reconcile_every == 0:
                    orphans = await reconcile_orphans(
                        cfg.base_dir, registry, index, cfg.staging_prefix,
                        cfg.orphan_grace_seconds,
                    )
                    if orphans:
                        logger.warning("Sweeper reconciled %d orphaned staging dir(s): %s", len(orphans), orphans)
            except Exception:
                logger.exception("Sweeper error")

    # -- programmatic publish ---------------------------------------------------

    async def publish(self, session_id: str) -> list[str]:
        """Publish a committed session server-side (consumer backends and the
        on_committed auto-publish path). Raises typed PublishError subclasses;
        ConflictError/MappingError from mfup-core pass through."""
        registry = self._require_registry()
        session = registry.get(session_id)
        if session is None:
            raise SessionNotFound(session_id)
        if session.state != SessionState.COMMITTED:
            raise NotCommitted(session.state.value)

        target = Path(session.target_dir)
        if not target.is_absolute():
            target = session.base_dir / target

        # Defense in depth: verify target stays within the session's base dir
        if not _is_safe_target(session.base_dir, session.target_dir):
            raise TargetEscapes(session.target_dir)

        if self.map_file is not None:
            # Per-file layout is the consumer's: run the (async) hook per
            # file first, then hand the precomputed plan to the sync mover.
            files = await asyncio.to_thread(
                list_payload_files, session.base_dir, session_id, self.cfg.staging_prefix,
            )
            mapping: dict[str, str] = {}
            for rel, size in files:
                try:
                    mapped = await self.map_file(FileMapRequest(
                        session_id=session_id,
                        path=rel,
                        name=rel.rsplit("/", 1)[-1],
                        size=size,
                        target_dir=session.target_dir,
                        meta=session.client_meta,
                        context=session.auth_context,
                    ))
                except Exception:
                    logger.exception("map_file hook raised for %s (%s)", session_id, rel)
                    raise MapFileHookError(rel) from None
                if mapped is not None:
                    mapping[rel] = mapped
            published = await asyncio.to_thread(
                publish_session_mapped,
                session.base_dir, session_id, target, mapping, self.cfg.staging_prefix,
                session.publish_action,
            )
        else:
            # publish_session is synchronous filesystem work (renames / merge
            # walks) — keep it off the event loop.
            published = await asyncio.to_thread(
                publish_session,
                session.base_dir, session_id, target, self.cfg.staging_prefix,
                session.publish_action,
            )

        await registry.remove(session_id)
        await self._require_index().remove(session_id)
        return published

    # -- on_committed ----------------------------------------------------------

    async def _after_commit(self, session: LiveSession, result: dict) -> None:
        """Fire the consumer's on_committed hook once per commit. A hook error
        must never damage the session; 'publish' return triggers server-side
        publish immediately."""
        if self.on_committed is None:
            return
        sd = staging_dir(session.base_dir, session.session_id, self.cfg.staging_prefix)
        try:
            verdict = await self.on_committed(CommitEvent(
                session_id=session.session_id,
                target_dir=session.target_dir,
                base_dir=str(session.base_dir),
                staging_dir=str(sd),
                files=result["files"],
                bytes=result["bytes"],
                meta=session.client_meta,
                context=session.auth_context,
            ))
        except Exception:
            logger.exception("on_committed hook raised for session %s", session.session_id)
            return
        if verdict == "publish":
            try:
                published = await self.publish(session.session_id)
                logger.info(
                    "on_committed auto-published session %s: %s",
                    session.session_id, published,
                )
            except Exception:
                logger.exception("on_committed auto-publish failed for session %s", session.session_id)

    async def _try_commit(self, session: LiveSession) -> Optional[dict]:
        """try_commit + on_committed, so no call site can forget the hook."""
        result = await session.try_commit()
        if result is not None and session.state == SessionState.COMMITTED:
            await self._after_commit(session, result)
        return result

    # -- router ----------------------------------------------------------------

    def _build_router(self) -> APIRouter:  # noqa: C901 — one closure per endpoint
        router = APIRouter()
        engine = self
        cfg = self.cfg

        # -------------------------------------------------------------------
        # WebSocket control endpoint: /mfup/control
        # -------------------------------------------------------------------

        @router.websocket("/mfup/control")
        async def control_endpoint(ws: WebSocket) -> None:
            await ws.accept()
            registry = engine._require_registry()
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
                    leg_id = msg["leg_id"]
                    target_dir = msg.get("target_dir", ".")
                    # The resume token is SERVER-issued (returned in HELLO_OK)
                    # — a client-chosen token would be a self-signed
                    # credential, useless as an authenticator for the
                    # data/publish endpoints.
                    resume_token = secrets.token_urlsafe(32)

                    # Client-attached session metadata (scope/purpose/ids).
                    # Untrusted; size-capped; handed to authorize + map_file.
                    client_meta = msg.get("meta")
                    meta_json: str | None = None
                    if client_meta is not None:
                        meta_json = json.dumps(client_meta, ensure_ascii=False)
                        if len(meta_json.encode()) > cfg.max_meta_bytes:
                            await ws.send_json({
                                "t": "SESSION_ABORT",
                                "code": "protocol_error",
                                "reason": f"meta exceeds {cfg.max_meta_bytes} bytes",
                            })
                            await ws.close()
                            return

                    # Consumer authorization (see mfup_core.hooks). Runs
                    # BEFORE anything is created. Deny → auth_failed.
                    auth_result = None
                    if engine.authorize is not None:
                        try:
                            auth_result = await engine.authorize(AuthRequest(
                                session_id=session_id,
                                target_dir=target_dir,
                                headers=dict(ws.headers),
                                client=f"{ws.client.host}:{ws.client.port}" if ws.client else "",
                                query=dict(ws.query_params),
                                meta=client_meta,
                            ))
                        except Exception:
                            logger.exception("Authorize hook raised for session %s — denying", session_id)
                            auth_result = None
                        if auth_result is None:
                            await ws.send_json({
                                "t": "SESSION_ABORT",
                                "code": "auth_failed",
                                "reason": "authorization denied",
                            })
                            await ws.close()
                            return
                        if auth_result.target_dir is not None:
                            # The hook may pin or MAP the target (it received
                            # the client-requested value in req.target_dir);
                            # still subject to the containment check below.
                            target_dir = auth_result.target_dir

                    # Per-session base directory (e.g. the user's home).
                    # Staging is created INSIDE it so publish remains a
                    # same-filesystem rename even on separate mounts.
                    session_base = registry.base_dir
                    if auth_result is not None and auth_result.base_dir is not None:
                        candidate = Path(auth_result.base_dir)
                        if not candidate.is_absolute():
                            logger.error(
                                "Authorize hook returned a relative base_dir %r for session %s — denying",
                                auth_result.base_dir, session_id,
                            )
                            await ws.send_json({
                                "t": "SESSION_ABORT",
                                "code": "auth_failed",
                                "reason": "authorization misconfigured (relative base_dir)",
                            })
                            await ws.close()
                            return
                        session_base = candidate
                        session_base.mkdir(parents=True, exist_ok=True)

                    # Validate target_dir stays within the session's base dir
                    if not _is_safe_target(session_base, target_dir):
                        await ws.send_json({
                            "t": "SESSION_ABORT",
                            "code": "bad_target_dir",
                            "reason": "target_dir escapes base directory",
                        })
                        await ws.close()
                        return

                    expires = datetime.now(timezone.utc) + timedelta(seconds=cfg.session_resume_ttl)
                    try:
                        session = await registry.create(
                            session_id, resume_token, leg_id, expires.isoformat(),
                            target_dir=target_dir,
                            base_dir=session_base if session_base is not registry.base_dir else None,
                            meta_json=meta_json,
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

                    # Register in Redis index for TTL-based cleanup (with
                    # paths). A Redis failure here must not leave an orphaned
                    # in-memory session behind (a retried HELLO would then hit
                    # "conflict"), so roll back and abort explicitly.
                    sd = staging_dir(session_base, session_id, cfg.staging_prefix)
                    try:
                        idx = engine._require_index()
                        # Absolute staging path in Redis meta is what makes
                        # sweeper / lazy-resume work for per-user base dirs.
                        await idx.register(session_id, expires, target_dir, str(sd))
                        session._on_expiry_change = lambda sid, exp: idx.update_expiry(sid, exp)
                    except Exception:
                        logger.exception("Redis register failed for session %s — rolling back", session_id)
                        await registry.remove(session_id)
                        # Without a Redis entry the sweeper would never find
                        # this staging dir — remove it now.
                        shutil.rmtree(str(sd), ignore_errors=True)
                        session = None
                        await ws.send_json({
                            "t": "SESSION_ABORT",
                            "code": "server_error",
                            "reason": "session index unavailable, retry later",
                        })
                        await ws.close()
                        return

                    # Apply authorize-hook constraints to the live session
                    # (persisted — they must survive restarts / lazy-resume).
                    if auth_result is not None:
                        session.apply_auth(
                            auth_result.max_total_bytes,
                            auth_result.max_files,
                            auth_result.context,
                        )

                    session.ws = ws
                    await ws.send_json({
                        "t": "HELLO_OK",
                        "epoch": session.epoch,
                        "expires_at": session.expires_at,
                        # Server-issued bearer token for data/probe/publish/RESUME.
                        "resume_token": resume_token,
                        "limits": {
                            "max_chunk_bytes": cfg.max_chunk_bytes,
                            "max_open_files": cfg.max_open_files,
                            "max_pending_files": cfg.max_pending_files,
                        },
                    })

                elif t == "RESUME":
                    session_id = msg["session_id"]
                    resume_token = msg["resume_token"]
                    leg_id = msg["leg_id"]

                    try:
                        try:
                            session = await registry.resume(session_id, resume_token, leg_id)
                        except KeyError:
                            # Lazy recovery: the session is not in THIS
                            # process's memory (different worker after a
                            # deploy/failover), but its durable state may
                            # still exist — Redis meta points at the staging
                            # dir. Ownership transfers to whichever worker
                            # holds the WS.
                            meta = await engine._require_index().get_meta(session_id)
                            if not meta or not meta.staging_dir:
                                raise
                            recovered = await registry.recover_session(session_id, Path(meta.staging_dir))
                            if recovered is None:
                                raise
                            logger.info("Lazy-recovered session %s for RESUME", session_id)
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
                    # Ensure expiry callback is wired for resumed sessions
                    idx = engine._require_index()
                    session._on_expiry_change = lambda sid, exp: idx.update_expiry(sid, exp)
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

                # Main control loop — CLIENT_ABORT / ACTION
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
                    # If in COMMITTING state, attempt commit before detaching
                    if session.state == SessionState.COMMITTING:
                        await engine._try_commit(session)
                    if session.ws is ws:
                        session.ws = None
                    if session.leg_id and session.state not in (
                        SessionState.COMMITTED, SessionState.ABORTED,
                    ):
                        session.detach_leg()
                    # Clean up aborted sessions immediately: staging + Redis
                    if session.state == SessionState.ABORTED:
                        sid = session.session_id
                        await registry.remove(sid)
                        idx = engine._require_index()
                        meta = await idx.get_meta(sid)
                        if meta and meta.staging_dir:
                            sd = Path(meta.staging_dir)
                        else:
                            # session.base_dir, not the global one: per-user homes.
                            sd = staging_dir(session.base_dir, sid, cfg.staging_prefix)
                        if sd.exists():
                            shutil.rmtree(str(sd), ignore_errors=True)
                        await idx.remove(sid)
                        logger.info("Cleaned up aborted session %s (staging=%s)", sid, sd)

        # -------------------------------------------------------------------
        # HTTP data endpoint: POST /mfup/data/{session_id}/{leg_id}
        # -------------------------------------------------------------------

        @router.post("/mfup/data/{session_id}/{leg_id}")
        async def data_endpoint(session_id: str, leg_id: str, request: Request, seq: int, final: int = 0, epoch: int = -1):
            registry = engine._require_registry()
            session = registry.get(session_id)

            if session is None:
                return JSONResponse(
                    {"error": "session not found"},
                    status_code=status.HTTP_410_GONE,
                )

            # Verify data channel auth token
            token = request.headers.get("x-mfup-token")
            if not token or token != session.resume_token:
                return JSONResponse(
                    {"error": "invalid token"},
                    status_code=status.HTTP_403_FORBIDDEN,
                )

            if session.leg_id != leg_id:
                return JSONResponse(
                    {"error": "stale leg", "expected": session.leg_id},
                    status_code=status.HTTP_409_CONFLICT,
                )

            # Epoch is mandatory: an old client omitting ?epoch= must not
            # silently bypass stale-POST fencing.
            if epoch < 0:
                return JSONResponse(
                    {"error": "epoch_required"},
                    status_code=status.HTTP_400_BAD_REQUEST,
                )

            # Reject requests from stale epochs (old reconnects / retries)
            if session.epoch != epoch:
                logger.warning(
                    "Rejected stale epoch for session %s: got %d, current %d",
                    session_id, epoch, session.epoch,
                )
                return JSONResponse(
                    {"error": "stale_epoch", "got": epoch, "expected": session.epoch},
                    status_code=status.HTTP_409_CONFLICT,
                )

            state = session.state
            if state not in (SessionState.ACTIVE, SessionState.PAUSED_BY_SERVER):
                return JSONResponse(
                    {"error": f"session in state {state.value}"},
                    status_code=status.HTTP_409_CONFLICT,
                )

            # Reject POSTs after final=1 has already been received for this leg
            if session.final_seq_seen:
                logger.warning(
                    "Rejected POST after final for session %s leg %s seq=%d",
                    session_id, leg_id, seq,
                )
                return JSONResponse(
                    {"error": "data_after_final", "detail": "final POST already received"},
                    status_code=status.HTTP_409_CONFLICT,
                )

            # Validate sequence number. Advancing is deferred: for buffered
            # (batch) POSTs seq only advances after the body was FULLY
            # processed, so a client may retry a failed POST with the same
            # seq. A duplicate of an already processed POST then gets
            # seq_mismatch with expected == seq + 1, which the client
            # interprets as "already delivered".
            if not session.validate_seq(seq):
                logger.warning(
                    "Seq gap/duplicate for session %s leg %s: got seq=%d, expected=%d",
                    session_id, leg_id, seq, session.last_data_seq + 1,
                )
                return JSONResponse(
                    {"error": "seq_mismatch", "got": seq, "expected": session.last_data_seq + 1},
                    status_code=status.HTTP_409_CONFLICT,
                )

            # Two body-handling strategies:
            #  - Buffered (atomic): Content-Length known and small enough —
            #    read the whole body first, process after. Either the entire
            #    POST is applied (and seq advances) or none of it is.
            #  - Streaming: chunked/oversized body (the duplex:"half" long
            #    POST). Frames apply as they arrive; a broken stream recovers
            #    through RESUME (new leg, new epoch), never retried by seq.
            content_length = request.headers.get("content-length")
            buffered = content_length is not None and int(content_length) <= cfg.max_buffered_body

            reader = FrameReader()
            body_received = 0
            frame_count = 0
            session_end_seen = False
            error_detail: str | None = None

            if buffered:
                body = bytearray()
                try:
                    async for chunk in request.stream():
                        body.extend(chunk)
                        if len(body) > cfg.max_buffered_body:
                            return JSONResponse(
                                {"error": "body_too_large"},
                                status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                            )
                except Exception as exc:
                    # Body never fully arrived — nothing was applied, seq not
                    # advanced, the client may retry this POST verbatim.
                    logger.warning(
                        "Buffered body read failed for session %s leg %s seq=%d: %s",
                        session_id, leg_id, seq, exc,
                    )
                    return JSONResponse(
                        {"error": "body_read_failed"},
                        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    )

                # Re-check freshness after the (awaited) body read.
                if session.leg_id != leg_id or session.epoch != epoch:
                    return JSONResponse(
                        {"error": "stale_epoch", "got": epoch, "expected": session.epoch},
                        status_code=status.HTTP_409_CONFLICT,
                    )

                session.db.begin_batch()
                try:
                    reader.feed(bytes(body))
                    frames = reader.drain()
                    for frame in frames:
                        await session.process_frame(frame, leg_id)
                        frame_count += 1
                        if isinstance(frame, SessionEndFrame):
                            session_end_seen = True
                    body_received = len(body)
                except Exception as exc:
                    error_detail = f"{type(exc).__name__}: {exc}"
                    logger.exception("Frame processing error for session %s leg %s seq=%d", session_id, leg_id, seq)
                finally:
                    session.db.end_batch()

                if error_detail is None:
                    session.advance_seq(seq)
                    if final == 1:
                        session.final_seq_seen = True
            else:
                # Streaming path: advance immediately (no retry-by-seq here).
                session.advance_seq(seq)
                if final == 1:
                    session.final_seq_seen = True

                session.db.begin_batch()
                frames_since_flush = 0
                try:
                    async for chunk in request.stream():
                        if session.leg_id != leg_id or session.epoch != epoch:
                            logger.warning("Data stream for stale leg/epoch %s seq=%d, aborting read", leg_id, seq)
                            break

                        reader.feed(chunk)
                        frames = reader.drain()
                        for frame in frames:
                            await session.process_frame(frame, leg_id)
                            frame_count += 1
                            frames_since_flush += 1
                            if isinstance(frame, SessionEndFrame):
                                session_end_seen = True
                        if frames_since_flush >= 500:
                            session.db.flush()
                            frames_since_flush = 0
                        body_received += len(chunk)

                except Exception as exc:
                    error_detail = f"{type(exc).__name__}: {exc}"
                    logger.exception("Data stream error for session %s leg %s seq=%d", session_id, leg_id, seq)
                finally:
                    session.db.end_batch()

            logger.info(
                "Data stream ended for session %s leg %s seq=%d: %d bytes, %d frames, final=%d%s",
                session_id, leg_id, seq, body_received, frame_count, final,
                f", error: {error_detail}" if error_detail else "",
            )

            # Only attempt commit when final=1 or SESSION_END was in frames
            commit_result = None
            if (final == 1 or session_end_seen) and session.state == SessionState.COMMITTING:
                commit_result = await engine._try_commit(session)

            if error_detail:
                return JSONResponse(
                    {"error": "data_stream_error", "detail": error_detail, "bytes_received": body_received},
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                )

            resp: dict = {"ok": True, "bytes_received": body_received, "frames": frame_count}
            if commit_result is not None:
                resp["commit"] = {"files": commit_result["files"], "bytes": commit_result["bytes"]}
            return JSONResponse(resp, status_code=status.HTTP_200_OK)

        # -------------------------------------------------------------------
        # HTTP probe endpoint: POST /mfup/probe/{session_id}
        # -------------------------------------------------------------------

        @router.post("/mfup/probe/{session_id}")
        async def probe_endpoint(session_id: str, request: Request):
            registry = engine._require_registry()
            session = registry.get(session_id)

            if session is None:
                return JSONResponse(
                    {"error": "session not found"},
                    status_code=status.HTTP_410_GONE,
                )

            token = request.headers.get("x-mfup-token")
            if not token or token != session.resume_token:
                return JSONResponse(
                    {"error": "invalid token"},
                    status_code=status.HTTP_403_FORBIDDEN,
                )

            state = session.state
            if state in (SessionState.COMMITTED, SessionState.ABORTED, SessionState.EXPIRED):
                return JSONResponse(
                    {"error": f"session in terminal state {state.value}"},
                    status_code=status.HTTP_410_GONE,
                )

            # Send PROBE_ACK after the first chunk arrives, with its size so
            # the client can verify real binary data arrived (Firefox
            # stringifies ReadableStream to 23 bytes of "[object ...]").
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

        # -------------------------------------------------------------------
        # Admin / status endpoints
        # -------------------------------------------------------------------

        def _admin_denied(request: Request) -> JSONResponse | None:
            """Admin routes require the token to be configured AND presented."""
            if not cfg.admin_token:
                return JSONResponse({"error": "admin routes disabled"}, status_code=status.HTTP_403_FORBIDDEN)
            if request.headers.get("x-mfup-admin-token") != cfg.admin_token:
                return JSONResponse({"error": "forbidden"}, status_code=status.HTTP_403_FORBIDDEN)
            return None

        @router.get("/mfup/sessions")
        async def list_sessions(request: Request):
            """List all active sessions (admin/debug)."""
            denied = _admin_denied(request)
            if denied:
                return denied
            registry = engine._require_registry()
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

        @router.get("/mfup/sessions/{session_id}")
        async def get_session_status(session_id: str, request: Request):
            """Get detailed status for a single session."""
            denied = _admin_denied(request)
            if denied:
                return denied
            registry = engine._require_registry()
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

        @router.post("/mfup/sessions/{session_id}/publish")
        async def publish_endpoint(session_id: str, request: Request):
            """Publish a committed session — atomic renames into target_dir."""
            registry = engine._require_registry()
            session = registry.get(session_id)
            if session is None:
                return JSONResponse({"error": "not found"}, status_code=404)

            # Publish moves files and destroys staging — same bearer auth as
            # the data plane: the session's resume token.
            token = request.headers.get("x-mfup-token")
            if not token or token != session.resume_token:
                return JSONResponse({"error": "invalid token"}, status_code=status.HTTP_403_FORBIDDEN)

            try:
                published = await engine.publish(session_id)
            except SessionNotFound:
                return JSONResponse({"error": "not found"}, status_code=404)
            except NotCommitted as exc:
                return JSONResponse(
                    {"error": f"cannot publish session in state {exc.state}"},
                    status_code=status.HTTP_409_CONFLICT,
                )
            except TargetEscapes:
                return JSONResponse(
                    {"error": "target_dir escapes base directory"},
                    status_code=status.HTTP_403_FORBIDDEN,
                )
            except MapFileHookError as exc:
                return JSONResponse(
                    {"error": "map_file_hook_error", "path": exc.path},
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                )
            except MappingError as exc:
                # Consumer-hook bug (escape / duplicate destination) — nothing
                # was moved.
                logger.error("Mapping error for session %s: %s", session_id, exc)
                return JSONResponse(
                    {"error": "mapping_error", "detail": str(exc)},
                    status_code=status.HTTP_409_CONFLICT,
                )
            except ConflictError as exc:
                return JSONResponse(
                    {"error": "conflict_files", "conflicting_files": exc.conflicting_files},
                    status_code=status.HTTP_409_CONFLICT,
                )
            except FileNotFoundError as exc:
                return JSONResponse({"error": str(exc)}, status_code=404)

            return {"published": published}

        @router.post("/mfup/sweep")
        async def sweep_endpoint(request: Request):
            """Manually trigger a sweep — queries Redis, no iterdir."""
            denied = _admin_denied(request)
            if denied:
                return denied
            registry = engine._require_registry()
            idx = engine._require_index()
            expired_ids = await idx.get_expired()
            removed = []
            for sid in expired_ids:
                await registry.remove(sid)
                meta = await idx.get_meta(sid)
                if meta and meta.staging_dir:
                    sd = Path(meta.staging_dir)
                else:
                    sd = staging_dir(registry.base_dir, sid, cfg.staging_prefix)
                if sd.exists():
                    shutil.rmtree(str(sd), ignore_errors=True)
                await idx.remove(sid)
                removed.append(sid)
            return {"removed": removed}

        # -------------------------------------------------------------------
        # Health check
        # -------------------------------------------------------------------

        @router.get("/health")
        async def health():
            return {"status": "ok", "protocol": PROTOCOL_VERSION, "crc32c": CRC32C_IMPL}

        return router
