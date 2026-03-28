"""MFUP/2 Redis session index — sorted set for TTL-based cleanup.

Keys:
  mfup:sessions              — sorted set: session_id → expires_at (unix ts)
  mfup:meta:{session_id}     — hash: target_dir, staging_dir (full paths)

Sweeper flow: ZRANGEBYSCORE → HGETALL meta → rmtree(staging_dir) → ZREM + DEL meta.
No iterdir. No SQLite opens for cleanup.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

import redis.asyncio as aioredis

logger = logging.getLogger("mfup.redis")

SESSIONS_KEY = "mfup:sessions"
META_PREFIX = "mfup:meta:"


class SessionMeta:
    """Paths associated with a session."""
    __slots__ = ("target_dir", "staging_dir")

    def __init__(self, target_dir: str, staging_dir: str) -> None:
        self.target_dir = target_dir
        self.staging_dir = staging_dir


class SessionIndex:
    """Async Redis wrapper for session TTL tracking + path metadata."""

    def __init__(self, redis_url: str = "redis://redis:6379/0") -> None:
        self._redis = aioredis.from_url(redis_url, decode_responses=True)

    async def register(
        self,
        session_id: str,
        expires_at: datetime,
        target_dir: str,
        staging_dir: str,
    ) -> None:
        """Register a session with expiry and paths (pipeline, single round-trip)."""
        meta_key = META_PREFIX + session_id
        pipe = self._redis.pipeline()
        pipe.zadd(SESSIONS_KEY, {session_id: expires_at.timestamp()})
        pipe.hset(meta_key, mapping={"target_dir": target_dir, "staging_dir": staging_dir})
        await pipe.execute()
        logger.debug("Registered session %s, expires at %s", session_id, expires_at.isoformat())

    async def update_expiry(self, session_id: str, expires_at: datetime) -> None:
        """Update the expiry score for an existing session."""
        await self._redis.zadd(SESSIONS_KEY, {session_id: expires_at.timestamp()})

    async def get_expired(self, now: datetime | None = None) -> list[str]:
        """Return session IDs whose expiry is <= now."""
        if now is None:
            now = datetime.now(timezone.utc)
        return await self._redis.zrangebyscore(SESSIONS_KEY, "-inf", now.timestamp())

    async def get_meta(self, session_id: str) -> SessionMeta | None:
        """Read paths for a session. Returns None if missing."""
        data = await self._redis.hgetall(META_PREFIX + session_id)
        if not data:
            return None
        return SessionMeta(
            target_dir=data.get("target_dir", "."),
            staging_dir=data.get("staging_dir", ""),
        )

    async def all_sessions(self) -> list[str]:
        """Return all session IDs (regardless of expiry)."""
        return await self._redis.zrange(SESSIONS_KEY, 0, -1)

    async def get_not_expired(self, now: datetime | None = None) -> list[str]:
        """Return session IDs whose expiry is > now (still alive)."""
        if now is None:
            now = datetime.now(timezone.utc)
        return await self._redis.zrangebyscore(SESSIONS_KEY, now.timestamp(), "+inf")

    async def remove(self, session_id: str) -> None:
        """Remove session from both sorted set and meta hash (pipeline)."""
        pipe = self._redis.pipeline()
        pipe.zrem(SESSIONS_KEY, session_id)
        pipe.delete(META_PREFIX + session_id)
        await pipe.execute()

    async def close(self) -> None:
        await self._redis.aclose()
