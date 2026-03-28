"""MFUP/2 publish (rename from staging) and session sweeper."""

from __future__ import annotations

import logging
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path

from .protocol import SessionState
from .storage import DEFAULT_STAGING_PREFIX, SessionDB, staging_dir

logger = logging.getLogger("mfup.publish")


def publish_session(base_dir: Path, session_id: str, target_dir: Path, prefix: str = DEFAULT_STAGING_PREFIX) -> list[str]:
    """Atomically rename each payload entry into target_dir under its original name.

    Uses os.rename which is atomic on the same filesystem.
    Returns list of published entry names.
    """
    sd = staging_dir(base_dir, session_id, prefix)
    payload = sd / "payload"

    if not payload.exists():
        raise FileNotFoundError(f"no payload directory for session {session_id}")

    target_dir.mkdir(parents=True, exist_ok=True)

    published: list[str] = []
    for entry in list(payload.iterdir()):
        dest = target_dir / entry.name
        if dest.exists():
            raise FileExistsError(f"target {dest} already exists")
        os.rename(str(entry), str(dest))
        published.append(entry.name)
        logger.info("Published session %s: %s → %s", session_id, entry.name, dest)

    # Clean up remaining staging dir (state.sqlite, empty payload, etc.)
    _cleanup_staging(sd)
    return published


def _cleanup_staging(sd: Path) -> None:
    """Remove leftover staging directory after publish."""
    try:
        shutil.rmtree(str(sd), ignore_errors=True)
    except Exception:
        logger.exception("Failed to clean staging dir %s", sd)


# ---------------------------------------------------------------------------
# Sweeper
# ---------------------------------------------------------------------------

def sweep(base_dir: Path, prefix: str = DEFAULT_STAGING_PREFIX) -> list[str]:
    """Scan base_dir for staging dirs and clean up terminal sessions.

    Returns list of removed session IDs.
    """
    removed: list[str] = []
    if not base_dir.exists():
        return removed

    now = datetime.now(timezone.utc)
    pfx = prefix + "."

    for entry in list(base_dir.iterdir()):
        if not entry.is_dir() or not entry.name.startswith(pfx):
            continue

        sid = entry.name[len(pfx):]
        db_path = entry / "state.sqlite"

        if not db_path.exists():
            # Orphaned staging dir — no valid state
            logger.warning("Removing orphaned staging dir for session %s", sid)
            shutil.rmtree(str(entry), ignore_errors=True)
            removed.append(sid)
            continue

        try:
            db = SessionDB(db_path)
        except Exception:
            logger.exception("Cannot open DB for session %s, removing", sid)
            shutil.rmtree(str(entry), ignore_errors=True)
            removed.append(sid)
            continue

        try:
            sess = db.get_session()
            if sess is None:
                db.close()
                shutil.rmtree(str(entry), ignore_errors=True)
                removed.append(sid)
                continue

            state = SessionState(sess["state"])
            expires_at_str = sess["expires_at"]

            # Parse expiry
            try:
                expires_at = datetime.fromisoformat(expires_at_str)
                if expires_at.tzinfo is None:
                    expires_at = expires_at.replace(tzinfo=timezone.utc)
            except (ValueError, TypeError):
                expires_at = now  # treat bad dates as expired

            # Delete terminal sessions
            if state in (SessionState.COMMITTED, SessionState.ABORTED):
                logger.info("Sweeping %s session %s", state.value, sid)
                db.close()
                shutil.rmtree(str(entry), ignore_errors=True)
                removed.append(sid)
                continue

            # Delete expired sessions
            if state in (SessionState.EXPIRED, SessionState.FAILED):
                logger.info("Sweeping %s session %s", state.value, sid)
                db.close()
                shutil.rmtree(str(entry), ignore_errors=True)
                removed.append(sid)
                continue

            # Expire waiting_resume sessions past their TTL
            if state == SessionState.WAITING_RESUME and now >= expires_at:
                logger.info("Expiring session %s (TTL passed)", sid)
                db.set_state(SessionState.EXPIRED)
                db.close()
                shutil.rmtree(str(entry), ignore_errors=True)
                removed.append(sid)
                continue

            db.close()

        except Exception:
            logger.exception("Error sweeping session %s", sid)
            try:
                db.close()
            except Exception:
                pass

    return removed
