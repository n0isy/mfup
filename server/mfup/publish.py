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


class ConflictError(Exception):
    """Raised when publish detects file conflicts requiring user action."""
    def __init__(self, conflicting_files: int):
        self.conflicting_files = conflicting_files
        super().__init__(f"{conflicting_files} conflicting file(s)")


def detect_conflicts(target_dir: Path, payload: Path) -> int:
    """Count files in payload that already exist in target_dir.

    Dirs are auto-merged (not conflicts). Only file-vs-file collisions count.
    """
    count = 0
    for entry in payload.iterdir():
        dest = target_dir / entry.name
        if not dest.exists():
            continue
        if entry.is_dir() and dest.is_dir():
            # Recurse into matching dirs
            count += detect_conflicts(dest, entry)
        elif entry.is_file() and dest.is_file():
            count += 1
        else:
            # Type mismatch (file vs dir) — counts as conflict
            count += 1
    return count


def _merge_tree(src: Path, dst: Path) -> list[str]:
    """Recursively merge src into dst, overwriting files. Returns published names."""
    published: list[str] = []
    for entry in list(src.iterdir()):
        dest = dst / entry.name
        if entry.is_dir():
            if dest.is_dir():
                # Merge into existing dir
                published.extend(_merge_tree(entry, dest))
            elif dest.exists():
                # Type conflict: replace file with dir
                dest.unlink()
                os.rename(str(entry), str(dest))
                published.append(entry.name)
            else:
                os.rename(str(entry), str(dest))
                published.append(entry.name)
        else:
            # File: overwrite or create
            if dest.exists():
                os.replace(str(entry), str(dest))
            else:
                os.rename(str(entry), str(dest))
            published.append(entry.name)
    return published


def publish_session(
    base_dir: Path,
    session_id: str,
    target_dir: Path,
    prefix: str = DEFAULT_STAGING_PREFIX,
    action: str | None = None,
) -> list[str]:
    """Publish payload entries into target_dir.

    If conflicts exist and action is None, raises ConflictError.
    If action is "merge_overwrite", merges dirs and overwrites files.

    Returns list of published entry names.
    """
    sd = staging_dir(base_dir, session_id, prefix)
    payload = sd / "payload"

    if not payload.exists():
        raise FileNotFoundError(f"no payload directory for session {session_id}")

    target_dir.mkdir(parents=True, exist_ok=True)

    conflicts = detect_conflicts(target_dir, payload)

    if conflicts > 0 and action is None:
        raise ConflictError(conflicts)

    if conflicts > 0 and action == "merge_overwrite":
        published = _merge_tree(payload, target_dir)
    else:
        # Clean path — no conflicts
        published = []
        for entry in list(payload.iterdir()):
            dest = target_dir / entry.name
            os.rename(str(entry), str(dest))
            published.append(entry.name)

    for name in published:
        logger.info("Published session %s: %s", session_id, name)

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
