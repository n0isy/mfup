"""MFUP/2 publish (rename from staging to target)."""

from __future__ import annotations

import logging
import os
import shutil
from pathlib import Path

from .storage import DEFAULT_STAGING_PREFIX, staging_dir

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
