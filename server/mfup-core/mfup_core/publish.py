"""MFUP/2 publish (rename from staging to target)."""

from __future__ import annotations

import logging
import os
import shutil
from pathlib import Path

from .storage import DEFAULT_STAGING_PREFIX, staging_dir, validate_node_name

logger = logging.getLogger("mfup.publish")


class ConflictError(Exception):
    """Raised when publish detects file conflicts requiring user action."""
    def __init__(self, conflicting_files: int):
        self.conflicting_files = conflicting_files
        super().__init__(f"{conflicting_files} conflicting file(s)")


class MappingError(Exception):
    """Raised when the consumer's map_file hook produced an unusable path
    (escape attempt, illegal segment, or two files mapped to one target)."""


def list_payload_files(
    base_dir: Path, session_id: str, prefix: str = DEFAULT_STAGING_PREFIX,
) -> list[tuple[str, int]]:
    """All files in the session's payload as ("/"-relative path, size)."""
    payload = staging_dir(base_dir, session_id, prefix) / "payload"
    if not payload.exists():
        raise FileNotFoundError(f"no payload directory for session {session_id}")
    out: list[tuple[str, int]] = []
    for root, _dirs, files in os.walk(payload):
        for fn in files:
            p = Path(root) / fn
            out.append((p.relative_to(payload).as_posix(), p.stat().st_size))
    out.sort()
    return out


def validate_mapped_path(rel: str) -> None:
    """Reject mapped destinations that could escape target_dir.

    Same per-segment rules as ingest names (no "..", separators inside a
    segment, NUL), plus: must be relative and non-empty.
    """
    if not rel or rel.startswith("/") or rel.startswith("\\"):
        raise MappingError(f"mapped path must be relative and non-empty: {rel!r}")
    for segment in rel.split("/"):
        try:
            validate_node_name(segment)
        except ValueError as exc:
            raise MappingError(f"illegal mapped path {rel!r}: {exc}") from exc


def publish_session_mapped(
    base_dir: Path,
    session_id: str,
    target_dir: Path,
    mapping: dict[str, str],
    prefix: str = DEFAULT_STAGING_PREFIX,
    action: str | None = None,
) -> list[str]:
    """Publish with a per-file layout decided by the consumer's map_file hook.

    `mapping` is {payload-relative source → target-relative destination};
    files absent from the mapping keep their client layout. Materializes
    FILES only (directories are implied; empty client dirs are not
    preserved in mapped mode). Same conflict semantics as the plain path:
    existing destinations require action == "merge_overwrite", otherwise
    ConflictError is raised before anything moves.
    """
    sd = staging_dir(base_dir, session_id, prefix)
    payload = sd / "payload"
    if not payload.exists():
        raise FileNotFoundError(f"no payload directory for session {session_id}")

    files = list_payload_files(base_dir, session_id, prefix)

    # Resolve, validate and collision-check the full plan BEFORE moving
    # anything — publish must not stop halfway on a consumer-hook bug.
    plan: list[tuple[Path, str]] = []
    seen: dict[str, str] = {}
    target_res = target_dir.resolve()
    for rel, _size in files:
        dest_rel = mapping.get(rel, rel)
        validate_mapped_path(dest_rel)
        dest = target_dir / dest_rel
        # Defense in depth after segment validation.
        if not dest.resolve().is_relative_to(target_res):
            raise MappingError(f"mapped path escapes target: {dest_rel!r}")
        if dest_rel in seen:
            raise MappingError(
                f"two files map to {dest_rel!r}: {seen[dest_rel]!r} and {rel!r}"
            )
        seen[dest_rel] = rel
        plan.append((payload / rel, dest_rel))

    conflicts = sum(1 for _src, dest_rel in plan if (target_dir / dest_rel).exists())
    if conflicts > 0 and action is None:
        raise ConflictError(conflicts)

    target_dir.mkdir(parents=True, exist_ok=True)
    published: list[str] = []
    for src, dest_rel in plan:
        dest = target_dir / dest_rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        if dest.exists() and action == "merge_overwrite":
            os.replace(str(src), str(dest))
        else:
            os.rename(str(src), str(dest))
        published.append(dest_rel)
        logger.info("Published (mapped) session %s: %s -> %s", session_id, src.name, dest_rel)

    _cleanup_staging(sd)
    return published


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
