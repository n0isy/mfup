"""MfupConfig — every knob of the FastAPI integration in one dataclass.

Two construction paths:
  - ``MfupConfig(...)``          — library embedding, explicit values,
                                   hooks as CALLABLES;
  - ``MfupConfig.from_env()``    — standalone/container deployment, values
                                   from MFUP_* env vars, hooks as dotted
                                   paths ("pkg.module:callable").

No environment variable is read at import time anywhere in this package —
only from_env() touches os.environ.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Union

#: A hook is either the callable itself or a dotted import path.
HookRef = Union[str, Callable[..., Any]]


@dataclass
class MfupConfig:
    #: Global base directory for staging + publish (per-session base_dir from
    #: the authorize hook overrides it per session).
    base_dir: Path = Path("/tmp/mfup-uploads")
    redis_url: str = "redis://redis:6379/0"
    session_resume_ttl: int = 3600
    leg_idle_timeout: int = 60
    max_chunk_bytes: int = 262144
    max_open_files: int = 1
    max_pending_files: int = 64
    sweep_interval: int = 300
    staging_prefix: str = ".incoming"
    #: Run the filesystem-orphan reconciliation every Nth sweep.
    reconcile_every: int = 4
    #: Minimum age before a staging dir may be reconciled as an orphan.
    orphan_grace_seconds: int = 600
    #: Buffered-body threshold for atomic batch POSTs.
    max_buffered_body: int = 16 * 1024 * 1024
    #: Cap on the JSON size of HELLO.meta.
    max_meta_bytes: int = 16384
    #: Bearer token for admin/debug routes. Empty → routes disabled.
    admin_token: str = ""
    #: Consumer hooks — callables or dotted paths (see mfup_core.hooks).
    authorize: Optional[HookRef] = None
    map_file: Optional[HookRef] = None
    on_committed: Optional[HookRef] = None

    @classmethod
    def from_env(cls, env: Optional[Mapping[str, str]] = None) -> "MfupConfig":
        e: Mapping[str, str] = os.environ if env is None else env

        def _int(name: str, default: int) -> int:
            return int(e.get(name, str(default)))

        return cls(
            base_dir=Path(e.get("MFUP_BASE_DIR", "/tmp/mfup-uploads")),
            redis_url=e.get("REDIS_URL", "redis://redis:6379/0"),
            session_resume_ttl=_int("MFUP_SESSION_RESUME_TTL", 3600),
            leg_idle_timeout=_int("MFUP_LEG_IDLE_TIMEOUT", 60),
            max_chunk_bytes=_int("MFUP_MAX_CHUNK_BYTES", 262144),
            max_open_files=_int("MFUP_MAX_OPEN_FILES", 1),
            max_pending_files=_int("MFUP_MAX_PENDING_FILES", 64),
            sweep_interval=_int("MFUP_SWEEP_INTERVAL", 300),
            staging_prefix=e.get("MFUP_STAGING_PREFIX", ".incoming"),
            reconcile_every=_int("MFUP_RECONCILE_EVERY", 4),
            orphan_grace_seconds=_int("MFUP_ORPHAN_GRACE", 600),
            max_buffered_body=_int("MFUP_MAX_BUFFERED_BODY", 16 * 1024 * 1024),
            max_meta_bytes=_int("MFUP_MAX_META_BYTES", 16384),
            admin_token=e.get("MFUP_ADMIN_TOKEN", ""),
            authorize=e.get("MFUP_AUTHORIZE") or None,
            map_file=e.get("MFUP_MAP_FILE") or None,
            on_committed=e.get("MFUP_ON_COMMITTED") or None,
        )
