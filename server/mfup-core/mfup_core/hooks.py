"""MFUP/2 extension hooks — config-driven, no library packaging required.

A consumer plugs their code in via environment variables holding dotted
paths, e.g.:

    MFUP_AUTHORIZE=myapp.uploads:authorize

The referenced module just has to be importable (on PYTHONPATH / mounted
into the container). The full contract lives in docs/EXTENDING.md.

Authorize contract
------------------

    async def authorize(req: AuthRequest) -> AuthResult | None:
        ...

Called once per HELLO, before the session is created. Return:
  - ``AuthResult(...)``  — allow, optionally constraining the session;
  - ``None``             — deny → the client gets SESSION_ABORT(auth_failed).

Raising is treated as a deny (logged server-side, generic reason sent).

If ``MFUP_AUTHORIZE`` is unset the server runs **allow-all** and logs a
warning at startup — acceptable for development, not for production.
"""

from __future__ import annotations

import importlib
import logging
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Mapping, Optional

logger = logging.getLogger("mfup.hooks")


@dataclass(frozen=True)
class AuthRequest:
    """Everything the server knows about an upload attempt at HELLO time."""

    session_id: str
    target_dir: str
    #: HTTP headers of the WebSocket handshake (cookies, Authorization, …).
    headers: Mapping[str, str]
    #: Client address as reported by the ASGI server ("ip:port" or "").
    client: str
    #: Query parameters of the WebSocket URL.
    query: Mapping[str, str]
    #: Arbitrary JSON the CONSUMER'S FRONTEND attached to the session
    #: (MfupSessionConfig.meta → HELLO.meta). Untrusted client input — the
    #: hook validates it. Typical use: upload scope/purpose ("avatars",
    #: {"album_id": 123}) that authorization and per-file mapping key on.
    meta: Any = None


@dataclass(frozen=True)
class AuthResult:
    """Permission plus per-session constraints. All limits optional."""

    #: Cap on total accepted payload bytes; exceeding aborts the session
    #: with SESSION_ABORT(quota_exceeded). None = unlimited.
    max_total_bytes: Optional[int] = None
    #: Cap on the number of file nodes; exceeding aborts likewise.
    max_files: Optional[int] = None
    #: Per-session BASE directory (absolute path) — e.g. the user's home.
    #: Overrides MFUP_BASE_DIR for this session: the staging dir is created
    #: inside it (publish stays a same-filesystem rename even when homes live
    #: on their own mount), relative target_dir resolves against it, and the
    #: containment check confines the session to it. Created if missing.
    #: None = the global MFUP_BASE_DIR.
    base_dir: Optional[str] = None
    #: Optional override of the client-requested target_dir (e.g. force
    #: uploads into a fixed subdirectory, or a MAPPING of the client's
    #: request — the hook receives req.target_dir and may prefix/rewrite it:
    #:     target_dir=f"incoming/{req.target_dir}"
    #: Escapes are impossible regardless: the resolved target must stay
    #: within the session's base_dir or HELLO is refused (bad_target_dir).
    target_dir: Optional[str] = None
    #: Free-form bag the consumer may use to correlate sessions with users;
    #: stored in memory on the LiveSession, never persisted or sent to the
    #: client.
    context: dict[str, Any] = field(default_factory=dict)


AuthorizeHook = Callable[[AuthRequest], Awaitable[Optional[AuthResult]]]


@dataclass(frozen=True)
class FileMapRequest:
    """One file about to be published — input to the map_file hook."""

    session_id: str
    #: Path of the file inside the uploaded tree, "/"-separated, as the
    #: client sent it (e.g. "photos/2024/img_001.jpg").
    path: str
    #: Basename convenience (last segment of `path`).
    name: str
    #: Actual size on disk, bytes.
    size: int
    #: The session's target_dir (already authorized/mapped at HELLO).
    target_dir: str
    #: Client-attached session meta (see AuthRequest.meta).
    meta: Any
    #: AuthResult.context from the authorize hook.
    context: Mapping[str, Any]


#: async (FileMapRequest) -> str | None
#:   str  — new path RELATIVE to target_dir (e.g. "media/img_001.jpg");
#:   None — keep the client's layout for this file.
#: Two files mapping to the same destination is a consumer bug → publish
#: fails with mapping_conflict. Escaping segments ("..", absolute, "\\")
#: fail publish likewise. The hook runs once per file at PUBLISH time, so
#: it does not need to be deterministic across retries of the transfer.
MapFileHook = Callable[[FileMapRequest], Awaitable[Optional[str]]]


@dataclass(frozen=True)
class CommitEvent:
    """A session just committed — input to the on_committed hook."""

    session_id: str
    #: The session's (authorized/mapped) target_dir.
    target_dir: str
    #: The session's base directory (per-user home or the global base).
    base_dir: str
    #: Absolute staging directory holding the committed payload.
    staging_dir: str
    #: Committed file count / total payload bytes (as sent in COMMIT_OK).
    files: int
    bytes: int
    #: Client-attached session meta (see AuthRequest.meta). Untrusted.
    meta: Any
    #: AuthResult.context from the authorize hook.
    context: Mapping[str, Any]


#: async (CommitEvent) -> str | None
#:   "publish" — the server publishes immediately (server-side decision:
#:               scan passed, billing ok, …). The browser's own publish call,
#:               if any, will find the session gone (404) — harmless.
#:   None      — do nothing; publish stays client-driven (or the consumer
#:               backend calls MfupEngine.publish() later).
#: Raising is logged and treated as None — a broken consumer hook must not
#: strand committed sessions.
OnCommittedHook = Callable[[CommitEvent], Awaitable[Optional[str]]]


def load_hook(dotted: str) -> Callable[..., Any]:
    """Import ``pkg.module:attr`` and return the attribute.

    Raises ImportError/AttributeError loudly — a misconfigured hook must
    fail at startup, not silently run allow-all.
    """
    module_path, sep, attr = dotted.partition(":")
    if not sep or not module_path or not attr:
        raise ImportError(
            f"invalid hook path {dotted!r}: expected 'package.module:callable'"
        )
    module = importlib.import_module(module_path)
    return getattr(module, attr)


def load_authorize_hook(dotted: str | None) -> AuthorizeHook | None:
    """Resolve MFUP_AUTHORIZE. None (unset) → allow-all with a warning."""
    if not dotted:
        logger.warning(
            "MFUP_AUTHORIZE is not set — running WITHOUT authorization "
            "(allow-all). Do not do this in production."
        )
        return None
    hook = load_hook(dotted)
    logger.info("Authorize hook loaded: %s", dotted)
    return hook  # type: ignore[return-value]


def load_map_file_hook(dotted: str | None) -> MapFileHook | None:
    """Resolve MFUP_MAP_FILE. None (unset) → identity layout."""
    if not dotted:
        return None
    hook = load_hook(dotted)
    logger.info("map_file hook loaded: %s", dotted)
    return hook  # type: ignore[return-value]


def load_on_committed_hook(dotted: str | None) -> OnCommittedHook | None:
    """Resolve MFUP_ON_COMMITTED. None (unset) → client-driven publish."""
    if not dotted:
        return None
    hook = load_hook(dotted)
    logger.info("on_committed hook loaded: %s", dotted)
    return hook  # type: ignore[return-value]


def resolve_hook(ref: Callable[..., Any] | str | None) -> Callable[..., Any] | None:
    """Accept a hook given either as the callable itself (library embedding:
    ``MfupConfig(authorize=my_func)``) or as a dotted path (env-driven:
    ``"pkg.module:callable"``). None passes through."""
    if ref is None:
        return None
    if callable(ref):
        return ref
    return load_hook(ref)
