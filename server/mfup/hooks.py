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


@dataclass(frozen=True)
class AuthResult:
    """Permission plus per-session constraints. All limits optional."""

    #: Cap on total accepted payload bytes; exceeding aborts the session
    #: with SESSION_ABORT(quota_exceeded). None = unlimited.
    max_total_bytes: Optional[int] = None
    #: Cap on the number of file nodes; exceeding aborts likewise.
    max_files: Optional[int] = None
    #: Optional override of the client-requested target_dir (e.g. force
    #: uploads into a per-user directory). Still validated against base_dir.
    target_dir: Optional[str] = None
    #: Free-form bag the consumer may use to correlate sessions with users;
    #: stored in memory on the LiveSession, never persisted or sent to the
    #: client.
    context: dict[str, Any] = field(default_factory=dict)


AuthorizeHook = Callable[[AuthRequest], Awaitable[Optional[AuthResult]]]


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
