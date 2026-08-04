"""mfup-core — the MFUP/2 upload engine, framework-free.

Protocol codec, per-session SQLite staging, the session state machine,
Redis expiry index, publish/mapping, and the consumer hook contracts.
HTTP/WebSocket wiring lives in the companion package ``mfup-fastapi``.
"""

from .protocol import (
    CRC32C_IMPL,
    PROTOCOL_VERSION,
    FrameReader,
    SessionState,
)
from .hooks import (
    AuthRequest,
    AuthResult,
    AuthorizeHook,
    CommitEvent,
    FileMapRequest,
    MapFileHook,
    OnCommittedHook,
    load_authorize_hook,
    load_hook,
    load_map_file_hook,
    load_on_committed_hook,
    resolve_hook,
)
from .session_manager import LiveSession, SessionRegistry
from .redis_index import SessionIndex
from .publish import (
    ConflictError,
    MappingError,
    list_payload_files,
    publish_session,
    publish_session_mapped,
)
from .storage import staging_dir, validate_node_name

__all__ = [
    "AuthRequest",
    "AuthResult",
    "AuthorizeHook",
    "CommitEvent",
    "ConflictError",
    "CRC32C_IMPL",
    "FileMapRequest",
    "FrameReader",
    "list_payload_files",
    "LiveSession",
    "load_authorize_hook",
    "load_hook",
    "load_map_file_hook",
    "load_on_committed_hook",
    "MapFileHook",
    "MappingError",
    "OnCommittedHook",
    "PROTOCOL_VERSION",
    "publish_session",
    "publish_session_mapped",
    "resolve_hook",
    "SessionIndex",
    "SessionRegistry",
    "SessionState",
    "staging_dir",
    "validate_node_name",
]
