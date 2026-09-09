"""MFUP/2 protocol types and binary frame decoder (server-side)."""

from __future__ import annotations

import enum
import struct
from dataclasses import dataclass
from typing import Optional


PROTOCOL_VERSION = "MFUP/2"
ROOT_NODE_ID = 0


# ---------------------------------------------------------------------------
# Frame tags
# ---------------------------------------------------------------------------
class FrameTag(enum.IntEnum):
    NODE = 0x01
    SUMMARY = 0x02
    FILE_OPEN = 0x03
    FILE_CHUNK = 0x04
    FILE_CLOSE = 0x05
    DIR_CLOSE = 0x06
    SESSION_END = 0x07
    CLIENT_ABORT = 0x08


class NodeKind(enum.IntEnum):
    DIR = 0x00
    FILE = 0x01


class ChecksumKind(enum.IntEnum):
    CRC32C = 0x01


# ---------------------------------------------------------------------------
# Decoded frame dataclasses
# ---------------------------------------------------------------------------
@dataclass(slots=True)
class NodeFrame:
    tag: FrameTag = FrameTag.NODE
    node_id: int = 0
    parent_id: int = 0
    kind: NodeKind = NodeKind.FILE
    name: str = ""
    size_hint: Optional[int] = None
    mtime_ms: Optional[int] = None


@dataclass(slots=True)
class SummaryFrame:
    tag: FrameTag = FrameTag.SUMMARY
    node_id: int = 0
    scan_done_units: int = 0
    scan_est_units: int = 0
    body_done_bytes: int = 0
    body_est_bytes: int = 0
    sealed: bool = False


@dataclass(slots=True)
class FileOpenFrame:
    tag: FrameTag = FrameTag.FILE_OPEN
    node_id: int = 0
    size: int = 0
    mtime_ms: Optional[int] = None


@dataclass(slots=True)
class FileChunkFrame:
    tag: FrameTag = FrameTag.FILE_CHUNK
    node_id: int = 0
    offset: int = 0
    length: int = 0
    checksum_kind: ChecksumKind = ChecksumKind.CRC32C
    checksum: int = 0
    payload: bytes = b""


@dataclass(slots=True)
class FileCloseFrame:
    tag: FrameTag = FrameTag.FILE_CLOSE
    node_id: int = 0
    size_sent: int = 0


@dataclass(slots=True)
class DirCloseFrame:
    tag: FrameTag = FrameTag.DIR_CLOSE
    node_id: int = 0


@dataclass(slots=True)
class SessionEndFrame:
    tag: FrameTag = FrameTag.SESSION_END
    scan_done_units: int = 0
    scan_est_units: int = 0
    body_done_bytes: int = 0
    body_est_bytes: int = 0
    sealed: bool = True


@dataclass(slots=True)
class ClientAbortFrame:
    tag: FrameTag = FrameTag.CLIENT_ABORT
    code: str = ""
    reason: str = ""


Frame = (
    NodeFrame
    | SummaryFrame
    | FileOpenFrame
    | FileChunkFrame
    | FileCloseFrame
    | DirCloseFrame
    | SessionEndFrame
    | ClientAbortFrame
)


# Server → Client control message: {"t": "PROBE_ACK"}


# ---------------------------------------------------------------------------
# Session states
# ---------------------------------------------------------------------------
class SessionState(str, enum.Enum):
    ACTIVE = "active"
    PAUSED_BY_SERVER = "paused_by_server"
    WAITING_RESUME = "waiting_resume"
    COMMITTING = "committing"
    COMMITTED = "committed"
    ABORTED = "aborted"
    EXPIRED = "expired"
    FAILED = "failed"


# ---------------------------------------------------------------------------
# Node statuses (in DB)
# ---------------------------------------------------------------------------
class NodeStatus(str, enum.Enum):
    OPEN = "open"
    CLOSED = "closed"
    REJECTED = "rejected"
    PRUNED = "pruned"


# ---------------------------------------------------------------------------
# Binary frame decoder
#
# Wire format: [4-byte big-endian length][1-byte tag][payload]
# The length covers tag + payload.
# ---------------------------------------------------------------------------

def _read_u8(data: memoryview, off: int) -> tuple[int, int]:
    return data[off], off + 1


def _read_u16(data: memoryview, off: int) -> tuple[int, int]:
    return struct.unpack_from("!H", data, off)[0], off + 2


def _read_u32(data: memoryview, off: int) -> tuple[int, int]:
    return struct.unpack_from("!I", data, off)[0], off + 4


def _read_u64(data: memoryview, off: int) -> tuple[int, int]:
    return struct.unpack_from("!Q", data, off)[0], off + 8


def _read_string(data: memoryview, off: int) -> tuple[str, int]:
    slen, off = _read_u16(data, off)
    s = bytes(data[off : off + slen]).decode("utf-8")
    return s, off + slen


def decode_frame_payload(tag: int, payload: memoryview) -> Frame:
    """Decode the payload bytes (after the tag byte) into a Frame dataclass."""
    off = 0

    if tag == FrameTag.NODE:
        node_id, off = _read_u32(payload, off)
        parent_id, off = _read_u32(payload, off)
        kind_val, off = _read_u8(payload, off)
        name, off = _read_string(payload, off)
        has_size, off = _read_u8(payload, off)
        size_hint = None
        if has_size:
            size_hint, off = _read_u64(payload, off)
        has_mtime, off = _read_u8(payload, off)
        mtime_ms = None
        if has_mtime:
            mtime_ms, off = _read_u64(payload, off)
        return NodeFrame(
            node_id=node_id,
            parent_id=parent_id,
            kind=NodeKind(kind_val),
            name=name,
            size_hint=size_hint,
            mtime_ms=mtime_ms,
        )

    elif tag == FrameTag.SUMMARY:
        node_id, off = _read_u32(payload, off)
        scan_done, off = _read_u64(payload, off)
        scan_est, off = _read_u64(payload, off)
        body_done, off = _read_u64(payload, off)
        body_est, off = _read_u64(payload, off)
        sealed_val, off = _read_u8(payload, off)
        return SummaryFrame(
            node_id=node_id,
            scan_done_units=scan_done,
            scan_est_units=scan_est,
            body_done_bytes=body_done,
            body_est_bytes=body_est,
            sealed=bool(sealed_val),
        )

    elif tag == FrameTag.FILE_OPEN:
        node_id, off = _read_u32(payload, off)
        size, off = _read_u64(payload, off)
        has_mtime, off = _read_u8(payload, off)
        mtime_ms = None
        if has_mtime:
            mtime_ms, off = _read_u64(payload, off)
        return FileOpenFrame(node_id=node_id, size=size, mtime_ms=mtime_ms)

    elif tag == FrameTag.FILE_CHUNK:
        node_id, off = _read_u32(payload, off)
        offset_val, off = _read_u64(payload, off)
        length, off = _read_u32(payload, off)
        ck_kind, off = _read_u8(payload, off)
        checksum, off = _read_u32(payload, off)
        data = bytes(payload[off : off + length])
        return FileChunkFrame(
            node_id=node_id,
            offset=offset_val,
            length=length,
            checksum_kind=ChecksumKind(ck_kind),
            checksum=checksum,
            payload=data,
        )

    elif tag == FrameTag.FILE_CLOSE:
        node_id, off = _read_u32(payload, off)
        size_sent, off = _read_u64(payload, off)
        return FileCloseFrame(node_id=node_id, size_sent=size_sent)

    elif tag == FrameTag.DIR_CLOSE:
        node_id, off = _read_u32(payload, off)
        return DirCloseFrame(node_id=node_id)

    elif tag == FrameTag.SESSION_END:
        scan_done, off = _read_u64(payload, off)
        scan_est, off = _read_u64(payload, off)
        body_done, off = _read_u64(payload, off)
        body_est, off = _read_u64(payload, off)
        sealed_val, off = _read_u8(payload, off)
        return SessionEndFrame(
            scan_done_units=scan_done,
            scan_est_units=scan_est,
            body_done_bytes=body_done,
            body_est_bytes=body_est,
            sealed=bool(sealed_val),
        )

    elif tag == FrameTag.CLIENT_ABORT:
        code, off = _read_string(payload, off)
        reason, off = _read_string(payload, off)
        return ClientAbortFrame(code=code, reason=reason)

    else:
        raise ValueError(f"unknown frame tag: {tag:#04x}")


class IncompleteFrameError(ValueError):
    pass


class FrameReader:
    """Incremental frame reader that buffers partial data from a streaming body.

    Feed chunks via `feed(data)` and iterate decoded frames via `drain()`.

    `max_frame_len` bounds the declared frame length: without it a corrupt or
    malicious 4-byte prefix claiming a multi-gigabyte frame would grow the
    buffer without limit while the reader waits for it to "complete".
    """

    # 1 MiB default: comfortably above MAX_CHUNK_BYTES (256 KiB) + headers
    # and any NODE frame with a long UTF-8 name.
    DEFAULT_MAX_FRAME_LEN = 1024 * 1024

    def __init__(self, max_frame_len: int = DEFAULT_MAX_FRAME_LEN) -> None:
        self._buf = bytearray()
        self._max_frame_len = max_frame_len

    def feed(self, data: bytes) -> None:
        self._buf.extend(data)

    def finish(self) -> None:
        """Check EOF after drain in O(1), without reparsing the body."""
        if self._buf:
            raise IncompleteFrameError("incomplete frame at end of request")

    def drain(self) -> list[Frame]:
        frames: list[Frame] = []
        pos = 0
        total = len(self._buf)

        while pos + 4 <= total:
            frame_len = struct.unpack_from("!I", self._buf, pos)[0]
            if frame_len > self._max_frame_len:
                raise ValueError(
                    f"frame length {frame_len} exceeds limit {self._max_frame_len}"
                )
            if pos + 4 + frame_len > total:
                break  # incomplete frame
            tag = self._buf[pos + 4]
            # Copy payload to a bytes object so no memoryview holds the buffer
            payload_bytes = bytes(self._buf[pos + 5 : pos + 4 + frame_len])
            frames.append(decode_frame_payload(tag, memoryview(payload_bytes)))
            pos += 4 + frame_len

        if pos > 0:
            del self._buf[:pos]

        return frames


# ---------------------------------------------------------------------------
# CRC-32C (Castagnoli) for checksum verification
#
# The C implementation from the `crc32c` package (SSE4.2/ARMv8 accelerated,
# ~GB/s) is a HARD dependency. A pure-Python fallback used to exist and was
# removed deliberately: it measured ~6 MB/s and blocked the event loop
# ~10 ms per 64 KiB chunk, silently throttling the whole server. Failing
# loudly at import beats degrading quietly in production.
# ---------------------------------------------------------------------------

try:
    import crc32c as _crc32c_native  # type: ignore
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "MFUP/2 requires the C-accelerated 'crc32c' package: pip install crc32c"
    ) from exc


def crc32c(data: bytes | memoryview, initial: int = 0) -> int:
    return _crc32c_native.crc32c(data, initial)


CRC32C_IMPL = "native"
