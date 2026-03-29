"""Test binary frame decoding and CRC-32C."""

import struct
from mfup.protocol import (
    FrameTag,
    NodeKind,
    ChecksumKind,
    FrameReader,
    crc32c,
    NodeFrame,
    FileChunkFrame,
    FileCloseFrame,
    SessionEndFrame,
    DirCloseFrame,
)


def _encode_string(s: str) -> bytes:
    b = s.encode("utf-8")
    return struct.pack("!H", len(b)) + b


def _build_frame(tag: int, payload: bytes) -> bytes:
    length = 1 + len(payload)
    return struct.pack("!I", length) + bytes([tag]) + payload


def test_crc32c_empty():
    assert crc32c(b"") == 0


def test_crc32c_known():
    # CRC-32C of "hello" should be 0x9a71bb4c
    assert crc32c(b"hello") == 0x9A71BB4C


def test_decode_node_frame():
    # Build a NODE frame manually
    payload = b""
    payload += struct.pack("!I", 42)        # node_id
    payload += struct.pack("!I", 1)         # parent_id
    payload += bytes([NodeKind.FILE])       # kind
    name = b"test.txt"
    payload += struct.pack("!H", len(name)) + name
    payload += bytes([1])                   # has size_hint
    payload += struct.pack("!Q", 1024)      # size_hint
    payload += bytes([0])                   # no mtime

    data = _build_frame(FrameTag.NODE, payload)
    reader = FrameReader()
    reader.feed(data)
    frames = reader.drain()

    assert len(frames) == 1
    f = frames[0]
    assert isinstance(f, NodeFrame)
    assert f.node_id == 42
    assert f.parent_id == 1
    assert f.kind == NodeKind.FILE
    assert f.name == "test.txt"
    assert f.size_hint == 1024
    assert f.mtime_ms is None


def test_decode_file_chunk_frame():
    payload_data = b"hello world"
    cs = crc32c(payload_data)

    payload = b""
    payload += struct.pack("!I", 10)                # node_id
    payload += struct.pack("!Q", 0)                 # offset
    payload += struct.pack("!I", len(payload_data))  # length
    payload += bytes([ChecksumKind.CRC32C])         # checksum_kind
    payload += struct.pack("!I", cs)                # checksum
    payload += payload_data

    data = _build_frame(FrameTag.FILE_CHUNK, payload)
    reader = FrameReader()
    reader.feed(data)
    frames = reader.drain()

    assert len(frames) == 1
    f = frames[0]
    assert isinstance(f, FileChunkFrame)
    assert f.node_id == 10
    assert f.offset == 0
    assert f.length == len(payload_data)
    assert f.checksum == cs
    assert f.payload == payload_data


def test_decode_dir_close():
    payload = struct.pack("!I", 5)
    data = _build_frame(FrameTag.DIR_CLOSE, payload)
    reader = FrameReader()
    reader.feed(data)
    frames = reader.drain()
    assert len(frames) == 1
    assert isinstance(frames[0], DirCloseFrame)
    assert frames[0].node_id == 5


def test_decode_session_end():
    payload = b""
    payload += struct.pack("!Q", 100)  # scan_done
    payload += struct.pack("!Q", 100)  # scan_est
    payload += struct.pack("!Q", 5000) # body_done
    payload += struct.pack("!Q", 5000) # body_est
    payload += bytes([1])              # sealed

    data = _build_frame(FrameTag.SESSION_END, payload)
    reader = FrameReader()
    reader.feed(data)
    frames = reader.drain()
    assert len(frames) == 1
    f = frames[0]
    assert isinstance(f, SessionEndFrame)
    assert f.scan_done_units == 100
    assert f.body_done_bytes == 5000
    assert f.sealed is True


def test_incremental_feed():
    """Feed partial data in multiple chunks, frames should still decode."""
    payload = struct.pack("!I", 7)  # node_id for DIR_CLOSE
    frame = _build_frame(FrameTag.DIR_CLOSE, payload)

    reader = FrameReader()
    # Feed one byte at a time
    for i in range(len(frame)):
        reader.feed(frame[i:i+1])
        frames = reader.drain()
        if i < len(frame) - 1:
            assert len(frames) == 0
        else:
            assert len(frames) == 1


def test_multiple_frames():
    """Multiple frames in one feed."""
    f1 = _build_frame(FrameTag.DIR_CLOSE, struct.pack("!I", 1))
    f2 = _build_frame(FrameTag.DIR_CLOSE, struct.pack("!I", 2))

    reader = FrameReader()
    reader.feed(f1 + f2)
    frames = reader.drain()
    assert len(frames) == 2
    assert frames[0].node_id == 1
    assert frames[1].node_id == 2
