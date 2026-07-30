"""Edge-case tests: bad filenames, type collisions, storage errors, commit
caps, and path-traversal defenses. These exercise LiveSession against a real
per-session SQLite DB in a tmp dir, with a fake WebSocket capturing the
control messages the server would send to the client.
"""

import errno
import pytest

from pathlib import Path

from mfup.protocol import (
    NodeKind,
    NodeFrame,
    FileOpenFrame,
    FileChunkFrame,
    FileCloseFrame,
    SessionEndFrame,
    SessionState,
    crc32c,
)
from mfup.session_manager import LiveSession, FileWriter, MAX_COMMIT_RETRIES
from mfup.storage import open_session_db, validate_node_name, resolve_payload_path, staging_dir


class FakeWS:
    """Captures control messages the session sends."""
    def __init__(self):
        self.sent = []

    async def send_json(self, msg):
        self.sent.append(msg)

    def of_type(self, t):
        return [m for m in self.sent if m.get("t") == t]


def make_session(tmp_path: Path, sid="s1") -> LiveSession:
    db = open_session_db(tmp_path, sid)
    db.init_session(sid, "tok", "2099-01-01T00:00:00+00:00", ".")
    s = LiveSession(sid, "tok", tmp_path, db, target_dir=".")
    s.attach_leg("leg1")
    s.ws = FakeWS()
    return s


def node(node_id, parent_id, kind, name, size=None):
    return NodeFrame(node_id=node_id, parent_id=parent_id, kind=kind, name=name, size_hint=size)


# ---------------------------------------------------------------------------
# validate_node_name — the first traversal defense
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("bad", ["", ".", "..", "a/b", "a\\b", "a\x00b", "/", "..\\.."])
def test_validate_node_name_rejects(bad):
    with pytest.raises(ValueError):
        validate_node_name(bad)


@pytest.mark.parametrize("ok", ["file.txt", "a b c", "файл ❤.md", ".hidden", "..foo", "foo.."])
def test_validate_node_name_accepts(ok):
    validate_node_name(ok)  # must not raise


# ---------------------------------------------------------------------------
# Illegal names dropped + rejected, never stored, never traverse
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_illegal_name_node_dropped_and_rejected(tmp_path):
    s = make_session(tmp_path)
    await s.process_frame(node(1, 0, NodeKind.FILE, ".."), "leg1")
    # Not stored
    assert s.db.get_node(1) is None
    # Counted so the commit invariant balances
    assert 1 in s.dropped_nodes
    # Client told to skip it
    assert s.ws.of_type("REJECT_FILE"), "expected REJECT_FILE for illegal name"


@pytest.mark.asyncio
async def test_traversal_name_never_escapes_payload(tmp_path):
    s = make_session(tmp_path)
    # Even a crafted "../../etc" style name is rejected at ingest.
    await s.process_frame(node(1, 0, NodeKind.FILE, "../evil"), "leg1")
    assert s.db.get_node(1) is None
    # Nothing was created outside payload
    assert not (tmp_path / "evil").exists()


# ---------------------------------------------------------------------------
# Type collisions: dir-on-file and file-on-dir must reject, not crash
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_dir_on_file_collision_rejects_not_crashes(tmp_path):
    s = make_session(tmp_path)
    # node 1: file "foo"; write a byte so it exists on disk
    await s.process_frame(node(1, 0, NodeKind.FILE, "foo", size=1), "leg1")
    await s.process_frame(FileOpenFrame(node_id=1, size=1), "leg1")
    payload = b"x"
    await s.process_frame(FileChunkFrame(node_id=1, offset=0, length=1, checksum=crc32c(payload), payload=payload), "leg1")
    await s.process_frame(FileCloseFrame(node_id=1, size_sent=1), "leg1")
    # node 2: DIR also named "foo" under root → mkdir over a file
    await s.process_frame(node(2, 0, NodeKind.DIR, "foo"), "leg1")
    # Must have rejected node 2, and the session is still ACTIVE (no crash)
    assert 2 in [m["node_id"] for m in s.ws.of_type("REJECT_FILE")]
    assert s.state == SessionState.ACTIVE


@pytest.mark.asyncio
async def test_file_on_dir_collision_rejects_not_crashes(tmp_path):
    s = make_session(tmp_path)
    # node 1: dir "bar"
    await s.process_frame(node(1, 0, NodeKind.DIR, "bar"), "leg1")
    # node 2: file also "bar" → open() over a directory raises IsADirectoryError
    await s.process_frame(node(2, 0, NodeKind.FILE, "bar", size=1), "leg1")
    await s.process_frame(FileOpenFrame(node_id=2, size=1), "leg1")
    assert 2 in [m["node_id"] for m in s.ws.of_type("REJECT_FILE")]
    assert s.state == SessionState.ACTIVE


# ---------------------------------------------------------------------------
# resolve_payload_path — broken chain / containment
# ---------------------------------------------------------------------------

def test_resolve_payload_path_broken_chain_raises(tmp_path):
    db = open_session_db(tmp_path, "s2")
    # node 5 has parent 99 which was never stored
    db.upsert_node(5, 99, NodeKind.FILE, "leaf.txt")
    with pytest.raises(ValueError):
        resolve_payload_path(tmp_path, "s2", db, 5)


def test_resolve_payload_path_normal(tmp_path):
    db = open_session_db(tmp_path, "s3")
    db.upsert_node(1, 0, NodeKind.DIR, "src")
    db.upsert_node(2, 1, NodeKind.FILE, "app.ts")
    p = resolve_payload_path(tmp_path, "s3", db, 2)
    payload_root = (staging_dir(tmp_path, "s3") / "payload").resolve()
    assert p.resolve().is_relative_to(payload_root)
    assert p.name == "app.ts"


# ---------------------------------------------------------------------------
# ENOSPC / fatal storage error → session aborted, client told
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_enospc_aborts_session(tmp_path, monkeypatch):
    s = make_session(tmp_path)
    await s.process_frame(node(1, 0, NodeKind.FILE, "big.bin", size=10), "leg1")
    await s.process_frame(FileOpenFrame(node_id=1, size=10), "leg1")

    # Make the next write raise ENOSPC
    def boom(self, data):
        raise OSError(errno.ENOSPC, "No space left on device")
    monkeypatch.setattr(FileWriter, "_sync_write", boom)

    payload = b"0123456789"
    await s.process_frame(
        FileChunkFrame(node_id=1, offset=0, length=10, checksum=crc32c(payload), payload=payload),
        "leg1",
    )
    assert s.state == SessionState.ABORTED
    aborts = s.ws.of_type("SESSION_ABORT")
    assert aborts and aborts[-1]["code"] == "storage_full"


# ---------------------------------------------------------------------------
# Commit retry cap: an unsatisfiable incomplete file must not loop forever
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_commit_retry_capped(tmp_path):
    s = make_session(tmp_path)
    # A file declared (final_size set) but never fully received.
    await s.process_frame(node(1, 0, NodeKind.FILE, "f.bin", size=100), "leg1")
    s.db.set_file_final(1, 100, "x")  # final_size=100, accepted_offset=0 → incomplete

    # Drive SESSION_END → try_commit repeatedly. The file never accepts a
    # byte (no progress), so the consecutive-no-progress cap must abort it.
    for _ in range(MAX_COMMIT_RETRIES + 4):
        s.db.set_state(SessionState.COMMITTING)
        await s.try_commit()
        if s.state == SessionState.FAILED:
            break

    assert s.state == SessionState.FAILED
    assert s.ws.of_type("SESSION_ABORT"), "expected SESSION_ABORT after retry cap"


# ---------------------------------------------------------------------------
# Duplicate FILE_OPEN must not leak a writer
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_duplicate_file_open_closes_prior_writer(tmp_path):
    s = make_session(tmp_path)
    await s.process_frame(node(1, 0, NodeKind.FILE, "f.bin", size=10), "leg1")
    await s.process_frame(FileOpenFrame(node_id=1, size=10), "leg1")
    first = s.writers[1]
    await s.process_frame(FileOpenFrame(node_id=1, size=10), "leg1")
    second = s.writers[1]
    assert first is not second
    assert first._fh.closed, "prior writer's file handle must be closed"


# ---------------------------------------------------------------------------
# Quotas (from the authorize hook) and chunk-size enforcement
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_byte_quota_aborts_session(tmp_path):
    s = make_session(tmp_path)
    s.quota_max_bytes = 5  # tiny quota
    await s.process_frame(node(1, 0, NodeKind.FILE, "f.bin", size=10), "leg1")
    await s.process_frame(FileOpenFrame(node_id=1, size=10), "leg1")
    payload = b"0123456789"
    await s.process_frame(
        FileChunkFrame(node_id=1, offset=0, length=10, checksum=crc32c(payload), payload=payload),
        "leg1",
    )
    assert s.state == SessionState.ABORTED
    aborts = s.ws.of_type("SESSION_ABORT")
    assert aborts and aborts[-1]["code"] == "quota_exceeded"


@pytest.mark.asyncio
async def test_file_quota_aborts_session(tmp_path):
    s = make_session(tmp_path)
    s.quota_max_files = 2
    await s.process_frame(node(1, 0, NodeKind.FILE, "a.bin", size=1), "leg1")
    await s.process_frame(node(2, 0, NodeKind.FILE, "b.bin", size=1), "leg1")
    assert s.state == SessionState.ACTIVE
    await s.process_frame(node(3, 0, NodeKind.FILE, "c.bin", size=1), "leg1")
    assert s.state == SessionState.ABORTED
    aborts = s.ws.of_type("SESSION_ABORT")
    assert aborts and aborts[-1]["code"] == "quota_exceeded"


@pytest.mark.asyncio
async def test_duplicate_node_does_not_double_count_file_quota(tmp_path):
    s = make_session(tmp_path)
    s.quota_max_files = 1
    await s.process_frame(node(1, 0, NodeKind.FILE, "a.bin", size=1), "leg1")
    # NODE replay (resume path) must not count the same file twice.
    await s.process_frame(node(1, 0, NodeKind.FILE, "a.bin", size=1), "leg1")
    assert s.state == SessionState.ACTIVE
    assert s.files_seen == 1


@pytest.mark.asyncio
async def test_oversized_chunk_nacked(tmp_path):
    s = make_session(tmp_path)
    s.max_chunk_bytes = 8
    await s.process_frame(node(1, 0, NodeKind.FILE, "f.bin", size=100), "leg1")
    await s.process_frame(FileOpenFrame(node_id=1, size=100), "leg1")
    payload = b"x" * 16  # over the limit
    await s.process_frame(
        FileChunkFrame(node_id=1, offset=0, length=16, checksum=crc32c(payload), payload=payload),
        "leg1",
    )
    nacks = s.ws.of_type("NACK_CHUNK")
    assert nacks and nacks[-1]["reason"] == "server_policy"
    assert s.state == SessionState.ACTIVE  # not fatal — client's problem


# ---------------------------------------------------------------------------
# Hook loading (config-driven authorize)
# ---------------------------------------------------------------------------

def test_load_hook_valid():
    from mfup.hooks import load_hook
    fn = load_hook("os.path:join")
    assert fn("a", "b") == "a/b"


@pytest.mark.parametrize("bad", ["", "no_colon", "nonexistent.module:fn", "os.path:nonexistent"])
def test_load_hook_invalid_raises(bad):
    from mfup.hooks import load_hook
    with pytest.raises((ImportError, AttributeError, ModuleNotFoundError)):
        load_hook(bad)


# ---------------------------------------------------------------------------
# Per-session base_dir (authorize hook: per-user homes)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_per_session_base_dir_staging_and_publish(tmp_path):
    from mfup.session_manager import SessionRegistry
    from mfup.publish import publish_session

    global_base = tmp_path / "global"
    home = tmp_path / "homes" / "alice"
    global_base.mkdir(parents=True)

    reg = SessionRegistry(global_base)
    s = await reg.create("sid1", "tok", "leg1", "2099-01-01T00:00:00+00:00",
                         target_dir="incoming", base_dir=home)
    s.ws = FakeWS()

    # Staging must live INSIDE the user's home, not the global base.
    assert (home / ".incoming.sid1" / "payload").is_dir()
    assert not (global_base / ".incoming.sid1").exists()

    # Upload one file and publish — it must land in the home's target.
    await s.process_frame(node(1, 0, NodeKind.FILE, "hello.txt", size=5), "leg1")
    await s.process_frame(FileOpenFrame(node_id=1, size=5), "leg1")
    payload = b"hello"
    await s.process_frame(
        FileChunkFrame(node_id=1, offset=0, length=5, checksum=crc32c(payload), payload=payload),
        "leg1",
    )
    await s.process_frame(FileCloseFrame(node_id=1, size_sent=5), "leg1")
    await s.process_frame(SessionEndFrame(scan_done_units=1, scan_est_units=1,
                                          body_done_bytes=5, body_est_bytes=5), "leg1")
    result = await s.try_commit()
    assert result and result["t"] == "COMMIT_OK"

    published = publish_session(home, "sid1", home / "incoming")
    assert published == ["hello.txt"]
    assert (home / "incoming" / "hello.txt").read_bytes() == b"hello"
    assert not (home / ".incoming.sid1").exists()  # staging reclaimed


@pytest.mark.asyncio
async def test_recover_session_derives_base_from_staging_parent(tmp_path):
    from mfup.session_manager import SessionRegistry

    global_base = tmp_path / "global"
    home = tmp_path / "homes" / "bob"
    global_base.mkdir(parents=True)

    reg1 = SessionRegistry(global_base)
    s1 = await reg1.create("sid2", "tok", "leg1", "2099-01-01T00:00:00+00:00",
                           target_dir=".", base_dir=home)
    s1.detach_leg()
    await reg1.remove("sid2")

    # "Another worker": fresh registry, recovery via the staging path only
    # (as lazy-resume does, from Redis meta). Base must come from the parent.
    reg2 = SessionRegistry(global_base)
    s2 = await reg2.recover_session("sid2", home / ".incoming.sid2")
    assert s2 is not None
    assert s2.base_dir == home
    # And it is resumable.
    resumed = await reg2.resume("sid2", "tok", "leg2")
    assert resumed.state == SessionState.ACTIVE


# ---------------------------------------------------------------------------
# Per-file mapping at publish (map_file hook) + auth/meta persistence
# ---------------------------------------------------------------------------

async def _upload_tree(s, files):
    """files: list of (node_id, parent_id, name, content) with parent dirs
    given as (node_id, parent_id, name, None) rows first."""
    for nid, pid, name, content in files:
        kind = NodeKind.DIR if content is None else NodeKind.FILE
        await s.process_frame(node(nid, pid, kind, name,
                                   size=None if content is None else len(content)), "leg1")
        if content is not None:
            await s.process_frame(FileOpenFrame(node_id=nid, size=len(content)), "leg1")
            await s.process_frame(FileChunkFrame(node_id=nid, offset=0, length=len(content),
                                                 checksum=crc32c(content), payload=content), "leg1")
            await s.process_frame(FileCloseFrame(node_id=nid, size_sent=len(content)), "leg1")
    n_nodes = len(files)
    total = sum(len(c) for _, _, _, c in files if c is not None)
    await s.process_frame(SessionEndFrame(scan_done_units=n_nodes, scan_est_units=n_nodes,
                                          body_done_bytes=total, body_est_bytes=total), "leg1")
    result = await s.try_commit()
    assert result and result["t"] == "COMMIT_OK", result


@pytest.mark.asyncio
async def test_mapped_publish_by_type(tmp_path):
    from mfup.publish import list_payload_files, publish_session_mapped

    s = make_session(tmp_path, sid="map1")
    await _upload_tree(s, [
        (1, 0, "shots", None),
        (2, 1, "a.jpg", b"JPG1"),
        (3, 1, "b.pdf", b"PDFPDF"),
        (4, 0, "c.jpg", b"JPG2!"),
    ])

    files = list_payload_files(tmp_path, "map1")
    assert [f[0] for f in files] == ["c.jpg", "shots/a.jpg", "shots/b.pdf"]

    # Consumer decision: images by scope, documents flat — client layout gone.
    mapping = {
        "shots/a.jpg": "media/avatars/a.jpg",
        "c.jpg": "media/avatars/c.jpg",
        "shots/b.pdf": "docs/b.pdf",
    }
    target = tmp_path / "final"
    published = publish_session_mapped(tmp_path, "map1", target, mapping)
    assert sorted(published) == ["docs/b.pdf", "media/avatars/a.jpg", "media/avatars/c.jpg"]
    assert (target / "media/avatars/a.jpg").read_bytes() == b"JPG1"
    assert (target / "docs/b.pdf").read_bytes() == b"PDFPDF"
    assert not (target / "shots").exists()          # client layout not replicated
    assert not (tmp_path / ".incoming.map1").exists()  # staging reclaimed


@pytest.mark.asyncio
async def test_mapped_publish_rejects_escape_and_collision(tmp_path):
    from mfup.publish import publish_session_mapped, MappingError

    s = make_session(tmp_path, sid="map2")
    await _upload_tree(s, [
        (1, 0, "x.bin", b"xx"),
        (2, 0, "y.bin", b"yy"),
    ])
    target = tmp_path / "final2"

    with pytest.raises(MappingError):
        publish_session_mapped(tmp_path, "map2", target, {"x.bin": "../evil"})
    with pytest.raises(MappingError):
        publish_session_mapped(tmp_path, "map2", target, {"x.bin": "same.bin", "y.bin": "same.bin"})
    # Nothing moved, staging intact after failed attempts.
    assert (tmp_path / ".incoming.map2" / "payload" / "x.bin").exists()
    assert not target.exists() or not any(target.iterdir())


@pytest.mark.asyncio
async def test_auth_and_meta_persist_across_recovery(tmp_path):
    import json as _json
    from mfup.session_manager import SessionRegistry

    reg1 = SessionRegistry(tmp_path)
    s1 = await reg1.create(
        "pers1", "tok", "leg1", "2099-01-01T00:00:00+00:00",
        meta_json=_json.dumps({"scope": "avatars", "album": 7}),
    )
    s1.apply_auth(12345, 10, {"user_id": "alice"})
    s1.detach_leg()
    await reg1.remove("pers1")

    # Fresh registry = restart / another worker.
    reg2 = SessionRegistry(tmp_path)
    s2 = await reg2.recover_session("pers1", tmp_path / ".incoming.pers1")
    assert s2 is not None
    assert s2.client_meta == {"scope": "avatars", "album": 7}
    assert s2.quota_max_bytes == 12345
    assert s2.quota_max_files == 10
    assert s2.auth_context == {"user_id": "alice"}
