import asyncio
import struct
from types import SimpleNamespace

import pytest
import pytest_asyncio
from fastapi import FastAPI
from fastapi.testclient import TestClient
from mfup_core import AuthResult
from mfup_core.protocol import FileOpenFrame, FileChunkFrame, FileCloseFrame, SessionEndFrame, SessionState, NodeKind, FrameReader, IncompleteFrameError, crc32c
from mfup_core.publish import ConflictError, MappingError, publish_session, publish_session_mapped
from mfup_core.storage import staging_dir, validate_session_id
from mfup_core.session_manager import SessionRegistry
from mfup_fastapi import MfupConfig, MfupEngine
from .test_edge_cases import make_session, node


@pytest.mark.parametrize("sid", ["", "x/../../other", "a/b", "a\\b", ".", "..", "a\0b", "a"*129, None, 1])
def test_session_id_before_staging(tmp_path, sid):
    with pytest.raises(ValueError, match="session_id"):
        staging_dir(tmp_path, sid)
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("sid", ["123", "a"*128, "upload_01.test-2", "01234567-89ab-cdef-0123-456789abcdef"])
def test_session_id_valid(sid):
    validate_session_id(sid)


@pytest_asyncio.fixture
async def session(tmp_path):
    s = make_session(tmp_path)
    yield s
    s._cancel_idle_timer()
    s._close_all_writers()
    s.db.close()


@pytest.mark.asyncio
async def test_duplicate_file_path(session, tmp_path):
    s = session
    for nid, content in [(1, b"AAA"), (2, b"BBB")]:
        for f in [node(nid, 0, NodeKind.FILE, "same.txt", 3), FileOpenFrame(node_id=nid, size=3),
                  FileChunkFrame(node_id=nid, offset=0, length=3, checksum=crc32c(content), payload=content),
                  FileCloseFrame(node_id=nid, size_sent=3)]:
            await s.process_frame(f, "leg1")
    await s.process_frame(node(2, 0, NodeKind.FILE, "same.txt", 3), "leg1")
    await s.process_frame(SessionEndFrame(scan_done_units=2, body_done_bytes=6), "leg1")
    assert await s.try_commit() == {"t": "COMMIT_OK", "files": 1, "bytes": 3}
    assert s.ws.of_type("REJECT_FILE")[0]["node_id"] == 2
    assert (staging_dir(tmp_path, "s1")/"payload/same.txt").read_bytes() == b"AAA"


@pytest.mark.asyncio
async def test_duplicate_file_through_directory_alias(session):
    s = session
    for nid in (1, 2):
        await s.process_frame(node(nid, 0, NodeKind.DIR, "folder"), "leg1")
    for nid, parent in ((3, 1), (4, 2)):
        await s.process_frame(node(nid, parent, NodeKind.FILE, "same.txt", 0), "leg1")
        await s.process_frame(FileOpenFrame(node_id=nid), "leg1")
    assert 3 in s.writers and 4 not in s.writers
    assert s.db.is_rejected(4)


@pytest.mark.asyncio
async def test_reopen_zero_offset(session):
    s = session
    await s.process_frame(node(1, 0, NodeKind.FILE, "empty", 0), "leg1")
    await s.process_frame(FileOpenFrame(node_id=1), "leg1")
    s.detach_leg(); s.attach_leg("leg2")
    await s.process_frame(FileOpenFrame(node_id=1), "leg2")
    await s.process_frame(FileCloseFrame(node_id=1), "leg2")
    await s.process_frame(SessionEndFrame(scan_done_units=1), "leg2")
    assert await s.try_commit() == {"t": "COMMIT_OK", "files": 1, "bytes": 0}


@pytest.mark.asyncio
async def test_recover_unjournaled_path(session, tmp_path):
    s = session
    await s.process_frame(node(1, 0, NodeKind.FILE, "leftover", 3), "leg1")
    p = staging_dir(tmp_path, "s1")/"payload/leftover"
    p.write_bytes(b"unconfirmed")
    await s.process_frame(FileOpenFrame(node_id=1, size=3), "leg1")
    await s.process_frame(FileChunkFrame(node_id=1, length=3, checksum=crc32c(b"new"), payload=b"new"), "leg1")
    await s.process_frame(FileCloseFrame(node_id=1, size_sent=3), "leg1")
    assert not s.db.is_rejected(1)
    assert p.read_bytes() == b"new"


@pytest.mark.asyncio
async def test_node_identity_cannot_change(session):
    await session.process_frame(node(1, 0, NodeKind.DIR, "original"), "leg1")
    with pytest.raises(ValueError, match="cannot change"):
        await session.process_frame(node(1, 0, NodeKind.DIR, "different"), "leg1")
    assert session.db.get_node(1)["name"] == "original"


def tree(base):
    payload = staging_dir(base, "s")/"payload"
    target = base/"target"
    payload.mkdir(parents=True); target.mkdir()
    return payload, target


@pytest.mark.parametrize("reversed", [False, True])
def test_mapping_file_is_parent(tmp_path, reversed):
    payload, target = tree(tmp_path)
    (payload/"a").write_text("A"); (payload/"b").write_text("B")
    mapping = {"a": "item/child", "b": "item"} if reversed else {"a": "item", "b": "item/child"}
    with pytest.raises(MappingError):
        publish_session_mapped(tmp_path, "s", target, mapping)
    assert list(target.iterdir()) == []
    assert sorted(p.name for p in payload.iterdir()) == ["a", "b"]


def test_merge_distinct_files(tmp_path):
    payload, target = tree(tmp_path)
    for directory in (payload, target): (directory/"folder").mkdir()
    (payload/"folder/new.txt").write_text("new"); (target/"folder/old.txt").write_text("old")
    publish_session(tmp_path, "s", target)
    assert (target/"folder/new.txt").read_text() == "new"
    assert (target/"folder/old.txt").read_text() == "old"


def test_replace_directory_with_file(tmp_path):
    payload, target = tree(tmp_path)
    (target/"item").mkdir(); (target/"item/old").write_text("old"); (payload/"item").write_text("new")
    with pytest.raises(ConflictError): publish_session(tmp_path, "s", target)
    assert (target/"item/old").read_text() == "old"
    publish_session(tmp_path, "s", target, action="merge_overwrite")
    assert (target/"item").read_text() == "new"


def test_reader_eof():
    r = FrameReader(); r.feed(bytes([0, 0, 0, 5, 6, 0]))
    assert r.drain() == []
    with pytest.raises(IncompleteFrameError): r.finish()
    r.feed(bytes([0, 0, 1])); assert r.drain()[0].node_id == 1
    r.finish()


class LocalIndex:
    """Only the expiry service is replaced; tests exercise the real router."""
    def __init__(self): self.entries = {}
    async def register(self, sid, expires, target, staging):
        self.entries[sid] = SimpleNamespace(target_dir=target, staging_dir=staging)
    async def get_meta(self, sid): return self.entries.get(sid)
    async def update_expiry(self, *args): pass
    async def remove(self, sid): self.entries.pop(sid, None)


@pytest.fixture
def http_app(tmp_path):
    async def allow(req): return AuthResult()
    e = MfupEngine(MfupConfig(base_dir=tmp_path, authorize=allow))
    e.registry = SessionRegistry(tmp_path); e.index = LocalIndex()
    app = FastAPI(); app.include_router(e.router)
    with TestClient(app) as client:
        yield client, e
        for sid in e.registry.all_sessions(): client.portal.call(e.registry.remove, sid)


def hello(sid="review"):
    return dict(t="HELLO", v="MFUP/2", session_id=sid, leg_id="one", target_dir=".")


@pytest.mark.parametrize("message", ["HELLO", "RESUME"])
def test_router_rejects_session_path(http_app, message):
    client, e = http_app
    with client.websocket_connect("/mfup/control") as ws:
        msg = hello("x/../../other"); msg["t"] = message
        ws.send_json(msg)
        assert ws.receive_json()["code"] == "bad_session_id"
    assert e.registry.all_sessions() == {}


@pytest.mark.parametrize("streaming", [False, True])
@pytest.mark.parametrize("with_end", [False, True])
def test_router_truncated_frame(http_app, streaming, with_end):
    client, e = http_app
    with client.websocket_connect("/mfup/control") as ws:
        ws.send_json(hello()); h = ws.receive_json()
        body = bytes([0, 0, 0, 100, 1])
        if with_end:
            body = struct.pack("!IBQQQQB", 34, 7, 0, 0, 0, 0, 1) + body
        res = client.post(f'/mfup/data/review/one?seq=0&final=1&epoch={h["epoch"]}',
                          headers={"X-MFUP-Token": h["resume_token"]}, content=iter([body]) if streaming else body)
        assert res.status_code == 400 and res.json()["error"] == "incomplete_frame"
        assert client.portal.call(lambda: e.registry.get("review").state) == SessionState.ACTIVE
        if not streaming:
            assert e.registry.get("review").final_seq_seen is False
            # The same sequence can carry a corrected body.
            res = client.post(f'/mfup/data/review/one?seq=0&epoch={h["epoch"]}',
                              headers={"X-MFUP-Token": h["resume_token"]}, content=b"")
            assert res.status_code == 200


@pytest.mark.parametrize("old_action", [None, "cancel", "CLIENT_ABORT"])
def test_old_control_cannot_detach_or_cancel_resumed_leg(http_app, old_action):
    client, e = http_app
    first_context = client.websocket_connect("/mfup/control")
    first = first_context.__enter__()
    first.send_json(hello()); h = first.receive_json()
    with client.websocket_connect("/mfup/control") as second:
        second.send_json(dict(t="RESUME", session_id="review", resume_token=h["resume_token"], leg_id="two"))
        resumed = second.receive_json()
        if old_action:
            first.send_json(dict(t="CLIENT_ABORT") if old_action == "CLIENT_ABORT" else dict(t="ACTION", action=old_action))
            client.portal.call(asyncio.sleep, 0.02)
        first_context.__exit__(None, None, None)
        s = e.registry.get("review")
        assert s.leg_id == "two"
        res = client.post(f'/mfup/data/review/two?seq=0&epoch={resumed["epoch"]}',
                          headers={"X-MFUP-Token": h["resume_token"]}, content=b"")
        assert res.status_code == 200


def test_recheck_control_after_pending_commit(http_app, monkeypatch):
    client, e = http_app
    entered, release = asyncio.Event(), asyncio.Event()
    async def pending_commit(session):
        session.db.set_state(SessionState.ACTIVE)
        entered.set()
        await release.wait()
    monkeypatch.setattr(e, "_try_commit", pending_commit)
    first_context = client.websocket_connect("/mfup/control")
    first = first_context.__enter__()
    first.send_json(hello()); h = first.receive_json()
    s = e.registry.get("review")
    client.portal.call(s.db.set_state, SessionState.COMMITTING)
    first.close()
    client.portal.call(asyncio.wait_for, entered.wait(), 2)
    try:
        with client.websocket_connect("/mfup/control") as second:
            second.send_json(dict(t="RESUME", session_id="review", resume_token=h["resume_token"], leg_id="two"))
            resumed = second.receive_json()
            client.portal.call(release.set)
            first_context.__exit__(None, None, None)
            assert s.leg_id == "two"
            res = client.post(f'/mfup/data/review/two?seq=0&epoch={resumed["epoch"]}',
                              headers={"X-MFUP-Token": h["resume_token"]}, content=b"")
            assert res.status_code == 200
    finally:
        client.portal.call(release.set)
