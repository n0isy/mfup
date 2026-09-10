import asyncio
import json
import sqlite3

import pytest
from fastapi.testclient import TestClient
from mfup_core import Engine, ProtocolError, relative_path
from mfup_fastapi import create_app


@pytest.fixture
def app(tmp_path):
    return create_app(
        tmp_path, lambda request: {}, limits=dict(partBytes=1024, batchBytes=8192, maxParts=16)
    )


@pytest.fixture
def client(app):
    with TestClient(app) as client:
        yield client


def ticket(client):
    return client.post("/mfup/sessions", json=dict(protocol="MFUP/3", targetDir="test")).json()


def headers(t):
    return {"Authorization": "Bearer " + t["token"], "X-MFUP-Epoch": str(t["epoch"])}


def call(client, t, action="", data=None):
    url = "/mfup/sessions/" + t["id"] + action
    return (
        client.get(url, headers=headers(t))
        if data is None
        else client.post(url, headers=headers(t), json=data)
    )


def send(client, t, files, bid="batch"):
    manifest = dict(
        files=[[name, len(content), 1, 0, len(content)] for name, content in files], dirs=[]
    )
    parts = [("manifest", (None, json.dumps(manifest)))] + [
        (str(i), ("file", content)) for i, (_, content) in enumerate(files)
    ]
    return client.post(
        "/mfup/sessions/" + t["id"] + "/batches/" + bid, headers=headers(t), files=parts
    )


def test_grouped_files_and_idempotent_publish(client, tmp_path):
    t = ticket(client)
    receipt = send(client, t, [("a", b"abc"), ("folder/b", b"def")])
    assert receipt.status_code == 200, receipt.text
    assert receipt.json()["parts"] == 2
    assert call(client, t, "/commit", dict(files=2, dirs=0, bytes=6)).status_code == 200
    first = call(client, t, "/publish", {})
    assert first.status_code == 200, first.text
    assert call(client, t, "/publish", {}).json() == first.json()
    assert (tmp_path / "published/test/folder/b").read_bytes() == b"def"


def test_exact_part_limit_and_spelling(client):
    t = ticket(client)
    assert send(client, t, [(f"f{i}", b"x") for i in range(16)]).status_code == 200
    other = ticket(client)
    assert (
        send(client, other, [("Folder/a", b"a"), ("folder/b", b"b")]).json()["error"]
        == "path_conflict"
    )


@pytest.mark.parametrize("choice", ["overwrite", "cancel"])
def test_existing_parent_file(client, tmp_path, choice):
    (tmp_path / "published/test").mkdir(parents=True)
    (tmp_path / "published/test/folder").write_bytes(b"old")
    t = ticket(client)
    assert send(client, t, [("folder/child", b"new")]).status_code == 200
    q = call(client, t).json()["asks"][0]
    assert q["id"] == "overwrite"
    assert call(client, t, "/answers", dict(id=q["id"], choice=choice)).status_code == 200
    if choice == "cancel":
        assert call(client, t).json()["state"] == "cancelled"
        assert (tmp_path / "published/test/folder").read_bytes() == b"old"
        return
    call(client, t, "/commit", dict(files=1, dirs=0, bytes=3))
    assert call(client, t, "/publish", {}).status_code == 200
    assert (tmp_path / "published/test/folder/child").read_bytes() == b"new"


def test_lost_receipt_and_immutable_duplicate(client, app):
    t = ticket(client)
    first = send(client, t, [("a", b"abc")])
    assert call(client, t, "/batches/batch").json() == first.json()
    assert send(client, t, [("a", b"xyz")]).status_code == 200
    assert app.state.mfup.engine.payload(t["id"], "a").read_bytes() == b"abc"
    assert send(client, t, [("b", b"abc")]).status_code == 409


def test_resume_epoch_and_pages(client):
    t = ticket(client)
    send(client, t, [("a", b"abc"), ("b", b"def")])
    assert call(client, t, "/resume", {}).json()["epoch"] == 2
    assert send(client, t, [("c", b"new")], "next").json()["error"] == "stale_epoch"
    page = call(client, t, "/files?limit=1").json()
    assert page["files"][0]["offsets"] == [0] and page["next"] == "a"


def test_question_does_not_pause_other_files(client, tmp_path):
    (tmp_path / "published/test").mkdir(parents=True)
    (tmp_path / "published/test/a").write_bytes(b"old")
    t = ticket(client)
    assert send(client, t, [("a", b"new")]).status_code == 200
    assert len(call(client, t).json()["asks"]) == 1
    assert send(client, t, [("b", b"other")], "next").status_code == 200
    assert call(client, t, "/commit", dict(files=2, dirs=0, bytes=8)).status_code == 200
    assert call(client, t, "/publish", {}).json()["error"] == "answers_required"
    assert call(client, t, "/properties", dict(overwrite=True)).status_code == 200
    assert call(client, t, "/publish", {}).status_code == 200
    assert (tmp_path / "published/test/a").read_bytes() == b"new"
    assert (tmp_path / "published/test/b").read_bytes() == b"other"


def test_bad_part_is_not_confirmed(client):
    t = ticket(client)
    manifest = dict(files=[["a", 3, 1, 0, 3]], dirs=[])
    result = client.post(
        "/mfup/sessions/" + t["id"] + "/batches/bad",
        headers=headers(t),
        files=[("manifest", (None, json.dumps(manifest))), ("0", ("a", b"longer"))],
    )
    assert result.status_code == 400, result.text
    assert call(client, t, "/files").json()["files"][0]["offsets"] == []
    assert send(client, t, [("a", b"abc")]).status_code == 200


def test_truncated_multipart_is_not_confirmed(client):
    t = ticket(client)
    body = b'--test\r\nContent-Disposition: form-data; name="manifest"\r\n\r\n{"files":[],"dirs":["empty"]}\r\n'
    response = client.post(
        "/mfup/sessions/" + t["id"] + "/batches/bad",
        headers={**headers(t), "Content-Type": "multipart/form-data; boundary=test"},
        content=body,
    )
    assert response.status_code == 400
    assert call(client, t, "/batches/bad").status_code == 404


def test_partial_file_is_rewritten_after_interruption(client, app, tmp_path):
    t = ticket(client)
    manifest = json.dumps(dict(files=[["partial", 2048, 1, 0, 1024]], dirs=[]))
    prefix = f'--part\r\nContent-Disposition: form-data; name="manifest"\r\n\r\n{manifest}\r\n--part\r\nContent-Disposition: form-data; name="0"; filename="partial"\r\n\r\n'
    response = client.post(
        "/mfup/sessions/" + t["id"] + "/batches/broken",
        headers={**headers(t), "Content-Type": "multipart/form-data; boundary=part"},
        content=prefix.encode() + b"A" * 500,
    )
    assert response.status_code == 400
    assert app.state.mfup.engine.payload(t["id"], "partial").stat().st_size > 0
    t["epoch"] = call(client, t, "/resume", {}).json()["epoch"]
    assert call(client, t, "/files").json()["files"][0]["offsets"] == []
    for offset, content in [(0, b"B" * 1024), (1024, b"C" * 1024)]:
        data = dict(files=[["partial", 2048, 1, offset, 1024]], dirs=[])
        response = client.post(
            "/mfup/sessions/" + t["id"] + "/batches/retry" + str(offset),
            headers=headers(t),
            files=[("manifest", (None, json.dumps(data))), ("0", ("partial", content))],
        )
        assert response.status_code == 200
    assert call(client, t, "/commit", dict(files=1, dirs=0, bytes=2048)).status_code == 200
    assert call(client, t, "/publish", {}).status_code == 200
    assert (tmp_path / "published/test/partial").read_bytes() == b"B" * 1024 + b"C" * 1024


def test_other_token_and_cancel(client):
    a, b = ticket(client), ticket(client)
    assert call(client, dict(a, token=b["token"])).status_code == 403
    send(client, a, [("a", b"data")])
    assert call(client, a, "/cancel", {}).json()["state"] == "cancelled"
    assert send(client, a, [("b", b"new")], "other").status_code == 409


@pytest.mark.asyncio
async def test_single_owner_and_range_size(tmp_path):
    engine = Engine(tmp_path, lambda request: {}, limits=dict(partBytes=1024))
    with pytest.raises(sqlite3.OperationalError, match="locked"):
        Engine(tmp_path, lambda request: {}, limits=dict(partBytes=1024))
    await engine.close()
    with pytest.raises(ProtocolError, match="part_size_in_use"):
        Engine(tmp_path, lambda request: {}, limits=dict(partBytes=2048))


@pytest.mark.asyncio
async def test_resume_and_sweep(tmp_path):
    engine = Engine(tmp_path, lambda request: {})
    t = await engine.create(dict(protocol="MFUP/3"), {})
    engine.db.execute("UPDATE sessions SET expires=0 WHERE id=?", (t["id"],))
    await engine.resume(t["id"])
    assert await engine.sweep() == 0
    batch = await engine.begin(t["id"], 2, "active", lambda: None)
    engine.db.execute("UPDATE sessions SET expires=0 WHERE id=?", (t["id"],))
    assert await engine.sweep() == 0
    engine.end(batch)
    assert sum(await asyncio.gather(engine.sweep(), engine.sweep())) == 1
    with pytest.raises(ProtocolError):
        await engine.resume(t["id"])
    await engine.close()


def test_restart_preserves_ranges_and_questions(tmp_path):
    def make():
        return create_app(
            tmp_path, lambda request: {}, limits=dict(partBytes=1024, batchBytes=8192, maxParts=16)
        )

    with TestClient(make()) as first:
        t = ticket(first)
        assert send(first, t, [("a", b"abc")]).status_code == 200
    with TestClient(make()) as second:
        t["epoch"] = call(second, t, "/resume", {}).json()["epoch"]
        assert call(second, t, "/files").json()["files"][0]["offsets"] == [0]
        assert send(second, t, [("b", b"def")], "second").status_code == 200
        call(second, t, "/commit", dict(files=2, dirs=0, bytes=6))
        assert call(second, t, "/publish", {}).status_code == 200


def test_interrupted_publication(tmp_path):
    def make():
        return create_app(
            tmp_path, lambda request: {}, limits=dict(partBytes=1024, batchBytes=8192, maxParts=16)
        )

    first_app = make()
    with TestClient(first_app) as first:
        t = ticket(first)
        send(first, t, [("a", b"aaa"), ("b", b"bbb")])
        call(first, t, "/commit", dict(files=2, dirs=0, bytes=6))
        engine = first_app.state.mfup.engine
        (tmp_path / "published/test").mkdir(parents=True)
        engine.payload(t["id"], "a").rename(tmp_path / "published/test/a")
        engine.db.execute("UPDATE sessions SET state='publishing' WHERE id=?", (t["id"],))
    with TestClient(make()) as second:
        assert call(second, t, "/resume", {}).json()["state"] == "publishing"
        assert call(second, t, "/publish", {}).json()["published"] == ["a", "b"]
    assert (tmp_path / "published/test/a").read_bytes() == b"aaa"
    assert (tmp_path / "published/test/b").read_bytes() == b"bbb"


@pytest.mark.parametrize("value", ["../a", "/a", "a\\b", "CON", "a/..", "x.", "a:b", "a\0b"])
def test_portable_paths(value):
    with pytest.raises(ProtocolError):
        relative_path(value)
