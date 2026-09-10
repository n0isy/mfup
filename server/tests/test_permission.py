import asyncio
import errno
import json
from pathlib import Path
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient
from mfup_core import Engine
from mfup_fastapi import create_app


@pytest.fixture
def app(tmp_path):
    return create_app(
        tmp_path, lambda r: {}, sweep_interval_ms=0, on_error=lambda e: None, max_meta_bytes=65536
    )


@pytest.fixture
def client(app):
    with TestClient(app) as c:
        yield c


def ticket(c, overwrite=False):
    return c.post(
        "/mfup/sessions", json=dict(protocol="MFUP/3", targetDir="test", overwrite=overwrite)
    ).json()


def call(c, t, action="", data=None):
    url = "/mfup/sessions/" + t["id"] + action
    headers = dict(Authorization="Bearer " + t["token"])
    return c.get(url, headers=headers) if data is None else c.post(url, headers=headers, json=data)


def send(c, t, files, bid):
    parts = [
        (
            "manifest",
            (None, json.dumps(dict(files=[[n, len(b), 1, 0, len(b)] for n, b in files], dirs=[]))),
        )
    ]
    parts += [(str(i), (n, b)) for i, (n, b) in enumerate(files)]
    return c.post(
        "/mfup/sessions/" + t["id"] + "/batches/" + bid,
        headers={"Authorization": "Bearer " + t["token"], "X-MFUP-Epoch": str(t["epoch"])},
        files=parts,
    )


def seed(base):
    target = base / "published/test"
    target.mkdir(parents=True)
    for name in ["a", "later"]:
        (target / name).write_bytes(b"old")


@pytest.mark.parametrize("phase", ["during", "after"])
def test_one_approval_covers_whole_session(client, tmp_path, phase):
    seed(tmp_path)
    t = ticket(client)
    assert send(client, t, [("a", b"new")], "first").status_code == 200
    state = call(client, t).json()
    assert state["overwriteRequired"] and len(state["asks"]) == 1
    if phase == "during":
        assert call(client, t, "/properties", dict(overwrite=True)).status_code == 200
    assert (tmp_path / "published/test/a").read_bytes() == b"old"
    assert send(client, t, [("later", b"new"), ("unique", b"new")], "next").status_code == 200
    assert call(client, t, "/commit", dict(files=3, dirs=0, bytes=9)).status_code == 200
    if phase == "after":
        assert call(client, t, "/publish", {}).json()["error"] == "answers_required"
        assert call(client, t, "/properties", dict(overwrite=True)).status_code == 200
    assert call(client, t).json()["asks"] == []
    assert call(client, t, "/properties", dict(overwrite=True)).status_code == 200
    assert call(client, t, "/properties", dict(overwrite=False)).status_code == 409
    assert call(client, t, "/resume", {}).json()["overwrite"] is True
    assert call(client, t, "/publish", {}).status_code == 200
    assert (tmp_path / "published/test/later").read_bytes() == b"new"


@pytest.mark.parametrize("phase", ["during", "after"])
def test_cancel_preserves_every_destination(client, app, tmp_path, phase):
    seed(tmp_path)
    t = ticket(client)
    assert send(client, t, [("a", b"new"), ("unique", b"new")], "first").status_code == 200
    if phase == "after":
        assert call(client, t, "/commit", dict(files=2, dirs=0, bytes=6)).status_code == 200
    state = call(client, t, "/cancel", {}).json()
    assert state["state"] == "cancelled" and not state["overwriteRequired"] and not state["asks"]
    assert call(client, t, "/properties", dict(overwrite=True)).status_code == 409
    assert call(client, t, "/publish", {}).status_code == 409
    assert (tmp_path / "published/test/a").read_bytes() == b"old"
    assert not (tmp_path / "published/test/unique").exists()
    assert not app.state.mfup.engine.staging(t["id"]).exists()


def test_preapproval_and_late_cancel(client, tmp_path):
    seed(tmp_path)
    t = ticket(client, True)
    assert send(client, t, [("a", b"new")], "first").status_code == 200
    assert call(client, t).json()["asks"] == []
    call(client, t, "/commit", dict(files=1, dirs=0, bytes=3))
    assert call(client, t, "/publish", {}).status_code == 200
    assert call(client, t, "/cancel", {}).json()["state"] == "published"


def test_real_sqlite_allocation_failure(client, app, tmp_path):
    db = app.state.mfup.engine.db
    count = db.execute("PRAGMA page_count").fetchone()[0]
    db.execute(f"PRAGMA max_page_count={count}")
    result = client.post("/mfup/sessions", json=dict(protocol="MFUP/3", meta="m" * 32768))
    assert result.status_code == 507, result.text
    assert result.json()["error"] == "storage_full"
    assert not list((tmp_path / "staging").iterdir())
    db.execute("PRAGMA max_page_count=100000")
    assert ticket(client)["id"]


def test_eio_is_not_reported_as_malformed_multipart(client, app):
    t = ticket(client)
    original = Path.touch

    def fail(file, *args, **kwargs):
        if str(t["id"]) in str(file):
            raise OSError(errno.EIO, "test disk failure")
        return original(file, *args, **kwargs)

    with patch.object(Path, "touch", fail):
        result = send(client, t, [("a", b"abc")], "failed")
    assert result.status_code == 503, result.text
    assert result.json()["error"] == "storage_unavailable"
    assert call(client, t, "/batches/failed").status_code == 404
    assert call(client, t).json()["error"]["code"] == "storage_unavailable"
    assert not app.state.mfup.engine.runtime(t["id"]).active
    t["epoch"] = call(client, t, "/resume", {}).json()["epoch"]
    assert send(client, t, [("a", b"abc")], "retry").status_code == 200


def test_approval_survives_restart(tmp_path):
    seed(tmp_path)
    with TestClient(create_app(tmp_path, lambda r: {})) as c:
        t = ticket(c)
        send(c, t, [("a", b"new")], "first")
        assert call(c, t, "/properties", dict(overwrite=True)).status_code == 200
    with TestClient(create_app(tmp_path, lambda r: {})) as c:
        t["epoch"] = call(c, t, "/resume", {}).json()["epoch"]
        assert send(c, t, [("later", b"new")], "next").status_code == 200
        assert call(c, t).json()["asks"] == []
        call(c, t, "/commit", dict(files=2, dirs=0, bytes=6))
        assert call(c, t, "/publish", {}).status_code == 200


@pytest.mark.asyncio
async def test_cancel_during_committed_processing_cannot_publish_later(tmp_path):
    entered, release = asyncio.Event(), asyncio.Event()

    async def hook(event):
        entered.set()
        await release.wait()
        return True

    e = Engine(tmp_path, lambda r: {}, on_committed=hook, on_error=lambda e: None)
    try:
        t = await e.create(dict(protocol="MFUP/3"), {})
        commit = asyncio.create_task(e.commit(t["id"], dict(files=0, dirs=0, bytes=0)))
        await entered.wait()
        assert (await e.cancel(t["id"]))["state"] == "cancelled"
        release.set()
        await commit
        assert e.snapshot(t["id"])["state"] == "cancelled"
    finally:
        release.set()
        await e.close()


def test_recover_real_sqlite_full_after_file_moves(client, app, tmp_path):
    t = ticket(client, True)
    files = [(f"f{i}-" + ("x" * 180), b"v") for i in range(80)]
    assert send(client, t, files, "data").status_code == 200
    assert call(client, t, "/commit", dict(files=80, dirs=0, bytes=80)).status_code == 200
    db = app.state.mfup.engine.db
    db.executescript(
        "CREATE TABLE allocation_probe(value BLOB); CREATE TRIGGER publication_allocation BEFORE UPDATE OF state ON sessions WHEN NEW.state='published' BEGIN INSERT INTO allocation_probe VALUES(zeroblob(1048576)); END;"
    )
    pages = db.execute("PRAGMA page_count").fetchone()[0]
    db.execute(f"PRAGMA max_page_count={pages}")
    failed = call(client, t, "/publish", {})
    assert failed.status_code == 507, failed.text
    assert failed.json()["error"] == "storage_full"
    assert (tmp_path / "published/test" / files[0][0]).read_bytes() == b"v"
    db.execute("PRAGMA max_page_count=100000")
    assert call(client, t, "/resume", {}).json()["state"] == "publishing"
    assert len(call(client, t, "/publish", {}).json()["published"]) == 80
    assert call(client, t).json()["error"] is None


def test_missing_staged_file_does_not_publish_old_destination(client, app, tmp_path):
    seed(tmp_path)
    t = ticket(client, True)
    send(client, t, [("a", b"new")], "data")
    call(client, t, "/commit", dict(files=1, dirs=0, bytes=3))
    app.state.mfup.engine.payload(t["id"], "a").unlink()
    result = call(client, t, "/publish", {})
    assert result.status_code == 503
    assert result.json()["error"] == "storage_unavailable"
    assert call(client, t).json()["state"] == "committed"
    assert (tmp_path / "published/test/a").read_bytes() == b"old"


@pytest.mark.asyncio
async def test_cancel_with_active_range_drains_and_cleans(tmp_path):
    seed(tmp_path)
    e = Engine(tmp_path, lambda r: {})
    try:
        t = await e.create(dict(protocol="MFUP/3", targetDir="test"), {})
        batch = await e.begin(t["id"], 1, "active", lambda: e.end(batch))
        await e.prepare(batch, dict(files=[["a", 4096, 1, 0, 4096]], dirs=[]))
        assert e.snapshot(t["id"])["overwriteRequired"]
        results = await asyncio.gather(e.cancel(t["id"]), e.cancel(t["id"]))
        assert [r["state"] for r in results] == ["cancelled", "cancelled"]
        assert not e.staging(t["id"]).exists()
        assert not e.runtime(t["id"]).active
        assert (tmp_path / "published/test/a").read_bytes() == b"old"
    finally:
        await e.close()


def test_early_mapping_preserves_sqlite_full_on_rollback(client, app):
    engine = app.state.mfup.engine
    engine.map_file = lambda r: ("d" * 180) + "/" + ("e" * 180) + "/" + r["path"]
    t = ticket(client)
    files = [(f"f{i}", b"v") for i in range(80)]
    db = engine.db
    pages = db.execute("PRAGMA page_count").fetchone()[0]
    db.execute(f"PRAGMA max_page_count={pages}")
    failed = send(client, t, files, "data")
    assert failed.status_code == 507, failed.text
    assert engine.snapshot(t["id"])["files"] == 0
    db.execute("PRAGMA max_page_count=100000")
    t["epoch"] = call(client, t, "/resume", {}).json()["epoch"]
    assert send(client, t, files, "data").status_code == 200
    assert call(client, t, "/commit", dict(files=80, dirs=0, bytes=80)).status_code == 200
    assert call(client, t, "/publish", {}).status_code == 200
