import asyncio
import json

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from mfup_core import Engine, ProtocolError
from mfup_fastapi import MfupConfig, MfupEngine, create_app


def create(client, meta=None, suffix=""):
    return client.post("/api/mfup/sessions" + suffix, json=dict(protocol="MFUP/3", meta=meta))


def call(client, ticket, action, data=None):
    return client.post(
        "/api/mfup/sessions/" + ticket["id"] + "/" + action,
        headers={"Authorization": "Bearer " + ticket["token"]},
        json=data or {},
    )


def upload(client, ticket, names=("a",), expected=200):
    parts = [
        (
            "manifest",
            (None, json.dumps(dict(files=[[name, 3, 1, 0, 3] for name in names], dirs=[]))),
        )
    ]
    parts += [(str(i), ("file", b"abc")) for i, _ in enumerate(names)]
    r = client.post(
        "/api/mfup/sessions/" + ticket["id"] + "/batches/first",
        headers={"Authorization": "Bearer " + ticket["token"], "X-MFUP-Epoch": "1"},
        files=parts,
    )
    assert r.status_code == expected, r.text
    if expected != 200:
        return r
    return call(client, ticket, "commit", dict(files=len(names), dirs=0, bytes=3 * len(names)))


def app_for(base, **options):
    return create_app(
        MfupConfig(
            base_dir=base,
            authorize=options.pop("authorize", lambda r: {}),
            prefix="/api",
            sweep_interval_ms=0,
            on_error=lambda e: None,
            **options,
        )
    )


def test_request_context_limits_and_authorize_failure(tmp_path):
    seen = []

    def authorize(r):
        seen.append(r)
        if r["meta"] == "fail":
            raise RuntimeError("consumer")
        return {}

    with TestClient(app_for(tmp_path, authorize=authorize, max_meta_bytes=20)) as c:
        assert create(c, "a" * 30).status_code == 413
        assert not seen
        ticket = create(c, dict(a=1), "?album=42").json()
        assert seen[0]["sessionId"] == ticket["id"]
        assert seen[0]["query"] == dict(album="42")
        assert seen[0]["client"]
        assert create(c, "fail").status_code == 403


def test_embedding_lifecycle_prefix_and_websocket(tmp_path):
    mfup = MfupEngine(MfupConfig(base_dir=tmp_path, authorize=lambda r: {}))
    app = FastAPI(lifespan=mfup.lifespan)
    app.include_router(mfup.router, prefix="/api")
    with TestClient(app) as c:
        ticket = create(c).json()
        assert c.get("/mfup/health").status_code == 404
        for _ in range(20):
            with c.websocket_connect("/api/mfup/control") as ws:
                ws.send_json(dict(type="subscribe", **ticket))
                assert ws.receive_json()["id"] == ticket["id"]
        assert c.portal.call(mfup.engine.sweep, 9007199254740991) == 1
    assert mfup.engine.closed


def test_metadata_mapping_staged_processing_and_restart(tmp_path):
    seen = []
    home = tmp_path / "home"

    def mapping(r):
        seen.append(r)
        with pytest.raises(ProtocolError):
            app.state.mfup.engine.open_staged(r["sessionId"], r["path"])
        return "mapped/" + r["name"]

    def committed(event):
        with app.state.mfup.engine.open_staged(event["sessionId"], "a") as file:
            assert file.read() == b"abc"

    app = app_for(
        tmp_path,
        on_committed=committed,
        authorize=lambda r: dict(baseDir=str(home), targetDir="scope", context=dict(uid=7)),
        map_file=mapping,
    )
    with TestClient(app) as c:
        t = create(c).json()
        assert upload(c, t).status_code == 200
        assert len(seen) == 1
        e = app.state.mfup.engine
        assert e.get_session(t["id"])["stagingDir"] == str(home / "staging" / t["id"])
        assert len(list(e.list_staged(t["id"]))) == 1
        c.portal.call(e.prepare_publish, t["id"])
        c.portal.call(e.prepare_publish, t["id"])
        assert len(seen) == 1
        assert seen[0]["targetDir"] == "scope" and seen[0]["context"] == dict(uid=7)

    def no_map(r):
        raise AssertionError("saved plan must be used")

    with TestClient(app_for(tmp_path, map_file=no_map)) as c:
        assert call(c, t, "resume").status_code == 200
        assert call(c, t, "publish").status_code == 200
    assert (home / "published/scope/mapped/a").read_bytes() == b"abc"


@pytest.mark.parametrize("kind", ["same", "parent", "case"])
def test_complete_mapping_plan_validation(tmp_path, kind):
    def mapping(r):
        return (
            "same"
            if kind == "same"
            else ("p" if r["path"] == "a" else "p/b")
            if kind == "parent"
            else ("Folder/a" if r["path"] == "a" else "folder/b")
        )

    app = app_for(tmp_path, map_file=mapping)
    with TestClient(app) as c:
        t = create(c).json()
        assert upload(c, t, ("a", "b"), expected=409).json()["error"] == "mapping_error"
        assert app.state.mfup.engine.snapshot(t["id"])["state"] == "uploading"
        app.state.mfup.engine.map_file = lambda r: None
        assert upload(c, t, ("a", "b")).status_code == 200
        assert call(c, t, "publish").status_code == 200
    assert (tmp_path / "published/uploads/a").read_bytes() == b"abc"


def test_hook_failure_explicit_retry_and_server_only(tmp_path):
    calls = 0

    def hook(event):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("temporary")
        return False

    app = app_for(tmp_path, on_committed=hook, client_publish=False)
    with TestClient(app) as c:
        t = create(c).json()
        r = upload(c, t)
        assert r.status_code == 200 and r.json()["processing"] == "failed"
        assert call(c, t, "publish").status_code == 403
        call(c, t, "commit", dict(files=1, dirs=0, bytes=3))
        assert calls == 1
        assert c.portal.call(app.state.mfup.engine.retry_committed, t["id"])["processing"] == "done"
        assert calls == 2
        assert c.portal.call(app.state.mfup.engine.publish, t["id"])["state"] == "published"


@pytest.mark.asyncio
async def test_running_hook_prevents_client_publish_and_cleanup(tmp_path):
    entered, release = asyncio.Event(), asyncio.Event()

    async def hook(event):
        entered.set()
        await release.wait()
        return False

    e = Engine(tmp_path, lambda r: {}, on_committed=hook)
    try:
        t = await e.create(dict(protocol="MFUP/3"), {})
        commit = asyncio.create_task(e.commit(t["id"], dict(files=0, dirs=0, bytes=0)))
        await entered.wait()
        try:
            with pytest.raises(ProtocolError) as exc:
                await e.publish_from_client(t["id"])
            assert exc.value.code == "processing_required"
            assert await e.sweep(9007199254740991) == 0
        finally:
            release.set()
            await commit
    finally:
        await e.close()


def test_server_auto_publish_continues_after_answer(tmp_path):
    dest = tmp_path / "published/uploads/a"
    dest.parent.mkdir(parents=True)
    dest.write_text("old")
    with TestClient(app_for(tmp_path, auto_publish=True, client_publish=False)) as c:
        t = create(c).json()
        r = upload(c, t).json()
        assert r["state"] == "committed"
        answer = call(c, t, "answers", dict(id=r["asks"][0]["id"], choice="overwrite"))
        assert answer.json()["state"] == "published"
        assert call(c, t, "publish").status_code == 403
    assert dest.read_bytes() == b"abc"


def test_boolean_committed_decision(tmp_path):
    with TestClient(app_for(tmp_path, on_committed=lambda r: True, client_publish=False)) as c:
        t = create(c).json()
        assert upload(c, t).json()["state"] == "published"


def test_config_from_env_resolves_hooks_and_rejects_invalid_values(tmp_path):
    config = MfupConfig.from_env(
        dict(
            MFUP_BASE_DIR=str(tmp_path),
            MFUP_AUTHORIZE="test_extending:allow",
            MFUP_AUTO_PUBLISH="true",
            MFUP_CLIENT_PUBLISH="0",
        )
    )
    options = config.engine_options()
    assert options["authorize"]({}) == {}
    assert options["auto_publish"] is True and options["client_publish"] is False
    with pytest.raises(ValueError):
        MfupConfig.from_env(dict(MFUP_AUTO_PUBLISH="sometimes"))
    with pytest.raises((ValueError, ModuleNotFoundError)):
        MfupConfig.from_env({}).engine_options()


def allow(request):
    return {}


def test_failed_processing_policy_and_root_survive_restart(tmp_path):
    home = tmp_path / "separate-root"

    def fail(event):
        raise RuntimeError("interrupted")

    with TestClient(
        app_for(
            tmp_path,
            authorize=lambda r: dict(
                baseDir=str(home), targetDir="area", clientPublish=False, context=dict(job=42)
            ),
            on_committed=fail,
        )
    ) as c:
        t = create(c).json()
        assert upload(c, t).json()["processing"] == "failed"

    def recovered(event):
        assert event["context"] == dict(job=42)
        assert event["baseDir"] == str(home)
        return True

    app = app_for(tmp_path, on_committed=recovered)
    with TestClient(app) as c:
        assert call(c, t, "publish").status_code == 403
        assert c.portal.call(app.state.mfup.engine.retry_committed, t["id"])["state"] == "published"
    assert (home / "published/area/a").read_bytes() == b"abc"


@pytest.mark.asyncio
async def test_early_mapping_question_and_cancellation(tmp_path):
    entered, release = asyncio.Event(), asyncio.Event()

    async def mapping(request):
        if request["name"] == "b":
            entered.set()
            await release.wait()
        return request["path"]

    engine = Engine(tmp_path, lambda r: {}, map_file=mapping)
    dest = tmp_path / "published/uploads"
    dest.mkdir(parents=True)
    (dest / "a").write_bytes(b"old")
    try:
        ticket = await engine.create(dict(protocol="MFUP/3"), {})
        sid = ticket["id"]
        batch = await engine.begin(sid, 1, "early", lambda: None)

        async def receive():
            try:
                return await engine.prepare(
                    batch, dict(files=[["a", 3, 1, 0, 3], ["b", 3, 1, 0, 3]], dirs=[])
                )
            finally:
                engine.end(batch)

        pending = asyncio.create_task(receive())
        await asyncio.wait_for(entered.wait(), 2)
        assert engine.snapshot(sid)["overwriteRequired"]
        assert engine.snapshot(sid)["confirmedBytes"] == 0
        await engine.set_properties(sid, dict(overwrite=True))
        assert (await asyncio.wait_for(engine.cancel(sid), 2))["state"] == "cancelled"
        with pytest.raises(ProtocolError):
            await pending
        assert await engine.sweep(9007199254740991) == 0
    finally:
        release.set()
        await engine.close()


@pytest.mark.asyncio
async def test_large_inventory_and_published_pages_stay_bounded(tmp_path):
    engine = Engine(tmp_path, lambda r: {})
    try:
        t = await engine.create(dict(protocol="MFUP/3"), {})

        def records():
            for i in range(100000):
                name = f"f{i:06}"
                yield t["id"], name, name, name, name

        engine.db.execute("BEGIN IMMEDIATE")
        engine.db.executemany(
            "INSERT INTO nodes(sid,path,kind,size,mtime,destination,source_key,destination_key,mapped,done) VALUES(?,?,'file',0,1,?,?,?,1,1)",
            records(),
        )
        engine.db.execute("COMMIT")
        engine.db.execute("UPDATE sessions SET state='committed' WHERE id=?", (t["id"],))
        unbounded = []
        engine.db.set_trace_callback(
            lambda sql: (
                unbounded.append(sql)
                if "SELECT * FROM nodes" in sql and "LIMIT" not in sql
                else None
            )
        )
        state = await engine.publish(t["id"])
        assert not unbounded
        assert state["files"] == state["publishedCount"] == 100000
        assert len(state["published"]) == 256
        assert state["publishedNext"] == "f000255"
        assert len(engine.published_page(t["id"], "f099990")["files"]) == 9
    finally:
        await engine.close()
