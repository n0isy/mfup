import importlib.util
import json
import tempfile
from pathlib import Path

from fastapi.testclient import TestClient
from mfup_core import AuthRequest, AuthResult, published_directory
from mfup_fastapi import MfupConfig

source = Path(__file__).with_name("example_app.py")
spec = importlib.util.spec_from_file_location("example_app", source)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
assert AuthRequest and AuthResult
with tempfile.TemporaryDirectory(prefix="mfup3-consumer-") as tmp:
    base = Path(tmp) / "data"
    home = Path(tmp) / "home"
    mapped = []

    def mapping(request):
        mapped.append(request)
        assert request["context"]["scope"] == "uploads"
        return "mapped/" + request["name"]

    def committed(event):
        assert event["baseDir"] == str(home)
        engine = app.state.mfup.engine
        assert [f["path"] for f in engine.list_staged(event["sessionId"])] == ["a.txt"]
        with engine.open_staged(event["sessionId"], "a.txt") as file:
            assert file.read() == b"consumer"
        return True

    app = module.create_example(
        base,
        scope_roots=dict(uploads=str(home)),
        map_file=mapping,
        on_committed=committed,
        client_publish=False,
    )
    with TestClient(app, base_url="https://testserver") as c:
        uid = c.get("/api/whoami").json()["user_id"]
        t = c.post(
            "/api/mfup/sessions", json=dict(protocol="MFUP/3", meta=dict(scope="uploads"))
        ).json()
        headers = {"Authorization": "Bearer " + t["token"], "X-MFUP-Epoch": "1"}
        prefix = "/api/mfup/sessions/" + t["id"]
        manifest = json.dumps(dict(files=[["a.txt", 8, 1, 0, 8]], dirs=[]))
        assert (
            c.post(
                prefix + "/batches/first",
                headers=headers,
                files=[("manifest", (None, manifest)), ("0", ("a.txt", b"consumer"))],
            ).status_code
            == 200
        )
        r = c.post(prefix + "/commit", headers=headers, json=dict(files=1, dirs=0, bytes=8))
        assert r.status_code == 200, r.text
        assert r.json()["state"] == "published"
        assert len(mapped) == 1
        assert c.post(prefix + "/publish", headers=headers, json={}).status_code == 403
        assert c.get("/api/file/uploads?path=mapped/a.txt").content == b"consumer"
        assert (
            published_directory(home, f"{uid}/uploads") / "mapped/a.txt"
        ).read_bytes() == b"consumer"
        c.cookies.clear()
        c.get("/api/whoami")
        assert c.get("/api/files/uploads").json()["entries"] == []
config = MfupConfig.from_env(
    dict(MFUP_AUTHORIZE="builtins:dict", MFUP_AUTO_PUBLISH="true", MFUP_CLIENT_PUBLISH="0")
)
assert config.engine_options()["authorize"]({}) == {}
assert config.auto_publish and not config.client_publish
print(
    "Installed wheels: public contracts, scoped example, staged reads, boolean publication and env config pass."
)
