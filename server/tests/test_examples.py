import importlib.util
import json
from pathlib import Path

from fastapi.testclient import TestClient

source = Path(__file__).resolve().parents[2] / "examples/multiuser-scopes/server/app.py"
spec = importlib.util.spec_from_file_location("scopes_example", source)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def create(client, scope="workspace", target="ignored", meta=None):
    return client.post(
        "/api/mfup/sessions",
        json=dict(
            protocol="MFUP/3", targetDir=target, meta=dict(scope=scope) if meta is None else meta
        ),
    )


def upload(client, ticket, name="a.txt", body=b"owned"):
    headers = {"Authorization": "Bearer " + ticket["token"], "X-MFUP-Epoch": str(ticket["epoch"])}
    prefix = "/api/mfup/sessions/" + ticket["id"]
    manifest = dict(files=[[name, len(body), 1, 0, len(body)]], dirs=[])
    result = client.post(
        prefix + "/batches/first",
        headers=headers,
        data={"manifest": json.dumps(manifest)},
        files={"0": (name, body)},
    )
    assert result.status_code == 200, result.text
    assert (
        client.post(
            prefix + "/commit", headers=headers, json=dict(files=1, dirs=0, bytes=len(body))
        ).status_code
        == 200
    )
    return prefix, headers


def test_example_identity_and_scope(tmp_path):
    app = module.create_example(tmp_path)
    with TestClient(app, base_url="https://testserver") as a:
        b = TestClient(app, base_url="https://testserver")
        uid_a, uid_b = (
            a.get("/api/whoami").json()["user_id"],
            b.get("/api/whoami").json()["user_id"],
        )
        assert uid_a != uid_b
        assert a.get("/api/whoami").json()["user_id"] == uid_a
        ticket = create(a, target=uid_b + "/scratch").json()
        prefix, headers = upload(a, ticket)
        assert a.post(prefix + "/publish", headers=headers, json={}).status_code == 200
        assert a.get("/api/files/workspace").json()["entries"] == [
            dict(name="a.txt", dir=False, size=5)
        ]
        assert a.get("/api/files/scratch").json()["entries"] == []
        assert b.get("/api/files/workspace").json()["entries"] == []
        assert b.get("/api/files/scratch").json()["entries"] == []
        assert (tmp_path / "published" / uid_a / "workspace/a.txt").read_bytes() == b"owned"
        assert b.get("/api/file/workspace?path=a.txt").status_code == 404


def test_example_requires_cookie_and_scope(tmp_path):
    with TestClient(module.create_example(tmp_path), base_url="https://testserver") as client:
        assert create(client).status_code == 403
        assert client.get("/api/files/workspace").status_code == 403
        client.get("/api/whoami")
        for meta in ({}, {"scope": "unknown"}, {"scope": ["workspace"]}, []):
            assert create(client, meta=meta).status_code == 403


def test_example_mapping_and_context_survive_restart(tmp_path):
    mapped = []

    def mapping(request):
        mapped.append(request)
        return "documents/" + request["path"]

    with TestClient(
        module.create_example(tmp_path, map_file=mapping), base_url="https://testserver"
    ) as client:
        uid = client.get("/api/whoami").json()["user_id"]
        ticket = create(client, "uploads").json()
        prefix, headers = upload(client, ticket)
        assert len(mapped) == 0
        client.portal.call(client.app.state.mfup.engine.prepare_publish, ticket["id"])
        assert len(mapped) == 1
        assert mapped[0]["context"] == dict(uid=uid, scope="uploads")

    def no_mapping(request):
        raise AssertionError("Mapping must remain persisted")

    with TestClient(
        module.create_example(tmp_path, map_file=no_mapping), base_url="https://testserver"
    ) as resumed:
        assert resumed.post(prefix + "/resume", headers=headers, json={}).status_code == 200
        assert resumed.post(prefix + "/publish", headers=headers, json={}).status_code == 200
    assert (tmp_path / "published" / uid / "uploads/documents/a.txt").read_bytes() == b"owned"


def test_example_scope_root_uses_public_contract(tmp_path):
    root = tmp_path / "scope-storage"
    app = module.create_example(tmp_path, scope_roots=dict(uploads=str(root)))
    with TestClient(app, base_url="https://testserver") as c:
        uid = c.get("/api/whoami").json()["user_id"]
        t = create(c, "uploads", target="other/scope").json()
        prefix, headers = upload(c, t)
        assert c.post(prefix + "/publish", headers=headers, json={}).status_code == 200
        info = app.state.mfup.engine.get_session(t["id"])
        assert info["baseDir"] == str(root)
        assert info["targetDir"] == f"{uid}/uploads"
        assert c.get("/api/file/uploads?path=a.txt").content == b"owned"
        assert c.get("/api/files/workspace").json()["entries"] == []
