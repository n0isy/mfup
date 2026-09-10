"""Linux integration test: unprivileged process, small tmpfs and read-only tmpfs."""

import json
import tempfile
from pathlib import Path

from fastapi.testclient import TestClient
from mfup_fastapi import create_app

with tempfile.TemporaryDirectory(prefix="mfup-storage-") as tmp:
    base = Path(tmp)
    root = "/storage/home"
    app = create_app(
        base, lambda r: dict(baseDir=root), sweep_interval_ms=0, on_error=lambda e: None
    )
    with TestClient(app) as c:

        def create():
            return c.post("/mfup/sessions", json=dict(protocol="MFUP/3"))

        t = create().json()
        prefix = "/mfup/sessions/" + t["id"]
        headers = {"Authorization": "Bearer " + t["token"], "X-MFUP-Epoch": "1"}
        payload = b"x" * 1024 * 1024

        def send(bid):
            manifest = json.dumps(dict(files=[["a", len(payload), 1, 0, len(payload)]], dirs=[]))
            return c.post(
                prefix + "/batches/" + bid,
                headers=headers,
                files=[("manifest", (None, manifest)), ("0", ("a", payload))],
            )

        dest = Path(root) / "published/uploads/a"
        dest.parent.mkdir(parents=True)
        dest.write_bytes(b"old")
        filler = Path("/storage/filler")
        filler.write_bytes(b"f" * (3584 * 1024))
        failed = send("full")
        assert failed.status_code == 507, failed.text
        assert failed.json()["error"] == "storage_full"
        assert c.get(prefix + "/batches/full", headers=headers).status_code == 404
        assert dest.read_bytes() == b"old"
        filler.unlink()
        headers["X-MFUP-Epoch"] = str(
            c.post(prefix + "/resume", headers=headers, json={}).json()["epoch"]
        )
        assert send("retry").status_code == 200
        assert (
            c.post(
                prefix + "/commit", headers=headers, json=dict(files=1, dirs=0, bytes=len(payload))
            ).status_code
            == 200
        )
        assert (
            c.post(prefix + "/properties", headers=headers, json=dict(overwrite=True)).status_code
            == 200
        )
        assert c.post(prefix + "/publish", headers=headers, json={}).status_code == 200
        assert dest.read_bytes() == payload
        root = "/readonly"
        r = create()
        assert r.status_code == 503, r.text
        assert r.json()["error"] == "storage_unavailable"
        directory = base / "no-access"
        directory.mkdir()
        directory.chmod(0o500)
        root = str(directory)
        try:
            r = create()
            assert r.status_code == 503, r.text
            assert r.json()["error"] == "storage_unavailable"
        finally:
            directory.chmod(0o700)
print(
    "Python real ENOSPC, EROFS, EACCES: classified, no false receipt, original destination retained, resume verified."
)
