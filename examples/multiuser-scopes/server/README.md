# multiuser-scopes — server

A regular FastAPI app that embeds the MFUP/2 engine from PyPI. All policy
(auto-created users, three zones, per-user layout) lives in ONE authorize
hook; see `app.py`.

## Run

Needs Python 3.10+ and a reachable Redis.

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
uvicorn app:app --port 8090
# sanity: curl -s localhost:8090/api/mfup/health  → "crc32c":"native"
```

Environment (all optional): `DEMO_DATA_DIR` (default `./data`),
`REDIS_URL` (default `redis://localhost:6379/0`).

Files land at `<DEMO_DATA_DIR>/<user_id>/<scope>/…` — the server decides,
whatever target the browser asked for.
