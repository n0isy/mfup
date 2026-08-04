# multiuser-scopes — Node edition

The same consumer example as [`../multiuser-scopes`](../multiuser-scopes),
with the Python/FastAPI server swapped for **express + `@mfup/server`**
(installed from npm, the way an outside integrator would). The React client
is literally the same directory — it only talks HTTP/WS, and both servers
speak the same protocol.

Differences from the Python edition:

| | Python (`:20061`) | Node (`:20062`) |
|---|---|---|
| Server | FastAPI + `mfup-fastapi` (PyPI) | express + `@mfup/server` (npm) |
| Session store | Redis (required) | **memory** — no Redis at all; restart recovery via a staging-dir scan |
| MFUP wiring | `app.include_router(engine.router, prefix="/api/mfup")` | `app.use("/api/mfup", mfup.middleware)` + `mfup.attach(server)` |
| Data on host | `./uploads/<uid>/<scope>/` | `./uploads-node/<uid>/<scope>/` |

Run (from the repo root):

```bash
docker compose up -d server-node client-node
# → http://localhost:20062
```

Or natively (server needs **Node ≥ 22.5** — `node:sqlite`):

```bash
# Terminal 1 — server (:8091; data lands in ./data/<uid>/<scope>/,
# override with DEMO_DATA_DIR; set REDIS_URL to switch to the Redis store)
cd examples/multiuser-scopes-node/server
npm install
node server.mjs

# Terminal 2 — the shared client from the Python edition
cd examples/multiuser-scopes/client
npm install
EXAMPLE_BACKEND_URL=http://localhost:8091 npm run dev -- --port 20062
```

Then poke the interactive parts: cancel mid-upload (single click, instant),
drop the same folder twice (the server ASKs — Overwrite / Cancel — while the
transfer keeps running), and restart the server mid-upload (the client
reconnects and resumes; with the memory store the session is re-discovered
from its staging directory on disk).
