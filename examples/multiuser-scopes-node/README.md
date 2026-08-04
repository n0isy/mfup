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

Or natively:

```bash
cd examples/multiuser-scopes-node/server
npm install && node server.mjs                     # :8091
cd ../../multiuser-scopes/client
EXAMPLE_BACKEND_URL=http://localhost:8091 npm run dev -- --port 20062
```
