# MFUP/2 — resumable multi-file upload

Move whole directory trees (`node_modules`-scale: tens of thousands of small
files) from a browser to a server — resumable, interactive, atomic.

- **One WebSocket control channel** (JSON) + **HTTP data legs** (binary
  frames, CRC-32C per chunk). Streaming (`duplex:"half"`) where the browser
  supports it, atomic batched POSTs everywhere else — detected by a live probe.
- **Resume** across page reloads, network drops, server restarts and worker
  failovers (per-session SQLite journal, epoch/leg fencing, lazy recovery
  from a Redis index).
- **Interactive transfers**: the server ASKs the user mid-flight (overwrite?
  cancel?) while the bytes keep flowing — the non-blocking control channel is
  the core of the design.
- **Atomic publish**: staged files `rename()` into the target directory;
  a consumer hook can re-lay-out every file (by type, scope, user).
- **Consumer control contract**: who (authorize by headers) × what (session
  meta) × where (per-user `base_dir`, target mapping) × how much (quotas) ×
  which layout (`map_file`) × what happens after (`on_committed`).

## Packages

| Package | Registry | What |
|---|---|---|
| [`packages/client`](packages/client) | npm `@mfup/client` | Browser SDK: session, ingestion (DnD/pickers), events + snapshot store |
| [`packages/react`](packages/react) | npm `@mfup/react` | React hooks: `useMfupUpload`, `useMfupDropzone`, `useMfupSession` |
| [`server/mfup-core`](server/mfup-core) | PyPI `mfup-core` | Engine: protocol, session state machine, storage, publish, hooks |
| [`server/mfup-fastapi`](server/mfup-fastapi) | PyPI `mfup-fastapi` | `MfupEngine` + `APIRouter` to mount into your FastAPI, or standalone server |

## Quick start (dev stack)

```bash
docker compose up -d          # redis + backend + demo build + caddy on :20060
# demo:        http://localhost:20060/
# react demo:  http://localhost:20060/react.html
```

## Integration guide — standalone, step by step

The full walkthrough for wiring MFUP into your product as a separate upload
service. (The alternative — embedding `engine.router` into your own FastAPI
app — is one `include_router` call; see `docs/EXTENDING.md` §0.)

### 1. Install the server

```bash
pip install mfup-fastapi            # pulls mfup-core; needs Python 3.10+
# not on PyPI yet? straight from the repo:
pip install "mfup-core @ git+https://github.com/n0isy/mfup#subdirectory=server/mfup-core" \
            "mfup-fastapi @ git+https://github.com/n0isy/mfup#subdirectory=server/mfup-fastapi"
```

Requirements: **Redis** (session expiry index) and a **POSIX filesystem**
for `MFUP_BASE_DIR` — publish is a same-filesystem `rename`, so the base dir
and your target dirs must live on one mount (per-user homes from the
authorize hook each stage inside themselves, so separate mounts per user
are fine).

### 2. Write your authorize hook (do not skip)

Without it the server runs allow-all and warns loudly. One async function:

```python
# myapp/uploads.py
from mfup_core import AuthRequest, AuthResult

async def authorize(req: AuthRequest) -> AuthResult | None:
    user = await session_from_cookies(req.headers)   # your auth
    if user is None:
        return None                                  # deny → auth_failed
    return AuthResult(
        base_dir=f"/srv/homes/{user.id}",            # per-user home
        max_total_bytes=10 * 2**30,
        max_files=200_000,
        context={"user_id": user.id},                # yours, in later hooks
    )
```

Optional, same pattern: `map_file` (final per-file layout at publish) and
`on_committed` (server-side publish decision) — contracts in
`docs/EXTENDING.md` §2b/§3.

### 3. Run it

```bash
MFUP_BASE_DIR=/srv/uploads \
REDIS_URL=redis://localhost:6379/0 \
MFUP_AUTHORIZE=myapp.uploads:authorize \
MFUP_ADMIN_TOKEN=$(openssl rand -hex 24) \
python -m mfup_fastapi              # listens on :8070 (MFUP_HOST/MFUP_PORT)
```

`myapp` just has to be importable (installed or on `PYTHONPATH`); a broken
hook path kills startup by design. Full env reference: `docs/EXTENDING.md` §6.

Verify:

```bash
curl -s localhost:8070/health
# {"status":"ok","protocol":"MFUP/2","crc32c":"native"}   ← "native" matters
```

### 4. Put it behind your reverse proxy

Route one prefix to the service; three things matter:

- **WebSocket upgrade** on `<prefix>/mfup/control`;
- **request buffering OFF** for `<prefix>/mfup/data/` (nginx:
  `proxy_request_buffering off`) — otherwise the streaming probe fails and
  clients silently fall back to batched POSTs (correct, just slower);
- **body size** ≥ `MFUP_MAX_BUFFERED_BODY` (default 16 MiB) for the batch path.

```nginx
location /api/uploads/ {
    proxy_pass http://127.0.0.1:8070/;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;      # WS
    proxy_set_header Connection "upgrade";
    proxy_request_buffering off;                  # streaming data leg
    client_max_body_size 20m;                     # batch data leg
    proxy_read_timeout 3600s;                     # long-lived control WS
}
```

### 5. Wire the frontend

```bash
npm install @mfup/client            # + @mfup/react react — if you use React
```

React (full UX in one hook pair — progress, conflict dialogs, auto-publish):

```tsx
const { snapshot, pendingAsks, busy, start, abort } = useMfupUpload({
  serverUrl: "/api/uploads",        // your proxy prefix from step 4
  meta: { scope: "attachments" },   // arrives in your authorize hook
});
const { isDragActive, getRootProps, getInputProps } = useMfupDropzone({
  disabled: busy, onSource: start,
});
```

Vanilla TS: `MfupSession` + `sourceFromDataTransfer` — the imperative
five-liner and the whole event/snapshot reference are in `docs/CLIENT.md`.

### 6. Smoke-test the loop

Drop a folder → watch `FILE_ACK`s stream → `committed` → files appear
atomically under the target dir. Drop the same folder again → the server
ASKs mid-transfer → answer Overwrite/Cancel — the upload never pauses.
Kill the server mid-upload and restart it → the client reconnects and
resumes from the accepted offsets.

### 7. Production checklist

- [ ] `MFUP_AUTHORIZE` set (no allow-all warning in the log)
- [ ] `/health` says `"crc32c":"native"`
- [ ] `MFUP_ADMIN_TOKEN` set (or admin routes stay disabled — also fine)
- [ ] quotas set in `AuthResult` (`max_total_bytes` / `max_files`)
- [ ] one worker per engine, or replicas with **sticky routing** on
      `session_id` (lazy-resume covers failover; round-robin is unsupported)
- [ ] retention untouched defaults are sane: TTL 1 h, sweeper 5 min,
      orphan reconciliation as the safety net

## Docs

- [`docs/EXTENDING.md`](docs/EXTENDING.md) — the integration contract
  (hooks, security model, topologies, versioning policy).
- [`docs/FULL.md`](docs/FULL.md) — full protocol & architecture reference.

## Testing

- `server/tests` — 49 unit tests (protocol vectors, edge cases, hooks).
- `e2e/` — Playwright suites on chromium/firefox/webkit, incl. chaos tests
  (backend killed mid-transfer, byte-exact disk verification).
- CI: Linux full-stack in docker compose, native WebKit on macOS.

## License

MIT
