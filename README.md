# MFUP/2 — resumable multi-file upload

[![e2e](https://github.com/n0isy/mfup/actions/workflows/e2e.yml/badge.svg)](https://github.com/n0isy/mfup/actions/workflows/e2e.yml)
[![release](https://github.com/n0isy/mfup/actions/workflows/release.yml/badge.svg)](https://github.com/n0isy/mfup/actions/workflows/release.yml)
[![npm @mfup/client](https://img.shields.io/npm/v/%40mfup%2Fclient?label=%40mfup%2Fclient&color=cb3837)](https://www.npmjs.com/package/@mfup/client)
[![npm @mfup/react](https://img.shields.io/npm/v/%40mfup%2Freact?label=%40mfup%2Freact&color=cb3837)](https://www.npmjs.com/package/@mfup/react)
[![npm @mfup/server](https://img.shields.io/npm/v/%40mfup%2Fserver?label=%40mfup%2Fserver&color=cb3837)](https://www.npmjs.com/package/@mfup/server)
[![PyPI mfup-core](https://img.shields.io/pypi/v/mfup-core?label=mfup-core&color=3775a9)](https://pypi.org/project/mfup-core/)
[![PyPI mfup-fastapi](https://img.shields.io/pypi/v/mfup-fastapi?label=mfup-fastapi&color=3775a9)](https://pypi.org/project/mfup-fastapi/)

Move whole directory trees (`node_modules`-scale: tens of thousands of small
files) from a browser to a server — resumable, interactive, atomic.

- **One WebSocket control channel** (JSON) + **HTTP data legs** (binary
  frames, CRC-32C per chunk). Streaming (`duplex:"half"`) where the browser
  supports it, atomic batched POSTs everywhere else — detected by a live probe.
- **Resume** across page reloads, network drops, server restarts and worker
  failovers (per-session SQLite journal, epoch/leg fencing, session-store
  recovery — Redis for multi-worker, or the Node server's default in-memory
  store with a filesystem rescan on restart).
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
| [`packages/client`](packages/client) | npm [`@mfup/client`](https://www.npmjs.com/package/@mfup/client) | Browser SDK: session, ingestion (DnD/pickers), events + snapshot store |
| [`packages/react`](packages/react) | npm [`@mfup/react`](https://www.npmjs.com/package/@mfup/react) | React hooks: `useMfupUpload`, `useMfupDropzone`, `useMfupSession` |
| [`packages/server`](packages/server) | npm [`@mfup/server`](https://www.npmjs.com/package/@mfup/server) | **Node server**: universal `(req,res)` + upgrade handler for express/vite/raw http; memory or Redis store; zero native deps |
| [`server/mfup-core`](server/mfup-core) | PyPI [`mfup-core`](https://pypi.org/project/mfup-core/) | Engine: protocol, session state machine, storage, publish, hooks |
| [`server/mfup-fastapi`](server/mfup-fastapi) | PyPI [`mfup-fastapi`](https://pypi.org/project/mfup-fastapi/) | `MfupEngine` + `APIRouter` to mount into your FastAPI, or standalone server |

Both servers speak the same wire protocol, share the on-disk session-journal
format and (in Redis mode) the same index keys — the CI e2e matrix runs the
full chaos suite against each. Pick FastAPI or Node by stack, not by feature.

Around the packages:

- [`examples/`](examples) — **consumer-grade examples** built strictly on the
  published packages (multiuser + scopes pattern), plus the validation
  record of the guide below;
- [`demo/`](demo) — the internal dev/test playground (e2e harness pages);
  not an integration reference;
- [`benchmarks/`](benchmarks) — the naive POST-per-file baseline and the
  race methodology.

## Quick start

```bash
docker compose up -d          # the CONSUMER example (multiuser scopes)
# app:  http://localhost:20061   — uploads land in ./uploads/<user_id>/<scope>/
```

The main compose app is `examples/multiuser-scopes` running the way an
outside integrator would (deps from PyPI/npm, not from this repo). The
internal protocol dev/test stack (e2e harness, chaos tests, benchmark
baseline) is separate:

```bash
docker compose -f e2e/docker-compose.yaml up -d    # :20060
```

## Integration guide — standalone, step by step

The full walkthrough for wiring MFUP into your product as a separate upload
service. (The alternative — embedding `engine.router` into your own FastAPI
app — is one `include_router` call; see `docs/EXTENDING.md` §0.)

This guide uses the Python server. **On a Node stack** the same protocol
ships as [`@mfup/server`](packages/server) — one universal handler pair for
express/vite/raw `node:http` (`mfup.middleware` + `mfup.attach(server)`),
a memory session store by default (no Redis needed for a single process),
and `npx mfup-server` for the standalone role with the same `MFUP_*` env
vars. See [packages/server/README.md](packages/server/README.md).

### 1. Install the server

```bash
pip install mfup-fastapi            # pulls mfup-core; needs Python 3.10+
# not on PyPI yet? straight from the repo:
pip install "mfup-core @ git+https://github.com/n0isy/mfup#subdirectory=server/mfup-core" \
            "mfup-fastapi @ git+https://github.com/n0isy/mfup#subdirectory=server/mfup-fastapi"
```

Requirements: **Redis** (the Python server's session expiry index; the Node
server defaults to an in-memory store and takes Redis as an opt-in) and a
**POSIX filesystem** for `MFUP_BASE_DIR` — publish is a same-filesystem
`rename`, so the base dir and your target dirs must live on one mount
(per-user homes from the authorize hook each stage inside themselves, so
separate mounts per user are fine).

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
# (the Node server reports "js-table" — that is its normal fast path)
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
- [ ] `/health` says `"crc32c":"native"` (Python) / `"js-table"` (Node)
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

- `server/tests` — pytest: protocol vectors, path validation, quotas,
  recovery, publication, control-channel ownership and hooks.
- `packages/server/test` — vitest: byte-level codec pinned
  against the client encoders, shared cases with the Python suite, and
  wire-level integration over real http + WebSocket (incl. RESUME).
- `e2e/` — Playwright suites on chromium/firefox/webkit, incl. chaos tests
  (backend killed mid-transfer, byte-exact disk verification).
- CI: Linux full-stack in docker compose as a **python | node backend
  matrix** (the same suite proves wire compatibility of both servers),
  native WebKit on macOS.

## License

MIT
