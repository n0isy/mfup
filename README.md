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

Minimal consumer wiring — backend:

```python
engine = MfupEngine(MfupConfig(base_dir=Path("/srv/uploads"), authorize=my_authorize))
app = FastAPI(lifespan=engine.lifespan)
app.include_router(engine.router, prefix="/api/uploads")
```

frontend:

```tsx
const { snapshot, pendingAsks, start } = useMfupUpload({ serverUrl: "/api/uploads" });
const { getRootProps, getInputProps } = useMfupDropzone({ onSource: start });
```

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
