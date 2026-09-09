# Extending MFUP/2 — the integration contract

This document is the contract for embedding the MFUP/2 upload engine into
your product. Everything here is considered stable; anything not documented
here is internal and may change.

Audience assumptions: your backend is Python, your frontend is TypeScript.

The code ships as five packages:

| Package | Registry | Contents |
|---|---|---|
| `@mfup/server` | npm | Node server: HTTP/WebSocket handlers, memory or Redis store |
| `mfup-core` | PyPI | engine: protocol, session machine, storage, publish, hook contracts |
| `mfup-fastapi` | PyPI | `MfupEngine` + `APIRouter` + standalone server |
| `@mfup/client` | npm | browser SDK (events + snapshot store, see `docs/CLIENT.md`) |
| `@mfup/react` | npm | hooks: `useMfupUpload`, `useMfupDropzone`, `useMfupSession` |

---

## 0. Two ways to run the server

**Standalone** (container / bare process) — config from `MFUP_*` env vars,
hooks as dotted paths:

```bash
MFUP_BASE_DIR=/srv/uploads MFUP_AUTHORIZE=myapp.uploads:authorize \
  python -m mfup_fastapi          # or: uvicorn mfup_fastapi.app:app
```

**Embedded** into your own FastAPI app — config as a dataclass, hooks as
plain callables, mounted under any prefix:

```python
from fastapi import FastAPI
from mfup_fastapi import MfupConfig, MfupEngine

engine = MfupEngine(MfupConfig(
    base_dir=Path("/srv/uploads"),
    redis_url="redis://localhost:6379/0",
    authorize=my_authorize,              # callable OR "pkg.mod:func"
    on_committed=my_on_committed,
))
app = FastAPI(lifespan=engine.lifespan)  # or call engine.startup()/shutdown()
app.include_router(engine.router, prefix="/api/uploads")
```

The router keeps the protocol's fixed `/mfup/*` namespace (plus `/health`)
under your prefix. Point the browser SDK at the same prefix —
`new MfupSession({ serverUrl: "https://host/api/uploads" })` — and every
path lines up. There is no module-level state: two engines in one process
are two independent instances. `MfupConfig.from_env()` is the exact env
mapping the standalone server uses (§6).

---

## 1. The security model in one paragraph

A session is created by an authenticated `HELLO` on the control WebSocket.
The server **issues** a `resume_token` in `HELLO_OK`; that token is the
bearer credential for everything that follows — data POSTs, the transport
probe, `publish`, and future `RESUME`s. The client never invents credentials.
Whether the `HELLO` itself is allowed — and under what constraints — is
**your** decision, expressed through the authorize hook below. Without the
hook the server runs allow-all and says so loudly in the log; never ship
that.

---

## 2. The authorize hook (`MFUP_AUTHORIZE`)

Pass a callable in `MfupConfig(authorize=...)`, or point `MFUP_AUTHORIZE`
at it with a dotted path:

```bash
MFUP_AUTHORIZE="myapp.uploads:authorize"     # package.module:callable
```

The module must be importable by the server process (install it, mount it
into the container, or extend `PYTHONPATH`). A path that fails to import
**kills startup** — deliberately: a typo must not silently disable auth.

### Signature

```python
from mfup_core import AuthRequest, AuthResult

async def authorize(req: AuthRequest) -> AuthResult | None:
    ...
```

Called once per `HELLO`, before any session state is created.

### `AuthRequest` (what you get)

| Field | Type | Meaning |
|---|---|---|
| `session_id` | `str` | Client-proposed session id (UUID by default). 1–128 ASCII letters, digits, `.`, `_`, `-`; starts with a letter or digit. Invalid IDs receive `bad_session_id` before hooks or staging |
| `target_dir` | `str` | Client-requested target, relative to `MFUP_BASE_DIR` |
| `headers` | `Mapping[str, str]` | HTTP headers of the WS handshake — cookies, `Authorization`, etc. |
| `client` | `str` | `"ip:port"` of the peer (as seen by the ASGI server) |
| `query` | `Mapping[str, str]` | Query params of the WS URL |
| `meta` | `Any` | **Consumer-attached session object** (`MfupSessionConfig.meta` → `HELLO.meta`): arbitrary JSON your frontend put on the session — upload scope, album id, purpose. Untrusted until your hook validates it. Size-capped (`MFUP_MAX_META_BYTES`, 16 KiB). Persisted with the session, so it survives restarts and reaches the publish-time map hook |

Typical flow: read your session cookie / bearer token from `req.headers`,
resolve the user, validate `req.meta` (it names *what* is being uploaded —
"avatars", `{"album_id": 123}`), decide.

### `AuthResult` (what you return)

| Field | Type | Effect |
|---|---|---|
| *(return `None`)* | — | **Deny.** Client gets `SESSION_ABORT{code: "auth_failed"}` |
| `max_total_bytes` | `int \| None` | Byte quota; exceeding → `SESSION_ABORT{code: "quota_exceeded"}`, staging reclaimed |
| `max_files` | `int \| None` | File-count quota; same abort semantics |
| `base_dir` | `str \| None` | **Per-session base directory** (absolute), e.g. the user's home. Replaces `MFUP_BASE_DIR` for this session: the staging dir is created *inside* it (publish stays a same-filesystem rename even when homes are separate mounts), relative targets resolve against it, containment confines the session to it. Created if missing. A relative path is a config error → deny |
| `target_dir` | `str \| None` | Replace **or map** the client's request — the hook receives the client value in `req.target_dir` and may rewrite it. Contained within the session's base dir regardless |
| `context` | `dict` | Your correlation bag (user id, org id, …). JSON-serializable context is persisted in the session journal for recovery; never sent to the client |

Raising an exception from the hook is treated as a deny (logged with
traceback server-side, generic reason to the client).

### Example: per-user homes + mapping the client's path

```python
# myapp/uploads.py
from mfup_core import AuthRequest, AuthResult
from myapp.auth import resolve_user  # your code

async def authorize(req: AuthRequest) -> AuthResult | None:
    user = await resolve_user(req.headers.get("cookie", ""))
    if user is None or not user.can_upload:
        return None
    return AuthResult(
        # Every user gets their own base — staging and targets never leave it.
        base_dir=f"/data/homes/{user.id}",
        # MAP the client's request under a fixed prefix instead of trusting
        # it verbatim. Escapes are impossible either way: the server refuses
        # any resolved target outside base_dir (bad_target_dir) — a client
        # sending "../bob" is rejected, not silently corrected.
        target_dir=f"incoming/{req.target_dir}",
        max_total_bytes=user.quota_remaining_bytes,
        max_files=50_000,
        context={"user_id": user.id},
    )
```

Notes on per-session `base_dir`:

- The sweeper and lazy-resume work unchanged — Redis meta stores the
  **absolute** staging path, and recovery derives the base from the staging
  dir's parent.
- The filesystem-orphan reconciliation net (`MFUP_RECONCILE_EVERY`) scans
  only the global `MFUP_BASE_DIR`. Orphans inside per-user homes are still
  cleaned by the Redis-driven sweeper (the primary mechanism); they are only
  unreachable in the rare "Redis lost the entry AND cleanup was interrupted"
  double-failure. If that matters to you, keep homes under one parent and
  point a periodic job at `<homes>/*/.incoming.*` older than a day.

Quota semantics: both quotas are enforced server-side during transfer
(counters are re-seeded from durable state on every reconnect, so resume
does not double-count). Violation is **terminal** — predictable for the
user, nothing partial is ever published, staging is deleted.

---

## 2b. Per-file layout: the map_file hook (`MFUP_MAP_FILE`)

The client uploads *its* directory tree; your product may want a different
final layout — by type, by scope, sharded. The map hook owns the layout:

```bash
MFUP_MAP_FILE="myapp.uploads:map_file"
```

```python
from mfup_core import FileMapRequest

async def map_file(req: FileMapRequest) -> str | None:
    scope = req.context.get("scope", "misc")        # from your authorize hook
    kind = "img" if req.name.endswith((".jpg", ".png")) else "other"
    return f"{scope}/{kind}/{req.name}"             # relative to target_dir
    # return None  → keep the client's path for this file
```

`FileMapRequest`: `session_id`, `path` (client-relative, `/`-separated),
`name`, `size` (on-disk bytes), `target_dir`, `meta` (client session object),
`context` (your `AuthResult.context`). Both `meta` and the JSON-serializable
part of `context` are persisted with the session, so mapping works even if
the server restarted between commit and publish.

Semantics — deliberately publish-time, not transfer-time:

- **Transfer/resume are untouched.** Staging remains a verbatim mirror of the
  client tree; the hook runs once per file at publish, so it need not be
  deterministic across transfer retries, and it may be slow-ish (a DB lookup
  per file is fine).
- **The whole plan is validated before anything moves**: an escaping path
  (`..`, absolute, `\`) or two files mapped to one destination fail the
  publish with `409 {"error": "mapping_error"}` and leave staging intact —
  a consumer-hook bug can't half-publish a session.
- Conflicts with existing files follow the usual rule: without an action →
  `409 conflict_files`; with `merge_overwrite` → `os.replace`.
- With a map hook configured the **ingest-time conflict ASK is disabled**
  (it checks the client layout, which no longer predicts final paths);
  conflicts surface at publish instead.
- Mapped publish materializes **files**; empty client directories are not
  preserved.

---

## 3. Publish is your call

The engine separates *committed* (all bytes verified, staged) from
*published* (moved into the target directory). `POST
/mfup/sessions/{id}/publish` requires the session's `X-MFUP-Token`, and the
demo triggers it from the browser — but in a product you will usually want
the **backend** to decide (virus scan, moderation, billing) before
publishing. Two supported patterns:

1. **Client-driven** (demo style): frontend calls `session.publish()` after
   the `committed` event (the SDK sends the token and types the errors —
   `PUBLISH_CONFLICT` carries the conflicting file list). Simplest; fine
   when commit itself is the only gate.
2. **Backend-driven**: your backend validates whatever it wants and calls
   `engine.publish(session_id)` (typed errors: `SessionNotFound`,
   `NotCommitted`, `ConflictError`, `MappingError`). Pair it with
   `AuthResult.context` to know whose session it is. The staging dir stays
   resumable/sweepable until you do.
3. **`on_committed` hook** (`MFUP_ON_COMMITTED` / `MfupConfig(on_committed=...)`):

   ```python
   from mfup_core import CommitEvent

   async def on_committed(ev: CommitEvent) -> str | None:
       # ev: session_id, target_dir, base_dir, staging_dir, files, bytes,
       #     meta (untrusted client JSON), context (from authorize)
       if await scan_ok(ev.staging_dir):
           return "publish"        # server publishes immediately
       return None                 # leave it staged; decide later
   ```

   Returning `"publish"` publishes server-side right after `COMMIT_OK`; the
   browser's own `publish()` call, if any, then finds the session gone
   (404) — the SDK currently throws `PUBLISH_FAILED`. Set React
   `autoPublish: false` when the backend owns publication; a 404 alone does
   not distinguish publication from expiry. Raising is logged and
   treated as `None`: a broken consumer hook never strands a committed
   session.

---

## 4. Deployment topologies

| Topology | Supported | Notes |
|---|---|---|
| **1 worker** (uvicorn single process) | ✅ default | Everything in this repo assumes it; compose file ships it |
| **N workers / replicas + sticky routing** | ✅ | Hash on `session_id` (it is in every data/probe/publish URL path and in the WS `HELLO`/`RESUME` payload). Ownership follows the control WebSocket; lazy-resume (below) covers failover |
| N workers, round-robin, no stickiness | ❌ | Control messages (FILE_ACK/ASK/…) travel over the WS held by one process; a data POST landing elsewhere is rejected (`409 stale leg/epoch`). Do not run this |

**Lazy-resume** (built in): a `RESUME` arriving at a worker that has never
seen the session recovers it from the Redis index + on-disk SQLite and takes
ownership. This is what makes deploys/restarts/failovers safe: the client
reconnects anywhere, the session follows the WebSocket.

Redis is required (session expiry index + cross-worker discovery). Staging
lives on a filesystem shared by all workers (`MFUP_BASE_DIR`).

---

## 5. Protocol versioning policy

- The wire version (`MFUP/2`) is independent of any package version. It only
  changes on **incompatible** changes to the binary framing or handshake.
- Servers reject a `HELLO` whose `v` they do not serve (`bad_version`).
- **Unknown JSON fields must be ignored** by both sides. All additive
  evolution (new optional fields on control messages) happens without a
  version bump — e.g. `resume_token` in `HELLO_OK`, `nodes_expected` in
  `COMMIT_RETRY` were added this way.
- New binary frame *tags* are a version bump. New NACK/abort *reason strings*
  are not — treat unknown reasons as their closest generic (`server_policy`
  / a fatal abort).

Until the first public release the protocol may still change without a
version bump. After it, `MFUP/2` is frozen and changes go to `MFUP/3`.

---

## 6. Server configuration reference

Every env var maps 1:1 to a `MfupConfig` field (`MfupConfig.from_env()`).
The standalone server reads them once at start; embedded engines take the
dataclass. Restart to apply either way.

| Env var | Default | Purpose |
|---|---|---|
| `MFUP_BASE_DIR` | `/tmp/mfup-uploads` | Root for staging dirs and relative targets |
| `MFUP_AUTHORIZE` | *(unset = allow-all + warning)* | Dotted path of the authorize hook |
| `MFUP_MAP_FILE` | *(unset = keep client layout)* | Dotted path of the per-file mapping hook (§2b) |
| `MFUP_ON_COMMITTED` | *(unset = client-driven publish)* | Dotted path of the on_committed hook (§3) |
| `MFUP_MAX_META_BYTES` | `16384` | Size cap for `HELLO.meta` JSON |
| `MFUP_ADMIN_TOKEN` | *(unset = admin routes disabled)* | Bearer for `/mfup/sessions*`, `/mfup/sweep` |
| `REDIS_URL` | `redis://redis:6379/0` | Session index |
| `MFUP_SESSION_RESUME_TTL` | `3600` | Seconds a detached session stays resumable |
| `MFUP_LEG_IDLE_TIMEOUT` | `60` | Seconds of frame silence before a leg detaches |
| `MFUP_MAX_CHUNK_BYTES` | `262144` | Advertised **and enforced** chunk cap |
| `MFUP_MAX_BUFFERED_BODY` | `16777216` | Atomic-batch POST body cap |
| `MFUP_SWEEP_INTERVAL` | `300` | Sweeper period, seconds |
| `MFUP_RECONCILE_EVERY` | `4` | Filesystem orphan scan every Nth sweep |
| `MFUP_ORPHAN_GRACE` | `600` | Min age before an unreferenced staging dir is reclaimed |
| `MFUP_STAGING_PREFIX` | `.incoming` | Staging dir name prefix |

Client (`MfupSession` config): `serverUrl`, `targetDir`, `meta` (the session
object delivered to your hooks), `chunkSize`, `maxReconnectAttempts`,
`reconnectDelayMs`; for resume — `sessionId`, `resumeToken` (the
server-issued one, from `getResumeState()`), `lastKnownEpoch`.

---

## 7. What v0 deliberately does NOT promise

So integrators size their expectations correctly:

- **No S3/GCS storage.** Payload lands on a POSIX filesystem; publish is a
  same-filesystem `rename`. Object-storage backends need a storage
  abstraction that does not exist yet.
- **No round-robin multi-worker** without sticky routing (see §4).
- **No `fsync` before `COMMIT_OK`** — "committed" means verified and in the
  OS page cache, not power-loss-durable.
- **No cross-language protocol kit.** `protocol.py` / `protocol.ts` are
  hand-mirrored; shared test vectors are planned before any third
  implementation.
- **No `on_published` hook** — `on_committed` (§3) plus `engine.publish()`
  cover the shipped orchestration surface; a post-publish notification is
  not promised yet.
- **Packages are published to PyPI/npm and built and pack-smoked in CI.**
  Version tags trigger `.github/workflows/release.yml`; registry setup and
  first-publication details are documented in that workflow.
