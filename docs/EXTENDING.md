# Extending MFUP/2 — the integration contract

This document is the contract for embedding the MFUP/2 upload engine into
your product **without** waiting for it to become a packaged library. You run
the server as-is (container or `python -m uvicorn mfup.app:app`) and plug
your code in through configuration. Everything here is considered stable;
anything not documented here is internal and may change.

Audience assumptions: your backend is Python, your frontend is TypeScript.

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

Point `MFUP_AUTHORIZE` at an async callable using a dotted path:

```bash
MFUP_AUTHORIZE="myapp.uploads:authorize"     # package.module:callable
```

The module must be importable by the server process (install it, mount it
into the container, or extend `PYTHONPATH`). A path that fails to import
**kills startup** — deliberately: a typo must not silently disable auth.

### Signature

```python
from mfup.hooks import AuthRequest, AuthResult

async def authorize(req: AuthRequest) -> AuthResult | None:
    ...
```

Called once per `HELLO`, before any session state is created.

### `AuthRequest` (what you get)

| Field | Type | Meaning |
|---|---|---|
| `session_id` | `str` | Client-proposed session id (UUID) |
| `target_dir` | `str` | Client-requested target, relative to `MFUP_BASE_DIR` |
| `headers` | `Mapping[str, str]` | HTTP headers of the WS handshake — cookies, `Authorization`, etc. |
| `client` | `str` | `"ip:port"` of the peer (as seen by the ASGI server) |
| `query` | `Mapping[str, str]` | Query params of the WS URL |

Typical flow: read your session cookie / bearer token from `req.headers`,
resolve the user, decide.

### `AuthResult` (what you return)

| Field | Type | Effect |
|---|---|---|
| *(return `None`)* | — | **Deny.** Client gets `SESSION_ABORT{code: "auth_failed"}` |
| `max_total_bytes` | `int \| None` | Byte quota; exceeding → `SESSION_ABORT{code: "quota_exceeded"}`, staging reclaimed |
| `max_files` | `int \| None` | File-count quota; same abort semantics |
| `target_dir` | `str \| None` | Override the client's target (e.g. force `users/{id}/incoming`); still contained within `MFUP_BASE_DIR` |
| `context` | `dict` | Your correlation bag (user id, org id, …). In-memory only; never persisted, never sent to the client |

Raising an exception from the hook is treated as a deny (logged with
traceback server-side, generic reason to the client).

### Example

```python
# myapp/uploads.py
from mfup.hooks import AuthRequest, AuthResult
from myapp.auth import resolve_user  # your code

async def authorize(req: AuthRequest) -> AuthResult | None:
    user = await resolve_user(req.headers.get("cookie", ""))
    if user is None or not user.can_upload:
        return None
    return AuthResult(
        target_dir=f"users/{user.id}/incoming",
        max_total_bytes=user.quota_remaining_bytes,
        max_files=50_000,
        context={"user_id": user.id},
    )
```

Quota semantics: both quotas are enforced server-side during transfer
(counters are re-seeded from durable state on every reconnect, so resume
does not double-count). Violation is **terminal** — predictable for the
user, nothing partial is ever published, staging is deleted.

---

## 3. Publish is your call

The engine separates *committed* (all bytes verified, staged) from
*published* (moved into the target directory). `POST
/mfup/sessions/{id}/publish` requires the session's `X-MFUP-Token`, and the
demo triggers it from the browser — but in a product you will usually want
the **backend** to decide (virus scan, moderation, billing) before
publishing. Two supported patterns:

1. **Client-driven** (demo style): frontend calls `publish` with
   `session.token` after the `committed` event. Simplest; fine when commit
   itself is the only gate.
2. **Backend-driven**: your frontend tells *your* backend "session X
   committed"; your backend validates whatever it wants and calls `publish`
   itself (it can read the token from its own records — pair it with
   `AuthResult.context` to know whose session it is). The staging dir stays
   resumable/sweepable until you do.

A server-side `on_committed` hook (config-driven, like authorize) is the
planned third pattern; not shipped yet — see §7.

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

All read at process start (import time). Restart to apply.

| Env var | Default | Purpose |
|---|---|---|
| `MFUP_BASE_DIR` | `/tmp/mfup-uploads` | Root for staging dirs and relative targets |
| `MFUP_AUTHORIZE` | *(unset = allow-all + warning)* | Dotted path of the authorize hook |
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

Client (`MfupSession` config): `serverUrl`, `targetDir`, `chunkSize`,
`maxReconnectAttempts`, `reconnectDelayMs`; for resume — `sessionId`,
`resumeToken` (the server-issued one, from `getResumeState()`),
`lastKnownEpoch`.

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
- **No server-side `on_committed` / `on_published` hooks yet** — publish
  orchestration is yours (§3).
- **Package publication** (PyPI/npm) is a separate step; today you embed by
  running this server and bundling `client/src` (the demo consumes it via a
  build-time alias).
