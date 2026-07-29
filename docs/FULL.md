# MFUP/2 — Multi-File Upload Protocol

**Packages:** `mfup-server` (Python), `@mfup/client` (TypeScript)
**Languages:** Python 3.10+ · TypeScript 5.5 (ES2022, browser-only)
**Frameworks:** FastAPI + uvicorn (server) · zero runtime dependencies (client) · Vite 6 (demo)
**Protocol version:** `MFUP/2`
**Stores:** SQLite (one DB per session) · Redis (session expiry index) · POSIX filesystem

---

## 1. Architecture

### 1.1 The problem

MFUP/2 exists to make a browser upload an entire directory tree — hundreds of files — without paying one HTTP round-trip per file. The project's design note (`docs/MFUP_RU.md`) records the motivating measurement: a 188-file, 1.0 MB project folder took **17.2 seconds** over conventional `multipart/form-data` POST-per-file, of which roughly 13 seconds was pure round-trip latency and ~2.9 s was `FormData` construction. Actual data transfer was ~1.3 s. The bottleneck was the protocol, not the bytes.

MFUP/2 collapses that to **two HTTP requests and one WebSocket** for the whole tree: a single streaming `POST` carrying every file body, a control WebSocket for metadata and acknowledgements, and a final `POST` to publish. The repository ships a side-by-side benchmark page (§5.2) that races the two approaches in the same browser.

### 1.2 Two channels, one session

A session is a triple of identifiers — `session_id`, `resume_token`, and a per-connection `leg_id` — shared across two cooperating transports:

```
                    ┌─────────────────────────────────────────────┐
   browser          │                                   server    │
                    │                                             │
  MfupSession       │   WebSocket  /mfup/control                  │
   ├─ ControlChannel├──────────────────────────────────────────► JSON control plane
   │                │   HELLO / RESUME / ACTION / CLIENT_ABORT    │   ├─ handshake
   │                │ ◄──────────────────────────────────────────┤   ├─ FILE_ACK / NACK
   │                │   HELLO_OK / FILE_ACK / ASK / COMMIT_OK     │   ├─ ASK (conflicts)
   │                │                                             │   └─ COMMIT_OK / RETRY
   ├─ DataChannel   │   POST /mfup/data/{session_id}/{leg_id}     │
   │                ├──────────────────────────────────────────► binary frame stream
   │                │   [u32 len][u8 tag][payload] × N            │   └─ writes to staging
   │                │                                             │
   └─ ProgressTracker   POST /mfup/sessions/{id}/publish          │
                    ├──────────────────────────────────────────► os.rename → target_dir
                    └─────────────────────────────────────────────┘
```

**There is no WebRTC anywhere in this codebase.** The term "data channel" refers to the HTTP `POST` data plane (`client/src/data-channel.ts`, `POST /mfup/data/...`), not `RTCDataChannel`. This was verified independently on both sides by grep for `RTCPeerConnection`, `webrtc`, `rtc`, `datachannel`, and `data_channel` — zero hits in `/workspace/client/src` and `/workspace/server`.

The split is deliberate: bulk bytes flow one-way over a transport that never waits for a reply, while every acknowledgement, error, and interactive decision travels out-of-band on the WebSocket. The uploader is therefore never blocked on a response mid-stream.

### 1.3 Interleaved scan and transfer

The client does not scan the tree and then upload it. `MfupSession.onDiscover` writes a `NODE` frame the instant a node is found and, for files, queues the body for immediate streaming. Directory metadata, file names, and file contents interleave in a single ordered byte stream:

```
NODE(1, dir, "src") → NODE(2, file, "index.ts") → FILE_OPEN(2) → FILE_CHUNK(2, …)
  → FILE_CLOSE(2) → NODE(3, file, "app.ts") → FILE_OPEN(3) → … → SESSION_END
```

Scan and pump run concurrently and throttle each other through a two-mark gate: ingestion parks when `fileQueue` reaches 10,000 entries (`SCAN_HIGH_WATER`) and resumes below 5,000 (`SCAN_LOW_WATER`). Because the browser File System APIs yield files lazily during traversal, nothing is buffered in memory ahead of the network.

### 1.4 Staging and the two-phase commit

Bytes never land in the caller's target directory during transfer. Each session owns a staging directory whose name is `{MFUP_STAGING_PREFIX}.{session_id}` (default `.incoming.<uuid>`), sited as a **sibling** of the target so that publication is a same-filesystem rename:

```
<base_dir>/
  .incoming.<session_id>/
    state.sqlite            ← all durable session state (WAL mode)
    payload/                ← mirrors the client's tree
      src/index.ts
  <target_dir>/             ← untouched until publish
```

Three phases separate "received" from "visible":

| Phase | Trigger | What it guarantees |
|---|---|---|
| **COMMITTING** | client sends `SESSION_END` | All bodies written to `payload/`; per-chunk CRC-32C verified on arrival |
| **COMMITTED** | `try_commit()` finds no incomplete files → `COMMIT_OK` | Every file's `accepted_offset == final_size`; nothing is missing |
| **Published** | explicit `POST /mfup/sessions/{id}/publish` | `os.rename` moves top-level entries into `target_dir` |

Commit is a *state flip*, not a filesystem operation — the pre-commit invariant (`get_incomplete_files()`) is what makes `COMMIT_OK` meaningful. If any file is short, the server sends `COMMIT_RETRY` with the incomplete list and reverts to `ACTIVE` instead of committing, so a truncated upload can never be published as complete. Publish itself is atomic per top-level entry; see §7 for the limits of that guarantee.

### 1.5 Durable state and crash recovery

**State lives in SQLite, not in memory.** `LiveSession.state` is a property that reads `SELECT state FROM sessions` on every access, and every write commits immediately. The same holds for `epoch`, `expires_at`, per-file `accepted_offset`, and the root progress summary. A process restart therefore loses no protocol state.

On startup, `lifespan()` asks Redis (not the filesystem) for the live session set, reopens each session's SQLite DB, and forces any session found in `ACTIVE`, `PAUSED_BY_SERVER`, or `COMMITTING` back to `WAITING_RESUME`. Including `COMMITTING` in that revert is essential: commit is client-initiated, so a session frozen mid-commit would otherwise be unresumable — `data_endpoint` rejects `COMMITTING` with `409` and `resume()` refuses it with `invalid_state`.

Redis holds a sorted set keyed by expiry (`mfup:sessions`) plus a metadata hash per session (`mfup:meta:{id}`). This is what lets both the sweeper and restart recovery work **without ever calling `iterdir()` or opening a SQLite file speculatively** — an explicit design goal stated in the `redis_index.py` docstring and the `SessionRegistry` class docstring.

### 1.6 Resume, legs, and epoch fencing

Every reconnect mints a new `leg_id` and increments the persisted `epoch`. Both are checked on the data path — up front and again on *every streamed chunk* — so a superseded connection whose TCP socket is still alive cannot write into the new leg's file state. Stale requests are rejected with `409 stale leg` or `409 stale_epoch`, and an in-flight body is abandoned mid-stream the moment the epoch changes.

Resume is byte-exact rather than file-exact. `RESUME_OK` reports each file's server-side `accepted_offset`; the client seeks its local `Blob` to that offset (`blobChunks(blob, offset)`) and re-streams only the remainder. On the server side, `FileWriter` opens the final payload path with `r+b`, seeks to `accepted_offset`, and **truncates** — discarding any bytes past the last acknowledged offset, so a resume can leave neither a hole nor a duplicated tail.

Note the deliberate exception: `COMMIT_RETRY` does *not* bump the epoch. The client resends on the same leg and epoch.

### 1.7 Integrity, progress, and conflicts

**Integrity** is per-chunk CRC-32C (Castagnoli, `0x82F63B78`), computed by the client and verified by the server before any write; a mismatch yields `NACK_CHUNK{reason: "bad_checksum"}` and nothing is written. A source comment in `client/src/protocol.ts` records that SHA-256 whole-file hashing was removed because it "was never verified server-side" — CRC-32C per chunk is the only integrity mechanism in the system.

**Progress** is a fixed 10/90 blend, `fraction = 0.1 × scanFrac + 0.9 × bodyFrac`, computed in `bigint` and clamped monotonically (`max(last, min(1, raw))`) so the bar starts moving on the first discovered node and never runs backwards when the scan estimate grows.

**Conflicts** are resolved without stalling the transfer. A separate three-state FSM (`clean` → `conflict_dir` → `conflict_files`) inspects where each node would land; a directory landing on a directory auto-merges silently, while a file-on-file collision sends a single `ASK` to the client. The upload keeps running while the user decides, and the answer (`merge_overwrite` or `cancel`) is applied at publish time.

### 1.8 Layering and where to look

| Layer | Server | Client | Section |
|---|---|---|---|
| Transport / routing | `app.py` — 8 routes, WS + HTTP | `control.ts`, `data-channel.ts`, `probe.ts` | §3.1, §4.4, §4.6, §4.7 |
| Wire codec | `protocol.py` (decode-only) | `protocol.ts` (encode-only) | §2, §3.3, §4.3 |
| Session state / orchestration | `session_manager.py` | `session.ts` | §3.2, §4.2 |
| Persistence | `storage.py` (SQLite + paths), `redis_index.py` | — | §3.4, §3.5 |
| Materialization | `publish.py` | — | §3.6 |
| Discovery / IO | — | `ingestion.ts`, `progress.ts`, `errors.ts` | §4.5, §4.8, §4.9 |

The codec asymmetry is worth noting: the server only ever *decodes* binary frames and the client only ever *encodes* them, because the reverse direction (server → client) is entirely JSON over the WebSocket. Neither side ships the other half.

### 1.9 Deployment shape

The whole stack runs from one `docker compose` file with **no Dockerfiles** — stock images, bind-mounted source, dependencies installed in each service's `command`. Caddy is the single published port (`20060:80`) and fronts everything on one origin, so the browser makes no cross-origin request and the stack carries no CORS configuration at all. A deliberately naive baseline server (`trivial-server/`) is deployed alongside as the benchmark control. See §6.

**The server is inherently single-process.** `SessionRegistry` is an in-memory dict; a second uvicorn worker would not see sessions created by the first (`registry.get()` → `None` → `410` on data POSTs). Redis indexes sessions for expiry but does not share the live registry.

---

## 2. Protocol Reference

This section is the shared contract. Implementation-specific details live in §3.3 (Python decoder) and §4.3 (TypeScript encoder).

### 2.1 Channel roles

| Channel | Transport | Encoding | Direction | Carries |
|---|---|---|---|---|
| Control | WebSocket `/mfup/control` | JSON, discriminated by `t` | bidirectional | Handshake, acks, conflicts, commit verdict |
| Data | HTTP `POST /mfup/data/{session_id}/{leg_id}` | Binary frames | client → server only | Tree metadata and file bodies |
| Probe | HTTP `POST /mfup/probe/{session_id}` | Raw bytes | client → server | Duplex-streaming capability check |
| Publish | HTTP `POST /mfup/sessions/{id}/publish` | JSON | client → server | Materialize staged tree |

Data and probe requests authenticate with the `X-MFUP-Token` header carrying the session's `resume_token` — the resume token doubles as the data-plane bearer token. Query parameters on the data endpoint are `?seq=<int>` (required), `?final=<0|1>`, and `?epoch=<int>`.

### 2.2 Binary frame envelope

```
[u32 length (big-endian)][u8 tag][payload …]
```

`length` covers the tag byte plus the payload (`1 + len(payload)`) and excludes itself. All multi-byte integers are big-endian. Strings are `[u16 length][UTF-8 bytes]`. Optional fields use a `u8` presence flag followed, if set, by the value.

There is **no version byte, magic number, or frame checksum in the envelope.** Version is negotiated once out of band via the `v` field of `HELLO`. Payload integrity is per-chunk only.

> The design note `docs/MFUP_RU.md` documents this envelope as `[tag:1][length:4][payload:N]`. That is incorrect — both implementations agree on length-first. Verified in `client/src/protocol.ts:290-298` (`view.setUint32(0, 1 + payloadSize); buf[4] = tag`) and `server/mfup/protocol.py:296-303` (`struct.unpack_from("!I", …, pos)` then `tag = self._buf[pos + 4]`).

### 2.3 Frame types

| Tag | Name | Payload layout |
|---|---|---|
| `0x01` | `NODE` | `u32 node_id`, `u32 parent_id`, `u8 kind`, `str name`, `u8 flag`+`[u64 size_hint]`, `u8 flag`+`[u64 mtime_ms]` |
| `0x02` | `SUMMARY` | `u32 node_id`, `u64 scan_done_units`, `u64 scan_est_units`, `u64 body_done_bytes`, `u64 body_est_bytes`, `u8 sealed` |
| `0x03` | `FILE_OPEN` | `u32 node_id`, `u64 size`, `u8 flag`+`[u64 mtime_ms]` |
| `0x04` | `FILE_CHUNK` | `u32 node_id`, `u64 offset`, `u32 length`, `u8 checksum_kind`, `u32 checksum`, `length` payload bytes |
| `0x05` | `FILE_CLOSE` | `u32 node_id`, `u64 size_sent` |
| `0x06` | `DIR_CLOSE` | `u32 node_id` |
| `0x07` | `SESSION_END` | `u64 scan_done_units`, `u64 scan_est_units`, `u64 body_done_bytes`, `u64 body_est_bytes`, `u8 sealed` |
| `0x08` | `CLIENT_ABORT` | `str code`, `str reason` |

`SESSION_END` is session-scoped and carries no `node_id`. Supporting enums: `NodeKind` (`DIR = 0x00`, `FILE = 0x01`), `ChecksumKind` (`CRC32C = 0x01`, the only member), `NodeStatus` (`open`, `closed`, `rejected`, `pruned`), and `ROOT_NODE_ID = 0`.

### 2.4 Control messages

**Client → server (4):**

| `t` | Fields |
|---|---|
| `HELLO` | `v`, `session_id`, `resume_token`, `leg_id`, `target_dir?` |
| `RESUME` | `session_id`, `resume_token`, `leg_id`, `last_known_epoch` |
| `CLIENT_ABORT` | `code`, `reason` |
| `ACTION` | `action` ∈ {`merge_overwrite`, `cancel`} |

> `target_dir` is carried on `HELLO` only. The client sends `last_known_epoch` on `RESUME`, but the server never reads it (`app.py` reads only `session_id`, `resume_token`, `leg_id`) — the server's own persisted epoch is authoritative. Harmless, but the field is decorative.

**Server → client (12):**

| `t` | Fields | Meaning |
|---|---|---|
| `HELLO_OK` | `epoch`, `expires_at`, `limits{max_chunk_bytes, max_open_files, max_pending_files}` | New session accepted |
| `RESUME_OK` | `epoch`, `expires_at`, `root_summary`, `files[]`, `pruned_nodes[]`, `rejected_files[]` | Resumed; carries per-file `accepted_offset` |
| `SESSION_ABORT` | `code`, `reason` | Handshake or session refused; WS closes |
| `FILE_ACK` | `node_id`, `accepted_offset` | Bytes durably accepted (~every 256 KiB) |
| `NACK_CHUNK` | `node_id`, `expected_offset`, `reason` | Chunk rejected; resync from `expected_offset` |
| `ASK` | — | Target-directory conflict; answer with `ACTION` |
| `PROBE_ACK` | `first_chunk_bytes` | Probe verdict input |
| `PRUNE_NODE` | `node_id`, `code`, `reason` | Stop descending this subtree |
| `REJECT_FILE` | `node_id`, `code`, `reason` | Drop this file |
| `FLOW` | `paused`, `reason` | Server-initiated backpressure |
| `COMMIT_RETRY` | `incomplete[{node_id, accepted_offset}]` | Pre-commit invariant failed; state reverted to `ACTIVE` |
| `COMMIT_OK` | `files`, `bytes` | All files complete; safe to publish |

`NACK_CHUNK.reason` ∈ {`bad_checksum`, `bad_offset`, `stale_epoch`, `server_policy`}. `FLOW.reason` ∈ {`backpressure`, `maintenance`, `storage_pressure`}. `SESSION_ABORT.code` ∈ {`bad_version`, `bad_target_dir`, `conflict`, `not_found`, `auth_failed`, `invalid_state`, `protocol_error`}.

`PRUNE_NODE`, `REJECT_FILE`, and `FLOW` are fully implemented on both sides but **no server code path ever emits them** — see §7.2.

### 2.5 Session lifecycle

Both sides model the same eight-member state vocabulary (`active`, `paused_by_server`, `waiting_resume`, `committing`, `committed`, `aborted`, `expired`, `failed`), though each reaches only a subset. The protocol-level flow:

```
      HELLO                 SESSION_END              COMMIT_OK
  ──────────────► ACTIVE ──────────────► COMMITTING ───────────► COMMITTED
                    ▲  │                      │                       │
        RESUME      │  │ disconnect /         │ COMMIT_RETRY          │ POST /publish
                    │  │ idle timeout         │ (incomplete files)    ▼
                    │  ▼                      │                  (deleted)
              WAITING_RESUME ◄────────────────┘
                    ▲          server restart
                    │
                    └── recover_session() reverts ACTIVE / PAUSED_BY_SERVER / COMMITTING

  any state ──── CLIENT_ABORT / ACTION cancel ────► ABORTED ──► staging rmtree'd immediately
```

Epoch semantics: initialised to `1` by `init_session`, incremented by every `attach_leg()`, so the **first epoch a client sees in `HELLO_OK` is 2**. Echoed back as `?epoch=` on every data POST and enforced both before and during the request body. `COMMIT_RETRY` is the one flow that does not bump it.

Sequence semantics: `?seq=` must equal `last_data_seq + 1` exactly. Duplicates and gaps both yield `409 seq_mismatch` before the body is read. `attach_leg()` and `try_commit()`'s retry branch both reset `last_data_seq = -1` and `final_seq_seen = False`.

### 2.6 Transport probing

Request streaming (`duplex: "half"`) is not universally available, so the client empirically probes it once per session against `POST /mfup/probe/{session_id}` before opening the data channel. Three gates must all pass: static `Request` feature detection, successful `fetch` construction, and a `PROBE_ACK` whose `first_chunk_bytes >= 1024` within 1500 ms.

That last gate exists for a specific browser bug: Firefox stringifies a `ReadableStream` request body to the literal `"[object ReadableStream]"` (23 bytes) rather than streaming it. Comparing the server's observed first-chunk size against what was sent detects this without corrupting a real upload.

The verdict selects between two **HTTP** data modes — never WebRTC:

| Mode | Shape | Backpressure |
|---|---|---|
| Streaming | One long `POST` per leg with `duplex: "half"`, hard-coded `?seq=0&final=1` | `ByteLengthQueuingStrategy` 4 MiB high-water mark; `drain()` parks on `desiredSize <= 0` |
| Batch | Sequential buffered `POST`s, 2 MiB flush threshold, monotonic `seq` | Promise chain guarantees never two POSTs in flight |

A probe failure is never fatal — it forces batch mode and emits a non-fatal `probeError`.

---
## 3. Server (Python)

The MFUP/2 server is a FastAPI/ASGI application that accepts resumable, chunked multi-file uploads. A session is driven by two cooperating transports: a long-lived **WebSocket control channel** carrying JSON messages, and one or more **HTTP POST "data legs"** carrying a stream of length-prefixed binary frames. Received bytes land in a per-session *staging* directory that also holds a per-session SQLite database of all durable state; a separate explicit **publish** step renames the staged payload into the caller's target directory. A Redis sorted set indexes sessions by expiry so cleanup and restart-recovery never have to walk the filesystem.

Package root: `server/mfup/`. `__init__.py` is empty (0 bytes) — a package marker only, exporting nothing.

The server has no counterpart to client-side metadata buffering; its equivalent is `LiveSession.build_resume_ok()`, which replays *persisted* state to the client on RESUME.

---

### 3.1 Application and endpoints (`app.py`)

Module-level globals `_registry: SessionRegistry`, `_session_index: SessionIndex`, and `_sweep_task: asyncio.Task` are populated by the `lifespan` async context manager and read through `get_registry()` / `get_session_index()` (both `assert` non-None).

#### Configuration

All configuration is read at **import time** from environment variables. There is no runtime reload.

| Constant | Env var | Default | Effect |
|---|---|---|---|
| `DEFAULT_BASE_DIR` | `MFUP_BASE_DIR` | `/tmp/mfup-uploads` | Root for staging dirs and relative target dirs; `mkdir(parents=True)` at startup |
| `SESSION_RESUME_TTL` | `MFUP_SESSION_RESUME_TTL` | `3600` | Seconds a detached session stays resumable |
| `LEG_IDLE_TIMEOUT` | `MFUP_LEG_IDLE_TIMEOUT` | `60` | Seconds of frame silence before the leg is force-detached |
| `MAX_CHUNK_BYTES` | `MFUP_MAX_CHUNK_BYTES` | `262144` | Advertised in `HELLO_OK.limits` only — **never enforced** |
| `MAX_OPEN_FILES` | `MFUP_MAX_OPEN_FILES` | `1` | Advertised only — **never enforced** |
| `MAX_PENDING_FILES` | `MFUP_MAX_PENDING_FILES` | `64` | Advertised only — **never enforced** |
| `SWEEP_INTERVAL` | `MFUP_SWEEP_INTERVAL` | `300` | Sweeper loop period in seconds |
| `STAGING_PREFIX` | `MFUP_STAGING_PREFIX` | `.incoming` | Staging dir name prefix |
| `REDIS_URL` | `REDIS_URL` | `redis://redis:6379/0` | Session index connection |

Verified by grep: `MAX_CHUNK_BYTES`, `MAX_OPEN_FILES`, and `MAX_PENDING_FILES` appear only at their definition and inside the `HELLO_OK` payload. No code path compares a chunk length, an open-writer count, or a pending-file count against them.

#### Lifespan

`lifespan(app)` runs three things on startup, in order:

1. Creates `DEFAULT_BASE_DIR` and constructs `SessionIndex(REDIS_URL)`.
2. **Restart recovery** (wrapped in `try/except Exception` + `logger.exception`): queries `get_not_expired()` and `get_expired()`, then for each alive session id reads `get_meta(sid)` and calls `registry.recover_session(sid, Path(meta.staging_dir))`. Sessions with no Redis meta or no `staging_dir` are skipped with a warning. Each recovered session gets its `_on_expiry_change` callback wired to `SessionIndex.update_expiry`.
3. Starts the `sweeper()` background task.

`sweeper()` loops forever: `sleep(SWEEP_INTERVAL)` → `get_expired()` → for each expired id, `registry.remove(sid)`, resolve the staging dir from Redis meta (falling back to `staging_dir(base, sid, STAGING_PREFIX)` by convention), `shutil.rmtree(..., ignore_errors=True)`, then `index.remove(sid)`. The docstring and comments stress the design point: **no `iterdir`, no SQLite opens during cleanup**. The whole body is wrapped in `try/except Exception` so one bad session cannot kill the loop.

On shutdown: cancel `_sweep_task`, await it swallowing `CancelledError`, then `await _session_index.close()`.

#### Endpoint table

Eight route decorators total: one `@app.websocket` and seven HTTP routes.

| Method | Path | Query / headers | Success response | Error responses |
|---|---|---|---|---|
| WS | `/mfup/control` | — (first JSON message must be `HELLO` or `RESUME`) | `HELLO_OK` or `RESUME_OK` JSON, then an open control loop | `SESSION_ABORT` JSON + `ws.close()` with `code` ∈ `bad_version`, `bad_target_dir`, `conflict`, `not_found`, `auth_failed`, `invalid_state`, `protocol_error` |
| POST | `/mfup/data/{session_id}/{leg_id}` | `?seq=<int>` (**required**), `?final=<0\|1>` (default `0`), `?epoch=<int>` (default `-1`); header `x-mfup-token` | `200` `{ok, bytes_received, frames}`, plus `commit: {files, bytes}` when a commit succeeded on this request | `410` session not found; `403` invalid token; `409` `stale leg` / `stale_epoch` / `session in state X` / `data_after_final` / `seq_mismatch`; `500` `data_stream_error`; `422` from FastAPI if `seq` is absent |
| POST | `/mfup/probe/{session_id}` | header `x-mfup-token` | `200` `{ok: true, total_bytes}` | `410` not found or terminal state; `403` invalid token |
| GET | `/mfup/sessions` | — | `200` array of `{session_id, state, epoch, leg_id, expires_at}` | — |
| GET | `/mfup/sessions/{session_id}` | — | `200` `{session_id, state, epoch, leg_id, expires_at, root_summary}` | `404` `{"error": "not found"}` |
| POST | `/mfup/sessions/{session_id}/publish` | — | `200` `{published: [names]}` | `404` not found / `FileNotFoundError`; `409` state ≠ `committed`, or `conflict_files` with `conflicting_files` count; `403` `target_dir escapes base directory` |
| POST | `/mfup/sweep` | — | `200` `{removed: [session_ids]}` | — |
| GET | `/health` | — | `200` `{status: "ok", protocol: "MFUP/2"}` | — |

**The four admin routes (`GET /mfup/sessions`, `GET /mfup/sessions/{id}`, `POST /mfup/sessions/{id}/publish`, `POST /mfup/sweep`) perform no authentication or authorization** — no `x-mfup-token` check, no resume-token check, nothing. Read directly from the handler bodies. `POST .../publish` in particular moves files into an operator-visible directory and then deletes the staging dir.

#### `control_endpoint(ws: WebSocket)` — WebSocket handshake and control loop

**HELLO path.** Validates `msg["v"] == PROTOCOL_VERSION`; a mismatch aborts with `bad_version` and reason `f"expected {PROTOCOL_VERSION}"`. Reads `session_id`, `resume_token`, `leg_id` (all `msg[...]`, so a missing key raises `KeyError` into the outer handler) and `target_dir` (`msg.get("target_dir", ".")`). Runs `_is_safe_target(registry.base_dir, target_dir)` and aborts with `bad_target_dir` on failure. Computes `expires = now + SESSION_RESUME_TTL`, calls `registry.create(...)`, then `SessionIndex.register(session_id, expires, target_dir, str(staging_dir(...)))` and installs the `_on_expiry_change` callback. A `ValueError` from `create` (session id already live) becomes `SESSION_ABORT code="conflict"`. Replies:

```json
{"t": "HELLO_OK", "epoch": <int>, "expires_at": "<iso8601>",
 "limits": {"max_chunk_bytes": ..., "max_open_files": ..., "max_pending_files": ...}}
```

**RESUME path.** `registry.resume(session_id, resume_token, leg_id)` maps exceptions to abort codes: `KeyError` → `not_found`, `PermissionError` → `auth_failed`, `ValueError` → `invalid_state` (reason carries the offending state name). On success re-wires `_on_expiry_change` and sends `session.build_resume_ok()`.

Any other first-message type → `SESSION_ABORT code="protocol_error"`.

**Control loop.** Blocks on `ws.receive_json()` and handles exactly two client message types:

| Message | Effect |
|---|---|
| `{"t": "CLIENT_ABORT"}` | `db.set_state(ABORTED)`, `detach_leg()`, break |
| `{"t": "ACTION", "action": "merge_overwrite"}` | Sets `session.publish_action` (consumed later by `publish_session`) |
| `{"t": "ACTION", "action": "cancel"}` | Sets `publish_action`, then `set_state(ABORTED)`, `detach_leg()`, break |

Any other `action` value is silently ignored (the guard is `action in ("merge_overwrite", "cancel")`).

**`finally` block** — this is where several of the recent invariants live:
1. If `session.state == COMMITTING`, `await session.try_commit()` — a disconnect that arrives after `SESSION_END` still gets a commit attempt.
2. Clear `session.ws` if it is still this socket.
3. If a leg is attached and state ∉ {`COMMITTED`, `ABORTED`}, `detach_leg()`.
4. If state is `ABORTED`, immediately `registry.remove(sid)`, `rmtree` the staging dir (path from Redis meta, falling back to convention), and `index.remove(sid)`. Aborted sessions are not left for the sweeper.

#### `data_endpoint(session_id, leg_id, request, seq, final=0, epoch=-1)`

Gate order matters — each check runs before the request body is touched:

1. `registry.get(session_id)` is `None` → `410 GONE`.
2. `x-mfup-token` header missing or ≠ `session.resume_token` → `403 FORBIDDEN`. The resume token doubles as the data-leg bearer token.
3. `session.leg_id != leg_id` → `409` `{"error": "stale leg", "expected": <current leg>}`.
4. **Epoch check:** `if epoch >= 0 and session.epoch != epoch` → `409` `{"error": "stale_epoch", "got", "expected"}`. The `epoch >= 0` guard means the default `-1` opts out entirely, so an old client that omits `?epoch=` bypasses stale-POST rejection.
5. State must be `ACTIVE` or `PAUSED_BY_SERVER`; anything else → `409` `session in state <x>`.
6. `session.final_seq_seen` already true → `409` `data_after_final`.
7. `session.validate_and_advance_seq(seq)` false → `409` `{"error": "seq_mismatch", "got", "expected": last_data_seq + 1}`.
8. If `final == 1`, set `session.final_seq_seen = True` **before** reading the body.

Then it streams: `async for chunk in request.stream()` → `reader.feed(chunk)` → `reader.drain()` → `await session.process_frame(frame, leg_id)` per frame. **Inside the loop it re-checks leg and epoch on every chunk** and `break`s out mid-body if either has changed — an in-flight POST is abandoned the instant a newer leg attaches. Exceptions during streaming are caught, formatted as `error_detail`, and logged; they do not propagate.

Commit is attempted only when `(final == 1 or session_end_seen) and session.state == COMMITTING`. If `error_detail` is set, the response is `500` `data_stream_error` with `bytes_received`; otherwise `200` with `frames` and the optional `commit` block.

#### `probe_endpoint(session_id, request)`

A connectivity/format probe. After the not-found (`410`), token (`403`), and terminal-state (`410` for `COMMITTED`/`ABORTED`/`EXPIRED`) checks, it drains the body and on the **first** chunk pushes `{"t": "PROBE_ACK", "first_chunk_bytes": len(chunk)}` over the control WebSocket. The inline comment states the purpose exactly: Firefox stringifies a `ReadableStream` request body to the literal `"[object ReadableStream]"` (23 bytes), so the client compares `first_chunk_bytes` against what it sent to detect the bug. Returns `{ok: true, total_bytes}`.

#### `publish_endpoint(session_id)`

Requires state `COMMITTED` (else `409`). Resolves `session.target_dir` against `base_dir` if relative, re-runs `_is_safe_target` as defense in depth (`403`), and calls `publish_session(base_dir, session_id, target, STAGING_PREFIX, action=session.publish_action)`. `ConflictError` → `409` with `conflicting_files`; `FileNotFoundError` → `404`. On success removes the session from both the in-memory registry and Redis.

#### `_is_safe_target(base_dir: Path, target_dir: str) -> bool`

Resolves `target_dir` (absolute as-is, relative against `base_dir`), then `resolved.relative_to(base_dir.resolve())` inside a `try`. `ValueError` → `False`. Called at HELLO and again at publish.

---

### 3.2 Session state machine (`session_manager.py`)

#### States

`SessionState` (defined in `protocol.py`, a `str`-valued enum) has **eight** members. Their reachability in the current code:

| State | Value | Written by | Reachable? |
|---|---|---|---|
| `ACTIVE` | `"active"` | `init_session`, `attach_leg`, `send_flow(False)`, `try_commit` COMMIT_RETRY branch | Yes |
| `PAUSED_BY_SERVER` | `"paused_by_server"` | `send_flow(True)` only | **No** — `send_flow` has zero callers (verified by grep) |
| `WAITING_RESUME` | `"waiting_resume"` | `detach_leg`, `recover_session` | Yes |
| `COMMITTING` | `"committing"` | `_handle_session_end` | Yes |
| `COMMITTED` | `"committed"` | `try_commit` success branch | Yes |
| `ABORTED` | `"aborted"` | `_handle_client_abort`, control-loop `CLIENT_ABORT`, control-loop `ACTION cancel` | Yes |
| `EXPIRED` | `"expired"` | nothing | **No** — only *read*, in `probe_endpoint`'s terminal-state check |
| `FAILED` | `"failed"` | nothing | **No** — never referenced outside the enum definition |

Expiry is therefore never modelled as a state transition: an expired session is deleted outright by the sweeper (registry removal + `rmtree` + Redis removal), so the `EXPIRED` value can only ever be observed if it were written externally into `state.sqlite`. `FAILED` is entirely vestigial.

State is **not** an in-memory attribute — `LiveSession.state` is a property that reads `SELECT state FROM sessions` through `db.get_state()` on every access, and every write goes through `db.set_state()` which commits immediately. State survives process restart by construction.

#### Transition table

| From | To | Trigger | Implementation |
|---|---|---|---|
| *(none)* | `ACTIVE` | `HELLO` → `SessionRegistry.create()` | `db.init_session()` inserts `state='active', epoch=1`, then `attach_leg()` sets `ACTIVE` and increments epoch to **2** |
| `WAITING_RESUME` / `ACTIVE` / `PAUSED_BY_SERVER` | `ACTIVE` | `RESUME` → `SessionRegistry.resume()` → `attach_leg(leg_id)` | Epoch incremented; `last_data_seq = -1`; `final_seq_seen = False`; all writers closed; idle timer reset |
| `ACTIVE` / `PAUSED_BY_SERVER` | `WAITING_RESUME` | `detach_leg()` — from WS disconnect (`finally`), leg idle timeout `_on_idle`, `ACTION cancel`, `CLIENT_ABORT`, or `registry.remove()` | Also sets `expires_at = now + session_resume_ttl` and fires `_on_expiry_change` → `SessionIndex.update_expiry` via `asyncio.ensure_future` (fire-and-forget) |
| `ACTIVE` | `PAUSED_BY_SERVER` | `send_flow(paused=True, reason)` | Unreachable — no caller |
| `PAUSED_BY_SERVER` | `ACTIVE` | `send_flow(paused=False, reason)` | Unreachable — no caller |
| `ACTIVE` | `COMMITTING` | `SESSION_END` frame (tag `0x07`) → `_handle_session_end` | Writes final root summary with `sealed=True`, closes all writers |
| `COMMITTING` | `COMMITTED` | `try_commit()` with `db.get_incomplete_files()` empty | Sends `{"t":"COMMIT_OK","files","bytes"}` on the control WS and returns it |
| `COMMITTING` | `ACTIVE` | `try_commit()` with incomplete files — **COMMIT_RETRY** | See below |
| *any* | `ABORTED` | `CLIENT_ABORT` frame (`0x08`), control JSON `{"t":"CLIENT_ABORT"}`, or `{"t":"ACTION","action":"cancel"}` | Staging dir + Redis entry deleted in the WS `finally` block |
| `ACTIVE` / `PAUSED_BY_SERVER` / `COMMITTING` | `WAITING_RESUME` | **Server restart** → `SessionRegistry.recover_session()` | See below |
| `COMMITTED` | *(session deleted)* | `POST /mfup/sessions/{id}/publish` | `publish_session()` then registry + Redis removal |

#### Epochs

The epoch is a monotonically increasing integer persisted in `sessions.epoch`, and is the mechanism for fencing off stale data legs.

- Initialised to `1` by `init_session`; `attach_leg()` calls `db.increment_epoch()` on every leg attach, so the **first epoch a client ever sees in `HELLO_OK` is 2**.
- Reported in `HELLO_OK.epoch` and `RESUME_OK.epoch`, and echoed back by the client as `?epoch=` on every data POST.
- `data_endpoint` rejects `409 stale_epoch` when `epoch >= 0 and session.epoch != epoch`, and re-checks per streamed chunk so an already-running POST is torn down when a reconnect bumps the epoch.
- Because each reconnect increments the epoch, an old TCP connection that survives a client-side reconnect cannot write into the new leg's file state.
- **COMMIT_RETRY deliberately does not bump the epoch** — the client resends on the same epoch and the same leg.

#### COMMIT_RETRY

`try_commit()` is the pre-commit invariant gate:

1. Returns `None` immediately unless state is `COMMITTING` (making it safe to call from both `data_endpoint` and the WS `finally` block).
2. If `self.writers` is non-empty it logs a warning and closes them — stale writers must not hold the check open.
3. Queries `db.get_incomplete_files()`, which returns every non-rejected, non-pruned file where `final_size IS NULL OR accepted_offset != final_size`. If any exist, it **reverts state to `ACTIVE`**, resets `final_seq_seen = False` and `last_data_seq = -1` so a fresh sequence of POSTs (including a new `final=1`) is accepted, and sends:

```json
{"t": "COMMIT_RETRY", "incomplete": [{"node_id": <int>, "accepted_offset": <int>}, ...]}
```

Note the client is told `accepted_offset` but **not** `final_size` — it must already know the expected size. Then returns `None` (so `data_endpoint` omits the `commit` key from its response).
4. Otherwise `db.count_committed_files()` → `(file_count, total_bytes)`, state → `COMMITTED`, and `{"t":"COMMIT_OK","files","bytes"}` is both sent and returned.

#### Restart-recovery revert (`recover_session`)

```python
async def recover_session(self, session_id: str, staging_path: Path) -> LiveSession | None
```

Opens `staging_path / "state.sqlite"` (returns `None` if missing or unopenable), reads the single `sessions` row (returns `None` if the table is empty), and refuses to recover any session already in a terminal state — only `ACTIVE`, `PAUSED_BY_SERVER`, `WAITING_RESUME`, and `COMMITTING` are recoverable. It then forces:

```python
if state in (ACTIVE, PAUSED_BY_SERVER, COMMITTING):
    db.set_state(SessionState.WAITING_RESUME)
```

The comment states the reason for including `COMMITTING`: *"commit is client-initiated, so after server restart the client must reconnect and re-send SESSION_END."* Without this, a session frozen mid-commit at restart would sit in `COMMITTING`, which `data_endpoint` rejects with `409` and which `resume()` refuses with `invalid_state` — it would be permanently stuck. The revert makes it resumable again. The recovered `LiveSession` is registered with **no leg and no WebSocket**; the client must send `RESUME`.

#### Observed gap in the detach guard

`detach_leg()` only rewrites state when it is `ACTIVE` or `PAUSED_BY_SERVER`. The `data_endpoint` and WS `finally` paths always call `try_commit()` first, so `COMMITTING` normally resolves to `COMMITTED` or back to `ACTIVE` before detach. But `_on_idle()` — the `leg_idle_timeout` callback — calls `detach_leg()` directly. A session that reaches `COMMITTING` and then goes idle without a disconnect is left in `COMMITTING` with `leg_id = None` and **`expires_at` not refreshed**, since only the `ACTIVE`/`PAUSED_BY_SERVER` branch extends the TTL. It would then be swept at its original expiry. This is read from the control flow, not from a comment; I have not confirmed whether the timing window is reachable in practice.

#### `LiveSession` API

| Member | Type | Description |
|---|---|---|
| `LiveSession(session_id, resume_token, base_dir, db, *, target_dir=".", staging_prefix=".incoming", session_resume_ttl=3600, leg_idle_timeout=60, publish_timeout=30)` | ctor | `publish_timeout` is stored and never read |
| `state` | property → `SessionState` | Reads `db.get_state()` every access |
| `expires_at` | property → `str` | Reads `db.get_expires_at()` every access |
| `validate_and_advance_seq(seq: int) -> bool` | method | `True` and advances iff `seq == last_data_seq + 1`; `False` on any gap or duplicate |
| `attach_leg(leg_id: str) -> int` | method | Closes writers, resets seq state, increments epoch, sets `ACTIVE`, arms idle timer, returns new epoch |
| `detach_leg() -> None` | method | Closes writers, cancels idle timer, conditionally → `WAITING_RESUME` + TTL refresh, clears `leg_id` and `ws` |
| `process_frame(frame: Frame, leg_id: str)` | async | Drops frames whose `leg_id` ≠ current leg with a warning; resets idle timer; dispatches by `isinstance` |
| `prune_node(node_id, code, reason)` | async | Persists prune, closes writer, sends `PRUNE_NODE`. **No callers** |
| `reject_file(node_id, code, reason)` | async | Persists rejection, closes writer, sends `REJECT_FILE`. **No callers** |
| `send_flow(paused: bool, reason: str)` | async | Flips `PAUSED_BY_SERVER`/`ACTIVE` and sends `FLOW`. **No callers** |
| `send_control(msg: dict)` | async | Best-effort `ws.send_json`; swallows and logs exceptions |
| `try_commit() -> Optional[dict]` | async | Pre-commit invariant gate; see COMMIT_RETRY above |
| `build_resume_ok() -> dict` | method | Builds the `RESUME_OK` payload from persisted state |

Server-initiated flow control, pruning, and rejection are therefore **implemented but unwired** — the protocol supports `FLOW`, `PRUNE_NODE`, and `REJECT_FILE`, and the DB has `pruned`/`rejected` tables and `is_pruned`/`is_rejected` guards on every frame handler, but nothing in the current server ever decides to invoke them.

#### Frame handlers

| Handler | Frame | Behavior |
|---|---|---|
| `_handle_node` | `NodeFrame` | Skips pruned nodes; runs `validate_node_name(f.name)` and **drops the node with a warning** on failure; `db.upsert_node(...)`. For `DIR` kind, `mkdir(parents=True, exist_ok=True)` at the resolved payload path. Runs `_check_conflict` for both kinds |
| `_handle_file_open` | `FileOpenFrame` | Skips rejected/pruned; NACKs `bad_offset` if the node is unknown; reads `accepted_offset` from `files` and constructs a `FileWriter` (resume-aware) into `self.writers[node_id]` |
| `_handle_file_chunk` | `FileChunkFrame` | Skips rejected/pruned; NACKs `bad_offset` if no writer; verifies CRC-32C (NACK `bad_checksum`); verifies `f.offset == writer.accepted_offset` (NACK `bad_offset`); writes; NACKs `server_policy` on write error; persists `accepted_offset`; read-modify-writes the root summary's `body_done_bytes`; sends `FILE_ACK` roughly every 256 KiB (`new_offset % 262144 < f.length or f.length == 0`) |
| `_handle_file_close` | `FileCloseFrame` | Pops and closes the writer, `db.set_file_final(node_id, f.size_sent, path)`, node status → `CLOSED`, sends a final `FILE_ACK`. **`f.size_sent` is stored as `final_size` without being compared to `accepted_offset`** — the mismatch is what `get_incomplete_files()` later detects and turns into COMMIT_RETRY |
| `_handle_dir_close` | `DirCloseFrame` | Node status → `CLOSED` |
| `_handle_summary` | `SummaryFrame` | Merges monotonically: `max()` on scan/est counters, `sealed or`; **keeps the server's own `body_done_bytes`**, ignoring the client's claim |
| `_handle_session_end` | `SessionEndFrame` | Trusts the client's scan/est values, keeps server `body_done_bytes`, forces `sealed=True`, closes all writers, state → `COMMITTING` |
| `_handle_client_abort` | `ClientAbortFrame` | Closes all writers, state → `ABORTED` |

#### Conflict FSM

Separate from the session state machine, `LiveSession.conflict_state` is a three-value string FSM: `"clean"` → `"conflict_dir"` → `"conflict_files"`. `_check_conflict(node_id, is_dir)` resolves where the node *would* land in `target_dir` via `_resolve_target_path` and inspects the filesystem. A directory landing on an existing directory advances to `conflict_dir` silently (auto-merge). Anything else that already exists — file-on-file or a type mismatch — jumps straight to the terminal `conflict_files` and sends `{"t": "ASK"}` once to the client, which is expected to reply with an `ACTION`. `_resolve_target_path` walks the `parent_id` chain to `ROOT_NODE_ID` and returns `None` if any node is missing or if the resolved path escapes `base_dir`.

#### `FileWriter`

```python
class FileWriter:
    def __init__(self, path: Path, node_id: int, accepted_offset: int = 0) -> None
    async def write(self, data: bytes, offset: int) -> int
    def close(self) -> None
```

Opens the **final payload path directly** — there is no per-file `.part` file. At `accepted_offset == 0` it opens `"wb"` (truncating); otherwise `"r+b"`, `seek(accepted_offset)`, `truncate()` — discarding any bytes past the last acknowledged offset so a resume can never leave a hole or duplicated tail. `write()` raises `ValueError` on any offset that is not exactly `accepted_offset`, and delegates the blocking write to `asyncio.to_thread(self._sync_write, data)`. `_sync_write` does `write()` + `flush()` — **no `fsync`**, so committed data is not durable against machine loss.

#### `SessionRegistry`

| Member | Signature | Description |
|---|---|---|
| `__init__` | `(base_dir: Path, staging_prefix=".incoming", **defaults)` | `defaults` are forwarded verbatim to every `LiveSession` |
| `create` | `async (session_id, resume_token, leg_id, expires_at, target_dir=".") -> LiveSession` | Raises `ValueError` if the id is already live; opens the DB, `init_session`, `attach_leg` |
| `resume` | `async (session_id, resume_token, leg_id) -> LiveSession` | `KeyError` if unknown, `PermissionError` on token mismatch, `ValueError` unless state ∈ {`WAITING_RESUME`, `ACTIVE`, `PAUSED_BY_SERVER`}; then `attach_leg` |
| `get` | `(session_id) -> Optional[LiveSession]` | Sync dict lookup, **not** locked |
| `remove` | `async (session_id) -> None` | Pops, `detach_leg()`, `db.close()` |
| `recover_session` | `async (session_id, staging_path) -> LiveSession \| None` | Restart recovery; see above |
| `all_sessions` | `() -> dict[str, LiveSession]` | Shallow copy |

The class docstring is explicit about the design choice: *"No startup scan — stale staging dirs are cleaned by the Redis-based sweeper."* Note that `resume()` looks only in the in-memory `_sessions` dict, so resumption depends entirely on the session having been recovered into memory at startup or created in this process lifetime.

---

### 3.3 Wire protocol implementation (`protocol.py`)

Two distinct encodings share one session:

- **Control channel (WebSocket):** JSON objects, discriminated by a `"t"` field. Not defined by any schema in this module — the message shapes exist only as literal dicts scattered across `app.py` and `session_manager.py`. The only protocol constant here is `PROTOCOL_VERSION = "MFUP/2"`.
- **Data legs (HTTP POST bodies):** a stream of length-prefixed binary frames, decoded by `FrameReader`.

#### Binary framing

```
[u32 big-endian length][u8 tag][payload...]
```

`length` covers the tag byte plus the payload (`1 + len(payload)`). All multi-byte integers are big-endian (`struct` format `"!"`). Strings are `[u16 length][UTF-8 bytes]`. Optional fields are encoded as a `u8` presence flag followed, if set, by the value.

There is **no version byte, magic number, or frame checksum in the envelope** — versioning is negotiated once, out of band, via the `v` field of the `HELLO` control message. Payload integrity is per-chunk only (`FILE_CHUNK` carries its own CRC-32C).

#### Frame types

The eight tags and their payload layouts are tabulated in §2.3. `SESSION_END` carries no `node_id` — it is session-scoped. All frame dataclasses use `@dataclass(slots=True)` and default every field, including `tag`.

Supporting enums: `NodeKind` (`DIR = 0x00`, `FILE = 0x01`), `ChecksumKind` (`CRC32C = 0x01` — the only kind), `NodeStatus` (`open`, `closed`, `rejected`, `pruned`), and the constant `ROOT_NODE_ID = 0`.

`Frame` is a union type alias over all eight dataclasses.

#### Decoder

| Symbol | Signature | Notes |
|---|---|---|
| `decode_frame_payload` | `(tag: int, payload: memoryview) -> Frame` | Raises `ValueError(f"unknown frame tag: {tag:#04x}")` on an unrecognised tag |
| `FrameReader` | `class` | `feed(data: bytes) -> None` appends to an internal `bytearray`; `drain() -> list[Frame]` decodes every complete frame and deletes consumed bytes from the front |
| `crc32c` | `(data: bytes \| memoryview, initial: int = 0) -> int` | Pure-Python Castagnoli implementation, polynomial `0x82F63B78`, table built once at import by `_init_crc32c_table()`; standard init/final XOR with `0xFFFFFFFF` |

`FrameReader.drain()` breaks on the first incomplete frame and leaves the partial bytes buffered, so frames may be split arbitrarily across HTTP chunks. It deliberately copies each payload into a fresh `bytes` object before wrapping it in a `memoryview` — the comment notes this is so no memoryview keeps a reference into the mutable buffer that `del self._buf[:pos]` would then invalidate.

The reader applies **no bound on `frame_len`**. A malicious or corrupt 4-byte prefix declaring a multi-gigabyte frame causes the buffer to grow until the declared length is satisfied, with no cap and no error. `MAX_CHUNK_BYTES` is never consulted here.

The pure-Python `crc32c` runs a per-byte loop over every chunk payload on the event loop thread — for a 256 KiB chunk that is 262,144 Python-level iterations, synchronously, blocking all other sessions. (Observation from reading the loop; not benchmarked.)

#### Control messages

The 4 client→server and 12 server→client message shapes are tabulated in §2.4. They are not defined by any schema in this module — the shapes exist only as literal dicts across `app.py` and `session_manager.py`.

`build_resume_ok()` maps each file's DB `NodeStatus` to a three-way client-facing string: `rejected` → `"rejected"`, `closed` → `"closed"`, everything else (including `pruned`) → `"open"`. Pruned nodes are additionally listed in `pruned_nodes`.

---

### 3.4 Storage layer (`storage.py`)

#### On-disk layout

```
<base_dir>/
  .incoming.<session_id>/          # staging_prefix + "." + session_id
    state.sqlite                   # (+ -wal, -shm from WAL mode)
    payload/                       # mirrors the client's directory tree
      <name>/<name>/<file>
  <target_dir>/                    # final location, populated only by publish
```

`base_dir` doubles as the parent for relative `target_dir` values, so staging and final trees are siblings — which is what makes `os.rename` at publish a same-filesystem, atomic-per-entry operation.

| Helper | Signature | Description |
|---|---|---|
| `staging_dir` | `(base_dir, session_id, prefix=".incoming") -> Path` | Pure path construction: `base_dir / f"{prefix}.{session_id}"` |
| `ensure_staging` | `(base_dir, session_id, prefix) -> Path` | `mkdir` the staging dir and its `payload/` subdir |
| `open_session_db` | `(base_dir, session_id, prefix) -> SessionDB` | `ensure_staging` then `SessionDB(sd / "state.sqlite")` |
| `validate_node_name` | `(name: str) -> None` | Raises `ValueError` on empty, `"."`, `".."`, or any name containing `/`, `\`, or NUL |
| `resolve_payload_path` | `(base_dir, session_id, db, node_id, prefix) -> Path` | Walks the `parent_id` chain to `ROOT_NODE_ID`, validating each component, then verifies containment |

The staging prefix is a configurable constant (`DEFAULT_STAGING_PREFIX = ".incoming"`, overridable via `MFUP_STAGING_PREFIX`) rather than a hardcoded literal, and the leading dot keeps staging dirs hidden from ordinary directory listings of `base_dir`.

#### Path-traversal defenses

Four independent layers, all read directly from the code:

1. **Name validation at ingest** — `_handle_node` calls `validate_node_name(f.name)` before the node is ever written to the DB, dropping the frame on failure. `..`, absolute-ish names, separators, and NUL bytes never enter `nodes`.
2. **Name validation at path construction** — `resolve_payload_path` calls `validate_node_name` again on every component it walks, so even a node that somehow reached the DB cannot produce a path.
3. **Containment check after resolution** — `resolve_payload_path` ends with `if not result.resolve().is_relative_to(payload_root.resolve()): raise ValueError(f"path traversal detected: {result}")`, catching symlink-based escapes that name validation alone cannot (`resolve()` follows links).
4. **Target-dir containment** — `app._is_safe_target` gates `target_dir` at HELLO and again at publish, and `LiveSession._resolve_target_path` independently returns `None` for any conflict-check path that escapes `base_dir`.

The `resolve()`-based checks are TOCTOU-susceptible in principle (an attacker who can create symlinks inside the payload dir between the check and the open could redirect a write), but exploiting that requires filesystem write access to the staging directory, which the upload path itself does not grant beyond validated names. Flagged as inference — no comment in the code addresses it.

#### Chunk assembly and commit atomicity

Chunks are **not** staged as separate part files and later concatenated. `FileWriter` writes directly into the file's final payload path at strictly contiguous offsets; assembly is just sequential appending. Resume is handled by seek-and-truncate to `accepted_offset` (see `FileWriter` above), which guarantees the file on disk never contains bytes past the last durably-recorded offset.

**Commit is a state flip, not a filesystem operation.** `try_commit()` verifies the invariant (`get_incomplete_files()` empty) and sets `state = COMMITTED`; nothing moves on disk. The actual materialization happens later in `publish.py`.

**Publish is atomic per entry, not as a whole.** `publish_session` loops over `payload.iterdir()` calling `os.rename` (or `os.replace` for existing files in the merge path). Each individual rename is atomic on a POSIX filesystem, but there is no transaction spanning the loop and no rollback: a failure partway through leaves some entries published and the rest still staged, with `_cleanup_staging` not yet run. Stated as read from the code — there is no attempt at all-or-nothing semantics.

#### `SessionDB`

One SQLite connection per session, opened with `timeout=5.0` and configured `PRAGMA journal_mode=WAL`, `PRAGMA busy_timeout=3000`, `PRAGMA synchronous=NORMAL`. The schema is applied with `executescript` on every open (all statements are `IF NOT EXISTS` / `INSERT OR IGNORE`), so opening an existing DB is idempotent.

Six tables:

```sql
sessions(session_id PK, resume_token, epoch DEFAULT 1, state DEFAULT 'active',
         target_dir DEFAULT '.', expires_at, created_at, updated_at)
nodes(node_id PK, parent_id, kind, name, size, mtime_ms, status DEFAULT 'open')
files(node_id PK REFERENCES nodes, accepted_offset DEFAULT 0, final_size,
      checksum_state BLOB, local_tmp_path)
pruned(node_id PK)
rejected(node_id PK, code, reason)
root_summary(id PK CHECK (id = 1), scan_done_units, scan_est_units,
             body_done_bytes, body_est_bytes, sealed)
```

`files.checksum_state` is declared but never written or read anywhere — presumably reserved for whole-file rolling checksums (inferred).

| Method | Signature | Description |
|---|---|---|
| `init_session` | `(session_id, resume_token, expires_at, target_dir=".")` | `INSERT OR REPLACE` with `epoch=1`, `state='active'` |
| `get_session` | `() -> Optional[sqlite3.Row]` | `SELECT * FROM sessions LIMIT 1` |
| `get_target_dir` | `() -> str` | Defined; **no callers** |
| `set_state` / `get_state` | `(SessionState)` / `-> SessionState` | Both commit/read immediately; `get_state` defaults to `ACTIVE` if the row is missing |
| `increment_epoch` / `get_epoch` | `-> int` | `UPDATE ... epoch = epoch + 1` then re-`SELECT` (two statements, not atomic as a unit) |
| `get_expires_at` / `set_expires_at` | `-> str` / `(str)` | ISO-8601 strings |
| `upsert_node` | `(node_id, parent_id, kind, name, size=None, mtime_ms=None)` | `INSERT OR REPLACE` into `nodes`; for `FILE` kind also `INSERT OR IGNORE` a `files` row at offset 0 — so re-sending a `NODE` frame **does not reset `accepted_offset`**, which is what makes NODE replay on resume safe |
| `get_node` / `set_node_status` | | |
| `get_file` / `set_accepted_offset` / `set_file_final` | | `set_file_final` writes `final_size` and `local_tmp_path` |
| `get_open_files` | `-> list[Row]` | Defined; **no callers** |
| `get_all_files` | `-> list[Row]` | Feeds `build_resume_ok` |
| `add_pruned` / `is_pruned` / `get_pruned_nodes` | | |
| `add_rejected` / `is_rejected` / `get_rejected_files` | | `add_rejected` also sets node status to `REJECTED` |
| `update_root_summary` / `get_root_summary` | | Single row, `id=1` |
| `count_committed_files` | `-> tuple[int, int]` | `COUNT(*), COALESCE(SUM(accepted_offset), 0)` over files whose node status is not `rejected`/`pruned` — the `bytes` figure in `COMMIT_OK` |
| `get_incomplete_files` | `-> list[dict]` | Non-rejected, non-pruned files where `final_size IS NULL OR accepted_offset != final_size` — the pre-commit invariant |

`sqlite3.Row` is assigned as `row_factory` lazily inside each reading method rather than once in `__init__`, so methods that return raw tuples (`get_epoch`, `get_state`, `get_target_dir`, `count_committed_files`) index positionally regardless. `set_state`, `set_expires_at`, and `increment_epoch` all issue `UPDATE sessions SET ...` **without a `WHERE` clause** — correct only because each DB is single-session by construction.

Every mutating method calls `self._conn.commit()` individually, so a single `FILE_CHUNK` performs at least two synchronous SQLite commits (`set_accepted_offset` plus the root-summary update) plus a read of the summary — three round-trips per chunk.

---

### 3.5 Redis index (`redis_index.py`)

An async wrapper over `redis.asyncio` holding session expiry and path metadata, so cleanup and startup recovery never need to scan the filesystem or open any SQLite file. The module docstring states the sweeper flow explicitly: *"ZRANGEBYSCORE → HGETALL meta → rmtree(staging_dir) → ZREM + DEL meta. No iterdir. No SQLite opens for cleanup."*

#### Key schema

| Key | Type | Contents |
|---|---|---|
| `mfup:sessions` (`SESSIONS_KEY`) | sorted set | member = `session_id`, score = `expires_at` as a Unix timestamp (float) |
| `mfup:meta:{session_id}` (`META_PREFIX + id`) | hash | fields `target_dir`, `staging_dir` (both absolute path strings) |

Neither key carries a Redis-native TTL — expiry is expressed purely as the sorted-set score, so entries persist until the sweeper (or an explicit `remove`) deletes them. This is deliberate: the server needs to *find* expired sessions in order to `rmtree` their staging directories, which native key expiry would make impossible.

#### API

`SessionMeta` is a two-slot value object (`target_dir`, `staging_dir`).

| Method | Signature | Redis operations |
|---|---|---|
| `__init__` | `(redis_url="redis://redis:6379/0")` | `aioredis.from_url(..., decode_responses=True)` |
| `register` | `async (session_id, expires_at: datetime, target_dir: str, staging_dir: str)` | Pipelined `ZADD` + `HSET` — one round trip |
| `update_expiry` | `async (session_id, expires_at: datetime)` | `ZADD` (re-scores the existing member) |
| `get_expired` | `async (now=None) -> list[str]` | `ZRANGEBYSCORE key -inf <now>` |
| `get_not_expired` | `async (now=None) -> list[str]` | `ZRANGEBYSCORE key <now> +inf` |
| `all_sessions` | `async () -> list[str]` | `ZRANGE key 0 -1` |
| `get_meta` | `async (session_id) -> SessionMeta \| None` | `HGETALL`; `None` on an empty hash |
| `remove` | `async (session_id)` | Pipelined `ZREM` + `DEL` |
| `close` | `async ()` | `aclose()` |

#### Is Redis required?

**Yes — required, not optional.** There is no in-memory fallback, no feature flag, and no `try`/`except` around the calls that matter:

- `SessionIndex(REDIS_URL)` is constructed unconditionally in `lifespan`.
- In the HELLO path, `await idx.register(...)` sits inside a `try` that catches only `ValueError`. A `redis.ConnectionError` propagates out to the WS handler's `except Exception`, so **`HELLO_OK` is never sent and session creation appears to fail** — even though `registry.create` already succeeded and left a live in-memory session behind.
- `publish_endpoint` and `sweep_endpoint` both await Redis calls with no error handling.

The two places that *do* tolerate a Redis outage are startup recovery (wrapped in `try/except Exception` — a failure just means zero sessions are recovered) and the sweeper loop body (logs and continues to the next interval). So a Redis outage degrades gracefully at boot and during cleanup, but hard-fails every new upload.

`register()` also boundary-crosses in one direction only: expiry updates flow *server → Redis* through the `_on_expiry_change` callback via `asyncio.ensure_future` (fire-and-forget, no result awaited, no error surfaced). A failed expiry update is silently lost, leaving the sorted-set score stale — the sweeper could then delete a session that is actually still alive.

---

### 3.6 Publish (`publish.py`)

Moves a committed session's `payload/` contents into the caller's target directory using renames.

| Symbol | Signature | Description |
|---|---|---|
| `ConflictError` | `Exception` with `conflicting_files: int` | Raised when conflicts exist and no `action` was supplied |
| `detect_conflicts` | `(target_dir: Path, payload: Path) -> int` | Counts file-vs-file collisions and type mismatches; dir-vs-dir pairs are recursed into, not counted |
| `publish_session` | `(base_dir, session_id, target_dir, prefix=".incoming", action: str \| None = None) -> list[str]` | Main entry point |
| `_merge_tree` | `(src, dst) -> list[str]` | Recursive overwrite-merge |
| `_cleanup_staging` | `(sd) -> None` | `shutil.rmtree(..., ignore_errors=True)`, itself wrapped in try/except |

`publish_session` flow: resolve `staging_dir/payload` (raises `FileNotFoundError` if absent) → `target_dir.mkdir(parents=True, exist_ok=True)` → `detect_conflicts`. If conflicts exist and `action is None`, raise `ConflictError`. If conflicts exist and `action == "merge_overwrite"`, run `_merge_tree`. Otherwise take the clean path: a flat loop of `os.rename(entry, target/entry.name)` over the payload's top-level entries — moving whole subtrees by a single rename each. Finally `_cleanup_staging` removes the entire staging dir including `state.sqlite`.

`_merge_tree` handles three cases per entry: dir-onto-dir recurses; dir-onto-file unlinks the file then renames; file-onto-anything uses `os.replace` (atomic overwrite) or `os.rename` (create).

Two behaviors worth calling out, both read directly:

- **`detect_conflicts` has its argument order inverted in the recursive call.** The signature is `(target_dir, payload)`, but the recursion is `detect_conflicts(dest, entry)` where `dest` is the *target* side and `entry` is the *payload* side — i.e. `(target, payload)`, matching the signature. The naming inside the loop (`entry` from `payload.iterdir()`, `dest` in `target_dir`) is consistent. No defect here, but the local names read backwards at a glance.
- **A conflict value of `action` other than `"merge_overwrite"` silently falls through to the clean path.** The `else` branch runs bare `os.rename` even when `conflicts > 0` — and `os.rename` onto an existing *directory* raises `OSError`, while onto an existing *file* it silently overwrites on POSIX. The only other `action` the control loop accepts is `"cancel"`, which aborts the session before publish is reachable, so this is not currently exploitable through the control channel. But `publish_endpoint` passes `session.publish_action` straight through with no validation of its own.

Publish is fully synchronous and blocking — `publish_endpoint` is an `async def` that calls `publish_session` directly, so a large `_merge_tree` blocks the event loop for its duration.

---

### 3.7 Concurrency and safety invariants

**Locking.** There are exactly two `asyncio.Lock` instances. `SessionRegistry._lock` (line 548) guards `create`, `resume`, `remove`, and `recover_session` — it protects the `_sessions` dict against interleaved creation/removal. `SessionRegistry.get()` is deliberately unlocked and synchronous.

`LiveSession._lock` (line 131) **is created and never acquired** — verified by grep: all four `async with self._lock` sites are in `SessionRegistry`. There is therefore **no per-session mutual exclusion**. Serialization of a session's work instead rests on two protocol-level invariants:

1. **One active leg per session.** `attach_leg` overwrites `leg_id` and closes all writers. Every data POST checks `session.leg_id == leg_id` up front and again on each streamed chunk; `process_frame` re-checks and drops frames from a stale leg.
2. **One epoch per leg.** Every `attach_leg` bumps the persisted epoch, and data POSTs carrying a stale `?epoch=` are rejected with `409` — including mid-stream, so an in-flight body is abandoned.

**Concurrent POSTs on the same leg are still possible.** `validate_and_advance_seq` is a pure synchronous function, so its check-and-advance cannot interleave, but everything after it is `await`-heavy. Two POSTs sent back-to-back with `seq=N` and `seq=N+1` both pass the gate and then interleave their frame processing at every `await`. In that case the safety net is `FileWriter.write`'s offset assertion: a chunk arriving at the wrong offset produces `NACK_CHUNK bad_offset` (or, if it raced past the writer's check, a `ValueError` caught and turned into `NACK_CHUNK server_policy`) rather than a corrupt file. Data integrity holds; throughput does not. This is inferred from the control flow — no comment addresses it and there is no test.

A related and more consequential race: `_handle_file_chunk` does a read-modify-write of the root summary (`get_root_summary()` → `update_root_summary(... body_done_bytes + len(payload) ...)`) with an `await` boundary in between. Interleaved chunk handling can lose byte counts. Since `body_done_bytes` is only a progress figure — `COMMIT_OK`'s `bytes` comes from `count_committed_files()` summing `accepted_offset` — the damage is cosmetic. Inferred, not documented in the code.

**Idempotency and duplicate handling.**

| Situation | Server behavior |
|---|---|
| Duplicate POST (same `seq` replayed) | `409 seq_mismatch` before the body is read — the request is cheap to reject and writes nothing |
| Out-of-order POST (`seq` gap) | `409 seq_mismatch` with `expected: last_data_seq + 1` |
| POST after `final=1` on the same leg | `409 data_after_final` |
| POST from a superseded connection | `409 stale leg` or `409 stale_epoch`, and mid-stream `break` if the leg/epoch changes after the body started |
| Duplicate `FILE_CHUNK` (offset already written) | `NACK_CHUNK` with `reason: "bad_offset"` and `expected_offset: accepted_offset` — **not** silently accepted, so the client resyncs from the acknowledged offset |
| Corrupt `FILE_CHUNK` | CRC-32C mismatch → `NACK_CHUNK reason: "bad_checksum"`; nothing is written |
| Duplicate `NODE` frame | `INSERT OR REPLACE` into `nodes` but `INSERT OR IGNORE` into `files` — replaying node metadata on resume does not reset `accepted_offset` |
| Duplicate `FILE_OPEN` | Constructs a fresh `FileWriter` at the persisted `accepted_offset` and overwrites `self.writers[node_id]`, leaking the previous file handle (the old writer is replaced, not closed) |
| Truncated upload → `SESSION_END` anyway | Caught by the pre-commit invariant → `COMMIT_RETRY` with the incomplete list, state back to `ACTIVE`, seq counters reset |
| Server restart mid-transfer | `recover_session` reverts `ACTIVE`/`PAUSED_BY_SERVER`/`COMMITTING` to `WAITING_RESUME`; client resumes and `build_resume_ok` reports per-file `accepted_offset` |
| Leg goes silent | `LEG_IDLE_TIMEOUT` (60s) fires `_on_idle` → `detach_leg` → `WAITING_RESUME` with the TTL refreshed |
| Session never resumed | `SESSION_RESUME_TTL` (3600s) elapses, sweeper `rmtree`s the staging dir and drops both Redis keys |

**Durability.** `FileWriter._sync_write` flushes but never `fsync`s, and SQLite runs at `synchronous=NORMAL` under WAL. A `COMMIT_OK` therefore guarantees the invariants held and the bytes reached the OS page cache — not that they survive a power loss. Read from the pragma settings and the write path; no comment claims otherwise.

**Unbounded resources.** No cap on `FrameReader` buffer growth, no enforcement of `MAX_CHUNK_BYTES` / `MAX_OPEN_FILES` / `MAX_PENDING_FILES`, no limit on the number of concurrent sessions in `SessionRegistry._sessions` (each holding an open SQLite connection), and no cap on total staged bytes.

---

### 3.8 Entry point and packaging

**`server/mfup/__main__.py`** — `python -m mfup` runs `uvicorn.run("mfup.app:app", host="0.0.0.0", port=8070, log_level="info")`. Host, port, and worker count are hardcoded; there is no CLI argument parsing. Note that the design is inherently **single-process**: `SessionRegistry` is in-memory, so a second worker would not see sessions created by the first (`registry.get()` returning `None` → `410` on data POSTs, `resume()` raising `KeyError` → `not_found`). Redis indexes sessions but does not share the live registry.

**`server/pyproject.toml`** — package `mfup-server` 0.1.0, `requires-python = ">=3.10"`.

| Dependency | Constraint | Used for |
|---|---|---|
| `fastapi` | `>=0.115.0` | ASGI app, routing, WebSocket |
| `uvicorn[standard]` | `>=0.30.0` | ASGI server |
| `websockets` | `>=13.0` | WebSocket transport for uvicorn |
| `redis` | `>=5.0.0` | `redis.asyncio` session index |

Optional `dev` extra: `pytest`, `pytest-asyncio`, `httpx`. `sqlite3` comes from the standard library. Note that `Path.is_relative_to` (used in `resolve_payload_path`) requires Python 3.9+, satisfied by the 3.10 floor; `dataclass(slots=True)` requires 3.10 exactly, so the floor is tight.

---

### 3.9 Tests (`tests/test_protocol.py`)

The **only** test file in the server (verified by `find`; `tests/__init__.py` is the sole other file in the directory). It contains **8 test functions**, all synchronous, all exercising `protocol.py` in isolation with no fixtures, no I/O, and no app instantiation. Two module-local helpers build wire bytes: `_encode_string(s)` and `_build_frame(tag, payload)`.

| Test | Verifies |
|---|---|
| `test_crc32c_empty` | `crc32c(b"") == 0` |
| `test_crc32c_known` | `crc32c(b"hello") == 0x9A71BB4C` — one known-answer vector |
| `test_decode_node_frame` | Full `NODE` round-trip including the `has_size=1` / `has_mtime=0` optional-field encoding; asserts all six fields |
| `test_decode_file_chunk_frame` | `FILE_CHUNK` decode: `node_id`, `offset`, `length`, `checksum`, and payload bytes |
| `test_decode_dir_close` | Minimal `DIR_CLOSE` decode |
| `test_decode_session_end` | `SESSION_END` decode: `scan_done_units`, `body_done_bytes`, `sealed` |
| `test_incremental_feed` | Feeds a `DIR_CLOSE` frame **one byte at a time**, asserting `drain()` yields nothing until the final byte — the core streaming-reassembly guarantee |
| `test_multiple_frames` | Two frames in a single `feed()` yield two decoded frames in order |

**Coverage of the binary decoder: 4 of 8 frame types.** Covered: `NODE` (0x01), `FILE_CHUNK` (0x04), `DIR_CLOSE` (0x06), `SESSION_END` (0x07). **Not covered: `SUMMARY` (0x02), `FILE_OPEN` (0x03), `FILE_CLOSE` (0x05), `CLIENT_ABORT` (0x08)** — notably `CLIENT_ABORT` is the only frame that exercises the two-string `_read_string` path back to back.

**What is not tested at all.** Every module except `protocol.py` has zero test coverage:

- **`app.py`** — no route is exercised. No test for any HTTP status path: the `403` token checks, `409 stale leg`, `409 stale_epoch`, `409 seq_mismatch`, `409 data_after_final`, `410 GONE`, `500 data_stream_error`, or the `422` from a missing `seq`. No WebSocket handshake test — `bad_version`, `bad_target_dir`, `conflict`, `not_found`, `auth_failed`, `invalid_state`, and `protocol_error` are all unverified. No lifespan, recovery, or sweeper test.
- **`session_manager.py`** — the state machine, which the batch context calls "the heart of the system," has no tests. No transition is verified. `try_commit`'s COMMIT_RETRY branch, `recover_session`'s `COMMITTING → WAITING_RESUME` revert, `attach_leg`'s epoch increment, `detach_leg`'s TTL refresh, `validate_and_advance_seq`, the conflict FSM, and `build_resume_ok` are all untested. `FileWriter`'s seek-and-truncate resume path — the mechanism preventing duplicated or missing bytes on reconnect — is untested.
- **`storage.py`** — no SQLite test. `get_incomplete_files` (the pre-commit invariant) and `count_committed_files` (the `COMMIT_OK` figures) are unverified. **Critically, none of the path-traversal defenses have a test**: `validate_node_name` is never called with `".."`, a `/`, a `\`, or a NUL byte, and `resolve_payload_path`'s containment check is never driven with an escaping input.
- **`publish.py`** — `detect_conflicts`, `_merge_tree`, `ConflictError`, and the clean-rename path are untested.
- **`redis_index.py`** — untested; no fakeredis or integration fixture.
- **`app._is_safe_target`** — untested despite being the outermost target-directory guard.
- **Decoder robustness** — no test feeds an unknown tag (the `ValueError` branch of `decode_frame_payload`), a truncated payload (which would raise `struct.error` out of `_read_*`), a `FILE_CHUNK` whose declared `length` exceeds the remaining payload (currently silently short-reads via slicing), or an oversized `frame_len`.
- **Async** — `pytest-asyncio` and `httpx` are declared as dev dependencies but no `async def` test and no HTTP client test exist. Both extras are currently unused.

The net effect is that the test suite validates the pure, deterministic decoding layer and nothing else: every stateful, security-relevant, and concurrency-sensitive behavior described in the sections above is unverified by automated tests.
---
## 4. Client SDK (TypeScript)

`@mfup/client` is the browser-side implementation of MFUP/2, a resumable multi-file upload protocol. A session pairs a JSON control channel (WebSocket) with a binary data channel (HTTP `POST`), walks a directory tree from any of four browser file-access APIs, streams file bodies as CRC32C-checksummed chunks, and drives a commit handshake at the end. Disconnects are recovered by reconnecting with a `RESUME` message and replaying only the bytes the server has not yet accepted.

Source root: `client/src`. Eight modules, ~2,300 lines. `session.ts` (1,002 lines) is the orchestrator; every other module is a leaf it composes.

---

### 4.1 Package surface (`client/src/index.ts`)

`index.ts` is a pure re-export barrel — 82 lines, no logic. It re-exports from six of the eight modules. `MfupSession` is the intended entry point; the lower-level pieces (`ControlChannel`, `DataChannel`, the frame encoders, `probeStreaming`) are exported so a consumer can build a custom orchestrator.

Note that every export path uses an explicit `.js` extension (`from "./protocol.js"`), consistent with `"type": "module"` and native ESM resolution.

#### Value exports

| Export | Kind | Source | Description |
|--------|------|--------|-------------|
| `MfupSession` | class | `session.js` | Main orchestrator; see §4.2 |
| `ControlChannel` | class | `control.js` | WebSocket control-plane wrapper |
| `DataChannel` | class | `data-channel.js` | Dual-mode HTTP data-plane writer |
| `ProgressTracker` | class | `progress.js` | Progress aggregation + listener bus |
| `NodeIdAllocator` | class | `ingestion.js` | Monotonic node-id counter |
| `MfupError` | class | `errors.js` | Structured error type |
| `MfupErrorCode` | const enum | `errors.js` | 22 string error codes |
| `MfupErrorLayer` | const enum | `errors.js` | 5 layer tags |
| `FrameTag` | const enum | `protocol.js` | 8 frame discriminators |
| `NodeKind` | const enum | `protocol.js` | `DIR = 0x00`, `FILE = 0x01` |
| `ChecksumKind` | const enum | `protocol.js` | `CRC32C = 0x01` (sole member) |
| `PROTOCOL_VERSION` | const | `protocol.js` | `"MFUP/2"` |
| `ROOT_NODE_ID` | const | `protocol.js` | `0` |
| `crc32c` | function | `protocol.js` | `(data: Uint8Array, initial = 0) => number` |
| `encodeFrame` | function | `protocol.js` | Tag-dispatching encoder |
| `encodeNodeFrame` | function | `protocol.js` | `(f: NodeFrame) => Uint8Array` |
| `encodeSummaryFrame` | function | `protocol.js` | `(f: SummaryFrame) => Uint8Array` |
| `encodeFileOpenFrame` | function | `protocol.js` | `(f: FileOpenFrame) => Uint8Array` |
| `encodeFileChunkFrame` | function | `protocol.js` | `(f: FileChunkFrame) => Uint8Array` |
| `encodeFileCloseFrame` | function | `protocol.js` | `(f: FileCloseFrame) => Uint8Array` |
| `encodeDirCloseFrame` | function | `protocol.js` | `(f: DirCloseFrame) => Uint8Array` |
| `encodeSessionEndFrame` | function | `protocol.js` | `(f: SessionEndFrame) => Uint8Array` |
| `encodeClientAbortFrame` | function | `protocol.js` | `(f: ClientAbortFrame) => Uint8Array` |
| `ingestFromHandles` | function | `ingestion.js` | File System Access API adapter |
| `ingestFromEntries` | function | `ingestion.js` | `webkitGetAsEntry()` adapter |
| `ingestFromFileList` | function | `ingestion.js` | `<input webkitdirectory>` adapter |
| `ingestFromFiles` | function | `ingestion.js` | Flat `File[]` adapter |
| `probeStreaming` | function | `probe.js` | Duplex-streaming capability probe |

The five enums are declared `export const enum`. Under `isolatedModules: true` with TypeScript ≥ 5.0 (the package pins `^5.5.0`), `const enum` is emitted as a regular runtime enum object, so these are usable as values by JS consumers — *this is an inference from the compiler configuration, not something asserted in the source.*

#### Type-only exports

35 type exports total, all re-exported with `export type`:

| Group | Types |
|-------|-------|
| Data frames (`protocol.js`) | `DataFrame`, `NodeFrame`, `SummaryFrame`, `FileOpenFrame`, `FileChunkFrame`, `FileCloseFrame`, `DirCloseFrame`, `SessionEndFrame`, `ClientAbortFrame` |
| Control messages (`protocol.js`) | `ClientControlMsg`, `ServerControlMsg`, `HelloMsg`, `ResumeMsg`, `HelloOkMsg`, `ResumeOkMsg`, `FileAckMsg`, `NackChunkMsg`, `FlowMsg`, `PruneNodeMsg`, `RejectFileMsg`, `SessionAbortMsg`, `CommitOkMsg` |
| Supporting (`protocol.js`) | `ServerLimits`, `RootSummary`, `ResumeFileStatus`, `SessionState` |
| Channels | `ControlChannelOpts`, `ControlEventMap`, `DataChannelOpts`, `DataCommitResult` |
| Ingestion | `DiscoveredNode`, `IngestCallback`, `IngestFilter` |
| Progress / probe / session | `ProgressSnapshot`, `ProgressListener`, `ProbeResult`, `MfupSessionConfig`, `MfupSessionEvents` |

**Declared in `protocol.ts` but *not* re-exported from `index.ts`:** `ClientAbortMsg`, `ActionMsg`, `ProbeAckMsg`, `AskMsg`, `CommitRetryMsg`. The first two are reachable via the `ClientControlMsg` union and the last three via `ServerControlMsg`, but their individual names are internal. `sessionAbortedByServer`, `dataHttpError` and the other 11 error factory functions in `errors.ts` are likewise not re-exported — consumers receive `MfupError` instances but cannot construct them through the public API.

---

### 4.2 Session orchestrator (`session.ts`)

`MfupSession` owns session identity, both channels, the file queue, reconnect policy, and the commit handshake. It is the only module that mutates protocol state.

#### Configuration

```ts
export interface MfupSessionConfig {
  /** Server base URL (http(s)://host) */
  serverUrl: string;
  /** Relative target directory on the server where files will be placed */
  targetDir?: string;
  /** Existing session_id for resume, or omit for new session */
  sessionId?: string;
  /** Existing resume_token for resume */
  resumeToken?: string;
  /** Last known epoch (for resume) */
  lastKnownEpoch?: number;
  /** Default chunk size in bytes (default 256 KiB) */
  chunkSize?: number;
  /** Auto-reconnect attempts (default: unlimited). Set null/undefined for infinite retry. */
  maxReconnectAttempts?: number | null;
  /** Reconnect delay base in ms (default 1000, caps at 20s) */
  reconnectDelayMs?: number;
}
```

| Field | Default | Constructor handling |
|-------|---------|----------------------|
| `serverUrl` | — (required) | Trailing `/` stripped via `.replace(/\/$/, "")` |
| `targetDir` | `"."` | Sent verbatim in `HELLO.target_dir` |
| `sessionId` | `crypto.randomUUID()` | |
| `resumeToken` | `crypto.randomUUID()` | Sent as `X-MFUP-Token` on data/probe POSTs |
| `lastKnownEpoch` | `undefined` | If non-null, seeds `epoch`, which forces `connect()` down the RESUME path |
| `chunkSize` | `262144` (256 KiB) | Effective chunk is `Math.min(chunkSize, limits.max_chunk_bytes)` |
| `maxReconnectAttempts` | `null` (infinite) | |
| `reconnectDelayMs` | `1000` | Exponential base |

`legId` is always freshly generated with `crypto.randomUUID()` in the constructor and again at the top of every `connect()` — each connection leg gets a new id.

#### Public API

| Member | Signature | Description |
|--------|-----------|-------------|
| `constructor` | `(config: MfupSessionConfig)` | Pure initialization; opens nothing |
| `state` | `get (): SessionState` | Current client-side state |
| `id` | `get (): string` | `sessionId` |
| `token` | `get (): string` | `resumeToken` |
| `currentEpoch` | `get (): number` | Last epoch from `HELLO_OK`/`RESUME_OK` |
| `streamingMode` | `get (): boolean \| null` | `null` until probed; then the probe verdict |
| `connect()` | `async (): Promise<void>` | Opens control, probes, opens data channel |
| `uploadHandles(handles)` | `async (handles: FileSystemHandle[]): Promise<void>` | Scan + upload + commit |
| `uploadEntries(entries)` | `async (entries: FileSystemEntry[]): Promise<void>` | Scan + upload + commit |
| `uploadFileList(files)` | `async (files: FileList): Promise<void>` | Scan + upload + commit |
| `uploadFiles(files)` | `async (files: File[]): Promise<void>` | Scan + upload + commit |
| `abort(code?, reason?)` | `(code = "client_cancel", reason = "user cancelled"): void` | Synchronous teardown |
| `sendAction(action)` | `(action: "merge_overwrite" \| "cancel"): void` | Answers a server `ASK` |
| `on(event, fn)` | `<K extends keyof MfupSessionEvents>(event: K, fn: (ev: MfupSessionEvents[K]) => void) => () => void` | Returns an unsubscribe closure |
| `onProgress(fn)` | `(fn: ProgressListener) => () => void` | Delegates to the internal `ProgressTracker` |
| `getProgress()` | `(): ProgressSnapshot` | Synchronous snapshot |
| `getResumeState()` | `(): { sessionId: string; resumeToken: string; epoch: number }` | Persist this to resume in another tab/reload |

All four `upload*` methods share one shape: build an `IngestFilter`, run the matching adapter with `ROOT_NODE_ID` as parent, wrap any non-`MfupError` throw in `ingestError(source, label, err)`, emit it, rethrow, and otherwise tail-call `finalizeScan()`. The `(source, label)` pairs are `("handles","FileSystemHandle[]")`, `("entries","FileSystemEntry[]")`, `("filelist","FileList")`, `("files","File[]")`. The returned promise resolves when `COMMIT_OK` (or a batch-mode commit result) lands.

#### Events

```ts
export interface MfupSessionEvents {
  progress: ProgressSnapshot;
  state: SessionState;
  committed: { files: number; bytes: number };
  ask: void;
  error: MfupError;
  reconnecting: { attempt: number; delay: number; maxAttempts: number | null };
}
```

`state`, `committed`, `ask`, `error`, and `reconnecting` are emitted through `MfupSession.emit`. The `progress` key is declared in the map but the session never calls `emit("progress", …)` — progress flows exclusively through `onProgress()` / `ProgressTracker`. Subscribing via `on("progress", …)` therefore yields nothing; *this is an observation from tracing every `emit` call site, not a documented caveat.*

#### State machine

`SessionState` is defined in `protocol.ts` with eight members:

```ts
export type SessionState =
  | "active" | "paused_by_server" | "waiting_resume" | "committing"
  | "committed" | "aborted" | "expired" | "failed";
```

The session initializes `_state = "active"` at field-declaration time (before `connect()` runs). Every transition:

| To | Site | Trigger |
|----|------|---------|
| `active` | end of `connect()` | control ready + data channel opened; also resets `reconnectCount = 0` |
| `active` | `flow` handler | `FLOW{paused:false}` while state was `paused_by_server` |
| `active` | `finalizeScan()` commit loop | after `COMMIT_RETRY`, before re-opening the data channel |
| `paused_by_server` | `flow` handler | `FLOW{paused:true}` |
| `waiting_resume` | `handleDisconnect()` | control `close` while active/paused, or a `DATA_WRITE_FAILED`/`DATA_HTTP_ERROR` from the data channel |
| `committing` | `finalizeScan()` | immediately after writing `SESSION_END` |
| `committed` | `finalizeScan()` | batch-mode commit parsed from the final POST body |
| `committed` | `commit_ok` handler | `COMMIT_OK` over the control channel |
| `aborted` | `abort()` | caller-initiated |
| `aborted` | `session_abort` handler | server `SESSION_ABORT` |
| `failed` | `handleDisconnect()` | `reconnectCount >= maxReconnectAttempts` |

`"expired"` is never assigned anywhere in the client — grep across `src/` finds it only in the `SessionState` union and in an unrelated HTTP-410 hint string in `errors.ts`. Expiry surfaces instead as a fatal `DATA_HTTP_ERROR` with `status: 410`. Server-side expiry mapping is outside this batch.

**Mapping to server states.** `SessionState` is the client's mirror of the server vocabulary. Two mappings are visible from the wire handling: `COMMIT_RETRY` carries a comment stating the server "reverted to ACTIVE," so the client mirrors it by calling `setState("active")`; and `commit_retry`'s handler notes "Epoch stays the same — only `attach_leg()` (reconnect) bumps epoch," naming a server function. The full server state table is documented in the server section.

#### Connect sequence (`connect()`)

1. Mint a fresh `legId`. `isResume = this.epoch > 0`.
2. Derive the WS URL: `serverUrl.replace(/^http/, "ws") + "/mfup/control"` (so `https://` → `wss://`).
3. Construct `ControlChannel`, wire ~13 event handlers via `wireControlEvents()`, call `open()`.
4. `await control.ready()` — resolves on `HELLO_OK`/`RESUME_OK`, rejects on WS close or `SESSION_ABORT`. On rejection, wrap in `wsConnectFailed(wsUrl, err)`, emit, and throw.
5. Replace `abortCtrl` with a fresh `AbortController` for this leg.
6. **Probe once per session:** if `_streamingMode === null`, `await probeStreaming({...})`. On throw, force `_streamingMode = false` and emit `probeError(err)` — a probe failure is never fatal.
7. Construct the `DataChannel` with `epoch`, the leg's `AbortSignal`, and `streaming: _streamingMode`.
8. Register `data.onError`, guarded by a captured `dc` reference so a stale channel's late error cannot trigger a reconnect on the current one. `DATA_WRITE_FAILED` and `DATA_HTTP_ERROR` escalate to `handleDisconnect(err)`; other codes are emitted only.
9. Fire `data.open()` without awaiting. Both settlement paths re-check `this.data !== openDc` and bail if stale. A resolved non-`ok` `Response` becomes `dataHttpError(url, status, statusText, body)`; a rejection becomes `dataOpenFailed(url, err)` unless it is already an `MfupError`. Both escalate to `handleDisconnect`, but only if `abortCtrl.signal.aborted` is false.
10. `setState("active")`, `reconnectCount = 0`.

#### Scan → queue → pump

`onDiscover(node)` is the `IngestCallback` passed to every adapter:

- Writes a `NODE` frame (`sizeHint` = node size, `mtimeMs` passed through).
- Increments both `scanDone` and `scanEst` by 1 — the "estimate" tracks discovered count exactly, so `scanFrac` is always 1.0 during scan. *Inferred consequence, not stated in source.*
- For files with an `openBody`, creates a `TrackedFile`, indexes it in `trackedFiles`, pushes it onto `fileQueue`, and adds its size to `bodyEstTotal` and to the progress tracker.
- Emits a root `SUMMARY` frame every 50 discovered nodes.
- Calls `kickPump()` — scan and upload run concurrently.
- **Scan backpressure:** if `fileQueue.length >= SCAN_HIGH_WATER` (10,000), awaits a promise stored in `_scanGateResolve`, halting ingestion. The pump releases the gate when the queue drains below `SCAN_LOW_WATER` (5,000). `abort()` also releases it so ingestion cannot hang.

```ts
interface TrackedFile {
  nodeId: number;
  size: bigint;
  acceptedOffset: bigint;
  openBody: (offsetBytes?: number) => AsyncIterable<Uint8Array>;
  status: "pending" | "streaming" | "sent" | "acked" | "rejected";
  /** Set by NACK handler — signals streamFile to abort and requeue. */
  nacked: boolean;
}
```

`kickPump()` is a no-op if `pumping` is already true; otherwise it assigns `pumpDone = pumpFiles()`. `pumpFiles()` loops while `fileQueue.length > 0 && _state === "active"`:

- `await waitIfPaused()` — blocks on `pauseResolve` while `FLOW{paused:true}` is in effect.
- Breaks if `data.failed || data.closed`, leaving recovery to `handleDisconnect`.
- Drops the head file if it is in `rejectedFiles` or `prunedNodes`, marking it `"rejected"` and calling `progress.skipFile()`.
- Shifts the file, releases the scan gate if the queue is now under the low-water mark, sets `activeFile`, and `await streamFile(file)`.
- If `streamFile` throws while the channel is dead, breaks silently; otherwise wraps in `ingestError("stream", "node N", err)`, emits, and rethrows.

#### `streamFile(file)`

1. `status = "streaming"`; write `FILE_OPEN{nodeId, size, mtimeMs: null}` — mtime is deliberately null on open even though the `NODE` frame carried it.
2. `chunks = file.openBody(Number(file.acceptedOffset))` — resume starts the read at the server's accepted offset, not at zero.
3. `offset = file.acceptedOffset`; `maxChunk = Math.min(this.chunkSize, this.limits.max_chunk_bytes)`.
4. For each raw chunk from the iterator, sub-slice into `maxChunk` pieces. Per piece: `waitIfPaused()`; bail to `"rejected"` if a `REJECT_FILE` arrived mid-stream; compute `crc32c(piece)`; write `FILE_CHUNK{nodeId, offset, length, checksumKind: CRC32C, checksum, payload}`; advance `offset`; `await data.drain()` (the backpressure gate); return early if the channel died; and if `file.nacked` is set, clear the flag, set `status = "pending"`, push the file back onto `fileQueue`, and return.
5. On normal completion, write `FILE_CLOSE{nodeId, sizeSent: offset}` and set `status = "sent"`.

The pieces are zero-copy where possible: `pos === 0 && end === raw.length ? raw : raw.subarray(pos, end)`.

#### Commit handshake (`finalizeScan()`)

Seals the scan, emits a final `SUMMARY`, awaits `pumpDone`, then drains: while `_state === "waiting_resume"` or the queue is non-empty, await `_reconnectPromise` (if any), throw if the state reached `failed`/`aborted`, and re-kick the pump. Then an unbounded commit loop:

1. Write `SESSION_END` with the full root summary and `sealed: true`; `setState("committing")`; `await data.close()`.
2. **Batch fast path** — if still `committing` and `data.commitResult` is set (parsed from the final POST's JSON body), transition to `committed`, emit `committed`, and return without ever waiting on the WebSocket.
3. Otherwise await a promise wired to `_commitResolve` / `_commitReject`, unless `_commitRetry` is already true. The pre-check on `_commitRetry` closes a documented race where `handleDisconnect` sets the flag before the promise exists. The awaited promise short-circuits if the state already reached `committed`/`aborted`/`failed`.
4. If `_commitRetry` is false → `COMMIT_OK` arrived; return.
5. Otherwise clear the flag, `data.abort("commit_retry")`, `setState("active")`, construct a *new* `DataChannel` on the same `legId` with `streaming: _streamingMode ?? false`, re-register the stale-guarded `onError`, `open().catch(() => {})`, re-pump the requeued files, drain again, and loop back to step 1.

The `COMMIT_RETRY` handler does the requeueing before `finalizeScan` sees the flag: for each `{node_id, accepted_offset}` in `msg.incomplete`, any tracked file not already `"acked"` has its `acceptedOffset` reset and is pushed back as `"pending"`.

#### Control-message handlers (`wireControlEvents()`) — 13 handlers

| Event | Effect |
|-------|--------|
| `hello_ok` | `epoch = msg.epoch`; `limits = msg.limits`; `progress.setExpiresAt(msg.expires_at)` |
| `resume_ok` | Same plus `progress.setFromRootSummary(...)` and `applyResumeState(msg)` |
| `file_ack` | Advances `acceptedOffset`; adds the positive delta to `bodyDoneTotal` and `progress.setBodyAccepted`; promotes `"sent"` → `"acked"` and calls `progress.acceptFile()` once `acceptedOffset >= size` |
| `nack_chunk` | Rewinds `acceptedOffset` to `msg.expected_offset`. If `"streaming"`, sets `nacked` so `streamFile` self-requeues; if `"sent"`, requeues directly. Always emits `nackChunk(...)` |
| `flow` | Sets `paused`; resolves `pauseResolve` on unpause; toggles `paused_by_server` ⇄ `active` |
| `prune_node` | Adds to `prunedNodes` (blocks future descent via `IngestFilter.shouldDescend`) |
| `reject_file` | Adds to `rejectedFiles`, marks the tracked file `"rejected"`, `progress.skipFile()` |
| `session_abort` | `setState("aborted")`, `data.abort(...)`, emit `sessionAbortedByServer`, reject any pending commit |
| `commit_ok` | `setState("committed")`, emit `committed`, resolve the commit promise |
| `commit_retry` | Requeue `msg.incomplete`, set `_commitRetry = true`, resolve the commit promise |
| `ask` | Re-emits as the session-level `ask` event; the app answers with `sendAction()` |
| `close` | If state is `active` or `paused_by_server`, escalates to `handleDisconnect(unknownError(CONTROL, "control channel closed unexpectedly"))` |
| `error` | Re-emits verbatim |

#### Resume (`applyResumeState`)

Merges `pruned_nodes` and `rejected_files` into the local sets, then walks `msg.files`: sets `acceptedOffset` from `accepted_offset`, maps `status: "rejected"` → local `"rejected"` (+ add to `rejectedFiles`) and `status: "closed"` → `"acked"`. Files the server knows about but the local scan has not re-discovered still contribute to the accepted/skipped tallies. Finally `progress.setFileCounts(acceptedCount, skippedCount)`, where `skippedCount` starts at `msg.pruned_nodes.length`.

#### Reconnect and backoff (`handleDisconnect`)

Guards: returns immediately if state is `committed`, `aborted`, or already `waiting_resume` — the last prevents double-entry when a WS close and a data-channel error fire together. Records `wasCommitting` before transitioning.

Sets `waiting_resume`, aborts the data channel, and creates a single `_reconnectPromise` that stays pending across *all* retries. A source comment explains why: resolving and re-creating it between attempts would let `finalizeScan`'s while-loop spin without an await and freeze the tab.

The retry loop:

- If `maxReconnectAttempts != null && reconnectCount >= maxReconnectAttempts` → `setState("failed")`, emit `sessionReconnectExhausted(reconnectCount, err)`, reject the pending commit, resolve and clear the reconnect promise, return.
- `reconnectCount++`; `delay = Math.min(reconnectDelayMs * 2 ** (reconnectCount - 1), 20_000)`.
- Emit `reconnecting {attempt, delay, maxAttempts}` **before** sleeping so the UI updates immediately, then emit `sessionReconnectFailed(attempt, err)`.
- Sleep via `setTimeout` stored in `_reconnectTimer` so `abort()` can cancel it. After waking, return early if `abort()` ran during the delay.
- `await this.connect()`. On success: `requeuePendingFiles()`, `kickPump()`, resolve and clear the reconnect promise, and if `wasCommitting`, set `_commitRetry = true` and resolve the commit promise so `finalizeScan` re-sends `SESSION_END`. Return.
- On failure: `data.abort("reconnect failed")`, `control.close()`, carry the new error forward, loop.

Backoff is pure exponential with base `reconnectDelayMs`, capped at 20 s — **no jitter**. With defaults the sequence is 1s, 2s, 4s, 8s, 16s, 20s, 20s… `reconnectCount` resets to 0 only on a successful `connect()`.

`requeuePendingFiles()` first replays `_pendingMeta` — `NODE`, `SUMMARY`, and `DIR_CLOSE` frames buffered by `safeWrite` while the channel was down — then requeues every tracked file in `"pending"`, `"streaming"`, or `"sent"` state that is neither rejected nor pruned and whose `acceptedOffset < size`.

#### `safeWrite(frame)`

The single funnel for all data-plane writes. If the channel is missing/closed/failed **and** state is `waiting_resume`, metadata frames (`NODE`, `SUMMARY`, `DIR_CLOSE`) are pushed to `_pendingMeta` for replay and file frames are silently dropped (they are recovered by requeue instead). Otherwise it emits `dataWriteFailed("data channel not available")`. A live write that throws is converted to an emitted error — `MfupError` passes through, anything else is wrapped in `dataWriteFailed("write failed", cause)`. `safeWrite` never throws.

#### `abort(code, reason)`

Fully synchronous: clears `_reconnectTimer`, resolves and clears the reconnect promise (unblocking `finalizeScan`), releases the scan gate, sends `CLIENT_ABORT` on the control channel, attempts a `CLIENT_ABORT` data frame inside a swallowing `try/catch`, closes both channels, aborts `abortCtrl`, sets state `aborted`, and rejects any pending commit with `new Error("aborted")` — a plain `Error`, not an `MfupError`.

---

### 4.3 Wire format (`protocol.ts`)

The client mirror of the server codec: 12 exported values, 26 exported types, encode-only (no decoder — server→client traffic is JSON over the WebSocket).

**Framing:** `[4-byte big-endian length][1-byte tag][payload]`. The length covers tag + payload and excludes itself. `allocFrame` writes `view.setUint32(0, 1 + payloadSize)` and `buf[4] = tag`; every encoder then writes from offset 5. All multi-byte integers are big-endian (no `littleEndian` argument is ever passed).

The eight tags and their payload layouts are tabulated in §2.3.

Sizes and offsets are 64-bit `bigint` throughout the frame types; `FileChunkFrame.length` and `checksum` are plain `number`s.

**Control-message unions.** `ClientControlMsg` = `HelloMsg | ResumeMsg | ClientAbortMsg | ActionMsg`. `ServerControlMsg` = 12 members: `HelloOkMsg`, `ResumeOkMsg`, `FileAckMsg`, `NackChunkMsg`, `FlowMsg`, `PruneNodeMsg`, `RejectFileMsg`, `SessionAbortMsg`, `CommitOkMsg`, `CommitRetryMsg`, `ProbeAckMsg`, `AskMsg`. All are tagged by a `t` field carrying the SCREAMING_SNAKE message name. JSON fields use `snake_case` while the binary frame types use `camelCase`.

`NackChunkMsg.reason` is a closed union: `"bad_checksum" | "bad_offset" | "stale_epoch" | "server_policy"`. `FlowMsg.reason` is `"backpressure" | "maintenance" | "storage_pressure"`. `ResumeFileStatus.status` is `"open" | "closed" | "rejected"`.

**CRC32C.** A 256-entry table is built at module load from the Castagnoli polynomial `0x82F63B78` (reflected). `crc32c(data, initial = 0)` is the standard reflected implementation with pre/post inversion, returning an unsigned 32-bit number. The `initial` parameter allows chained/incremental computation, though the session always calls it per-piece with the default. A source comment records that `HashKind` and SHA-256 were removed because "CRC32C per-chunk is sufficient; SHA-256 was never verified server-side" — **per-chunk CRC32C is the only integrity mechanism in this SDK.**

---

### 4.4 Data channel (`data-channel.ts`)

HTTP transport for binary frames on one leg. No WebRTC. Two modes selected at construction.

```ts
export interface DataChannelOpts {
  baseUrl: string;
  sessionId: string;
  legId: string;
  /** Resume token for data channel auth (sent as X-MFUP-Token header). */
  resumeToken: string;
  /** Current session epoch — used to reject stale requests server-side. */
  epoch: number;
  signal?: AbortSignal;
  /** Enable streaming mode (one long POST with duplex:"half") */
  streaming?: boolean;
  /** Batch flush threshold in bytes (default 2 MiB). Only used in batch mode. */
  flushBytes?: number;
}

export interface DataCommitResult { files: number; bytes: number; }
```

Endpoint: `${baseUrl}/mfup/data/${sessionId}/${legId}`, query `?seq=<n>&final=<0|1>&epoch=<n>`, headers `Content-Type: application/x-mfup` and `X-MFUP-Token: <resumeToken>`. `DEFAULT_FLUSH_BYTES = 2 * 1024 * 1024`.

| Member | Signature | Description |
|--------|-----------|-------------|
| `closed` / `failed` / `bytesSent` | getters | `boolean`, `boolean`, `number` |
| `commitResult` | `DataCommitResult \| null` | Parsed from the final batch POST body |
| `onError(fn)` | `(fn: (err: MfupError) => void) => void` | Single-slot callback (assignment, not a set) |
| `open()` | `(): Promise<Response \| void>` | `Response` in streaming mode, `void` in batch |
| `write(frame)` | `(frame: DataFrame): void` | Always synchronous; encodes then enqueues/buffers |
| `drain()` | `(): Promise<void>` | Backpressure gate |
| `close()` | `(): Promise<void>` | Graceful; final flush in batch mode |
| `abort(reason?)` | `(reason?: string): void` | Errors the stream / discards the buffer |

**Streaming mode.** `_openStreaming()` builds a `ReadableStream<Uint8Array>` with `new ByteLengthQueuingStrategy({ highWaterMark: 4 * 1024 * 1024 })` and fires one `fetch(url + "?seq=0&final=1&epoch=…", { method: "POST", body: stream, duplex: "half", signal })`. The whole leg is a single `POST` — hence the hard-coded `seq=0&final=1`. `write()` calls `controller.enqueue(encoded)` and adds to `bytesSent`. Backpressure: `drain()` inspects `controller.desiredSize`; if `<= 0`, it parks on a promise stored in `_drainResolve`, which the stream's `pull()` callback resolves once the network consumes enough bytes. `close()` calls `controller.close()`; `abort()` calls `controller.error(new Error(reason ?? "aborted"))`. Both first resolve any parked `drain()` so the pump loop cannot deadlock.

**Batch mode.** `open()` returns a promise that only settles when `close()`/`abort()` fires `_doneResolve`. `write()` appends the encoded frame to `_batchBuffer` and tracks `_batchBufferBytes`. `drain()` flushes when the buffer reaches `flushBytes`. `_flushBatch(final)` swaps out the buffer, concatenates (`_concatChunks` returns the single chunk unchanged when `length === 1`), assigns a monotonic `seq`, and appends the POST to `_flushChain` — a promise chain that guarantees **never two POSTs in flight**. Non-`ok` responses set `_failed = true` and report `dataHttpError`. On the `final` POST it parses the JSON body: `json.commit` populates `commitResult`, `json.error` produces a `dataHttpError(url, status, "commit_error", json.error)`. A parse failure is swallowed — a source comment notes "commit_ok via WS is the fallback." Network throws set `_failed` and report `dataWriteFailed`.

**What happens when the channel drops.** The channel itself only sets `_failed` and invokes `onError`. Recovery lives entirely in the session: `DATA_WRITE_FAILED` and `DATA_HTTP_ERROR` route to `handleDisconnect`, which aborts the channel, enters `waiting_resume`, buffers subsequent metadata frames, and on reconnect constructs a brand-new `DataChannel` with the new `legId` and bumped `epoch`. In-flight file bodies are re-streamed from the server-acknowledged offset. There is no in-channel retry — a `DataChannel` instance is single-use.

---

### 4.5 Ingestion (`ingestion.ts`)

Four adapters over the four browser APIs for getting at local files, each normalizing to one callback shape.

```ts
export interface DiscoveredNode {
  nodeId: number;
  parentId: number;
  kind: NodeKind;
  /** For files — exact size. For dirs — null. */
  size: bigint | null;
  mtimeMs: bigint | null;
  name: string;
  /** For files: async iterable of body chunks; accepts a resume offset. Null for dirs. */
  openBody: ((offsetBytes?: number) => AsyncIterable<Uint8Array>) | null;
}

export type IngestCallback = (node: DiscoveredNode) => void | Promise<void>;

export interface IngestFilter {
  shouldDescend?(nodeId: number, name: string): boolean;
  shouldInclude?(nodeId: number, name: string): boolean;
}
```

All four adapters share the signature `(source, rootParentId: number, ids: NodeIdAllocator, cb: IngestCallback, filter?: IngestFilter) => Promise<void>` and `await` the callback, so a slow consumer (the scan gate) throttles the walk.

| Adapter | Input | Traversal |
|---------|-------|-----------|
| `ingestFromHandles` | `FileSystemHandle[]` | Recursive `walkHandle`; uses `dirHandle.entries()` (cast to `any`) and `fileHandle.getFile()` |
| `ingestFromEntries` | `FileSystemEntry[]` | Recursive `walkEntry`; `readAllEntries` + promisified `entry.file()` |
| `ingestFromFileList` | `FileList` | Two-pass: builds a virtual tree by splitting `webkitRelativePath` on `/` and allocating ids, then DFS-emits |
| `ingestFromFiles` | `File[]` | Flat — every file is a direct child of `rootParentId`, no directories |

`readAllEntries(dir)` loops `reader.readEntries()` until it yields an empty batch, because the API is not guaranteed to return everything in one call.

**Reading and chunking.** All four adapters produce bodies from one primitive:

```ts
const READ_SLICE_SIZE = 65536; // 64 KiB slices

async function* blobChunks(blob: Blob, offset = 0): AsyncGenerator<Uint8Array> {
  let pos = offset;
  const size = blob.size;
  while (pos < size) {
    const end = Math.min(pos + READ_SLICE_SIZE, size);
    const buf = await blob.slice(pos, end).arrayBuffer();
    yield new Uint8Array(buf);
    pos = end;
  }
}
```

The `offset` parameter is what makes mid-file resume work — `streamFile` passes `Number(file.acceptedOffset)` and the generator seeks by slicing. A source comment records the rationale for the async generator over `ReadableStream`: `Blob.stream()` produces "Error in input stream" on multi-chunk files in Firefox, and `ReadableStream({pull})` produced "The operation was aborted"; `slice()` + `arrayBuffer()` is "the most universally reliable path."

Because `READ_SLICE_SIZE` (64 KiB) is smaller than the default `chunkSize` (256 KiB), the session's sub-slicing loop normally runs exactly once per read and each 64 KiB slice becomes one `FILE_CHUNK` frame. *This is an inference from the two constants, not stated anywhere.* The sub-slicing exists to honor a server `max_chunk_bytes` below the read slice size.

**Hashing / integrity.** Ingestion computes no hashes. The only integrity check is the `crc32c(piece)` the session computes per outgoing chunk. `mtimeMs` comes from `file.lastModified` and `size` from `file.size`, both widened to `bigint`.

**`NodeIdAllocator`.** `alloc(): number` returns a monotonically increasing id starting at `1` (0 is reserved for `ROOT_NODE_ID`); `get current(): number` returns the last allocated id. One instance per session, shared across every adapter call so ids stay unique across a resumed scan.

A behavioral detail worth noting: `walkHandle` and `walkEntry` call `ids.alloc()` *before* consulting the filter, so pruned/excluded nodes still consume an id. In `ingestFromFiles` the same holds — ids are allocated then possibly skipped.

---

### 4.6 Control channel (`control.ts`)

A thin typed WebSocket wrapper. Sends JSON, dispatches server messages onto a typed event bus, and exposes a `ready()` promise for the handshake.

```ts
export interface ControlChannelOpts {
  url: string;            // ws(s)://host/mfup/control
  sessionId: string;
  resumeToken: string;
  legId: string;
  targetDir: string;
  /** If set, we send RESUME instead of HELLO */
  lastKnownEpoch?: number | null;
}
```

| Member | Signature | Description |
|--------|-----------|-------------|
| `epoch` / `limits` / `expiresAt` | getters | `number`, `ServerLimits \| null`, `string \| null` |
| `ready()` | `(): Promise<void>` | Resolves on `HELLO_OK`/`RESUME_OK`; rejects on close or `SESSION_ABORT`. Settled once (`_readySettled`) |
| `open()` | `(): void` | Constructs the `WebSocket` and installs 4 handlers |
| `close(code = 1000, reason = "")` | `(code?: number, reason?: string): void` | |
| `send(msg)` | `(msg: ClientControlMsg): void` | `JSON.stringify` via optional-chained `ws?.send` |
| `sendAbort(code, reason)` | `(code: string, reason: string): void` | Sends `{t:"CLIENT_ABORT", code, reason}` |
| `sendAction(action)` | `(action: "merge_overwrite" \| "cancel"): void` | Sends `{t:"ACTION", action}` |
| `on(event, fn)` | `<K extends keyof ControlEventMap>(...) => () => void` | Returns unsubscribe |

**Handshake.** `onopen` branches on `lastKnownEpoch != null`: non-null sends `RESUME {session_id, resume_token, leg_id, last_known_epoch}`, null sends `HELLO {v: PROTOCOL_VERSION, session_id, resume_token, leg_id, target_dir}`. Note `target_dir` is only carried on `HELLO`.

**Outbound message types:** 4 (`HELLO`, `RESUME`, `CLIENT_ABORT`, `ACTION`).

**Inbound dispatch:** 12 cases, one per `ServerControlMsg` member, mapped to lowercase event names — `hello_ok`, `resume_ok`, `file_ack`, `nack_chunk`, `flow`, `prune_node`, `reject_file`, `session_abort`, `commit_ok`, `commit_retry`, `probe_ack`, `ask`. `HELLO_OK` caches `epoch`/`limits`/`expiresAt` and settles ready; `RESUME_OK` caches `epoch`/`expiresAt` (not `limits` — `ResumeOkMsg` carries none) and settles ready; `SESSION_ABORT` emits both `session_abort` and an `error` carrying `wsHandshakeFailed(code, reason)`, then rejects ready.

`ControlEventMap` adds two synthetic events beyond the 12 wire messages: `error: MfupError` and `close: { code: number; reason: string }`. A malformed frame emits `wsMessageParse(raw.slice(0,200), err)`; `ws.onerror` emits `wsConnectFailed(url)`; `ws.onclose` always emits `close`, additionally emits `wsClosedUnexpected(code, reason)` when the code is not `1000`, and always settles `ready` with a rejection.

---

### 4.7 Transport probe (`probe.ts`)

```ts
export interface ProbeResult { streaming: boolean; latencyMs: number; }

export async function probeStreaming(opts: {
  baseUrl: string; sessionId: string; resumeToken: string;
  control: ControlChannel; signal?: AbortSignal; timeoutMs?: number;
}): Promise<ProbeResult>
```

Empirically determines whether the browser *and* the server/proxy path really support `duplex: "half"` request streaming. Defaults: `timeoutMs = 1500`, probe chunk size 1024 bytes. Endpoint `${baseUrl}/mfup/probe/${sessionId}` with header `X-MFUP-Token`.

Three gates, all of which must pass:

1. **Static feature detection.** Constructs a `Request` with a `ReadableStream` body *without* `duplex`, then *with* it. Streaming is accepted only if the first throws (Chrome's "duplex member must be specified") and the second succeeds. Firefox — where the no-duplex construction may not throw — is rejected here without any network traffic.
2. **Fetch construction.** The real streaming `fetch` is wrapped in `try/catch`; a synchronous throw returns `{streaming: false, latencyMs: 0}`. So does a throwing `controller.enqueue(chunkA)`.
3. **Server round-trip.** After enqueuing a 1 KB zero-filled chunk A, races a `PROBE_ACK` on the control channel against `timeoutMs`. The verdict is `msg.first_chunk_bytes ?? 0 >= 1024` — a comment explains this catches browsers that stringify the body ("[object ReadableStream]" = 23 bytes) instead of streaming it. Timeout → `false`.

Afterward it enqueues chunk B, closes the stream, and awaits (and ignores) the fetch response, swallowing errors in both. `latencyMs` is `Math.round(performance.now() - start)` when streaming succeeded, and `0` otherwise — it is never populated on the negative path.

The session probes exactly once per session (`_streamingMode === null` guard in `connect()`), caches the verdict for the session lifetime including across reconnects, and treats any thrown probe as `streaming: false` plus a non-fatal `probeError`.

---

### 4.8 Error taxonomy (`errors.ts`)

```ts
export class MfupError extends Error {
  readonly code: MfupErrorCode;
  readonly layer: MfupErrorLayer;
  readonly action: string;     // human-readable remediation hint
  readonly fatal: boolean;
  readonly detail: Record<string, unknown>;
  readonly timestamp: string;  // ISO 8601, set at construction
  toJSON(): Record<string, unknown>;   // flat; unwraps an Error cause to {name,message,stack}
  summary(): string;                   // `[${layer}/${code}] ${message}`
}
```

`name` is set to `"MfupError"`. The native `cause` option is used, so `err.cause` chains.

`MfupErrorLayer` has 5 members: `CONTROL = "control"`, `DATA = "data"`, `SESSION = "session"`, `INGEST = "ingest"`, `PROTOCOL = "protocol"`.

**Retryability is expressed by the `fatal` flag** — there is no separate `retryable` field. `fatal: false` means the session may keep going (usually via reconnect); `fatal: true` means the upload cannot continue as-is. Note that `fatal` is advisory metadata: `handleDisconnect` does not consult it, keying instead on the specific codes `DATA_WRITE_FAILED` and `DATA_HTTP_ERROR`.

`MfupErrorCode` declares 22 codes. Sixteen are constructed by 13 factory functions; **six are declared but never constructed anywhere in the client** (verified by grep for `MfupErrorCode.<NAME>`): `WS_SEND_FAILED`, `DATA_STREAM_ERROR`, `SESSION_ABORT_FAILED`, `SESSION_COMMIT_FAILED`, `SESSION_ENDED_BAD_STATE`, `INGEST_READ_ERROR`. They appear to be reserved for future use, or matched against server-side codes — *inference.*

| Code | Layer | Factory | `fatal` | Raised when |
|------|-------|---------|---------|-------------|
| `WS_CONNECT_FAILED` | control | `wsConnectFailed(url, cause?)` | `false` | `ws.onerror`, or `control.ready()` rejects in `connect()` |
| `WS_HANDSHAKE_FAILED` | control | `wsHandshakeFailed(code, reason)` | **`true`** | `SESSION_ABORT` received. `action` branches on `bad_version` / `conflict` / `auth_failed` |
| `WS_CLOSED_UNEXPECTED` | control | `wsClosedUnexpected(wsCode, wsReason)` | `false` | WS close with a code other than `1000`. `action` branches on `1006` (network) / `1008` (policy) |
| `WS_MESSAGE_PARSE` | control | `wsMessageParse(rawData, cause)` | `false` | `JSON.parse` of a control frame throws; stores the first 200 chars |
| `WS_SEND_FAILED` | control | *(none)* | — | Never constructed |
| `DATA_OPEN_FAILED` | data | `dataOpenFailed(url, cause)` | **`true`** | Streaming `fetch` throws, or `data.open()` rejects. Sniffs the cause message for `duplex`/`ReadableStream`/`body` and sets `detail.isDuplexIssue`, switching `action` to the Chrome/Edge 105+ / HTTP-2-TLS hint |
| `DATA_HTTP_ERROR` | data | `dataHttpError(url, status, statusText, body?)` | `status === 410` | Non-`ok` data POST response, or a `json.error` in the final batch body. `action` branches on 409 / 410 / 503. Body truncated to 500 chars |
| `DATA_WRITE_FAILED` | data | `dataWriteFailed(reason, cause?)` | `false` | Channel unavailable in `safeWrite`, `enqueue` throws, `write()` before `open()`, or a batch POST throws |
| `DATA_STREAM_ERROR` | data | *(none)* | — | Never constructed |
| `DATA_CHANNEL_CLOSED` | data | `dataChannelClosed()` | `false` | `DataChannel.write()` called after `close()`/`abort()` — the one factory that is also **thrown**, not just reported |
| `SESSION_ABORTED_BY_SERVER` | session | `sessionAbortedByServer(code, reason)` | **`true`** | `SESSION_ABORT` handled at the session layer |
| `SESSION_ABORT_FAILED` | session | *(none)* | — | Never constructed |
| `SESSION_RECONNECT_FAILED` | session | `sessionReconnectFailed(attempt, cause)` | `false` | Emitted on every reconnect attempt, before the backoff sleep |
| `SESSION_RECONNECT_EXHAUSTED` | session | `sessionReconnectExhausted(attempts, lastCause?)` | **`true`** | `reconnectCount >= maxReconnectAttempts`; also rejects the pending commit |
| `SESSION_COMMIT_FAILED` | session | *(none)* | — | Never constructed |
| `SESSION_ENDED_BAD_STATE` | session | *(none)* | — | Never constructed |
| `INGEST_HANDLE_ERROR` | ingest | `ingestError(source, name, cause)` | `false` | Any adapter throw, and any non-channel-death throw from `streamFile` |
| `INGEST_READ_ERROR` | ingest | *(none)* | — | Never constructed |
| `NACK_BAD_CHECKSUM` | protocol | `nackChunk(...)` w/ `bad_checksum` | `false` | Server CRC mismatch; chunk is resent |
| `NACK_BAD_OFFSET` | protocol | `nackChunk(...)` w/ `bad_offset` | `false` | Offset disagreement; stream restarts from `expected_offset` |
| `NACK_STALE_EPOCH` | protocol | `nackChunk(...)` w/ `stale_epoch` | **`true`** | Session resumed elsewhere — "this tab's data is stale" |
| `NACK_SERVER_POLICY` | protocol | `nackChunk(...)` w/ `server_policy` | `false` | Server-side policy rejection |
| `UNKNOWN` | *varies* | `unknownError(layer, message, cause?)` | `false` | Control channel closed unexpectedly (session layer); also the fallback when `nackChunk` sees an unmapped reason |
| `UNKNOWN` | data | `probeError(cause)` | `false` | `probeStreaming` throws. `action`: "Will use batch upload mode. This is normal for Firefox/Safari." |

Five codes are fatal: `WS_HANDSHAKE_FAILED`, `DATA_OPEN_FAILED`, `SESSION_ABORTED_BY_SERVER`, `SESSION_RECONNECT_EXHAUSTED`, `NACK_STALE_EPOCH`; plus `DATA_HTTP_ERROR` conditionally on HTTP 410. Note that `probeError` reuses `MfupErrorCode.UNKNOWN` rather than a dedicated code, so it is only distinguishable from `unknownError` by its message and its `layer: DATA`.

---

### 4.9 Progress (`progress.ts`)

```ts
export interface ProgressSnapshot {
  scanDoneUnits: bigint;    // nodes discovered
  scanEstUnits: bigint;     // estimated total (grows during scan)
  bodyDoneBytes: bigint;    // bytes accepted by the server (from FILE_ACK)
  bodyEstBytes: bigint;     // estimated total from size hints
  acceptedFiles: number;
  skippedFiles: number;     // pruned + rejected
  scanSealed: boolean;
  expiresAt: string | null; // session resume deadline
  fraction: number | null;  // blended 0–1, monotonic; null before any data
}

export type ProgressListener = (snap: ProgressSnapshot) => void;
```

| Method | Signature | Caller |
|--------|-----------|--------|
| `on(fn)` | `(fn: ProgressListener) => () => void` | `MfupSession.onProgress` |
| `snapshot()` | `(): ProgressSnapshot` | `MfupSession.getProgress` |
| `updateScan(done, est, sealed)` | `(bigint, bigint, boolean) => void` | `emitSummary()` |
| `addBodyEstimate(bytes)` | `(bigint) => void` | `onDiscover` per file |
| `setBodyAccepted(bytes)` | `(bigint) => void` | `file_ack` handler; ignores non-increasing values |
| `acceptFile()` | `() => void` | `file_ack` when a file reaches full size |
| `skipFile()` | `() => void` | `reject_file` handler and the pump's prune/reject paths |
| `setFileCounts(accepted, skipped)` | `(number, number) => void` | `applyResumeState` |
| `setExpiresAt(v)` | `(string \| null) => void` | `hello_ok` / `resume_ok` |
| `setFromRootSummary(s)` | `(RootSummary-shaped) => void` | `resume_ok` |
| `advanceBody(nodeId, acceptedOffset)` | `(number, bigint) => void` | **Empty body — a no-op stub.** Its own comment says the session must compute the delta; nothing calls it |

**Emission cadence is change-driven, not timed.** Every mutator calls the private `notify()`, which builds a fresh snapshot and invokes every listener synchronously. There is no throttling, debouncing, or `requestAnimationFrame` batching. The practical rates: `updateScan` fires once per 50 discovered nodes plus once at seal (`emitSummary` is called on that cadence in `onDiscover`); `addBodyEstimate` fires once per discovered file; `setBodyAccepted` fires once per `FILE_ACK` with a positive delta; `acceptFile`/`skipFile` once per file. A consumer rendering directly from this callback should expect bursty, unthrottled calls during scan — *this is an inference about consumer impact, not a source claim.*

**Fraction computation.** Returns `null` while both estimates are zero. Otherwise `raw = 0.1 * scanFrac + 0.9 * bodyFrac`, a fixed 10/90 blend. Each sub-fraction is computed in bigint with 4-decimal precision (`Number(done * 10000n / est) / 10000`) to avoid float overflow on large byte counts. Monotonicity is enforced by `Math.max(this._lastFraction, Math.min(1, raw))` with `_lastFraction` persisted — the fraction never decreases even when a growing scan estimate would otherwise pull it backwards, and never exceeds 1. `_lastFraction` is a private field with no reset, so it survives reconnects.

Because `onDiscover` increments `scanDone` and `scanEst` together, `scanFrac` is effectively pinned at 1.0 during a live scan and the fraction is dominated by the 90% body term. *Inference from the two increments in `session.ts`.*

---

### 4.10 Build and packaging

`client/package.json`:

| Field | Value |
|-------|-------|
| `name` / `version` | `@mfup/client` / `0.1.0` |
| `type` | `module` — ESM only, no CJS build or dual export map |
| `main` | `dist/index.js` |
| `types` | `dist/index.d.ts` |
| `files` | `["dist"]` |
| `scripts.build` | `tsc` |
| `scripts.check` | `tsc --noEmit` |
| `devDependencies` | `typescript: ^5.5.0` |
| `license` | MIT |

**Zero runtime dependencies** and no `dependencies` block at all — the SDK relies solely on browser globals: `fetch`, `ReadableStream`, `ByteLengthQueuingStrategy`, `WebSocket`, `AbortController`, `TextEncoder`, `DataView`, `crypto.randomUUID`, `performance.now`, `Blob`, `File`, and the File System Access / `webkitGetAsEntry` APIs. There is no `exports` map, no bundler, and no test script.

`client/tsconfig.json`:

| Option | Value | Consequence |
|--------|-------|-------------|
| `target` / `module` | `ES2022` / `ES2022` | Native `bigint`, top-level classes, `Error` `cause` |
| `moduleResolution` | `bundler` | Though sources still write explicit `.js` extensions |
| `lib` | `["ES2022", "DOM", "DOM.Iterable"]` | Browser-only; no Node types |
| `rootDir` / `outDir` | `src` / `dist` | Matches `main`/`types` |
| `declaration` / `declarationMap` / `sourceMap` | all `true` | Ships `.d.ts`, `.d.ts.map`, `.js.map` |
| `strict` | `true` | |
| `isolatedModules` | `true` | Forces `export type` for all type re-exports in `index.ts`; also determines how the five `const enum`s are emitted |
| `skipLibCheck`, `forceConsistentCasingInFileNames`, `esModuleInterop`, `resolveJsonModule` | `true` | |
| `include` / `exclude` | `["src"]` / `["node_modules", "dist"]` | |

Three `@ts-expect-error` suppressions exist for `duplex: "half"`, which is absent from the TypeScript DOM lib types — two in `probe.ts` (lines 47 and 73) and one in `data-channel.ts` (line 127). One `as any` cast covers `FileSystemDirectoryHandle.entries()` in `ingestion.ts`, and another covers `webkitRelativePath` on `File`.
---
## 5. Example Application

A Vite-based browser demo living at `/workspace/example`. It ships two independent pages built from a shared client SDK: a **main demo** (`index.html` + `src/demo.ts`) that exercises the full session lifecycle with progress, error, and conflict UI, and a **comparison page** (`compare.html` + `src/compare.ts`) that races MFUP against a sequential multipart-POST baseline. Both pages are plain TypeScript modules with no framework — all styling is inline `<style>` in the HTML, and all DOM wiring is imperative `getElementById` + `addEventListener`. The demo consumes the client SDK's TypeScript **sources** directly via a Vite path alias rather than a built package.

| Path | Role |
|------|------|
| `example/index.html` | Main demo page — markup + all CSS, loads `/src/demo.ts` as a module |
| `example/src/demo.ts` | Main demo logic; canonical `MfupSession` usage example |
| `example/compare.html` | Comparison page — two-lane "race" markup, loads `/src/compare.ts` |
| `example/src/compare.ts` | MFUP lane + traditional-POST lane, timers, flattening helpers |
| `example/vite.config.ts` | Dev server, backend proxies, `@mfup/client` alias, dual-entry build |
| `example/package.json` | `mfup-demo`, private, ESM; only `vite` + `typescript` as devDeps |
| `example/tsconfig.json` | ES2022 / ESNext / `bundler` resolution, strict; includes client sources |

---

### 5.1 Main demo page (`index.html` + `src/demo.ts`)

#### User-facing workflow

The page presents a single dropzone ("Drag & drop files / folders here, or **browse**"). Dropping files or folders, or picking files via the browse control, immediately creates a session and starts uploading — there is no separate "Start" button. Once running, a progress panel appears showing:

| UI element | DOM id | Source |
|------------|--------|--------|
| State badge | `state-badge` | `SessionState`, uppercased with `_` → space |
| Session id (first 8 chars) | `session-id-label` | `session.id.slice(0, 8)` |
| Progress bar | `bar` | `ProgressSnapshot.fraction × 100` |
| Transferred | `stat-bytes` | `snap.bodyDoneBytes` |
| Files Accepted | `stat-files` | `snap.acceptedFiles` |
| Scanned | `stat-scanned` | `snap.scanDoneUnits` |
| Skipped | `stat-skipped` | `snap.skippedFiles` |

Five buttons exist: **browse** (`browse-btn`), **Abort** (`btn-abort`, disabled until a session is live), **Reset** (`btn-reset`), and **Overwrite** / **Cancel** inside the conflict modal. Below the panel are two collapsible regions: an **Errors** list of detail cards (code, layer, fatal/recoverable, message, suggested action, JSON detail, expandable stack trace and cause chain) and a timestamped **log** stream with four severity colors (`info`/`ok`/`warn`/`err`).

When the server signals a target-directory conflict, a modal overlay ("File Conflict — Files already exist in the target directory. Overwrite existing files?") blocks with Overwrite / Cancel. The badge CSS defines seven states: `active`, `committing`, `committed`, `failed`, `aborted`, `waiting_resume`, `paused_by_server`.

#### Input acquisition strategy

The file header documents a deliberate three-tier drop priority, chosen because Firefox invalidates `DataTransferItem.getAsFile()` blob data after the handler returns:

1. `getAsFileSystemHandle()` — Chrome/Edge, persistent `FileSystemHandle`s. Called synchronously across all items; if any item lacks the method the whole tier is abandoned.
2. `webkitGetAsEntry()` — Firefox/Safari, persistent `FileSystemEntry` objects supporting lazy streaming and directory traversal.
3. `e.dataTransfer.files` — last-resort fallback, logged as a `warn`.

Both tier-1 and tier-2 calls must happen in the same tick as the drop event; the code does so and only then awaits `Promise.all(handles)`. Two hidden inputs back the picker: `file-input` (`multiple`) and `folder-input` (`webkitdirectory`).

> **Observation from the code:** `folderInput.click()` is never called anywhere in `demo.ts` — `browse-btn` and the dropzone click both trigger `fileInput.click()`. The `folderInput` `change` listener (the only caller of `session.uploadFileList`) is therefore unreachable through the current markup, even though the input element exists in `index.html`.

#### Canonical call sequence

`startUpload(mode, source)` is the single entry point for every input path. Its SDK usage, in order:

```ts
import { MfupSession, MfupError, type ProgressSnapshot, type SessionState }
  from "@mfup/client/index.js";

const serverUrl = `${location.protocol}//${location.host}`;

// 1. Construct
session = new MfupSession({ serverUrl, targetDir: ".", chunkSize: 256 * 1024 });
session.id;                          // read immediately, before connect()

// 2. Subscribe BEFORE connecting
session.onProgress(renderProgress);  // ProgressSnapshot stream
session.on("state",        (s)   => renderState(s));
session.on("ask",          ()    => showConflictModal());
session.on("committed",    async (ev) => { /* ev.files, ev.bytes */ });
session.on("reconnecting", (ev)  => { /* ev.attempt, ev.maxAttempts, ev.delay */ });
session.on("error",        (err) => renderError(err));   // err: MfupError

// 3. Connect (probes transport)
await session.connect();
session.streamingMode;   // true -> "streaming (duplex:half)", false -> "batch (sequential POST)"
session.currentEpoch;    // logged after connect

// 4. Upload — one of four adapters, chosen by input source
await session.uploadHandles(handles);    // FileSystemHandle[]
await session.uploadEntries(entries);    // FileSystemEntry[]
await session.uploadFileList(fileList);  // FileList  (webkitdirectory input)
await session.uploadFiles(files);        // File[]
```

Interactive controls map to two further methods:

```ts
session.sendAction("merge_overwrite" | "cancel");  // from the conflict modal
session.abort();                                   // Abort button
session.state;                                     // read in catch to suppress abort-as-error
```

| SDK surface used | Kind | Purpose in the demo |
|------------------|------|---------------------|
| `new MfupSession(config)` | constructor | `serverUrl`, `targetDir: "."`, `chunkSize: 262144` |
| `session.id` | getter | Session label, publish URL |
| `session.state` | getter | Distinguish user abort from fatal error |
| `session.currentEpoch` | getter | Logged after connect |
| `session.streamingMode` | getter | Logged as streaming vs batch mode |
| `session.onProgress(fn)` | method | Drives bar + four stat tiles |
| `session.on(event, fn)` | method | 5 events: `state`, `ask`, `committed`, `reconnecting`, `error` |
| `session.connect()` | async | Opens control channel, probes transport |
| `session.uploadHandles/Entries/FileList/Files` | async | 4 ingestion adapters |
| `session.sendAction(a)` | method | Conflict resolution |
| `session.abort()` | method | User-initiated abort |

**Publish is not an SDK call.** After `committed` fires, the demo issues a raw request itself:

```ts
const resp = await fetch(`${serverUrl}/mfup/sessions/${session.id}/publish`, { method: "POST" });
const data = await resp.json();   // { published: string[] }
```

The commit → publish handoff is therefore application responsibility, not something `MfupSession` performs.

#### Commit / conflict ordering

The `committed` handler is `async` and awaits `conflictPromise` before publishing. The server can reach COMMITTED while the conflict modal is still open; the demo holds the publish call until the user answers, and skips publish entirely if the answer was `cancel` or if `cancelled` was already set.

#### Error rendering

`renderError(err: MfupError)` reads `code`, `layer`, `fatal`, `message`, `action`, `detail`, `timestamp`, `stack`, and `cause`. `formatCauseChain()` walks `err.cause` up to a depth of 5, prefixing each level with `Caused by:`. Non-fatal errors get a `.warning` class; fatal errors force the badge to `failed` unless the session is already `aborted`. Two caps bound DOM growth:

| Constant | Value | Effect |
|----------|-------|--------|
| `MAX_LOG_ENTRIES` | 200 | Oldest log lines trimmed from the front |
| `MAX_ERROR_CARDS` | 50 | Cards prepended newest-first, oldest trimmed |

#### Reset behavior

`btn-reset` clears all UI state and sets `session = null`. It does **not** call `session.abort()` first — since `startUpload` guards re-entry with `if (session) return`, pressing Reset mid-upload clears the guard and leaves the previous session running without a UI. (Read directly from the handler; no abort call is present.)

---

### 5.2 Comparison page (`compare.html` + `src/compare.ts`)

#### User-facing workflow

A wider dropzone ("Drag & drop files or folders here") sits above a hidden two-column race grid. On drop or file-pick, both lanes start **simultaneously** and the grid becomes visible:

- **Left lane — "MFUP/2 Protocol"** (blue): live timer, progress bar, `Files:` and `Sent:` counters.
- **Right lane — "Traditional (file-by-file)"** (orange): same widgets.

Each lane carries a badge cycling through `Waiting` → `Uploading` → `Done` / `Failed` (`setBadge`). Timers render at 0.1 s precision and turn green on completion. A shared log below both lanes prefixes entries with `[MFUP]` or `[Traditional]`. A **Reset** button (disabled while running) clears both lanes. The same File Conflict modal from the main page is present and is wired only to the MFUP lane.

#### What is compared against what

| | MFUP lane | Traditional lane |
|---|---|---|
| Function | `runMfupLane(mode, source, timer)` | `runTrivialLane(flatFiles, timer)` |
| Transport | `MfupSession` — protocol streaming/batch | `fetch(POST)` with `FormData`, one file per request, strictly sequential `await` in a `for` loop |
| Endpoint | `/mfup/*` → `backend:8070` | `/trivial/upload` → `trivial:8071` (prefix stripped) |
| Target | `targetDir: "mfup-target"` | server-side `UPLOAD_DIR`, path passed as a `path` form field |
| Chunking | `chunkSize: 256 * 1024` | none — whole file per request |
| Directory structure | preserved by protocol ingestion | reconstructed server-side from the `path` field |
| Failure handling | throws → lane badge `error` | logs and `continue`s to the next file; lane can still finish `done` |

#### Measurement mechanism

`LaneTimer` wraps `performance.now()`:

| Method | Behavior |
|--------|----------|
| `start()` | Records `startMs`, begins a `setInterval` re-render every 100 ms |
| `stop()` | Records `endMs`, clears the interval, renders final value |
| `elapsed()` | `(endMs \|\| performance.now()) - startMs` |
| `reset()` | Clears interval, resets display to `0.0 sec` |

`startRace(mode, source, flatFiles)` launches both lanes through `Promise.allSettled([...])`, so neither lane's failure aborts the other, and both timers run concurrently.

Metrics collected per lane: **elapsed wall-clock time** (the headline number), **file count**, and **bytes sent**. MFUP's counters come from `ProgressSnapshot` (`acceptedFiles`, `bodyDoneBytes`, `fraction`); the traditional lane's counters are computed client-side by accumulating `file.size` after each successful response, with percentage against a precomputed `totalBytes` reduce.

#### Methodological caveats visible in the code

These are read directly from `compare.ts`; the significance ranking is the reviewer's assessment. They matter because `docs/MFUP_RU.md` reports a headline **12× speedup (1.4 s vs 17.2 s on 188 files / 1.0 MB)** produced by exactly this harness.

1. **The lanes contend with each other.** `Promise.allSettled` runs both concurrently over the same uplink, browser connection pool, and CPU. Neither timer measures an isolated upload.
2. **The traditional lane's directory scan is untimed; MFUP's is timed.** `flattenHandles()` / `flattenEntry()` fully traverse directories and materialize every `File` object in the *drop handler*, before `startRace` is called. The MFUP lane receives raw handles/entries and performs its own traversal *inside* the timed region. This asymmetry disadvantages MFUP.
3. **Publish time is excluded from MFUP** but has no analogue in the baseline. The MFUP timer stops when `uploadX()` resolves; the `POST .../publish` is fired from an un-awaited async listener and is not counted.
4. **Two different servers, not two different protocols.** `backend:8070` is the FastAPI MFUP app; `trivial:8071` is a separate FastAPI service. The measurement conflates protocol design with server implementation.
5. **The baseline is deliberately worst-case.** The traditional lane issues exactly one in-flight request at a time (`await` inside `for`). Real-world uploaders typically parallelize 4–6 requests. The UI labels this honestly as "file-by-file".
6. **Single sample, no warm-up, no repetition, no median.** One run per drop; nothing is discarded or averaged.
7. **Different byte accounting.** MFUP reports protocol-acknowledged `bodyDoneBytes`; the baseline reports optimistic client-side `file.size` sums.
8. **Partial success reads as success.** A traditional-lane file that fails is logged and skipped; the lane still stops the timer and shows `Done`.
9. **Only wall-clock is compared.** No integrity verification, resumability, or connection-count comparison is instrumented.
10. **Flat fallback path.** In `mode: "files"`, `flat` is built as `{ file, path: f.name }` with no directory prefix, so relative structure is dropped for the baseline on that path.

The qualitative conclusion — that per-file round-trips dominate at high file counts — is well supported by the design. The specific multiplier should be read as an illustration produced by this harness, not a controlled benchmark.

#### Additional code observations

- `compare.ts` supports **three** modes (`handles`, `entries`, `files`) — there is no `filelist` mode, unlike `demo.ts`.
- `folderInput` is declared at the top of `compare.ts` but never referenced again: no `change` listener, no `.click()`, and `btnReset` clears only `fileInput.value`. The `folder-input` element in `compare.html` is therefore inert.
- The MFUP lane registers no `error` or `state` listener — failures surface only through the `try/catch` around `connect()`/`uploadX()`.

---

### 5.3 Vite configuration (`vite.config.ts`)

```ts
export default defineConfig({
  root: ".",
  publicDir: "public",
  resolve: { alias: { "@mfup/client": path.resolve(__dirname, "../client/src") } },
  build:   { rollupOptions: { input: {
             main:    path.resolve(__dirname, "index.html"),
             compare: path.resolve(__dirname, "compare.html") } } },
  server:  { host: "0.0.0.0", port: 3000, strictPort: true, allowedHosts: true,
             fs: { allow: ["/client", "/app"] },
             proxy: { /* 4 rules */ } },
});
```

| Key | Value | Effect |
|-----|-------|--------|
| `root` | `"."` | Project root is `example/` |
| `publicDir` | `"public"` | Static passthrough (directory exists and is empty) |
| `resolve.alias` | `@mfup/client` → `../client/src` | Bare-specifier prefix rewrite to the SDK's TypeScript sources |
| `build.rollupOptions.input` | `main`, `compare` | Two HTML entry points → multi-page build into `dist/` |
| `server.host` | `0.0.0.0` | Listens on all interfaces (required inside a container) |
| `server.port` / `strictPort` | `3000` / `true` | Fail rather than auto-increment |
| `server.allowedHosts` | `true` | Accepts any `Host` header — needed when proxied behind Caddy |
| `server.fs.allow` | `["/client", "/app"]` | Grants dev-server file access outside root; matches the compose mounts |

**Proxy rules — four entries, all `changeOrigin: true`:**

| Path prefix | Target | Notes |
|-------------|--------|-------|
| `/mfup/control` | `http://backend:8070` | `ws: true` — WebSocket upgrade for the control channel |
| `/mfup` | `http://backend:8070` | Data POSTs, `/mfup/sessions/{id}/publish` |
| `/health` | `http://backend:8070` | Health probe |
| `/trivial` | `http://trivial:8071` | `rewrite: (path) => path.replace(/^\/trivial/, "")` |

The more specific `/mfup/control` key is declared before `/mfup`; this ordering is presumably intentional so the WebSocket-upgrade rule matches first, since Vite evaluates proxy keys in declaration order (inferred, not stated in a comment). The proxy targets are Docker Compose service hostnames, and the same routing is mirrored in the repository's `Caddyfile` for the built path.

---

### 5.4 Client SDK resolution and running the demo

#### How `@mfup/client` is resolved

**Not a published package and not a workspace link — a build-time path alias to source.** Verified evidence:

- `example/package.json` declares `"dependencies": {}` — empty. Only `vite ^6.0.0` and `typescript ^5.7.0` are devDependencies.
- `example/node_modules/@mfup` does not exist.
- There is no root-level `package.json`, so there is no npm/pnpm workspace.
- Resolution happens solely through `resolve.alias["@mfup/client"] → path.resolve(__dirname, "../client/src")`.

Both pages import `@mfup/client/index.js`. Because the alias is a prefix replacement, this becomes `<repo>/client/src/index.js`, which Vite resolves to `client/src/index.ts` (the `.js` suffix is the TypeScript ESM authoring convention). The client's own `package.json` declares `"main": "dist/index.js"`, but the demo bypasses `dist/` entirely — **the client does not need to be built for the demo to run.**

Type-checking is aligned the same way: `tsconfig.json` sets `"include": ["src/**/*.ts", "../client/src/**/*.ts"]`, pulling the SDK sources into the demo's own program under `strict` mode with `moduleResolution: "bundler"`.

Inside Docker, `__dirname` is `/app` and the alias resolves to `/client/src` — which is exactly why `server.fs.allow` lists `/client` alongside `/app`.

#### Server URL discovery

Neither page hardcodes a backend address. Both compute:

```ts
const serverUrl = `${window.location.protocol}//${window.location.host}`;
```

All MFUP traffic therefore goes back to whatever origin served the page, and the Vite proxy (dev) or Caddy (built) routes it onward. `MfupSession` derives its WebSocket URL by swapping `http` → `ws` on this value.

#### Running it

**Full stack via Docker Compose** (from `/workspace`) — the intended path, since the proxy targets are compose service names:

```
docker compose up
```

`caddy` serves `./example/dist` — the *built* output produced by `frontend-build` — while the `frontend` dev-server service is only `expose`d on the compose network, not published. So `http://localhost:20060` reaches the production build; reaching the hot-reloading Vite dev server requires publishing port 3000 or entering the network yourself.

**Standalone npm scripts** (`example/package.json` — two scripts):

| Script | Command | Result |
|--------|---------|--------|
| `npm run dev` | `vite` | Dev server on `0.0.0.0:3000` with HMR and the four proxies |
| `npm run build` | `vite build` | Static `dist/` with both `index.html` and `compare.html` |

Running `npm install && npm run dev` outside Compose starts the UI, but every backend call fails unless the hostnames `backend` and `trivial` resolve — the proxy targets are not configurable via environment variables.

**Pages once running:** `/` (or `/index.html`) for the main demo, `/compare.html` for the comparison.

---
## 6. Deployment & Infrastructure

The whole MFUP stack runs from a single `docker compose` file at the repository root. There are no Dockerfiles: every service uses a stock upstream image, bind-mounts the relevant source directory, and installs its dependencies in the container's `command` at startup. Only one port reaches the host — Caddy on `20060` — and Caddy fronts the compiled demo, the MFUP server, and the trivial baseline server on a single origin, so the browser client never makes a cross-origin request. Persistent state is limited to one bind-mounted directory, `./uploads`, shared by both upload servers and excluded from version control.

### 6.1 Service topology (`docker-compose.yaml`)

Six services. Five use `expose` (visible only inside the compose network); `caddy` is the only service with a `ports:` mapping.

| Service | Image | Port | Host-published | Volumes | Environment | depends_on |
|---|---|---|---|---|---|---|
| `redis` | `redis:7-alpine` | `expose: 6379` | no | none | none | — |
| `backend` | `python:3.12-slim` | `expose: 8070` | no | `./server:/app`, `./uploads:/data/uploads` | `MFUP_BASE_DIR=/data/uploads`, `REDIS_URL=redis://redis:6379/0` | `redis` |
| `trivial` | `python:3.12-slim` | `expose: 8071` | no | `./trivial-server:/app`, `./uploads:/data/uploads` | `UPLOAD_DIR=/data/uploads/trivial-target` | — |
| `frontend` | `node:20-alpine` | `expose: 3000` | no | `./example:/app`, `./client:/client` | none | — |
| `frontend-build` | `node:20-alpine` | none | no | `./example:/app`, `./client:/client` | none | — |
| `caddy` | `caddy:2-alpine` | `80` in container | **yes — `20060:80`** | `./Caddyfile:/etc/caddy/Caddyfile:ro`, `./example/dist:/srv/dist:ro` | none | `backend`, `frontend-build`, `trivial` |

Startup commands:

| Service | Command |
|---|---|
| `backend` | `pip install --quiet fastapi uvicorn[standard] websockets redis && python -m uvicorn mfup.app:app --host 0.0.0.0 --port 8070 --log-level info` |
| `trivial` | `pip install --quiet fastapi uvicorn[standard] python-multipart && python -m uvicorn app:app --host 0.0.0.0 --port 8071 --log-level info` |
| `frontend` | `npm install && npx vite` |
| `frontend-build` | `npm install && npx vite build` |

Key behaviors:

- **No image build step.** `backend` sets `working_dir: /app` with `./server` mounted there, so `uvicorn mfup.app:app` resolves the `mfup` package straight from the mounted source tree. `server/pyproject.toml` is never installed — dependencies are named explicitly in the `pip install` line. Editing Python source on the host changes what the container runs on next restart.
- **No `healthcheck:` blocks are defined anywhere in the file.** Readiness is expressed only through `depends_on`, which waits for container *start*, not for the process inside to be serving. Both Python services do expose an HTTP `/health` endpoint, but Compose is not configured to poll them.
- **Redis has no volume.** Nothing is mounted at `/data`, so the Redis dataset lives only in the container's writable layer. Inferred: the session index is therefore treated as reconstructible or expendable across container recreation, not as durable state. Note the consequence — losing Redis loses the ability to *find* live sessions at restart, even though their SQLite state survives on disk (see §7.1).
- **`./uploads` is shared by two writers.** `backend` treats `/data/uploads` as its root (`MFUP_BASE_DIR`), while `trivial` writes under `/data/uploads/trivial-target` (`UPLOAD_DIR`). Both point at the same host directory, so a side-by-side benchmark run leaves both result trees under one path for comparison.
- **`frontend-build` is a one-shot job.** It has no port and no long-running process — `vite build` writes `./example/dist` and the container exits. `caddy` serves that same directory read-only at `/srv/dist`, which is why `caddy` lists `frontend-build` in `depends_on`. Because `depends_on` does not wait for completion, Caddy can come up while the build is still running and serve an empty or stale `dist` (inferred from the absence of a completion condition).
- **`frontend` (the Vite dev server) is not wired into the published path.** It is not referenced by the Caddyfile and not in Caddy's `depends_on`, and its port 3000 is `expose`d rather than published, so with the file as written the dev server is not reachable from the host. Inferred: it is an opt-in development service intended to be reached from inside the compose network — supported by `example/vite.config.ts`, whose `server.proxy` block re-implements the Caddy routes against the compose DNS names.

### 6.2 Reverse proxy and single-origin routing (`Caddyfile`)

A single site block listening on `:80` inside the container, reached from the host at `http://localhost:20060`. Four `handle` blocks, evaluated as an ordered routing table:

| Match | Action | Target |
|---|---|---|
| `/mfup/*` | `reverse_proxy` (prefix preserved) | `backend:8070` |
| `/health` (exact path) | `reverse_proxy` | `backend:8070` |
| `/trivial/*` | `uri strip_prefix /trivial`, then `reverse_proxy` | `trivial:8071` |
| (no matcher — fallback) | `root * /srv/dist`, `try_files {path} /index.html`, `file_server` | static demo build |

The `/mfup/*` rule forwards the path unchanged, so the MFUP server's routes line up one-to-one: the control WebSocket at `/mfup/control`, data POSTs at `/mfup/data/{session_id}/{leg_id}`, `/mfup/probe/{session_id}`, `/mfup/sessions`, `/mfup/sessions/{session_id}`, `/mfup/sessions/{session_id}/publish`, and `/mfup/sweep`. Caddy's `reverse_proxy` passes WebSocket upgrades through natively, so no extra directive is needed for the control channel. `/trivial/*` is the mirror case: the prefix is stripped before forwarding, so `/trivial/upload` arrives at the baseline server as `/upload`.

The single-origin arrangement means the browser sees exactly one host and port for everything — the HTML and JS bundle, the MFUP REST surface, the control WebSocket, and the trivial server. Nothing is cross-origin, so there is no CORS configuration anywhere in the stack, no preflight cost on the data-POST path, and the WebSocket opens against the same authority as the page. It also keeps the comparison honest: both servers are measured through the identical proxy hop.

The fallback `try_files {path} /index.html` is the standard SPA pattern. Note that the built demo has two HTML entry points; `compare.html` is served directly by `file_server` because it exists on disk, before the `/index.html` fallback applies.

### 6.3 Baseline upload server (`trivial-server/app.py`)

A **benchmark control, not production code** — its own docstring reads "Trivial file-by-file upload server for comparison demo." It exists solely to give the demo a naive upload implementation to measure MFUP against: one HTTP request per file, no protocol on top. 47 lines of FastAPI, two routes.

| Route | Method | Description |
|---|---|---|
| `/upload` | POST | Accepts one `multipart/form-data` file plus an optional `path` form field; streams it to disk under `BASE_DIR` |
| `/health` | GET | Returns `{"status": "ok"}` |

| Symbol | Type | Description |
|---|---|---|
| `BASE_DIR` | const | `Path(os.environ.get("UPLOAD_DIR", "/data/uploads"))` — write root; compose overrides it to `/data/uploads/trivial-target` |
| `app` | `FastAPI` | Titled "Trivial Upload Server" |
| `upload_file(file, path)` | async route handler | Writes the upload, returns `{"ok": True, "file": str(rel), "size": size}` |
| `health()` | async route handler | Liveness probe |

Behavior in `upload_file`: the destination is `BASE_DIR / path / file.filename` (or `BASE_DIR / file.filename` when `path` is empty). Before any directory is created, it applies a path-traversal guard — `dest.resolve().relative_to(BASE_DIR.resolve())`, returning HTTP 403 with `{"error": "path traversal"}` on `ValueError`. Only then does it `mkdir(parents=True, exist_ok=True)` the parent and stream the body to disk in 256 KiB chunks, accumulating the byte count it reports back.

How it differs from the real MFUP server: no sessions, no session IDs, no legs, no resume, no manifest, no probe, no publish, no sweep, and no WebSocket control channel — the entire surface is a single stateless POST. It does not use Redis, and unlike `backend` it needs `python-multipart` because it parses form uploads. An interrupted transfer leaves a truncated file with no way to continue it; the client must re-send the whole file. That gap is precisely what the comparison demo is built to show.

### 6.4 Repository packing helper (`repomix.sh`)

A three-line POSIX shell wrapper:

```sh
npx repomix@latest \
  --no-file-summary \
  --header-text "" \
  -o repomix-output.xml \
  "$@"
```

It packs the entire repository into one XML file for ingestion by an LLM, suppressing repomix's per-file summary block and boilerplate header. `"$@"` forwards extra arguments through, so callers can narrow the scope without editing the script. The output file is gitignored, so running it never dirties the working tree.

### 6.5 Version control exclusions (`.gitignore`)

Seven entries, all generated or runtime artifacts:

| Pattern | What it excludes |
|---|---|
| `node_modules/` | npm dependencies installed by `frontend` / `frontend-build` |
| `dist/` | Vite build output, including `example/dist` that Caddy serves |
| `*.egg-info/` | Python packaging metadata |
| `__pycache__/` | Python bytecode caches |
| `.vite/` | Vite's dependency-optimization cache |
| `uploads/` | **Runtime upload storage** — the bind-mount target for both `backend` and `trivial` |
| `repomix-output.xml` | Output of `repomix.sh` |

`uploads/` is the significant one: it is the only durable state the stack produces, holding completed uploads, in-progress `.incoming.<uuid>` staging entries written by the MFUP server, and the `trivial-target/` subtree. Confirmed by `git ls-files uploads` — zero tracked files.

Note that `dist/` being ignored while `caddy` mounts `./example/dist:/srv/dist:ro` is what makes `frontend-build` load-bearing: the directory Caddy serves does not exist in a fresh clone and must be produced by that service before the site has content.

### 6.6 Running the stack locally

Everything below follows directly from the config files; no scripts or Makefiles beyond these exist.

- **Full stack:** `docker compose up` from `/workspace` starts all six services. `frontend-build` runs `npm install && vite build` and exits; the rest stay up.
- **Serving stack only (skip the dev server):** `docker compose up caddy` pulls in `backend`, `frontend-build`, and `trivial` via `depends_on`, and `redis` transitively via `backend`.
- **Access point:** `http://localhost:20060` — the only host-published port. The demo shell is served from the built `example/dist`; `/mfup/*` and `/health` route to the MFUP server; `/trivial/*` routes to the baseline with the prefix stripped.
- **First run is slow by design.** Every container installs dependencies at startup, so the first `up` includes network installs. Because `depends_on` does not wait for readiness, Caddy may briefly return 502 on `/mfup/*` or serve an empty `/srv/dist` while `backend` and `frontend-build` are still installing (inferred from the absence of healthchecks).
- **Rebuilding the demo after a client or example change:** `docker compose up frontend-build` re-runs `vite build` into `example/dist`; Caddy picks up the new files from the read-only mount without a restart, since it reads from disk per request.
- **Restarting the server after a Python change:** `docker compose restart backend`. The source is bind-mounted, so no rebuild is needed, but uvicorn is started without `--reload`, so the process must be restarted to pick up edits.
- **Vite dev mode caveat:** `docker compose up frontend` starts Vite on container port 3000 with its own proxy rules, but that port is only `expose`d, never published. Reaching the dev server from a host browser requires publishing the port; that mapping is not present in `docker-compose.yaml`.
- **Uploaded data:** lands in `/workspace/uploads` on the host — MFUP output at the root, trivial-server output under `trivial-target/`. Deleting that directory resets all upload state on disk; Redis state is separate and disappears when the `redis` container is removed.

---
## 7. Known Gaps & Divergences

This section synthesizes findings that span module boundaries. Each item was read from source; where an item is an inference about consequences rather than a direct reading, it says so.

### 7.1 Design doc vs implementation

`docs/MFUP_RU.md` is the project's design note. Three of its claims no longer match the code:

| Claim in `docs/MFUP_RU.md` | Actual implementation |
|---|---|
| Frame envelope is `[tag:1][length:4][payload:N]` | It is `[length:4][tag:1][payload]` — length first. Both implementations agree (`client/src/protocol.ts:290`, `server/mfup/protocol.py:296`) |
| "At restart the server scans `.incoming.*` directories and recovers sessions" | There is **no filesystem scan**. `SessionRegistry`'s docstring states "No startup scan"; recovery iterates `SessionIndex.get_not_expired()` from Redis. A session absent from Redis is never recovered, even though its staging dir and SQLite state are intact on disk |
| "Either all files are published, or none" | Publish is atomic **per top-level entry**, not as a whole. `publish_session` loops `os.rename` with no transaction and no rollback; a failure partway leaves some entries published and the rest staged |

The doc's architectural narrative (two channels, interleaved scan/transfer, staging→publish, blended progress) is otherwise accurate and remains the best statement of intent.

### 7.2 Implemented but unwired

Three protocol features are fully built on both sides — frame types, DB tables, client handlers, guards on every server frame handler — but **no server code path ever triggers them**:

| Feature | Server method | Client handler | Status |
|---|---|---|---|
| Server-initiated backpressure | `LiveSession.send_flow()` | `flow` → `paused_by_server` ⇄ `active` | No callers (verified by grep) |
| Subtree pruning | `LiveSession.prune_node()` | `prune_node` → `IngestFilter.shouldDescend` | No callers |
| File rejection | `LiveSession.reject_file()` | `reject_file` → `progress.skipFile()` | No callers |

Consequently `SessionState.PAUSED_BY_SERVER` is unreachable on the server, and the client's `waitIfPaused()` gate never blocks. `SessionState.FAILED` is vestigial server-side (never written), and `EXPIRED` is only ever *read* — the sweeper deletes expired sessions outright rather than transitioning them, so expiry surfaces to the client as a fatal `DATA_HTTP_ERROR` with `status: 410`.

Six of the client's 22 `MfupErrorCode` members are declared but never constructed: `WS_SEND_FAILED`, `DATA_STREAM_ERROR`, `SESSION_ABORT_FAILED`, `SESSION_COMMIT_FAILED`, `SESSION_ENDED_BAD_STATE`, `INGEST_READ_ERROR`. `ProgressTracker.advanceBody()` is an empty stub with no callers.

### 7.3 Advertised but unenforced limits

`HELLO_OK.limits` advertises three caps that the server **never checks**. Verified by grep — each appears only at its definition and inside the `HELLO_OK` payload:

| Limit | Default | Enforced? |
|---|---|---|
| `MFUP_MAX_CHUNK_BYTES` | 262144 | No — no code compares a chunk length against it |
| `MFUP_MAX_OPEN_FILES` | 1 | No — no open-writer count is checked |
| `MFUP_MAX_PENDING_FILES` | 64 | No — no pending-file count is checked |

The client does honor `max_chunk_bytes` (`Math.min(chunkSize, limits.max_chunk_bytes)`), so the limits function as a client-side hint rather than a server-side control. Note that `RESUME_OK` carries no `limits` field, so a session constructed directly with `lastKnownEpoch` (the resume-only path) keeps the client's built-in defaults — `{262144, 1, 64}`, which happen to match the server's defaults.

Related unbounded resources: `FrameReader` applies **no cap on `frame_len`**, so a corrupt or malicious 4-byte prefix declaring a multi-gigabyte frame grows the buffer without limit or error. There is also no cap on concurrent sessions (each holding an open SQLite connection) and no cap on total staged bytes.

### 7.4 Durability

`COMMIT_OK` means "the invariants held and the bytes reached the OS page cache" — not "the bytes survive power loss." `FileWriter._sync_write` calls `write()` + `flush()` with **no `fsync`**, and SQLite runs at `synchronous=NORMAL` under WAL. No comment in the code claims stronger durability.

### 7.5 Authentication

The data and probe endpoints authenticate via the `X-MFUP-Token` header (the resume token). The **four administrative routes perform no authentication or authorization at all** — no token check of any kind, read directly from the handler bodies:

- `GET /mfup/sessions` — lists every live session id, state, epoch, and leg
- `GET /mfup/sessions/{id}` — full session detail
- `POST /mfup/sessions/{id}/publish` — **moves files into an operator-visible directory and then deletes the staging dir**
- `POST /mfup/sweep` — triggers cleanup

Publish in particular is a state-changing, destructive operation reachable by anyone who can guess or observe a session id. The demo application calls it directly from the browser, so this is by design for the demo — but it is not a posture for untrusted networks.

Path traversal, by contrast, is defended in four independent layers (name validation at ingest, name validation at path construction, post-`resolve()` containment check, and `target_dir` containment at both HELLO and publish). None of those layers has a test (§7.7).

### 7.6 Concurrency observations

`LiveSession._lock` **is created and never acquired** — all four `async with self._lock` sites belong to `SessionRegistry`, not `LiveSession`. There is therefore no per-session mutual exclusion. Serialization rests instead on two protocol-level invariants: one active leg per session (checked up front, per-chunk, and again in `process_frame`), and one epoch per leg (bumped on every `attach_leg`).

Two consequences, both inferred from control flow rather than documented:

1. **Concurrent POSTs on the same leg remain possible.** `validate_and_advance_seq` is synchronous so its check-and-advance cannot interleave, but everything after it is `await`-heavy. Two back-to-back POSTs with `seq=N` and `seq=N+1` both pass the gate and then interleave. The safety net is `FileWriter.write`'s offset assertion — a misordered chunk produces `NACK_CHUNK` rather than a corrupt file. Data integrity holds; throughput does not.
2. **The root summary's `body_done_bytes` can lose counts.** `_handle_file_chunk` does a read-modify-write across an `await` boundary. The damage is cosmetic: `COMMIT_OK`'s `bytes` figure comes from `count_committed_files()` summing `accepted_offset`, not from the summary.

Two smaller items: a duplicate `FILE_OPEN` replaces `self.writers[node_id]` without closing the previous writer, leaking a file handle; and `_on_idle()` calls `detach_leg()` directly, so a session that reaches `COMMITTING` and then goes idle without disconnecting is left in `COMMITTING` with its TTL unrefreshed (whether that window is reachable in practice was not confirmed).

The pure-Python `crc32c` runs a per-byte loop on the event loop thread — 262,144 Python-level iterations for a 256 KiB chunk, synchronously, blocking all other sessions. Observed from the loop, not benchmarked.

### 7.7 Test coverage

The server has **one test file**, `server/tests/test_protocol.py`, with 8 synchronous tests. It covers the pure binary decoder and nothing else: 4 of 8 frame types (`NODE`, `FILE_CHUNK`, `DIR_CLOSE`, `SESSION_END`), two CRC-32C vectors, incremental byte-at-a-time feeding, and multi-frame batching.

Everything stateful is untested:

| Area | Coverage |
|---|---|
| `app.py` — all 8 routes, every status path, WS handshake, lifespan, sweeper | none |
| `session_manager.py` — the entire state machine, COMMIT_RETRY, restart revert, epoch increment, `FileWriter` seek-and-truncate resume | none |
| `storage.py` — SQLite layer, `get_incomplete_files` (the pre-commit invariant), **all four path-traversal defenses** | none |
| `publish.py` — conflict detection, merge, clean rename | none |
| `redis_index.py` | none |
| Decoder robustness — unknown tag, truncated payload, oversized `frame_len` | none |

`pytest-asyncio` and `httpx` are declared as dev dependencies but no async or HTTP-client test exists. The client package has no test script and no tests at all.

The net position: the deterministic codec is verified; every security-relevant, stateful, and concurrency-sensitive behavior described in this document is verified only by reading.

---

## 8. Project Structure

```
/workspace
├── server/                      Python — MFUP/2 server (§3)
│   ├── mfup/
│   │   ├── app.py               FastAPI app: 8 routes, lifespan, sweeper
│   │   ├── session_manager.py   LiveSession state machine, FileWriter, SessionRegistry
│   │   ├── storage.py           SQLite SessionDB, staging paths, traversal guards
│   │   ├── protocol.py          Binary frame decoder, crc32c, PROTOCOL_VERSION
│   │   ├── redis_index.py       Expiry sorted set + per-session metadata hash
│   │   ├── publish.py           Staging → target_dir rename, conflict merge
│   │   └── __main__.py          python -m mfup → uvicorn on 0.0.0.0:8070
│   ├── tests/test_protocol.py   8 decoder tests (the only test file)
│   └── pyproject.toml           mfup-server 0.1.0, requires-python >=3.10
│
├── client/                      TypeScript — @mfup/client browser SDK (§4)
│   ├── src/
│   │   ├── index.ts             Re-export barrel (no logic)
│   │   ├── session.ts           MfupSession orchestrator — 1002 lines
│   │   ├── protocol.ts          Binary frame encoders, crc32c, control-msg types
│   │   ├── data-channel.ts      HTTP data plane: streaming + batch modes
│   │   ├── control.ts           Typed WebSocket wrapper
│   │   ├── ingestion.ts         4 browser file-API adapters, blobChunks
│   │   ├── probe.ts             duplex:"half" capability probe
│   │   ├── errors.ts            MfupError, 22 codes, 5 layers
│   │   └── progress.ts          ProgressTracker, blended monotonic fraction
│   ├── package.json             ESM only, zero runtime dependencies
│   └── tsconfig.json            ES2022, strict, declaration output to dist/
│
├── example/                     Vite demo — two pages (§5)
│   ├── src/demo.ts              Canonical MfupSession usage
│   ├── src/compare.ts           MFUP vs file-by-file race harness
│   ├── index.html               Main demo (markup + all CSS)
│   ├── compare.html             Comparison page
│   └── vite.config.ts           @mfup/client alias → ../client/src, 4 proxies
│
├── trivial-server/app.py        Benchmark control: naive POST-per-file server (§6.3)
│
├── docs/
│   ├── FULL.md                  This document
│   └── MFUP_RU.md               Russian design note — motivation, benchmarks (see §7.1)
│
├── docker-compose.yaml          6 services, no Dockerfiles (§6.1)
├── Caddyfile                    Single-origin routing, host port 20060 (§6.2)
├── repomix.sh                   Repo → single XML for LLM ingestion
└── uploads/                     Runtime storage (gitignored) — staging + published trees
```

**Approximate sizes** (tokens, excluding lockfiles): server 18.4k across 11 files · client 23.2k across 11 files · example 13.8k across 7 files · infrastructure 0.9k across 4 files.
