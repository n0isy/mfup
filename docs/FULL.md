# MFUP/2 — Multi-File Upload Protocol

**Packages:** `mfup-core` + `mfup-fastapi` (PyPI) · `@mfup/client` + `@mfup/react` + `@mfup/server` (npm)
**Languages:** Python 3.10+ · TypeScript 5.5 (ES2022; browser for the client, Node ≥ 22.13 for the server)
**Frameworks:** FastAPI + uvicorn or any Node HTTP stack (server) · zero runtime dependencies (client) · Vite 6 (demo)
**Protocol version:** `MFUP/2`
**Stores:** SQLite (one DB per session) · Redis or in-memory (session expiry index) · POSIX filesystem
**Hard runtime dependency:** the C-accelerated `crc32c` package — Python server only (§3.2); the Node server has no native dependency (§5)
**License:** MIT
<!-- ⬆️ 1 ⬆️ -->

The problem: moving whole directory trees — `node_modules`-scale, tens of thousands of
small files — from a browser to a server, resumably, interactively, and atomically. The
industry default is one multipart POST per file; the measured cost of that on a 188-file,
1.0 MB tree is 17.2 s, of which only ~1.3 s is transfer and the rest is round-trip latency
and `FormData` construction (§8).
<!-- ⬆️ 2 ⬆️ -->

---

## 1. Architecture

### 1.1 The split every design decision follows

MFUP/2 separates **control** from **data**, and separates both from **durability**.

```
browser                                    server
┌─────────────────────┐                    ┌──────────────────────────────┐
│ @mfup/react (hooks) │                    │  host FastAPI app            │
├─────────────────────┤                    │  └─ include_router(          │
│ @mfup/client        │                    │       engine.router)         │
│  ├ MfupSession      │  WS  /mfup/control ├──────────────────────────────┤
│  ├ ControlChannel   │<══════════════════>│  mfup-fastapi                │
│  ├ DataChannel      │  POST /mfup/data   │   MfupEngine                 │
│  ├ ingestion (DnD)  │───────────────────>│    ├ routes + fencing        │
│  ├ ProgressTracker  │  POST /mfup/probe  │    ├ lifecycle + sweeper     │
│  └ probe            │───────────────────>│    └ hook invocation         │
└─────────────────────┘  POST …/publish    ├──────────────────────────────┤
                         ───────────────── >│  mfup-core (no HTTP at all)  │
                                            │   ├ FrameReader / CRC-32C    │
                                            │   ├ LiveSession + Registry   │
                                            │   ├ SessionDB (SQLite)       │
                                            │   ├ publish (rename)         │
                                            │   └ hooks contracts          │
                                            └───────┬──────────┬───────────┘
                                                    │          │
                                         ┌──────────▼──┐  ┌────▼─────────────┐
                                         │ Redis       │  │ POSIX filesystem │
                                         │ expiry zset │  │ staging → target │
                                         └─────────────┘  └──────────────────┘
```
<!-- ⬆️ 3 ⬆️ -->

**Control is a WebSocket carrying JSON**; **data is HTTP POSTs carrying length-prefixed
binary frames**. They are deliberately different transports because they have opposite
requirements. Control must stay responsive while gigabytes are in flight — that is what
makes the interactive `ASK` ("this file exists, overwrite?") possible *without pausing the
transfer*. Data must be able to use whatever the browser and the proxy path actually
support, which is discovered empirically rather than assumed (§1.4).
<!-- ⬆️ 4 ⬆️ -->

**`mfup-core` knows nothing about HTTP.** It receives decoded frames and a duck-typed
object with a `send_json` method. This is why the session state machine, the SQLite
journal, the commit invariants, and the publish step are all testable in-process with a
fake WebSocket and no network (§9).

The diagram shows the Python server. A second implementation, `@mfup/server` (§5), sits
behind the identical wire and the identical on-disk journal, so the browser side of the
diagram is unchanged when it is swapped in.
<!-- ⬆️ 5 ⬆️ -->

### 1.2 Layers and what each one owns

| Layer | Package | Owns | Never touches |
|---|---|---|---|
| Hooks | `@mfup/react` | React binding: context, `useSyncExternalStore` adapter, prop getters | protocol, network |
| Client | `@mfup/client` | Session state machine, ingestion, chunking, retry, progress, probe | server policy |
| Transport | `mfup-fastapi` | Routes, auth/token/leg/epoch/seq validation, lifecycle, sweeper, hook invocation | durable state |
| Core | `mfup-core` | Frame codec, session state machine, SQLite journal, CRC verification, publish, hook contracts | HTTP, FastAPI |
| Server (Node) | `@mfup/server` | The whole server side in one package: universal `(req,res)` + `upgrade` handler, plus a full port of the core semantics (§5) | the browser |
| Stores | — | Redis or in-memory expiry index; per-session SQLite; POSIX staging + target trees | — |
<!-- ⬆️ 6 ⬆️ -->

The dependency arrow points one way only: `mfup-fastapi` imports from `mfup_core`;
`mfup_core` imports nothing from the transport. On the client, `@mfup/react` peer-depends
on `@mfup/client` and adds no protocol logic. `@mfup/server` collapses the two server rows
into a single package — it depends on neither Python package and re-implements the same
contracts natively, so the split above describes its internal module boundaries rather
than its dependency graph.
<!-- ⬆️ 7 ⬆️ -->

### 1.3 Data flow: one upload, end to end

1. **`HELLO`** over the WebSocket carries the protocol version, a client-generated
   `session_id`, a `leg_id`, a `target_dir`, and arbitrary `meta`. The server calls the
   consumer's `authorize` hook, which is the single policy surface: it may deny, pin a
   per-user `base_dir`, rewrite `target_dir`, and set byte/file quotas. The server then
   creates the session, registers it in Redis, and replies **`HELLO_OK`** with a
   **server-issued** `resume_token` and its limits.
2. **Ingestion** walks the dropped tree (FileSystem handles, `webkitGetAsEntry` entries,
   `webkitRelativePath` lists, or a flat `File[]`), emitting `NODE` frames that carry
   structure as `(nodeId, parentId, name)` triples — never as path strings.
3. **Transfer** runs a scan and a file pump concurrently. Each file emits `FILE_OPEN`,
   a stream of `FILE_CHUNK` frames (each carrying an absolute offset and a CRC-32C of its
   payload), then `FILE_CLOSE`. The server verifies each chunk's checksum before writing,
   writes at the exact offset, and records the new `accepted_offset` in SQLite *after* the
   write returns — so the durable offset is never ahead of the bytes.
4. **Acknowledgement** flows back on the control channel: `FILE_ACK` roughly every 256 KiB,
   `NACK_CHUNK` on checksum/offset/policy failures, `ASK` on a target conflict,
   `REJECT_FILE`/`PRUNE_NODE` for per-node refusals, `FLOW` for backpressure.
5. **`SESSION_END`** moves the session to `COMMITTING`. The server checks two invariants:
   the node count it recorded plus the nodes it deliberately dropped must equal the count
   the client claims to have scanned, and no file may be incomplete. Failure sends
   `COMMIT_RETRY` and returns the session to `ACTIVE`; success sends `COMMIT_OK`.
6. **Publish** `rename()`s staged entries into the target directory — one syscall per
   top-level entry on the clean path. A `map_file` hook may re-lay-out every file first.
   Staging is then removed.
<!-- ⬆️ 8 ⬆️ -->

### 1.4 Design decisions worth knowing

**Durability lives in one SQLite file per session, inside that session's staging
directory.** One file per session means recovery opens exactly one database, cleanup is a
single `rmtree` that takes the journal with it, and there is no cross-session write
contention. The DB is opened WAL with `busy_timeout=3000`.
<!-- ⬆️ 9 ⬆️ -->

**Resume is built on three independent counters.** `epoch` is bumped in SQLite on every
leg attach and fences stale POSTs; `leg_id` fences frames from a superseded connection;
`seq` orders POSTs within a leg. A buffered POST advances `seq` only after the whole body
processed successfully, which is what makes retrying an identical POST safe — and makes a
duplicate detectable as `seq_mismatch`.
<!-- ⬆️ 10 ⬆️ -->

**The client never trusts, and the server never assumes.** The streaming verdict is
*probed*: the client checks whether `duplex: "half"` even constructs, then sends 1 KiB
and waits for the server to report on the control channel how many bytes actually arrived.
A browser that stringified the stream sends 23 bytes and fails the check; a proxy that
buffered the whole body answers too early and also fails. The verdict is cached in
`localStorage` for 24 h (§6.9).
<!-- ⬆️ 11 ⬆️ -->

**A failed data POST used to be able to lose `NODE` frames**, which is precisely how a
`COMMIT_OK` could be returned for an incomplete tree. Three mechanisms close that: an
`unknown_node` NACK triggers a parent-chain re-send, a reconnect replays the entire node
map (idempotent server-side), and the server's node-count commit invariant refuses to
commit a tree it cannot fully account for. `meta-loss.spec.ts` is the regression test.
<!-- ⬆️ 12 ⬆️ -->

**Failures are classified, not uniformly retried.** `ENOSPC`/`EDQUOT`/`EROFS`/`EFBIG`
abort the session, because retrying cannot clear a full disk. Every other `OSError` is a
recoverable `NACK_CHUNK`. Failures scoped to one node — an illegal name, a file/directory
collision — reject that node and let the rest of the tree commit. A commit that cannot
make progress terminates after 5 *consecutive no-progress* rounds rather than looping.
<!-- ⬆️ 13 ⬆️ -->

**Publish is a same-filesystem `rename`.** That is why `AuthResult.base_dir` exists: per-user
homes on separate mounts each stage *inside themselves*, so the final move never degrades
into a copy. Each individual rename is atomic; the publish as a whole is a loop of renames,
so mapped publish validates the entire plan before moving anything.
<!-- ⬆️ 14 ⬆️ -->

**Retention has three layers.** A resume TTL (default 1 h) scored into a Redis sorted set;
an idle-leg timeout (default 60 s) that detaches an inactive leg; and a periodic sweeper
that reclaims expired staging directories by path — plus a filesystem reconciliation pass
for staging that Redis has forgotten about, guarded by a grace window so it never deletes
something it cannot verify.
<!-- ⬆️ 15 ⬆️ -->

**One worker per engine, or sticky routing on `session_id`.** Lazy resume covers failover:
a `RESUME` for a session this worker does not know triggers recovery from the Redis-recorded
staging path. Round-robin across workers is unsupported.
<!-- ⬆️ 16 ⬆️ -->

---

## 2. Wire protocol at a glance

Two channels, four HTTP endpoints, eight binary frame tags, sixteen JSON control messages.
The authoritative per-field detail is in §3.2 (server codec), §4 (routes and messages) and
§6.3 (client types).
<!-- ⬆️ 17 ⬆️ -->

### 2.1 Endpoints

| Transport | Path | Carries |
|---|---|---|
| WebSocket | `/mfup/control` | JSON control messages, both directions |
| `POST` | `/mfup/data/{session_id}/{leg_id}?seq=&final=&epoch=` | Length-prefixed binary frames |
| `POST` | `/mfup/probe/{session_id}` | Streaming-capability probe body |
| `POST` | `/mfup/sessions/{session_id}/publish` | — (moves staged files into the target) |
| `GET` | `/health` | `{"status","protocol","crc32c"}` |
<!-- ⬆️ 18 ⬆️ -->

Plus three admin routes (`GET /mfup/sessions`, `GET /mfup/sessions/{id}`, `POST /mfup/sweep`),
disabled entirely unless `MFUP_ADMIN_TOKEN` is set. All non-WebSocket endpoints authenticate
with the server-issued session token in the `X-MFUP-Token` header.
<!-- ⬆️ 19 ⬆️ -->

### 2.2 Binary frame envelope

```
[u32 BE length][u8 tag][payload]        length covers tag + payload, not itself
```
<!-- ⬆️ 20 ⬆️ -->

All multi-byte integers are big-endian. Strings are `[u16 length][UTF-8 bytes]`.
The decoder caps a declared frame length at 1 MiB, so a corrupt 4-byte prefix cannot make
the reader buffer indefinitely.
<!-- ⬆️ 21 ⬆️ -->

| Tag | Value | Direction | Purpose |
|---|---|---|---|
| `NODE` | `0x01` | C→S | Declare a tree node: `(node_id, parent_id, kind, name, size?, mtime?)` |
| `SUMMARY` | `0x02` | C→S | Progress counters for a node (root summary every 50 nodes) |
| `FILE_OPEN` | `0x03` | C→S | Begin a file body |
| `FILE_CHUNK` | `0x04` | C→S | `(node_id, offset, length, CRC-32C, payload)` |
| `FILE_CLOSE` | `0x05` | C→S | End a file body, declaring `size_sent` |
| `DIR_CLOSE` | `0x06` | C→S | Close a directory |
| `SESSION_END` | `0x07` | C→S | Seal the scan and request commit |
| `CLIENT_ABORT` | `0x08` | C→S | Defined in both codecs; the reference client aborts over the control channel instead |
<!-- ⬆️ 22 ⬆️ -->

Structure travels as `(node_id, parent_id, name)` — never as a path string. The server
rebuilds paths by walking the parent chain, validating every segment and asserting the
result stays inside the payload root, with a cycle detector and a hard error on a broken
chain. This is the containment boundary: no name can escape, because no path is ever
transmitted.
<!-- ⬆️ 23 ⬆️ -->

### 2.3 Control messages

Client → server (4):

| `t` | Purpose |
|---|---|
| `HELLO` | Open a session: version, ids, `target_dir`, `meta` |
| `RESUME` | Re-attach with the server-issued `resume_token` and a new `leg_id` |
| `CLIENT_ABORT` | Terminal client-side abort |
| `ACTION` | Answer an `ASK`: `merge_overwrite` or `cancel` |
<!-- ⬆️ 24 ⬆️ -->

Server → client (12):

| `t` | Purpose |
|---|---|
| `HELLO_OK` | `epoch`, `expires_at`, **server-issued** `resume_token`, `limits` |
| `RESUME_OK` | `epoch`, `expires_at`, `root_summary`, per-file `accepted_offset`, pruned/rejected ids |
| `FILE_ACK` | Cumulative `accepted_offset` for a file (~every 256 KiB) |
| `NACK_CHUNK` | `bad_checksum` \| `bad_offset` \| `stale_epoch` \| `server_policy` \| `unknown_node` |
| `FLOW` | Pause/resume: `backpressure` \| `maintenance` \| `storage_pressure` |
| `ASK` | Interactive question, currently `target_conflict` — carries the basename only |
| `PRUNE_NODE` | Drop a subtree |
| `REJECT_FILE` | Reject one file, session continues |
| `COMMIT_RETRY` | Commit invariant failed; lists incomplete files and node counts |
| `COMMIT_OK` | Committed: `files`, `bytes` |
| `SESSION_ABORT` | Terminal: `storage_full`, `quota_exceeded`, `auth_failed`, `commit_failed`, … |
| `PROBE_ACK` | How many bytes of the probe body actually arrived |
<!-- ⬆️ 25 ⬆️ -->

The asymmetry is the point: the client asks for very little and the server narrates
continuously, which is what lets the UI show real per-file state and lets the server
interrupt with a question mid-flight.
<!-- ⬆️ 26 ⬆️ -->

### 2.4 Session states

`active` → `paused_by_server` → `waiting_resume` → `committing` → **`committed`**, with
**`aborted`**, **`failed`** and **`expired`** as the other terminals. `committing` is
resumable on purpose: commit is client-initiated, so a leg that drops mid-commit must be
able to reconnect and re-send `SESSION_END`. Both the server (§3.3) and the client (§6.2)
implement this machine; the client never assigns `expired`, which exists for parity.

<!-- ⬆️ 27 ⬆️ -->
---

## 3. mfup-core — Protocol Core

`mfup-core` is the framework-free engine of MFUP/2: the binary frame codec, the session state machine, a per-session SQLite journal in a staging directory, CRC-32C chunk verification, a Redis expiry index, the publish (staging → target `rename()`) step, and the consumer hook contracts. It speaks no HTTP — `mfup-fastapi` supplies the WebSocket control channel and HTTP data legs and drives these primitives. Package version `0.2.0`, `requires-python >=3.10`, built with hatchling, wheel packages `["mfup_core"]`. Runtime dependencies are exactly two: `redis>=5.0.0` and `crc32c>=2.7`. The package ships `py.typed`.
<!-- ⬆️ 28 ⬆️ -->

Source: `server/mfup-core/mfup_core/`
<!-- ⬆️ 29 ⬆️ -->

### 3.1 Public API surface (`__init__.py`)

`__all__` re-exports 26 names from five submodules. Everything else (`SessionDB`, `open_session_db`, `ensure_staging`, `resolve_payload_path`, `NodeStatus`, `NodeKind`, `ChecksumKind`, `FrameTag`, `ROOT_NODE_ID`, `crc32c`, `decode_frame_payload`, `SessionMeta`, the frame dataclasses) is reachable only via its submodule — `mfup-fastapi` imports several of those directly, e.g. `from mfup_core.redis_index import SessionIndex`.
<!-- ⬆️ 30 ⬆️ -->

| Export | Kind | Origin | Description |
|---|---|---|---|
| `PROTOCOL_VERSION` | `str` const | `protocol` | `"MFUP/2"` |
| `CRC32C_IMPL` | `str` const | `protocol` | Always `"native"` — `mfup-core` has no other implementation (the Node server reports `"js-table"`, §5) |
| `SessionState` | `str`-Enum | `protocol` | The 8 session states (§3.3) |
| `FrameReader` | class | `protocol` | Incremental length-prefixed frame decoder |
| `LiveSession` | class | `session_manager` | In-memory state + frame handlers for one session |
| `SessionRegistry` | class | `session_manager` | Process-wide `dict[str, LiveSession]` with an `asyncio.Lock` |
| `SessionIndex` | class | `redis_index` | Async Redis wrapper for the expiry ZSET + meta hash |
| `staging_dir(base_dir, session_id, prefix)` | function | `storage` | Computes `<base_dir>/<prefix>.<session_id>` (pure, no I/O) |
| `validate_node_name(name)` | function | `storage` | Raises `ValueError` on `""`, `"."`, `".."`, `/`, `\`, `NUL` |
| `publish_session(...)` | function | `publish` | Rename payload entries into `target_dir` |
| `publish_session_mapped(...)` | function | `publish` | Publish with a consumer-supplied per-file layout |
| `list_payload_files(base_dir, session_id, prefix)` | function | `publish` | `list[tuple[str, int]]` of `(posix-relative path, size)` |
| `ConflictError` | exception | `publish` | Carries `.conflicting_files: int` |
| `MappingError` | exception | `publish` | Unusable `map_file` output (escape, illegal segment, collision) |
| `AuthRequest` / `AuthResult` / `AuthorizeHook` | dataclass ×2, type alias | `hooks` | HELLO-time authorization contract |
| `FileMapRequest` / `MapFileHook` | dataclass, type alias | `hooks` | Publish-time per-file relayout contract |
| `CommitEvent` / `OnCommittedHook` | dataclass, type alias | `hooks` | Post-commit server-side decision contract |
| `load_hook`, `load_authorize_hook`, `load_map_file_hook`, `load_on_committed_hook`, `resolve_hook` | functions | `hooks` | Dotted-path hook resolution |
<!-- ⬆️ 31 ⬆️ -->

### 3.2 Protocol codec (`protocol.py`)

Defines the wire vocabulary and the server-side decoder. Wire format is `[4-byte big-endian length][1-byte tag][payload]`, where the length covers tag + payload. All multi-byte integers are big-endian (`!H`, `!I`, `!Q`); strings are a `u16` byte length followed by UTF-8.
<!-- ⬆️ 32 ⬆️ -->

**Eight frame tags**, each with a matching `@dataclass(slots=True)`:

| `FrameTag` | Value | Dataclass | Payload fields (in wire order) |
|---|---|---|---|
| `NODE` | `0x01` | `NodeFrame` | `node_id:u32`, `parent_id:u32`, `kind:u8`, `name:str`, `has_size:u8`+`size_hint:u64?`, `has_mtime:u8`+`mtime_ms:u64?` |
| `SUMMARY` | `0x02` | `SummaryFrame` | `node_id:u32`, `scan_done_units:u64`, `scan_est_units:u64`, `body_done_bytes:u64`, `body_est_bytes:u64`, `sealed:u8` |
| `FILE_OPEN` | `0x03` | `FileOpenFrame` | `node_id:u32`, `size:u64`, `has_mtime:u8`+`mtime_ms:u64?` |
| `FILE_CHUNK` | `0x04` | `FileChunkFrame` | `node_id:u32`, `offset:u64`, `length:u32`, `checksum_kind:u8`, `checksum:u32`, `payload[length]` |
| `FILE_CLOSE` | `0x05` | `FileCloseFrame` | `node_id:u32`, `size_sent:u64` |
| `DIR_CLOSE` | `0x06` | `DirCloseFrame` | `node_id:u32` |
| `SESSION_END` | `0x07` | `SessionEndFrame` | `scan_done_units:u64`, `scan_est_units:u64`, `body_done_bytes:u64`, `body_est_bytes:u64`, `sealed:u8` |
| `CLIENT_ABORT` | `0x08` | `ClientAbortFrame` | `code:str`, `reason:str` |
<!-- ⬆️ 33 ⬆️ -->

Supporting enums: `NodeKind.DIR = 0x00` / `NodeKind.FILE = 0x01`; `ChecksumKind.CRC32C = 0x01` (the only member); `ROOT_NODE_ID = 0`. `Frame` is the union of the eight dataclasses. `decode_frame_payload(tag, payload)` raises `ValueError(f"unknown frame tag: {tag:#04x}")` for anything else.
<!-- ⬆️ 34 ⬆️ -->

#### `FrameReader`

| Member | Type | Description |
|---|---|---|
| `DEFAULT_MAX_FRAME_LEN` | class const | `1024 * 1024` (1 MiB) — chosen to sit above `MAX_CHUNK_BYTES` (256 KiB) plus headers and long UTF-8 names |
| `__init__(max_frame_len=DEFAULT_MAX_FRAME_LEN)` | ctor | Allocates a `bytearray` accumulation buffer |
| `feed(data: bytes) -> None` | method | Appends to the buffer |
| `drain() -> list[Frame]` | method | Decodes every complete frame, consumes them from the buffer, leaves the partial tail |
<!-- ⬆️ 35 ⬆️ -->

`drain()` raises `ValueError(f"frame length {n} exceeds limit {m}")` when a declared length exceeds `max_frame_len` — without that bound, a corrupt or hostile 4-byte prefix would grow the buffer indefinitely while the reader waited for the "rest" of the frame. Each payload is copied into a fresh `bytes` before decoding so no `memoryview` keeps the buffer alive across the `del self._buf[:pos]` compaction.
<!-- ⬆️ 36 ⬆️ -->

#### CRC-32C

The `crc32c` C extension (SSE4.2 / ARMv8 accelerated) is imported at module scope and a failure is re-raised immediately:

```python
try:
    import crc32c as _crc32c_native
except ImportError as exc:
    raise ImportError(
        "MFUP/2 requires the C-accelerated 'crc32c' package: pip install crc32c"
    ) from exc

def crc32c(data: bytes | memoryview, initial: int = 0) -> int:
    return _crc32c_native.crc32c(data, initial)

CRC32C_IMPL = "native"
```
<!-- ⬆️ 37 ⬆️ -->

There is **no** pure-Python fallback — one existed and was removed deliberately: it measured ~6 MB/s and blocked the event loop ~10 ms per 64 KiB chunk. Importing `mfup_core` at all fails if the extension is missing; `CRC32C_IMPL` is therefore a constant `"native"`, not a runtime probe. The Node server reaches the opposite conclusion from the same measurement: a table-driven CRC-32C in JS is fast enough on V8 to need no native module at all (§5).
<!-- ⬆️ 38 ⬆️ -->

### 3.3 Session state machine (`SessionState`)

Eight states, persisted as the `sessions.state` TEXT column (the enum is `str`-valued, so `.value` is the stored string):

| State | Stored value | Meaning |
|---|---|---|
| `ACTIVE` | `active` | A leg is attached and frames are being ingested |
| `PAUSED_BY_SERVER` | `paused_by_server` | Server sent `FLOW{paused:true}` |
| `WAITING_RESUME` | `waiting_resume` | No leg attached; resumable until `expires_at` |
| `COMMITTING` | `committing` | `SESSION_END` received, commit invariants pending |
| `COMMITTED` | `committed` | Terminal — invariants passed, `COMMIT_OK` sent |
| `ABORTED` | `aborted` | Terminal — client abort, storage failure, or quota violation |
| `EXPIRED` | `expired` | Terminal — **never assigned inside `mfup-core`**; declared for the transport, which treats it as terminal |
| `FAILED` | `failed` | Terminal — commit retries exhausted with no forward progress |
<!-- ⬆️ 39 ⬆️ -->

#### Transitions (all in `session_manager.py`)

| From | To | Trigger | Side effects |
|---|---|---|---|
| — | `ACTIVE` | `SessionDB.init_session()` via `SessionRegistry.create()` | Row inserted with `epoch = 1` |
| any | `ACTIVE` | `LiveSession.attach_leg(leg_id)` (from `create()` and `resume()`) | Closes stale writers; `last_data_seq = -1`; `final_seq_seen = False`; `commit_retries = 0`; re-seeds `files_seen`/`bytes_accepted` from `count_committed_files()`; `increment_epoch()`; resets the idle timer |
| `ACTIVE` | `PAUSED_BY_SERVER` | `send_flow(paused=True, reason)` | Sends `{"t":"FLOW","paused":true,...}` |
| `PAUSED_BY_SERVER` | `ACTIVE` | `send_flow(paused=False, reason)` | Only if currently paused |
| `ACTIVE` / `PAUSED_BY_SERVER` / `COMMITTING` | `WAITING_RESUME` | `detach_leg()` — disconnect or `_on_idle` after `leg_idle_timeout` | Closes writers, cancels idle timer, sets `expires_at = now + session_resume_ttl`, fires `_on_expiry_change`, clears `leg_id` and `ws` |
| `ACTIVE` | `COMMITTING` | `SESSION_END` frame (`_handle_session_end`) | Writes the client's final root summary with `sealed=True`, closes all writers |
| `COMMITTING` | `COMMITTED` | `try_commit()` with both invariants satisfied | Sends `{"t":"COMMIT_OK","files":n,"bytes":n}` |
| `COMMITTING` | `ACTIVE` | `try_commit()` invariant failure, retries not exhausted | Sends `COMMIT_RETRY`; resets `final_seq_seen`/`last_data_seq` so a new final POST is accepted |
| `COMMITTING` | `FAILED` | `try_commit()` after `MAX_COMMIT_RETRIES` (5) consecutive no-progress retries | Sends `SESSION_ABORT{code:"commit_failed"}` |
| any | `ABORTED` | `CLIENT_ABORT` frame (`_handle_client_abort`) | Closes writers, logs client code/reason |
| any | `ABORTED` | `abort_storage_error(exc)` | Sends `SESSION_ABORT` with `code = "storage_full"` for `ENOSPC`/`EDQUOT`, else `"storage_error"` |
| any | `ABORTED` | `abort_quota(reason)` | Sends `SESSION_ABORT{code:"quota_exceeded"}` |
| `ACTIVE` / `PAUSED_BY_SERVER` / `COMMITTING` | `WAITING_RESUME` | `SessionRegistry.recover_session()` after a process restart | Forced because no leg exists post-restart |
<!-- ⬆️ 40 ⬆️ -->

`COMMITTING` is deliberately included in both the `detach_leg` and `recover_session` transitions: commit is client-initiated, so a session whose leg dropped mid-commit must be resumable and the client re-sends `SESSION_END` after `RESUME`. Correspondingly, `SessionRegistry.resume()` admits only `WAITING_RESUME`, `ACTIVE`, `PAUSED_BY_SERVER` and raises `ValueError(f"cannot resume session in state {state.value}")` otherwise (so a `COMMITTING` session must first pass through `detach_leg`). `recover_session()` returns `None` for the four terminal states.
<!-- ⬆️ 41 ⬆️ -->

### 3.4 Storage layer (`storage.py`)

#### On-disk layout

```
<base_dir>/
├── .incoming.<session_id>/          # staging_dir(); prefix = DEFAULT_STAGING_PREFIX = ".incoming"
│   ├── state.sqlite                # the per-session journal (+ WAL/SHM sidecars)
│   └── payload/                    # a 1:1 mirror of the client's tree
│       └── <dir>/<dir>/<file>
└── <target_dir>/                   # final destination (publish moves entries here)
```
<!-- ⬆️ 42 ⬆️ -->

There is no temp-suffix scheme: payload files are written at their **final relative names** inside `payload/`, and publish renames them (or their top-level ancestor directories) into `target_dir`. `AuthResult.base_dir` exists precisely so staging is created inside the user's own base — publish then stays a same-filesystem `rename()` even when homes live on separate mounts.
<!-- ⬆️ 43 ⬆️ -->

| Helper | Signature | Behavior |
|---|---|---|
| `staging_dir` | `(base_dir: Path, session_id: str, prefix=".incoming") -> Path` | Pure path join, `<base>/<prefix>.<sid>`; no I/O |
| `ensure_staging` | `(base_dir, session_id, prefix) -> Path` | `mkdir(parents=True, exist_ok=True)` on staging and `payload/` |
| `open_session_db` | `(base_dir, session_id, prefix) -> SessionDB` | `ensure_staging()` then opens `state.sqlite` |
| `validate_node_name` | `(name: str) -> None` | Raises `ValueError` on empty, `"."`, `".."`, or a name containing `/`, `\`, or `\x00` |
| `resolve_payload_path` | `(base_dir, session_id, db, node_id, prefix) -> Path` | Walks the `parent_id` chain to `ROOT_NODE_ID` building the relative path |
<!-- ⬆️ 44 ⬆️ -->

`resolve_payload_path` applies three defenses: a `seen: set[int]` cycle detector (`ValueError("parent cycle detected at node …")`), a hard error on a missing ancestor (`ValueError("broken parent chain: node … unknown")` — a silent `break` would resolve a *shortened* path and write the file into the wrong directory), `validate_node_name` on every component, and finally a `result.resolve().is_relative_to(payload_root.resolve())` containment check.
<!-- ⬆️ 45 ⬆️ -->

#### `SessionDB`

Opens `sqlite3.connect(str(db_path), timeout=5.0)` and applies three pragmas — `journal_mode=WAL`, `busy_timeout=3000`, `synchronous=NORMAL` — then `executescript(_SCHEMA)`. Two additive columns are applied in place rather than via versioned migrations, because sessions are short-lived (TTL-bounded):

```python
for col in ("meta_json", "auth_json"):
    try:
        self._conn.execute(f"ALTER TABLE sessions ADD COLUMN {col} TEXT")
    except sqlite3.OperationalError:
        pass  # column already exists
```
<!-- ⬆️ 46 ⬆️ -->

**One DB per session** falls out of the layout: the database lives *inside* the session's staging directory, so `_cleanup_staging()`'s single `shutil.rmtree` reclaims the journal together with the payload; a session is recovered by opening exactly one file (`recover_session` reads `staging_path / "state.sqlite"`); and each `LiveSession` owns one `SessionDB` connection with at most one active leg, so the write path has a single writer and never contends across sessions.
<!-- ⬆️ 47 ⬆️ -->

#### Schema — 6 tables, no secondary indices

```sql
CREATE TABLE IF NOT EXISTS sessions (
    session_id   TEXT PRIMARY KEY,
    resume_token TEXT NOT NULL,
    epoch        INTEGER NOT NULL DEFAULT 1,
    state        TEXT NOT NULL DEFAULT 'active',
    target_dir   TEXT NOT NULL DEFAULT '.',
    expires_at   TEXT NOT NULL,
    created_at   TEXT NOT NULL,
    updated_at   TEXT NOT NULL
);          -- plus meta_json TEXT, auth_json TEXT added by ALTER at open

CREATE TABLE IF NOT EXISTS nodes (
    node_id    INTEGER PRIMARY KEY,
    parent_id  INTEGER NOT NULL,
    kind       TEXT NOT NULL,          -- 'dir' | 'file'
    name       TEXT NOT NULL,
    size       INTEGER,
    mtime_ms   INTEGER,
    status     TEXT NOT NULL DEFAULT 'open'   -- NodeStatus
);

CREATE TABLE IF NOT EXISTS files (
    node_id          INTEGER PRIMARY KEY REFERENCES nodes(node_id),
    accepted_offset  INTEGER NOT NULL DEFAULT 0,
    final_size       INTEGER,
    checksum_state   BLOB,
    local_tmp_path   TEXT
);

CREATE TABLE IF NOT EXISTS pruned (
    node_id  INTEGER PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS rejected (
    node_id  INTEGER PRIMARY KEY,
    code     TEXT NOT NULL,
    reason   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS root_summary (
    id               INTEGER PRIMARY KEY CHECK (id = 1),
    scan_done_units  INTEGER NOT NULL DEFAULT 0,
    scan_est_units   INTEGER NOT NULL DEFAULT 0,
    body_done_bytes  INTEGER NOT NULL DEFAULT 0,
    body_est_bytes   INTEGER NOT NULL DEFAULT 0,
    sealed           INTEGER NOT NULL DEFAULT 0
);

INSERT OR IGNORE INTO root_summary (id) VALUES (1);
```
<!-- ⬆️ 48 ⬆️ -->

Constraints in force: `sessions.session_id`, `nodes.node_id`, `files.node_id`, `pruned.node_id`, `rejected.node_id` are PRIMARY KEYs; `files.node_id` carries a `REFERENCES nodes(node_id)` foreign key (declared, but `PRAGMA foreign_keys` is not enabled, so it is not enforced); `root_summary` is pinned to a single row by `CHECK (id = 1)`. **No `CREATE INDEX` statements exist** — every query is a PK lookup, a full scan of a small table, or a `files JOIN nodes` on the two PKs. `checksum_state BLOB` is declared but never read or written by any code in the package: no rolling whole-file checksum is persisted. `local_tmp_path` is written once by `set_file_final()` with the absolute payload path of the closed file.
<!-- ⬆️ 49 ⬆️ -->

`NodeStatus` (also `str`-valued) has four members: `OPEN = "open"`, `CLOSED = "closed"`, `REJECTED = "rejected"`, `PRUNED = "pruned"`.
<!-- ⬆️ 50 ⬆️ -->

#### `SessionDB` API

Every mutating method ends in `_maybe_commit()`, which commits unless batch mode is active.

| Method | Returns | Description |
|---|---|---|
| `conn` | `sqlite3.Connection` | Property exposing the raw connection |
| `close()` | `None` | Closes the connection |
| `begin_batch()` / `end_batch()` / `flush()` | `None` | Deferred-commit mode. A data POST carrying thousands of frames wraps its work in `begin_batch`/`end_batch` so mutations land in one transaction — 10–100× cheaper for many-small-files uploads. `flush()` commits without leaving the mode |
| `init_session(session_id, resume_token, expires_at, target_dir=".", meta_json=None)` | `None` | `INSERT OR REPLACE` with an explicit column list (positional VALUES would break under the additive ALTERs), `epoch=1`, state `active` |
| `set_auth_json(auth_json)` | `None` | Persists authorize-hook quotas + context so they survive restart / lazy-resume on another worker |
| `get_session()` | `sqlite3.Row \| None` | Full session row |
| `get_target_dir()` / `get_expires_at()` / `set_expires_at(s)` | `str` / `str` / `None` | Target dir and expiry accessors |
| `get_state()` | `SessionState` | Defaults to `ACTIVE` when the row is missing |
| `set_state(state)` | `None` | Also bumps `updated_at` |
| `get_epoch()` / `increment_epoch()` | `int` | Leg fencing counter |
| `upsert_node(node_id, parent_id, kind, name, size=None, mtime_ms=None)` | `bool` | `INSERT OR REPLACE` into `nodes`; for FILE kind also `INSERT OR IGNORE` into `files`. Returns `True` **iff a new `files` row was created**, letting the caller keep the file-count quota without SQL aggregates |
| `get_node(node_id)` / `set_node_status(node_id, status)` | `Row \| None` / `None` | Node accessors |
| `get_file(node_id)` | `Row \| None` | File progress row |
| `set_accepted_offset(node_id, offset)` | `None` | Durable resume point |
| `set_file_final(node_id, final_size, local_path)` | `None` | Records the client-declared final size and the on-disk path at FILE_CLOSE |
| `get_open_files()` | `list[Row]` | `files JOIN nodes` where `n.status IN ('open')` |
| `get_all_files()` | `list[Row]` | `(node_id, accepted_offset, status)` for every file — feeds `RESUME_OK` |
| `add_pruned` / `is_pruned` / `get_pruned_nodes` | `None` / `bool` / `list[int]` | Server-initiated subtree drops |
| `add_rejected(node_id, code, reason)` / `is_rejected` / `get_rejected_files` | `None` / `bool` / `list[int]` | `add_rejected` also sets the node status to `rejected` |
| `update_root_summary(scan_done, scan_est, body_done, body_est, sealed)` | `None` | Writes the single `id=1` row |
| `get_root_summary()` | `dict` | Five keys; returns all-zero defaults if the row is somehow absent |
| `count_nodes()` | `int` | `COUNT(*) FROM nodes` — the commit node-count invariant |
| `count_committed_files()` | `tuple[int, int]` | `(count, COALESCE(SUM(accepted_offset),0))` over `status NOT IN ('rejected','pruned')` |
| `get_incomplete_files()` | `list[dict]` | Non-rejected/pruned files where `final_size IS NULL OR accepted_offset != final_size` |
<!-- ⬆️ 51 ⬆️ -->

### 3.5 Session manager (`session_manager.py`)

#### `FileWriter`

One open file handle per in-flight file, keyed by `node_id` in `LiveSession.writers`.

| Member | Description |
|---|---|
| `__init__(path, node_id, accepted_offset=0)` | `mkdir(parents=True, exist_ok=True)` on the parent. `accepted_offset == 0` → `open(path, "wb")`; otherwise `open(path, "r+b")`, `seek(accepted_offset)`, `truncate()` — a resume discards any bytes past the last durably-recorded offset |
| `async write(data, offset) -> int` | Raises `ValueError(f"bad offset: expected {…}, got {…}")` on mismatch; otherwise `await asyncio.to_thread(self._sync_write, data)` and returns the new `accepted_offset` |
| `_sync_write(data)` | `write()` + `flush()` — no `fsync`; durability against power loss is not claimed, only against process crash |
| `close()` | Closes the handle |
<!-- ⬆️ 52 ⬆️ -->

#### `LiveSession`

Constructor keywords and their defaults: `target_dir="."`, `staging_prefix=DEFAULT_STAGING_PREFIX`, `session_resume_ttl=3600`, `leg_idle_timeout=60`, `publish_timeout=30`, `max_chunk_bytes=262144` (256 KiB), `conflict_check=True`.
<!-- ⬆️ 53 ⬆️ -->

At construction it restores state that must survive a restart or a lazy-resume on a different worker: `meta_json` → `client_meta`, and `auth_json` → `quota_max_bytes`, `quota_max_files`, `auth_context` (unparseable JSON is logged as a warning and ignored, not fatal). Running quota totals `files_seen` / `bytes_accepted` are then seeded from `db.count_committed_files()`.
<!-- ⬆️ 54 ⬆️ -->

| Member | Type | Description |
|---|---|---|
| `state` | property → `SessionState` | Reads through to SQLite every access (never cached) |
| `expires_at` | property → `str` | Reads through to SQLite |
| `epoch` | `int` | Leg fencing counter, mirrored from the DB |
| `writers` | `dict[int, FileWriter]` | One writer per open file |
| `ws` | `Any` | Duck-typed control channel; only `send_json(dict)` is ever called — the core never imports FastAPI |
| `conflict_state` | `str` | FSM: `"clean"` → `"conflict_dir"` → `"conflict_files"` |
| `dropped_nodes` | `set[int]` | Nodes rejected at ingest (illegal name); counted in the commit node-count invariant so a tree with a few bad names can still commit |
| `publish_action` | `str \| None` | Set by the client's ACTION message |
| `validate_seq(seq)` / `advance_seq(seq)` / `validate_and_advance_seq(seq)` | methods | `seq == last_data_seq + 1`. `advance_seq` is called only after the POST body is fully handled, so a client may safely retry a failed POST with the same seq (a genuine duplicate then gets `seq_mismatch`) |
| `attach_leg(leg_id) -> int` | method | See §3.3; returns the new epoch |
| `detach_leg()` | method | See §3.3 |
| `async process_frame(frame, leg_id)` | method | **Leg fence**: frames whose `leg_id` differs from `self.leg_id` are logged and dropped. Resets the idle timer, then dispatches on frame type |
| `async prune_node(node_id, code, reason)` | method | Marks pruned, closes the writer, sends `PRUNE_NODE` |
| `async reject_file(node_id, code, reason)` | method | Marks rejected, closes the writer, sends `REJECT_FILE` |
| `async send_flow(paused, reason)` | method | Flips state and sends `FLOW` |
| `apply_auth(max_total_bytes, max_files, context)` | method | Sets quotas and persists them via `set_auth_json`. A non-JSON-serializable `context` is kept in memory but persisted as `{}` with a warning |
| `async abort_quota(reason)` / `async abort_storage_error(exc)` | methods | Terminal aborts (§3.3) |
| `async try_commit() -> dict \| None` | method | The commit gate (below) |
| `build_resume_ok() -> dict` | method | `{t, epoch, expires_at, root_summary, files[{node_id, accepted_offset, status}], pruned_nodes, rejected_files}`; per-file status is normalized to `"rejected"` / `"closed"` / `"open"` |
| `send_control(msg)`, `_send_file_ack`, `_send_nack` | methods | All wrap `ws.send_json` in try/except — a dead control channel logs but never raises into the ingest path |
<!-- ⬆️ 55 ⬆️ -->

#### Frame handlers

| Handler | Frame | Behavior |
|---|---|---|
| `_handle_node` | `NODE` | Skips pruned nodes. Illegal name → adds to `dropped_nodes` and `reject_file(…, "illegal_name", …)` (rejecting rather than ignoring stops an `unknown_node` NACK storm). Unknown non-root parent → `NACK_CHUNK{reason:"unknown_node"}` and **the node is not stored** (its payload path could not be resolved, or would silently resolve short). Otherwise upserts; a newly created file row increments `files_seen` and may trip `abort_quota`. DIR nodes are `mkdir`-ed immediately; an `OSError`/`ValueError` there (e.g. a sibling FILE node already claimed the path) rejects just that node with `fs_conflict` rather than failing the upload |
| `_handle_file_open` | `FILE_OPEN` | Skips rejected/pruned. Unknown node or unresolvable path → `unknown_node` NACK. A duplicate `FILE_OPEN` pops and closes the previous writer first so the handle cannot leak. An `OSError` in `_FATAL_STORAGE_ERRNOS` aborts the session; any other `OSError` rejects the single file with `fs_conflict` so the rest of the tree still commits |
| `_handle_file_chunk` | `FILE_CHUNK` | The ordered validation gate — see below |
| `_handle_file_close` | `FILE_CLOSE` | Pops and closes the writer, `set_file_final(node_id, f.size_sent, path)`, status → `closed`, sends a final `FILE_ACK` |
| `_handle_dir_close` | `DIR_CLOSE` | Status → `closed` |
| `_handle_summary` | `SUMMARY` | Merges monotonically: `max()` of each scan/estimate counter, `sealed` OR-ed, and **`body_done_bytes` is kept server-side** — the client's number is ignored |
| `_handle_session_end` | `SESSION_END` | Writes the client's final scan counters with `sealed=True`, closes all writers, state → `COMMITTING` |
| `_handle_client_abort` | `CLIENT_ABORT` | Closes writers, state → `ABORTED` |
<!-- ⬆️ 56 ⬆️ -->

`_handle_file_chunk` performs its checks in a fixed order, and every rejection path leaves `accepted_offset` untouched:

1. Rejected/pruned node → silently ignored.
2. No writer → `NACK_CHUNK` with `reason = "unknown_node"` if the node is unknown, else `"bad_offset"`, carrying the DB's `accepted_offset`.
3. `f.length > self.max_chunk_bytes` → `NACK_CHUNK{reason:"server_policy"}`.
4. `bytes_accepted + f.length > quota_max_bytes` → `abort_quota` (checked **before** writing).
5. **Checksum**: when `f.checksum_kind == ChecksumKind.CRC32C`, `crc32c(f.payload)` (initial `0`) must equal `f.checksum`, else `NACK_CHUNK{reason:"bad_checksum"}` — nothing is written and the offset does not advance, so the client simply re-sends from `expected_offset`.
6. `f.offset != writer.accepted_offset` → `NACK_CHUNK{reason:"bad_offset"}`.
7. Write. An `OSError` whose errno is in `_FATAL_STORAGE_ERRNOS = {ENOSPC, EDQUOT, EROFS, EFBIG}` calls `abort_storage_error` (retrying cannot clear a capacity failure — a client that kept resending would just spin); any other exception yields `NACK_CHUNK{reason:"server_policy"}`.
8. On success: `set_accepted_offset`, `bytes_accepted += f.length`, `body_done_bytes` incremented in `root_summary`, and a cumulative `FILE_ACK` is emitted roughly every 256 KiB (`new_offset % (256*1024) < f.length or f.length == 0`).
<!-- ⬆️ 57 ⬆️ -->

CRC-32C is therefore verified in exactly one place, per chunk, at ingest — and never recomputed at close, commit, or publish.
<!-- ⬆️ 58 ⬆️ -->

#### Commit invariants (`try_commit`)

Returns `None` unless the state is `COMMITTING`. Any surviving writers are closed with a warning first. Then two invariants:

1. **Node count** — `db.get_root_summary()["scan_done_units"]` (the client's final scan total from `SESSION_END`) must equal `db.count_nodes() + len(self.dropped_nodes)`. This closes the hole where NODE frames lost with a failed data POST would make whole files invisible to the completeness check below.
2. **Completeness** — `db.get_incomplete_files()` must be empty; the retry payload lists `{node_id, accepted_offset}` for each.
<!-- ⬆️ 59 ⬆️ -->

Either failure calls `_commit_retry_exhausted()`, which compares the tuple `(count_nodes(), accepted_bytes)` against `_commit_progress`. If it grew, the counter resets to 0 and the loop continues; only `MAX_COMMIT_RETRIES = 5` *consecutive no-progress* rounds trip the guard. A large-file resume that legitimately needs many rounds keeps progressing and is never falsely killed, while a genuinely stuck file (ENOSPC, unsatisfiable write) stalls and fails. Non-exhausted → state back to `ACTIVE`, `final_seq_seen = False`, `last_data_seq = -1`, `COMMIT_RETRY` sent. Exhausted → state `FAILED` plus `SESSION_ABORT{code:"commit_failed"}`. On success → `COMMITTED` and `{"t": "COMMIT_OK", "files": file_count, "bytes": total_bytes}` is both sent and returned.
<!-- ⬆️ 60 ⬆️ -->

#### Conflict pre-check (ingest-time)

`_check_conflict(node_id, is_dir)` runs on every NODE frame when `conflict_check` is true. `_resolve_target_path` rebuilds where the node *would* land under `target_dir` (resolving a relative `target_dir` against `base_dir`) and returns `None` if the resolved path escapes `base_dir`. A dir-vs-dir match advances `"clean" → "conflict_dir"` silently; anything else jumps to the terminal `"conflict_files"` and sends `{"t":"ASK","code":"target_conflict","node_id":…,"name":…}` exactly once — `name` is the conflicting basename only, never a server-side path, and `code`/`node_id`/`name` are additive fields older clients ignore. When a `map_file` hook is configured the transport passes `conflict_check=False`, because the ingest check inspects the *client's* layout which no longer predicts the final one; conflicts then surface at publish instead.
<!-- ⬆️ 61 ⬆️ -->

#### `SessionRegistry`

An in-process `dict[str, LiveSession]` guarded by an `asyncio.Lock`, with **no startup scan** — stale staging directories are reclaimed by the Redis-driven sweeper in the transport.
<!-- ⬆️ 62 ⬆️ -->

| Method | Description |
|---|---|
| `__init__(base_dir, staging_prefix=".incoming", **defaults)` | `defaults` are forwarded verbatim to every `LiveSession` constructor |
| `async create(session_id, resume_token, leg_id, expires_at, target_dir=".", base_dir=None, meta_json=None)` | Raises `ValueError` if the id already exists. `base_dir` overrides the registry base for this one session (per-user home from the authorize hook) so staging lives inside it and publish stays a same-filesystem rename. Opens the DB, `init_session`, constructs the `LiveSession`, `attach_leg` |
| `async resume(session_id, resume_token, leg_id)` | `KeyError` if unknown, `PermissionError("invalid resume token")` on token mismatch, `ValueError` for a non-resumable state; otherwise `attach_leg` |
| `get(session_id)` | Non-locking dict lookup |
| `async remove(session_id)` | Pops, `detach_leg()`, `db.close()` |
| `async recover_session(session_id, staging_path)` | Opens `staging_path / "state.sqlite"`; returns `None` if missing, unopenable, the sessions table is empty, or the state is terminal. Forces `ACTIVE`/`PAUSED_BY_SERVER`/`COMMITTING` → `WAITING_RESUME`. Derives the session's base dir as `staging_path.parent` — correct by construction (`staging = <base>/<prefix>.<sid>`) and the reason per-user base dirs need not be persisted anywhere else. If the id raced into the registry meanwhile, the freshly opened DB is closed and the existing session returned |
| `all_sessions()` | Shallow copy of the registry dict |
<!-- ⬆️ 63 ⬆️ -->

### 3.6 Publish (`publish.py`)

Two publish strategies plus the helpers they share.

| Export | Signature | Description |
|---|---|---|
| `ConflictError` | `__init__(conflicting_files: int)` | Message `"{n} conflicting file(s)"`; `.conflicting_files` is read by the transport to build the client ASK |
| `MappingError` | `Exception` | Escape attempt, illegal segment, or two files mapped to one destination |
| `list_payload_files` | `(base_dir, session_id, prefix) -> list[tuple[str, int]]` | `os.walk` over `payload/`, POSIX-relative paths and `st_size`, sorted. `FileNotFoundError` if `payload/` is absent |
| `validate_mapped_path` | `(rel: str) -> None` | Must be relative and non-empty; each `/`-separated segment passes `validate_node_name` |
| `detect_conflicts` | `(target_dir, payload) -> int` | Recursive count. Dir-vs-dir recurses (auto-merge, not a conflict); file-vs-file counts 1; a type mismatch counts 1 |
| `publish_session` | `(base_dir, session_id, target_dir, prefix, action=None) -> list[str]` | Whole-tree publish |
| `publish_session_mapped` | `(base_dir, session_id, target_dir, mapping, prefix, action=None) -> list[str]` | Per-file publish driven by the `map_file` hook |
<!-- ⬆️ 64 ⬆️ -->

**`publish_session`** creates `target_dir`, counts conflicts, and raises `ConflictError` if any exist while `action is None`. With `action == "merge_overwrite"` it runs `_merge_tree`, which recurses into matching directories, `os.replace`s colliding files, `unlink`s a file standing where a directory must go, and `os.rename`s everything else. With no conflicts it takes the fast path: one `os.rename` per **top-level entry** of `payload/`, so an entire uploaded directory moves with a single syscall. Returns the published entry names.
<!-- ⬆️ 65 ⬆️ -->

**`publish_session_mapped`** takes `mapping: dict[str, str]` (payload-relative source → target-relative destination); sources absent from the mapping keep their client layout. It builds and fully validates the plan **before moving anything** — per-segment validation, a `dest.resolve().is_relative_to(target_dir.resolve())` containment check, and a `seen` dict that raises `MappingError(f"two files map to {dest_rel!r}: …")` on collision — so a consumer-hook bug cannot leave the publish half-applied. It then counts conflicts against the plan and raises `ConflictError` if `action is None`. It materializes **files only**: parent directories are created on demand and empty client directories are not preserved.
<!-- ⬆️ 66 ⬆️ -->

Both paths finish with `_cleanup_staging(sd)`, a `shutil.rmtree(..., ignore_errors=True)` that removes `state.sqlite`, the now-empty `payload/`, and the staging directory itself.
<!-- ⬆️ 67 ⬆️ -->

**Atomicity.** Each individual file lands via `os.rename` (or `os.replace` when overwriting), which is atomic on a POSIX filesystem — a reader never sees a partial file at the destination. The publish *as a whole* is not transactional: it is a loop of renames, so a crash midway leaves some entries published and the rest in staging. The design mitigations are (a) full plan validation before the first move in mapped mode, and (b) `AuthResult.base_dir`, which keeps staging and target on one filesystem so `rename` cannot degrade into a copy.
<!-- ⬆️ 68 ⬆️ -->

### 3.7 Hooks (`hooks.py`)

Three extension points, resolved from dotted `pkg.module:attr` strings (env-driven) or passed as callables (library embedding). The referenced module only has to be importable — no plugin packaging.

| Hook | Type alias | Fires | Return contract |
|---|---|---|---|
| `authorize` (`MFUP_AUTHORIZE`) | `AuthorizeHook = Callable[[AuthRequest], Awaitable[Optional[AuthResult]]]` | Once per HELLO, **before the session is created** | `AuthResult(...)` allows (optionally constraining); `None` **vetoes** → client gets `SESSION_ABORT(auth_failed)`. Raising is treated as a deny (logged server-side, generic reason to the client) |
| `map_file` (`MFUP_MAP_FILE`) | `MapFileHook = Callable[[FileMapRequest], Awaitable[Optional[str]]]` | Once per file at **publish** time | `str` — new path relative to `target_dir`; `None` — keep the client's layout for that file. Two files mapping to one destination, or an escaping/absolute/illegal path, fails publish with `mapping_conflict` / `MappingError`. Because it runs at publish, it need not be deterministic across transfer retries |
| `on_committed` (`MFUP_ON_COMMITTED`) | `OnCommittedHook = Callable[[CommitEvent], Awaitable[Optional[str]]]` | Right after a session commits | `"publish"` — server publishes immediately (the browser's own publish call then 404s, harmlessly); `None` — do nothing, publish stays client-driven. Raising is logged and treated as `None` so a broken hook cannot strand committed sessions |
<!-- ⬆️ 69 ⬆️ -->

`authorize` is the only hook that can veto. `map_file` cannot block a publish but can fail it by returning an unusable path. `on_committed` can only *add* a server-side publish.
<!-- ⬆️ 70 ⬆️ -->

#### Hook payloads

```python
@dataclass(frozen=True)
class AuthRequest:
    session_id: str
    target_dir: str
    headers: Mapping[str, str]   # WebSocket handshake headers (cookies, Authorization, …)
    client: str                  # ASGI-reported "ip:port" or ""
    query: Mapping[str, str]     # WebSocket URL query parameters
    meta: Any = None             # HELLO.meta — UNTRUSTED client JSON; the hook validates it

@dataclass(frozen=True)
class AuthResult:
    max_total_bytes: Optional[int] = None   # exceeded → SESSION_ABORT(quota_exceeded)
    max_files: Optional[int] = None         # exceeded → SESSION_ABORT(quota_exceeded)
    base_dir: Optional[str] = None          # absolute per-session base (e.g. user home); created if missing
    target_dir: Optional[str] = None        # override / rewrite of the client request
    context: dict[str, Any] = field(default_factory=dict)

@dataclass(frozen=True)
class FileMapRequest:
    session_id: str
    path: str          # "/"-separated path inside the uploaded tree, as the client sent it
    name: str          # last segment of `path`
    size: int          # actual size on disk
    target_dir: str    # already authorized/mapped at HELLO
    meta: Any          # client session meta
    context: Mapping[str, Any]   # AuthResult.context

@dataclass(frozen=True)
class CommitEvent:
    session_id: str
    target_dir: str
    base_dir: str
    staging_dir: str   # absolute staging dir holding the committed payload
    files: int         # as sent in COMMIT_OK
    bytes: int
    meta: Any
    context: Mapping[str, Any]
```
<!-- ⬆️ 71 ⬆️ -->

`AuthResult.base_dir` overrides `MFUP_BASE_DIR` for that session: staging is created inside it, a relative `target_dir` resolves against it, and the containment check confines the session to it. `AuthResult.target_dir` may prefix or rewrite the client's request (`target_dir=f"incoming/{req.target_dir}"`); escapes remain impossible because the resolved target must stay within the session's `base_dir` or HELLO is refused with `bad_target_dir`. `AuthResult.context` is a free-form correlation bag, never sent to the client.
<!-- ⬆️ 72 ⬆️ -->

#### Loaders

| Function | Behavior |
|---|---|
| `load_hook(dotted) -> Callable` | Splits on `":"`; raises `ImportError(f"invalid hook path {dotted!r}: expected 'package.module:callable'")` when either half is missing, then `importlib.import_module` + `getattr`. Import/attribute errors propagate loudly — a misconfigured hook must fail at startup rather than silently run allow-all |
| `load_authorize_hook(dotted \| None)` | `None` → returns `None` **and logs a warning**: "running WITHOUT authorization (allow-all). Do not do this in production." |
| `load_map_file_hook(dotted \| None)` | `None` → identity layout |
| `load_on_committed_hook(dotted \| None)` | `None` → client-driven publish |
| `resolve_hook(ref)` | Accepts a callable (library embedding, `MfupConfig(authorize=my_func)`) or a dotted string (env-driven); `None` passes through |
<!-- ⬆️ 73 ⬆️ -->

### 3.8 Redis expiry index (`redis_index.py`)

A thin `redis.asyncio` wrapper over exactly two key patterns:

| Key | Redis type | Contents |
|---|---|---|
| `mfup:sessions` (`SESSIONS_KEY`) | sorted set | member = `session_id`, score = `expires_at.timestamp()` (float unix seconds) |
| `mfup:meta:{session_id}` (`META_PREFIX = "mfup:meta:"`) | hash | fields `target_dir` and `staging_dir`, both full paths |
<!-- ⬆️ 74 ⬆️ -->

**No Redis-native TTL is ever set on either key** — `EXPIRE`/`SETEX` are not used. Expiry is entirely score-based and swept: `ZRANGEBYSCORE → HGETALL meta → rmtree(staging_dir) → ZREM + DEL meta`. That design is the whole point of the index: cleanup needs no `iterdir` of the base directory and no SQLite opens.
<!-- ⬆️ 75 ⬆️ -->

| Method | Redis commands | Description |
|---|---|---|
| `__init__(redis_url="redis://redis:6379/0")` | — | `aioredis.from_url(..., decode_responses=True)`, so all reads return `str` |
| `async register(session_id, expires_at, target_dir, staging_dir)` | `ZADD` + `HSET` in one pipeline | Single round-trip registration |
| `async update_expiry(session_id, expires_at)` | `ZADD` | Re-scores an existing member. Wired to `LiveSession._on_expiry_change` by the transport, so each `detach_leg()` pushes the new resume deadline |
| `async get_expired(now=None)` | `ZRANGEBYSCORE(-inf, now)` | Sweeper input; `now` defaults to `datetime.now(timezone.utc)` |
| `async get_not_expired(now=None)` | `ZRANGEBYSCORE(now, +inf)` | Still-alive sessions |
| `async is_registered(session_id)` | `ZSCORE` | `True` iff a score exists |
| `async get_meta(session_id)` | `HGETALL` | `SessionMeta \| None`; missing fields default to `target_dir="."`, `staging_dir=""` |
| `async all_sessions()` | `ZRANGE(0, -1)` | Every id regardless of expiry |
| `async remove(session_id)` | `ZREM` + `DEL` in one pipeline | Full deregistration |
| `async close()` | — | `await self._redis.aclose()` |
<!-- ⬆️ 76 ⬆️ -->

`SessionMeta` is a `__slots__ = ("target_dir", "staging_dir")` value object.
<!-- ⬆️ 77 ⬆️ -->

**Failure behavior.** Every method awaits the Redis client directly with no try/except, no retry, and no in-process fallback — an unreachable Redis surfaces as `redis.ConnectionError` to the caller. The transport treats the index as mandatory (`MfupEngine._require_index()` at HELLO, at publish, and in the sweeper loop), so with Redis down: new sessions cannot be registered, the expiry sweeper cannot enumerate anything, and stale staging directories accumulate until it returns. In-flight ingest itself does not touch Redis — chunk writes, offsets, and commit invariants all run against the per-session SQLite, so an already-established session keeps transferring; only registration, expiry re-scoring, and cleanup are blocked.
<!-- ⬆️ 78 ⬆️ -->

### 3.9 Concurrency, failure handling, and retention

**Locking.** `SessionRegistry._lock` (an `asyncio.Lock`) guards the four mutating paths — `create`, `resume`, `remove`, `recover_session` — so registry-dict races and duplicate DB opens are impossible. `LiveSession._lock` is constructed but never acquired anywhere in the package; per-session serialization comes instead from the leg invariant ("each session has at most one active leg", enforced by the `leg_id` fence in `process_frame` and by `attach_leg` closing all writers) plus the single-writer-per-file rule (`writers` is keyed by `node_id`, and a duplicate `FILE_OPEN` closes the prior handle before replacing it). SQLite itself runs in WAL mode with `busy_timeout=3000` and a 5-second connect timeout.
<!-- ⬆️ 79 ⬆️ -->

**Fencing.** Two independent counters: `epoch`, bumped in SQLite on every `attach_leg`, and `last_data_seq`, a strictly-incrementing per-leg POST sequence reset to `-1` on attach. `advance_seq` is deliberately called only after a POST body is fully processed, so retrying a failed POST with the same seq is safe.
<!-- ⬆️ 80 ⬆️ -->

**Partial writes.** Bytes reach disk with `write()` + `flush()` (no `fsync`) and `accepted_offset` is committed to SQLite only *after* the write returns, so the persisted offset is never ahead of the data. On resume, `FileWriter` opens `r+b`, seeks to `accepted_offset` and `truncate()`s — any bytes written past the last recorded offset are discarded rather than trusted. A chunk that fails CRC, offset, or size validation is never written at all.
<!-- ⬆️ 81 ⬆️ -->

**Crash recovery.** No startup directory scan. The transport hands `recover_session(session_id, staging_path)` a staging path; the core reopens `state.sqlite`, refuses terminal states, forces resumable ones to `WAITING_RESUME`, and derives the base dir as `staging_path.parent`. Quotas and client meta are rehydrated from `auth_json` / `meta_json`, and `files_seen` / `bytes_accepted` are recounted from the DB so quota accounting stays accurate across the restart. A session caught mid-`COMMITTING` is recovered too — the client reconnects and re-sends `SESSION_END`.
<!-- ⬆️ 82 ⬆️ -->

**Fatal vs. transient failures.** `_FATAL_STORAGE_ERRNOS = {ENOSPC, EDQUOT, EROFS, EFBIG}` is the dividing line. Those errno values abort the session (state `ABORTED`, `SESSION_ABORT` with `storage_full` or `storage_error`) because retrying cannot clear them. Every other `OSError` produces a `NACK_CHUNK{reason:"server_policy"}` and lets the client retry, bounded client-side. Failures scoped to a single node — an illegal name, a file/directory path collision — call `reject_file` so the remainder of the tree still commits.
<!-- ⬆️ 83 ⬆️ -->

**Retention / GC.** Three mechanisms compose: `session_resume_ttl` (default 3600 s) sets the new `expires_at` on every `detach_leg`, propagated to the Redis ZSET score via `_on_expiry_change`; `leg_idle_timeout` (default 60 s) is an `asyncio` `call_later` timer reset on every processed frame that calls `detach_leg()` on expiry; and the Redis sweeper reclaims expired staging directories by path without touching the filesystem index or SQLite. A successful publish removes the staging directory itself through `_cleanup_staging`'s `shutil.rmtree(..., ignore_errors=True)`, and an `ABORTED` session is left for the transport's cleanup path to reclaim.
<!-- ⬆️ 84 ⬆️ -->

---

## 4. mfup-fastapi — Transport Layer

`mfup-fastapi` binds the storage/session engine of `mfup-core` to FastAPI. It exposes one class, `MfupEngine`, that owns all mutable state (session registry, Redis index, sweeper task, consumer hooks) and publishes an `APIRouter` carrying every MFUP/2 endpoint under a fixed `/mfup/*` namespace plus `/health`. There is no module-level state: two engines in one process are two independent instances. A host application mounts the router at any prefix and drives `startup()`/`shutdown()` from its own lifespan; the standalone server (`python -m mfup_fastapi`, or `uvicorn mfup_fastapi.app:app`) is `create_app(MfupConfig.from_env())`. Control traffic (session negotiation, per-file ACK/NACK, flow control, commit) runs over a WebSocket; payload bytes run over HTTP POSTs of length-prefixed binary frames.
<!-- ⬆️ 85 ⬆️ -->

Source: `server/mfup-fastapi/mfup_fastapi/`
<!-- ⬆️ 86 ⬆️ -->

### 4.1 Package surface (`__init__.py`)

| Export | Type | Description |
|--------|------|-------------|
| `create_app(config=None)` | function | Builds a standalone `FastAPI(title="MFUP/2 Server", lifespan=engine.lifespan)`, includes `engine.router` at root, stores the engine on `app.state.mfup_engine`, returns the app. Defaults to `MfupConfig.from_env()`. Imports `fastapi` lazily so importing the package never reads the environment. |
| `MfupConfig` | dataclass | All configuration (§4.7). |
| `MfupEngine` | class | The engine + router. |
| `PublishError` | exception | Base class for typed publish failures. |
| `SessionNotFound` | exception | `publish()` called for an unknown session id. |
| `NotCommitted` | exception | Session is not in `COMMITTED`; carries `.state` (str). |
| `TargetEscapes` | exception | `target_dir` resolves outside the session base dir. |
| `MapFileHookError` | exception | The `map_file` hook raised; carries `.path` (the payload-relative path). |
| `reconcile_orphans(...)` | async function | Filesystem retention safety net (§4.6). |
<!-- ⬆️ 87 ⬆️ -->

Integration in a host app:

```python
engine = MfupEngine(MfupConfig(base_dir=Path("/srv/uploads"),
                               redis_url="redis://localhost:6379/0",
                               authorize=my_authorize))
app = FastAPI(lifespan=engine.lifespan)
app.include_router(engine.router, prefix="/api/uploads")
```
<!-- ⬆️ 88 ⬆️ -->

The router's paths are relative to the mount prefix, so the browser client's `serverUrl` must point at that same prefix (`https://host/api/uploads`). Consumers with their own lifespan call `engine.startup()` / `engine.shutdown()` from it instead of using `engine.lifespan`.
<!-- ⬆️ 89 ⬆️ -->

### 4.2 Standalone entry points (`app.py`, `__main__.py`)

`app.py` is the only module that builds an app at import time — `app = create_app()` for uvicorn's `module:attr` convention — and calls `logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")`. Library consumers never import it.
<!-- ⬆️ 90 ⬆️ -->

`__main__.py` runs `uvicorn.run("mfup_fastapi.app:app", host=..., port=..., log_level="info")`.
<!-- ⬆️ 91 ⬆️ -->

| Env var | Default | Effect |
|---------|---------|--------|
| `MFUP_HOST` | `0.0.0.0` | uvicorn bind host |
| `MFUP_PORT` | `8070` | uvicorn bind port |
<!-- ⬆️ 92 ⬆️ -->

All other configuration for the standalone path comes from `MfupConfig.from_env()`.
<!-- ⬆️ 93 ⬆️ -->

### 4.3 Routes (`engine.py` — `MfupEngine._build_router`)

Seven HTTP routes and one WebSocket endpoint. Paths are shown relative to the router mount prefix.

| Method | Path | Auth | Request | Success response | Error responses |
|--------|------|------|---------|------------------|-----------------|
| `WEBSOCKET` | `/mfup/control` | `authorize` hook on `HELLO`; `resume_token` on `RESUME` | JSON control messages | JSON control messages | `SESSION_ABORT` frame then close |
| `POST` | `/mfup/data/{session_id}/{leg_id}?seq=&final=&epoch=` | header `x-mfup-token` = session `resume_token` | binary body of length-prefixed frames | `200 {"ok": true, "bytes_received": int, "frames": int}`, plus `"commit": {"files": int, "bytes": int}` when a commit succeeded on this POST | `400 {"error":"epoch_required"}`, `403 {"error":"invalid token"}`, `409 {"error":"stale leg","expected":<leg>}`, `409 {"error":"stale_epoch","got":int,"expected":int}`, `409 {"error":"session in state <s>"}`, `409 {"error":"data_after_final","detail":...}`, `409 {"error":"seq_mismatch","got":int,"expected":int}`, `410 {"error":"session not found"}`, `413 {"error":"body_too_large"}`, `500 {"error":"body_read_failed"}`, `500 {"error":"data_stream_error","detail":str,"bytes_received":int}` |
| `POST` | `/mfup/probe/{session_id}` | header `x-mfup-token` | arbitrary streamed body | `200 {"ok": true, "total_bytes": int}` | `403 {"error":"invalid token"}`, `410 {"error":"session not found"}`, `410 {"error":"session in terminal state <s>"}` |
| `GET` | `/mfup/sessions` | admin token | — | `200` JSON array of `{session_id, state, epoch, leg_id, expires_at}` | `403 {"error":"admin routes disabled"}` / `403 {"error":"forbidden"}` |
| `GET` | `/mfup/sessions/{session_id}` | admin token | — | `200 {session_id, state, epoch, leg_id, expires_at, root_summary}` | `403` (as above), `404 {"error":"not found"}` |
| `POST` | `/mfup/sessions/{session_id}/publish` | header `x-mfup-token` (NOT admin) | — | `200 {"published": [str, ...]}` | `403 {"error":"invalid token"}`, `403 {"error":"target_dir escapes base directory"}`, `404 {"error":"not found"}`, `404 {"error": <FileNotFoundError text>}`, `409 {"error":"cannot publish session in state <s>"}`, `409 {"error":"mapping_error","detail":str}`, `409 {"error":"conflict_files","conflicting_files":[...]}`, `500 {"error":"map_file_hook_error","path":str}` |
| `POST` | `/mfup/sweep` | admin token | — | `200 {"removed": [session_id, ...]}` | `403` (as above) |
| `GET` | `/health` | none | — | `200 {"status":"ok","protocol": PROTOCOL_VERSION, "crc32c": CRC32C_IMPL}` | — |
<!-- ⬆️ 94 ⬆️ -->

`PROTOCOL_VERSION` is `"MFUP/2"` and `CRC32C_IMPL` is `"native"`, both imported from `mfup_core.protocol`. Note `/health` sits outside the `/mfup/*` namespace.
<!-- ⬆️ 95 ⬆️ -->

**Admin gate** — `_admin_denied(request)` is a local helper applied to `GET /mfup/sessions`, `GET /mfup/sessions/{session_id}` and `POST /mfup/sweep`. It returns `403 {"error":"admin routes disabled"}` when `cfg.admin_token` is empty (the default — admin routes are off unless configured), and `403 {"error":"forbidden"}` when header `x-mfup-admin-token` does not match. `POST /mfup/sessions/{id}/publish` is deliberately not admin-gated: it is authenticated by the session's own `resume_token`, the same bearer as the data plane, because publishing moves files and destroys staging on behalf of that session.
<!-- ⬆️ 96 ⬆️ -->

### 4.4 Control channel — WebSocket `/mfup/control`

The socket is accepted immediately; the first message must be `HELLO` or `RESUME`, anything else gets `SESSION_ABORT/protocol_error` and a close. After the handshake the endpoint loops on `receive_json()` handling `CLIENT_ABORT` and `ACTION`; all other server→client traffic on this socket is emitted by the `LiveSession` (from `mfup-core`) as data frames are processed on the HTTP channel — `session.ws` is set to this socket during the handshake.
<!-- ⬆️ 97 ⬆️ -->

#### Client → server

| `t` | Fields | Effect |
|-----|--------|--------|
| `HELLO` | `v` (must equal `"MFUP/2"`), `session_id`, `leg_id`, `target_dir` (optional, default `"."`), `meta` (optional, arbitrary JSON) | Creates the session. Version mismatch → `SESSION_ABORT/bad_version`. `meta` is JSON-encoded and rejected with `SESSION_ABORT/protocol_error` if it exceeds `max_meta_bytes`; it is untrusted and is passed to `authorize` and `map_file`. |
| `RESUME` | `session_id`, `resume_token`, `leg_id` | Re-attaches to an existing session, transferring leg ownership to this socket. |
| `CLIENT_ABORT` | — | Sets state `ABORTED`, detaches the leg, breaks the loop (staging + Redis entry are then reclaimed in the `finally` block). |
| `ACTION` | `action` ∈ `"merge_overwrite"` \| `"cancel"` | Sets `session.publish_action`. `"cancel"` additionally sets `ABORTED`, detaches the leg and breaks the loop. Any other `action` value is ignored. |
<!-- ⬆️ 98 ⬆️ -->

`HELLO` handling in order: version check → `meta` size cap → `authorize` hook → per-session `base_dir` resolution → `target_dir` containment check → `registry.create(...)` → Redis registration → `apply_auth` → `HELLO_OK`.
<!-- ⬆️ 99 ⬆️ -->

- The **resume token is server-issued**: `secrets.token_urlsafe(32)`, generated during `HELLO` and returned in `HELLO_OK`. A client-chosen token would be a self-signed credential and useless as an authenticator for the data/probe/publish endpoints.
- The `authorize` hook receives `AuthRequest(session_id, target_dir, headers=dict(ws.headers), client="{host}:{port}", query=dict(ws.query_params), meta=client_meta)`. Returning `None` — or raising, which is caught and logged — yields `SESSION_ABORT/auth_failed`. A returned `target_dir` overrides the client's request (pin or map) but is still subject to containment. A returned `base_dir` must be absolute; a relative one is a misconfiguration and yields `SESSION_ABORT/auth_failed` with reason `"authorization misconfigured (relative base_dir)"`. The per-session base dir is `mkdir(parents=True, exist_ok=True)`'d and staging is created *inside* it, so publish stays a same-filesystem rename even when users live on separate mounts. `auth_result.max_total_bytes`, `max_files` and `context` are pushed into the session via `session.apply_auth(...)` and persisted so they survive restarts and lazy resume.
- Containment is checked by the module-level `_is_safe_target(base_dir, target_dir)`, which resolves the target (absolute as-is, relative against `base_dir`) and requires `relative_to(base_dir.resolve())` to succeed.
- `registry.create` raising `ValueError` (session id already exists) → `SESSION_ABORT/conflict`.
- Redis registration failure is **rolled back explicitly**: the in-memory session is removed and the staging dir `rmtree`'d, then `SESSION_ABORT/server_error` — otherwise a retried `HELLO` would hit `conflict` and the staging dir would be invisible to the zset-driven sweeper forever. On success the absolute staging path is stored in Redis meta (what makes the sweeper and lazy resume work with per-user base dirs) and `session._on_expiry_change` is wired to `index.update_expiry`.
<!-- ⬆️ 100 ⬆️ -->

`RESUME` handling: `registry.resume(session_id, resume_token, leg_id)`; on `KeyError` the engine attempts **lazy recovery** — it reads Redis meta for the session, and if a `staging_dir` is recorded, calls `registry.recover_session(...)` and retries the resume. This is what lets a session survive a deploy or failover onto a different worker: ownership transfers to whichever worker holds the WebSocket. Failure modes map to `SESSION_ABORT` codes `not_found` (`KeyError`), `auth_failed` (`PermissionError`, i.e. bad token) and `invalid_state` (`ValueError`, reason is the exception text).
<!-- ⬆️ 101 ⬆️ -->

#### Server → client

Emitted directly by `engine.py`:

| `t` | Fields | When |
|-----|--------|------|
| `HELLO_OK` | `epoch`, `expires_at`, `resume_token`, `limits: {max_chunk_bytes, max_open_files, max_pending_files}` | Successful `HELLO`. |
| `RESUME_OK` | `epoch`, `expires_at`, `root_summary`, `files: [{node_id, accepted_offset, status}]` (status ∈ `"open"`/`"closed"`/`"rejected"`), `pruned_nodes`, `rejected_files` | Successful `RESUME`. Built by `LiveSession.build_resume_ok()` from persisted SQLite state. |
| `PROBE_ACK` | `first_chunk_bytes` | Sent on the control socket when the first body chunk of a `POST /mfup/probe/{session_id}` arrives. |
| `SESSION_ABORT` | `code`, `reason` | Handshake failures (table below). Always followed by `ws.close()`. |
<!-- ⬆️ 102 ⬆️ -->

`SESSION_ABORT` codes raised by the transport layer:

| `code` | Cause |
|--------|-------|
| `bad_version` | `HELLO.v` ≠ `"MFUP/2"`; reason `"expected MFUP/2"` |
| `protocol_error` | `meta` exceeds `max_meta_bytes`, or first message is neither `HELLO` nor `RESUME` (reason `"expected HELLO or RESUME, got <t>"`) |
| `auth_failed` | `authorize` returned `None`/raised, hook returned a relative `base_dir`, or `RESUME` presented an invalid token |
| `bad_target_dir` | `target_dir` escapes the session base directory |
| `conflict` | Session id already exists in the registry |
| `server_error` | Redis registration failed (`"session index unavailable, retry later"`) |
| `not_found` | `RESUME` for an unknown/expired session |
| `invalid_state` | `RESUME` rejected by session state machine |
<!-- ⬆️ 103 ⬆️ -->

Emitted on the same socket by `mfup_core.session_manager.LiveSession` while HTTP data frames are processed (detailed in §3.5; listed here because they are part of this wire):

| `t` | Fields |
|-----|--------|
| `FILE_ACK` | `node_id`, `accepted_offset` |
| `NACK_CHUNK` | `node_id`, `expected_offset`, `reason` |
| `ASK` | `code` (`"target_conflict"`), `node_id`, `name` (conflicting basename only, never a server path) |
| `PRUNE_NODE` | `node_id`, `code`, `reason` |
| `REJECT_FILE` | `node_id`, `code`, `reason` |
| `FLOW` | `paused` (bool), `reason` |
| `COMMIT_RETRY` | `incomplete: [{node_id, accepted_offset}]`, and for the node-count invariant also `nodes_expected`, `nodes_seen` |
| `COMMIT_OK` | `files`, `bytes` |
| `SESSION_ABORT` | `code` ∈ `storage_full`, `storage_error`, `quota_exceeded`, `commit_failed`; `reason` |
<!-- ⬆️ 104 ⬆️ -->

The `ASK`/`ACTION` pair is the conflict negotiation: the server asks once when a target file already exists, and the client answers with `ACTION` (`merge_overwrite` or `cancel`).
<!-- ⬆️ 105 ⬆️ -->

### 4.5 Data channel — `POST /mfup/data/{session_id}/{leg_id}`

**Framing.** The body is a stream of MFUP/2 binary frames, `[4-byte big-endian length][1-byte tag][payload]` where the length covers tag + payload, decoded incrementally by `mfup_core.protocol.FrameReader` (`feed()` / `drain()`, 1 MiB max declared frame length). Attribution is carried inside the frames, not in the HTTP envelope: `FILE_CHUNK` frames carry `node_id` + absolute `offset`, `NODE` frames establish the `node_id` → name/parent tree, and `FILE_OPEN`/`FILE_CLOSE`/`DIR_CLOSE`/`SUMMARY`/`SESSION_END`/`CLIENT_ABORT` complete the vocabulary. The HTTP path and query only identify *which session and which leg* the frames belong to and where they sit in the POST ordering.
<!-- ⬆️ 106 ⬆️ -->

**Query/header envelope.**

| Parameter | Location | Required | Description |
|-----------|----------|----------|-------------|
| `session_id` | path | yes | Session to attribute frames to |
| `leg_id` | path | yes | Must equal the session's current `leg_id`, else `409 stale leg` with `"expected"` |
| `seq` | query (int) | yes | Monotonic POST sequence for this leg |
| `final` | query (int, default `0`) | no | `1` marks the last POST of the leg |
| `epoch` | query (int, default `-1`) | effectively yes | Fencing token; `< 0` → `400 epoch_required`. Mandatory by design so an old client omitting it cannot silently bypass stale-POST fencing |
| `x-mfup-token` | header | yes | Session `resume_token` |
<!-- ⬆️ 107 ⬆️ -->

**Validation order** (each short-circuits): session exists (else `410`) → token match (`403`) → leg match (`409`) → epoch present (`400`) → epoch match (`409 stale_epoch`) → state ∈ {`ACTIVE`, `PAUSED_BY_SERVER`} (`409`) → `final` not already seen for this leg (`409 data_after_final`) → `session.validate_seq(seq)` (`409 seq_mismatch` with `got`/`expected`).
<!-- ⬆️ 108 ⬆️ -->

**Two body strategies.** `buffered = content-length is present and ≤ cfg.max_buffered_body`.

- *Buffered (atomic)* — the whole body is accumulated first (aborting with `413 body_too_large` if it grows past the cap despite the declared length), then leg/epoch freshness is **re-checked after the awaited read**, then frames are fed and processed inside `session.db.begin_batch()` / `end_batch()`. `seq` advances (and `final_seq_seen` is set when `final=1`) only after the whole body processed without error. A body that never fully arrives returns `500 body_read_failed` with nothing applied and `seq` not advanced, so the client may retry the identical POST. Because advancing is deferred, a duplicate of an already-processed POST gets `seq_mismatch` with `expected == seq + 1`, which the client reads as "already delivered".
- *Streaming* — used for chunked or oversized bodies (the `duplex: "half"` long POST). `seq` advances and `final_seq_seen` is set **immediately**, before reading; frames apply as they arrive. Each loop iteration re-checks leg/epoch and breaks out on staleness. `session.db.flush()` runs every 500 frames. There is no retry-by-seq here: a broken stream recovers through `RESUME`, which allocates a new leg and epoch.
<!-- ⬆️ 109 ⬆️ -->

**Failure mid-POST.** Exceptions during frame processing are caught, recorded as `error_detail = "<ExcType>: <msg>"`, logged with a traceback, and `session.db.end_batch()` still runs in `finally`. The response is `500 {"error":"data_stream_error","detail":..., "bytes_received":...}`. In the buffered path an error means `seq` is not advanced, keeping the POST retryable.
<!-- ⬆️ 110 ⬆️ -->

**Commit.** A commit is attempted only when `final == 1` or a `SessionEndFrame` was seen *and* the session is in `COMMITTING`. It goes through `engine._try_commit(session)`, whose result (when non-`None`) is echoed as `"commit": {"files", "bytes"}` in the 200 response. Every request logs one line: bytes received, frame count, `final`, and any error.
<!-- ⬆️ 111 ⬆️ -->

### 4.6 Probe, publish, and lifecycle

#### Probe channel — `POST /mfup/probe/{session_id}`

A liveness/compatibility probe for the streaming upload path. It consumes the request body and, on the **first** chunk, pushes `PROBE_ACK {first_chunk_bytes}` down the session's control socket so the client can verify that real binary data reached the server — Firefox stringifies a `ReadableStream` body into 23 bytes of `"[object ...]"`, which this catches. A failure to send `PROBE_ACK` is logged, not fatal. Sessions in `COMMITTED`, `ABORTED` or `EXPIRED` are rejected with `410`.
<!-- ⬆️ 112 ⬆️ -->

#### Programmatic publish

| Member | Type | Description |
|--------|------|-------------|
| `MfupEngine.publish(session_id) -> list[str]` | async method | Publishes a `COMMITTED` session server-side. Used by `POST /mfup/sessions/{id}/publish`, by consumer backends directly, and by the `on_committed` auto-publish path. |
<!-- ⬆️ 113 ⬆️ -->

Behavior: resolves the session (`SessionNotFound` if absent), requires `SessionState.COMMITTED` (`NotCommitted(state)`), resolves `target_dir` against `session.base_dir` when relative, and re-runs `_is_safe_target` as defense in depth (`TargetEscapes`). With no `map_file` hook it calls `publish_session` via `asyncio.to_thread` (the renames/merge walk are synchronous filesystem work and must stay off the event loop). With a `map_file` hook it first lists payload files (`list_payload_files`, also off-loop), awaits the hook per file with `FileMapRequest(session_id, path, name, size, target_dir, meta, context)`, collects non-`None` results into a mapping, and calls `publish_session_mapped`. A raising hook is logged and re-raised as `MapFileHookError(rel)` with the original traceback suppressed. On success the session is removed from both the registry and the Redis index, and the list of published paths is returned. `ConflictError` and `MappingError` from `mfup-core` propagate to the caller (the HTTP endpoint maps both to `409`).
<!-- ⬆️ 114 ⬆️ -->

#### Commit hook wiring

| Member | Description |
|--------|-------------|
| `_try_commit(session)` | Wraps `session.try_commit()` and fires `_after_commit` when the result is non-`None` and the state reached `COMMITTED`, so no call site can forget the hook. Used by the data endpoint and by the WebSocket `finally` block. |
| `_after_commit(session, result)` | Invokes `on_committed` once per commit with `CommitEvent(session_id, target_dir, base_dir, staging_dir, files, bytes, meta, context)`. A raising hook is logged and swallowed — it must never damage the session. A return value of `"publish"` triggers `engine.publish(...)` immediately; a failure there is logged, not raised. |
<!-- ⬆️ 115 ⬆️ -->

#### Lifecycle

| Member | Description |
|--------|-------------|
| `startup()` | `mkdir` the base dir; construct `SessionIndex(redis_url)` and `SessionRegistry(base_dir, staging_prefix, session_resume_ttl, leg_idle_timeout, max_chunk_bytes, conflict_check=(map_file is None))`; recover sessions; reconcile orphans; start the sweeper task. |
| `shutdown()` | Cancel and await the sweeper task (absorbing `CancelledError`), then `await index.close()`. |
| `lifespan(app)` | `@asynccontextmanager` calling `startup()` then `shutdown()` in a `finally`. Drop-in for `FastAPI(lifespan=engine.lifespan)`. |
| `_require_registry()` / `_require_index()` | Assert that `startup()` has run; used by every endpoint. |
<!-- ⬆️ 116 ⬆️ -->

**Hook resolution happens in `__init__`, not at startup.** Callables pass through; dotted `"pkg.mod:func"` strings are imported eagerly via `resolve_hook` so a misconfigured hook fails at construction and never boots. When `config.authorize` is `None`, `load_authorize_hook(None)` is used, which logs the allow-all warning.
<!-- ⬆️ 117 ⬆️ -->

`conflict_check` is disabled whenever a `map_file` hook is present: with per-file mapping the client's layout no longer predicts final paths, so the ingest-time conflict `ASK` would be noise — publish-time conflict handling (`409` → client `ACTION`) takes over.
<!-- ⬆️ 118 ⬆️ -->

**Startup recovery.** Redis is queried for alive and expired session ids; for each alive id with recorded meta the registry re-opens the on-disk SQLite state via `recover_session(sid, Path(meta.staging_dir))` and re-wires `_on_expiry_change` to `index.update_expiry`. Sessions without meta are skipped with a warning. The whole recovery block is wrapped in `try/except` with `logger.exception` — a recovery failure does not prevent the server from booting.
<!-- ⬆️ 119 ⬆️ -->

**Sweeper** (`_sweeper`, background `asyncio.Task`). Every `sweep_interval` seconds: fetch expired ids from Redis, and for each `registry.remove(sid)`, `rmtree` the staging dir (taken from Redis meta, falling back to `staging_dir(base_dir, sid, staging_prefix)`), then `index.remove(sid)`. Every `reconcile_every` sweeps it additionally runs `reconcile_orphans`. All exceptions are logged and the loop continues. `POST /mfup/sweep` performs the same expired-session pass on demand (without the reconciliation) and returns the removed ids.
<!-- ⬆️ 120 ⬆️ -->

**`reconcile_orphans(base, registry, index, prefix, grace_seconds=600)`** is a free function (tests exercise it directly) and the retention safety net. The zset-driven sweeper can only clean what Redis still knows about; a staging dir becomes unreachable when cleanup was interrupted between `rmtree` and `ZREM`, when `rmtree` silently failed under `ignore_errors`, or when Redis lost the entry. It scans `base` for directories named `{prefix}.{session_id}` and deletes one only when **all three** hold: not in the in-memory registry, not registered in Redis, and last modified at least `grace_seconds` ago. If Redis is unreachable it skips the directory rather than delete something it cannot verify. After `rmtree` it also attempts `index.remove(sid)` as belt and braces, and returns the list of removed session ids.
<!-- ⬆️ 121 ⬆️ -->

**WebSocket disconnect and in-flight sessions.** `WebSocketDisconnect` and any other exception are caught and logged (never re-raised), then the `finally` block runs:

1. If the session is no longer the one in the registry (`registry.get(sid) is not session`), the local reference is dropped after clearing `session.ws`, and nothing else is touched. This is the publish race: `publish()` removes the session and **closes its SQLite handle** while the socket is still open, and touching `session.state`/`session.db` afterwards raises `sqlite3.ProgrammingError` inside `finally`, surfacing as a bogus ASGI exception after a perfectly good upload.
2. Otherwise: if the state is `COMMITTING`, one last `_try_commit(session)` is attempted — a client that disconnects right after its final POST still gets committed.
3. `session.ws` is cleared if it is still this socket, and `detach_leg()` runs when a leg is attached and the state is neither `COMMITTED` nor `ABORTED` (so a later `RESUME` can claim a fresh leg).
4. If the state is `ABORTED`, cleanup is immediate rather than deferred to the sweeper: `registry.remove(sid)`, `rmtree` of the staging dir (from Redis meta when available, else computed from `session.base_dir` — the per-session one, not the global base — and `staging_prefix`), and `index.remove(sid)`.
<!-- ⬆️ 122 ⬆️ -->

### 4.7 Configuration (`config.py`)

`MfupConfig` is a `@dataclass` with two construction paths: direct instantiation for library embedding (hooks as callables) and `MfupConfig.from_env(env=None)` for standalone/container deployment (hooks as dotted `"pkg.module:callable"` paths). No environment variable is read at import time anywhere in the package — only `from_env()` touches `os.environ`, and it accepts an explicit mapping for tests. Integer fields go through a local `_int(name, default)` helper. `HookRef = Union[str, Callable[..., Any]]`.
<!-- ⬆️ 123 ⬆️ -->

| Field | Type | Default | Env var | Effect |
|-------|------|---------|---------|--------|
| `base_dir` | `Path` | `Path("/tmp/mfup-uploads")` | `MFUP_BASE_DIR` | Global base directory for staging + publish; a per-session `base_dir` from the `authorize` hook overrides it for that session |
| `redis_url` | `str` | `"redis://redis:6379/0"` | `REDIS_URL` | Backing store for the session index (TTL zset + meta) |
| `session_resume_ttl` | `int` | `3600` | `MFUP_SESSION_RESUME_TTL` | Seconds a session stays resumable; sets `expires_at` at `HELLO` |
| `leg_idle_timeout` | `int` | `60` | `MFUP_LEG_IDLE_TIMEOUT` | Seconds of leg inactivity before the leg is detached |
| `max_chunk_bytes` | `int` | `262144` | `MFUP_MAX_CHUNK_BYTES` | Largest accepted chunk; advertised in `HELLO_OK.limits` |
| `max_open_files` | `int` | `1` | `MFUP_MAX_OPEN_FILES` | Concurrently open files per session; advertised in `HELLO_OK.limits` |
| `max_pending_files` | `int` | `64` | `MFUP_MAX_PENDING_FILES` | Pending-file window; advertised in `HELLO_OK.limits` |
| `sweep_interval` | `int` | `300` | `MFUP_SWEEP_INTERVAL` | Seconds between sweeper passes |
| `staging_prefix` | `str` | `".incoming"` | `MFUP_STAGING_PREFIX` | Prefix of per-session staging dir names (`{prefix}.{session_id}`) |
| `reconcile_every` | `int` | `4` | `MFUP_RECONCILE_EVERY` | Run filesystem-orphan reconciliation every Nth sweep |
| `orphan_grace_seconds` | `int` | `600` | `MFUP_ORPHAN_GRACE` | Minimum staging-dir age before it may be reconciled as an orphan |
| `max_buffered_body` | `int` | `16 * 1024 * 1024` | `MFUP_MAX_BUFFERED_BODY` | Threshold below which a data POST is buffered and applied atomically; also the hard cap that produces `413 body_too_large` |
| `max_meta_bytes` | `int` | `16384` | `MFUP_MAX_META_BYTES` | Cap on the JSON size of `HELLO.meta` |
| `admin_token` | `str` | `""` | `MFUP_ADMIN_TOKEN` | Bearer for `x-mfup-admin-token`; empty disables the admin routes entirely |
| `authorize` | `Optional[HookRef]` | `None` | `MFUP_AUTHORIZE` | Per-session authorization; `None` means allow-all (logged as a warning) |
| `map_file` | `Optional[HookRef]` | `None` | `MFUP_MAP_FILE` | Per-file destination mapping at publish; its presence also disables ingest-time conflict checks |
| `on_committed` | `Optional[HookRef]` | `None` | `MFUP_ON_COMMITTED` | Post-commit notification; returning `"publish"` auto-publishes |
<!-- ⬆️ 124 ⬆️ -->

Empty strings for the three hook env vars are normalized to `None` (`e.get(...) or None`).
<!-- ⬆️ 125 ⬆️ -->

### 4.8 Package metadata and cross-package dependencies

`mfup-fastapi` 0.2.0, hatchling build backend, MIT, `requires-python >= 3.10`, wheel packages `["mfup_fastapi"]`.
<!-- ⬆️ 126 ⬆️ -->

| Dependency | Constraint |
|------------|-----------|
| `mfup-core` | `==0.2.0` (pinned) |
| `fastapi` | `>=0.115.0` |
| `uvicorn[standard]` | `>=0.30.0` |
| `websockets` | `>=13.0` |
<!-- ⬆️ 127 ⬆️ -->

Optional `dev` extra: `pytest`, `pytest-asyncio`, `httpx`. `[tool.pytest.ini_options] asyncio_mode = "auto"`.
<!-- ⬆️ 128 ⬆️ -->

`engine.py` imports from `mfup_core` only: `hooks` (`AuthRequest`, `AuthorizeHook`, `CommitEvent`, `FileMapRequest`, `MapFileHook`, `OnCommittedHook`, `load_authorize_hook`, `resolve_hook`), `protocol` (`CRC32C_IMPL`, `PROTOCOL_VERSION`, `FrameReader`, `SessionEndFrame`, `SessionState`), `session_manager` (`SessionRegistry`, `LiveSession`), `publish` (`ConflictError`, `MappingError`, `list_payload_files`, `publish_session`, `publish_session_mapped`), `redis_index` (`SessionIndex`), and `storage` (`staging_dir`). All durable state — SQLite session DBs, chunk writers, commit invariants, the Redis index — lives in `mfup-core`; this package contributes the HTTP/WebSocket surface, request validation and fencing, the lifecycle wiring, and the hook invocation points.
<!-- ⬆️ 129 ⬆️ -->

---

## 5. @mfup/server — Node Server

`@mfup/server` is a second, independent MFUP/2 server implementation: the same protocol, the same on-disk session journal, and the same Redis key layout as `mfup-fastapi`, written in TypeScript for Node. The browser packages talk to either without knowing which is behind the socket — that wire-compatibility is enforced in CI, where the entire Playwright suite runs twice, once per backend (§9.8). Version `0.2.0`, MIT, ESM-only (`"type": "module"`, `"sideEffects": false`), `engines.node >= 22.13.0`.
<!-- ⬆️ 130 ⬆️ -->

Source: `packages/server/src/` — `session.ts` (1124 lines), `handler.ts` (1024), `engine.ts` (602), `storage.ts` (517), `protocol.ts` (369), `publish.ts` (291), `store.ts` (134), `hooks.ts` (128), `store-redis.ts` (121), `index.ts` (98), `vite.ts` (63), `logger.ts` (46), plus `bin/mfup-server.mjs` (116).
<!-- ⬆️ 131 ⬆️ -->

**Zero native dependencies.** SQLite comes from `node:sqlite` (the reason for the Node ≥ 22.13 floor), CRC-32C is a table-driven JS implementation, and `ws` is the only runtime dependency. `redis` is an *optional* peer (`peerDependenciesMeta.redis.optional`), needed only for the Redis session store. This is the deliberate counterpart to the Python server's hard `crc32c` C-extension requirement (§3.2): there a pure-Python fallback was removed for being too slow; here the JS table implementation is fast enough on V8 that no native module is needed. `CRC32C_IMPL` is correspondingly the constant `"js-table"`, against `"native"` on the Python side.
<!-- ⬆️ 132 ⬆️ -->

### 5.1 The universal handler (`handler.ts`)

The integration contract is a **pair** of primitives rather than a router, because a WebSocket upgrade never reaches an ordinary request handler.

| Member | Signature | Description |
|--------|-----------|-------------|
| `handle(req, res, url?)` | `Promise<boolean>` | Serves any MFUP HTTP endpoint (data, probe, publish, admin, `/health`). Returns `false` **without touching `req`/`res`** when the path is not MFUP's. `url` overrides `req.url` for frameworks that rewrite it |
| `upgrade(req, socket, head)` | `boolean` | Claims the `/mfup/control` WebSocket. Synchronous verdict: `true` = claimed, `false` = not ours |
| `middleware(req, res, next?)` | `void` | Connect/express sugar over `handle`; calls `next()` for foreign paths |
| `attach(server)` | `void` | Subscribes `upgrade` on a `http.Server` |
| `engine` | `MfupEngine` | Programmatic `publish()`, `sweep()`, registry access |
| `ready()` | `Promise<void>` | Resolves once startup (store connect, recovery, sweeper) finished. Called lazily by `handle`/`upgrade`, so awaiting it is optional |
| `close()` | `Promise<void>` | Stops the sweeper, closes sockets and the store |
<!-- ⬆️ 133 ⬆️ -->

`createMfup(options)` returns that object. The mount prefix is inferred from the request path by locating the `/mfup/` segment, so an express `app.use("/api/uploads", …)` mount needs no configuration; `basePath` exists for raw `node:http` servers mounted under a prefix the handler cannot infer. The route surface is identical to §4.3 — the same seven HTTP paths plus the control WebSocket.
<!-- ⬆️ 134 ⬆️ -->

Four integration shapes are supported from the same object: express/connect (`app.use(prefix, mfup.middleware)` plus `mfup.attach(server)`), bare `node:http` (`if (await mfup.handle(req, res)) return;`), Vite (`mfupDev()` from `@mfup/server/vite`, which mounts the middleware on the dev server's connect stack and claims the control upgrade while leaving Vite's own HMR socket alone), and standalone (`npx mfup-server`).
<!-- ⬆️ 135 ⬆️ -->

### 5.2 Pluggable session store (`store.ts`, `store-redis.ts`)

Where `mfup-fastapi` treats Redis as mandatory (§3.8), the Node server abstracts it behind a `SessionStore` interface — `register`, `updateExpiry`, `getExpired`, `getNotExpired`, `isRegistered`, `getMeta`, `allSessions`, `remove`, `close` — and accepts `store: "memory"` (default), a `"redis://…"` URL, or a custom implementation.
<!-- ⬆️ 136 ⬆️ -->

| Store | Trade-off |
|-------|-----------|
| `MemoryStore` (default) | No external service. After a restart the engine re-discovers live sessions with a depth-limited scan of `baseDir` for staging directories — each carries its own SQLite journal holding expiry and auth state. `scanDepth` (default `2`) covers both `<base>/.incoming.*` and per-user `<base>/<uid>/.incoming.*` |
| `RedisStore` | Cross-worker lazy resume/failover and scan-free cleanup. Uses the **same keys as the Python `SessionIndex`** — the `mfup:sessions` zset and `mfup:meta:{session_id}` hashes — so a Node and a Python server are interchangeable behind one Redis. Requires the optional `redis` peer |
<!-- ⬆️ 137 ⬆️ -->

The memory default is what makes the single-process case dependency-free; the Redis option is what makes the two server implementations operationally equivalent.
<!-- ⬆️ 138 ⬆️ -->

### 5.3 Semantics parity

The port carries the `mfup-core` / `mfup-fastapi` semantics over intact, not approximately: epoch/leg/seq fencing with deferred seq advance (retry-by-seq), the two commit invariants with the consecutive-no-progress cap (`MAX_COMMIT_RETRIES = 5`), the fatal-vs-transient storage split (`ENOSPC`/`EDQUOT`/`EROFS`/`EFBIG` abort, everything else NACKs), parent-chain containment, publish rename plus mapped publish, the three hooks, the sweeper with `reconcileOrphans`, and the WebSocket-`finally` cleanup including the publish race guard (§4.6). The SQLite layer uses the same six tables and the same three pragmas (`journal_mode=WAL`, `busy_timeout=3000`, `synchronous=NORMAL`) as §3.4, so a staging directory written by one server is readable by the other.
<!-- ⬆️ 139 ⬆️ -->

Hooks are the Python contracts camelCased — `authorize(req)` → `AuthResult | null` (fields `maxTotalBytes`, `maxFiles`, `baseDir`, `targetDir`, `context`), `mapFile(req)` → `string | null`, `onCommitted(ev)` → `"publish" | null` — with the same firing points and the same veto/failure semantics as §3.7.
<!-- ⬆️ 140 ⬆️ -->

### 5.4 Standalone CLI (`bin/mfup-server.mjs`)

`npx mfup-server` runs a standalone server configured entirely from the environment, at parity with `python -m mfup_fastapi` (§4.7): `MFUP_HOST` (`0.0.0.0`), `MFUP_PORT` (`8070`), `MFUP_BASE_DIR` (`<tmp>/mfup-uploads`), `MFUP_STORE` (`memory`, falling back to `REDIS_URL`), `MFUP_SESSION_RESUME_TTL`, `MFUP_LEG_IDLE_TIMEOUT`, `MFUP_MAX_CHUNK_BYTES`, `MFUP_MAX_OPEN_FILES`, `MFUP_MAX_PENDING_FILES`, `MFUP_SWEEP_INTERVAL`, `MFUP_STAGING_PREFIX`, `MFUP_RECONCILE_EVERY`, `MFUP_ORPHAN_GRACE`, `MFUP_SCAN_DEPTH`, `MFUP_MAX_BUFFERED_BODY`, `MFUP_MAX_META_BYTES`, `MFUP_ADMIN_TOKEN`, and the three hook refs `MFUP_AUTHORIZE` / `MFUP_MAP_FILE` / `MFUP_ON_COMMITTED` in `module#export` form (where the Python side uses `pkg.module:callable`).
<!-- ⬆️ 141 ⬆️ -->

### 5.5 Package surface (`index.ts`)

The barrel re-exports the handler (`createMfup`, `Mfup`), the engine and its typed publish errors (`MfupEngine`, `resolveOptions`, `MfupOptions`, `ResolvedMfupOptions`, `PublishError`, `SessionNotFound`, `NotCommitted`, `TargetEscapes`, `MapFileHookError`, `reconcileOrphans`, `isSafeTarget`), the hook contract types, the store (`MemoryStore`, `RedisStore`, `resolveStore`, `SessionStore`, `SessionMeta`), the protocol primitives (`PROTOCOL_VERSION`, `ROOT_NODE_ID`, `CRC32C_IMPL`, `FrameTag`, `NodeKind`, `ChecksumKind`, `SessionState`, `NodeStatus`, `FrameReader`, `decodeFramePayload`, `crc32c` and the eight frame types), the storage/publish building blocks (`SessionDB`, `stagingDir`, `ensureStaging`, `openSessionDb`, `validateNodeName`, `resolvePayloadPath`, `ConflictError`, `MappingError`, `listPayloadFiles`, `publishSession`, `publishSessionMapped`, `validateMappedPath`, `detectConflicts`), the session layer (`LiveSession`, `SessionRegistry`, `FileWriter`, `MAX_COMMIT_RETRIES`, plus `SessionUnknownError` / `BadTokenError` / `BadStateError` and the `ControlSocket` type), and the logger controls (`setLogLevel`, `setLogSink`). A second entry point, `@mfup/server/vite`, exports the `mfupDev` plugin.
<!-- ⬆️ 142 ⬆️ -->

---

## 6. @mfup/client — Browser Client

`@mfup/client` (v0.2.6, MIT) is the browser-side reference implementation of MFUP/2. It has **zero runtime dependencies** — the only declared dependency is `typescript ^5.5.0` as a devDependency. It is an ESM-only package (`"type": "module"`, `"sideEffects": false`) compiled with `target/module: ES2022`, `moduleResolution: bundler`, `strict: true`, `isolatedModules: true`, and `lib: ["ES2022", "DOM", "DOM.Iterable"]`. Because `isolatedModules` is on, every enum-like construct is a plain `as const` object plus a value-union type alias — never a `const enum` — so consumers building with esbuild/swc/vite are not broken.
<!-- ⬆️ 143 ⬆️ -->

The client speaks two transports to the `mfup-fastapi` server:

| Endpoint | Method | Purpose | Built in |
|----------|--------|---------|----------|
| `{ws(s)://host}/mfup/control` | WebSocket | JSON control messages (HELLO/RESUME, ACKs, flow, ASK, commit) | `session.ts:483`, `control.ts` |
| `{base}/mfup/probe/{sessionId}` | POST | Streaming-capability probe with a `ReadableStream` body | `probe.ts:29` |
| `{base}/mfup/data/{sessionId}/{legId}?seq=&final=&epoch=` | POST | Binary frame payload (`Content-Type: application/x-mfup`) | `data-channel.ts:81` |
| `{base}/mfup/sessions/{sessionId}/publish` | POST | Move staged files into the target directory | `session.ts:696` |
<!-- ⬆️ 144 ⬆️ -->

All non-WebSocket requests carry the server-issued bearer token in the `X-MFUP-Token` header. The WebSocket URL is derived from `serverUrl` by `.replace(/^http/, "ws")`; page-relative `serverUrl` values (e.g. `"/api/uploads"`) are first resolved against `location.origin`.
<!-- ⬆️ 145 ⬆️ -->

Source files (`/workspace/packages/client/src/`), by size: `session.ts` (1609 lines), `protocol.ts` (466), `data-channel.ts` (422), `ingestion.ts` (330), `errors.ts` (329), `control.ts` (237), `progress.ts` (187), `probe.ts` (140), `index.ts` (92), `dnd.ts` (87).
<!-- ⬆️ 146 ⬆️ -->

---

### 6.1 Public API surface (`index.ts`)

`index.ts` is a pure re-export barrel — it contains no logic. It exports 2 constants, 13 protocol values, 26 protocol types, and 8 modules' worth of classes/functions/types.
<!-- ⬆️ 147 ⬆️ -->

**Constants and protocol values**

| Export | Type | Description |
|--------|------|-------------|
| `PROTOCOL_VERSION` | `const "MFUP/2"` | Sent as `HELLO.v` |
| `ROOT_NODE_ID` | `const 0` | Parent id of every top-level ingested node |
| `FrameTag` | const object + type | 8 single-byte frame discriminators |
| `NodeKind` | const object + type | `DIR: 0x00`, `FILE: 0x01` |
| `ChecksumKind` | const object + type | `CRC32C: 0x01` (only member) |
| `crc32c(data, initial = 0)` | function | Castagnoli CRC-32C over a `Uint8Array`, returns `number` |
| `encodeFrame(f)` | function | Dispatches on `f.tag` to the specific encoder |
| `encodeNodeFrame(f)` | function | Serialises a `NodeFrame` |
| `encodeSummaryFrame(f)` | function | Serialises a `SummaryFrame` |
| `encodeFileOpenFrame(f)` | function | Serialises a `FileOpenFrame` |
| `encodeFileChunkFrame(f)` | function | Serialises a `FileChunkFrame` |
| `encodeFileCloseFrame(f)` | function | Serialises a `FileCloseFrame` |
| `encodeDirCloseFrame(f)` | function | Serialises a `DirCloseFrame` |
| `encodeSessionEndFrame(f)` | function | Serialises a `SessionEndFrame` |
| `encodeClientAbortFrame(f)` | function | Serialises a `ClientAbortFrame` |
<!-- ⬆️ 148 ⬆️ -->

All 9 encoders return `Uint8Array`. There is no decoder in the client — MFUP/2 binary frames are strictly client→server.
<!-- ⬆️ 149 ⬆️ -->

**Classes and functions**

| Export | Kind | Module | Description |
|--------|------|--------|-------------|
| `MfupSession` | class | `session.ts` | Main entry point; owns the state machine and all channels |
| `ControlChannel` | class | `control.ts` | WebSocket wrapper with a typed 14-event bus |
| `DataChannel` | class | `data-channel.ts` | Dual-mode (streaming/batch) binary uploader |
| `ProgressTracker` | class | `progress.ts` | Merges scan units and body bytes into a monotonic fraction |
| `NodeIdAllocator` | class | `ingestion.ts` | Monotonic node id source, starts at 1 (0 is root) |
| `ingestFromHandles` | function | `ingestion.ts` | Walk `FileSystemHandle[]` roots |
| `ingestFromEntries` | function | `ingestion.ts` | Walk `FileSystemEntry[]` roots (`webkitGetAsEntry`) |
| `ingestFromFileList` | function | `ingestion.ts` | Rebuild a tree from `webkitRelativePath` |
| `ingestFromFiles` | function | `ingestion.ts` | Flat `File[]` — all children of root |
| `probeStreaming` | function | `probe.ts` | Detect `duplex:"half"` support |
| `sourceFromDataTransfer` | function | `dnd.ts` | Synchronously extract an `UploadSource` from a drop event |
| `sourceFromInput` | function | `dnd.ts` | Extract an `UploadSource` from `<input type=file>` |
| `MfupError` | class | `errors.ts` | Structured error with `code`/`layer`/`action`/`fatal` |
| `MfupErrorCode` | const object + type | `errors.ts` | 25 error codes |
| `MfupErrorLayer` | const object + type | `errors.ts` | 5 layers |
<!-- ⬆️ 150 ⬆️ -->

**Exported types**

`DataFrame`, `NodeFrame`, `SummaryFrame`, `FileOpenFrame`, `FileChunkFrame`, `FileCloseFrame`, `DirCloseFrame`, `SessionEndFrame`, `ClientAbortFrame`, `ClientControlMsg`, `ServerControlMsg`, `HelloMsg`, `ResumeMsg`, `HelloOkMsg`, `ResumeOkMsg`, `FileAckMsg`, `NackChunkMsg`, `FlowMsg`, `PruneNodeMsg`, `RejectFileMsg`, `SessionAbortMsg`, `CommitOkMsg`, `ServerLimits`, `RootSummary`, `ResumeFileStatus`, `SessionState` (26 from `protocol.ts`), plus `ControlChannelOpts`, `ControlEventMap`, `DataChannelOpts`, `DataCommitResult`, `DiscoveredNode`, `IngestCallback`, `IngestFilter`, `ProgressSnapshot`, `ProgressListener`, `ProbeResult`, `UploadSource`, `MfupSessionConfig`, `MfupSessionEvents`, `MfupSessionSnapshot`, `MfupAsk`, `MfupAskAction`, `MfupFileRef`.
<!-- ⬆️ 151 ⬆️ -->

Not re-exported despite being defined in `protocol.ts`: `ClientAbortMsg`, `ActionMsg`, `ProbeAckMsg`, `AskMsg`, `CommitRetryMsg`. The 17 error factory functions in `errors.ts` (`wsConnectFailed`, `nackChunk`, `publishConflict`, …) are also internal — consumers get `MfupError` instances but construct none.
<!-- ⬆️ 152 ⬆️ -->

---

### 6.2 `MfupSession` (session.ts)

The session orchestrator. It owns the control channel, the data channel, ingestion, per-file tracking, progress, reconnect/resume, and the commit loop. It exposes both a typed event bus (`on`) and a `useSyncExternalStore`-compatible snapshot store (`subscribe` + `getSnapshot`).
<!-- ⬆️ 153 ⬆️ -->

#### Configuration

```ts
export interface MfupSessionConfig {
  serverUrl: string;
  targetDir?: string;
  meta?: unknown;
  sessionId?: string;
  resumeToken?: string;
  lastKnownEpoch?: number;
  chunkSize?: number;
  maxReconnectAttempts?: number | null;
  reconnectDelayMs?: number;
}
```
<!-- ⬆️ 154 ⬆️ -->

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `serverUrl` | `string` | — (required) | `http(s)://host` or a page-relative path; resolved via `new URL(serverUrl, location.origin)` and stripped of a trailing `/` |
| `targetDir` | `string` | `"."` | Relative destination on the server; sent as `HELLO.target_dir` |
| `meta` | `unknown` | `undefined` | Arbitrary JSON sent as `HELLO.meta`; omitted from the message entirely when `undefined`. Fed to the server's `authorize`/`map_file` hooks |
| `sessionId` | `string` | `genUUID()` | Reuse to resume an existing session |
| `resumeToken` | `string` | `""` | Server-issued; empty for a new session, overwritten by `HELLO_OK.resume_token` |
| `lastKnownEpoch` | `number` | `undefined` | When set, seeds `epoch`; `connect()` then sends `RESUME` instead of `HELLO` |
| `chunkSize` | `number` | `262144` (256 KiB) | Effective chunk size is `Math.min(chunkSize, limits.max_chunk_bytes)` |
| `maxReconnectAttempts` | `number \| null` | `null` (infinite) | Reconnect budget |
| `reconnectDelayMs` | `number` | `1000` | Base for exponential backoff `delay = min(base * 2^(n-1), 20_000)` |
<!-- ⬆️ 155 ⬆️ -->

Server limits default to `{ max_chunk_bytes: 262144, max_open_files: 1, max_pending_files: 64 }` until `HELLO_OK` replaces them.
<!-- ⬆️ 156 ⬆️ -->

#### Public members

| Member | Type | Description |
|--------|------|-------------|
| `state` | getter → `SessionState` | Current state |
| `id` | getter → `string` | Session id |
| `token` | getter → `string` | Current resume token (server-issued after `HELLO_OK`) |
| `currentEpoch` | getter → `number` | Current epoch |
| `streamingMode` | getter → `boolean \| null` | `null` until the probe (or its cache) resolves |
| `connect()` | `Promise<void>` | Opens the WS, awaits `HELLO_OK`/`RESUME_OK`, probes streaming, opens the data channel. Throws if state is `aborted`/`failed` |
| `upload(source)` | `Promise<void>` | Accepts `UploadSource \| FileList \| File[]`; dispatches to the four specific uploaders |
| `uploadHandles(handles)` | `Promise<void>` | `FileSystemHandle[]` roots |
| `uploadEntries(entries)` | `Promise<void>` | `FileSystemEntry[]` roots |
| `uploadFileList(files)` | `Promise<void>` | `ArrayLike<File>` with `webkitRelativePath` |
| `uploadFiles(files)` | `Promise<void>` | Flat `File[]` |
| `settleAsks()` | `Promise<MfupAskAction \| null>` | Waits until no ask is pending; returns `"cancel"` if any answer was cancel, `"merge_overwrite"` if asks were raised and none cancelled, `null` if none was ever raised. Resolves early on a terminal state |
| `publish()` | `Promise<{ published: string[] }>` | POSTs the publish endpoint; throws `PUBLISH_CONFLICT` on a 409 `conflict_files` body, `PUBLISH_FAILED` otherwise |
| `abort(code = "client_cancel", reason = "user cancelled")` | `void` | Instant teardown (see below) |
| `sendAction(action)` | `void` | Sends `ACTION`; settles every pending ask with `action`; `"cancel"` also calls `abort()` |
| `on(event, fn)` | `() => void` | Typed event subscription; returns an unsubscribe function |
| `onProgress(fn)` | `() => void` | Shorthand for `ProgressTracker.on` |
| `subscribe(fn)` | `() => void` | Snapshot-store subscribe (`useSyncExternalStore`) |
| `getSnapshot()` | `MfupSessionSnapshot` | Referentially stable until a version bump |
| `getProgress()` | `ProgressSnapshot` | Current progress snapshot |
| `getResumeState()` | `{ sessionId; resumeToken; epoch }` | Persist this to resume in a later page load |
<!-- ⬆️ 157 ⬆️ -->

#### State machine

`SessionState` is declared in `protocol.ts` with 8 members. The client assigns 7 of them; **`"expired"` is part of the union but is never set by any client code path** — it exists for parity with the server's session model.
<!-- ⬆️ 158 ⬆️ -->

| State | Entered from | Trigger | Emitted / side effects |
|-------|--------------|---------|------------------------|
| `active` | initial, `waiting_resume`, `paused_by_server`, `committing` | Field initialiser; end of `connect()`; `FLOW{paused:false}`; `COMMIT_RETRY` handling | `state`; clears `_reconnectInfo`, resets `reconnectCount` |
| `paused_by_server` | `active` | `FLOW{paused:true}` | `state`; `waitIfPaused()` blocks the pump and the chunk writer |
| `waiting_resume` | `active`, `paused_by_server`, `committing` | `handleDisconnect()` (WS close, `DATA_WRITE_FAILED`, `DATA_HTTP_ERROR`, data-open rejection) | `state`, then one `reconnecting` + one `error` per attempt; data channel aborted; metadata frames buffered into `_pendingMeta` |
| `committing` | `active` | `finalizeScan()` after writing `SESSION_END` | `state`; `data.close()` flushes the final POST |
| `committed` | `committing` | `COMMIT_OK`, or a `commit` object in the final batch POST response | `state` + `committed{files,bytes}`; clears `_reconnectInfo` and `_currentFileRef` |
| `aborted` | any non-terminal | `abort()`, `sendAction("cancel")`, `SESSION_ABORT` | `state` + `abort{by:"client"\|"server",code,reason}`; terminal |
| `failed` | `waiting_resume` | Reconnect budget exhausted, or a `fatal` `MfupError` from `connect()` during reconnect | `state` + `error`; terminal |
| `expired` | — | never assigned by the client | — |
<!-- ⬆️ 159 ⬆️ -->

`setState()` enforces terminality: once `_state` is `aborted` or `failed`, any further transition to a different state is silently ignored. This prevents an in-flight reconnect that completes after `abort()` from resurrecting the session.
<!-- ⬆️ 160 ⬆️ -->

**Instant cancel.** `abort()` does, in order: clear the reconnect timer, resolve `_reconnectResolve` and `_scanGateResolve` (so `finalizeScan` and ingestion unblock), `setState("aborted")` *first* (every loop keys off state), `control.sendAbort(code, reason)`, `data.abort(reason)` — the hard path, which discards the batch buffer rather than flushing megabytes — `control.close()`, `abortCtrl.abort()` (kills the in-flight XHR/fetch), emit `abort`, and reject any pending commit wait. The chunk writer additionally checks `_state === "aborted" || "failed"` on every chunk and returns immediately. Note that the session never emits a `CLIENT_ABORT` *data frame*; `ClientAbortFrame` is imported but unused — aborts travel over the control WebSocket. `DIR_CLOSE` frames are likewise never written (the tag appears only in the `_pendingMeta` buffering allow-list).
<!-- ⬆️ 161 ⬆️ -->

#### Events (`MfupSessionEvents`)

12 events, all emitted from `session.ts`:

| Event | Payload | Emitted when |
|-------|---------|--------------|
| `progress` | `ProgressSnapshot` | Every `ProgressTracker` mutation (send-progress is rate-limited inside the tracker) |
| `state` | `SessionState` | Every accepted `setState()` |
| `committed` | `{ files: number; bytes: number }` | `markCommitted()` — from `COMMIT_OK` or the final batch POST body |
| `published` | `{ published: string[] }` | `publish()` succeeded |
| `ask` | `MfupAsk` | Server `ASK` arrived |
| `ask:answered` | `MfupAsk` | A pending ask was settled by `sendAction()` (including via `ask.respond()`) |
| `file:start` | `MfupFileRef` | `streamFile()` begins a file body |
| `file:ack` | `MfupFileRef & { acceptedBytes: bigint; complete: boolean }` | `FILE_ACK`; `complete` is `status === "sent" && acceptedOffset >= size` |
| `file:reject` | `MfupFileRef & { code: string; reason: string }` | Server `REJECT_FILE`, or the local NACK budget fired (`code: "nack_budget"`) |
| `abort` | `{ by: "client" \| "server"; code: string; reason: string }` | `abort()` or `SESSION_ABORT` |
| `error` | `MfupError` | Every `emitError()` call |
| `reconnecting` | `{ attempt: number; delay: number; maxAttempts: number \| null }` | Before each backoff sleep in `handleDisconnect()` |
<!-- ⬆️ 162 ⬆️ -->

```ts
export interface MfupFileRef {
  nodeId: number;
  /** Client-relative path ("dir/sub/name.ext"), reconstructed from the scan. */
  path: string;
  size: bigint;
}
```
<!-- ⬆️ 163 ⬆️ -->

`path` is reconstructed by `nodePath()`, walking `nodeMeta` parent links up to `ROOT_NODE_ID` and joining with `/`; it falls back to `String(nodeId)` when the chain is unavailable. The path is resolved *before* a completed file is dropped from `nodeMeta`.
<!-- ⬆️ 164 ⬆️ -->

#### Interactive asks

```ts
export type MfupAskAction = "merge_overwrite" | "cancel";

export interface MfupAsk {
  readonly id: number;          // monotonic per session, usable as a React key
  readonly code: string;        // "target_conflict" (default when the server omits it)
  readonly nodeId: number | null;
  readonly name: string | null;
  readonly answered: MfupAskAction | null;
  respond(action: MfupAskAction): void;
}
```
<!-- ⬆️ 165 ⬆️ -->

`respond()` is a no-op once `answered` is non-null; otherwise it routes through `sendAction()`. Because `ACTION` is session-wide, `sendAction()` settles **every** pending ask with the same answer and emits one `ask:answered` per settled ask. The transfer keeps running while a question is pending.
<!-- ⬆️ 166 ⬆️ -->

#### Snapshot store

```ts
export interface MfupSessionSnapshot {
  sessionId: string;
  state: SessionState;
  epoch: number;
  streaming: boolean | null;
  progress: ProgressSnapshot;
  fraction: number | null;
  asks: readonly MfupAsk[];
  pendingAsks: number;
  reconnect: { attempt: number; delay: number; maxAttempts: number | null } | null;
  committed: { files: number; bytes: number } | null;
  published: readonly string[] | null;
  currentFile: MfupFileRef | null;
  recentErrors: readonly MfupError[];   // ring of 20, newest last
  fatalError: MfupError | null;         // first fatal error only
}
```
<!-- ⬆️ 167 ⬆️ -->

`getSnapshot()` caches by an internal `_snapVersion` counter, so the returned object is referentially stable until something changes. `markSnapshotDirty(immediate)` bumps the version and either flushes immediately (state, ask, committed, abort, fatal errors) or coalesces through a `SNAPSHOT_THROTTLE_MS = 33` timer (progress ticks). If nobody is subscribed, no timer is armed.
<!-- ⬆️ 168 ⬆️ -->

#### Scan, pump, and per-file tracking

Ingestion callbacks arrive in `onDiscover()`, which: builds and records a `NodeFrame` in `nodeMeta`, writes it, increments `scanDone`/`scanEst`, and for files creates a `TrackedFile` (`{nodeId, size, acceptedOffset, openBody, status, nacked, nackCount}` with `status: "pending" | "streaming" | "sent" | "acked" | "rejected"`), pushes it onto `fileQueue`, and adds its size to the body estimate. A root `SUMMARY` frame is written every 50 discovered nodes.
<!-- ⬆️ 169 ⬆️ -->

Scan backpressure: ingestion awaits a gate promise once `fileQueue.length >= SCAN_HIGH_WATER` (10 000) and is released when the pump drains it below `SCAN_LOW_WATER` (5 000).
<!-- ⬆️ 170 ⬆️ -->

`pumpFiles()` runs concurrently with the scan and processes **one file at a time** (`activeFile`, matching the default `max_open_files: 1`). Per file it writes `FILE_OPEN`, iterates `openBody(startOffset)`, re-slices each 64 KiB read into `min(chunkSize, limits.max_chunk_bytes)` pieces, computes `crc32c(piece)`, writes a `FILE_CHUNK`, awaits `data.drain()`, and finally writes `FILE_CLOSE` with `sizeSent`. The writer bails out on terminal state, on the file being rejected mid-stream, and on a data channel that has failed *or* closed. If `file.nacked` was set by the NACK handler, the file is requeued and `streamFile` returns.
<!-- ⬆️ 171 ⬆️ -->

**Writer fencing.** `streamFile()` captures `const dc = this.data` at entry and treats that channel as the only one it may write to — the client-side analogue of the server's `leg_id` fence. It returns immediately whenever `this.data !== dc`: before each chunk write, after each `dc.drain()`, and before `FILE_CLOSE`. Backpressure awaits `dc.drain()`, never `this.data`, so a swap mid-await cannot redirect the writer. Without the fence a writer parked in `drain()` at disconnect time survived the reconnect, woke against the *new* channel and re-sent chunks at its stale offset; the server answered `bad_offset` per chunk (32 per flush), which burned the file's `MAX_FILE_NACKS` budget client-side, the legitimate re-pump skipped the now-rejected file, and the commit rounds went data-free until `commit_failed`.
<!-- ⬆️ 172 ⬆️ -->

`MAX_FILE_NACKS = 8` — the NACK budget counts *consecutive* failures (`nackCount` is reset to 0 on any forward `FILE_ACK`). Exceeding it adds the node to `rejectedFiles` and emits `file:reject` with `code: "nack_budget"`, keeping the session alive.
<!-- ⬆️ 173 ⬆️ -->

#### Recovery: lost NODE metadata

Three mechanisms re-send node metadata the server may never have received (a data POST that dies in transit takes its `NODE` frames with it):

1. `NACK_CHUNK{reason:"unknown_node"}` → `resendNodeChain(nodeId)` re-writes the node's parent chain root→leaf before the body re-stream.
2. On reconnect, `requeuePendingFiles()` replays `_pendingMeta` (buffered `NODE`/`SUMMARY`/`DIR_CLOSE`), then re-writes **the entire** `nodeMeta` map (idempotent server-side), then requeues every non-acked, non-rejected file whose `acceptedOffset < size`.
3. `COMMIT_RETRY` with `nodes_expected !== nodes_seen` sets `_resendAllMeta`, which replays all of `nodeMeta` on the fresh data channel before bodies.
<!-- ⬆️ 174 ⬆️ -->

`nodeMeta` entries for files are deleted once the file is fully acked (the server durably knows them); directory entries live for the session.
<!-- ⬆️ 175 ⬆️ -->

#### Commit loop

`finalizeScan()` seals the scan, emits a final `SUMMARY`, awaits `pumpDone`, then loops while `waiting_resume`, `fileQueue.length > 0`, **or `pumping`** — awaiting `_reconnectPromise`, re-kicking the pump when the queue refilled, and awaiting the live `pumpDone` on every iteration. `pumping` is part of the condition because after a `RESUME` the fresh pump shifts the requeued file into `activeFile` before the queue is re-read, so an empty queue does not mean the transfer is done; entering the commit loop there closed the data channel under an in-flight writer. The same three-part condition guards the post-`COMMIT_RETRY` drain. It then enters a `while (true)` commit loop: write `SESSION_END` with the root summary, `setState("committing")`, `await data.close()`. In batch mode the final POST response may already carry `{commit: {files, bytes}}` — that short-circuits to committed. Otherwise it awaits `COMMIT_OK` (resolve) or `COMMIT_RETRY` (set `_commitRetry`, abort the old data channel, `setState("active")`, construct a fresh `DataChannel`, replay metadata if flagged, re-pump, and loop).

`markCommitted()` is idempotent — it returns immediately when `_committed` is already set. In batch mode the commit genuinely arrives **twice**: echoed in the final POST response body *and* as `COMMIT_OK` on the control socket. Without the guard the `committed` event fired twice, consumers published twice, and the second publish hit a fatal `404` because the session was already removed.
<!-- ⬆️ 176 ⬆️ -->

#### Probe verdict cache

`connect()` reads `localStorage["mfup:streaming-probe:" + serverUrl]` (JSON `{v: boolean, ts: number}`) and uses it if younger than 24 hours; otherwise it runs `probeStreaming()` and writes the verdict back. Both reads and writes are wrapped in try/catch so private mode and worker contexts fall through to probing. A probe throw sets `_streamingMode = false` and emits a non-fatal error.
<!-- ⬆️ 177 ⬆️ -->

`genUUID()` uses `crypto.randomUUID()` when available and otherwise builds an RFC 4122 v4 from `crypto.getRandomValues` — plain-HTTP intranet origins are not secure contexts and lack `randomUUID`.
<!-- ⬆️ 178 ⬆️ -->

---

### 6.3 `protocol.ts` — wire types and codec

#### Constants

```ts
export const PROTOCOL_VERSION = "MFUP/2";
export const ROOT_NODE_ID = 0;

export const FrameTag = {
  NODE:         0x01,
  SUMMARY:      0x02,
  FILE_OPEN:    0x03,
  FILE_CHUNK:   0x04,
  FILE_CLOSE:   0x05,
  DIR_CLOSE:    0x06,
  SESSION_END:  0x07,
  CLIENT_ABORT: 0x08,
} as const;
export type FrameTag = (typeof FrameTag)[keyof typeof FrameTag];

export const NodeKind = { DIR: 0x00, FILE: 0x01 } as const;
export type NodeKind = (typeof NodeKind)[keyof typeof NodeKind];

export const ChecksumKind = { CRC32C: 0x01 } as const;
export type ChecksumKind = (typeof ChecksumKind)[keyof typeof ChecksumKind];
```
<!-- ⬆️ 179 ⬆️ -->

A comment records that `HashKind` was removed: per-chunk CRC32C is sufficient and SHA-256 was never verified server-side.
<!-- ⬆️ 180 ⬆️ -->

#### Data frames (client → server)

```ts
export interface NodeFrame {
  tag: typeof FrameTag.NODE;
  nodeId: number;
  parentId: number;
  kind: NodeKind;
  name: string;
  sizeHint: bigint | null;
  mtimeMs: bigint | null;
}

export interface SummaryFrame {
  tag: typeof FrameTag.SUMMARY;
  nodeId: number;
  scanDoneUnits: bigint;
  scanEstUnits: bigint;
  bodyDoneBytes: bigint;
  bodyEstBytes: bigint;
  sealed: boolean;
}

export interface FileOpenFrame {
  tag: typeof FrameTag.FILE_OPEN;
  nodeId: number;
  size: bigint;
  mtimeMs: bigint | null;
}

export interface FileChunkFrame {
  tag: typeof FrameTag.FILE_CHUNK;
  nodeId: number;
  offset: bigint;
  length: number;
  checksumKind: ChecksumKind;
  checksum: number;
  payload: Uint8Array;
}

export interface FileCloseFrame {
  tag: typeof FrameTag.FILE_CLOSE;
  nodeId: number;
  sizeSent: bigint;
}

export interface DirCloseFrame {
  tag: typeof FrameTag.DIR_CLOSE;
  nodeId: number;
}

export interface SessionEndFrame {
  tag: typeof FrameTag.SESSION_END;
  rootSummary: {
    scanDoneUnits: bigint;
    scanEstUnits: bigint;
    bodyDoneBytes: bigint;
    bodyEstBytes: bigint;
    sealed: true;
  };
}

export interface ClientAbortFrame {
  tag: typeof FrameTag.CLIENT_ABORT;
  code: string;
  reason: string;
}

export type DataFrame =
  | NodeFrame | SummaryFrame | FileOpenFrame | FileChunkFrame
  | FileCloseFrame | DirCloseFrame | SessionEndFrame | ClientAbortFrame;
```
<!-- ⬆️ 181 ⬆️ -->

#### Binary layout

Every frame is `[u32 BE length][u8 tag][payload]`, where `length` covers tag + payload but not itself. All multi-byte integers are big-endian (`DataView` defaults). Strings inside `CLIENT_ABORT` use a `[u16 len][utf8 bytes]` segment; the `NODE` name uses the same `u16`-prefixed form inline.
<!-- ⬆️ 182 ⬆️ -->

| Frame | Payload layout | Payload size |
|-------|----------------|--------------|
| `NODE` | `node_id(u32) parent_id(u32) kind(u8) name_len(u16) name(bytes) size_flag(u8) [size(u64)] mtime_flag(u8) [mtime(u64)]` | variable |
| `SUMMARY` | `node_id(u32) scan_done(u64) scan_est(u64) body_done(u64) body_est(u64) sealed(u8)` | 37 |
| `FILE_OPEN` | `node_id(u32) size(u64) mtime_flag(u8) [mtime(u64)]` | 13 or 21 |
| `FILE_CHUNK` | `node_id(u32) offset(u64) length(u32) checksum_kind(u8) checksum(u32) payload(bytes)` | 21 + payload |
| `FILE_CLOSE` | `node_id(u32) size_sent(u64)` | 12 |
| `DIR_CLOSE` | `node_id(u32)` | 4 |
| `SESSION_END` | `scan_done(u64) scan_est(u64) body_done(u64) body_est(u64) sealed(u8=1)` | 33 |
| `CLIENT_ABORT` | `code_len(u16) code(bytes) reason_len(u16) reason(bytes)` | variable |
<!-- ⬆️ 183 ⬆️ -->

`crc32c(data, initial = 0)` is a table-driven Castagnoli implementation (polynomial `0x82F63B78` reflected), with a 256-entry `Uint32Array` built at module load. It returns an unsigned 32-bit `number` and accepts an `initial` value for incremental use.
<!-- ⬆️ 184 ⬆️ -->

#### Control messages (JSON over WebSocket)

Client → server:

```ts
export interface HelloMsg {
  t: "HELLO";
  v: typeof PROTOCOL_VERSION;
  session_id: string;
  leg_id: string;
  target_dir: string;
  meta?: unknown;   // size-capped server-side (default 16 KiB)
}

export interface ResumeMsg {
  t: "RESUME";
  session_id: string;
  resume_token: string;
  leg_id: string;
  last_known_epoch: number | null;
}

export interface ClientAbortMsg { t: "CLIENT_ABORT"; code: string; reason: string; }

export interface ActionMsg { t: "ACTION"; action: "merge_overwrite" | "cancel"; }

export type ClientControlMsg = HelloMsg | ResumeMsg | ClientAbortMsg | ActionMsg;
```
<!-- ⬆️ 185 ⬆️ -->

Server → client:

```ts
export interface ServerLimits {
  max_chunk_bytes: number;
  max_open_files: number;
  max_pending_files: number;
}

export interface HelloOkMsg {
  t: "HELLO_OK";
  epoch: number;
  expires_at: string;
  /** SERVER-issued bearer token for data/probe/publish and future RESUMEs. */
  resume_token: string;
  limits: ServerLimits;
}

export interface RootSummary {
  scan_done_units: number;
  scan_est_units: number;
  body_done_bytes: number;
  body_est_bytes: number;
  sealed: boolean;
}

export interface ResumeFileStatus {
  node_id: number;
  accepted_offset: number;
  status: "open" | "closed" | "rejected";
}

export interface ResumeOkMsg {
  t: "RESUME_OK";
  epoch: number;
  expires_at: string;
  root_summary: RootSummary;
  files: ResumeFileStatus[];
  pruned_nodes: number[];
  rejected_files: number[];
}

export interface FileAckMsg { t: "FILE_ACK"; node_id: number; accepted_offset: number; }

export interface NackChunkMsg {
  t: "NACK_CHUNK";
  node_id: number;
  expected_offset: number;
  reason: "bad_checksum" | "bad_offset" | "stale_epoch" | "server_policy" | "unknown_node";
}

export interface FlowMsg {
  t: "FLOW";
  paused: boolean;
  reason: "backpressure" | "maintenance" | "storage_pressure";
}

export interface PruneNodeMsg  { t: "PRUNE_NODE";  node_id: number; code: string; reason: string; }
export interface RejectFileMsg { t: "REJECT_FILE"; node_id: number; code: string; reason: string; }
export interface SessionAbortMsg { t: "SESSION_ABORT"; code: string; reason: string; }
export interface CommitOkMsg   { t: "COMMIT_OK"; files: number; bytes: number; }
export interface ProbeAckMsg   { t: "PROBE_ACK"; first_chunk_bytes?: number; }

export interface AskMsg {
  t: "ASK";
  code?: string;      // currently only "target_conflict"; absent on older servers
  node_id?: number;
  name?: string;
}

export interface CommitRetryMsg {
  t: "COMMIT_RETRY";
  incomplete: { node_id: number; accepted_offset: number }[];
  nodes_expected?: number;   // present when the node-count invariant failed
  nodes_seen?: number;
}

export type ServerControlMsg =
  | HelloOkMsg | ResumeOkMsg | FileAckMsg | NackChunkMsg | FlowMsg
  | PruneNodeMsg | RejectFileMsg | SessionAbortMsg | CommitOkMsg
  | CommitRetryMsg | ProbeAckMsg | AskMsg;

export type SessionState =
  | "active" | "paused_by_server" | "waiting_resume" | "committing"
  | "committed" | "aborted" | "expired" | "failed";
```
<!-- ⬆️ 186 ⬆️ -->

Note the asymmetry: 4 client message kinds, 12 server message kinds.
<!-- ⬆️ 187 ⬆️ -->

---

### 6.4 `control.ts` — WebSocket control channel

`ControlChannel` wraps a single `WebSocket`, sends the opening handshake, parses inbound JSON, and dispatches onto a typed event bus. It holds no upload state.
<!-- ⬆️ 188 ⬆️ -->

```ts
export interface ControlChannelOpts {
  url: string;                    // ws(s)://host/mfup/control
  sessionId: string;
  resumeToken: string;
  legId: string;
  targetDir: string;              // relative path on the server
  meta?: unknown;                 // HELLO.meta
  lastKnownEpoch?: number | null; // if set, send RESUME instead of HELLO
}
```
<!-- ⬆️ 189 ⬆️ -->

| Member | Type | Description |
|--------|------|-------------|
| `epoch` | getter → `number` | Set from `HELLO_OK`/`RESUME_OK`, `0` before |
| `limits` | getter → `ServerLimits \| null` | Set from `HELLO_OK` only |
| `expiresAt` | getter → `string \| null` | Set from `HELLO_OK`/`RESUME_OK` |
| `ready()` | `Promise<void>` | Resolves on `HELLO_OK`/`RESUME_OK`; rejects on WS close or `SESSION_ABORT` |
| `open()` | `void` | Constructs the socket and wires `onopen`/`onmessage`/`onerror`/`onclose` |
| `close(code = 1000, reason = "")` | `void` | Closes the socket |
| `send(msg: ClientControlMsg)` | `void` | JSON-stringifies and sends (no-op when the socket is null) |
| `sendAbort(code, reason)` | `void` | Sends `CLIENT_ABORT` |
| `sendAction(action)` | `void` | Sends `ACTION` |
| `on(event, fn)` | `() => void` | Subscribe; returns unsubscribe |
<!-- ⬆️ 190 ⬆️ -->

`onopen` sends `RESUME` when `lastKnownEpoch != null`, otherwise `HELLO`. `HELLO` deliberately carries **no** `resume_token` — the token is server-issued and arrives in `HELLO_OK`. `meta` is added to `HELLO` only when not `undefined`.
<!-- ⬆️ 191 ⬆️ -->

`ControlEventMap` has 14 keys: the 12 server message kinds mapped to lowercase snake names (`hello_ok`, `resume_ok`, `file_ack`, `nack_chunk`, `flow`, `prune_node`, `reject_file`, `session_abort`, `commit_ok`, `commit_retry`, `probe_ack`, `ask`) plus `error: MfupError` and `close: { code: number; reason: string }`.
<!-- ⬆️ 192 ⬆️ -->

`SESSION_ABORT` is handled specially: it emits `session_abort`, emits `error` with `wsHandshakeFailed(code, reason)`, **and** rejects `ready()` with that same typed fatal error — this is how the session's reconnect loop distinguishes "the server refuses this session forever" (`not_found`, `auth_failed`, `bad_version`) from a transient network failure. A close with `code !== 1000` also emits `wsClosedUnexpected`. Parse failures emit `wsMessageParse` with the first 200 characters of the raw payload.
<!-- ⬆️ 193 ⬆️ -->

---

### 6.5 `data-channel.ts` — chunk transport

`DataChannel` moves encoded frames for a single leg. It has two mutually exclusive modes chosen at construction.
<!-- ⬆️ 194 ⬆️ -->

```ts
export interface DataChannelOpts {
  baseUrl: string;
  sessionId: string;
  legId: string;
  resumeToken: string;              // X-MFUP-Token header
  epoch: number;                    // query param; server rejects stale epochs
  signal?: AbortSignal;
  streaming?: boolean;              // default false
  flushBytes?: number;              // default 2 * 1024 * 1024
  onUploadProgress?: (bytesOnWire: number) => void;
}

export interface DataCommitResult { files: number; bytes: number; }
```
<!-- ⬆️ 195 ⬆️ -->

| Member | Type | Description |
|--------|------|-------------|
| `closed` / `failed` / `bytesSent` | getters | Channel status and cumulative bytes handed over |
| `commitResult` | `DataCommitResult \| null` | Parsed from the final batch POST response body |
| `onError(fn)` | `void` | Register the async error sink (single callback, last wins) |
| `open()` | `Promise<Response \| void>` | Streaming → the fetch promise; batch → resolves when `close()` finishes |
| `write(frame)` | `void` (synchronous) | Encodes and enqueues/buffers; throws `dataChannelClosed()` when closed |
| `drain()` | `Promise<void>` | Backpressure gate (see below) |
| `close()` | `Promise<void>` | Streaming → close the stream controller; batch → flush with `final=1` |
| `abort(reason?)` | `void` | Streaming → error the controller; batch → discard the buffer and resolve `open()` |
<!-- ⬆️ 196 ⬆️ -->

#### Streaming mode (`duplex: "half"`)

`_openStreaming()` issues one long-lived `fetch` to `…?seq=0&final=1&epoch={epoch}` with a `ReadableStream` body and `duplex: "half"`, governed by `new ByteLengthQueuingStrategy({ highWaterMark: 4 * 1024 * 1024 })` (4 MiB). `write()` calls `controller.enqueue(encoded)` directly. `drain()` blocks when `controller.desiredSize <= 0` and is released by the stream's `pull()` callback, i.e. when the network has actually consumed data — this is what keeps memory bounded when the disk outruns the link, and it is also why the *session* treats writer progress as real network progress in streaming mode.
<!-- ⬆️ 197 ⬆️ -->

#### Batch mode (default)

`write()` appends to `_batchBuffer`. `drain()` flushes once `_batchBufferBytes >= flushBytes` (default 2 MiB). `close()` flushes the remainder with `final=1`.
<!-- ⬆️ 198 ⬆️ -->

**Serialisation.** All flushes go through `_flushChain: Promise<void>` — POSTs are chained, so there is never more than one request in flight. Each flush takes the next `seq` from a monotonic counter and posts to `…?seq={seq}&final={0|1}&epoch={epoch}`. `_concatChunks` avoids a copy when the buffer holds exactly one chunk.
<!-- ⬆️ 199 ⬆️ -->

**Retry and backoff** (`_postWithRetry`): `MAX_ATTEMPTS = 4`, `RETRY_DELAYS = [500, 1000, 2000]` ms before attempts 2, 3, 4 (with `?? 2000` as the fallback). The loop breaks immediately if `signal.aborted`, both before the delay and after it. The retry policy:

| Outcome | Behaviour |
|---------|-----------|
| `2xx` | Success. On `final`, parse the JSON body for `commit` (→ `commitResult`) and `error` (→ `dataHttpError`). Add `body.byteLength` to `bytesSent` |
| Network error / timeout (thrown) | Record `dataWriteFailed` and retry |
| `5xx` and `attempt < MAX_ATTEMPTS - 1` | Retry with the **same** `seq` |
| `409` with body `{error:"seq_mismatch", expected: seq + 1}` on attempt > 0 | Treated as success — the previous "failed" attempt actually landed |
| Any other `4xx`/`409` | Not retryable; break |
| All attempts exhausted | `_failed = true`, `_onError(lastErr)` |
<!-- ⬆️ 200 ⬆️ -->

Retrying the same `seq` is safe because the server processes each batch POST atomically and advances its seq counter only after full processing. The comment records why this exists: frames inside a failed POST (notably `NODE` metadata killed by a flaky proxy) used to be silently lost, which is precisely how `COMMIT_OK` could be returned on an incomplete tree.
<!-- ⬆️ 201 ⬆️ -->

**Wire-level upload progress.** `_xhrPost()` uses `XMLHttpRequest` rather than `fetch` because XHR is the only browser API that exposes real upload progress. Before `send()` it captures `base = this._bytesSent` (bytes of previously *completed* posts) and installs:

```ts
xhr.upload.onprogress = (e) => { this.opts.onUploadProgress?.(base + e.loaded); };
```

so the callback always receives a cumulative byte count including the in-flight body. The session feeds this straight into `progress.setBodySent(BigInt(n))`. Without it a slow link showed a 2 MiB jump per completed POST instead of a moving bar.
<!-- ⬆️ 202 ⬆️ -->

**Cancellation.** `_xhrPost` registers `signal.addEventListener("abort", () => xhr.abort(), { once: true })` and removes it in a `cleanup()` run from every terminal handler. `onabort` rejects with `DOMException("aborted", "AbortError")`, `onerror`/`ontimeout` reject with `TypeError`. Combined with `DataChannel.abort()` discarding the batch buffer and the retry loop's two `signal.aborted` checks, a user cancel stops the network immediately rather than after draining megabytes.
<!-- ⬆️ 203 ⬆️ -->

---

### 6.6 `ingestion.ts` + `dnd.ts` — input → manifest

#### Core types (`ingestion.ts`)

```ts
export interface DiscoveredNode {
  nodeId: number;
  parentId: number;
  kind: NodeKind;
  name: string;
  size: bigint | null;      // exact for files, null for dirs
  mtimeMs: bigint | null;
  /** Files: async iterable of body chunks, optionally starting at a byte
   *  offset (implemented via Blob.slice). Directories: null. */
  openBody: ((offsetBytes?: number) => AsyncIterable<Uint8Array>) | null;
}

export type IngestCallback = (node: DiscoveredNode) => void | Promise<void>;

export interface IngestFilter {
  shouldDescend?(nodeId: number, name: string): boolean;  // false prunes the subtree
  shouldInclude?(nodeId: number, name: string): boolean;  // false skips the file
}
```
<!-- ⬆️ 204 ⬆️ -->

`NodeIdAllocator` hands out ids starting at `1` (`0` is reserved for the root); `current` returns the last allocated id.
<!-- ⬆️ 205 ⬆️ -->

**Body reading.** All four adapters read through one private async generator:

```ts
const READ_SLICE_SIZE = 65536; // 64 KiB slices
async function* blobChunks(blob: Blob, offset = 0): AsyncGenerator<Uint8Array>
```
<!-- ⬆️ 206 ⬆️ -->

It uses `blob.slice(pos, end).arrayBuffer()` in a loop rather than `Blob.stream()` or a `ReadableStream({pull})`. The comment records why: Firefox raises "Error in input stream" from `Blob.stream()` on multi-chunk files, and `ReadableStream({pull})` produced "The operation was aborted" in some cases. `slice + arrayBuffer` is the universally reliable path, and the `offset` parameter is what makes mid-file resume possible.
<!-- ⬆️ 207 ⬆️ -->

#### The four adapters

All four share the signature `(source, rootParentId: number, ids: NodeIdAllocator, cb: IngestCallback, filter?: IngestFilter) => Promise<void>`.

| Function | Input | Tree walk | Skipping |
|----------|-------|-----------|----------|
| `ingestFromHandles` | `FileSystemHandle[]` | Recursive `walkHandle`; directories iterate `(dirHandle as any).entries()`; file metadata from `getFile()` (`size`, `lastModified`) | `shouldDescend` false → the directory node is **not emitted** and the subtree is skipped; `shouldInclude` false → file skipped |
| `ingestFromEntries` | `FileSystemEntry[]` | Recursive `walkEntry`; directory children via `readAllEntries()`; file via promisified `entry.file()` | Same as above |
| `ingestFromFileList` | `ArrayLike<File>` with `webkitRelativePath` | Two passes: build a `VNode` tree by splitting `webkitRelativePath` on `/` and allocating ids, then DFS-emit | Same as above, applied during the emit pass |
| `ingestFromFiles` | `File[]` | Flat — every file is a direct child of `rootParentId` | `shouldInclude` false → `continue` (id already consumed) |
<!-- ⬆️ 208 ⬆️ -->

**Path normalisation.** There is no string path normalisation anywhere in ingestion. Structure is carried entirely by `(nodeId, parentId)` pairs and a single-segment `name` per node; only `ingestFromFileList` parses a path, splitting `webkitRelativePath` on `/` and treating the last segment as the file. Client-relative display paths (`"dir/sub/name.ext"`) are reconstructed later in `session.ts` by `nodePath()` walking parent links. Note that `ingestFromFileList` marks a `VNode` as a file whenever it is the last path segment for some entry, so a name colliding between a file and a directory resolves to the file branch.
<!-- ⬆️ 209 ⬆️ -->

**Directory listing completeness.** `readAllEntries()` wraps `FileSystemDirectoryReader.readEntries` in a recursive callback loop that accumulates batches and resolves only when a batch comes back empty — a single call is not guaranteed to return all children.
<!-- ⬆️ 210 ⬆️ -->

**Node ids are allocated before filtering.** `walkHandle`/`walkEntry` call `ids.alloc()` first, then consult the filter, so a filtered-out node still burns an id. This is what allows the session's filter (`shouldDescend: id => !prunedNodes.has(id)`, `shouldInclude: id => !rejectedFiles.has(id)`) to be keyed on ids the server sent back.
<!-- ⬆️ 211 ⬆️ -->

**Handle body reuse.** In `walkHandle`, `openBody` closes over the `File` already fetched during the scan rather than re-calling `getFile()`; the same holds for the entry and file-list adapters.
<!-- ⬆️ 212 ⬆️ -->

#### `dnd.ts` — extracting a source from browser events

```ts
export type UploadSource =
  | { kind: "handles";  handles: Promise<FileSystemHandle[]> }
  | { kind: "entries";  entries: FileSystemEntry[] }
  | { kind: "filelist"; files: ArrayLike<File> }   // tree via webkitRelativePath
  | { kind: "files";    files: File[] };           // flat
```
<!-- ⬆️ 213 ⬆️ -->

| Function | Signature | Behaviour |
|----------|-----------|-----------|
| `sourceFromDataTransfer` | `(dt: DataTransfer) => UploadSource \| null` | **Synchronous by contract** — must be called inside the `drop` handler |
| `sourceFromInput` | `(input: HTMLInputElement) => UploadSource \| null` | Copies out of the live `FileList` so the caller can reset `input.value` immediately |
<!-- ⬆️ 214 ⬆️ -->

`sourceFromDataTransfer` tries three strategies in order:

1. **`getAsFileSystemHandle()`** (Chrome/Edge) — collects one promise per item. It requires **every** item to support the call; a single miss sets `allHandles = false` and breaks out. Returns `{kind:"handles", handles}` where `handles` is a `Promise.all(...)` filtered for nulls. The promise is deliberately left unresolved because the `DataTransferItem` list is dead by the next microtask — the *calls* happen synchronously, the *awaiting* happens later.
2. **`webkitGetAsEntry()`** (Firefox/Safari, also Chrome) — returns `{kind:"entries"}`. `FileSystemEntry` objects are not tied to the `DataTransfer` lifecycle, so lazy streaming works.
3. **`DataTransfer.files`** — last resort, returns `{kind:"files"}`. The header comment notes Firefox invalidates these blobs after the drop handler returns, so lazy streaming may fail.
<!-- ⬆️ 215 ⬆️ -->

If `dt.items` is empty or absent, it falls straight to `dt.files`, returning `null` when there are no files at all.
<!-- ⬆️ 216 ⬆️ -->

`sourceFromInput` returns `{kind:"filelist"}` when `input.webkitdirectory` is truthy **or** the first file has a non-empty `webkitRelativePath`; otherwise `{kind:"files"}`. `MfupSession.upload()` consumes this union directly, and also accepts a bare `FileList` or `File[]`.
<!-- ⬆️ 217 ⬆️ -->

---

### 6.7 `progress.ts` — progress model

`ProgressTracker` merges two independent work units — *scan units* (one per discovered node, dimensionless) and *body bytes* — into one monotonic 0–1 fraction, while keeping exact counters available.
<!-- ⬆️ 218 ⬆️ -->

```ts
export interface ProgressSnapshot {
  scanDoneUnits: bigint;    // nodes discovered so far
  scanEstUnits: bigint;     // estimated total scan units (grows during scan)
  bodyDoneBytes: bigint;    // bytes ACCEPTED by the server (FILE_ACK)
  bodySentBytes: bigint;    // bytes handed to the transport; runs ahead of bodyDoneBytes
  bodyEstBytes: bigint;     // estimated total body bytes (sum of size hints)
  acceptedFiles: number;    // files fully accepted
  skippedFiles: number;     // pruned + rejected
  scanSealed: boolean;
  expiresAt: string | null;
  fraction: number | null;  // blended 0–1, monotonic, null before any data
}

export type ProgressListener = (snap: ProgressSnapshot) => void;
```
<!-- ⬆️ 219 ⬆️ -->

Units: `scanDoneUnits`/`scanEstUnits` are node counts (`session.ts` increments both by 1 per discovered node); all `body*` fields are bytes. All five are `bigint`; the two file counters are plain `number`.
<!-- ⬆️ 220 ⬆️ -->

**Three distinct byte counters.** `bodyEstBytes` is the denominator (accumulated from file sizes at scan time). `bodySentBytes` is what left the client. `bodyDoneBytes` is what the server durably accepted. `bodySentBytes` is what makes the bar move *between* server ACKs.
<!-- ⬆️ 221 ⬆️ -->

| Method | Signature | Description |
|--------|-----------|-------------|
| `on(fn)` | `(ProgressListener) => () => void` | Subscribe; returns unsubscribe |
| `updateScan(done, est, sealed)` | `(bigint, bigint, boolean) => void` | Replace scan counters; notifies |
| `addBodyEstimate(bytes)` | `(bigint) => void` | Add to the denominator; notifies |
| `setBodyAccepted(bytes)` | `(bigint) => void` | Monotonic — ignores values `<= _bodyDone`; notifies on increase |
| `setBodySent(total)` | `(bigint) => void` | Monotonic; **throttled** (see below) |
| `acceptFile()` / `skipFile()` | `() => void` | Increment the file counters; notify |
| `setFileCounts(accepted, skipped)` | `(number, number) => void` | Bulk-set during resume |
| `setExpiresAt(v)` | `(string \| null) => void` | Set the resume deadline; notifies |
| `setFromRootSummary(s)` | `(RootSummary-shaped) => void` | Bulk-restore from `RESUME_OK.root_summary`; converts each `number` to `bigint` |
| `snapshot()` | `() => ProgressSnapshot` | Builds a fresh object and computes `fraction` |
| `advanceBody(nodeId, acceptedOffset)` | `(number, bigint) => void` | **Empty body.** A comment states the session computes deltas itself and calls `setBodyAccepted` |
<!-- ⬆️ 222 ⬆️ -->

**Throttling.** Only `setBodySent` is rate-limited, via `static readonly SENT_NOTIFY_MS = 200` (≤5 notifications/sec). If at least 200 ms have elapsed since the last send-notify it fires immediately; otherwise it arms a single trailing `setTimeout` for the remainder so the final value always lands. Every other mutator notifies synchronously on every call — the session's own `SNAPSHOT_THROTTLE_MS = 33` coalescer is the second-stage damper for snapshot subscribers.
<!-- ⬆️ 223 ⬆️ -->

**Fraction computation.** Returns `null` while both `scanEst` and `bodyEst` are `0n`. Otherwise:

- `scanFrac = Number(scanDone * 10000n / scanEst) / 10000` (or `1` when `scanEst === 0n` and sealed) — the ×10000 detour keeps the division in `bigint` for 4-decimal precision.
- `bodyFrac` uses `ahead = max(min(bodySent, bodyEst), bodyDone)` — sent bytes are clamped to the estimate (retries can resend the same bytes) and never fall below what the server has accepted.
- `raw = 0.1 * scanFrac + 0.9 * bodyFrac` — a fixed 10 % scan / 90 % body blend.
- `clamped = Math.max(_lastFraction, Math.min(1, raw))`, and `_lastFraction` is updated — the bar can never move backwards even when estimates grow.
<!-- ⬆️ 224 ⬆️ -->

---

### 6.8 `errors.ts` — error taxonomy

Every failure surfaces as an `MfupError`, a subclass of `Error` carrying a machine-readable code, a layer, a human-facing remediation hint, and a fatality flag.
<!-- ⬆️ 225 ⬆️ -->

```ts
export class MfupError extends Error {
  readonly code: MfupErrorCode;
  readonly layer: MfupErrorLayer;
  readonly action: string;              // remediation hint, safe to show a user
  readonly fatal: boolean;              // true → not retryable, session is dead
  readonly detail: Record<string, unknown>;
  readonly timestamp: string;           // ISO 8601, set at construction
  toJSON(): Record<string, unknown>;    // flat form, with the cause chain unwrapped
  summary(): string;                    // `[${layer}/${code}] ${message}`
}
```
<!-- ⬆️ 226 ⬆️ -->

`MfupErrorLayer` has 5 members: `CONTROL: "control"`, `DATA: "data"`, `SESSION: "session"`, `INGEST: "ingest"`, `PROTOCOL: "protocol"`.
<!-- ⬆️ 227 ⬆️ -->

`MfupErrorCode` declares 25 codes. There is no per-code error *class* — `MfupError` is the only class, and 17 factory functions construct the codes actually in use. `fatal` is the retryability signal: `handleDisconnect()` gives up permanently on a `fatal` error from `connect()`, and the session records the first fatal error in `snapshot.fatalError`.
<!-- ⬆️ 228 ⬆️ -->

| Code | Layer | Factory | Thrown / emitted when | Fatal |
|------|-------|---------|----------------------|-------|
| `WS_CONNECT_FAILED` | control | `wsConnectFailed(url, cause?)` | `WebSocket.onerror`, or `connect()` fails with a non-`MfupError` cause | no |
| `WS_HANDSHAKE_FAILED` | control | `wsHandshakeFailed(code, reason)` | Server `SESSION_ABORT`; also rejects `ControlChannel.ready()`. Action text branches on `bad_version` / `conflict` / `auth_failed` | **yes** |
| `WS_CLOSED_UNEXPECTED` | control | `wsClosedUnexpected(wsCode, wsReason)` | WS close with `code !== 1000`. Action branches on 1006 (network) and 1008 (policy) | no |
| `WS_MESSAGE_PARSE` | control | `wsMessageParse(rawData, cause)` | `JSON.parse` of an inbound frame throws; `detail.rawData` is truncated to 200 chars | no |
| `WS_SEND_FAILED` | — | *(declared, no factory, no call site)* | — | — |
| `DATA_OPEN_FAILED` | data | `dataOpenFailed(url, cause)` | The streaming `fetch` throws, or `DataChannel.open()` rejects. Sniffs the cause message for `duplex`/`ReadableStream`/`body` and sets `detail.isDuplexIssue` | **yes** |
| `DATA_HTTP_ERROR` | data | `dataHttpError(url, status, statusText, body?)` | Non-2xx data POST, or a `{error}` field in the final POST body. Action branches on 409 / 410 / 503; body truncated to 500 chars | only on `410` |
| `DATA_WRITE_FAILED` | data | `dataWriteFailed(reason, cause?)` | `enqueue` throws, channel not opened, POST network error, or `safeWrite` with no channel | no |
| `DATA_STREAM_ERROR` | — | *(declared, no factory, no call site)* | — | — |
| `DATA_CHANNEL_CLOSED` | data | `dataChannelClosed()` | `DataChannel.write()` called after `close()`/`abort()` | no |
| `SESSION_ABORTED_BY_SERVER` | session | `sessionAbortedByServer(code, reason)` | `SESSION_ABORT` handler in `session.ts` | **yes** |
| `SESSION_ABORT_FAILED` | — | *(declared, no factory, no call site)* | — | — |
| `SESSION_RECONNECT_FAILED` | session | `sessionReconnectFailed(attempt, cause)` | Emitted once per reconnect attempt, before the backoff sleep | no |
| `SESSION_RECONNECT_EXHAUSTED` | session | `sessionReconnectExhausted(attempts, lastCause?)` | `reconnectCount >= maxReconnectAttempts`; also rejects the pending commit wait | **yes** |
| `SESSION_COMMIT_FAILED` | — | *(declared, no factory, no call site)* | — | — |
| `SESSION_ENDED_BAD_STATE` | — | *(declared, no factory, no call site)* | — | — |
| `PUBLISH_CONFLICT` | session | `publishConflict(conflictingFiles)` | `publish()` gets 409 with body `{error:"conflict_files"}`; `detail.conflictingFiles` holds the list. Action: ask the user, `sendAction("merge_overwrite")`, `publish()` again | no |
| `PUBLISH_FAILED` | session | `publishFailed(status, body)` | Any other publish failure; `status: 0` for a network throw | only on `404` |
| `INGEST_HANDLE_ERROR` | ingest | `ingestError(source, name, cause)` | Any of the four `upload*()` methods catches a non-`MfupError`, or `streamFile` throws while the channel is alive | no |
| `INGEST_READ_ERROR` | — | *(declared, no factory, no call site)* | — | — |
| `NACK_BAD_CHECKSUM` | protocol | `nackChunk(...)` with `reason:"bad_checksum"` | `NACK_CHUNK` — data corruption; chunk is resent | no |
| `NACK_BAD_OFFSET` | protocol | `nackChunk(...)` with `reason:"bad_offset"` | `NACK_CHUNK` — file rewinds to `expected_offset` | no |
| `NACK_STALE_EPOCH` | protocol | `nackChunk(...)` with `reason:"stale_epoch"` | `NACK_CHUNK` — the session was resumed elsewhere; this tab's data is stale | **yes** |
| `NACK_SERVER_POLICY` | protocol | `nackChunk(...)` with `reason:"server_policy"` | `NACK_CHUNK` — server refused the data | no |
| `UNKNOWN` | caller-supplied | `unknownError(layer, message, cause?)`, `probeError(cause)`, `nackChunk` fallback | Control channel closed unexpectedly (layer `control`); streaming probe threw (layer `data`, action: "Will use batch upload mode. This is normal for Firefox/Safari."); a `NACK_CHUNK` reason outside the 4 mapped ones — notably `unknown_node` | no |
<!-- ⬆️ 229 ⬆️ -->

Six codes (`WS_SEND_FAILED`, `DATA_STREAM_ERROR`, `SESSION_ABORT_FAILED`, `SESSION_COMMIT_FAILED`, `SESSION_ENDED_BAD_STATE`, `INGEST_READ_ERROR`) appear only in the `MfupErrorCode` declaration — no factory constructs them and nothing in `src/` references them. `nackChunk` maps only 4 of the 5 `NackChunkMsg.reason` values, so an `unknown_node` NACK surfaces as `UNKNOWN` at layer `protocol` while still triggering `resendNodeChain()`.
<!-- ⬆️ 230 ⬆️ -->

---

### 6.9 `probe.ts` — streaming capability detection

```ts
export interface ProbeResult { streaming: boolean; latencyMs: number; }

export async function probeStreaming(opts: {
  baseUrl: string;
  sessionId: string;
  resumeToken: string;
  control: ControlChannel;
  signal?: AbortSignal;
  timeoutMs?: number;   // default 1500
}): Promise<ProbeResult>
```
<!-- ⬆️ 231 ⬆️ -->

The probe answers one question: does this browser, over this transport path, actually support `duplex: "half"` request streaming? The verdict picks `DataChannel`'s mode for the session's lifetime.
<!-- ⬆️ 232 ⬆️ -->

**Stage 1 — synchronous feature check (free, no network).** Two `Request` constructions against a dummy URL with a closed `ReadableStream` body:

- Without `duplex` → must **throw** (Chrome raises `TypeError: duplex member must be specified`).
- With `duplex: "half"` → must **succeed**.
<!-- ⬆️ 233 ⬆️ -->

Streaming is possible only if both hold. Any other combination returns `{streaming: false, latencyMs: 0}` immediately, with zero network cost — this is the fast path for Firefox and Safari.
<!-- ⬆️ 234 ⬆️ -->

**Stage 2 — empirical round trip.** A `fetch` POST to `{baseUrl}/mfup/probe/{sessionId}` with `X-MFUP-Token`, a `ReadableStream` body, `duplex: "half"`, and the caller's `signal`. A synchronous throw here also returns `{streaming: false, latencyMs: 0}`. A 1 KiB zero-filled `chunkA` is enqueued (a throw on enqueue is another early `false`), and `performance.now()` is sampled.
<!-- ⬆️ 235 ⬆️ -->

**The verdict race** — first of three signals wins, guarded by a `settled` latch:

| Signal | Verdict |
|--------|---------|
| `PROBE_ACK` on the control channel | `(msg.first_chunk_bytes ?? 0) >= 1024` |
| `fetchPromise` resolves with `!resp.ok` | `false` |
| `fetchPromise` rejects | `false` |
| `timeoutMs` (default 1500 ms) elapses | `false` |
<!-- ⬆️ 236 ⬆️ -->

Checking `first_chunk_bytes` is what distinguishes real binary streaming from a browser that stringified the stream — a stringified `ReadableStream` arrives as 23 bytes, well under the 1 KiB threshold. Listening to the fetch promise is what makes the negative verdict fast: Chrome on plain HTTP/1.1 rejects streaming bodies immediately with `ERR_H2_OR_QUIC_REQUIRED`, and without that listener the probe sat out the full 1500 ms timeout before falling back. An early *successful* response also counts as a "no" — it means a proxy buffered the entire body, so the connection is not duplex.
<!-- ⬆️ 237 ⬆️ -->

Afterwards a second 1 KiB `chunkB` is enqueued and the stream closed (errors swallowed — the stream may already be errored from an abort), and the fetch is awaited and discarded. `latencyMs` is `Math.round(performance.now() - start)` when streaming succeeded, otherwise `0`.
<!-- ⬆️ 238 ⬆️ -->

**Caching.** `MfupSession` wraps this in a `localStorage` cache keyed `mfup:streaming-probe:{serverUrl}` holding `{v: boolean, ts: number}` with a 24-hour TTL, so a returning visitor pays zero probe cost. The rationale recorded in `session.ts`: the verdict depends on browser plus transport path, neither of which changes between visits. All `localStorage` access is try/catch-wrapped, so private mode and worker contexts simply re-probe.
<!-- ⬆️ 239 ⬆️ -->

---

## 7. @mfup/react — React Bindings

`@mfup/react` is a dependency-free hook layer (React is a peer, not a bundled dep) over `@mfup/client`. It contributes no protocol logic of its own: it owns a config context, a `useSyncExternalStore` adapter over `MfupSession`, a lifecycle hook that creates exactly one session per `start()` call, and drag-and-drop/file-picker prop getters that delegate extraction to the client's `sourceFromDataTransfer` / `sourceFromInput`. `examples/multiuser-scopes` consumes that surface from the published npm registry (plus `mfup-fastapi` from PyPI) to demonstrate server-enforced per-user upload scopes.
<!-- ⬆️ 240 ⬆️ -->

### 7.1 Package layout (`packages/react/`)

Five source modules, one barrel. `package.json` declares `"type": "module"`, `"sideEffects": false`, ESM-only exports (`import` + `types`, no `require` condition), and ships both `dist` and `src`.
<!-- ⬆️ 241 ⬆️ -->

| Field | Value |
|-------|-------|
| `name` / `version` | `@mfup/react` / `0.2.0` |
| `exports` | `.` → `./dist/index.js` + `./dist/index.d.ts`; `./package.json` |
| `peerDependencies` | `@mfup/client` `^0.2.0`, `react` `>=18` |
| `devDependencies` | `@mfup/client` `*`, `@types/react` `^18.3.0`, `react` `^18.3.0`, `typescript` `^5.5.0` |
| `scripts` | `build` (`tsc`), `check` (`tsc --noEmit`), `prepack` (`npm run build`) |
<!-- ⬆️ 242 ⬆️ -->

`tsconfig.json` targets ES2022 / module ES2022 with `moduleResolution: "bundler"`, `lib: ["ES2022","DOM","DOM.Iterable"]`, `strict`, `declaration` + `declarationMap` + `sourceMap`, `isolatedModules`, `rootDir: src` → `outDir: dist`.
<!-- ⬆️ 243 ⬆️ -->

The `react >=18` floor is load-bearing: `useMfupSession` is built on `useSyncExternalStore`, which is React 18+.
<!-- ⬆️ 244 ⬆️ -->

### 7.2 Public surface (`src/index.ts`)

Five value exports (one component, four hooks), five locally-defined type exports, and nine types re-exported verbatim from `@mfup/client` so an app can import everything from one specifier.
<!-- ⬆️ 245 ⬆️ -->

| Export | Kind | Source | Description |
|--------|------|--------|-------------|
| `MfupProvider` | component | `context.ts` | Supplies app-wide config defaults |
| `useMfupConfig()` | hook | `context.ts` | Nearest provider config, or `null` |
| `useMfupSession(session)` | hook | `useMfupSession.ts` | Reactive snapshot of an `MfupSession` |
| `useMfupUpload(options?)` | hook | `useMfupUpload.ts` | One upload lifecycle per `start()` |
| `useMfupDropzone(opts)` | hook | `useMfupDropzone.ts` | Drop + picker prop getters |
| `MfupConfigValue` | type | `context.ts` | Provider config shape |
| `UseMfupUploadOptions`, `UseMfupUploadResult` | types | `useMfupUpload.ts` | Upload hook I/O |
| `UseMfupDropzoneOptions`, `MfupDropzoneRootProps` | types | `useMfupDropzone.ts` | Dropzone hook I/O |
<!-- ⬆️ 246 ⬆️ -->

Re-exported from `@mfup/client` (types only): `MfupAsk`, `MfupAskAction`, `MfupError`, `MfupFileRef`, `MfupSession`, `MfupSessionSnapshot`, `ProgressSnapshot`, `SessionState`, `UploadSource`.
<!-- ⬆️ 247 ⬆️ -->

### 7.3 Config context (`src/context.ts`)

A `createContext<MfupConfigValue | null>(null)`. `MfupProvider` is written with `createElement` rather than JSX, so the package builds with plain `tsc` and no JSX runtime configuration.
<!-- ⬆️ 248 ⬆️ -->

```ts
export interface MfupConfigValue {
  /** Server base URL (may include a path prefix if the consumer mounted the
   * MFUP router under one, e.g. "https://host/api/uploads"). */
  serverUrl: string;
  targetDir?: string;
  /** Default session meta (delivered to the server's authorize/map hooks). */
  meta?: unknown;
  chunkSize?: number;
  maxReconnectAttempts?: number | null;
}
```
<!-- ⬆️ 249 ⬆️ -->

| Export | Signature | Behavior |
|--------|-----------|----------|
| `MfupProvider` | `(props: { config: MfupConfigValue; children?: ReactNode })` | Renders `MfupContext.Provider` with `value = props.config`. The value is passed through unmemoized — callers should hold a stable `config` object to avoid re-rendering consumers. |
| `useMfupConfig` | `(): MfupConfigValue \| null` | `useContext(MfupContext)`. Returns `null` when no provider is mounted; `useMfupUpload` tolerates that and requires per-call `serverUrl` instead. |
<!-- ⬆️ 250 ⬆️ -->

Read by `useMfupUpload`, which merges provider defaults under per-call options.
<!-- ⬆️ 251 ⬆️ -->

### 7.4 `useMfupSession` (`src/useMfupSession.ts`)

Adapts an `MfupSession` to React's external-store protocol. `MfupSession` already implements `subscribe(onChange) => unsubscribe` and `getSnapshot()` with referentially stable, coalesced snapshots, so the hook is a direct pass-through — tear-free under concurrent rendering, and costing exactly one subscription regardless of how many snapshot fields the component reads.
<!-- ⬆️ 252 ⬆️ -->

```ts
export function useMfupSession(session: MfupSession | null): MfupSessionSnapshot | null
```
<!-- ⬆️ 253 ⬆️ -->

| Aspect | Behavior |
|--------|----------|
| Null session | Accepted. `subscribe` becomes a shared `noopUnsubscribe`, `getSnapshot` returns `null`. |
| SSR | The `getServerSnapshot` argument is `() => null`, so server rendering yields `null` rather than throwing. |
| Re-render trigger | Once per new snapshot identity published by the session. Because snapshots are coalesced by the client, bursts of wire events collapse into a bounded number of renders. |
| Identity deps | `subscribe` and `getSnapshot` are `useCallback`s keyed on `[session]` — swapping sessions re-subscribes; re-rendering with the same session does not. |
| Cleanup | The unsubscribe returned by `session.subscribe` is invoked by `useSyncExternalStore` on unmount or session change. No abort is performed here. |
<!-- ⬆️ 254 ⬆️ -->

### 7.5 `useMfupUpload` (`src/useMfupUpload.ts`)

The primary hook: one `MfupSession` per `start()` call, driven to completion (connect → upload → settle asks → publish). Three design constraints are visible in the code:

- The session is constructed **inside `start()`**, never in an effect, so React StrictMode's double-invoked effects cannot spawn ghost sessions.
- Option callbacks are read through `optsRef` (reassigned on every render), so inline lambdas neither re-subscribe listeners nor go stale — the four `s.on(...)` handlers are registered once and always dispatch to the latest props.
- `autoPublish` awaits `settleAsks()`, so the transfer keeps streaming while a conflict dialog is open and publish happens only after the user's verdict.
<!-- ⬆️ 255 ⬆️ -->

The module-level `const TERMINAL = ["committed", "aborted", "failed"]` defines the three terminal states used by `busy`, `start`'s re-entrancy guard, `reset`, and the unmount cleanup.
<!-- ⬆️ 256 ⬆️ -->

#### Options

```ts
export interface UseMfupUploadOptions {
  serverUrl?: string;          // falls back to <MfupProvider>
  targetDir?: string;
  meta?: unknown;              // → server-side authorize/map_file hooks
  chunkSize?: number;
  maxReconnectAttempts?: number | null;
  autoPublish?: boolean;       // default true
  abortOnUnmount?: boolean;    // default false
  onCommitted?(ev: { files: number; bytes: number }): void;
  onPublished?(files: string[]): void;
  onAsk?(ask: MfupAsk): void;
  onError?(err: MfupError): void;
}
```
<!-- ⬆️ 257 ⬆️ -->

| Option | Default | Resolution / effect |
|--------|---------|---------------------|
| `serverUrl` | — | `opts.serverUrl ?? ctx.serverUrl`; missing ⇒ `start()` throws `"useMfupUpload: serverUrl missing — pass it in options or via <MfupProvider>"` |
| `targetDir`, `meta`, `chunkSize`, `maxReconnectAttempts` | provider value | `opts.X ?? ctx.X`, passed straight into the `MfupSession` constructor |
| `autoPublish` | `true` | After `settleAsks()`, publishes only when the verdict is not `"cancel"` **and** `s.state === "committed"` |
| `abortOnUnmount` | `false` | Uploads survive route changes by default |
| `onCommitted` | — | Bound to the session's `committed` event; receives `{ files, bytes }` |
| `onPublished` | — | Bound to `published`; receives `ev.published` (a `string[]`) |
| `onAsk` | — | Bound to `ask`; receives the `MfupAsk` |
| `onError` | — | Bound to `error`; receives the `MfupError` |
<!-- ⬆️ 258 ⬆️ -->

#### Result

```ts
export interface UseMfupUploadResult {
  session: MfupSession | null;
  snapshot: MfupSessionSnapshot | null;
  pendingAsks: readonly MfupAsk[];
  busy: boolean;
  start(source: UploadSource | FileList | File[]): Promise<void>;
  abort(): void;
  publish(): Promise<{ published: string[] }>;
  reset(): void;
}
```
<!-- ⬆️ 259 ⬆️ -->

| Member | Type | Behavior |
|--------|------|----------|
| `session` | `MfupSession \| null` | State-backed; `null` until the first `start()`, `null` again after `reset()` |
| `snapshot` | `MfupSessionSnapshot \| null` | `useMfupSession(session)` — the sole re-render driver during a transfer |
| `pendingAsks` | `readonly MfupAsk[]` | `useMemo` over `snapshot.asks.filter(a => a.answered === null)`; recomputed only when the snapshot identity changes; `[]` when no snapshot |
| `busy` | `boolean` | `snapshot !== null && !TERMINAL.includes(snapshot.state)` |
| `start` | `(source) => Promise<void>` | Accepts an `UploadSource`, a `FileList`, or `File[]`. Constructs the session, registers the four listeners, commits it to `sessionRef` **and** `setSession`, then `await connect()` → `await upload(source)` → `await settleAsks()` → conditional `await publish()`. Rejects on fatal errors. |
| `abort` | `() => void` | `sessionRef.current?.abort()` with no arguments; no-op when there is no session |
| `publish` | `() => Promise<{ published: string[] }>` | Manual publish for `autoPublish: false` flows; throws `"useMfupUpload: no session to publish"` when `sessionRef` is empty |
| `reset` | `() => void` | If the current session is non-terminal, aborts it with `("client_reset", "hook reset")`; then clears `sessionRef` and `setSession(null)` so `start()` can run again |
<!-- ⬆️ 260 ⬆️ -->

#### Re-entrancy, re-render, and cleanup semantics

| Concern | Guarantee |
|---------|-----------|
| Concurrent starts | `start()` throws `"useMfupUpload: an upload is already running — abort() it or wait"` when the previous session's `state` is not in `TERMINAL`. The check reads `sessionRef` (synchronous), not React state. |
| Callback identity | `start`, `abort`, `publish`, `reset` are all `useCallback(..., [])` — referentially stable for the component's lifetime, so they are safe as effect deps or as props to memoized children. |
| Options freshness | `optsRef.current = options` and `ctxRef.current = ctx` execute on every render; `start()` re-reads both at call time, so late-changing `serverUrl`/`meta` are picked up without recreating anything. |
| Re-renders | Two sources only: `setSession` (once per `start`, once per `reset`) and snapshot publication via `useSyncExternalStore`. Passing new inline option lambdas causes no extra subscription churn. |
| Unmount | A `useEffect(..., [])` cleanup aborts with `("client_unmount", "component unmounted")` **only** when `optsRef.current.abortOnUnmount` is true and the session is non-terminal. With the default `false`, an in-flight upload keeps running after the component unmounts. |
| StrictMode | The dev-mode mount/unmount/remount cycle runs that cleanup while `sessionRef.current` is still `null` (no session exists before `start()`), so it is inert; session creation in `start()` is what makes this safe. |
<!-- ⬆️ 261 ⬆️ -->

Usage:

```tsx
const { snapshot, pendingAsks, busy, start, abort } = useMfupUpload({
  meta: { scope: "workspace" },
  onPublished: () => refresh(),
});
```
<!-- ⬆️ 262 ⬆️ -->

### 7.6 `useMfupDropzone` (`src/useMfupDropzone.ts`)

Prop getters for a drop target and a file input. All source extraction is delegated to `sourceFromDataTransfer` / `sourceFromInput` from `@mfup/client`, invoked **synchronously inside the event handler** — required because `DataTransferItem.getAsFileSystemHandle` / `webkitGetAsEntry` are only valid during the drop event.
<!-- ⬆️ 263 ⬆️ -->

```ts
export interface UseMfupDropzoneOptions {
  onSource(source: UploadSource): void;
  disabled?: boolean;
}

export interface MfupDropzoneRootProps {
  onDragEnter(e: DragEvent<HTMLElement>): void;
  onDragOver(e: DragEvent<HTMLElement>): void;
  onDragLeave(e: DragEvent<HTMLElement>): void;
  onDrop(e: DragEvent<HTMLElement>): void;
}
```
<!-- ⬆️ 264 ⬆️ -->

| Return member | Type | Behavior |
|---------------|------|----------|
| `isDragActive` | `boolean` | `useState`; true between the outermost `dragenter` and the matching `dragleave`/`drop`. Not set when `disabled`. |
| `getRootProps()` | `() => MfupDropzoneRootProps` | Stable (`useCallback([])`). Returns a fresh object of four handlers to spread onto the drop container. |
| `getInputProps(o?)` | `(o?: { directory?: boolean }) => {...}` | Stable (`useCallback([])`). Returns `type: "file"`, `multiple: true`, an `onChange`, plus `webkitdirectory: ""` when `o.directory` is true (folder picker). |
<!-- ⬆️ 265 ⬆️ -->

Handler details:

| Event | Action |
|-------|--------|
| `onDragEnter` | `preventDefault()`, increments `depthRef`, sets drag-active unless `disabled` |
| `onDragOver` | `preventDefault()` only — required for the browser to fire `drop` |
| `onDragLeave` | Decrements `depthRef` (floored at 0); clears drag-active only at depth 0 |
| `onDrop` | `preventDefault()`, zeroes depth, clears drag-active; returns early when `disabled`; otherwise `sourceFromDataTransfer(e.dataTransfer)` and calls `onSource` if non-null |
| input `onChange` | Returns early when `disabled`; `sourceFromInput(e.currentTarget)`, then sets `e.currentTarget.value = ""` (safe because `sourceFromInput` copies out of the live `FileList`) so re-picking the same folder re-fires; calls `onSource` if non-null |
<!-- ⬆️ 266 ⬆️ -->

The `depthRef` counter exists because `dragenter`/`dragleave` bubble from every child element — without it the highlight flickers as the pointer crosses children. `optsRef` mirrors the latest options each render, so `disabled` and `onSource` are always read fresh despite the getters being stable. The hook holds no session and performs no cleanup on unmount; it owns only local state.
<!-- ⬆️ 267 ⬆️ -->

### 7.7 `examples/multiuser-scopes` — dogfooding consumer app

A two-part example (FastAPI server + Vite/React SPA) written strictly against the **published** packages, the way an outside consumer would: `mfup-fastapi==0.2.0` from PyPI and `@mfup/client` / `@mfup/react` from npm — no workspace links. It demonstrates the OIDC-style auth pipeline in miniature: users are auto-created via cookie, each user gets three file zones, and a single `authorize` hook converts `(cookie, HELLO.meta.scope)` into a server-owned destination so files always land at `<DATA_DIR>/<user_id>/<scope>/…` regardless of what the browser requested.
<!-- ⬆️ 268 ⬆️ -->

#### Dependencies (exact, as declared)

| Side | Package | Version spec | Installed |
|------|---------|--------------|-----------|
| server | `mfup-fastapi` | `==0.2.0` (pinned) | — |
| client | `@mfup/client` | `^0.2.6` | 0.2.6 |
| client | `@mfup/react` | `^0.2.0` | 0.2.0 |
| client | `react`, `react-dom` | `^18.3.1` | — |
| client (dev) | `@types/react` `^18.3.12`, `@types/react-dom` `^18.3.1`, `typescript` `^5.5.0`, `vite` `^6.0.0` | — | — |
<!-- ⬆️ 269 ⬆️ -->

The client is `"private": true`, `"type": "module"`, named `mfup-example-multiuser-scopes`, with scripts `dev` (`vite`), `build` (`tsc --noEmit && vite build`), and `preview` (`vite preview`). Its `tsconfig.json` is `noEmit` with `jsx: "react-jsx"`, ES2022 target, bundler resolution, `strict`, and `isolatedModules`.
<!-- ⬆️ 270 ⬆️ -->

#### Server (`server/app.py`)

Module constants: `DATA_DIR = Path(os.environ.get("DEMO_DATA_DIR", "./data")).resolve()`, `SCOPES = ("workspace", "scratch", "uploads")`, `COOKIE_NAME = "demo_uid"`.
<!-- ⬆️ 271 ⬆️ -->

| Symbol | Kind | Description |
|--------|------|-------------|
| `_uid_from_headers(headers)` | function | Parses the `Cookie` header with `SimpleCookie`, extracts `demo_uid`, and accepts it only if `uid.isalnum()` and `8 <= len(uid) <= 64`; else `None`. Takes a plain `Mapping[str, str]` so the same code serves HTTP requests and the WebSocket handshake. |
| `authorize(req: AuthRequest)` | async hook | The entire multiuser/scope policy. Returns `AuthResult \| None`. |
| `engine` | `MfupEngine` | Built from `MfupConfig(base_dir=DATA_DIR, redis_url=os.environ.get("REDIS_URL", "redis://localhost:6379/0"), authorize=authorize)` |
| `app` | `FastAPI` | `title="MFUP example — multiuser scopes"`, `lifespan=engine.lifespan` |
| `whoami` | route `GET /api/whoami` | Auto-creates the demo user |
| `list_files` | route `GET /api/files/{scope}` | Top-level listing of one zone |
<!-- ⬆️ 272 ⬆️ -->

The router is mounted under a prefix inside an otherwise ordinary app:

```python
app = FastAPI(title="MFUP example — multiuser scopes", lifespan=engine.lifespan)
app.include_router(engine.router, prefix="/api/mfup")
```
<!-- ⬆️ 273 ⬆️ -->

**How scoping is enforced.** `authorize` is the only policy surface, and it denies by returning `None` in two cases: no valid cookie (the SPA is expected to call `/api/whoami` first), and `req.meta["scope"]` not in `SCOPES` (also covering non-dict `meta`). Otherwise it returns:

```python
return AuthResult(
    base_dir=str(DATA_DIR / uid),   # per-user home (staging lives inside)
    target_dir=scope,               # SERVER owns the layout: <uid>/<scope>/
    max_total_bytes=512 * 2**20,    # 512 MiB per session
    max_files=20_000,
    context={"uid": uid, "scope": scope},
)
```
<!-- ⬆️ 274 ⬆️ -->

Because `base_dir` is rooted at the user's own directory and `target_dir` is fixed to the validated scope name, a client-supplied `targetDir` cannot escape or cross into another user's tree. Staging lives inside the per-user home, so commit is a rename within one filesystem subtree. `context` carries `{uid, scope}` forward to the engine's downstream hooks. Per-session limits are 512 MiB and 20,000 files.
<!-- ⬆️ 275 ⬆️ -->

**Own routes.** `GET /api/whoami` mints `uid = secrets.token_hex(8)` on first visit (16 hex chars — satisfying the alnum / 8–64 check), creates `DATA_DIR/uid`, and sets the cookie with `max_age=30*24*3600`, `httponly=True`, `samesite="lax"`; it returns `{"user_id": uid, "scopes": [...]}`. `GET /api/files/{scope}` returns `403` with `{"error": "unknown user or scope"}` for a missing cookie or unknown scope, otherwise lists `DATA_DIR/uid/scope` one level deep, directories first then by name, as `{"scope": ..., "entries": [{name, dir, size}]}` with `size: null` for directories.
<!-- ⬆️ 276 ⬆️ -->

#### Client (`client/src/`)

`main.tsx` mounts `<App />` inside `<StrictMode>` via `createRoot` and imports `./style.css` — the StrictMode wrapper is exactly the condition `useMfupUpload` is designed against (session created in `start()`, not in an effect).
<!-- ⬆️ 277 ⬆️ -->

`App.tsx` bootstraps the user, then wires the provider once at the root:

```tsx
<MfupProvider config={{ serverUrl: `${location.origin}/api/mfup` }}>
```
<!-- ⬆️ 278 ⬆️ -->

`App` fetches `/api/whoami` in a mount effect, stores `user_id`, renders `creating your demo user…` until it resolves, and falls back to the string `"(server unreachable — is examples/…/server running?)"` on failure. It then renders one `<ScopeZone>` per entry of `SCOPES = ["workspace", "scratch", "uploads"] as const`.
<!-- ⬆️ 279 ⬆️ -->

`ScopeZone` is the whole demo, and touches only the published hook surface:

| Wiring | Code | Purpose |
|--------|------|---------|
| Upload | `useMfupUpload({ meta: { scope }, onPublished: () => void refresh() })` | The scope travels as session meta → arrives as `AuthRequest.meta` on the server; the listing refreshes after publish |
| Dropzone | `useMfupDropzone({ disabled: busy, onSource: (src) => { start(src).catch(() => {}) } })` | Drops and picks feed `start()`; the zone is inert while an upload runs; the rejection is swallowed because failures are surfaced through the snapshot |
| Root props | `<section className={"zone" + (isDragActive ? " drag" : "")} {...getRootProps()}>` | Drag highlight |
| Pickers | `<input {...getInputProps()} />` and `<input {...getInputProps({ directory: true })} />` | Files picker and folder picker in the same header |
| Listing | `fetch(\`/api/files/${scope}\`)` in a `useCallback`/`useEffect` pair keyed on `scope` | Server-disk viewer; a transient failure keeps the previous listing |
<!-- ⬆️ 280 ⬆️ -->

Rendering reads exclusively from the snapshot store: `snapshot.state`, `snapshot.fraction` (→ percent bar), `snapshot.reconnect.attempt` (badge reads `reconnecting #N`), `snapshot.progress.acceptedFiles`, `snapshot.progress.bodyDoneBytes`, `snapshot.currentFile.path`, `snapshot.published`, and `snapshot.fatalError.{code,message}`. Derived flags are `cancelled = state === "aborted"`, `failed = state === "failed"`, and `done = state === "committed" && snapshot.published != null`. A `Cancel upload` button calls `abort` directly. Note `fmtBytes(n: number | bigint)` — byte counters may arrive as `bigint`, and it coerces with `Number(n)`.
<!-- ⬆️ 281 ⬆️ -->

The interactive conflict path is the point of the example: `pendingAsks.map(...)` renders one dialog per unanswered ask with two buttons, `ask.respond("merge_overwrite")` and `ask.respond("cancel")`, while the transfer keeps streaming in the background. Dropping the same folder twice triggers it; answering `cancel` aborts the session so nothing is published and the previous files stay untouched.
<!-- ⬆️ 282 ⬆️ -->

#### Running it

```bash
# server (Python 3.10+, reachable Redis)
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
uvicorn app:app --port 8090
# sanity: curl -s localhost:8090/api/mfup/health  → "crc32c":"native"

# client
npm run dev     # Vite on 0.0.0.0:20061
```
<!-- ⬆️ 283 ⬆️ -->

Optional server environment: `DEMO_DATA_DIR` (default `./data`) and `REDIS_URL` (default `redis://localhost:6379/0`).
<!-- ⬆️ 284 ⬆️ -->

`vite.config.ts` fixes both `server` and `preview` to `host: "0.0.0.0"`, `port: 20061`, `strictPort: true`, `allowedHosts: true`, and proxies a single prefix:

```ts
proxy: { "/api": { target: BACKEND, changeOrigin: true, ws: true } }
```
<!-- ⬆️ 285 ⬆️ -->

with `BACKEND = process.env.EXAMPLE_BACKEND_URL ?? "http://localhost:8090"`. One rule covers both the app's own `/api/whoami` and `/api/files/*` routes and the mounted MFUP router at `/api/mfup/*`; `ws: true` is what forwards the control-channel WebSocket upgrade. Because the SPA derives `serverUrl` from `location.origin`, the proxy also makes the cookie same-origin, which is what lets `authorize` read `demo_uid` off the WebSocket handshake headers.
<!-- ⬆️ 286 ⬆️ -->

#### The Node edition (`examples/multiuser-scopes-node/`)

The same example against `@mfup/server` instead of `mfup-fastapi`, proving the wire-compatibility contract from a consumer's seat: `server/server.mjs` is an express app that installs `@mfup/server` from npm and wires MFUP in **two lines** — `app.use("/api/mfup", mfup.middleware)` for the HTTP endpoints and `mfup.attach(server)` for the control WebSocket. The policy is the identical single `authorize` hook, returning `baseDir: <DATA_DIR>/<uid>`, `targetDir: scope`, `maxTotalBytes: 512 * 2**20` and `maxFiles: 20_000` for a valid cookie and a `meta.scope` in `["workspace","scratch","uploads"]`, and `null` otherwise. It serves its own `GET /api/whoami` (which additionally reports `server: "@mfup/server (node)"`) and `GET /api/files/:scope`.
<!-- ⬆️ 287 ⬆️ -->

It runs with **no Redis at all** — the default memory session store, with live sessions re-discovered after a restart by scanning `DATA_DIR` for staging directories (§5.2). Data lands in `./uploads-node/<uid>/<scope>/`, against `./uploads/…` for the Python edition. Crucially the **React client is reused unchanged** from `examples/multiuser-scopes/client`: both editions bind-mount the same client directory, so nothing on the browser side is aware of which server it is talking to.
<!-- ⬆️ 288 ⬆️ -->

From the repo root, `docker compose up -d server-node client-node` serves the app on `http://localhost:20062`. Natively the server needs **Node ≥ 22.13** for `node:sqlite`: `npm install && node server.mjs` in `examples/multiuser-scopes-node/server` (listens on `:8091`, data under `./data/<uid>/<scope>/`, overridable with `DEMO_DATA_DIR`; setting `REDIS_URL` switches the memory store for the Redis one), then `npm install && EXAMPLE_BACKEND_URL=http://localhost:8091 npm run dev -- --port 20062` in `examples/multiuser-scopes/client` — the shared client directory from the Python edition.
<!-- ⬆️ 289 ⬆️ -->

---

## 8. Demo Application & Benchmarks

The `demo/` directory is a Vite 6 multi-page app that exercises `@mfup/client` and `@mfup/react` against a live MFUP/2 server. It is not published — it aliases the sibling packages by **source path**, so editing `packages/client/src/*` hot-reloads the demo. Four HTML pages sit on four independent TypeScript entry points: an interactive upload demo, a head-to-head benchmark against a naive POST-per-file baseline, a scripted harness driven by the Playwright e2e suite, and a React-hooks smoke target. The baseline for the benchmark is `benchmarks/trivial-server/app.py`, a deliberately minimal FastAPI server deployed alongside the MFUP backend and proxied at `/trivial/*`.
<!-- ⬆️ 290 ⬆️ -->

Note that `demo/` is the internal dev/test playground, not an integration reference — for that, see `examples/multiuser-scopes` (§7.7), which is what the root `docker-compose.yaml` runs.
<!-- ⬆️ 291 ⬆️ -->

### 8.1 Entry points

| Page | Script | Rollup input name | Scenario exercised | URL on the e2e stack |
|------|--------|-------------------|--------------------|----------------------|
| `demo/index.html` | `demo/src/demo.ts` | `main` | Full interactive upload UX: drag-and-drop, folder picker, progress, conflict modal, error cards, abort/reset | `/` |
| `demo/compare.html` | `demo/src/compare.ts` | `compare` | MFUP/2 vs sequential multipart POST, two lanes racing on the same file set | `/compare.html` |
| `demo/e2e.html` | `demo/src/e2e.ts` | `e2e` | Headless control surface (`window.mfupE2E`) building OPFS trees for Playwright | `/e2e.html` |
| `demo/react.html` | `demo/src/react-demo.tsx` | `react` | `@mfup/react` hooks (`MfupProvider`, `useMfupUpload`, `useMfupDropzone`) with `data-testid` hooks | `/react.html` |
<!-- ⬆️ 292 ⬆️ -->

**Running them.** The demo is served two ways. The internal dev/test stack (`e2e/docker-compose.yaml`) runs `frontend-build` (`npm ci && npm run build`) and has Caddy serve `demo/dist` on host port **20060**, proxying `/mfup/*` and `/health` to `backend:8070` and `/trivial/*` (prefix stripped) to `trivial:8071`. For hot reload, the `frontend` service is gated behind the `dev` Compose profile and runs `vite` on port 3000. Natively, `npm run dev` in `demo/` starts the same dev server on `0.0.0.0:3000`; `npm run build` then `npm run preview` serves the static build on port 20060. Playwright's `baseURL` defaults to `http://localhost:20060` (`MFUP_BASE_URL` overrides).
<!-- ⬆️ 293 ⬆️ -->

All four scripts derive the server URL from the page itself — `demo.ts` and `compare.ts` compute `` `${location.protocol}//${location.host}` ``, `e2e.ts` and `react-demo.tsx` use `location.origin` — so the client always talks back through whatever proxy served the page.
<!-- ⬆️ 294 ⬆️ -->

### 8.2 Main demo (`demo/src/demo.ts` + `demo/index.html`)

Wires `MfupSession` to a drag-and-drop UI. Sessions are created with `targetDir: "."` and `chunkSize: 256 * 1024`.
<!-- ⬆️ 295 ⬆️ -->

The file-header comment documents the drop-handler priority chain and why it exists: `getAsFileSystemHandle()` (Chrome/Edge, persistent handles) → `webkitGetAsEntry()` (Firefox/Safari, persistent `FileSystemEntry`) → `<input>` file picker. `DataTransferItem.getAsFile()` and `e.dataTransfer.files` are deliberately not used for drag-and-drop because Firefox invalidates the underlying blob after the handler returns. The page delegates the whole chain to `sourceFromDataTransfer(e.dataTransfer)`, which must run synchronously inside the drop handler — and does.
<!-- ⬆️ 296 ⬆️ -->

| Control (element id) | Trigger | Effect |
|----------------------|---------|--------|
| `#dropzone` | `dragover` / `dragleave` | Toggles `.dragover` class |
| `#dropzone` | `drop` | `sourceFromDataTransfer` → `startUpload`; ignored if a session is live |
| `#dropzone` | `click` | Opens `#file-input` if no session is live |
| `#browse-btn` | `click` | Opens `#file-input` (`multiple`), `stopPropagation` |
| `#browse-folder-btn` | `click` | Opens `#folder-input` (`webkitdirectory`), `stopPropagation` |
| `#file-input` / `#folder-input` | `change` | `sourceFromInput(input)` → `startUpload` |
| `#btn-abort` | `click` | `session.abort()`; disabled outside `active` / `paused_by_server` / `waiting_resume` |
| `#btn-reset` | `click` | Aborts a live session with reason `client_reset` before clearing all UI state |
| `#modal-overwrite` | `click` | `ask.respond("merge_overwrite")` |
| `#modal-cancel` | `click` | `ask.respond("cancel")`, sets `cancelled = true` |
<!-- ⬆️ 297 ⬆️ -->

The progress panel (`#progress`) shows a state badge (`#state-badge`, class-driven per `SessionState`), a truncated session id, a fill bar (`#bar`), and four stat tiles:

| Tile id | Label | Source field |
|---------|-------|--------------|
| `#stat-bytes` | Transferred | `snap.bodyDoneBytes` |
| `#stat-files` | Files Accepted | `snap.acceptedFiles` |
| `#stat-scanned` | Scanned | `snap.scanDoneUnits` |
| `#stat-skipped` | Skipped | `snap.skippedFiles` |
<!-- ⬆️ 298 ⬆️ -->

| Internal | Behavior |
|----------|----------|
| `scheduleProgress(snap)` | Coalesces progress renders to one per `requestAnimationFrame` — `ProgressTracker` emits synchronously on every mutation, which for many-small-file trees is tens of thousands of DOM writes |
| `renderState(state)` | `committed` → green bar at 100%, abort disabled; `aborted`/`failed` → red bar, abort disabled; `active`/`paused_by_server`/`waiting_resume` → abort enabled |
| `renderError(err)` | Prepends an error card: code, `layer / FATAL\|recoverable`, message, action, `detail` JSON, collapsible stack trace + cause chain. Capped at `MAX_ERROR_CARDS = 50` |
| `formatCauseChain(err)` | Walks `.cause` up to depth 5, joining each stack with a `Caused by:` prefix |
| `log(msg, cls)` | Timestamped entries (`en-US`, `hour12: false`, `fractionalSecondDigits: 3`), capped at `MAX_LOG_ENTRIES = 200` |
| `fmtBytes(n)` | `B` / `KB` / `MB` (1 decimal) / `GB` (2 decimals), accepts `bigint` |
<!-- ⬆️ 299 ⬆️ -->

Session events handled: `state`, `ask`, `committed`, `reconnecting`, `error`, plus `onProgress`. The `committed` handler is the interesting one — if a conflict modal is still open it awaits `conflictPromise` before calling `session.publish()`, and skips publishing entirely if the user chose `cancel`. `reconnecting` overwrites the badge with `RECONNECTING <attempt>/<max>` (`∞` when `maxAttempts` is null). On connect the log records the negotiated mode: `streaming (duplex:half)` or `batch (sequential POST)` from `session.streamingMode`.
<!-- ⬆️ 300 ⬆️ -->

### 8.3 Benchmark race (`demo/src/compare.ts` + `demo/compare.html`)

The performance claim of the project, measured in-browser. One dropzone feeds **both** lanes; `startRace` launches them with `Promise.allSettled([runMfupLane(...), runTrivialLane(...)])`, so the two uploads run **concurrently over the same connection budget**.
<!-- ⬆️ 301 ⬆️ -->

**Input preparation (outside both timers).** The drop handler resolves the dropped items three ways, in order, and produces two things: the *native source* for MFUP and a flattened `FlatFile[]` (`{ file: File; path: string }`) for the baseline.

| Path | Detection | MFUP mode | Flattener |
|------|-----------|-----------|-----------|
| 1 | every item yields `getAsFileSystemHandle()` (Chrome/Edge) | `handles` → `session.uploadHandles()` | `flattenHandles()` — recurses `dir.entries()`, calls `getFile()` per file |
| 2 | `webkitGetAsEntry()` returns entries (Firefox/Safari) | `entries` → `session.uploadEntries()` | `flattenEntry()` — recurses `createReader().readEntries()` in batches until empty |
| 3 | fallback `e.dataTransfer.files` | `files` → `session.uploadFiles()` | one `FlatFile` per file, `path = f.name` |
<!-- ⬆️ 302 ⬆️ -->

Clicking the dropzone opens `#file-input` (`multiple`) and takes path 3 with `mode: "files"`.
<!-- ⬆️ 303 ⬆️ -->

Note the asymmetry, which is inherent to the design: directory traversal and `File` materialization for the baseline happen in the drop handler **before** `startRace`, so the baseline's timer excludes tree walking, while MFUP's scan happens inside `session.upload*()` and therefore inside its timer. Per-file `FormData` construction is inside the baseline timer.
<!-- ⬆️ 304 ⬆️ -->

**What each lane measures.**

| | MFUP lane (`runMfupLane`) | Baseline lane (`runTrivialLane`) |
|---|---|---|
| Timer starts | before `session.connect()` | before the POST loop |
| Work timed | `connect()` (incl. streaming probe) + `uploadHandles`/`uploadEntries`/`uploadFiles` (scan, chunked transfer, commit) | for each file: build `FormData`, `await fetch(POST /trivial/upload)` — strictly one at a time, sequentially |
| Timer stops | after the upload call resolves | after the last POST returns |
| Excluded | `publish()` — issued from the `committed` handler, after the timer stopped | nothing after the loop |
| Request shape | 1 WebSocket control channel + the data channel (streaming `duplex:half`, or sequential POSTs in batch mode) | N multipart requests, N = file count |
| Failure handling | catch → badge `error`, log | non-`ok` response logs and `continue`s to the next file |
| Target | `targetDir: "mfup-target"`, `chunkSize: 256 * 1024` | `UPLOAD_DIR` of the trivial service |
<!-- ⬆️ 305 ⬆️ -->

**Reported metrics.** Per lane: elapsed wall time via `LaneTimer` (`performance.now()`, re-rendered on a 100 ms `setInterval`, displayed as `N.N sec`), a percentage fill bar, a file counter, and a byte counter. Sources differ — MFUP reads `ProgressSnapshot.fraction`, `.acceptedFiles`, `.bodyDoneBytes`; the baseline accumulates `file.size` after each *successful* POST and computes `doneBytes / totalBytes`. `LaneTimer.elapsed()` supplies the final `[MFUP] Complete in X sec` / `[Traditional] Complete in X sec` log lines. There is no aggregate speedup figure computed in code — the comparison is the two timers side by side.
<!-- ⬆️ 306 ⬆️ -->

**Publish.** The MFUP lane does not use `session.publish()`; it issues the HTTP call directly so the demo shows the wire shape:

```ts
await fetch(`${serverUrl}/mfup/sessions/${session.id}/publish`, {
  method: "POST",
  headers: { "X-MFUP-Token": session.token },
});
```
<!-- ⬆️ 307 ⬆️ -->

**Conflicts.** The `ask` handler reveals `#conflict-overlay` and answers via `session.sendAction("merge_overwrite" | "cancel")`; the `committed` handler awaits that answer before publishing and skips publish on `cancel`.
<!-- ⬆️ 308 ⬆️ -->

**UI.** Two `.lane` panels (`.lane.mfup`, `.lane.trivial`) titled "MFUP/2 Protocol" and "Traditional (file-by-file)", each with badge (`waiting` / `running` / `done` / `error`), a 2.4 rem timer, a bar, and files/bytes stats — element ids `mfup-{timer,bar,badge,files,bytes}` and `trivial-{timer,bar,badge,files,bytes}`. `#btn-reset` (disabled during a race) clears both lanes, the log, and the file input.
<!-- ⬆️ 309 ⬆️ -->

#### Baseline server (`benchmarks/trivial-server/app.py`)

FastAPI app titled `Trivial Upload Server`, run by the `trivial` Compose service with `python-multipart`, uvicorn on port 8071, and `UPLOAD_DIR=/data/uploads/trivial-target`. It is the industry-default pattern MFUP/2 is measured against: no sessions, no resume, no integrity checks, no batching.
<!-- ⬆️ 310 ⬆️ -->

| Route | Method | Parameters | Behavior |
|-------|--------|-----------|----------|
| `/upload` | POST | `file: UploadFile = File(...)`, `path: str = Form(default="")` | Joins `path / file.filename`, resolves against `BASE_DIR` and returns `403 {"error": "path traversal"}` if it escapes, `mkdir(parents=True, exist_ok=True)`, streams the body to disk in 256 KiB reads, returns `{"ok": true, "file": <rel>, "size": <bytes>}` |
| `/health` | GET | — | `{"status": "ok"}` |
<!-- ⬆️ 311 ⬆️ -->

`BASE_DIR` comes from `UPLOAD_DIR` (default `/data/uploads`). The demo reaches `/upload` as `/trivial/upload`; the Vite proxy and the Caddyfile both strip the `/trivial` prefix.
<!-- ⬆️ 312 ⬆️ -->

Per `benchmarks/README.md`, the motivating measurement (from `docs/MFUP_RU.md`) is a 188-file, 1.0 MB project folder uploaded POST-per-file in **17.2 s** — roughly 13 s of request round-trips and 2.9 s of `FormData` construction against ~1.3 s of actual transfer. The README explicitly warns that the gap is RTT-dependent and that the race page should be run over a real network path, not localhost.
<!-- ⬆️ 313 ⬆️ -->

### 8.4 E2E harness (`demo/src/e2e.ts` + `demo/e2e.html`)

Not part of the demo UX. `e2e.html` is a bare page containing a single `<pre id="status">` element; the script exposes a control surface on `window.mfupE2E` and builds deterministic file trees in **OPFS**, uploading them through a real `MfupSession` so the `ingestFromHandles` adapter is driven by genuine `FileSystemDirectoryHandle` objects.
<!-- ⬆️ 314 ⬆️ -->

**Control surface — `window.mfupE2E` (6 functions, assigned at module end):**

| Function | Signature | Purpose |
|----------|-----------|---------|
| `detect()` | `() => Promise<{ opfs: boolean; createWritable: boolean }>` | Capability probe. Opens OPFS, creates and removes a `.probe` file, reports whether `createWritable` exists. Specs use it to skip on engines without OPFS write support |
| `run(opts)` | `(RunOpts) => Promise<RunResult>` | Full happy-path/conflict run: build tree → connect → `uploadHandles([root])` → optionally answer the ASK → publish |
| `runAndAbort(opts)` | `(RunOpts & { abortAfterBytes: number }) => Promise<{ sessionId, state, log }>` | Starts an upload without awaiting it, polls progress every 50 ms, and calls `session.abort("client_cancel", "test abort")` once `bodyDoneBytes >= abortAfterBytes`. Returns the session id so a test can assert disk/Redis cleanup |
| `progress()` | `() => object \| null` | Last `ProgressSnapshot` flattened to numbers: `bodyDoneBytes`, `bodyEstBytes`, `scanDoneUnits`, `acceptedFiles`, `fraction`, plus live `state` |
| `bytesFor(key, size)` | `(string, number) => Uint8Array` | Deterministic content generator (xorshift seeded by FNV-1a) |
| `fnv1a(s)` | `(string) => number` | 32-bit FNV-1a hash |
<!-- ⬆️ 315 ⬆️ -->

`bytesFor`/`fnv1a` are duplicated in `e2e/lib/gen.ts` and must stay byte-for-byte identical — the Node-side verifier regenerates expected content to compare against what landed on disk.
<!-- ⬆️ 316 ⬆️ -->

**Input/output shapes:**

```ts
interface ManifestEntry { path: string; size: number }          // "/"-separated, relative to root
interface ConflictOpts {
  action: "merge_overwrite" | "cancel";
  when: "on_ask" | "after_commit";   // answer immediately, or only after COMMIT_OK arrived
}
interface RunOpts {
  rootName: string;      // top-level dir name, unique per test run
  targetDir: string;     // server-side target dir
  manifest: ManifestEntry[];
  chunkSize?: number;    // default 256 * 1024
  contentSeed?: string;  // default rootName — vary it to prove overwrite replaced bytes
  conflict?: ConflictOpts;  // absent → never answer an ASK
}
interface RunResult {
  sessionId: string; streaming: boolean | null; epoch: number; state: string;
  committed: { files: number; bytes: number } | null;
  published: unknown; publishStatus: number | null;
  uploadError: string | null; askSeen: boolean;
  errors: { code: string; msg: string; fatal: boolean }[];
  log: string[]; reconnects: number;
}
```
<!-- ⬆️ 317 ⬆️ -->

**Observable side channels.** `#status` in `e2e.html` is stepped through `building OPFS tree...` → `connecting...` → `uploading...` → `publishing...` → `done`. The returned `log` array is the machine-readable trace, with these tags: `state:<s>`, `streaming:<bool>`, `reconnecting:<attempt>:<delay>`, `committed:<files>:<bytes>`, `ask:<code>[:<name>]`, `answer:<action>`, `uploadError:<msg>` (and `uploadErr:<msg>` in `runAndAbort`).
<!-- ⬆️ 318 ⬆️ -->

**Behaviors specs depend on:**
- `buildOpfsTree` removes any existing `rootName` recursively before recreating it, so reruns are clean; file bytes are `bytesFor(seed + "/" + entry.path, size)`.
- `ConflictOpts.when === "after_commit"` makes the ASK handler await the `committed` promise before responding — this is the interactive-transfer invariant, that the upload keeps running while the user decides and the answer may land either side of `COMMIT_OK`.
- Publish is retried up to **4 times** with a 300 ms delay, but only when the error code is `PUBLISH_CONFLICT` — the ACTION travels over WebSocket while publish is HTTP, so a `merge_overwrite` answer can still be in flight. Any other error breaks out immediately. `publishStatus` is taken from `e.detail.status`, or `409` when the code is `PUBLISH_CONFLICT`.
- Publish is skipped entirely when nothing committed or when `conflict.action === "cancel"`.
<!-- ⬆️ 319 ⬆️ -->

Specs driving this page: `abort.spec.ts`, `conflict-matrix.spec.ts`, `meta-loss.spec.ts`, `opfs-upload.spec.ts`, `resume.spec.ts`, `retention.spec.ts` (all navigate to `/e2e.html`).
<!-- ⬆️ 320 ⬆️ -->

### 8.5 React demo (`demo/src/react-demo.tsx` + `demo/react.html`)

The full upload UX built only on the public `@mfup/react` surface, mounted with `createRoot` into `#root` inside `<StrictMode>` and wrapped in `<MfupProvider config={{ serverUrl: location.origin }}>`. It doubles as the e2e smoke target for the React package (`e2e/tests/react.spec.ts`).
<!-- ⬆️ 321 ⬆️ -->

Two hooks do all the work:

```tsx
const { snapshot, pendingAsks, busy, start, abort, reset } = useMfupUpload({
  targetDir: ".", meta: { app: "react-demo" },
  onCommitted, onPublished, onError,
});
const { isDragActive, getRootProps, getInputProps } = useMfupDropzone({
  disabled: busy,
  onSource: (src) => start(src),
});
```
<!-- ⬆️ 322 ⬆️ -->

`getInputProps()` produces the file input; `getInputProps({ directory: true })` produces the folder input.
<!-- ⬆️ 323 ⬆️ -->

**`data-testid` surface (14):**

| testid | Element | Content / action |
|--------|---------|------------------|
| `dropzone` | div with `getRootProps()` | Adds `.dragover` when `isDragActive` |
| `file-input` | input | `getInputProps()` |
| `folder-input` | input | `getInputProps({ directory: true })` |
| `state` | span | `snapshot?.state ?? "idle"` |
| `reconnect` | span | Rendered only when `snapshot.reconnect` exists: `reconnecting #<attempt>` |
| `bar` | `<progress max=100>` | `Math.round(snapshot.fraction * 100)` |
| `stats` | div | `<acceptedFiles> files · <bodyDoneBytes>` plus `· sending <currentFile.path>` when present |
| `published` | div | `snapshot.published.join(", ")` |
| `ask` | div, one per `pendingAsks` entry | `"<name>" already exists. Overwrite?` |
| `ask-overwrite` | button | `ask.respond("merge_overwrite")` |
| `ask-cancel` | button | `ask.respond("cancel")` |
| `abort` | button | Rendered only while `busy`; calls `abort` |
| `reset` | button | Rendered when not `busy` and a snapshot exists; calls `reset` |
| `log` | `<pre>` | Rolling 50-line log fed by `onCommitted` / `onPublished` / fatal `onError` / dropzone `onSource` |
<!-- ⬆️ 324 ⬆️ -->

### 8.6 Build configuration

#### `demo/vite.config.ts`

Proxy targets default to docker-compose service names and are overridable by env var for CI jobs without Docker (the macOS WebKit runner):

| Env var | Default |
|---------|---------|
| `MFUP_BACKEND_URL` | `http://backend:8070` |
| `MFUP_TRIVIAL_URL` | `http://trivial:8071` |
<!-- ⬆️ 325 ⬆️ -->

The `PROXY` object (4 rules) is shared by `server` and `preview`:

| Prefix | Target | Options |
|--------|--------|---------|
| `/mfup/control` | `BACKEND` | `changeOrigin`, **`ws: true`** — must precede `/mfup` so the WebSocket upgrade is not swallowed by the generic rule |
| `/mfup` | `BACKEND` | `changeOrigin` |
| `/health` | `BACKEND` | `changeOrigin` |
| `/trivial` | `TRIVIAL` | `changeOrigin`, `rewrite: p => p.replace(/^\/trivial/, "")` |
<!-- ⬆️ 326 ⬆️ -->

| Section | Setting | Value / rationale |
|---------|---------|-------------------|
| `resolve.dedupe` | `["react", "react-dom"]` | The aliased `@mfup/react` source would otherwise resolve React from the workspace root while example code resolves its own copy — two React instances break the hooks dispatcher |
| `resolve.alias` | `@mfup/client` → `../packages/client/src/index.ts` | Exact-file alias: the demo builds against package **source** for cross-package hot reload; consumers install the built packages and import the same specifiers |
| | `@mfup/react` → `../packages/react/src/index.ts` | same |
| `build.rollupOptions.input` | `main`, `compare`, `e2e`, `react` | The four HTML pages, resolved absolutely |
| `server` | `host: "0.0.0.0"`, `port: 3000`, `strictPort: true`, `allowedHosts: true` | Container-reachable dev server; `allowedHosts: true` accepts any Host header (proxied/tunnelled access) |
| `server.fs.allow` | `[<repo root>, "/packages", "/app"]` | Repo root for native runs plus the docker-compose mount points |
| `preview` | `host: "0.0.0.0"`, `port: 20060`, `strictPort: true`, `allowedHosts: true`, same `PROXY` | Serves the built `dist` statically — used by the macOS CI job because WebKit plus the HMR dev server hangs on the frozen macOS WebKit build; static files match what Linux/Caddy serves |
<!-- ⬆️ 327 ⬆️ -->

`publicDir` is `public` (currently empty) and `root` is `.`.
<!-- ⬆️ 328 ⬆️ -->

#### `demo/tsconfig.json`

`target`/`lib` ES2022 with `DOM` + `DOM.Iterable`, `module: ESNext`, `moduleResolution: "bundler"`, `strict: true`, `jsx: "react-jsx"`, `esModuleInterop`, `skipLibCheck`, `forceConsistentCasingInFileNames`. `paths` mirrors the Vite aliases with an extra deep-import mapping:

| Specifier | Maps to |
|-----------|---------|
| `@mfup/client` | `../packages/client/src/index.ts` |
| `@mfup/client/*` | `../packages/client/src/*` |
| `@mfup/react` | `../packages/react/src/index.ts` |

<!-- ⬆️ 329 ⬆️ -->
`include` covers `src/**/*.ts`, `src/**/*.tsx`, and both package source trees, so type-checking the demo also type-checks the packages it aliases.
<!-- ⬆️ 330 ⬆️ -->

#### `demo/package.json`

Private, `type: "module"`, name `mfup-demo`. It is **not** a workspace member of the root `mfup-monorepo` (which lists only `packages/client` and `packages/react`) — it installs independently from its own `package-lock.json`.
<!-- ⬆️ 331 ⬆️ -->

| Script | Command |
|--------|---------|
| `dev` | `vite` |
| `build` | `vite build` |
| `preview` | `vite preview` |

<!-- ⬆️ 332 ⬆️ -->
Dependencies: `react` / `react-dom` `^18.3.1`. Dev dependencies: `@types/react` `^18.3.31`, `@types/react-dom` `^18.3.7`, `typescript` `^5.7.0`, `vite` `^6.0.0`. Neither `@mfup/client` nor `@mfup/react` appears as a dependency — they are resolved purely through the alias/paths mapping.
<!-- ⬆️ 333 ⬆️ -->

---

## 9. Testing & Deployment

MFUP/2 is verified by three independent layers. `server/tests/` is a pytest suite that drives `mfup-core` and `mfup-fastapi` in-process — real per-session SQLite databases in `tmp_path`, a fake WebSocket capturing control messages, no network. `packages/server/test/` is a vitest suite that does the same for `@mfup/server`, plus wire-level integration over a real `http` + `ws` server. `e2e/` is a Playwright suite that drives Chromium, Firefox and WebKit against a full containerized stack (Caddy → backend → Redis, plus a built demo bundle), including chaos tests that `docker compose kill` the backend mid-upload and assert byte-exact recovery; it runs against the Python and the Node backend in turn (§9.8). Both stacks share one bind-mounted `uploads/` directory, which is how the Node-side assertions read what the server actually wrote.
<!-- ⬆️ 334 ⬆️ -->

| Layer | Location | Runner | Coverage |
|-------|----------|--------|-------|
| Python unit / integration | `server/tests/` | `cd server && python -m pytest tests -q` | Protocol, paths, recovery, publication and FastAPI control/data handlers |
| Node unit / integration | `packages/server/test/` | `npm test -w @mfup/server` | Matching protocol and lifecycle cases plus real HTTP/WebSocket requests |
| Browser end-to-end | `e2e/tests/` | `cd e2e && npx playwright test` | Chromium, Firefox and WebKit; backend restart and byte comparisons |
<!-- ⬆️ 335 ⬆️ -->

### 9.1 Wire-protocol tests (`server/tests/test_protocol.py`)

Eight synchronous tests covering CRC-32C and the incremental `FrameReader`. Frames are hand-assembled with `struct` (`_build_frame` writes `!I` length = 1 + payload, then the tag byte), so the tests pin the exact on-wire byte layout rather than round-tripping through the encoder.
<!-- ⬆️ 336 ⬆️ -->

| Test | Scenario / invariant asserted |
|------|-------------------------------|
| `test_crc32c_empty` | `crc32c(b"")` is `0`. |
| `test_crc32c_known` | `crc32c(b"hello")` is `0x9A71BB4C` — pins the Castagnoli polynomial against a known vector. |
| `test_decode_node_frame` | A hand-built `NODE` payload (`!I` node_id, `!I` parent_id, kind byte, `!H`-prefixed UTF-8 name, presence byte + `!Q` size_hint, presence byte for mtime) decodes to one `NodeFrame` with `node_id=42`, `parent_id=1`, `kind=FILE`, `name="test.txt"`, `size_hint=1024`, `mtime_ms is None`. |
| `test_decode_file_chunk_frame` | A `FILE_CHUNK` payload (`!I` node_id, `!Q` offset, `!I` length, checksum-kind byte, `!I` checksum, body) decodes to a `FileChunkFrame` whose `payload` and `checksum` match the CRC computed over the body. |
| `test_decode_dir_close` | A 4-byte `DIR_CLOSE` payload decodes to `DirCloseFrame(node_id=5)`. |
| `test_decode_session_end` | A `SESSION_END` payload (four `!Q` counters + sealed byte) decodes with `scan_done_units=100`, `body_done_bytes=5000`, `sealed is True`. |
| `test_incremental_feed` | Feeding a frame **one byte at a time** yields zero frames on every byte except the last, which yields exactly one — the reader never emits a partial frame and never loses buffered bytes. |
| `test_multiple_frames` | Two `DIR_CLOSE` frames fed in a single `feed()` drain as two frames in order (`node_id` 1 then 2). |
<!-- ⬆️ 337 ⬆️ -->

### 9.2 Hardening suite (`server/tests/test_edge_cases.py`)

The shared fixtures are `FakeWS`, which records every control message and exposes `of_type(t)`, and `make_session(tmp_path, sid)`, which opens a real session DB via `open_session_db`, calls `db.init_session(...)`, constructs `LiveSession(sid, "tok", tmp_path, db, target_dir=".")`, attaches leg `"leg1"` and swaps in the fake socket. Two helpers stream whole trees: `_send_tree(s, files)` emits NODE/FILE_OPEN/FILE_CHUNK/FILE_CLOSE per entry plus a terminal `SessionEndFrame`, and `_upload_tree` additionally asserts `try_commit()` returns `COMMIT_OK`. `test_review.py` adds path ownership, mapped-plan, incomplete-request and control ownership regressions, including requests through the FastAPI router.
<!-- ⬆️ 338 ⬆️ -->

#### Name validation and path traversal

| Test | Scenario / invariant asserted |
|------|-------------------------------|
| `test_validate_node_name_rejects` | `validate_node_name` raises `ValueError` for all 8 params: `""`, `"."`, `".."`, `"a/b"`, `"a\\b"`, `"a\x00b"`, `"/"`, `"..\\.."` — empty, dot-entries, both separators, and NUL. |
| `test_validate_node_name_accepts` | Passes without raising for all 6 params: `"file.txt"`, `"a b c"`, `"файл ❤.md"`, `".hidden"`, `"..foo"`, `"foo.."` — unicode, spaces, leading dot and dot-prefixed/suffixed names stay legal. |
| `test_illegal_name_node_dropped_and_rejected` | A `NODE` named `".."` is not stored (`db.get_node(1) is None`), *is* recorded in `s.dropped_nodes` so the commit node-count invariant still balances, and the client receives `REJECT_FILE`. |
| `test_traversal_name_never_escapes_payload` | A `"../evil"` node is rejected at ingest and no `evil` entry is created outside the payload root. |
| `test_resolve_payload_path_broken_chain_raises` | `resolve_payload_path` raises `ValueError` when a node's parent id (99) was never stored — an orphan chain can never resolve to a path. |
| `test_resolve_payload_path_normal` | For `src/app.ts`, the resolved path is `is_relative_to(staging_dir(base, sid)/"payload")` and has `name == "app.ts"`. |
<!-- ⬆️ 339 ⬆️ -->

#### Type collisions

| Test | Scenario / invariant asserted |
|------|-------------------------------|
| `test_dir_on_file_collision_rejects_not_crashes` | A file `foo` is written to disk, then a `DIR` node also named `foo` arrives (mkdir over a file). Node 2 appears in a `REJECT_FILE` message and `s.state` remains `ACTIVE`. |
| `test_file_on_dir_collision_rejects_not_crashes` | A dir `bar` exists, then `FILE_OPEN` for a file named `bar` raises `IsADirectoryError` internally; node 2 is `REJECT_FILE`-ed and the session stays `ACTIVE`. |
<!-- ⬆️ 340 ⬆️ -->

#### Storage failure and commit termination

| Test | Scenario / invariant asserted |
|------|-------------------------------|
| `test_enospc_aborts_session` | `FileWriter._sync_write` is monkeypatched to raise `OSError(errno.ENOSPC)`. The session transitions to `ABORTED` and the last `SESSION_ABORT` carries `code == "storage_full"`. |
| `test_commit_retry_capped` | A file with `final_size=100` but `accepted_offset=0` can never complete. Driving `set_state(COMMITTING)` + `try_commit()` for `MAX_COMMIT_RETRIES + 4` iterations must reach `FAILED` and emit `SESSION_ABORT` — the consecutive-no-progress cap prevents an infinite commit loop. |
| `test_duplicate_file_open_closes_prior_writer` | A second `FILE_OPEN` for the same node installs a *different* writer object and the first writer's `_fh.closed` is true — no leaked file handle. |
<!-- ⬆️ 341 ⬆️ -->

#### Quotas and chunk policy

| Test | Scenario / invariant asserted |
|------|-------------------------------|
| `test_byte_quota_aborts_session` | With `quota_max_bytes = 5`, a 10-byte chunk drives the session to `ABORTED` with `SESSION_ABORT` code `"quota_exceeded"`. |
| `test_file_quota_aborts_session` | With `quota_max_files = 2`, two file nodes keep the session `ACTIVE`; the third aborts it with `"quota_exceeded"`. |
| `test_duplicate_node_does_not_double_count_file_quota` | With `quota_max_files = 1`, replaying the *same* NODE frame (the resume path) leaves the session `ACTIVE` and `files_seen == 1` — re-sent metadata is idempotent against the quota. |
| `test_oversized_chunk_nacked` | With `max_chunk_bytes = 8`, a 16-byte chunk produces `NACK_CHUNK` with `reason == "server_policy"` while the session stays `ACTIVE` — a client policy violation is recoverable, not fatal. |
<!-- ⬆️ 342 ⬆️ -->

#### Hook loading

| Test | Scenario / invariant asserted |
|------|-------------------------------|
| `test_load_hook_valid` | `load_hook("os.path:join")` returns the callable; `fn("a","b") == "a/b"`. |
| `test_load_hook_invalid_raises` | Raises `ImportError`/`AttributeError`/`ModuleNotFoundError` for all 4 params: `""`, `"no_colon"`, `"nonexistent.module:fn"`, `"os.path:nonexistent"`. |
<!-- ⬆️ 343 ⬆️ -->

#### Per-session base directories and recovery

| Test | Scenario / invariant asserted |
|------|-------------------------------|
| `test_per_session_base_dir_staging_and_publish` | `SessionRegistry(global_base).create(..., target_dir="incoming", base_dir=home)` stages at `home/.incoming.sid1/payload` and creates **nothing** under `global_base`. A one-file upload commits (`COMMIT_OK`), `publish_session(home, "sid1", home/"incoming")` returns `["hello.txt"]`, the bytes match, and the staging dir is reclaimed. |
| `test_recover_session_derives_base_from_staging_parent` | After detaching and removing a session, a *fresh* `SessionRegistry` recovers it from the staging path alone (`recover_session("sid2", home/".incoming.sid2")`); `s2.base_dir == home` — the base is derived from the staging parent, as lazy-resume does from Redis meta — and `resume(...)` returns state `ACTIVE`. |
| `test_auth_and_meta_persist_across_recovery` | `client_meta` (`{"scope":"avatars","album":7}`), `quota_max_bytes=12345`, `quota_max_files=10` and `auth_context={"user_id":"alice"}` all survive registry teardown and `recover_session` on a new registry — authorization results are durable, not per-connection. |
<!-- ⬆️ 344 ⬆️ -->

#### Mapped publish

| Test | Scenario / invariant asserted |
|------|-------------------------------|
| `test_mapped_publish_by_type` | `list_payload_files` returns sorted relative paths `["c.jpg","shots/a.jpg","shots/b.pdf"]`. `publish_session_mapped` with a per-file mapping writes `media/avatars/{a,c}.jpg` and `docs/b.pdf`, the client's `shots/` layout is **not** replicated in the target, and staging is reclaimed. |
| `test_mapped_publish_rejects_escape_and_collision` | `MappingError` for a mapping to `"../evil"` (escape) and for two sources mapped to the same destination (collision). After both failures the staging payload is intact and the target is absent or empty — mapping validation is all-or-nothing. |
<!-- ⬆️ 345 ⬆️ -->

#### `on_committed` hook and programmatic publish

| Test | Scenario / invariant asserted |
|------|-------------------------------|
| `test_on_committed_notification_fields` | The event carries `session_id="oc1"`, `target_dir="out"`, `files=1`, `bytes=5`, `meta={"scope":"docs"}`, `context={"user_id":"alice"}` and an existing `staging_dir`. Returning `None` means notification only: the session stays in the registry and the target dir is never created. |
| `test_on_committed_publish_verdict` | Returning `"publish"` publishes server-side: bytes land in `out/b.txt`, the registry entry is gone, `index.removed == ["oc2"]`, and staging is removed. |
| `test_on_committed_hook_error_is_contained` | A hook raising `RuntimeError` still yields `COMMIT_OK` to the client, state `COMMITTED`, a live registry entry and intact staging — a broken consumer hook cannot damage a completed commit. |
| `test_engine_publish_typed_errors` | `MfupEngine.publish` raises `SessionNotFound` for an unknown id and `NotCommitted` for an `ACTIVE` session; after commit it returns `["d.txt"]`, writes the bytes and removes the session from the index. |
<!-- ⬆️ 346 ⬆️ -->

`make_engine` constructs `MfupEngine(MfupConfig(base_dir=tmp_path, on_committed=...))` and wires `registry` and a `FakeIndex` by hand, deliberately skipping `startup()` so the hook tests need no Redis.
<!-- ⬆️ 347 ⬆️ -->

### 9.3 Node server suite (`packages/server/test/`)

A vitest suite covering `@mfup/server` in-process and over HTTP/WebSocket, run with `npm test -w @mfup/server`. `helpers.ts` supplies the shared fixtures; `review.test.ts` adds path ownership, mapped-plan and incomplete-frame regressions.
<!-- ⬆️ 348 ⬆️ -->

| File | Cases | Covers |
|------|-------|--------|
| `protocol.test.ts` | 14 | Byte-layout of the frame codec and codec symmetry against `@mfup/client`'s encoders — the client is a devDependency precisely so the two implementations are cross-checked |
| `edge-cases.test.ts` | 22 | Mirror of the Python `test_edge_cases.py` hardening suite: name validation, traversal, type collisions, quotas, chunk policy, storage failure, commit-retry cap |
| `handler.test.ts` | 13 | Wire-level integration over a real `http` + `ws` server: data-plane fencing (bad token `403`, stale leg `409`, stale epoch `409`, bad seq `409`), the probe endpoint, and a full drop-socket → `RESUME` → finish → publish cycle |
| `store.test.ts` | 6 | `MemoryStore` behavior and staging-scan restart recovery |
<!-- ⬆️ 349 ⬆️ -->

### 9.4 Playwright specs (`e2e/tests/`)

Every spec resolves `UPLOADS` to `<repo>/uploads` and asserts on the files the server actually wrote, using `verifyTree` from `e2e/lib/gen.ts`. Chaos specs resolve `REPO` to `e2e/` and shell out to `docker compose` with that cwd, so they always act on the **e2e** stack. Tests tagged `@chromium-only` are excluded at collection time on firefox/webkit via `grepInvert`, so they never instantiate a page fixture there.
<!-- ⬆️ 350 ⬆️ -->

| Spec file | Test(s) | Entry point | Verified behavior |
|-----------|---------|-------------|-------------------|
| `opfs-upload.spec.ts` | `OPFS handles upload → commit → publish → bytes verified` | `/e2e.html` | Uploads a generated OPFS tree via real `FileSystemDirectoryHandle`s (`MfupSession.uploadHandles()`), skipping if `detect()` reports no `opfs`/`createWritable`. Asserts no fatal errors, `committed.bytes === totalBytes(manifest)`, `publishStatus === 200`, and byte-exact tree at `uploads/e2e/<rootName>`. Runs on all three engines. |
| `folder-input.spec.ts` | `webkitdirectory input upload via demo page` | `/` (index.html) | Drives the demo's `#folder-input` (`ingestFromFileList` adapter) with a real on-disk tree written by `writeTreeToDisk`. Waits for `#log` to contain `Published:`, asserts `#state-badge` is `COMMITTED` (attaching `#error-list` text as the failure message), and verifies `uploads/<rootName>` — the demo uses `targetDir "."`. Runs on all three engines. |
| `react.spec.ts` | `react hooks: folder upload → auto-publish → bytes on disk` | `/react.html` | Smoke for `@mfup/react`: a folder set on `[data-testid=folder-input]` must drive `useMfupUpload`/`useMfupDropzone` through commit and autoPublish. Asserts `[data-testid=log]` contains `published:`, `[data-testid=state]` is exactly `committed`, `[data-testid=published]` contains the root name, and bytes match. `openReactPage` captures `pageerror`/console errors so a mount failure reports the cause rather than a bare timeout. Runs on all three engines. |
| `react.spec.ts` | `react hooks: conflict ask dialog → overwrite → published @chromium-only` | `/react.html` | Second upload into the same target must surface the hook-rendered dialog (`[data-testid=ask]`); clicking `[data-testid=ask-overwrite]` publishes and leaves a byte-exact tree. |
| `conflict.spec.ts` | `second upload into same target → ASK → merge_overwrite → published @chromium-only` | `/` (index.html) | Vanilla demo conflict path: upload, `#btn-reset`, re-upload → `#conflict-overlay` becomes visible → `#modal-overwrite` → `Published:` and byte-exact tree. |
| `conflict-matrix.spec.ts` | 4 cases from `CASES` (`overwrite`/`cancel` × `on_ask`/`after_commit`), all `@chromium-only` | `/e2e.html` (`mfupE2E.run`) | Round 1 uploads v1 and publishes; round 2 re-uploads the same tree with `contentSeed = rootName + "#v2"` and an answer scheduled either mid-transfer (`on_ask`) or after commit (`after_commit`). `askSeen` must be true in every case. Overwrite: `committed.bytes === totalBytes(manifest)`, `publishStatus === 200`, and `verifyTree(..., v2seed)` proves the **new** bytes replaced the old. Cancel: `publishStatus` is null, the target still verifies against the v1 seed, local state is terminal (`aborted`/`failed`), and `expectStagingGone` polls up to 15 s for the server to remove the `.incoming.<sessionId>` directory. Each case gets a 240 s timeout. |
| `resume.spec.ts` | `backend restart mid-upload → reconnect → resume → verified bytes @chromium-only` | `/e2e.html` | Uploads `standardManifest({ bigFileBytes: 60_000_000, smallFiles: 8 })`, polls `mfupE2E.progress()` until `bodyDoneBytes > 2_000_000`, then `docker compose kill backend` + `start backend` (SIGKILL, because `restart`'s SIGTERM lets uvicorn drain and the upload can win that race). Asserts `committed.bytes === totalBytes`, `reconnects >= 1`, `publishStatus === 200`, and a byte-exact tree at `uploads/e2e-resume/<rootName>`. 420 s timeout. Exercises Redis recovery, COMMITTING→ACTIVE revert, `FileWriter` seek+truncate, epoch fencing and client backoff. |
| `resume.spec.ts` | `graceful restart mid-upload of a single big file → resumes and commits @chromium-only` | `/e2e.html` | Regression for the commit-during-pump race the SIGKILL test above passed on interleaving luck. A single 120 MB `big.bin`, `docker compose restart backend` (graceful SIGTERM) once `bodyDoneBytes > 5_000_000`. One file means that after `RESUME` the requeued file is shifted into `activeFile` while the queue reads empty — the exact shape that let `finalizeScan` enter the commit loop mid-stream. Asserts `committed.bytes === totalBytes`, `publishStatus === 200`, and a byte-exact tree. 420 s timeout. |
| `meta-loss.spec.ts` | `backend killed mid-scan of many small files → full tree still published @chromium-only` | `/e2e.html` | Regression for a real bug where lost NODE frames produced `COMMIT_OK` for 16 of 65 files. Uses a local `manySmallFiles(600)` generator (paths `d${i%20}/s${i%5}/f-${i}.bin`, sizes `10_000 + (i*37)%8000`, ≈8.4 MB spanning several 2 MiB batch POSTs), kills the backend once `bodyDoneBytes > 100_000` so metadata is guaranteed in flight, and asserts `committed.files === 600`, `committed.bytes === totalBytes`, `publishStatus === 200`, and a byte-exact tree. Covers batch-POST transport retry at the same seq, NODE-chain re-send on reconnect, `unknown_node` NACKs, and the server's node-count commit invariant. 420 s timeout. |
| `abort.spec.ts` | `cancel mid-upload → aborted immediately, never resurrected @chromium-only` | `/e2e.html` (`mfupE2E.runAndAbort`) | A single 48 MiB file cancelled after 4 MiB. `state` must be `aborted`, `state:aborted` must appear in the log, and **no** `state:active`/`committing`/`committed` entry may follow it — an in-flight reconnect must not resurrect an aborted session, and the upload promise must settle rather than hang awaiting a commit that can never arrive. |
| `retention.spec.ts` | `client abort mid-transfer leaves no disk or Redis trace @chromium-only` | `/e2e.html` | Aborts after 500 KB of a 40-file / 8 MB tree; asserts `state === "aborted"`, then polls up to 15 s for `uploads/.incoming.<sid>` to disappear (cleanup runs in the WS `finally` block) and asserts `ZSCORE mfup:sessions <sid>` is empty and `EXISTS mfup:meta:<sid>` is `0`. Redis is inspected with `docker compose exec -T redis redis-cli`. |
| `retention.spec.ts` | `no orphaned staging dirs accumulate (reconciliation net) @chromium-only` | none (compose only) | Plants a fake orphan **inside** the backend container (`/data/uploads/.incoming.orphan-*` with a 1 KB payload file, aged past the 600 s `ORPHAN_GRACE` by `TZ=UTC touch -d '<explicit UTC timestamp>'` — a GNU relative date like `'20 minutes ago'` is rejected by the busybox `touch` in the alpine-based Node backend, while this format is accepted by both) so it is owned by the same user as the bind mount. Confirms it exists and has no zset entry, restarts the backend, polls `/health` for readiness, then asserts startup reconciliation removed the directory. |
<!-- ⬆️ 351 ⬆️ -->

Playwright runner configuration (`e2e/playwright.config.ts`):

| Setting | Value |
|---------|-------|
| `testDir` | `./tests` |
| `timeout` | `60_000` (chaos/resume specs raise their own via `test.setTimeout`) |
| `retries` | `0` globally |
| `workers` | `1` — sequential, because all tests share one server and one `uploads/` dir |
| `reporter` | `[["list"]]` |
| `use.baseURL` | `process.env.MFUP_BASE_URL ?? "http://localhost:20060"` |
| `use.ignoreHTTPSErrors` | `true` |
| `use.navigationTimeout` / `actionTimeout` | `30_000` each |
| Project `chromium` | `Desktop Chrome`; runs everything including `@chromium-only` |
| Project `firefox` | `Desktop Firefox`; `grepInvert: /@chromium-only/` |
| Project `webkit` | `Desktop Safari`; `grepInvert: /@chromium-only/`; `retries: 1` when `process.env.CI` (absorbs sporadic macOS cold-start slowness) |
<!-- ⬆️ 352 ⬆️ -->

### 9.5 Fixture generator (`e2e/lib/gen.ts`)

Deterministic tree generator plus on-disk verifier. Its header states that `bytesFor()` **must match `demo/src/e2e.ts` byte-for-byte** — the browser generates content with the same algorithm the Node-side verifier re-derives, so no fixture bytes ever cross the wire out of band.
<!-- ⬆️ 353 ⬆️ -->

| Export | Type | Description |
|--------|------|-------------|
| `ManifestEntry` | interface | `{ path: string; size: number }`; `path` is relative and always `/`-separated. |
| `fnv1a(s)` | function | 32-bit FNV-1a over the string's char codes, via `Math.imul`, returned unsigned. |
| `bytesFor(key, size)` | function | Fills a `Uint8Array(size)` from an xorshift32 PRNG (`<<13`, `>>>17`, `<<5`) seeded with `fnv1a(key) \|\| 1`, taking the low byte each step. Content is a pure function of `(key, size)`. |
| `standardManifest(opts?)` | function | The project-shaped tree (see below). |
| `totalBytes(manifest)` | function | Sum of all entry sizes. |
| `writeTreeToDisk(baseDir, rootName, manifest)` | function | `rm -rf`s `baseDir/rootName`, recreates every entry with `bytesFor(rootName + "/" + entry.path, size)`, returns the root path. Used by the `<input webkitdirectory>` specs, which write into `e2e/.tmp`. |
| `verifyTree(uploadedRoot, rootName, manifest, contentSeed?)` | function | Returns an array of problem strings — `MISSING: <path>`, `SIZE: <path> expected N got M`, or `BYTES: <path> first diff at offset N`. Empty array means byte-exact. `contentSeed` defaults to `rootName`; passing a different seed (e.g. `rootName + "#v2"`) is how the conflict matrix proves an overwrite actually replaced bytes. |
<!-- ⬆️ 354 ⬆️ -->

`standardManifest({ bigFileBytes?, smallFiles? })` composition:

| Entries | Path pattern | Size |
|---------|--------------|------|
| `smallFiles` (default 60) | `${dir}/file-${i}.ts`, cycling `dirs = ["src", "src/components", "src/utils", "assets", "assets/img", "deep/a/b/c/d"]` | `512 + ((i * 997) % 7000)` → 512–7511 bytes |
| 1 | `README.md` | 3000 |
| 1 | `empty.txt` | 0 (zero-length file coverage) |
| 1 | `файл — тест ❤.txt` | 2048 (non-ASCII name coverage) |
| 1 | `assets/img/photo.bin` | 300000 |
| 1 | `bundle.bin` | `bigFileBytes` (default 2000000) |
<!-- ⬆️ 355 ⬆️ -->

The default manifest is 65 entries totalling 2,560,458 bytes, and covers nested directories up to five levels deep, a unicode filename, an empty file and one large file in a single tree. Specs override the knobs for their purpose: `{smallFiles: 8, bigFileBytes: 60_000_000}` for resume, `{smallFiles: 30, bigFileBytes: 1_500_000}` for the conflict matrix, `{smallFiles: 40, bigFileBytes: 8_000_000}` for retention, `{smallFiles: 20, bigFileBytes: 200_000}` for the conflict spec.
<!-- ⬆️ 356 ⬆️ -->

### 9.6 E2E stack topology (`e2e/docker-compose.yaml` + `e2e/Caddyfile`)

Six services; only Caddy publishes a host port. Everything the browser touches enters through `localhost:20060`.
<!-- ⬆️ 357 ⬆️ -->

| Service | Image | Ports | Command | Environment | Volumes |
|---------|-------|-------|---------|-------------|---------|
| `redis` | `redis:7-alpine` | `expose 6379` | image default | — | — |
| `backend` | `python:3.12-slim` | `expose 8070` | `pip install --quiet fastapi uvicorn[standard] websockets redis crc32c && python -m uvicorn mfup_fastapi.app:app --host 0.0.0.0 --port 8070 --log-level info` (workdir `/app`, `depends_on: redis`) | `MFUP_BASE_DIR=/data/uploads`, `REDIS_URL=redis://redis:6379/0`, `MFUP_ADMIN_TOKEN=dev-admin-token`, `PYTHONPATH=/app/mfup-core:/app/mfup-fastapi` | `../server:/app`, `../uploads:/data/uploads` |
| `trivial` | `python:3.12-slim` | `expose 8071` | `pip install --quiet fastapi uvicorn[standard] python-multipart && python -m uvicorn app:app --host 0.0.0.0 --port 8071 --log-level info` | `UPLOAD_DIR=/data/uploads/trivial-target` | `../benchmarks/trivial-server:/app`, `../uploads:/data/uploads` |
| `frontend` | `node:20-alpine` | `expose 3000` | `npm ci && npm run dev` | — | `../demo:/app`, `../packages:/packages`, anonymous `/app/node_modules` |
| `frontend-build` | `node:20-alpine` | — | `npm ci && npm run build` | — | `../demo:/app`, `../packages:/packages`, anonymous `/app/node_modules` |
| `caddy` | `caddy:2-alpine` | **`20060:80`** | image default (`depends_on: backend, frontend-build, trivial`) | — | `./Caddyfile:/etc/caddy/Caddyfile:ro`, `../demo/dist:/srv/dist:ro` |
<!-- ⬆️ 358 ⬆️ -->

**Swapping in the Node backend.** `e2e/docker-compose.node.yaml` is an overlay that replaces exactly one service:

```
docker compose -f e2e/docker-compose.yaml -f e2e/docker-compose.node.yaml up -d
```

It swaps `backend` for `node:24-alpine` running `node bin/mfup-server.mjs` out of a `../packages` bind mount, with `MFUP_BASE_DIR=/data/uploads`, `REDIS_URL=redis://redis:6379/0`, `MFUP_ADMIN_TOKEN=dev-admin-token` and `MFUP_PORT=8070`. Redis, Caddy, `trivial`, `frontend-build`, the `uploads` bind mount and port 20060 are inherited unchanged, so the entire Playwright suite — including the chaos specs and the retention spec that inspects Redis directly — runs against the Node server without modification. That works precisely because `RedisStore` uses the same key layout as the Python `SessionIndex` (§5.2). The overlay requires `npm run build -w @mfup/server` on the host first: the container installs only runtime deps (`ws` + `redis`) and runs the prebuilt `dist/`.
<!-- ⬆️ 359 ⬆️ -->

Two deliberate constraints are documented in the file itself. `frontend` sits behind the `dev` profile so a plain `docker compose up` does not start it — otherwise it and `frontend-build` would `npm ci` concurrently into the same bind-mounted `../demo` and clobber each other's `node_modules` (`vite: not found`). And both node services run `npm ci && npm run …` rather than `npx vite`, which would fetch the latest vite from the registry instead of the lockfile's. The anonymous `/app/node_modules` volume keeps installed packages out of the bind-mounted host directory and unshared between containers.
<!-- ⬆️ 360 ⬆️ -->

Caddy's `:80` site block routes in this order:

| Route | Handling |
|-------|----------|
| `/mfup/*` | `reverse_proxy backend:8070` — all protocol traffic, HTTP and the WebSocket upgrade |
| `/health` | `reverse_proxy backend:8070` — the readiness probe used by CI and the retention spec |
| `/trivial/*` | `uri strip_prefix /trivial` then `reverse_proxy trivial:8071` — the benchmark baseline server |
| everything else | `root * /srv/dist`, `try_files {path} /index.html`, `file_server` — the built demo bundle with SPA fallback |
<!-- ⬆️ 361 ⬆️ -->

Request flow: the browser loads `http://localhost:20060/` (or `/e2e.html`, `/react.html`) → Caddy serves static files from `demo/dist`, which `frontend-build` produced into the bind mount and Caddy mounts read-only. Client protocol calls go to `/mfup/*` → Caddy → `backend:8070` (uvicorn running `mfup_fastapi.app:app` directly off the `../server` bind mount via `PYTHONPATH`, no editable install). The backend persists session state in `redis:6379` and writes payloads under `/data/uploads`, which is the host's `uploads/` directory — the same directory `verifyTree` reads from Node. Chaos specs bypass HTTP entirely and manipulate containers with `docker compose kill/start/restart/exec` run from `e2e/`.
<!-- ⬆️ 362 ⬆️ -->

### 9.7 Dev stack (`docker-compose.yaml`)

The root compose file is described in its own header as **the main application of the repo**: the consumer example `examples/multiuser-scopes` running exactly as an outside integrator would run it — server dependencies from PyPI, client dependencies from npm, nothing built from this repo's sources. `docker compose up -d` brings up **both editions**: the Python server on `http://localhost:20061` (uploads to `./uploads/<user_id>/<scope>/…`) and the Node server on `http://localhost:20062` (uploads to `./uploads-node/<user_id>/<scope>/…`). The two run the same React client against the same protocol; only the backend differs, and the Node edition needs no Redis.
<!-- ⬆️ 363 ⬆️ -->

| Service | Image | Ports | Command | Environment | Volumes |
|---------|-------|-------|---------|-------------|---------|
| `redis` | `redis:7-alpine` | `expose 6379` | image default | — | — |
| `server` | `python:3.12-slim` | `expose 8090` | `pip install --quiet -r requirements.txt && python -m uvicorn app:app --host 0.0.0.0 --port 8090 --log-level info` (workdir `/app`, `depends_on: redis`) | `DEMO_DATA_DIR=/data/uploads`, `REDIS_URL=redis://redis:6379/0` | `./examples/multiuser-scopes/server:/app`, `./uploads:/data/uploads` |
| `client` | `node:20-alpine` | **`20061:20061`** | `npm install --no-audit --no-fund && npm run dev` (vite dev server with hot reload; its `/api` proxy targets the server service) | `EXAMPLE_BACKEND_URL=http://server:8090` | `./examples/multiuser-scopes/client:/app`, anonymous `/app/node_modules` |
| `server-node` | `node:24-alpine` | `expose 8091` | `npm install --no-audit --no-fund --no-package-lock && node server.mjs` (express + `@mfup/server` from npm; **no Redis**, memory store) | `DEMO_DATA_DIR=/data/uploads`, `PORT=8091` | `./examples/multiuser-scopes-node/server:/app`, `./uploads-node:/data/uploads`, anonymous `/app/node_modules` |
| `client-node` | `node:20-alpine` | **`20062:20062`** | `npm ci --no-audit --no-fund && npm run dev -- --port 20062` — `npm ci` (read-only lockfile) so the two client containers never fight over the bind-mounted lockfile | `EXAMPLE_BACKEND_URL=http://server-node:8091` | `./examples/multiuser-scopes/client:/app` (**the same client directory as the Python edition**), anonymous `/app/node_modules` |
<!-- ⬆️ 364 ⬆️ -->

Differences from the e2e stack:

| Aspect | Dev stack (root) | E2E stack (`e2e/`) |
|--------|------------------|--------------------|
| Host port | `20061` (Python edition) + `20062` (Node edition), both vite dev servers | `20060` (Caddy) |
| Edge | none — vite dev server proxies `/api` to the backend | Caddy reverse proxy with four route groups |
| Application | `examples/multiuser-scopes` (`app:app`) and `examples/multiuser-scopes-node` (`server.mjs`) | `mfup_fastapi.app:app`, or `@mfup/server` via the `docker-compose.node.yaml` overlay |
| Package source | PyPI `requirements.txt` + npm registry — nothing built from repo sources | `../server` bind mount on `PYTHONPATH`, `../packages` aliased into the demo build |
| Frontend | vite dev server, hot reload | pre-built static `demo/dist` served by Caddy (`frontend-build`), optional `dev`-profile hot-reload service |
| Backend port | 8090 | 8070 |
| Base dir env | `DEMO_DATA_DIR` | `MFUP_BASE_DIR` |
| Extras | — | `trivial` benchmark baseline server, `MFUP_ADMIN_TOKEN=dev-admin-token` |
| Shared | `redis:7-alpine`, `./uploads` bind-mounted to `/data/uploads` | same |
<!-- ⬆️ 365 ⬆️ -->

### 9.8 npm scripts and CI

| Script | Package | Command | Effect |
|--------|---------|---------|--------|
| `build` | root (`mfup-monorepo`) | `npm run build -w @mfup/client && npm run build -w @mfup/react && npm run build -w @mfup/server` | Builds all three npm workspaces (`packages/client`, `packages/react`, `packages/server`) in dependency order. |
| `check` | root | `npm run check -w @mfup/client && npm run check -w @mfup/react && npm run check -w @mfup/server` | Runs each workspace's own check (type-check/lint) task. |
| `test:server` | root | `npm test -w @mfup/server` | Runs the `@mfup/server` vitest suite (§9.3). |
| `test` | `e2e` (`mfup-e2e`) | `playwright test` | Runs the Playwright suite against `MFUP_BASE_URL` (default `http://localhost:20060`). |
<!-- ⬆️ 366 ⬆️ -->

The root package is `private: true` with `workspaces: ["packages/client", "packages/react", "packages/server"]`; `e2e` is `private: true`, `type: "module"`, with `@playwright/test ^1.49.0` as its only devDependency. Python tests have no npm entry point — CI runs `cd server && python -m pytest tests -q`.
<!-- ⬆️ 367 ⬆️ -->

CI (`.github/workflows/e2e.yml`) runs five jobs. `server-unit` installs `./server/mfup-core` and `./server/mfup-fastapi[dev]` and runs pytest. `node-server-unit` installs the workspaces, builds `@mfup/client` (a test dependency of the server suite, which cross-checks the two codecs) and runs `npm test -w @mfup/server`. `packaging` builds and `npm pack`s all three npm workspaces, installs the tarballs into a scratch project and asserts `@mfup/client`, `@mfup/react` and `@mfup/server` all import, then `python -m build` + `twine check` both wheels and asserts `len(MfupEngine(MfupConfig()).router.routes) == 8`. `linux-e2e` is a **`backend: [python, node]` matrix** (`fail-fast: false`): both legs bring up `docker compose -f e2e/docker-compose.yaml`, and the `node` leg additionally layers `-f e2e/docker-compose.node.yaml` after building `@mfup/server` on the host; each leg installs Chromium/Firefox/WebKit, waits up to 120 s for `/health` plus an `/e2e.html` response containing `mfupE2E`, and runs `npx playwright test`, dumping backend logs and uploading artifacts on failure. `macos-webkit` runs the suite against a natively-started backend with the statically-served demo build. Running the unchanged Playwright suite against both servers is what enforces the wire-compatibility contract between them.
<!-- ⬆️ 368 ⬆️ -->

### 9.9 Invariants the suites protect

**Resume.** Killing the backend with SIGKILL mid-upload never loses or duplicates bytes: the client reconnects, RESUMEs, and the published tree matches the source byte-for-byte with `committed.bytes` equal to the manifest total (`resume.spec.ts`). A recovered session reconstructs its `base_dir` from the staging directory's parent, and its quotas, `auth_context` and `client_meta` survive a registry restart (`test_recover_session_derives_base_from_staging_parent`, `test_auth_and_meta_persist_across_recovery`). Replayed NODE frames are idempotent and do not double-count the file quota (`test_duplicate_node_does_not_double_count_file_quota`).
<!-- ⬆️ 369 ⬆️ -->

**Commit ordering.** The commit loop is never entered while a file body is in flight. `finalizeScan()`'s drain condition includes `this.pumping`, not just an empty `fileQueue` — after a `RESUME` the fresh pump shifts the requeued file into `activeFile` before the queue is re-checked, so "queue empty" is not "transfer done". A gracefully restarted backend mid-upload of a single large file is the interleaving that exposes it, and `resume.spec.ts`'s `docker compose restart` regression pins it. The paired client-side leg fence (§6.2) keeps a superseded writer from ever reaching the replacement channel.
<!-- ⬆️ 370 ⬆️ -->

**Metadata loss.** A commit can never report success on an incomplete tree. Killing the backend while metadata for hundreds of files is in flight still yields `committed.files === 600` and a byte-exact tree — the server's node-count invariant, `unknown_node` NACKs, same-seq batch retry and NODE-chain re-send together close the gap (`meta-loss.spec.ts`). Rejected nodes are counted in `dropped_nodes` precisely so this invariant stays balanced (`test_illegal_name_node_dropped_and_rejected`).
<!-- ⬆️ 371 ⬆️ -->

**Conflict resolution.** The overwrite question is non-blocking: the answer may arrive mid-transfer or after commit, and all four combinations behave identically per action (`conflict-matrix.spec.ts`). Overwrite publishes the *new* bytes — verified against the v2 content seed, not merely "a file exists". Cancel leaves the target byte-identical to v1, attempts no publish, drives the session to a terminal local state, and leaves no staging directory behind.
<!-- ⬆️ 372 ⬆️ -->

**Retention / GC.** A session that ends by abort or cancel leaves nothing: no `.incoming.<sid>` directory, no `mfup:sessions` zset member, no `mfup:meta:<sid>` hash (`retention.spec.ts`). Staging that outlives its Redis registration past the 600 s `ORPHAN_GRACE` window is removed by startup reconciliation. Successful publish also reclaims staging, both in-process (`test_per_session_base_dir_staging_and_publish`, `test_mapped_publish_by_type`) and server-side via the `on_committed` `"publish"` verdict.
<!-- ⬆️ 373 ⬆️ -->

**Abort semantics.** Cancel is instant and terminal. The session goes to `aborted` and no subsequent event may move it to `active`, `committing` or `committed`; the upload promise settles rather than waiting for a commit that can never arrive (`abort.spec.ts`).
<!-- ⬆️ 374 ⬆️ -->

**Containment.** No name can escape the payload root: separators, dot-entries, NUL and empty names are rejected at ingest, an unresolvable parent chain raises, resolved paths are asserted `is_relative_to` the payload root, and mapped publish rejects both `..` escapes and destination collisions without moving anything.
<!-- ⬆️ 375 ⬆️ -->

**Failure containment.** A full disk aborts with `storage_full`, quota breaches abort with `quota_exceeded`, oversized chunks are NACKed with `server_policy` while the session stays alive, type collisions reject a node rather than crashing the session, an unsatisfiable commit terminates at `MAX_COMMIT_RETRIES` instead of looping, a duplicate `FILE_OPEN` closes the prior file handle, and an exception from a consumer's `on_committed` hook cannot damage a completed commit.
<!-- ⬆️ 376 ⬆️ -->

---

## 10. Project Structure

```
/
├── packages/                      # npm workspaces (published)
│   ├── client/                    # @mfup/client 0.2.6 — zero runtime deps
│   │   └── src/
│   │       ├── session.ts         # MfupSession: state machine, pump, commit loop (1641 ln)
│   │       ├── protocol.ts        # wire types, encoders, CRC-32C            (466 ln)
│   │       ├── data-channel.ts    # streaming/batch transport, retry, XHR    (422 ln)
│   │       ├── ingestion.ts       # 4 tree-walk adapters → DiscoveredNode    (330 ln)
│   │       ├── errors.ts          # MfupError + 25 codes, 17 factories       (329 ln)
│   │       ├── control.ts         # WebSocket wrapper, 14-event bus          (237 ln)
│   │       ├── progress.ts        # ProgressTracker, blended fraction        (187 ln)
│   │       ├── probe.ts           # duplex:"half" capability probe           (140 ln)
│   │       ├── index.ts           # re-export barrel                          (92 ln)
│   │       └── dnd.ts             # sourceFromDataTransfer / sourceFromInput  (87 ln)
│   ├── react/                     # @mfup/react 0.2.0 — peer: client + react>=18
│   │   └── src/{index,context,useMfupSession,useMfupUpload,useMfupDropzone}.ts
│   └── server/                    # @mfup/server 0.2.0 — Node ≥22.13, deps: ws (redis optional)
│       ├── src/
│       │   ├── session.ts         # LiveSession, FileWriter, SessionRegistry  (1124 ln)
│       │   ├── handler.ts         # universal handle()/upgrade() + routes     (1024 ln)
│       │   ├── engine.ts          # publish, sweeper, reconcileOrphans         (602 ln)
│       │   ├── storage.ts         # SessionDB on node:sqlite, path resolution  (517 ln)
│       │   ├── protocol.ts        # frame codec, FrameReader, js-table CRC-32C (369 ln)
│       │   ├── publish.ts         # rename staging → target, mapped publish    (291 ln)
│       │   ├── store.ts           # SessionStore iface + MemoryStore           (134 ln)
│       │   ├── hooks.ts           # authorize / mapFile / onCommitted          (128 ln)
│       │   ├── store-redis.ts     # RedisStore — Python-compatible key layout  (121 ln)
│       │   └── index.ts / vite.ts / logger.ts
│       ├── bin/mfup-server.mjs    # standalone CLI, MFUP_* env parity          (116 ln)
│       └── test/                  # vitest: protocol, lifecycle, HTTP/WebSocket
│
├── server/                        # PyPI packages
│   ├── mfup-core/mfup_core/       # 0.2.0 — deps: redis>=5, crc32c>=2.7
│   │   ├── session_manager.py     # LiveSession, FileWriter, SessionRegistry
│   │   ├── storage.py             # SessionDB (SQLite), path resolution
│   │   ├── protocol.py            # frame codec, FrameReader, SessionState
│   │   ├── hooks.py               # authorize / map_file / on_committed
│   │   ├── publish.py             # rename staging → target, mapped publish
│   │   ├── redis_index.py         # SessionIndex (expiry zset + meta hash)
│   │   └── __init__.py            # 26 public exports
│   ├── mfup-fastapi/mfup_fastapi/ # 0.2.0 — pins mfup-core==0.2.0
│   │   ├── engine.py              # MfupEngine: routes, lifecycle, sweeper
│   │   ├── config.py              # MfupConfig + from_env()
│   │   ├── app.py / __main__.py   # standalone entry points
│   │   └── __init__.py            # create_app, typed publish errors
│   └── tests/                     # pytest: protocol, lifecycle, FastAPI router
│       ├── test_edge_cases.py     # hardening: names, quotas, ENOSPC, hooks
│       └── test_protocol.py       # CRC vectors + FrameReader
│
├── examples/
│   ├── multiuser-scopes/          # CONSUMER example — published packages only
│   │   ├── server/app.py          # authorize hook → per-user base_dir + scope
│   │   └── client/src/{App.tsx,main.tsx}
│   └── multiuser-scopes-node/     # same app on @mfup/server (express, memory store)
│       └── server/server.mjs      # 2-line wiring; reuses the client above
│
├── demo/                          # internal dev/test playground (Vite 6, 4 pages)
│   ├── src/{demo,compare,e2e}.ts, src/react-demo.tsx
│   ├── {index,compare,e2e,react}.html
│   └── vite.config.ts             # aliases packages by SOURCE path
│
├── e2e/                           # Playwright: 11 tests, chromium/firefox/webkit
│   ├── tests/*.spec.ts            # incl. chaos: resume, meta-loss, abort, retention
│   ├── lib/gen.ts                 # deterministic manifests + byte-exact verifier
│   ├── docker-compose.yaml        # 6 services, Caddy on :20060
│   ├── docker-compose.node.yaml   # overlay: swap the backend for @mfup/server
│   └── Caddyfile
│
├── benchmarks/trivial-server/     # naive POST-per-file baseline (the comparison)
├── docker-compose.yaml            # MAIN app: the consumer example, :20061 (py) + :20062 (node)
├── uploads/                       # shared bind mount — staging + published trees (Python)
├── uploads-node/                  # same, for the Node edition
└── docs/
    ├── FULL.md                    # this document
    ├── EXTENDING.md               # integration contract (hooks, security, topologies)
    ├── CLIENT.md                  # browser SDK reference
    └── MFUP_RU.md                 # original design notes (RU)
```
<!-- ⬆️ 377 ⬆️ -->
