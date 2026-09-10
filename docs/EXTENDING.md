# Extension API

MFUP/3 exposes equivalent Node and Python integration contracts. Event and result fields use camelCase in both languages; Python methods and constructor options use snake_case. SQL tables, staging filenames and internal coordinators are not application APIs.

## Embedding

Node `createMfup(options)` returns `.engine`, `.handle(req, res)`, `.attach(server)` and `.close()`. `prefix` applies to both HTTP and WebSocket. Python `MfupEngine(MfupConfig(...))` provides `.engine`, `.router`, `.lifespan`, `.startup()` and `.shutdown()`.

```python
from fastapi import FastAPI
from mfup_fastapi import MfupConfig, MfupEngine

async def authorize(request):
    user = await resolve_user(request['headers'])
    if user is None:
        return None
    return dict(targetDir=f"{user.id}/uploads", context=dict(userId=user.id))

mfup = MfupEngine(MfupConfig(base_dir='./data', authorize=authorize, prefix='/api'))
app = FastAPI(lifespan=mfup.lifespan)
app.include_router(mfup.router, prefix=mfup.config.prefix)
```

`create_app(config)` supplies a complete FastAPI app and applies its prefix. `create_app(base_dir, authorize, **options)` and `MfupEngine(base_dir, authorize, **options)` are also supported. Run lifecycle once per engine. The application supplies `resolve_user` in the example above.

## Authorization and application variables

`authorize(AuthRequest)` is required and may be synchronous or asynchronous. It runs before session persistence and staging creation. Returning null/None denies creation. Exceptions are logged and returned as `403 denied`.

| AuthRequest field | Meaning                                                                       |
| ----------------- | ----------------------------------------------------------------------------- |
| sessionId         | Server-assigned session identifier                                            |
| headers           | Request headers, including application cookies                                |
| client, query     | Peer address and creation query; supplied by the caller for direct core calls |
| targetDir         | Requested relative destination, default uploads                               |
| meta              | Arbitrary client JSON, default null                                           |

| AuthResult field           | Meaning                                              |
| -------------------------- | ---------------------------------------------------- |
| baseDir                    | Absolute session root; defaults to the engine root   |
| targetDir                  | Authorized relative destination                      |
| context                    | Application JSON object, retained only on the server |
| maxFiles, maxTotalBytes    | Per-session quotas                                   |
| autoPublish, clientPublish | Per-session publication policy                       |

Additional application variables belong in `meta`. The engine passes their values and structure as JSON to authorize, `onCommitted` and `mapFile`. A `scope`, `albumId` or `projectId` field has no special meaning to the protocol. Applications validate the fields they use and can return derived values in context. Meta is not automatically flattened into hook fields or merged with context or configuration.

```ts
const session = new MfupSession({
  serverUrl: location.origin,
  meta: {
    projectId: "project-7",
    albumId: "summer",
    options: { original: true },
  },
});
// The application validates projectId/albumId in authorize.
// A configured mapFile can use request.meta and request.context.
```

Results, meta and context are persisted; resume does not call authorize again. Default UTF-8 JSON size limits are maxMetaBytes=16384 and maxContextBytes=65536. Python uses max_meta_bytes/max_context_bytes. Each map callback receives the saved values. Application modifications to a callback argument do not update the session.

Per-session staging and published files use the chosen baseDir; SQLite stays in the engine root. `publishedDirectory(baseDir, targetDir)` / `published_directory(base_dir, target_dir)` returns the final directory. One process owns the database and all roots assigned through it.

The [consumer examples](../examples/README.md) implement anonymous cookies and three application zones through this contract. `scopeRoots` / `scope_roots` is example configuration. The core and adapters do not expose a separate scope registry or endpoint.

## Public types and methods

Node exports AuthRequest, AuthResult, FileMapRequest, CommitEvent, StagedFile, Options and Limits. Python exports the event/result TypedDicts and includes `py.typed`. ProtocolError with code/status is public.

CommitEvent fields: sessionId, targetDir, baseDir, stagingDir, files, bytes, meta, context. FileMapRequest fields: sessionId, path, name, size, targetDir, meta, context. StagedFile fields: path, size, mtime, localPath.

| Node engine                                    | Python engine                  | Purpose                                                      |
| ---------------------------------------------- | ------------------------------ | ------------------------------------------------------------ |
| getSession(id)                                 | get_session(id)                | Server session information, metadata, context and processing |
| listStaged(id)                                 | list_staged(id)                | Iterate staged file metadata in pages of 256                 |
| openStaged(id, path)                           | open_staged(id, path)          | ReadStream / binary file for accepted contents               |
| preparePublish(id)                             | prepare_publish(id)            | Validate and save the destination plan without moving files  |
| publish(id)                                    | publish(id)                    | Trusted backend publication                                  |
| retryCommitted(id)                             | retry_committed(id)            | Explicitly retry failed/interrupted processing               |
| setProperties(id, properties)                  | set_properties(id, properties) | Update session overwrite permission                          |
| snapshot, resume, answer, cancel, sweep, close | same names                     | Session state and lifecycle                                  |

Staged access is available after commit, including from processing/mapping hooks. Close streams/files before publication. Staging does not mirror the source directory tree; use openStaged or localPath.

## Processing and publication policy

`onCommitted(CommitEvent)` / `on_committed` runs outside the session lock. It can read staged files and call backend publish. Its boolean result selects automatic server action:

- true: attempt publication;
- false: do not start automatic publication;
- undefined/None: use the session's server autoPublish policy.

| Setting                               | Default | Behavior                                               |
| ------------------------------------- | ------- | ------------------------------------------------------ |
| SDK/React autoPublish                 | true    | Call publish automatically after commit/approval       |
| Server autoPublish / auto_publish     | false   | Attempt publication after successful server processing |
| Server clientPublish / client_publish | true    | Permit HTTP publish requests from the session owner    |

SDK autoPublish=false still allows explicit `session.publish()`. A false hook result does not disable client publication; set clientPublish=false for backend-only decisions. HTTP publish then returns 403 server_publish_only; trusted engine.publish remains available. For deferred backend processing use clientPublish=false and autoPublish=false, then explicitly publish when ready.

Processing status is persisted as none/pending/running/done/failed. A hook failure keeps commit accepted and returns processing=failed. Repeated commit does not rerun a completed hook. retryCommitted retries unfinished/failed processing, coalesces concurrent calls and does not repeat successful processing. Interrupted running status becomes failed on startup. External side effects require application idempotency keyed by sessionId; durable exactly-once execution is not provided.

Client publication is blocked with 409 processing_required while processing is unfinished or failed. Server autoPublish can resume after overwrite approval; a deferred backend with autoPublish=false explicitly calls publish again.

The synchronous onError/on_error callback receives hook, sessionId and error. Without it, diagnostics use the server logger. A logger failure does not change the upload result. Raw exception details are not sent in client snapshots.

Cancellation prevents late publication by callbacks. Staging cleanup waits for callbacks; sweep skips active callbacks and planning. Close waits for callbacks too, so the application owns their timeouts.

## File mapping

`mapFile(FileMapRequest)` / `map_file` runs after commit, while file contents are accessible. Return a relative destination within targetDir, or null/None to keep the input path.

```ts
mapFile: async ({ path, name, meta, context }) => {
  const category = await lookupCategory(meta, context, path);
  return category ? `${category}/${name}` : null;
};
```

The application supplies lookupCategory. The hook does not run during reception or range retries. Concurrent plan requests are coalesced. Each file is mapped once per planning attempt; all destination paths and collisions are validated before any moves. A failed hook or plan returns 409 mapping_error and retains staging. An explicit retry may compute the plan again.

A successful complete plan is stored in one SQLite transaction and reused after approval, retry and restart. Do not recursively call preparePublish/publish for the same session from mapFile. With a mapping hook, explicit empty directories are not published; without it, input paths and empty directories are retained.

Publication uses same-filesystem moves. Each file rename is atomic; an entire tree is not one transaction. Partial publication does not retain rollback copies of replaced files. Keep application writes away from destinations of unfinished publication.

## Client and React

`MfupSession` exposes connect, upload, pause, resume, setOverwrite, answer, publish, cancel, exportTicket, getSnapshot, subscribe and dispose. Sources include File arrays, FileList, handles, entries and AsyncIterable. Call sourceFromDataTransfer synchronously in a drop handler.

React `useMfupUpload(options)` exposes start, session, snapshot, pendingAsks, answer, setOverwrite, pause, resume and cancel. It subscribes through useSyncExternalStore. `useMfupSession(session)` subscribes to an existing session.

Render at most one overwrite prompt and one operational error per upload. `setOverwrite(true)` persists approval for the whole session. Cancellation remains cancelling until confirmed; a late cancellation may return published. `errorInfo` carries code/status/phase/retryable. `trackUploadProgress` enables XHR sentBytes estimates alongside confirmedBytes. See [PROTOCOL.md](PROTOCOL.md) for states, counters, deadlines and retry behavior.

## Standalone configuration

Node configFromEnv loads hooks such as `./hooks.mjs#authorize` or `package#export`; without a fragment it uses the default export. Relative paths are resolved from cwd. Run `mfup-server` from the Node installation.

Python MfupConfig.from_env accepts `module:attribute` hook names. Run `python -m mfup_fastapi` or the Python installation's `mfup-server`. Hook import failures and missing authorize stop startup. The two executables have the same name in their respective environments.

| Environment variable                        | Default                                     |
| ------------------------------------------- | ------------------------------------------- |
| MFUP_BASE_DIR                               | ./data                                      |
| MFUP_AUTHORIZE                              | Required                                    |
| MFUP_MAP_FILE, MFUP_ON_COMMITTED            | Unset                                       |
| MFUP_PREFIX                                 | Empty                                       |
| MFUP_AUTO_PUBLISH, MFUP_CLIENT_PUBLISH      | false, true                                 |
| MFUP_TTL_MS, MFUP_SWEEP_INTERVAL_MS         | 86400000, 60000; sweep=0 disables the timer |
| MFUP_MAX_META_BYTES, MFUP_MAX_CONTEXT_BYTES | 16384, 65536                                |
| MFUP_CONCURRENCY, MFUP_MAX_PARTS            | 6, 128                                      |
| MFUP_BATCH_BYTES, MFUP_PART_BYTES           | 33554432, 16777216                          |
| MFUP_HOST, MFUP_PORT                        | 127.0.0.1, 3001                             |

Hook imports and absolute roots are server configuration, not values selected from browser metadata. Unknown JSON fields are tolerated; applications should have a fallback for unknown error codes. [Performance](PERFORMANCE.md) describes metadata, mapping and payload costs.

[Russian](ru/EXTENDING.md)
