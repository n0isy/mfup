# @mfup/client — events, snapshot, and the UI-binding contract

This is the reference for building UIs (including `@mfup/react`, which is
built ONLY on the surface described here). Two complementary APIs:

- **Typed events** (`session.on(event, fn)`) — raw, fire on every occurrence,
  for logs/telemetry/imperative reactions.
- **Snapshot store** (`session.subscribe(fn)` + `session.getSnapshot()`) —
  an immutable, referentially-stable aggregate for declarative UIs. This is
  the `useSyncExternalStore` contract: subscribe returns an unsubscribe, the
  snapshot object is reused until something changes, and change
  notifications are **coalesced (~33 ms)** for high-frequency progress ticks
  while state-changing events flush immediately. A 50k-file upload does not
  re-render your app 50k times.

## Events (`MfupSessionEvents`)

| Event | Payload | When |
|---|---|---|
| `state` | `SessionState` | every lifecycle transition (`active`, `paused_by_server`, `waiting_resume`, `committing`, `committed`, `aborted`, `failed`) |
| `progress` | `ProgressSnapshot` | every progress mutation (scan units, accepted bytes, file counts, blended monotonic `fraction`) — coalesce in UIs |
| `file:start` | `MfupFileRef` (`nodeId`, `path`, `size`) | a file's body starts streaming |
| `file:ack` | `MfupFileRef & { acceptedBytes, complete }` | the server durably accepted bytes (FILE_ACK); `complete: true` → whole file on the server |
| `file:reject` | `MfupFileRef & { code, reason }` | file permanently skipped (server REJECT_FILE: `illegal_name`, `fs_conflict`, … or client `nack_budget`) |
| `ask` | `MfupAsk` | the server asked an interactive question mid-transfer; answer via `ask.respond("merge_overwrite" \| "cancel")` — the transfer keeps running while the dialog is open |
| `ask:answered` | `MfupAsk` | a pending ask was answered (any code path) |
| `committed` | `{ files, bytes }` | COMMIT_OK — every byte verified and staged |
| `published` | `{ published: string[] }` | `session.publish()` succeeded — files moved into the target |
| `reconnecting` | `{ attempt, delay, maxAttempts }` | before each reconnect attempt (fires before the backoff delay) |
| `abort` | `{ by: "client" \| "server", code, reason }` | terminal teardown |
| `error` | `MfupError` (code, layer, `fatal`, action hint, cause chain) | every error, recoverable ones included |

`MfupAsk`: `{ id, code /* "target_conflict" */, nodeId, name, answered, respond() }`.
`respond()` routes through `sendAction()`, which settles *all* pending asks
(the ACTION is session-wide) — `ask.answered` and the snapshot stay coherent
whichever way the answer was sent. `"cancel"` is terminal for the session.

## Snapshot (`MfupSessionSnapshot`)

```ts
{
  sessionId, state, epoch,
  streaming,            // null until the transport probe ran
  progress, fraction,   // blended monotonic 0–1
  asks, pendingAsks,    // render dialogs from asks.filter(a => !a.answered)
  reconnect,            // { attempt, delay, maxAttempts } | null
  committed,            // { files, bytes } | null
  published,            // string[] | null
  currentFile,          // MfupFileRef | null — what is streaming right now
  recentErrors,         // last ≤20 MfupErrors
  fatalError,           // first fatal error | null — why the session died
}
```

Intentional non-goal: the snapshot carries **no per-file table** — at
node_modules scale that would rebuild a 50k-entry array per tick. Aggregate
counts live in `progress`; per-file detail is event-only (`file:*`).

## The full happy path, imperative form

```ts
const session = new MfupSession({ serverUrl, targetDir, meta });
await session.connect();                    // HELLO → HELLO_OK (server issues resume_token)
await session.upload(source);               // scan + stream + commit; source from
                                            // sourceFromDataTransfer / sourceFromInput / FileList / File[]
if (await session.settleAsks() !== "cancel") // wait out any open dialog; never hangs
  await session.publish();                  // atomic renames; PUBLISH_CONFLICT is retryable
```

`@mfup/react`'s `useMfupUpload().start(source)` is exactly this sequence
with `autoPublish` on by default.

## Resume across page reloads

Persist `session.getResumeState()` (`{ sessionId, resumeToken, epoch }` —
the token is the server-issued one) and construct `MfupSession` with those
values later; `connect()` then RESUMEs instead of HELLOing. Reconnects
within a page's lifetime are automatic and need nothing from you.
