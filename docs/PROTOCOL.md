# MFUP/3 HTTP protocol

Files use native `multipart/form-data`. Session control uses JSON HTTP. WebSocket supplies state notifications. All paths below are relative to the configured adapter prefix.

## Endpoints

| Method and path                         | Input                                                           | Result                                            |
| --------------------------------------- | --------------------------------------------------------------- | ------------------------------------------------- |
| GET `/mfup/health`                      | None                                                            | `protocol`, `backend`                             |
| POST `/mfup/sessions`                   | `protocol: "MFUP/3"`, optional `targetDir`, `meta`, `overwrite` | `id`, `token`, `epoch`, `limits`                  |
| GET `/mfup/sessions/:id`                | Bearer token                                                    | Remote snapshot                                   |
| POST `.../:id/resume`                   | `{}`                                                            | Advanced epoch, snapshot and limits               |
| GET `.../:id/files?after=...&limit=256` | Path cursor                                                     | Files with received offsets and `next` cursor     |
| POST `.../:id/batches/:batchId`         | Multipart; `X-MFUP-Epoch`                                       | Receipt: `id`, `parts`, `bytes`, `confirmedBytes` |
| GET `.../:id/batches/:batchId`          | None                                                            | Saved receipt or 404                              |
| POST `.../:id/status`                   | `epoch`, `files: FilePart[]` (at most maxParts)                 | `received: boolean[]`, `confirmedBytes`           |
| GET `.../:id/published?after=...`       | Source-path cursor                                              | At most 256 destination `files`, `next`           |
| POST `.../:id/commit`                   | `files`, `dirs`, `bytes`                                        | Committed or published snapshot                   |
| POST `.../:id/properties`               | `overwrite: boolean`                                            | Snapshot                                          |
| POST `.../:id/answers`                  | `id: "overwrite"`, `choice: "overwrite"` or `"cancel"`          | Snapshot                                          |
| POST `.../:id/publish`                  | `{}`                                                            | Snapshot                                          |
| POST `.../:id/cancel`                   | `{}`                                                            | Cancelled or already published snapshot           |

Session-ID endpoints require `Authorization: Bearer <token>`. Application authorization runs on creation. `targetDir` defaults to `uploads`; `meta` defaults to null; `overwrite` defaults to false. Arbitrary application variables belong in `meta`, including a field named `scope`. Their JSON values and nesting are preserved for authorization, processing and mapping. They do not become protocol settings.

Authorization results, destination, metadata and context are persisted. Resume uses the existing session and does not repeat authorization or replace metadata. The default UTF-8 JSON limits are 16 KiB for meta, 64 KiB for server context and 256 KiB for an HTTP control body. Context and absolute storage roots are not sent to the browser.

`POST status` checks only the current basket ranges; the SDK never downloads the entire file index. Snapshots and receipts carry a global `confirmedBytes` counter. `published` contains the first page (at most 256 paths), `publishedCount` the total and `publishedNext` the cursor for GET published. Applications consume result pages one at a time. The default file quota is 1000000.

## Multipart basket

The first part is a text field named `manifest`:

```json
{
  "files": [
    ["photos/a.jpg", 1000, 1750000000000, 0, 1000],
    ["photos/b.jpg", 30000000, 1750000000000, 16777216, 13222784]
  ],
  "dirs": ["photos", "empty-directory"]
}
```

Each file tuple is `[path, fullSize, mtimeMs, offset, length]`. File parts follow in tuple order with names `0`, `1`, etc. The multipart filename does not choose the destination. The browser sets the boundary; callers must not supply a Content-Type header without its boundary.

```js
const body = new FormData();
body.append("manifest", JSON.stringify(manifest));
body.append("0", fileA, fileA.name);
body.append("1", fileB.slice(16777216), fileB.name);
```

The manifest is limited to 256 KiB. File parts plus directory entries count toward `maxParts`. Defaults are concurrency 6, maxParts 128, batchBytes 33554432 and partBytes 16777216. An offset is a multiple of partBytes; length is the smaller of partBytes and remaining file bytes. An empty file has one zero-length range at offset zero. Concurrent ranges must not overlap.

The server validates ordering, sizes and the closing boundary, writes payload and commits range metadata before returning a receipt. A batch ID identifies an immutable operation. Reusing it with a different manifest returns 409. A saved receipt does not replace already accepted bytes. After losing a response, check the receipt before resending data.

Protocol counts and offsets are nonnegative safe integers. File paths are relative, use `/`, are normalized to NFC, and are limited to 1024 UTF-8 bytes with 240 bytes per segment. Names and prefixes differing only in case cannot coexist in a session. These path rules apply to file destinations, not to arbitrary strings inside meta. Total file/directory nodes are bounded by `4 * maxFiles + 1024`.

## State and notifications

Server state is `uploading -> committed -> publishing -> published`, or `cancelled`. Commit requires all ranges and matching file/directory/byte totals. Repeating a successful commit returns the current state without repeating application processing.

A remote snapshot contains `id`, `epoch`, `state`, `files`, `bytes`, `asks`, `published`, `overwrite`, `overwriteRequired`, `error`, `clientPublish` and `processing`. Processing is `none`, `pending`, `running`, `done` or `failed`. A failed processing hook leaves accepted files committed and reports `processing: "failed"`; it does not turn an accepted commit into a failed upload.

The client opens `/mfup/control` and sends `{ "type": "subscribe", "id": "...", "token": "..." }` within five seconds. The server sends complete snapshots. The SDK reconnects the control socket after an interruption; HTTP GET also retrieves current state. WebSocket does not carry file data or state-changing answers.

Resume drains admitted data requests and advances the epoch. New data requests must carry that epoch. The server enforces the concurrency limit independently of the client scheduler. File listing pages contain at most 256 files; a large file can have a long offsets list. Metadata space is proportional to files plus ranges.

Tickets authorize access to one session. After a browser reload, the user selects the source again; matching uses relative path, size and mtime. Accepted ranges are not resent. Pausing aborts active client requests; it is not a server terminal state. Control HTTP has a default 30-second deadline (`requestTimeoutMs`); it does not impose that deadline on long data POSTs.

The default `retries: 1000` permits 1000 additional attempts per basket (1001 including the initial attempt). `retryDelayMs` defaults to 1000 and `retryMaxDelayMs` to 36000. Delays are 1, 2, 4, 8, 16, 32, then 36 seconds: 1000 delays total 9 h 57 min 27 s, plus request time. This is an approximate ten-hour recovery dialogue, not a ten-hour expiry deadline. Retry options are nonnegative safe integers; zero retries still permits checking a lost receipt. The delay exponent is bounded for large budgets. Control requests used for connect, resume, commit and publication use the same retry policy. Pause and cancellation interrupt data backoff immediately. A lost creation response can leave an empty session until TTL because session creation has no client idempotency key.

Eligible failures are network errors, timeouts, HTTP 429/500/502/503/504 and 409 busy/range_busy/stale_epoch; `retryable: false` stops automatic retries. Each failed data attempt checks its receipt. An offline or timed-out receipt probe consumes that attempt, never bypasses the remaining budget. A confirmed basket is not resent. Unconfirmed ranges can be resent from their range boundary; a partly accepted basket has no receipt. Defaults are 16 MiB ranges and up to 32 MiB per basket, not a restart of every large file.

The SDK owns the source iterator and at most 10000 pending work records, including active baskets. Enumeration stops at that high-water mark and resumes at 5000, and also stops during retry backoff or a held error. `maxReady` can lower the bound (1–10000); its low-water mark is half that value. Confirmed File/entry references are released. Consumers must pass the source directly and must not collect or retain a duplicate entries/File list. After exhaustion or an operational error, `upload()` rejects but the SDK retains the bounded pending stream. Call `session.retry()` (React `retry()`) without a source to continue after the cause is resolved. Cancel/dispose release the retained transfer. A page reload destroys in-memory access; only that case requires source selection again.

Server retention defaults to 24 hours, renewed by session operations; it exceeds the default recovery delay budget. Published files remain in their application directory. A custom shorter TTL can expire an offline session; retries do not recreate an expired session or prove an earlier publication result.

## Overwrite and cancellation

Overwrite is one persisted, monotonic session permission. The first destination conflict sets `overwriteRequired: true`; data reception can continue. The application presents one optional prompt for the upload. `POST properties {"overwrite":true}` grants approval for all current and subsequent conflicts. Approval is stored before acknowledgment and survives resume/restart. Repeating true is idempotent; changing true back to false returns `409 overwrite_already_approved`.

`asks` is empty or contains one item:

```json
{
  "id": "overwrite",
  "message": "overwrite_required",
  "choices": ["overwrite", "cancel"],
  "answer": null
}
```

After approval, cancellation or publication, no question remains. There is no per-file skip. `answers` is an alternate endpoint for the same session-wide actions.

| Decision | During upload                                                                                        | After commit, before publication                       |
| -------- | ---------------------------------------------------------------------------------------------------- | ------------------------------------------------------ |
| Approve  | Persist permission; continue reception without more prompts; existing files remain until publication | Publication proceeds according to client/server policy |
| Cancel   | Persist cancelled, stop requests and prevent late receipts/publication                               | Remove staging; leave existing destinations unchanged  |

Cancellation is idempotent and serialized with publication. Once publication has completed, cancel returns published. Cancellation during a callback prevents its late completion from publishing; staging cleanup waits for callbacks. Cleanup failure does not reverse cancellation and can be retried by the sweeper. Files already replaced by a partially completed publication are not restored.

The SDK displays `cancelling` until cancellation is confirmed by a response or snapshot. If confirmation cannot be obtained, it reports that cancellation is unconfirmed and allows retry. Late responses do not demote an already confirmed cancelled/published state.

## Publication and durability

SDK autoPublish defaults to true, server autoPublish to false, and server clientPublish to true. `clientPublish: false` returns `403 server_publish_only` for HTTP publish; trusted backend publication remains available. Unfinished or failed processing blocks client publication with `409 processing_required`. Server autoPublish may publish during commit or after overwrite approval. Approval never bypasses commit completeness.

Mapping runs when a file first arrives in a basket manifest, before its body is consumed. The hook receives metadata only; content validation belongs in onCommitted using staged streams. Destination checks raise the first overwrite prompt as soon as a mapped destination is known, even while later metadata or bodies are pending. Validated mappings are saved with each accepted manifest and reused for later ranges and resume/restart. Publication iterates this persisted plan in bounded pages. A file rename is atomic; publishing a tree is not one transaction and does not retain rollback copies. Applications must not change destinations during unfinished publication.

SQLite/WAL and session roots have one process owner. partBytes is fixed for the metadata directory. Receipts survive process restart; payload fsync for power-loss guarantees is not performed. See [extension contracts](EXTENDING.md) for application processing and storage layout.

## Errors

Operational responses include `error`, `message`, `retryable` and `phase`:

```json
{
  "error": "storage_full",
  "message": "Server storage is full",
  "retryable": false,
  "phase": "upload"
}
```

Use the error code, not message text, for application behavior. Messages may differ between adapters. Raw exceptions and absolute paths stay in server diagnostics.

| HTTP | Representative codes                                                                                                                                |
| ---- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| 400  | bad_manifest, bad_range, bad_path, bad_property, part_size_mismatch, incomplete_multipart                                                           |
| 403  | denied, server_publish_only                                                                                                                         |
| 404  | not_found, unknown_question                                                                                                                         |
| 408  | request_timeout                                                                                                                                     |
| 409  | stale_epoch, file_changed, batch_conflict, range_busy, answers_required, incomplete, mapping_error, processing_required, overwrite_already_approved |
| 413  | batch_too_large, body_too_large, quota_exceeded, meta_too_large                                                                                     |
| 429  | busy                                                                                                                                                |
| 500  | server_error                                                                                                                                        |
| 503  | storage_unavailable                                                                                                                                 |
| 507  | storage_full                                                                                                                                        |

ENOSPC, EDQUOT and SQLite FULL map to storage_full. Storage read/write/open failures such as EIO, EROFS and EACCES map to storage_unavailable. They are not reported as malformed multipart. No receipt is issued for a failed basket; request slots are released even if a close/flush fails.

The first operational failure is recorded in `snapshot.error` as `{code,status,phase,retryable}` and exposed by the SDK as `errorInfo`. If SQLite cannot store it, available process state is used; a failed database cannot provide persistence guarantees. Resume or explicit publication clears the previous failure when storage is available. `retryable: false` prevents automatic body retransmission; an explicit retry is possible after the cause is resolved. The UI presents one error for the upload rather than errors from every interrupted request.

## Client byte counters

`confirmedBytes` counts receipt-backed payload. With `trackUploadProgress: true`, XHR upload events estimate active payload for `sentBytes`; notifications are coalesced every 50 ms. A custom fetch takes precedence and does not supply XHR events. Multipart headers and boundaries mean sentBytes is an estimate, not a network-interface byte counter or storage acknowledgment. Aborted estimates are removed; the display can move backwards. The demo shows at most 99% until published, including when all bytes have been sent but approval is pending.

Snapshot confirmation can precede an HTTP receipt. The SDK tracks locally acknowledged bytes separately so `sentBytes` counts overlapping active payload only once. Late receipts from an earlier resume epoch do not add that payload again.

[Russian](ru/PROTOCOL.md)
