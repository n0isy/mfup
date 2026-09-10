# @mfup/client

Native browser FormData uploads, up to six concurrent baskets, incremental
directory discovery, interactive questions and resumable file ranges.

The SDK owns the stream: at most 10000 pending records including active baskets, with enumeration resuming at 5000. Do not collect or retain a duplicate entries/File list. The default 1000 retries with backoff capped at 36 seconds provide about ten hours of waiting. After exhaustion, call session.retry() without a source; the SDK retains its bounded queue. sourceFromInput clears the input after enumeration completes.

```ts
import { MfupSession, sourceFromInput } from "@mfup/client";
const session = new MfupSession({
  serverUrl: location.origin,
  trackUploadProgress: true,
});
session.subscribe(() => console.log(session.getSnapshot()));
await session.upload(sourceFromInput(input));
session.dispose();
```

An upload has one optional overwrite approval: render `snapshot.overwriteRequired`
and call `session.setOverwrite(true)` or `session.cancel()`. Approval persists
for the entire session, including future conflicts and resume. There are no
per-file decisions or skip action. `overwrite: true` can provide initial consent.

Save `session.exportTicket()` after connect; reselect source files after reload.
Use `autoPublish: false` for explicit publication. Cancellation remains
`cancelling` until confirmed by the server; a late cancel can return `published`.
File content stays in native File/Blob objects.

Render one error per upload from `snapshot.errorInfo` (code/status/phase/retryable).
Storage failures use storage_full (507) or storage_unavailable (503) and do not
automatically resend the body. Network failures first check the receipt.
Control HTTP has a 30-second deadline (`requestTimeoutMs`); long data POSTs do not.

`trackUploadProgress: true` uses native XHR upload events for in-flight progress.
`sentBytes` estimates confirmed payload plus active uploads; `confirmedBytes`
only includes server receipts. Upload event updates are coalesced at 50 ms.
The default transport and an injected `fetch` keep using fetch. On interruption,
unconfirmed estimates can move backwards; resume always uses durable receipts.

[Extension API](https://github.com/n0isy/mfup/blob/main/docs/EXTENDING.md) · [HTTP protocol](https://github.com/n0isy/mfup/blob/main/docs/PROTOCOL.md)

[Russian](README_ru.md)
