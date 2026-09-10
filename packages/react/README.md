# @mfup/react

React 18+ integration for `@mfup/client`.

```tsx
import { useMfupUpload } from "@mfup/react";
const { start, snapshot, setOverwrite, pause, resume, cancel } = useMfupUpload({
  serverUrl: location.origin,
  meta: { scope: "workspace" },
  trackUploadProgress: true,
});
```

Use `useMfupSession(session)` for an externally owned session. Render a single
optional `snapshot.overwriteRequired` prompt and call `setOverwrite(true)`
or `cancel()`. Render one error from `snapshot.errorInfo`; no per-file dialogs. The hook disposes its client
on unmount; it does not delete the server-side upload. Keep the ticket in the
application if uploads should survive navigation or reload.

Render `snapshot.sentBytes` for in-flight progress and `snapshot.confirmedBytes`
for server acknowledgements. A completed bar should depend on `state === 'published'`.
Use the client package's `sourceFromInput` / `sourceFromDataTransfer` adapters
in picker/drop handlers. Call the drop adapter synchronously inside the event.

[Extension API](https://github.com/n0isy/mfup/blob/main/docs/EXTENDING.md) · [HTTP protocol](https://github.com/n0isy/mfup/blob/main/docs/PROTOCOL.md)

[Russian](README_ru.md)
