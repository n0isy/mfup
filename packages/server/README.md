# @mfup/server

Node 22.13+ MFUP/3 server: streamed multipart, durable receipts, user questions,
per-session roots and application hooks. One process owns the metadata directory.

```ts
import { createServer } from "node:http";
import { createMfup } from "@mfup/server";
const mfup = createMfup({
  baseDir: "./state",
  prefix: "/api",
  authorize: async ({ headers, meta }) => {
    const user = await resolveUser(headers); // application
    const scope = resolveScope(user, meta); // application
    return scope
      ? {
          baseDir: scope.root,
          targetDir: `${user.id}/${scope.name}`,
          context: { uid: user.id, scope: scope.name },
        }
      : null;
  },
});
const server = createServer(async (req, res) => {
  if (!(await mfup.handle(req, res))) {
    res.writeHead(404);
    res.end();
  }
});
mfup.attach(server);
server.listen(3000);
// Shutdown: await mfup.close(); server.close();
```

The browser uses `serverUrl: location.origin + '/api'`. Scopes and identity
belong to the application. `publishedDirectory(baseDir,targetDir)` resolves
the corresponding directory for application listing/download handlers.

`mapFile` runs after commit, returns a relative path or null, and persists a
validated plan before moves. `onCommitted` receives roots, counts, meta/context;
`engine.listStaged` and `openStaged` expose accepted files for processing.
A boolean callback result overrides server `autoPublish` (default false).
`clientPublish: false` reserves publication for `engine.publish(id)` on the
backend. The SDK's own autoPublish controls its automatic request only.
Errors in onCommitted leave commit successful with `processing: 'failed'`;
`engine.retryCommitted(id)` explicitly retries the processing step.

Exports include AuthRequest, AuthResult, FileMapRequest, CommitEvent, StagedFile,
Options, ProtocolError (code/status), createMfup, Engine and configFromEnv.
Standalone: `MFUP_AUTHORIZE='./hooks.mjs#authorize' mfup-server`.
See the repository's `docs/EXTENDING.md` and `examples/` for the complete contract.

[Extension API](https://github.com/n0isy/mfup/blob/main/docs/EXTENDING.md) · [HTTP protocol](https://github.com/n0isy/mfup/blob/main/docs/PROTOCOL.md)

[Russian](README_ru.md)
