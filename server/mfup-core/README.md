# mfup-core

The MFUP/2 resumable multi-file upload **engine** — framework-free.

MFUP/2 moves whole directory trees (think `node_modules`-scale: tens of
thousands of small files) from a browser to a server over one WebSocket
control channel (JSON) plus HTTP data legs (binary frames), with:

- **resume** across page reloads, network drops and server restarts
  (per-session SQLite journal in a staging directory, epoch/leg fencing);
- **interactive transfers** — the server can ASK the user mid-flight
  (overwrite? cancel?) without stopping the stream;
- **integrity** — CRC-32C per chunk (C-accelerated, hard dependency),
  commit invariants that catch lost metadata;
- **atomic publish** — staged files move into the target directory with
  `rename()`, optionally re-laid-out per file by a consumer hook;
- **retention** — Redis expiry index plus a filesystem reconciliation
  safety net; failed/cancelled sessions leave no garbage.

This package contains the protocol codec, the session state machine,
storage/publish, the Redis index, and the consumer hook contracts
(`AuthRequest/AuthResult`, `FileMapRequest`, `CommitEvent`). It does **not**
speak HTTP: pair it with [`mfup-fastapi`](https://pypi.org/project/mfup-fastapi/)
(or write your own transport shell against these primitives).

The browser side lives on npm: `@mfup/client` (TypeScript SDK) and
`@mfup/react` (hooks).

Docs and source: <https://github.com/n0isy/mfup> — see `docs/EXTENDING.md`
for the integration contract and `docs/FULL.md` for the protocol.
