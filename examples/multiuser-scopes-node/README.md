# Anonymous users and application zones — Node

This example uses public MFUP/3 packages, an anonymous cookie and a shared React client. Identity, zone selection and destination roots are application rules implemented in authorize. Scope is an ordinary meta field.

From the repository root after installing dependencies and building:

```bash
node examples/multiuser-scopes-node/server/server.mjs
```

The backend exposes the MFUP adapter under /api and application routes /api/whoami, /api/files/{scope} and /api/file/{scope}. It validates identity for uploads, listings and downloads. Lifecycle manages session cleanup. See the [example guide](../README.md) for the client, Docker, data layout and configuration.

[Russian](README_ru.md)
