# Integration examples

The Node and FastAPI examples use public MFUP/3 packages and share a React client. They implement application policy through authorize and mapFile/map_file.

| Example                                                  | Backend                      |
| -------------------------------------------------------- | ---------------------------- |
| [multiuser-scopes-node](multiuser-scopes-node/README.md) | Node HTTP/WebSocket adapter  |
| [multiuser-scopes](multiuser-scopes/README.md)           | FastAPI router and lifecycle |

From the repository root, `docker compose up -d` serves Node at http://localhost:20060 and Python at http://localhost:20060/python/.

For local development run `npm ci --no-audit --no-fund`, `npm run build`, then `npm run dev`. The UI is at http://localhost:3000 and the Node backend is at 127.0.0.1:3001. For Python install `./server/mfup-core` and `./server/mfup-fastapi` and set `MFUP_BACKEND=python`. PowerShell uses `$env:MFUP_BACKEND = 'python'`. PYTHON_BIN selects the interpreter, PORT the UI port, EXAMPLE_API_PORT the backend port and MFUP_EXAMPLE_DATA the storage directory.

## Application policy

The browser receives an anonymous HttpOnly, SameSite=Lax cookie with a random token. A derived user ID is displayed. Each user has workspace, scratch and uploads zones. These are example values in meta.scope, not protocol fields. Authorize validates identity and zone, sets `targetDir=<uid>/<scope>`, quotas of 20,000 files and 512 MiB, and server context. The browser's requested targetDir does not override this layout.

`scopeRoots` (Node) / `scope_roots` (Python) selects an absolute root for a known application zone. Uploads, listings and downloads share the same rule and use public publishedDirectory/published_directory. Output is `<baseDir>/published/<uid>/<scope>/<mapped path>`. Default Compose roots are data/example-node and data/example-python. There is one process per metadata directory and no Redis dependency.

Application endpoints are `/api/whoami`, `/api/files/{scope}?path=...` and `/api/file/{scope}?path=...`. Identity and zone are checked for reads as well as uploads. Deleting the cookie loses access through that anonymous identity; account recovery is outside the example.

## Upload UI

Each zone supports files, folders, drag/drop, pause/resume, one session-wide overwrite approval or cancellation, and a single operational error message. Tickets are stored per backend/user/zone. After reload, select the same source again. The XHR byte bar shows sent and confirmed payload, capped at 99% until publication. Files can keep transferring while approval is pending.

For backend processing, pass `clientPublish: false, onCommitted: handler` to the Node factory, or `client_publish=False, on_committed=handler` to Python. The handler can read listStaged/openStaged and return true after processing. Metadata and server context arrive through CommitEvent. See [extension API](../docs/EXTENDING.md) and [protocol](../docs/PROTOCOL.md).

`npm run test:examples` exercises the UI. Unit/HTTP tests cover ownership, listing/download, per-session roots, mapping and restart. Installed-package checks copy these server examples into an independent project and execute them using built distributions.

[Russian](README_ru.md)
