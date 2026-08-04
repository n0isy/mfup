# Examples — consuming MFUP/2 from the published packages

Everything here is written **as an outside consumer**: dependencies come
from PyPI (`mfup-fastapi`) and npm (`@mfup/client`, `@mfup/react`), never
from this repo's sources, and the code follows the root README's
integration guide step by step. This directory doubles as the validation
record of that guide (notes below).

| Example | Pattern it proves |
|---|---|
| [`multiuser-scopes/`](multiuser-scopes) | auto-created users (id cookie) × three file zones (`workspace` / `scratch` / `uploads`) → server-owned layout `data/<user_id>/<scope>/…`; router mounted under a prefix inside a bigger FastAPI app; one `useMfupUpload`+`useMfupDropzone` pair per zone |

## Running multiuser-scopes

Terminal 1 — server (needs Redis on localhost:6379):

```bash
cd examples/multiuser-scopes/server
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
uvicorn app:app --port 8090
```

Terminal 2 — client:

```bash
cd examples/multiuser-scopes/client
npm install
npm run dev        # → http://localhost:20061
```

Open the page: a user id is minted via cookie, three drop zones appear;
whatever you drop lands under `server/data/<your_id>/<scope>/` and the
zone's listing refreshes. Open a second browser (or private window) — you
are a different user with different directories.

## Validation notes — walking the root README as a stranger

Done on 2026-08-04 against the actually published `mfup-fastapi 0.2.0`,
`@mfup/client 0.2.0`, `@mfup/react 0.2.0`. What worked, what tripped:

**Worked exactly as documented**
- `pip install mfup-fastapi` → `MfupEngine(MfupConfig(...))` →
  `include_router(engine.router, prefix="/api/mfup")`; `/api/mfup/health`
  answers with `"crc32c":"native"` behind the prefix.
- The whole multiuser/scope policy fit into ONE authorize hook
  (`AuthResult(base_dir=…, target_dir=scope, context=…)`); the client never
  learns the layout — it only sends `meta: { scope }`.
- `npm install @mfup/client @mfup/react` → the entire UI is
  `useMfupUpload` + `useMfupDropzone` per zone; conflict dialogs
  (`pendingAsks` → `ask.respond`) and auto-publish came for free.
- Types from the published `d.ts` files pass `tsc --noEmit` in a strict
  consumer project.

**Tripped / fixed**
1. **Relative `serverUrl` did not work on 0.2.0.** The README's frontend
   snippet suggested `serverUrl: "/api/uploads"`, but the client derived
   the WebSocket URL with a plain `http→ws` replace, which requires an
   absolute URL. Workaround used here:
   `serverUrl: \`${location.origin}/api/mfup\``. Fixed in the client
   (relative URLs now resolve against `location.origin`) — ships in the
   next release.
2. A zone's listing fetch could reject during dev-server restarts and
   surfaced as an unhandled "Failed to fetch" — example code now catches
   it. Not a library issue, but a reminder that example code is read as
   gospel: keep it defensive.

**Paper cuts worth improving later (no blocker)**
- The authorize hook receives raw `headers`; extracting a cookie means
  `SimpleCookie` boilerplate. A parsed `req.cookies` mapping on
  `AuthRequest` would remove ~10 lines from every cookie-auth consumer.
- `uvicorn` comes as a dependency of `mfup-fastapi`, which is convenient
  (`uvicorn app:app` just works) but consumers embedding into an existing
  ASGI stack may be surprised by the extra pin — consider an extra
  (`mfup-fastapi[server]`) someday.
