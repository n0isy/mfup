# mfup-fastapi

FastAPI integration for the MFUP/2 resumable multi-file upload engine
([`mfup-core`](https://pypi.org/project/mfup-core/)).

## Embed into your app

```python
from pathlib import Path
from fastapi import FastAPI
from mfup_fastapi import MfupConfig, MfupEngine
from mfup_core import AuthRequest, AuthResult

async def authorize(req: AuthRequest) -> AuthResult | None:
    user = await my_auth(req.headers)          # cookies / Authorization
    if user is None:
        return None                            # → SESSION_ABORT(auth_failed)
    return AuthResult(
        base_dir=f"/srv/homes/{user.id}",      # per-user home
        max_total_bytes=10 * 2**30,            # 10 GiB quota
        context={"user_id": user.id},
    )

engine = MfupEngine(MfupConfig(
    base_dir=Path("/srv/uploads"),
    redis_url="redis://localhost:6379/0",
    authorize=authorize,                       # a callable — or "pkg.mod:func"
))

app = FastAPI(lifespan=engine.lifespan)
app.include_router(engine.router, prefix="/api/uploads")
```

Point the browser SDK (`@mfup/client` on npm) at the same prefix:
`new MfupSession({ serverUrl: "https://host/api/uploads" })`.

## Run standalone

```bash
pip install mfup-fastapi
MFUP_BASE_DIR=/srv/uploads REDIS_URL=redis://localhost:6379/0 \
  python -m mfup_fastapi
```

All `MfupConfig` fields map 1:1 to `MFUP_*` environment variables
(`MfupConfig.from_env()`); hooks are dotted paths there
(`MFUP_AUTHORIZE=myapp.uploads:authorize`).

## Hooks

| Hook | When | Controls |
|---|---|---|
| `authorize` | HELLO, before anything is created | allow/deny, per-user `base_dir`, target mapping, byte/file quotas, `context` |
| `map_file` | publish, per file | final layout (by type/scope/anything) |
| `on_committed` | after COMMIT_OK | notification; return `"publish"` to publish server-side (scan/moderation/billing flows). Or call `engine.publish(session_id)` yourself later. |

Requires Redis (expiry index) and a POSIX filesystem. One worker per engine
instance; resume across workers is handled via lazy recovery from the Redis
index. Full contract: `docs/EXTENDING.md` in the repository.

Docs and source: <https://github.com/n0isy/mfup>
