"""MFUP/2 consumer example — multiuser + scopes.

Written strictly against the public packages (pip install mfup-fastapi) and
the root README's integration guide, the way an outside consumer would.

The pattern it demonstrates:
  - users are AUTO-CREATED: first /api/whoami visit mints an id cookie;
  - every user has three file zones ("scopes"): workspace / scratch / uploads;
  - the authorize hook turns (cookie, HELLO.meta.scope) into a per-user home
    and a server-owned target dir, so files land at

        <DATA_DIR>/<user_id>/<scope>/...

    no matter what the client asked for;
  - the MFUP router is mounted under a prefix (/api/mfup) inside a normal
    FastAPI app that also has its own routes (/api/whoami, /api/files).
"""

from __future__ import annotations

import os
import secrets
from http.cookies import SimpleCookie
from pathlib import Path
from typing import Mapping, Optional

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from mfup_core import AuthRequest, AuthResult
from mfup_fastapi import MfupConfig, MfupEngine

DATA_DIR = Path(os.environ.get("DEMO_DATA_DIR", "./data")).resolve()
SCOPES = ("workspace", "scratch", "uploads")
COOKIE_NAME = "demo_uid"


def _uid_from_headers(headers: Mapping[str, str]) -> Optional[str]:
    """Extract and sanity-check the demo user id from the Cookie header.
    Works for both plain HTTP requests and the WebSocket handshake."""
    jar = SimpleCookie()
    jar.load(headers.get("cookie", ""))
    morsel = jar.get(COOKIE_NAME)
    uid = morsel.value if morsel else None
    if uid and uid.isalnum() and 8 <= len(uid) <= 64:
        return uid
    return None


# ---------------------------------------------------------------------------
# The MFUP authorize hook — the whole multiuser/scope policy lives here.
# ---------------------------------------------------------------------------

async def authorize(req: AuthRequest) -> AuthResult | None:
    uid = _uid_from_headers(req.headers)
    if uid is None:
        return None  # no cookie → deny; the SPA calls /api/whoami first

    scope = req.meta.get("scope") if isinstance(req.meta, dict) else None
    if scope not in SCOPES:
        return None  # unknown zone → deny

    return AuthResult(
        base_dir=str(DATA_DIR / uid),   # per-user home (staging lives inside)
        target_dir=scope,               # SERVER owns the layout: <uid>/<scope>/
        max_total_bytes=512 * 2**20,    # 512 MiB per session
        max_files=20_000,
        context={"uid": uid, "scope": scope},
    )


engine = MfupEngine(MfupConfig(
    base_dir=DATA_DIR,
    redis_url=os.environ.get("REDIS_URL", "redis://localhost:6379/0"),
    authorize=authorize,
))

app = FastAPI(title="MFUP example — multiuser scopes", lifespan=engine.lifespan)
app.include_router(engine.router, prefix="/api/mfup")


# ---------------------------------------------------------------------------
# The consumer app's own routes
# ---------------------------------------------------------------------------

@app.get("/api/whoami")
async def whoami(request: Request):
    """Auto-create the demo user: mint an id cookie on first visit."""
    uid = _uid_from_headers(request.headers)
    fresh = uid is None
    if fresh:
        uid = secrets.token_hex(8)
        (DATA_DIR / uid).mkdir(parents=True, exist_ok=True)
    resp = JSONResponse({"user_id": uid, "scopes": list(SCOPES)})
    if fresh:
        resp.set_cookie(
            COOKIE_NAME, uid,
            max_age=30 * 24 * 3600, httponly=True, samesite="lax",
        )
    return resp


@app.get("/api/files/{scope}")
async def list_files(scope: str, request: Request):
    """Top-level listing of one zone — a demo viewer, not a file manager."""
    uid = _uid_from_headers(request.headers)
    if uid is None or scope not in SCOPES:
        return JSONResponse({"error": "unknown user or scope"}, status_code=403)
    root = DATA_DIR / uid / scope
    entries = []
    if root.exists():
        for p in sorted(root.iterdir(), key=lambda x: (not x.is_dir(), x.name)):
            entries.append({
                "name": p.name,
                "dir": p.is_dir(),
                "size": None if p.is_dir() else p.stat().st_size,
            })
    return {"scope": scope, "entries": entries}
