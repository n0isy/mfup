"""Public-package consumer: anonymous identity -> authorized scope -> server-owned layout."""

import asyncio
import contextlib
import hashlib
import heapq
import os
import re
import secrets
from http.cookies import SimpleCookie
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse
from mfup_core import AuthRequest, AuthResult, ProtocolError, published_directory, relative_path
from mfup_fastapi import MfupConfig, MfupEngine

SCOPES = ("workspace", "scratch", "uploads")
COOKIE = "mfup3_python_user"


def identity(headers):
    jar = SimpleCookie()
    with contextlib.suppress(Exception):
        jar.load(headers.get("cookie", ""))
    token = jar[COOKIE].value if COOKIE in jar else ""
    return (
        hashlib.sha256(token.encode()).hexdigest()[:32]
        if re.fullmatch(r"[a-f0-9]{64}", token)
        else None
    )


def create_example(base_dir=None, *, scope_roots=None, **options):
    base = Path(base_dir or os.environ.get("DEMO_DATA_DIR", "./data/example-python")).resolve()

    roots = {scope: Path((scope_roots or {}).get(scope, base)).resolve() for scope in SCOPES}

    async def authorize(request: AuthRequest) -> AuthResult | None:
        uid = identity(request["headers"])
        meta = request["meta"]
        scope = meta.get("scope") if isinstance(meta, dict) else None
        if not uid or scope not in SCOPES:
            return None
        return dict(
            baseDir=str(roots[scope]),
            targetDir=f"{uid}/{scope}",
            maxTotalBytes=512 * 2**20,
            maxFiles=1000000,
            context=dict(uid=uid, scope=scope),
        )

    mfup = MfupEngine(MfupConfig(base_dir=base, authorize=authorize, prefix="/api", **options))

    app = FastAPI(title="MFUP/3 multiuser scopes", lifespan=mfup.lifespan)
    app.include_router(mfup.router, prefix=mfup.config.prefix)
    app.state.mfup = mfup

    @app.get("/api/whoami")
    async def whoami(request: Request):
        uid = identity(request.headers)
        token = None
        if not uid:
            token = secrets.token_hex(32)
            uid = hashlib.sha256(token.encode()).hexdigest()[:32]
        response = JSONResponse(
            dict(user_id=uid, scopes=SCOPES, backend="python"),
            headers={"cache-control": "no-store"},
        )
        if token:
            response.set_cookie(
                COOKIE,
                token,
                max_age=2592000,
                httponly=True,
                samesite="lax",
                secure=os.environ.get("COOKIE_SECURE") == "1",
            )
        return response

    @app.get("/api/files/{scope}")
    @app.get("/api/file/{scope}")
    async def files(scope: str, request: Request, path: str = ""):
        uid = identity(request.headers)
        if not uid or scope not in SCOPES:
            return JSONResponse(dict(error="unknown_user_or_scope"), status_code=403)
        try:
            rel = relative_path(path) if path else ""
            target = published_directory(roots[scope], f"{uid}/{scope}") / rel
            if request.url.path.startswith("/api/file/"):
                if not rel or not target.is_file():
                    return JSONResponse(dict(error="not_found"), status_code=404)
                return FileResponse(
                    target,
                    filename=target.name,
                    media_type="application/octet-stream",
                    headers={"cache-control": "no-store"},
                )

            def listing():
                if not target.exists():
                    return dict(entries=[], next=None)
                after = request.query_params.get("after", "")
                with os.scandir(target) as iterator:
                    candidates = ((("0:" if p.is_dir() else "1:") + p.name, p) for p in iterator)
                    page = heapq.nsmallest(
                        257, (p for p in candidates if p[0] > after), key=lambda p: p[0]
                    )
                return dict(
                    entries=[
                        dict(
                            name=p.name,
                            dir=p.is_dir(),
                            size=None if p.is_dir() else p.stat().st_size,
                        )
                        for _, p in page[:256]
                    ],
                    next=page[255][0] if len(page) > 256 else None,
                )

            return JSONResponse(
                dict(scope=scope, path=rel, **await asyncio.to_thread(listing)),
                headers={"cache-control": "no-store"},
            )
        except ProtocolError as error:
            return JSONResponse(dict(error=error.code), status_code=error.status)

    return app
