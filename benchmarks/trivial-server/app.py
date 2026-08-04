"""Trivial file-by-file upload server for comparison demo."""

import os
import logging
from pathlib import Path

from fastapi import FastAPI, UploadFile, File, Form
from fastapi.responses import JSONResponse

logger = logging.getLogger("trivial")
logging.basicConfig(level=logging.INFO)

BASE_DIR = Path(os.environ.get("UPLOAD_DIR", "/data/uploads"))

app = FastAPI(title="Trivial Upload Server")


@app.post("/upload")
async def upload_file(
    file: UploadFile = File(...),
    path: str = Form(default=""),
):
    """Accept a single file upload (multipart/form-data)."""
    rel = Path(path) / file.filename if path else Path(file.filename)
    dest = BASE_DIR / rel

    # Safety: stay within BASE_DIR
    try:
        dest.resolve().relative_to(BASE_DIR.resolve())
    except ValueError:
        return JSONResponse({"error": "path traversal"}, status_code=403)

    dest.parent.mkdir(parents=True, exist_ok=True)

    size = 0
    with open(dest, "wb") as f:
        while chunk := await file.read(256 * 1024):
            f.write(chunk)
            size += len(chunk)

    return {"ok": True, "file": str(rel), "size": size}


@app.get("/health")
async def health():
    return {"status": "ok"}
