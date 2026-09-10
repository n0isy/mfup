from __future__ import annotations

from typing import Any, Awaitable, Callable, TypedDict


class AuthRequest(TypedDict):
    sessionId: str
    headers: dict[str, str]
    client: str
    query: dict[str, str]
    targetDir: str
    meta: Any


class AuthResult(TypedDict, total=False):
    baseDir: str
    targetDir: str
    maxFiles: int
    maxTotalBytes: int
    context: dict[str, Any]
    autoPublish: bool
    clientPublish: bool


class CommitEvent(TypedDict):
    sessionId: str
    targetDir: str
    baseDir: str
    stagingDir: str
    files: int
    bytes: int
    context: dict[str, Any]
    meta: Any


class FileMapRequest(TypedDict):
    sessionId: str
    path: str
    name: str
    size: int
    targetDir: str
    context: dict[str, Any]
    meta: Any


class StagedFile(TypedDict):
    path: str
    size: int
    mtime: int
    localPath: str


Authorize = Callable[[AuthRequest], AuthResult | None | Awaitable[AuthResult | None]]
MapFile = Callable[[FileMapRequest], str | None | Awaitable[str | None]]
OnCommitted = Callable[[CommitEvent], bool | None | Awaitable[bool | None]]
