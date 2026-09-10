from __future__ import annotations

import asyncio
import contextlib
import errno
import hashlib
import hmac
import inspect
import json
import logging
import re
import secrets
import shutil
import sqlite3
import time
import unicodedata
import uuid
from pathlib import Path
from typing import BinaryIO, Iterator

from .contracts import Authorize, MapFile, OnCommitted, StagedFile


class ProtocolError(Exception):
    def __init__(self, status, code, *, retryable=None, phase=None):
        super().__init__(code)
        self.status, self.code = status, code
        self.retryable, self.phase = retryable, phase


def normalize_error(error, phase=None):
    if isinstance(error, ProtocolError):
        error.phase = error.phase or phase
        return error
    code = getattr(error, "errno", None)
    sqlite_code = getattr(error, "sqlite_errorcode", 0) & 255
    # Python 3.10 does not expose sqlite_errorcode.
    sqlite_message = str(error).lower() if isinstance(error, sqlite3.Error) else ""
    if (
        code in (errno.ENOSPC, errno.EDQUOT)
        or sqlite_code == 13
        or "database or disk is full" in sqlite_message
    ):
        return ProtocolError(507, "storage_full", retryable=False, phase=phase)
    if (
        isinstance(error, OSError)
        or sqlite_code in (5, 6, 8, 10, 11, 14, 26)
        or any(x in sqlite_message for x in ("disk i/o", "readonly", "read-only", "unable to open"))
    ):
        return ProtocolError(503, "storage_unavailable", retryable=False, phase=phase)
    return ProtocolError(500, "server_error", retryable=False, phase=phase)


def check(condition, code, status=400):
    if not condition:
        raise ProtocolError(status, code)


def integer(value):
    check(type(value) is int and 0 <= value <= 9007199254740991, "bad_number")
    return value


def identifier(value):
    check(
        isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}", value), "bad_id"
    )
    return value


def relative_path(value):
    check(
        isinstance(value, str)
        and 0 < len(value.encode()) <= 1024
        and unicodedata.normalize("NFC", value) == value,
        "bad_path",
    )
    for segment in value.split("/"):
        check(
            0 < len(segment.encode()) <= 240
            and not re.search(r'[\\<>:"|?*\x00-\x1f\x7f]', segment)
            and segment not in (".", "..")
            and segment[-1] not in ". "
            and not re.match(r"^(con|prn|aux|nul|com[1-9]|lpt[1-9])(?:\.|$)", segment, re.I),
            "bad_path",
        )
    return value


async def maybe(value):
    return await value if inspect.isawaitable(value) else value


def published_directory(base_dir, target_dir) -> Path:
    return Path(base_dir).resolve() / "published" / relative_path(target_dir)


class Runtime:
    def __init__(self):
        self.lock = asyncio.Lock()
        self.active = {}
        self.batches = set()
        self.ranges = set()
        self.listeners = set()
        self.transition = False
        self.overwrite = False
        self.conflict = False
        self.failure = None
        self.base_dir = None
        self.published_dir = None
        self.processing = None
        self.planning = None
        self.cancelling = None


class Engine:
    def __init__(
        self,
        base_dir,
        authorize: Authorize,
        *,
        limits=None,
        ttl_ms=86400000,
        map_file: MapFile | None = None,
        on_committed: OnCommitted | None = None,
        auto_publish=False,
        client_publish=True,
        max_meta_bytes=16384,
        max_context_bytes=65536,
        on_error=None,
    ):
        self.base = Path(base_dir).resolve()
        self.authorize, self.map_file, self.on_committed = authorize, map_file, on_committed
        self.ttl_ms = ttl_ms
        check(callable(authorize), "missing_authorize")
        check(type(auto_publish) is bool and type(client_publish) is bool, "bad_config")
        self.auto_publish, self.client_publish = auto_publish, client_publish
        self.max_meta_bytes, self.max_context_bytes = (
            integer(max_meta_bytes),
            integer(max_context_bytes),
        )
        self.on_error = on_error
        self.limits = dict(
            concurrency=6, maxParts=128, batchBytes=32 * 1024**2, partBytes=16 * 1024**2
        )
        self.limits.update(limits or {})
        check(
            all(type(v) is int and v > 0 for v in self.limits.values())
            and self.limits["concurrency"] <= 6
            and self.limits["maxParts"] <= 1024
            and self.limits["partBytes"] <= self.limits["batchBytes"],
            "bad_limits",
        )
        self.base.mkdir(parents=True, exist_ok=True)
        (self.base / "staging").mkdir(exist_ok=True)
        (self.base / "published").mkdir(exist_ok=True)
        self.db = sqlite3.connect(
            self.base / "metadata.sqlite", isolation_level=None, check_same_thread=False
        )
        self.db.row_factory = sqlite3.Row
        try:
            self.db.executescript("""PRAGMA busy_timeout=1000; PRAGMA locking_mode=EXCLUSIVE; PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;
          CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value INTEGER NOT NULL);
          CREATE TABLE IF NOT EXISTS sessions(id TEXT PRIMARY KEY, token TEXT NOT NULL, epoch INTEGER NOT NULL,
            state TEXT NOT NULL, target TEXT NOT NULL, context TEXT NOT NULL, meta TEXT NOT NULL,
            max_files INTEGER NOT NULL, max_bytes INTEGER NOT NULL, expires INTEGER NOT NULL,
            published TEXT NOT NULL DEFAULT '[]');
          CREATE TABLE IF NOT EXISTS nodes(sid TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
            path TEXT NOT NULL, kind TEXT NOT NULL, size INTEGER NOT NULL, mtime INTEGER NOT NULL,
            destination TEXT NOT NULL, done INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(sid,path));
          CREATE TABLE IF NOT EXISTS parts(sid TEXT NOT NULL, path TEXT NOT NULL, offset INTEGER NOT NULL,
            length INTEGER NOT NULL, PRIMARY KEY(sid,path,offset),
            FOREIGN KEY(sid,path) REFERENCES nodes(sid,path) ON DELETE CASCADE);
          CREATE TABLE IF NOT EXISTS batches(sid TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
            id TEXT NOT NULL, signature TEXT NOT NULL, receipt TEXT NOT NULL, PRIMARY KEY(sid,id));
          CREATE TABLE IF NOT EXISTS asks(sid TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
            id TEXT NOT NULL, path TEXT NOT NULL, message TEXT NOT NULL, answer TEXT, PRIMARY KEY(sid,id));""")
            columns = {row["name"] for row in self.db.execute("PRAGMA table_info(sessions)")}
            for name, spec in dict(
                overwrite="INTEGER NOT NULL DEFAULT 0",
                conflict="INTEGER NOT NULL DEFAULT 0",
                failure="TEXT NOT NULL DEFAULT ''",
                base_dir="TEXT NOT NULL DEFAULT ''",
                map_files="INTEGER NOT NULL DEFAULT 0",
                mapped="INTEGER NOT NULL DEFAULT 0",
                auto_publish="INTEGER NOT NULL DEFAULT 0",
                client_publish="INTEGER NOT NULL DEFAULT 1",
                hook_status="TEXT NOT NULL DEFAULT 'none'",
            ).items():
                if name not in columns:
                    self.db.execute(f"ALTER TABLE sessions ADD COLUMN {name} {spec}")
            if "overwrite" not in columns:
                self.db.execute("UPDATE sessions SET conflict=1 WHERE id IN (SELECT sid FROM asks)")
            self.db.execute("UPDATE sessions SET hook_status='failed' WHERE hook_status='running'")
            self.db.execute(
                "INSERT OR IGNORE INTO settings(key,value) VALUES('partBytes',?)",
                (self.limits["partBytes"],),
            )
            check(
                self.db.execute("SELECT value FROM settings WHERE key='partBytes'").fetchone()[0]
                == self.limits["partBytes"],
                "part_size_in_use",
            )
        except BaseException:
            self.db.close()
            raise
        self.cleanup_tasks = set()
        self.runtimes = {}
        self.closed = False
        self.publish_lock = asyncio.Lock()

    def runtime(self, sid):
        if sid not in self.runtimes:
            self.runtimes[sid] = Runtime()
        return self.runtimes[sid]

    def row(self, sid):
        identifier(sid)
        row = self.db.execute("SELECT * FROM sessions WHERE id=?", (sid,)).fetchone()
        check(row is not None, "not_found", 404)
        self.runtime(sid).base_dir = Path(row["base_dir"]) if row["base_dir"] else self.base
        self.runtime(sid).overwrite = bool(row["overwrite"])
        self.runtime(sid).conflict = bool(row["conflict"])
        return row

    def authenticate(self, sid, token):
        row = self.row(sid)
        check(isinstance(token, str) and hmac.compare_digest(row["token"], token), "denied", 403)
        return row

    def touch(self, sid):
        self.db.execute(
            "UPDATE sessions SET expires=? WHERE id=?", (int(time.time() * 1000) + self.ttl_ms, sid)
        )

    def report(self, hook, sid, error):
        try:
            if self.on_error:
                self.on_error(dict(hook=hook, sessionId=sid, error=error))
            else:
                logging.getLogger("mfup3.hooks").error("%s failed (%s)", hook, sid, exc_info=error)
        except Exception:
            pass

    async def create(self, data, headers, request=None):
        check(not self.closed, "closed", 503)
        check(isinstance(data, dict) and data.get("protocol") == "MFUP/3", "bad_protocol")
        check(type(data.get("overwrite", False)) is bool, "bad_property")
        target = relative_path(data.get("targetDir") or "uploads")
        meta = json.dumps(
            data.get("meta"), ensure_ascii=False, separators=(",", ":"), allow_nan=False
        )
        check(len(meta.encode()) <= self.max_meta_bytes, "meta_too_large", 413)
        sid, token = str(uuid.uuid4()), secrets.token_hex(32)
        try:
            auth = await maybe(
                self.authorize(
                    dict(
                        {"client": "", "query": {}, **(request or {})},
                        sessionId=sid,
                        headers=headers,
                        targetDir=target,
                        meta=json.loads(meta),
                    )
                )
            )
        except Exception as exc:
            self.report("authorize", sid, exc)
            raise ProtocolError(403, "denied") from None
        check(isinstance(auth, dict), "denied", 403)
        base = auth.get("baseDir", str(self.base))
        check(isinstance(base, str) and Path(base).is_absolute(), "bad_base_dir")
        base = Path(base).resolve()
        target = relative_path(auth.get("targetDir", target))
        context = auth.get("context", {})
        check(isinstance(context, dict), "bad_context")
        context = json.dumps(context, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
        check(len(context.encode()) <= self.max_context_bytes, "context_too_large")
        auto, client = (
            auth.get("autoPublish", self.auto_publish),
            auth.get("clientPublish", self.client_publish),
        )
        check(type(auto) is bool and type(client) is bool, "bad_config")
        max_files, max_bytes = (
            integer(auth.get("maxFiles", 100000)),
            integer(auth.get("maxTotalBytes", 9007199254740991)),
        )
        stage = base / "staging" / sid
        stage.mkdir(parents=True, exist_ok=True)
        try:
            self.db.execute(
                "INSERT INTO sessions(id,token,epoch,state,target,context,meta,max_files,max_bytes,expires,base_dir,map_files,auto_publish,client_publish,hook_status,overwrite) VALUES(?,?,1,'uploading',?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    sid,
                    token,
                    target,
                    context,
                    meta,
                    max_files,
                    max_bytes,
                    int(time.time() * 1000) + self.ttl_ms,
                    str(base),
                    int(self.map_file is not None),
                    int(auto),
                    int(client),
                    "pending" if self.on_committed else "none",
                    int(data.get("overwrite", False)),
                ),
            )
        except BaseException:
            shutil.rmtree(stage, ignore_errors=True)
            raise
        self.runtime(sid).base_dir = base
        return dict(id=sid, token=token, epoch=1, limits=self.limits)

    def staging(self, sid):
        runtime = self.runtime(sid)
        if runtime.base_dir is None:
            self.row(sid)
        return runtime.base_dir / "staging" / identifier(sid)

    def payload(self, sid, name):
        return self.staging(sid) / hashlib.sha256(name.encode()).hexdigest()

    def destination(self, row, name):
        base = Path(row["base_dir"]) if row["base_dir"] else self.base
        r = self.runtime(row["id"])
        if r.published_dir is None:
            r.published_dir = published_directory(base, row["target"])
        return r.published_dir / name

    def get_session(self, sid):
        row, state = self.row(sid), self.snapshot(sid)
        return dict(
            sessionId=sid,
            targetDir=row["target"],
            baseDir=row["base_dir"] or str(self.base),
            stagingDir=str(self.staging(sid)),
            files=state["files"],
            bytes=state["bytes"],
            context=json.loads(row["context"]),
            meta=json.loads(row["meta"]),
            state=row["state"],
            processing=row["hook_status"],
        )

    def list_staged(self, sid) -> Iterator[StagedFile]:
        check(self.row(sid)["state"] == "committed", "bad_state", 409)
        after = ""
        while True:
            page = list(
                self.db.execute(
                    "SELECT * FROM nodes WHERE sid=? AND kind='file' AND path>? ORDER BY path LIMIT 256",
                    (sid, after),
                )
            )
            for f in page:
                yield dict(
                    path=f["path"],
                    size=f["size"],
                    mtime=f["mtime"],
                    localPath=str(self.payload(sid, f["path"])),
                )
            if len(page) < 256:
                return
            after = page[-1]["path"]

    def open_staged(self, sid, name) -> BinaryIO:
        check(self.row(sid)["state"] == "committed", "bad_state", 409)
        node = self.db.execute(
            "SELECT * FROM nodes WHERE sid=? AND path=? AND kind='file'", (sid, relative_path(name))
        ).fetchone()
        check(node is not None, "not_found", 404)
        return self.payload(sid, name).open("rb")

    def needs_overwrite(self, row):
        return (
            not row["overwrite"]
            and bool(row["conflict"])
            and row["state"] not in ("cancelled", "published")
        )

    def questions(self, sid):
        return (
            [
                dict(
                    id="overwrite",
                    message="overwrite_required",
                    choices=["overwrite", "cancel"],
                    answer=None,
                )
            ]
            if self.needs_overwrite(self.row(sid))
            else []
        )

    def record_failure(self, sid, error, phase):
        exc = normalize_error(error, phase)
        if exc.status < 500:
            return exc
        self.report(phase, sid, error)
        try:
            row, r = self.row(sid), self.runtime(sid)
            if row["state"] == "cancelled":
                return exc
            r.failure = r.failure or (
                json.loads(row["failure"])
                if row["failure"]
                else dict(code=exc.code, status=exc.status, phase=phase, retryable=False)
            )
            with contextlib.suppress(Exception):
                self.db.execute(
                    "UPDATE sessions SET failure=? WHERE id=? AND failure=''",
                    (json.dumps(r.failure), sid),
                )
            with contextlib.suppress(Exception):
                self.emit(sid)
        except Exception:
            pass
        return exc

    def clear_failure(self, sid):
        if self.row(sid)["failure"]:
            self.db.execute("UPDATE sessions SET failure='' WHERE id=?", (sid,))
        self.runtime(sid).failure = None

    def snapshot(self, sid):
        row = self.row(sid)
        totals = self.db.execute(
            "SELECT COUNT(*) AS files,COALESCE(SUM(size),0) AS bytes FROM nodes WHERE sid=? AND kind='file'",
            (sid,),
        ).fetchone()
        return dict(
            id=sid,
            epoch=row["epoch"],
            state=row["state"],
            files=totals["files"],
            bytes=totals["bytes"],
            asks=self.questions(sid),
            published=json.loads(row["published"]),
            clientPublish=bool(row["client_publish"]),
            processing=row["hook_status"],
            overwrite=bool(row["overwrite"]),
            overwriteRequired=self.needs_overwrite(row),
            error=self.runtime(sid).failure
            or (json.loads(row["failure"]) if row["failure"] else None),
        )

    def emit(self, sid):
        listeners = self.runtime(sid).listeners
        if listeners:
            state = self.snapshot(sid)
            for listener in tuple(listeners):
                with contextlib.suppress(Exception):
                    listener(state)

    def subscribe(self, sid, listener):
        self.row(sid)
        runtime = self.runtime(sid)
        runtime.listeners.add(listener)
        listener(self.snapshot(sid))
        return lambda: runtime.listeners.discard(listener)

    def conflicts(self, sid, row, name, kind):
        r = self.runtime(sid)
        if r.overwrite or r.conflict or row["overwrite"] or row["conflict"]:
            return
        pieces = name.split("/")
        for i in range(1, len(pieces) + 1):
            dest = self.destination(row, "/".join(pieces[:i]))
            if dest.exists() and (i == len(pieces) and kind == "file" or not dest.is_dir()):
                self.db.execute("UPDATE sessions SET conflict=1 WHERE id=? AND overwrite=0", (sid,))
                r.conflict = True
                return

    async def set_properties(self, sid, properties):
        check(
            isinstance(properties, dict) and type(properties.get("overwrite")) is bool,
            "bad_property",
        )
        r = self.runtime(sid)
        async with r.lock:
            row = self.row(sid)
            check(row["state"] != "cancelled" and not r.transition, "bad_state", 409)
            check(
                not row["overwrite"] or properties["overwrite"], "overwrite_already_approved", 409
            )
            if bool(row["overwrite"]) != properties["overwrite"]:
                self.db.execute(
                    "UPDATE sessions SET overwrite=? WHERE id=?",
                    (int(properties["overwrite"]), sid),
                )
                r.overwrite = properties["overwrite"]
                self.touch(sid)
                self.emit(sid)
        row = self.row(sid)
        if (
            row["auto_publish"]
            and row["state"] in ("committed", "publishing")
            and row["hook_status"] in ("none", "done")
            and row["overwrite"]
        ):
            try:
                await self.publish(sid)
            except ProtocolError as exc:
                if exc.code != "answers_required":
                    raise
        return self.snapshot(sid)

    async def answer(self, sid, qid, choice):
        check(qid == "overwrite", "unknown_question", 404)
        check(choice in ("overwrite", "cancel"), "bad_choice")
        return (
            await self.cancel(sid)
            if choice == "cancel"
            else await self.set_properties(sid, dict(overwrite=True))
        )

    async def begin(self, sid, epoch, bid, abort):
        identifier(bid)
        r = self.runtime(sid)
        async with r.lock:
            row = self.row(sid)
            check(not r.transition and row["state"] == "uploading", "bad_state", 409)
            check(row["epoch"] == epoch, "stale_epoch", 409)
            check(len(r.active) < self.limits["concurrency"] and bid not in r.batches, "busy", 429)
            done = asyncio.get_running_loop().create_future()
            r.active[done] = abort
            r.batches.add(bid)
            self.touch(sid)
            return dict(sid=sid, id=bid, epoch=epoch, done=done)

    def receipt(self, sid, bid):
        row = self.db.execute(
            "SELECT receipt FROM batches WHERE sid=? AND id=?", (sid, identifier(bid))
        ).fetchone()
        return json.loads(row[0]) if row else None

    async def prepare(self, batch, data):
        check(
            isinstance(data, dict)
            and isinstance(data.get("files"), list)
            and isinstance(data.get("dirs"), list),
            "bad_manifest",
        )
        check(0 < len(data["files"]) + len(data["dirs"]) <= self.limits["maxParts"], "bad_manifest")
        files = []
        for p in data["files"]:
            check(isinstance(p, list) and len(p) == 5, "bad_manifest")
            p = [relative_path(p[0]), *map(integer, p[1:])]
            check(
                p[3] % self.limits["partBytes"] == 0
                and p[3] <= p[1]
                and p[4] == min(self.limits["partBytes"], p[1] - p[3])
                and (p[4] > 0 or p[1] == 0),
                "bad_range",
            )
            files.append(p)
        dirs = [relative_path(d) for d in data["dirs"]]
        check(sum(p[4] for p in files) <= self.limits["batchBytes"], "batch_too_large", 413)
        manifest = dict(files=files, dirs=dirs)
        signature = hashlib.sha256(
            json.dumps(manifest, ensure_ascii=False, separators=(",", ":")).encode()
        ).hexdigest()
        sid, bid = batch["sid"], batch["id"]
        row = self.row(sid)
        r = self.runtime(sid)
        async with r.lock:
            current = self.row(sid)
            check(current["epoch"] == batch["epoch"], "stale_epoch", 409)
            check(current["state"] == "uploading" and not r.transition, "bad_state", 409)
            old = self.db.execute(
                "SELECT signature,receipt FROM batches WHERE sid=? AND id=?", (sid, bid)
            ).fetchone()
            if old:
                check(old["signature"] == signature, "batch_conflict", 409)
                return json.loads(old["receipt"])
            keys = [(p[0], p[3]) for p in files]
            check(
                len(set(keys)) == len(keys) and not any(k in r.ranges for k in keys),
                "range_busy",
                409,
            )
            existing = [dict(n) for n in self.db.execute("SELECT * FROM nodes WHERE sid=?", (sid,))]
            nodes = {n["path"]: n for n in existing}
            destinations = {n["destination"].lower(): n for n in existing}
            spelling = {}

            def check_spelling(name):
                pieces = name.split("/")
                for i in range(1, len(pieces) + 1):
                    prefix = "/".join(pieces[:i])
                    lower = prefix.lower()
                    check(lower not in spelling or spelling[lower] == prefix, "path_conflict", 409)
                    spelling[lower] = prefix

            for n in existing:
                check_spelling(n["destination"])
            additions = []

            def add(name, kind, size, mtime, destination):
                previous = nodes.get(name)
                if previous:
                    check(
                        previous["kind"] == kind
                        and previous["size"] == size
                        and previous["mtime"] == mtime
                        and previous["destination"] == destination,
                        "file_changed",
                        409,
                    )
                    return
                check_spelling(destination)
                lower = destination.lower()
                check(lower not in destinations, "path_conflict", 409)
                parent = lower
                while "/" in parent:
                    parent = parent.rsplit("/", 1)[0]
                    check(destinations.get(parent, {}).get("kind") != "file", "path_conflict", 409)
                if kind == "file":
                    check(
                        not any(p.startswith(lower + "/") for p in destinations),
                        "path_conflict",
                        409,
                    )
                n = dict(
                    path=name, kind=kind, size=size, mtime=mtime, destination=destination, done=0
                )
                nodes[name] = n
                destinations[lower] = n
                additions.append(n)

            for d in dirs:
                add(d, "directory", 0, 0, d)
            for p in files:
                add(p[0], "file", p[1], p[2], p[0])
            file_nodes = [n for n in nodes.values() if n["kind"] == "file"]
            check(
                len(nodes) <= row["max_files"] * 4 + 1024
                and len(file_nodes) <= row["max_files"]
                and sum(n["size"] for n in file_nodes) <= row["max_bytes"],
                "quota_exceeded",
                413,
            )
            self.db.execute("BEGIN IMMEDIATE")
            try:
                for n in additions:
                    self.db.execute(
                        "INSERT INTO nodes(sid,path,kind,size,mtime,destination) VALUES(?,?,?,?,?,?)",
                        (sid, n["path"], n["kind"], n["size"], n["mtime"], n["destination"]),
                    )
                    if not row["map_files"]:
                        self.conflicts(sid, row, n["destination"], n["kind"])
                self.db.execute("COMMIT")
            except BaseException:
                with contextlib.suppress(sqlite3.Error):
                    self.db.execute("ROLLBACK")
                raise
            batch.update(manifest=manifest, signature=signature, entries=[])
            for p in files:
                file_path = self.payload(sid, p[0])
                if not file_path.exists():
                    file_path.touch(exist_ok=False)
                skip = bool(
                    self.db.execute(
                        "SELECT 1 FROM parts WHERE sid=? AND path=? AND offset=?", (sid, p[0], p[3])
                    ).fetchone()
                )
                batch["entries"].append(dict(item=p, filePath=file_path, skip=skip))
            r.ranges.update(keys)
            self.emit(sid)
            return None

    async def complete(self, batch):
        sid, bid = batch["sid"], batch["id"]
        r = self.runtime(sid)
        async with r.lock:
            row = self.row(sid)
            check(
                row["state"] == "uploading" and not r.transition and row["epoch"] == batch["epoch"],
                "bad_state",
                409,
            )
            receipt = dict(
                id=bid,
                parts=len(batch["manifest"]["files"]),
                bytes=sum(p[4] for p in batch["manifest"]["files"]),
            )
            self.db.execute("BEGIN IMMEDIATE")
            try:
                self.db.executemany(
                    "INSERT OR IGNORE INTO parts(sid,path,offset,length) VALUES(?,?,?,?)",
                    [(sid, p[0], p[3], p[4]) for p in batch["manifest"]["files"]],
                )
                self.db.execute(
                    "INSERT INTO batches(sid,id,signature,receipt) VALUES(?,?,?,?)",
                    (sid, bid, batch["signature"], json.dumps(receipt)),
                )
                self.touch(sid)
                self.db.execute("COMMIT")
            except BaseException:
                with contextlib.suppress(sqlite3.Error):
                    self.db.execute("ROLLBACK")
                raise
            return receipt

    def end(self, batch):
        r = self.runtime(batch["sid"])
        r.ranges.difference_update((p[0], p[3]) for p in batch.get("manifest", {}).get("files", []))
        r.batches.discard(batch["id"])
        r.active.pop(batch["done"], None)
        if not batch["done"].done():
            batch["done"].set_result(None)

    async def resume(self, sid):
        r = self.runtime(sid)
        async with r.lock:
            check(
                not r.transition
                and self.row(sid)["state"] in ("uploading", "committed", "publishing", "published"),
                "bad_state",
                409,
            )
            r.transition = True
            active = list(r.active)
        try:
            await asyncio.gather(*active)
            async with r.lock:
                self.clear_failure(sid)
                self.db.execute("UPDATE sessions SET epoch=epoch+1 WHERE id=?", (sid,))
                self.touch(sid)
                return dict(self.snapshot(sid), limits=self.limits)
        finally:
            r.transition = False

    def resume_page(self, sid, after="", limit=256):
        self.row(sid)
        limit = max(1, min(integer(limit), 256))
        files = list(
            self.db.execute(
                "SELECT * FROM nodes WHERE sid=? AND kind='file' AND path>? ORDER BY path LIMIT ?",
                (sid, after, limit),
            )
        )
        return dict(
            files=[
                dict(
                    path=f["path"],
                    size=f["size"],
                    mtime=f["mtime"],
                    offsets=[
                        p[0]
                        for p in self.db.execute(
                            "SELECT offset FROM parts WHERE sid=? AND path=? ORDER BY offset",
                            (sid, f["path"]),
                        )
                    ],
                )
                for f in files
            ],
            next=files[-1]["path"] if len(files) == limit else None,
        )

    async def commit(self, sid, totals):
        r = self.runtime(sid)
        async with r.lock:
            row = self.row(sid)
            if row["state"] in ("committed", "published"):
                return self.snapshot(sid)
            check(row["state"] == "uploading" and not r.transition and not r.active, "busy", 409)
            files = list(self.db.execute("SELECT * FROM nodes WHERE sid=? AND kind='file'", (sid,)))
            dirs = self.db.execute(
                "SELECT COUNT(*) FROM nodes WHERE sid=? AND kind='directory'", (sid,)
            ).fetchone()[0]
            check(
                integer(totals.get("files")) == len(files)
                and integer(totals.get("dirs")) == dirs
                and integer(totals.get("bytes")) == sum(f["size"] for f in files),
                "scan_mismatch",
                409,
            )
            for f in files:
                count, size = self.db.execute(
                    "SELECT COUNT(*),COALESCE(SUM(length),0) FROM parts WHERE sid=? AND path=?",
                    (sid, f["path"]),
                ).fetchone()
                check(
                    count
                    == max(
                        1, (f["size"] + self.limits["partBytes"] - 1) // self.limits["partBytes"]
                    )
                    and size == f["size"],
                    "incomplete",
                    409,
                )
            self.db.execute("UPDATE sessions SET state='committed' WHERE id=?", (sid,))
            self.touch(sid)
            self.emit(sid)
        return await self.retry_committed(sid)

    async def retry_committed(self, sid):
        r = self.runtime(sid)
        if r.processing:
            await asyncio.shield(r.processing)
            return self.snapshot(sid)

        async def process():
            async with r.lock:
                row = self.row(sid)
                check(row["state"] == "committed" and not r.transition, "bad_state", 409)
                if row["hook_status"] not in ("none", "done"):
                    self.db.execute("UPDATE sessions SET hook_status='running' WHERE id=?", (sid,))
                    self.emit(sid)
            auto = bool(row["auto_publish"])
            if row["hook_status"] not in ("none", "done"):
                try:
                    check(self.on_committed is not None, "hook_unavailable", 503)
                    decision = await maybe(self.on_committed(self.get_session(sid)))
                    if self.row(sid)["state"] == "cancelled":
                        return
                    check(decision is None or type(decision) is bool, "bad_hook_result")
                    if decision is not None:
                        auto = decision
                    self.db.execute(
                        "UPDATE sessions SET hook_status='done',auto_publish=? WHERE id=?",
                        (int(auto), sid),
                    )
                except Exception as exc:
                    if self.row(sid)["state"] == "cancelled":
                        return
                    self.db.execute("UPDATE sessions SET hook_status='failed' WHERE id=?", (sid,))
                    self.report("onCommitted", sid, exc)
                    self.emit(sid)
                    return
            if auto and self.row(sid)["state"] != "published":
                try:
                    await self.publish(sid)
                except Exception as exc:
                    if not isinstance(exc, ProtocolError) or exc.code != "answers_required":
                        self.report("publish", sid, exc)
            self.emit(sid)

        task = asyncio.create_task(process())
        r.processing = task

        def finished(_):
            if r.processing is task:
                r.processing = None

        task.add_done_callback(finished)
        await asyncio.shield(task)
        return self.snapshot(sid)

    async def _prepare_plan(self, sid):
        r = self.runtime(sid)
        if r.planning:
            await asyncio.shield(r.planning)
            return self.snapshot(sid)

        async def prepare():
            async with r.lock:
                row = self.row(sid)
                check(
                    row["state"] in ("committed", "publishing", "published") and not r.transition,
                    "bad_state",
                    409,
                )
            if row["state"] == "published":
                return
            if row["map_files"] and not row["mapped"]:
                check(self.map_file is not None, "hook_unavailable", 503)
                files = list(
                    self.db.execute(
                        "SELECT * FROM nodes WHERE sid=? AND kind='file' ORDER BY path", (sid,)
                    )
                )
                plan = []
                try:
                    for f in files:
                        dest = await maybe(
                            self.map_file(
                                dict(
                                    sessionId=sid,
                                    path=f["path"],
                                    name=f["path"].rsplit("/", 1)[-1],
                                    size=f["size"],
                                    targetDir=row["target"],
                                    context=json.loads(row["context"]),
                                    meta=json.loads(row["meta"]),
                                )
                            )
                        )
                        plan.append((f["path"], relative_path(f["path"] if dest is None else dest)))
                    names, spelling = set(), {}
                    for _, dest in plan:
                        lower = dest.lower()
                        check(lower not in names, "mapping_error", 409)
                        names.add(lower)
                        parts = dest.split("/")
                        for i in range(1, len(parts) + 1):
                            prefix = "/".join(parts[:i])
                            key = prefix.lower()
                            check(
                                key not in spelling or spelling[key] == prefix, "mapping_error", 409
                            )
                            spelling[key] = prefix
                    for _, dest in plan:
                        parts = dest.lower().split("/")
                        for i in range(1, len(parts)):
                            check("/".join(parts[:i]) not in names, "mapping_error", 409)
                except Exception as exc:
                    self.report("mapFile", sid, exc)
                    raise ProtocolError(409, "mapping_error") from None
                async with r.lock:
                    check(
                        self.row(sid)["state"] == "committed" and not r.transition, "bad_state", 409
                    )
                    self.db.execute("BEGIN IMMEDIATE")
                    try:
                        self.db.executemany(
                            "UPDATE nodes SET destination=? WHERE sid=? AND path=?",
                            ((dest, sid, name) for name, dest in plan),
                        )
                        self.db.execute("UPDATE sessions SET mapped=1 WHERE id=?", (sid,))
                        self.db.execute("COMMIT")
                    except BaseException:
                        with contextlib.suppress(sqlite3.Error):
                            self.db.execute("ROLLBACK")
                        raise

        task = asyncio.create_task(prepare())
        r.planning = task

        def finished(_):
            if r.planning is task:
                r.planning = None

        task.add_done_callback(finished)
        await asyncio.shield(task)
        return self.snapshot(sid)

    async def prepare_publish(self, sid):
        await self._prepare_plan(sid)
        r = self.runtime(sid)
        async with r.lock:
            current = self.row(sid)
            for n in self.db.execute("SELECT * FROM nodes WHERE sid=?", (sid,)):
                if current["map_files"] and n["kind"] == "directory":
                    continue
                if not n["done"] and (
                    n["kind"] == "directory" or self.payload(sid, n["path"]).exists()
                ):
                    self.conflicts(sid, current, n["destination"], n["kind"])
            self.touch(sid)
            self.emit(sid)
        return self.snapshot(sid)

    async def publish_from_client(self, sid):
        def allowed():
            row = self.row(sid)
            check(bool(row["client_publish"]), "server_publish_only", 403)
            check(row["hook_status"] in ("none", "done"), "processing_required", 409)

        allowed()
        return await self.publish(sid, _before_move=allowed)

    async def publish(self, sid, *, _before_move=None):
        try:
            self.clear_failure(sid)
            return await self._publish(sid, _before_move=_before_move)
        except Exception as exc:
            raise self.record_failure(sid, exc, "publish") from exc

    async def _publish(self, sid, *, _before_move=None):
        await self._prepare_plan(sid)
        r = self.runtime(sid)
        async with self.publish_lock, r.lock:
            if _before_move:
                _before_move()
            row = self.row(sid)
            if row["state"] == "published":
                return self.snapshot(sid)
            check(
                row["state"] in ("committed", "publishing") and not r.transition and not r.active,
                "bad_state",
                409,
            )
            nodes = [
                n
                for n in self.db.execute("SELECT * FROM nodes WHERE sid=? ORDER BY path", (sid,))
                if not row["map_files"] or n["kind"] == "file"
            ]
            for n in nodes:
                present = n["kind"] == "directory" or self.payload(sid, n["path"]).exists()
                if not n["done"] and not present and row["state"] == "committed":
                    raise ProtocolError(
                        503, "storage_unavailable", retryable=False, phase="publish"
                    )
                if not n["done"] and present:
                    self.conflicts(sid, row, n["destination"], n["kind"])
            if any(q["answer"] is None for q in self.questions(sid)):
                self.emit(sid)
                raise ProtocolError(409, "answers_required")

            def ensure_directory(name):
                pieces = [p for p in name.split("/") if p]
                for i in range(1, len(pieces) + 1):
                    relative = "/".join(pieces[:i])
                    dest = self.destination(row, relative)
                    if dest.exists() and not dest.is_dir():
                        check(bool(row["overwrite"]), "answers_required", 409)
                        dest.unlink()
                    dest.mkdir(parents=True, exist_ok=True)

            self.db.execute("UPDATE sessions SET state='publishing' WHERE id=?", (sid,))
            for n in nodes:
                if n["done"]:
                    continue
                dest = self.destination(row, n["destination"])
                if n["kind"] == "directory":
                    ensure_directory(n["destination"])
                else:
                    self.destination(row, "").mkdir(parents=True, exist_ok=True)
                    ensure_directory(
                        n["destination"].rsplit("/", 1)[0] if "/" in n["destination"] else ""
                    )
                    source = self.payload(sid, n["path"])
                    if source.exists():
                        if dest.exists():
                            check(
                                bool(row["overwrite"]),
                                "answers_required",
                                409,
                            )
                            if dest.is_dir():
                                shutil.rmtree(dest)
                        source.replace(dest)
                    else:
                        check(dest.exists(), "missing_payload", 409)
            published = [n["destination"] for n in nodes if n["kind"] == "file"]
            # The persisted plan and source/destination presence recover partial moves.
            # A single metadata transaction avoids a WAL commit for every file.
            self.db.execute("BEGIN IMMEDIATE")
            try:
                self.db.execute("UPDATE nodes SET done=1 WHERE sid=?", (sid,))
                self.db.execute(
                    "UPDATE sessions SET state='published',published=? WHERE id=?",
                    (json.dumps(published), sid),
                )
                self.touch(sid)
                self.db.execute("COMMIT")
            except BaseException:
                with contextlib.suppress(sqlite3.Error):
                    self.db.execute("ROLLBACK")
                raise
            if self.staging(sid).exists():
                shutil.rmtree(self.staging(sid))
            self.emit(sid)
            return self.snapshot(sid)

    async def cancel(self, sid):
        r = self.runtime(sid)
        if r.cancelling:
            return await asyncio.shield(r.cancelling)
        task = asyncio.create_task(self._cancel(sid))
        r.cancelling = task

        def finished(_):
            if r.cancelling is task:
                r.cancelling = None

        task.add_done_callback(finished)
        return await asyncio.shield(task)

    async def _cancel(self, sid):
        r = self.runtime(sid)
        async with r.lock:
            row = self.row(sid)
            if row["state"] in ("published", "cancelled"):
                return self.snapshot(sid)
            check(not r.transition, "busy", 409)
            self.db.execute("UPDATE sessions SET state='cancelled' WHERE id=?", (sid,))
            r.transition = True
            active = list(r.active)
            for abort in list(r.active.values()):
                abort()
            self.touch(sid)
            self.emit(sid)
        await asyncio.gather(*active, return_exceptions=True)
        r.transition = False

        async def cleanup():
            try:
                if self.staging(sid).exists():
                    shutil.rmtree(self.staging(sid))
            except Exception as exc:
                self.report("cancel_cleanup", sid, exc)

        callbacks = [t for t in (r.processing, r.planning) if t]
        if callbacks:

            async def later():
                await asyncio.gather(*callbacks, return_exceptions=True)
                await cleanup()

            task = asyncio.create_task(later())
            self.cleanup_tasks.add(task)
            task.add_done_callback(self.cleanup_tasks.discard)
        else:
            await cleanup()
        return self.snapshot(sid)

    async def sweep(self, now=None):
        now = int(time.time() * 1000) if now is None else now
        candidates = list(self.db.execute("SELECT id FROM sessions WHERE expires<=?", (now,)))
        removed = 0
        for (sid,) in candidates:
            r = self.runtime(sid)
            async with r.lock:
                row = self.db.execute("SELECT * FROM sessions WHERE id=?", (sid,)).fetchone()
                if (
                    row is None
                    or row["expires"] > now
                    or r.active
                    or r.transition
                    or r.listeners
                    or r.processing
                    or r.planning
                ):
                    continue
                r.transition = True
                try:
                    if self.staging(sid).exists():
                        shutil.rmtree(self.staging(sid))
                    self.db.execute("DELETE FROM sessions WHERE id=?", (sid,))
                    self.runtimes.pop(sid, None)
                    removed += 1
                finally:
                    r.transition = False
        return removed

    async def close(self):
        if self.closed:
            return
        self.closed = True
        active = [done for r in self.runtimes.values() for done in r.active]
        for r in self.runtimes.values():
            for abort in list(r.active.values()):
                abort()
        await asyncio.gather(*active)
        await asyncio.gather(
            *(t for r in self.runtimes.values() for t in (r.processing, r.planning) if t),
            return_exceptions=True,
        )
        await asyncio.gather(
            *(r.cancelling for r in self.runtimes.values() if r.cancelling), return_exceptions=True
        )
        await asyncio.gather(*self.cleanup_tasks, return_exceptions=True)
        self.db.close()
