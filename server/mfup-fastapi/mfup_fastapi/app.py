from __future__ import annotations

import asyncio
import contextlib
import json
from contextlib import asynccontextmanager

from anyio import CancelScope
from fastapi import APIRouter, FastAPI, Request, WebSocket
from fastapi.responses import JSONResponse
from mfup_core import Engine, ProtocolError
from mfup_core.engine import check, integer, normalize_error
from python_multipart.multipart import MultipartParser, parse_options_header
from starlette.requests import ClientDisconnect

from .config import MfupConfig


async def json_body(request):
    chunks = bytearray()
    async for chunk in request.stream():
        chunks.extend(chunk)
        check(len(chunks) <= 262144, "body_too_large", 413)
    try:
        data = json.loads(chunks or b"{}")
        check(isinstance(data, dict), "bad_json")
        return data
    except (ValueError, TypeError):
        raise ProtocolError(400, "bad_json")


class MfupEngine:
    def __init__(self, base_dir, authorize=None, **options):
        config = (
            base_dir
            if isinstance(base_dir, MfupConfig)
            else MfupConfig(base_dir=base_dir, authorize=authorize, **options)
        )
        self.config = config
        self.engine = Engine(**config.engine_options())
        self._sweeper = None
        self.router = APIRouter()
        engine = self.engine

        @self.router.get("/mfup/health")
        async def health():
            return dict(protocol="MFUP/3", backend="python")

        @self.router.post("/mfup/sessions")
        async def create(request: Request):
            try:
                return JSONResponse(
                    await engine.create(
                        await json_body(request),
                        dict(request.headers),
                        {
                            "client": f"{request.client.host}:{request.client.port}"
                            if request.client
                            else "",
                            "query": dict(request.query_params),
                        },
                    ),
                    status_code=201,
                )
            except Exception as exc:
                return self.error(engine.record_failure("", exc, "create"))

        @self.router.api_route("/mfup/sessions/{sid}", methods=["GET"])
        @self.router.api_route("/mfup/sessions/{sid}/{action}", methods=["GET", "POST"])
        @self.router.api_route("/mfup/sessions/{sid}/{action}/{bid}", methods=["GET", "POST"])
        async def route(request: Request, sid: str, action: str = "", bid: str = ""):
            authenticated = False
            phase = "upload" if action == "batches" else (action or "status")
            try:
                engine.authenticate(
                    sid, request.headers.get("authorization", "").removeprefix("Bearer ")
                )
                authenticated = True
                if request.method == "GET":
                    if not action:
                        return engine.snapshot(sid)
                    if action == "published":
                        return engine.published_page(sid, request.query_params.get("after", ""))
                    if action == "files":
                        raw_limit = request.query_params.get("limit", "256")
                        check(raw_limit.isdecimal(), "bad_number")
                        return engine.resume_page(
                            sid, request.query_params.get("after", ""), int(raw_limit)
                        )
                    if action == "batches" and bid:
                        result = engine.receipt(sid, bid)
                        check(result is not None, "not_found", 404)
                        return result
                if request.method == "POST":
                    if action == "batches" and bid:
                        return await self.receive(request, sid, bid)
                    data = await json_body(request)
                    if action == "status":
                        return engine.range_status(sid, data)
                    if action == "resume":
                        return await engine.resume(sid)
                    if action == "commit":
                        return await engine.commit(sid, data)
                    if action == "publish":
                        return await engine.publish_from_client(sid)
                    if action == "cancel":
                        return await engine.cancel(sid)
                    if action == "properties":
                        return await engine.set_properties(sid, data)
                    if action == "answers":
                        return await engine.answer(sid, data.get("id"), data.get("choice"))
                raise ProtocolError(404, "not_found")
            except Exception as exc:
                return self.error(
                    engine.record_failure(sid, exc, phase)
                    if authenticated
                    else normalize_error(exc, phase)
                )

        @self.router.websocket("/mfup/control")
        async def control(ws: WebSocket):
            await ws.accept()
            unsubscribe = None
            tasks = []
            queue = asyncio.Queue(maxsize=1)

            def update(state):
                if queue.full():
                    queue.get_nowait()
                queue.put_nowait(state)

            try:
                message = await asyncio.wait_for(ws.receive_json(), 5)
                check(message.get("type") == "subscribe", "bad_message")
                engine.authenticate(message.get("id"), message.get("token"))
                unsubscribe = engine.subscribe(message["id"], update)

                async def send():
                    while True:
                        await ws.send_json(await queue.get())

                async def receive():
                    while True:
                        message = await ws.receive()
                        if message["type"] == "websocket.disconnect":
                            return

                tasks = [asyncio.create_task(send()), asyncio.create_task(receive())]
                await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            except Exception:
                with contextlib.suppress(Exception):
                    await ws.close(code=1008)
            finally:
                if unsubscribe:
                    unsubscribe()
                for task in tasks:
                    task.cancel()
                with CancelScope(shield=True):
                    await asyncio.gather(*tasks, return_exceptions=True)

    async def startup(self):
        if self._sweeper is not None or not self.config.sweep_interval_ms:
            return

        async def sweep():
            while True:
                await asyncio.sleep(self.config.sweep_interval_ms / 1000)
                try:
                    await self.engine.sweep()
                except Exception as exc:
                    self.engine.report("sweep", "", exc)

        self._sweeper = asyncio.create_task(sweep())

    async def shutdown(self):
        if self._sweeper:
            self._sweeper.cancel()
            await asyncio.gather(self._sweeper, return_exceptions=True)
            self._sweeper = None
        await self.engine.close()

    @asynccontextmanager
    async def lifespan(self, app):
        await self.startup()
        try:
            yield
        finally:
            await self.shutdown()

    @staticmethod
    def error(exc):
        return JSONResponse(
            dict(error=exc.code, message=str(exc), retryable=exc.retryable, phase=exc.phase),
            status_code=exc.status,
        )

    async def receive(self, request, sid, bid):
        epoch = request.headers.get("x-mfup-epoch", "")
        check(epoch.isdecimal(), "bad_epoch")
        task = asyncio.current_task()
        batch = await self.engine.begin(sid, integer(int(epoch)), bid, task.cancel)
        output = None
        failed = False
        try:
            content_type, params = parse_options_header(request.headers.get("content-type", ""))
            check(
                content_type == b"multipart/form-data"
                and 0 < len(params.get(b"boundary", b"")) <= 200,
                "bad_multipart",
            )
            events = []
            callbacks = {}
            for name in ("part_begin", "header_end", "headers_finished", "part_end", "end"):
                callbacks["on_" + name] = lambda name=name: events.append((name,))
            for name in ("header_field", "header_value", "part_data"):
                callbacks["on_" + name] = lambda data, start, end, name=name: events.append(
                    (name, data, start, end)
                )
            parser = MultipartParser(params[b"boundary"], callbacks)
            manifest_seen = ended = False
            cached = None
            count = total = length = header_size = 0
            mode = ""
            headers = {}
            field, value, manifest = bytearray(), bytearray(), bytearray()
            expected = 0
            stream = request.stream().__aiter__()
            while True:
                try:
                    chunk = await asyncio.wait_for(stream.__anext__(), 30)
                except StopAsyncIteration:
                    break
                except asyncio.TimeoutError:
                    raise ProtocolError(408, "request_timeout") from None
                total += len(chunk)
                check(
                    total <= self.engine.limits["batchBytes"] + 1024 * 1024, "body_too_large", 413
                )
                parser.write(chunk)
                for event in events:
                    name = event[0]
                    if name == "part_begin":
                        headers = {}
                        field, value = bytearray(), bytearray()
                        header_size = 0
                    elif name in ("header_field", "header_value"):
                        data = memoryview(event[1])[event[2] : event[3]]
                        header_size += len(data)
                        check(header_size <= 8192, "headers_too_large", 413)
                        (field if name == "header_field" else value).extend(data)
                    elif name == "header_end":
                        headers[bytes(field).lower()] = bytes(value)
                        check(len(headers) <= 32, "headers_too_large", 413)
                        field, value = bytearray(), bytearray()
                    elif name == "headers_finished":
                        _, options = parse_options_header(headers.get(b"content-disposition", b""))
                        part_name = options.get(b"name", b"")
                        if part_name == b"manifest" and b"filename" not in options:
                            check(not manifest_seen and count == 0, "bad_manifest")
                            manifest_seen = True
                            mode = "manifest"
                        else:
                            check(
                                manifest_seen and "manifest" in batch or cached is not None,
                                "bad_manifest",
                            )
                            check(
                                part_name == str(count).encode() and b"filename" in options,
                                "bad_part",
                            )
                            check(count < self.engine.limits["maxParts"], "too_many_parts", 413)
                            mode, length = "file", 0
                            if cached is None:
                                check(count < len(batch["entries"]), "unexpected_part")
                                entry = batch["entries"][count]
                                expected = entry["item"][4]
                                if not entry["skip"]:
                                    output = open(entry["filePath"], "r+b")
                                    output.seek(entry["item"][3])
                            count += 1
                    elif name == "part_data":
                        data = memoryview(event[1])[event[2] : event[3]]
                        if mode == "manifest":
                            manifest.extend(data)
                            check(len(manifest) <= 262144, "bad_manifest")
                        else:
                            length += len(data)
                            if cached is None:
                                check(length <= expected, "part_size_mismatch")
                                if output:
                                    output.write(data)
                    elif name == "part_end":
                        if mode == "manifest":
                            try:
                                raw = json.loads(manifest)
                            except ValueError:
                                raise ProtocolError(400, "bad_manifest")
                            cached = await self.engine.prepare(batch, raw)
                        else:
                            if output:
                                output.close()
                                output = None
                            if cached is None:
                                check(length == expected, "part_size_mismatch")
                    elif name == "end":
                        ended = True
                events.clear()
            parser.finalize()
            check(ended and manifest_seen, "incomplete_multipart")
            if cached is not None:
                return cached
            check(count == len(batch["manifest"]["files"]), "missing_part")
            return await self.engine.complete(batch)
        except (
            ClientDisconnect,
            asyncio.CancelledError,
            ValueError,
            KeyError,
        ):
            failed = True
            raise ProtocolError(400, "incomplete_multipart")
        except BaseException:
            failed = True
            raise
        finally:
            try:
                if output:
                    output.close()
            except OSError:
                if not failed:
                    raise
            finally:
                self.engine.end(batch)


def create_app(base_dir, authorize=None, **options):
    mfup = MfupEngine(base_dir, authorize, **options)
    app = FastAPI(lifespan=mfup.lifespan)
    app.include_router(mfup.router, prefix=mfup.config.prefix)
    app.state.mfup = mfup
    return app
