import * as fs from "node:fs";
import * as fsp from "node:fs/promises";
import * as path from "node:path";
import {
  createHash,
  randomBytes,
  randomUUID,
  timingSafeEqual,
} from "node:crypto";
import {
  Store,
  normalizeError,
  type Failure,
  Mutex,
  ProtocolError,
  check,
  identifier,
  integer,
  relativePath,
  publishedDirectory,
} from "./store.js";

import type {
  Options,
  Limits,
  AuthResult,
  CommitEvent,
  RequestContext,
  StagedFile,
} from "./contracts.js";
export type { Options, Limits, AuthResult } from "./contracts.js";

interface SessionRow {
  id: string;
  token: string;
  epoch: number;
  state: string;
  target: string;
  context: string;
  meta: string;
  max_files: number;
  max_bytes: number;
  expires: number;
  published: string;
  base_dir: string;
  map_files: number;
  mapped: number;
  auto_publish: number;
  client_publish: number;
  hook_status: string;
  overwrite: number;
  conflict: number;
  failure: string;
}
interface NodeRow {
  path: string;
  kind: "file" | "directory";
  size: number;
  mtime: number;
  destination: string;
  done: number;
}
interface Active {
  done: Promise<void>;
  finish: () => void;
  abort: () => void;
}
interface Runtime {
  mutex: Mutex;
  active: Set<Active>;
  batches: Set<string>;
  ranges: Set<string>;
  listeners: Set<(state: unknown) => void>;
  transition: boolean;
  overwrite?: boolean;
  conflict?: boolean;
  failure?: Failure | null;
  baseDir?: string;
  publishedDir?: string;
  processing?: Promise<unknown>;
  planning?: Promise<unknown>;
  cancelling?: Promise<unknown>;
}
export type Part = [string, number, number, number, number];
export interface Manifest {
  files: Part[];
  dirs: string[];
}
export interface Batch {
  id: string;
  sid: string;
  epoch: number;
  active: Active;
  manifest?: Manifest;
  signature?: string;
  entries?: { item: Part; filePath: string; skip: boolean }[];
}

export class Engine {
  readonly store: Store;
  readonly limits: Limits;
  readonly options: Options;
  private runtimes = new Map<string, Runtime>();
  private timer?: ReturnType<typeof setInterval>;
  private closed = false;
  private publishLock = new Mutex();
  private cleanups = new Set<Promise<void>>();
  constructor(options: Options) {
    this.options = { ...options, baseDir: path.resolve(options.baseDir) };
    this.limits = {
      concurrency: 6,
      maxParts: 128,
      batchBytes: 32 * 1024 ** 2,
      partBytes: 16 * 1024 ** 2,
      ...options.limits,
    };
    check(
      this.limits.concurrency >= 1 &&
        this.limits.concurrency <= 6 &&
        Object.values(this.limits).every(
          (n) => Number.isSafeInteger(n) && n > 0,
        ) &&
        this.limits.partBytes <= this.limits.batchBytes &&
        this.limits.maxParts <= 1024,
      "bad_limits",
    );
    check(typeof options.authorize === "function", "missing_authorize");
    for (const value of [options.autoPublish, options.clientPublish])
      check(value === undefined || typeof value === "boolean", "bad_config");
    for (const value of [
      options.maxMetaBytes ?? 16384,
      options.maxContextBytes ?? 65536,
    ])
      integer(value);
    this.store = new Store(this.options.baseDir);
    try {
      this.store.run(
        "INSERT OR IGNORE INTO settings(key,value) VALUES('partBytes',?)",
        this.limits.partBytes,
      );
      check(
        this.store.get<{ value: number }>(
          "SELECT value FROM settings WHERE key='partBytes'",
        )?.value === this.limits.partBytes,
        "part_size_in_use",
      );
    } catch (error) {
      this.store.close();
      throw error;
    }
    fs.mkdirSync(path.join(this.options.baseDir, "staging"), {
      recursive: true,
    });
    fs.mkdirSync(path.join(this.options.baseDir, "published"), {
      recursive: true,
    });
    if (options.sweepIntervalMs !== 0) {
      this.timer = setInterval(() => {
        void this.sweep().catch(() => {});
      }, options.sweepIntervalMs ?? 60000);
      this.timer.unref();
    }
  }
  private runtime(id: string): Runtime {
    let r = this.runtimes.get(id);
    if (!r) {
      r = {
        mutex: new Mutex(),
        active: new Set(),
        batches: new Set(),
        ranges: new Set(),
        listeners: new Set(),
        transition: false,
      };
      this.runtimes.set(id, r);
    }
    return r;
  }
  row(id: string): SessionRow {
    identifier(id);
    const row = this.store.get<SessionRow>(
      "SELECT * FROM sessions WHERE id=?",
      id,
    );
    if (!row) throw new ProtocolError(404, "not_found");
    this.runtime(id).baseDir = row.base_dir || this.options.baseDir;
    this.runtime(id).overwrite = Boolean(row.overwrite);
    this.runtime(id).conflict = Boolean(row.conflict);
    return row;
  }
  authenticate(id: string, token: string) {
    const row = this.row(id),
      a = Buffer.from(row.token),
      b = Buffer.from(token);
    check(a.length === b.length && timingSafeEqual(a, b), "denied", 403);
    return row;
  }
  private touch(id: string) {
    this.store.run(
      "UPDATE sessions SET expires=? WHERE id=?",
      Date.now() + (this.options.ttlMs ?? 86400000),
      id,
    );
  }
  private report(hook: string, sessionId: string, error: unknown) {
    try {
      if (this.options.onError)
        this.options.onError({ hook, sessionId, error });
      else console.error(`MFUP ${hook} failed (${sessionId})`, error);
    } catch {
      /* Diagnostics must not change session state. */
    }
  }
  async create(
    input: {
      protocol?: unknown;
      targetDir?: unknown;
      meta?: unknown;
      overwrite?: unknown;
    },
    headers: Record<string, string>,
    request: RequestContext = {},
  ) {
    check(!this.closed, "closed", 503);
    check(input.protocol === "MFUP/3", "bad_protocol");
    check(
      input.overwrite === undefined || typeof input.overwrite === "boolean",
      "bad_property",
    );
    const target =
      input.targetDir === undefined || input.targetDir === ""
        ? "uploads"
        : relativePath(input.targetDir);
    const meta = JSON.stringify(input.meta ?? null);
    check(
      Buffer.byteLength(meta) <= (this.options.maxMetaBytes ?? 16384),
      "meta_too_large",
      413,
    );
    const id = randomUUID(),
      token = randomBytes(32).toString("hex");
    let auth: AuthResult | null;
    try {
      auth = await this.options.authorize({
        ...request,
        sessionId: id,
        headers,
        targetDir: target,
        meta: JSON.parse(meta),
      });
    } catch (error) {
      this.report("authorize", id, error);
      throw new ProtocolError(403, "denied");
    }
    check(auth && typeof auth === "object", "denied", 403);
    check(
      auth.baseDir === undefined ||
        (typeof auth.baseDir === "string" && path.isAbsolute(auth.baseDir)),
      "bad_base_dir",
    );
    const base = path.resolve(auth.baseDir ?? this.options.baseDir);
    const targetDir = relativePath(auth.targetDir ?? target);
    const context = JSON.stringify(auth.context ?? {});
    check(
      auth.context === undefined ||
        (auth.context !== null &&
          typeof auth.context === "object" &&
          !Array.isArray(auth.context)),
      "bad_context",
    );
    check(
      Buffer.byteLength(context) <= (this.options.maxContextBytes ?? 65536),
      "context_too_large",
    );
    const auto = auth.autoPublish ?? this.options.autoPublish ?? false;
    const client = auth.clientPublish ?? this.options.clientPublish ?? true;
    check(
      typeof auto === "boolean" && typeof client === "boolean",
      "bad_config",
    );
    const maxFiles = integer(auth.maxFiles ?? 100000),
      maxBytes = integer(auth.maxTotalBytes ?? Number.MAX_SAFE_INTEGER);
    const stage = path.join(base, "staging", id);
    fs.mkdirSync(stage, { recursive: true });
    try {
      this.store.run(
        "INSERT INTO sessions(id,token,epoch,state,target,context,meta,max_files,max_bytes,expires,base_dir,map_files,auto_publish,client_publish,hook_status,overwrite) VALUES(?,?,1,'uploading',?,?,?,?,?,?,?,?,?,?,?,?)",
        id,
        token,
        targetDir,
        context,
        meta,
        maxFiles,
        maxBytes,
        Date.now() + (this.options.ttlMs ?? 86400000),
        base,
        Number(Boolean(this.options.mapFile)),
        Number(auto),
        Number(client),
        this.options.onCommitted ? "pending" : "none",
        Number(input.overwrite ?? false),
      );
    } catch (error) {
      try {
        fs.rmSync(stage, { recursive: true, force: true });
      } catch (cleanupError) {
        this.report("create_cleanup", id, cleanupError);
      }
      throw error;
    }
    this.runtime(id).baseDir = base;
    return { id, token, epoch: 1, limits: this.limits };
  }
  staging(id: string) {
    const base =
      this.runtime(id).baseDir ??
      (this.row(id).base_dir || this.options.baseDir);
    return path.join(base, "staging", identifier(id));
  }
  payload(id: string, file: string) {
    return path.join(
      this.staging(id),
      createHash("sha256").update(file).digest("hex"),
    );
  }
  destination(row: SessionRow, file: string) {
    const r = this.runtime(row.id);
    r.publishedDir ??= publishedDirectory(
      row.base_dir || this.options.baseDir,
      row.target,
    );
    return path.join(r.publishedDir, file);
  }
  getSession(id: string): CommitEvent & { state: string; processing: string } {
    const row = this.row(id),
      snapshot = this.snapshot(id);
    return {
      sessionId: id,
      targetDir: row.target,
      baseDir: row.base_dir || this.options.baseDir,
      stagingDir: this.staging(id),
      files: snapshot.files,
      bytes: snapshot.bytes,
      context: JSON.parse(row.context),
      meta: JSON.parse(row.meta),
      state: row.state,
      processing: row.hook_status,
    };
  }
  *listStaged(id: string): Generator<StagedFile> {
    check(this.row(id).state === "committed", "bad_state", 409);
    let after = "";
    for (;;) {
      const page = this.store.all<NodeRow>(
        "SELECT * FROM nodes WHERE sid=? AND kind='file' AND path>? ORDER BY path LIMIT 256",
        id,
        after,
      );
      for (const file of page)
        yield {
          path: file.path,
          size: file.size,
          mtime: file.mtime,
          localPath: this.payload(id, file.path),
        };
      if (page.length < 256) return;
      after = page.at(-1)!.path;
    }
  }
  openStaged(id: string, file: string) {
    check(this.row(id).state === "committed", "bad_state", 409);
    const node = this.store.get<NodeRow>(
      "SELECT * FROM nodes WHERE sid=? AND path=? AND kind='file'",
      id,
      relativePath(file),
    );
    check(node, "not_found", 404);
    return fs.createReadStream(this.payload(id, file));
  }
  snapshot(id: string) {
    const row = this.row(id);
    const totals = this.store.get<{ files: number; bytes: number }>(
      "SELECT COUNT(*) AS files, COALESCE(SUM(size),0) AS bytes FROM nodes WHERE sid=? AND kind='file'",
      id,
    )!;
    return {
      id,
      epoch: row.epoch,
      state: row.state,
      ...totals,
      asks: this.questions(id),
      published: JSON.parse(row.published) as string[],
      clientPublish: Boolean(row.client_publish),
      processing: row.hook_status,
      overwrite: Boolean(row.overwrite),
      overwriteRequired: this.needsOverwrite(row),
      error:
        this.runtime(id).failure ??
        (row.failure ? (JSON.parse(row.failure) as Failure) : null),
    };
  }
  private needsOverwrite(row: SessionRow) {
    return (
      !row.overwrite &&
      Boolean(row.conflict) &&
      !["cancelled", "published"].includes(row.state)
    );
  }
  questions(id: string) {
    return this.needsOverwrite(this.row(id))
      ? [
          {
            id: "overwrite",
            message: "overwrite_required",
            choices: ["overwrite", "cancel"],
            answer: null,
          },
        ]
      : [];
  }
  recordFailure(id: string, error: unknown, phase: string) {
    const e = normalizeError(error, phase);
    if (e.status < 500) return e;
    this.report(phase, id, error);
    try {
      const row = this.row(id),
        r = this.runtime(id);
      if (row.state === "cancelled") return e;
      const failure: Failure = {
        code: e.code,
        status: e.status,
        phase,
        retryable: false,
      };
      r.failure ??= row.failure ? JSON.parse(row.failure) : failure;
      try {
        this.store.run(
          "UPDATE sessions SET failure=? WHERE id=? AND failure=''",
          JSON.stringify(r.failure),
          id,
        );
      } catch {}
      try {
        this.emit(id);
      } catch {}
    } catch {}
    return e;
  }
  private clearFailure(id: string) {
    const row = this.row(id);
    if (row.failure)
      this.store.run("UPDATE sessions SET failure='' WHERE id=?", id);
    this.runtime(id).failure = null;
  }
  subscribe(id: string, listener: (state: unknown) => void) {
    this.row(id);
    const runtime = this.runtime(id);
    runtime.listeners.add(listener);
    listener(this.snapshot(id));
    return () => runtime.listeners.delete(listener);
  }
  private emit(id: string) {
    const runtime = this.runtime(id);
    if (!runtime.listeners.size) return;
    const value = this.snapshot(id);
    for (const fn of runtime.listeners) {
      try {
        fn(value);
      } catch {}
    }
  }
  private conflicts(id: string, row: SessionRow, name: string, kind: string) {
    const r = this.runtime(id);
    if (r.overwrite || r.conflict || row.overwrite || row.conflict) return;
    const pieces = name.split("/");
    for (let i = 1; i <= pieces.length; i++) {
      const relative = pieces.slice(0, i).join("/"),
        dest = this.destination(row, relative);
      if (
        fs.existsSync(dest) &&
        ((i === pieces.length && kind === "file") ||
          !fs.statSync(dest).isDirectory())
      ) {
        this.store.run(
          "UPDATE sessions SET conflict=1 WHERE id=? AND overwrite=0",
          id,
        );
        r.conflict = true;
        return;
      }
    }
  }
  async setProperties(id: string, properties: { overwrite?: unknown }) {
    check(
      properties && typeof properties.overwrite === "boolean",
      "bad_property",
    );
    const r = this.runtime(id);
    await r.mutex.run(() => {
      const row = this.row(id);
      check(row.state !== "cancelled" && !r.transition, "bad_state", 409);
      check(
        !row.overwrite || properties.overwrite,
        "overwrite_already_approved",
        409,
      );
      if (Boolean(row.overwrite) !== properties.overwrite) {
        this.store.run(
          "UPDATE sessions SET overwrite=? WHERE id=?",
          Number(properties.overwrite),
          id,
        );
        r.overwrite = properties.overwrite as boolean;
        this.touch(id);
        this.emit(id);
      }
    });
    const row = this.row(id);
    if (
      row.auto_publish &&
      ["committed", "publishing"].includes(row.state) &&
      ["none", "done"].includes(row.hook_status) &&
      row.overwrite
    ) {
      try {
        await this.publish(id);
      } catch (e) {
        if (!(e instanceof ProtocolError && e.code === "answers_required"))
          throw e;
      }
    }
    return this.snapshot(id);
  }
  async answer(id: string, question: string, choice: string) {
    check(question === "overwrite", "unknown_question", 404);
    check(["overwrite", "cancel"].includes(choice), "bad_choice");
    return choice === "cancel"
      ? this.cancel(id)
      : this.setProperties(id, { overwrite: true });
  }
  async begin(
    id: string,
    epoch: number,
    batchId: string,
    abort: () => void,
  ): Promise<Batch> {
    identifier(batchId);
    const r = this.runtime(id);
    return r.mutex.run(() => {
      const row = this.row(id);
      check(!r.transition && row.state === "uploading", "bad_state", 409);
      check(row.epoch === epoch, "stale_epoch", 409);
      check(
        r.active.size < this.limits.concurrency && !r.batches.has(batchId),
        "busy",
        429,
      );
      let finish!: () => void;
      const done = new Promise<void>((resolve) => {
        finish = resolve;
      });
      const active = { done, finish, abort };
      r.active.add(active);
      r.batches.add(batchId);
      this.touch(id);
      return { sid: id, epoch, id: batchId, active };
    });
  }
  receipt(id: string, batchId: string) {
    const row = this.store.get<{ receipt: string }>(
      "SELECT receipt FROM batches WHERE sid=? AND id=?",
      id,
      identifier(batchId),
    );
    return row ? JSON.parse(row.receipt) : null;
  }
  async prepare(batch: Batch, raw: unknown) {
    check(raw && typeof raw === "object", "bad_manifest");
    const data = raw as Manifest;
    check(
      Array.isArray(data.files) &&
        Array.isArray(data.dirs) &&
        data.files.length + data.dirs.length > 0 &&
        data.files.length + data.dirs.length <= this.limits.maxParts,
      "bad_manifest",
    );
    const files: Part[] = data.files.map((item) => {
      check(Array.isArray(item) && item.length === 5, "bad_manifest");
      const p: Part = [
        relativePath(item[0]),
        integer(item[1]),
        integer(item[2]),
        integer(item[3]),
        integer(item[4]),
      ];
      check(
        p[3] % this.limits.partBytes === 0 &&
          p[3] <= p[1] &&
          p[4] === Math.min(this.limits.partBytes, p[1] - p[3]) &&
          (p[4] > 0 || p[1] === 0),
        "bad_range",
      );
      return p;
    });
    const dirs = data.dirs.map(relativePath);
    check(
      files.reduce((n, p) => n + p[4], 0) <= this.limits.batchBytes,
      "batch_too_large",
      413,
    );
    const signature = createHash("sha256")
      .update(JSON.stringify({ files, dirs }))
      .digest("hex");
    const row = this.row(batch.sid);
    return this.runtime(batch.sid).mutex.run(() => {
      const r = this.runtime(batch.sid),
        current = this.row(batch.sid);
      check(current.epoch === batch.epoch, "stale_epoch", 409);
      check(current.state === "uploading" && !r.transition, "bad_state", 409);
      const old = this.store.get<{ signature: string; receipt: string }>(
        "SELECT signature,receipt FROM batches WHERE sid=? AND id=?",
        batch.sid,
        batch.id,
      );
      if (old) {
        check(old.signature === signature, "batch_conflict", 409);
        return JSON.parse(old.receipt);
      }
      const keys = files.map((p) => `${p[0]}\0${p[3]}`);
      check(
        new Set(keys).size === keys.length &&
          keys.every((k) => !r.ranges.has(k)),
        "range_busy",
        409,
      );
      const existing = this.store.all<NodeRow>(
        "SELECT * FROM nodes WHERE sid=?",
        batch.sid,
      );
      const nodes = new Map(existing.map((n) => [n.path, n]));
      const destinations = new Map(
        existing.map((n) => [n.destination.toLowerCase(), n]),
      );
      const spelling = new Map<string, string>();
      const checkSpelling = (name: string) => {
        const pieces = name.split("/");
        for (let i = 1; i <= pieces.length; i++) {
          const prefix = pieces.slice(0, i).join("/"),
            key = prefix.toLowerCase();
          check(
            !spelling.has(key) || spelling.get(key) === prefix,
            "path_conflict",
            409,
          );
          spelling.set(key, prefix);
        }
      };
      for (const n of existing) checkSpelling(n.destination);
      const additions: NodeRow[] = [];
      const add = (
        name: string,
        kind: NodeRow["kind"],
        size: number,
        mtime: number,
        destination: string,
      ) => {
        const previous = nodes.get(name);
        if (previous) {
          check(
            previous.kind === kind &&
              previous.size === size &&
              previous.mtime === mtime &&
              previous.destination === destination,
            "file_changed",
            409,
          );
          return;
        }
        checkSpelling(destination);
        const lower = destination.toLowerCase();
        const conflict = destinations.get(lower);
        check(!conflict, "path_conflict", 409);
        for (let parent = lower; parent.includes("/");) {
          parent = parent.slice(0, parent.lastIndexOf("/"));
          check(
            destinations.get(parent)?.kind !== "file",
            "path_conflict",
            409,
          );
        }
        if (kind === "file")
          check(
            ![...destinations.keys()].some((p) => p.startsWith(lower + "/")),
            "path_conflict",
            409,
          );
        const n: NodeRow = {
          path: name,
          kind,
          size,
          mtime,
          destination,
          done: 0,
        };
        nodes.set(name, n);
        destinations.set(lower, n);
        additions.push(n);
      };
      for (const dir of dirs) add(dir, "directory", 0, 0, dir);
      for (const p of files) add(p[0], "file", p[1], p[2], p[0]);
      const fileNodes = [...nodes.values()].filter((n) => n.kind === "file");
      check(
        nodes.size <= row.max_files * 4 + 1024 &&
          fileNodes.length <= row.max_files &&
          fileNodes.reduce((n, f) => n + f.size, 0) <= row.max_bytes,
        "quota_exceeded",
        413,
      );
      this.store.transaction(() => {
        for (const n of additions) {
          this.store.run(
            "INSERT INTO nodes(sid,path,kind,size,mtime,destination) VALUES(?,?,?,?,?,?)",
            batch.sid,
            n.path,
            n.kind,
            n.size,
            n.mtime,
            n.destination,
          );
          if (!row.map_files)
            this.conflicts(batch.sid, row, n.destination, n.kind);
        }
      });
      batch.manifest = { files, dirs };
      batch.signature = signature;
      batch.entries = files.map((item) => {
        const filePath = this.payload(batch.sid, item[0]);
        if (!fs.existsSync(filePath)) {
          const fd = fs.openSync(filePath, "wx");
          fs.closeSync(fd);
        }
        return {
          item,
          filePath,
          skip: !!this.store.get(
            "SELECT 1 FROM parts WHERE sid=? AND path=? AND offset=?",
            batch.sid,
            item[0],
            item[3],
          ),
        };
      });
      for (const key of keys) r.ranges.add(key);
      this.emit(batch.sid);
      return null;
    });
  }
  async complete(batch: Batch) {
    return this.runtime(batch.sid).mutex.run(() => {
      check(batch.manifest && batch.signature, "bad_manifest");
      const row = this.row(batch.sid),
        r = this.runtime(batch.sid);
      check(
        row.state === "uploading" && !r.transition && row.epoch === batch.epoch,
        "bad_state",
        409,
      );
      const receipt = {
        id: batch.id,
        parts: batch.manifest.files.length,
        bytes: batch.manifest.files.reduce((sum, p) => sum + p[4], 0),
      };
      this.store.transaction(() => {
        for (const p of batch.manifest!.files)
          this.store.run(
            "INSERT OR IGNORE INTO parts(sid,path,offset,length) VALUES(?,?,?,?)",
            batch.sid,
            p[0],
            p[3],
            p[4],
          );
        this.store.run(
          "INSERT INTO batches(sid,id,signature,receipt) VALUES(?,?,?,?)",
          batch.sid,
          batch.id,
          batch.signature!,
          JSON.stringify(receipt),
        );
        this.touch(batch.sid);
      });
      return receipt;
    });
  }
  end(batch: Batch) {
    const r = this.runtime(batch.sid);
    for (const p of batch.manifest?.files ?? [])
      r.ranges.delete(`${p[0]}\0${p[3]}`);
    r.batches.delete(batch.id);
    r.active.delete(batch.active);
    batch.active.finish();
  }
  async resume(id: string) {
    const r = this.runtime(id);
    const active = await r.mutex.run(() => {
      const row = this.row(id);
      check(
        !r.transition &&
          ["uploading", "committed", "publishing", "published"].includes(
            row.state,
          ),
        "bad_state",
        409,
      );
      r.transition = true;
      return [...r.active];
    });
    await Promise.all(active.map((a) => a.done));
    return r.mutex.run(() => {
      try {
        this.clearFailure(id);
        this.store.run("UPDATE sessions SET epoch=epoch+1 WHERE id=?", id);
        this.touch(id);
        return { ...this.snapshot(id), limits: this.limits };
      } finally {
        r.transition = false;
      }
    });
  }
  resumePage(id: string, after = "", limit = 256) {
    this.row(id);
    limit = Math.max(1, Math.min(integer(limit), 256));
    const files = this.store.all<NodeRow>(
      "SELECT * FROM nodes WHERE sid=? AND kind='file' AND path>? ORDER BY path LIMIT ?",
      id,
      after,
      limit,
    );
    return {
      files: files.map((f) => ({
        path: f.path,
        size: f.size,
        mtime: f.mtime,
        offsets: this.store
          .all<{ offset: number }>(
            "SELECT offset FROM parts WHERE sid=? AND path=? ORDER BY offset",
            id,
            f.path,
          )
          .map((p) => p.offset),
      })),
      next: files.length === limit ? files.at(-1)!.path : null,
    };
  }
  async commit(
    id: string,
    totals: { files: number; dirs: number; bytes: number },
  ) {
    let newlyCommitted = false;
    await this.runtime(id).mutex.run(() => {
      const row = this.row(id),
        r = this.runtime(id);
      if (["committed", "published"].includes(row.state))
        return this.snapshot(id);
      check(
        row.state === "uploading" && !r.transition && r.active.size === 0,
        "busy",
        409,
      );
      const files = this.store.all<NodeRow>(
        "SELECT * FROM nodes WHERE sid=? AND kind='file'",
        id,
      );
      const dirs = this.store.get<{ n: number }>(
        "SELECT COUNT(*) AS n FROM nodes WHERE sid=? AND kind='directory'",
        id,
      )!.n;
      check(
        integer(totals.files) === files.length &&
          integer(totals.dirs) === dirs &&
          integer(totals.bytes) === files.reduce((n, f) => n + f.size, 0),
        "scan_mismatch",
        409,
      );
      for (const f of files) {
        const p = this.store.get<{ n: number; bytes: number }>(
          "SELECT COUNT(*) AS n,COALESCE(SUM(length),0) AS bytes FROM parts WHERE sid=? AND path=?",
          id,
          f.path,
        )!;
        check(
          p.n === Math.max(1, Math.ceil(f.size / this.limits.partBytes)) &&
            p.bytes === f.size,
          "incomplete",
          409,
        );
      }
      this.store.run("UPDATE sessions SET state='committed' WHERE id=?", id);
      this.touch(id);
      newlyCommitted = true;
      this.emit(id);
      return this.snapshot(id);
    });
    if (newlyCommitted) return this.retryCommitted(id);
    if (this.runtime(id).processing) await this.runtime(id).processing;
    return this.snapshot(id);
  }
  async retryCommitted(id: string) {
    const r = this.runtime(id);
    if (r.processing) {
      await r.processing;
      return this.snapshot(id);
    }
    const task = (async () => {
      const row = await r.mutex.run(() => {
        const row = this.row(id);
        check(row.state === "committed" && !r.transition, "bad_state", 409);
        if (!["none", "done"].includes(row.hook_status)) {
          this.store.run(
            "UPDATE sessions SET hook_status='running' WHERE id=?",
            id,
          );
          this.emit(id);
        }
        return row;
      });
      let auto = Boolean(row.auto_publish);
      if (!["none", "done"].includes(row.hook_status)) {
        try {
          check(this.options.onCommitted, "hook_unavailable", 503);
          const decision = await this.options.onCommitted(this.getSession(id));
          if (this.row(id).state === "cancelled") return;
          check(
            decision === undefined || typeof decision === "boolean",
            "bad_hook_result",
          );
          if (decision !== undefined) auto = decision;
          this.store.run(
            "UPDATE sessions SET hook_status='done',auto_publish=? WHERE id=?",
            Number(auto),
            id,
          );
        } catch (error) {
          if (this.row(id).state === "cancelled") return;
          this.store.run(
            "UPDATE sessions SET hook_status='failed' WHERE id=?",
            id,
          );
          this.report("onCommitted", id, error);
          this.emit(id);
          return;
        }
      }
      if (auto && this.row(id).state !== "published") {
        try {
          await this.publish(id);
        } catch (error) {
          if (!(
            error instanceof ProtocolError && error.code === "answers_required"
          ))
            this.report("publish", id, error);
        }
      }
      this.emit(id);
    })();
    r.processing = task;
    try {
      await task;
      return this.snapshot(id);
    } finally {
      r.processing = undefined;
    }
  }
  private async preparePlan(id: string) {
    const r = this.runtime(id);
    if (r.planning) {
      await r.planning;
      return this.snapshot(id);
    }
    const task = (async () => {
      const row = await r.mutex.run(() => {
        const row = this.row(id);
        check(
          ["committed", "publishing", "published"].includes(row.state) &&
            !r.transition,
          "bad_state",
          409,
        );
        return row;
      });
      if (row.state === "published") return;
      if (row.map_files && !row.mapped) {
        check(this.options.mapFile, "hook_unavailable", 503);
        const files = this.store.all<NodeRow>(
          "SELECT * FROM nodes WHERE sid=? AND kind='file' ORDER BY path",
          id,
        );
        const plan: { path: string; destination: string }[] = [];
        try {
          for (const f of files) {
            const dest = await this.options.mapFile({
              sessionId: id,
              path: f.path,
              name: f.path.split("/").at(-1)!,
              size: f.size,
              targetDir: row.target,
              context: JSON.parse(row.context),
              meta: JSON.parse(row.meta),
            });
            plan.push({
              path: f.path,
              destination: relativePath(dest === null ? f.path : dest),
            });
          }
          const names = new Set<string>(),
            spelling = new Map<string, string>();
          for (const f of plan) {
            const lower = f.destination.toLowerCase();
            check(!names.has(lower), "mapping_error", 409);
            names.add(lower);
            const parts = f.destination.split("/");
            for (let i = 1; i <= parts.length; i++) {
              const prefix = parts.slice(0, i).join("/"),
                key = prefix.toLowerCase();
              check(
                !spelling.has(key) || spelling.get(key) === prefix,
                "mapping_error",
                409,
              );
              spelling.set(key, prefix);
            }
          }
          for (const f of plan) {
            const parts = f.destination.toLowerCase().split("/");
            for (let i = 1; i < parts.length; i++)
              check(
                !names.has(parts.slice(0, i).join("/")),
                "mapping_error",
                409,
              );
          }
        } catch (error) {
          this.report("mapFile", id, error);
          throw new ProtocolError(409, "mapping_error");
        }
        await r.mutex.run(() => {
          check(
            this.row(id).state === "committed" && !r.transition,
            "bad_state",
            409,
          );
          this.store.transaction(() => {
            for (const f of plan)
              this.store.run(
                "UPDATE nodes SET destination=? WHERE sid=? AND path=?",
                f.destination,
                id,
                f.path,
              );
            this.store.run("UPDATE sessions SET mapped=1 WHERE id=?", id);
          });
        });
      }
    })();
    r.planning = task;
    try {
      await task;
      return this.snapshot(id);
    } finally {
      r.planning = undefined;
    }
  }
  async preparePublish(id: string) {
    await this.preparePlan(id);
    const r = this.runtime(id);
    await r.mutex.run(() => {
      const current = this.row(id);
      const nodes = this.store.all<NodeRow>(
        "SELECT * FROM nodes WHERE sid=?",
        id,
      );
      for (const n of nodes) {
        if (current.map_files && n.kind === "directory") continue;
        if (
          !n.done &&
          (n.kind === "directory" || fs.existsSync(this.payload(id, n.path)))
        )
          this.conflicts(id, current, n.destination, n.kind);
      }
      this.touch(id);
      this.emit(id);
    });
    return this.snapshot(id);
  }
  async publishFromClient(id: string) {
    const allowed = () => {
      const row = this.row(id);
      check(Boolean(row.client_publish), "server_publish_only", 403);
      check(
        ["none", "done"].includes(row.hook_status),
        "processing_required",
        409,
      );
    };
    allowed();
    return this.publish(id, allowed);
  }
  async publish(id: string, beforeMove?: () => void) {
    try {
      this.clearFailure(id);
      return await this.publishImpl(id, beforeMove);
    } catch (error) {
      throw this.recordFailure(id, error, "publish");
    }
  }
  private async publishImpl(id: string, beforeMove?: () => void) {
    await this.preparePlan(id);

    return this.publishLock.run(() =>
      this.runtime(id).mutex.run(async () => {
        beforeMove?.();
        const row = this.row(id),
          r = this.runtime(id);
        if (row.state === "published") return this.snapshot(id);
        check(
          ["committed", "publishing"].includes(row.state) &&
            !r.transition &&
            r.active.size === 0,
          "bad_state",
          409,
        );
        const nodes = this.store
          .all<NodeRow>("SELECT * FROM nodes WHERE sid=? ORDER BY path", id)
          .filter((n) => !row.map_files || n.kind === "file");
        for (const n of nodes.filter((n) => !n.done)) {
          const present =
            n.kind === "directory" || fs.existsSync(this.payload(id, n.path));
          if (!present && row.state === "committed")
            throw new ProtocolError(
              503,
              "storage_unavailable",
              "Accepted file is unavailable",
              false,
              "publish",
            );
          if (present) this.conflicts(id, row, n.destination, n.kind);
        }
        if (this.questions(id).some((q) => q.answer === null)) {
          this.emit(id);
          throw new ProtocolError(409, "answers_required");
        }
        const ensureDirectory = async (name: string) => {
          const pieces = name.split("/").filter(Boolean);
          for (let i = 1; i <= pieces.length; i++) {
            const relative = pieces.slice(0, i).join("/"),
              dest = this.destination(row, relative);
            if (fs.existsSync(dest) && !fs.statSync(dest).isDirectory()) {
              check(Boolean(row.overwrite), "answers_required", 409);
              await fsp.rm(dest, { force: true });
            }
            await fsp.mkdir(dest, { recursive: true });
          }
        };
        this.store.run("UPDATE sessions SET state='publishing' WHERE id=?", id);
        for (const n of nodes) {
          if (n.done) continue;
          const dest = this.destination(row, n.destination);
          if (n.kind === "directory") {
            await ensureDirectory(n.destination);
          } else {
            const parent = n.destination.includes("/")
              ? n.destination.slice(0, n.destination.lastIndexOf("/"))
              : "";
            await fsp.mkdir(this.destination(row, ""), { recursive: true });
            await ensureDirectory(parent);
            const source = this.payload(id, n.path);
            if (fs.existsSync(source)) {
              if (fs.existsSync(dest)) {
                check(Boolean(row.overwrite), "answers_required", 409);
                if (fs.statSync(dest).isDirectory())
                  await fsp.rm(dest, { recursive: true, force: true });
              }
              await fsp.rename(source, dest);
            } else check(fs.existsSync(dest), "missing_payload", 409);
          }
        }
        const published = nodes
          .filter((n) => n.kind === "file")
          .map((n) => n.destination);
        // The saved plan plus source/destination presence recover interrupted renames.
        // Finish metadata in one transaction, avoiding a WAL commit per file.
        this.store.transaction(() => {
          this.store.run("UPDATE nodes SET done=1 WHERE sid=?", id);
          this.store.run(
            "UPDATE sessions SET state='published',published=? WHERE id=?",
            JSON.stringify(published),
            id,
          );
          this.touch(id);
        });
        await fsp.rm(this.staging(id), { recursive: true, force: true });
        this.emit(id);
        return this.snapshot(id);
      }),
    );
  }
  async cancel(id: string) {
    const r = this.runtime(id);
    if (r.cancelling) {
      await r.cancelling;
      return this.snapshot(id);
    }
    const task = this.cancelImpl(id);
    r.cancelling = task;
    try {
      return await task;
    } finally {
      r.cancelling = undefined;
    }
  }
  private async cancelImpl(id: string) {
    const r = this.runtime(id);
    const active = await r.mutex.run(() => {
      const row = this.row(id);
      if (row.state === "published" || row.state === "cancelled") return [];
      check(!r.transition, "busy", 409);
      this.store.run("UPDATE sessions SET state='cancelled' WHERE id=?", id);
      r.transition = true;
      for (const a of r.active) a.abort();
      this.touch(id);
      this.emit(id);
      return [...r.active];
    });
    await Promise.allSettled(active.map((a) => a.done));
    if (this.row(id).state === "published") return this.snapshot(id);
    r.transition = false;
    const cleanup = async () => {
      try {
        await fsp.rm(this.staging(id), { recursive: true, force: true });
      } catch (error) {
        this.report("cancel_cleanup", id, error);
      }
    };
    const callbacks = [r.processing, r.planning].filter(Boolean);
    if (callbacks.length) {
      const task = Promise.allSettled(callbacks).then(cleanup);
      this.cleanups.add(task);
      void task.finally(() => this.cleanups.delete(task));
    } else await cleanup();
    return this.snapshot(id);
  }
  async sweep(now = Date.now()) {
    const candidates = this.store.all<{ id: string }>(
      "SELECT id FROM sessions WHERE expires<=?",
      now,
    );
    let removed = 0;
    for (const { id } of candidates) {
      const r = this.runtime(id);
      await r.mutex.run(async () => {
        const row = this.store.get<SessionRow>(
          "SELECT * FROM sessions WHERE id=?",
          id,
        );
        if (
          !row ||
          row.expires > now ||
          r.active.size ||
          r.transition ||
          r.planning ||
          r.processing ||
          r.listeners.size
        )
          return;
        r.transition = true;
        try {
          await fsp.rm(this.staging(id), { recursive: true, force: true });
          this.store.run("DELETE FROM sessions WHERE id=?", id);
          this.runtimes.delete(id);
          removed++;
        } finally {
          r.transition = false;
        }
      });
    }
    return removed;
  }
  async close() {
    if (this.closed) return;
    this.closed = true;
    clearInterval(this.timer);
    const all = [...this.runtimes.values()].flatMap((r) => [...r.active]);
    for (const a of all) a.abort();
    await Promise.all(all.map((a) => a.done));
    await Promise.allSettled(
      [...this.runtimes.values()].flatMap((r) =>
        [r.processing, r.planning].filter(Boolean),
      ),
    );
    await Promise.allSettled(
      [...this.runtimes.values()].flatMap((r) =>
        r.cancelling ? [r.cancelling] : [],
      ),
    );
    await Promise.allSettled(this.cleanups);
    this.store.close();
  }
}
