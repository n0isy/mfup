import { DatabaseSync, type StatementSync } from "node:sqlite";
import * as fs from "node:fs";
import * as path from "node:path";

export class Store {
  readonly db: DatabaseSync;
  private statements = new Map<string, StatementSync>();
  private closed = false;
  constructor(base: string) {
    fs.mkdirSync(base, { recursive: true });
    this.db = new DatabaseSync(path.join(base, "metadata.sqlite"));
    try {
      this.db
        .exec(`PRAGMA busy_timeout=1000; PRAGMA locking_mode=EXCLUSIVE; PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;
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
        id TEXT NOT NULL, path TEXT NOT NULL, message TEXT NOT NULL, answer TEXT, PRIMARY KEY(sid,id));`);
      const columns = new Set(
        (
          this.db.prepare("PRAGMA table_info(sessions)").all() as {
            name: string;
          }[]
        ).map((c) => c.name),
      );
      for (const [name, spec] of Object.entries({
        declared_files: "INTEGER NOT NULL DEFAULT 0",
        declared_dirs: "INTEGER NOT NULL DEFAULT 0",
        declared_bytes: "INTEGER NOT NULL DEFAULT 0",
        confirmed_bytes: "INTEGER NOT NULL DEFAULT 0",
        overwrite: "INTEGER NOT NULL DEFAULT 0",
        conflict: "INTEGER NOT NULL DEFAULT 0",
        failure: "TEXT NOT NULL DEFAULT ''",
        base_dir: "TEXT NOT NULL DEFAULT ''",
        map_files: "INTEGER NOT NULL DEFAULT 0",
        mapped: "INTEGER NOT NULL DEFAULT 0",
        auto_publish: "INTEGER NOT NULL DEFAULT 0",
        client_publish: "INTEGER NOT NULL DEFAULT 1",
        hook_status: "TEXT NOT NULL DEFAULT 'none'",
      })) {
        if (!columns.has(name))
          this.db.exec(`ALTER TABLE sessions ADD COLUMN ${name} ${spec}`);
      }
      const nodeColumns = new Set(
        this.db
          .prepare("PRAGMA table_info(nodes)")
          .all()
          .map((c) => c.name),
      );
      for (const [name, spec] of Object.entries({
        source_key: "TEXT",
        destination_key: "TEXT",
        mapped: "INTEGER NOT NULL DEFAULT 0",
      }))
        if (!nodeColumns.has(name))
          this.db.exec(`ALTER TABLE nodes ADD COLUMN ${name} ${spec}`);
      if (!nodeColumns.has("source_key")) {
        let afterSid = "",
          afterPath = "";
        const page = this.db.prepare(
          "SELECT sid,path,destination,kind FROM nodes WHERE (sid,path)>(?,?) ORDER BY sid,path LIMIT 256",
        );
        const update = this.db.prepare(
          "UPDATE nodes SET source_key=?,destination_key=? WHERE sid=? AND path=?",
        );
        while (true) {
          const rows = page.all(afterSid, afterPath) as {
            sid: string;
            path: string;
            destination: string;
            kind: string;
          }[];
          if (!rows.length) break;
          this.transaction(() => {
            for (const n of rows)
              update.run(
                n.path.toLowerCase(),
                n.destination.toLowerCase(),
                n.sid,
                n.path,
              );
          });
          afterSid = rows.at(-1)!.sid;
          afterPath = rows.at(-1)!.path;
        }
        this.db.exec(
          "UPDATE nodes SET mapped=1 WHERE sid IN (SELECT id FROM sessions WHERE mapped=1 OR map_files=0)",
        );
        this.db.exec(
          "UPDATE nodes SET destination_key=NULL WHERE kind='directory' AND sid IN (SELECT id FROM sessions WHERE map_files=1)",
        );
      }
      if (!columns.has("declared_files"))
        this.db.exec(
          "UPDATE sessions SET declared_files=(SELECT COUNT(*) FROM nodes WHERE sid=sessions.id AND kind='file'), declared_dirs=(SELECT COUNT(*) FROM nodes WHERE sid=sessions.id AND kind='directory'), declared_bytes=COALESCE((SELECT SUM(size) FROM nodes WHERE sid=sessions.id),0), confirmed_bytes=COALESCE((SELECT SUM(length) FROM parts WHERE sid=sessions.id),0)",
        );
      this.db.exec(`
CREATE INDEX IF NOT EXISTS nodes_destination_key ON nodes(sid,destination_key);
CREATE INDEX IF NOT EXISTS nodes_source_key ON nodes(sid,source_key);
CREATE TRIGGER IF NOT EXISTS count_nodes_insert AFTER INSERT ON nodes BEGIN
  UPDATE sessions SET declared_files=declared_files+(NEW.kind='file'),
    declared_dirs=declared_dirs+(NEW.kind='directory'), declared_bytes=declared_bytes+NEW.size WHERE id=NEW.sid;
END;
CREATE TRIGGER IF NOT EXISTS count_parts_insert AFTER INSERT ON parts BEGIN
  UPDATE sessions SET confirmed_bytes=confirmed_bytes+NEW.length WHERE id=NEW.sid;
END;
`);
      if (!columns.has("overwrite"))
        this.db.exec(
          "UPDATE sessions SET conflict=1 WHERE id IN (SELECT sid FROM asks)",
        );
      this.db.exec(
        "UPDATE sessions SET hook_status='failed' WHERE hook_status='running'",
      );
    } catch (error) {
      this.db.close();
      throw error;
    }
  }
  run(sql: string, ...args: (string | number | null)[]) {
    return this.statement(sql).run(...args);
  }
  private statement(sql: string) {
    let stmt = this.statements.get(sql);
    if (!stmt) {
      stmt = this.db.prepare(sql);
      this.statements.set(sql, stmt);
    }
    return stmt;
  }
  get<T = Record<string, unknown>>(
    sql: string,
    ...args: (string | number)[]
  ): T | undefined {
    return this.statement(sql).get(...args) as T | undefined;
  }
  all<T = Record<string, unknown>>(
    sql: string,
    ...args: (string | number)[]
  ): T[] {
    return this.statement(sql).all(...args) as T[];
  }
  transaction<T>(fn: () => T): T {
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const value = fn();
      this.db.exec("COMMIT");
      return value;
    } catch (e) {
      try {
        this.db.exec("ROLLBACK");
      } catch {
        /* SQLite may already have rolled back. */
      }
      throw e;
    }
  }
  close() {
    if (this.closed) return;
    this.closed = true;
    this.statements.clear();
    this.db.close();
  }
}

export class Mutex {
  private tail: Promise<unknown> = Promise.resolve();
  run<T>(fn: () => T | Promise<T>): Promise<T> {
    const next = this.tail.then(fn);
    this.tail = next.catch(() => {});
    return next;
  }
}

export class ProtocolError extends Error {
  constructor(
    public status: number,
    public code: string,
    message = code,
    public retryable?: boolean,
    public phase?: string,
  ) {
    super(message);
  }
}
export function check(
  condition: unknown,
  code: string,
  status = 400,
): asserts condition {
  if (!condition) throw new ProtocolError(status, code);
}
export function relativePath(value: unknown): string {
  check(
    typeof value === "string" &&
      value.length > 0 &&
      Buffer.byteLength(value) <= 1024 &&
      value === value.normalize("NFC"),
    "bad_path",
  );
  for (const segment of value.split("/")) {
    check(
      segment.length > 0 &&
        Buffer.byteLength(segment) <= 240 &&
        !/[\\<>:"|?*\x00-\x1f\x7f]/.test(segment) &&
        !/[. ]$/.test(segment) &&
        segment !== "." &&
        segment !== ".." &&
        !/^(con|prn|aux|nul|com[1-9]|lpt[1-9])(?:\.|$)/i.test(segment),
      "bad_path",
    );
  }
  return value;
}
export function identifier(value: unknown): string {
  check(
    typeof value === "string" &&
      /^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$/.test(value),
    "bad_id",
  );
  return value;
}
/** Resolve the application-visible directory using the same layout as Engine.publish. */
export function publishedDirectory(baseDir: string, targetDir: string): string {
  return path.join(path.resolve(baseDir), "published", relativePath(targetDir));
}
export function integer(value: unknown): number {
  check(
    typeof value === "number" && Number.isSafeInteger(value) && value >= 0,
    "bad_number",
  );
  return value;
}

export interface Failure {
  code: string;
  status: number;
  phase: string;
  retryable: boolean;
}
/** Preserve OS/SQLite failures instead of reporting malformed request data. */
export function normalizeError(error: unknown, phase?: string): ProtocolError {
  if (error instanceof ProtocolError) {
    error.phase ??= phase;
    return error;
  }
  const e = error as { code?: string; errcode?: number; message?: string };
  const sqlite = e?.code?.startsWith("ERR_SQLITE") ? (e.errcode ?? 0) & 255 : 0;
  if (
    ["ENOSPC", "EDQUOT", "SQLITE_FULL"].includes(e?.code ?? "") ||
    sqlite === 13
  )
    return new ProtocolError(
      507,
      "storage_full",
      "Server storage is full",
      false,
      phase,
    );
  if (
    [
      "EIO",
      "EROFS",
      "EACCES",
      "EPERM",
      "ENODEV",
      "ENOENT",
      "ENOTDIR",
      "EMFILE",
      "ENFILE",
      "EBUSY",
      "EFBIG",
      "EXDEV",
      "SQLITE_IOERR",
      "SQLITE_READONLY",
      "SQLITE_CANTOPEN",
    ].includes(e?.code ?? "") ||
    [5, 6, 8, 10, 11, 14, 26].includes(sqlite)
  )
    return new ProtocolError(
      503,
      "storage_unavailable",
      "Server storage is unavailable",
      false,
      phase,
    );
  return new ProtocolError(
    500,
    "server_error",
    "Server could not complete the operation",
    false,
    phase,
  );
}
