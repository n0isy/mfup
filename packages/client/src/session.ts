import {
  PROTOCOL,
  MfupError,
  type Choice,
  type Entry,
  type Limits,
  type Manifest,
  type Receipt,
  type RemoteState,
  type ResumeFile,
  type Snapshot,
  type Source,
  type Ticket,
} from "./types.js";
import { fromFiles } from "./ingestion.js";
import { baskets, type Work } from "./scheduler.js";
import { uploadMultipart } from "./upload.js";

export interface SessionOptions {
  serverUrl?: string;
  targetDir?: string;
  meta?: unknown;
  ticket?: Ticket;
  concurrency?: number;
  batchDelayMs?: number;
  maxReady?: number;
  retries?: number;
  /** Additional consumer headers for HTTP requests. Browser WebSocket uses the issued ticket. */
  headers?: Record<string, string>;
  autoPublish?: boolean;
  overwrite?: boolean;
  requestTimeoutMs?: number;
  fetch?: typeof fetch;
  /** Native XHR upload events for smooth in-flight progress. A custom fetch takes precedence. */
  trackUploadProgress?: boolean;
}
const delay = (ms: number) =>
  new Promise<void>((resolve) => setTimeout(resolve, ms));
export class MfupSession {
  ticket: Ticket | null;
  private snapshot: Snapshot = {
    state: "idle",
    discovered: 0,
    confirmedBytes: 0,
    sentBytes: 0,
    totalBytes: 0,
    activeRequests: 0,
    maxActiveRequests: 0,
    batches: 0,
    scanDone: false,
    asks: [],
    error: null,
  };
  private listeners = new Set<() => void>();
  private notifyWaiters = new Set<() => void>();
  private controllers = new Set<AbortController>();
  private inFlight = new Map<AbortController, number>();
  private progressTimer?: ReturnType<typeof setTimeout>;
  private remoteFiles = new Map<string, ResumeFile>();
  private ws?: WebSocket;
  private wsTimer?: ReturnType<typeof setTimeout>;
  private disposed = false;
  private paused = false;
  private cancelled = false;
  private stopping = false;
  private connection?: Promise<Ticket>;
  private fatal: unknown = null;
  private running = false;
  readonly options: SessionOptions;
  constructor(options: SessionOptions = {}) {
    this.options = options;
    this.ticket = options.ticket ?? null;
  }
  getSnapshot = (): Readonly<Snapshot> => this.snapshot;
  subscribe = (listener: () => void) => {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  };
  private update(patch: Partial<Snapshot> = {}) {
    const confirmed = patch.confirmedBytes ?? this.snapshot.confirmedBytes;
    this.snapshot = {
      ...this.snapshot,
      ...patch,
      sentBytes:
        confirmed + [...this.inFlight.values()].reduce((sum, n) => sum + n, 0),
    };
    if (patch.state === "published") {
      clearTimeout(this.progressTimer);
      this.progressTimer = undefined;
      clearTimeout(this.wsTimer);
      this.ws?.close();
    }
    for (const listener of this.listeners) listener();
    this.wake();
  }
  private wake() {
    for (const fn of this.notifyWaiters) fn();
    this.notifyWaiters.clear();
  }
  private wait() {
    return new Promise<void>((resolve) => this.notifyWaiters.add(resolve));
  }
  private base() {
    return (
      this.options.serverUrl ??
      (typeof location !== "undefined" ? location.origin : "")
    ).replace(/\/$/, "");
  }
  private endpoint(suffix = "") {
    return `${this.base()}/mfup/sessions/${this.ticket!.id}${suffix}`;
  }
  private headers() {
    return {
      ...this.options.headers,
      ...(this.ticket ? { Authorization: `Bearer ${this.ticket.token}` } : {}),
    };
  }
  private async request<T>(url: string, init: RequestInit = {}): Promise<T> {
    const controller = init.signal ? undefined : new AbortController();
    let expired = false;
    const timer = controller
      ? setTimeout(() => {
          expired = true;
          controller.abort();
        }, this.options.requestTimeoutMs ?? 30000)
      : undefined;
    try {
      const response = await (this.options.fetch ?? fetch)(url, {
        ...init,
        signal: init.signal ?? controller?.signal,
        headers: { ...this.headers(), ...init.headers },
        credentials: "same-origin",
      });
      let data: any;
      try {
        data = await response.json();
      } catch {
        throw new MfupError(
          response.ok ? "bad_response" : "http_error",
          "Server returned an unreadable response",
          response.status,
        );
      }
      if (!response.ok)
        throw new MfupError(
          data.error ?? "http_error",
          data.message ?? data.error ?? "Request failed",
          response.status,
          data.retryable,
          data.phase,
        );
      return data as T;
    } catch (error) {
      if (expired)
        throw new MfupError("request_timeout", "Request timed out", 408);
      if (error instanceof TypeError)
        throw new MfupError("network_error", "Connection failed", 0);
      throw error;
    } finally {
      clearTimeout(timer);
    }
  }
  private fail(error: unknown) {
    const e =
      error instanceof MfupError
        ? error
        : new MfupError(
            "client_error",
            (error as Error)?.message ?? "Operation failed",
          );
    this.fatal ??= e;
    const first = this.fatal instanceof MfupError ? this.fatal : e;
    if (!this.snapshot.errorInfo)
      this.update({
        state: "failed",
        error: first.message,
        errorInfo: {
          code: first.code,
          status: first.status,
          phase: first.phase ?? "upload",
          retryable: first.retryable ?? false,
        },
      });
    for (const c of this.controllers) c.abort();
    this.wake();
  }

  private post<T>(suffix: string, data: unknown = {}) {
    return this.request<T>(this.endpoint(suffix), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(data),
    });
  }
  private apply(remote: RemoteState) {
    if (
      ["published", "cancelled"].includes(this.snapshot.state) &&
      remote.state !== this.snapshot.state
    )
      return;
    const terminal = ["published", "cancelled"].includes(remote.state);
    if (terminal) this.fatal = null;
    if (remote.state === "cancelled") {
      this.cancelled = true;
      this.paused = false;
      for (const controller of this.controllers) controller.abort();
      clearTimeout(this.wsTimer);
      this.ws?.close();
    }
    const overwrite =
      Boolean(remote.overwrite) || Boolean(this.snapshot.overwrite);
    this.update({
      ...(terminal ? { error: null, errorInfo: null } : {}),
      overwrite,
      overwriteRequired:
        !overwrite &&
        Boolean(remote.overwriteRequired ?? remote.asks?.length) &&
        !["cancelled", "published"].includes(remote.state),
      asks:
        overwrite || ["cancelled", "published"].includes(remote.state)
          ? []
          : (remote.asks ?? []).slice(0, 1),
      clientPublish: remote.clientPublish ?? true,
      processing: remote.processing ?? "none",
      ...(remote.state === "published" ? { state: "published" as const } : {}),
      ...(remote.state === "cancelled" ? { state: "cancelled" as const } : {}),
    });
    if (
      remote.error &&
      !this.stopping &&
      !["cancelled", "published"].includes(remote.state)
    )
      this.fail(
        new MfupError(
          remote.error.code,
          remote.error.code,
          remote.error.status,
          remote.error.retryable,
          remote.error.phase,
        ),
      );
  }
  async refresh() {
    const remote = await this.request<RemoteState>(this.endpoint());
    this.apply(remote);
    return remote;
  }
  private async loadFiles() {
    this.remoteFiles.clear();
    let after = "";
    do {
      const page = await this.request<{
        files: ResumeFile[];
        next: string | null;
      }>(this.endpoint(`/files?after=${encodeURIComponent(after)}`));
      for (const file of page.files) this.remoteFiles.set(file.path, file);
      after = page.next ?? "";
    } while (after);
    this.update({
      confirmedBytes: [...this.remoteFiles.values()].reduce(
        (sum, file) =>
          sum +
          file.offsets.reduce(
            (n, offset) =>
              n + Math.min(this.ticket!.limits.partBytes, file.size - offset),
            0,
          ),
        0,
      ),
    });
  }
  async connect(): Promise<Ticket> {
    if (this.connection) return this.connection;
    this.connection = this.connectImpl();
    try {
      return await this.connection;
    } finally {
      this.connection = undefined;
    }
  }
  private async connectImpl() {
    if (this.disposed) throw new MfupError("disposed", "Session is closed");
    this.update({ state: "connecting" });
    if (this.ticket) {
      const remote = await this.post<RemoteState & { limits: Limits }>(
        "/resume",
      );
      this.ticket = {
        ...this.ticket,
        epoch: remote.epoch,
        limits: remote.limits,
      };
      this.apply(remote);
      await this.loadFiles();
      this.update({
        state:
          remote.state === "published"
            ? "published"
            : ["committed", "publishing"].includes(remote.state)
              ? "committed"
              : "uploading",
      });
    } else {
      this.ticket = await this.request<Ticket>(`${this.base()}/mfup/sessions`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          protocol: PROTOCOL,
          targetDir: this.options.targetDir,
          meta: this.options.meta,
          overwrite: this.options.overwrite,
        }),
      });
      if (!this.stopping)
        this.update({
          state: "uploading",
          overwrite: this.options.overwrite ?? false,
        });
    }
    this.control();
    return this.ticket;
  }
  private control() {
    if (
      typeof WebSocket === "undefined" ||
      this.disposed ||
      this.cancelled ||
      this.snapshot.state === "published"
    )
      return;
    this.ws?.close();
    clearTimeout(this.wsTimer);
    const ws = new WebSocket(
      this.base().replace(/^http/, "ws") + "/mfup/control",
    );
    this.ws = ws;
    ws.onopen = () =>
      ws.send(
        JSON.stringify({
          type: "subscribe",
          id: this.ticket!.id,
          token: this.ticket!.token,
        }),
      );
    ws.onmessage = (event) => {
      try {
        this.apply(JSON.parse(event.data));
      } catch {}
    };
    ws.onclose = () => {
      if (
        this.ws === ws &&
        !this.disposed &&
        !this.cancelled &&
        this.snapshot.state !== "published"
      )
        this.wsTimer = setTimeout(() => this.control(), 1000);
    };
    ws.onerror = () => {};
  }
  exportTicket(): Ticket {
    if (!this.ticket) throw new MfupError("not_connected", "Connect first");
    return { ...this.ticket, limits: { ...this.ticket.limits } };
  }
  async answer(id: string, choice: Choice) {
    if (choice === "cancel") {
      await this.cancel();
      return;
    }
    if (id !== "overwrite")
      throw new MfupError("unknown_question", "Unknown session decision", 404);
    await this.setOverwrite(true);
  }
  async setOverwrite(overwrite = true) {
    try {
      this.apply(await this.post<RemoteState>("/properties", { overwrite }));
    } catch (error) {
      if (
        overwrite &&
        this.snapshot.overwrite &&
        error instanceof MfupError &&
        ["network_error", "request_timeout"].includes(error.code)
      )
        return;
      throw error;
    }
  }
  pause() {
    if (!this.running) return;
    this.paused = true;
    for (const controller of this.controllers) controller.abort();
    this.controllers.clear();
    this.inFlight.clear();
    this.update({ state: "paused", activeRequests: 0 });
  }
  async resume() {
    if (!this.ticket) throw new MfupError("not_connected", "Connect first");
    this.fatal = null;
    this.stopping = false;
    this.update({ error: null, errorInfo: null });
    const remote = await this.post<RemoteState & { limits: Limits }>("/resume");
    this.ticket.epoch = remote.epoch;
    this.ticket.limits = remote.limits;
    await this.loadFiles();
    this.apply(remote);
    this.paused = false;
    this.update({
      state:
        remote.state === "published"
          ? "published"
          : ["committed", "publishing"].includes(remote.state)
            ? "committed"
            : "uploading",
    });
  }
  async cancel() {
    this.stopping = true;
    this.paused = false;
    for (const controller of this.controllers) controller.abort();
    this.inFlight.clear();
    this.update({ state: "cancelling", overwriteRequired: false, asks: [] });
    try {
      if (this.connection) await this.connection;
      if (this.ticket) {
        const result = await this.post<RemoteState>("/cancel");
        this.apply(result);
        this.cancelled = result.state === "cancelled";
        this.update({
          state: result.state === "published" ? "published" : "cancelled",
        });
      } else {
        this.cancelled = true;
        this.update({ state: "cancelled" });
      }
      this.stopping = false;
      this.update({ error: null, errorInfo: null });
      this.ws?.close();
      clearTimeout(this.wsTimer);
    } catch (error) {
      if (["cancelled", "published"].includes(this.snapshot.state)) {
        this.stopping = false;
        this.update({ error: null, errorInfo: null });
        return;
      }
      if (error instanceof MfupError) error.phase = "cancel";
      this.fatal = error;
      this.update({ errorInfo: null });
      this.fail(error);
      throw error;
    }
  }
  dispose() {
    this.disposed = true;
    clearTimeout(this.progressTimer);
    this.inFlight.clear();
    for (const controller of this.controllers) controller.abort();
    this.ws?.close();
    clearTimeout(this.wsTimer);
    this.wake();
  }
  private alive() {
    if (this.cancelled || this.stopping || this.disposed)
      throw new MfupError("cancelled", "Upload stopped");
    if (this.fatal) throw this.fatal;
  }
  private async unpaused() {
    this.alive();
    while (this.paused) {
      await this.wait();
      this.alive();
    }
  }
  private confirm(group: Work[]) {
    let bytes = 0;
    for (const { part } of group)
      if (part) {
        let state = this.remoteFiles.get(part[0]);
        if (!state) {
          state = { path: part[0], size: part[1], mtime: part[2], offsets: [] };
          this.remoteFiles.set(part[0], state);
        }
        if (!state.offsets.includes(part[3])) {
          state.offsets.push(part[3]);
          bytes += part[4];
        }
      }
    this.update({
      confirmedBytes: this.snapshot.confirmedBytes + bytes,
      batches: this.snapshot.batches + 1,
    });
  }
  private async send(group: Work[]) {
    const id = crypto.randomUUID();
    for (let attempt = 0; ; attempt++) {
      await this.unpaused();
      const controller = new AbortController();
      this.controllers.add(controller);
      const active = this.controllers.size;
      this.update({
        activeRequests: active,
        maxActiveRequests: Math.max(active, this.snapshot.maxActiveRequests),
      });
      try {
        const form = new FormData(),
          manifest: Manifest = {
            files: group.flatMap((w) => (w.part ? [w.part] : [])),
            dirs: group.flatMap((w) => (w.dir ? [w.dir] : [])),
          };
        form.append("manifest", JSON.stringify(manifest));
        let n = 0;
        for (const work of group)
          if (work.part) {
            const p = work.part,
              f = work.file!;
            form.append(
              String(n++),
              p[3] === 0 && p[4] === f.size ? f : f.slice(p[3], p[3] + p[4]),
              f.name,
            );
          }
        const url = this.endpoint(`/batches/${id}`);
        const headers = { "X-MFUP-Epoch": String(this.ticket!.epoch) };
        if (
          this.options.trackUploadProgress &&
          !this.options.fetch &&
          typeof XMLHttpRequest !== "undefined"
        ) {
          const pendingBytes = group.reduce(
            (sum, { part }) =>
              sum +
              (part && !this.remoteFiles.get(part[0])?.offsets.includes(part[3])
                ? part[4]
                : 0),
            0,
          );
          await uploadMultipart(
            url,
            form,
            { ...this.headers(), ...headers },
            controller.signal,
            (fraction) => {
              if (controller.signal.aborted || this.disposed) return;
              this.inFlight.set(
                controller,
                Math.floor(pendingBytes * fraction),
              );
              if (!this.progressTimer)
                this.progressTimer = setTimeout(() => {
                  this.progressTimer = undefined;
                  this.update();
                }, 50);
            },
          );
        } else {
          await this.request<Receipt>(url, {
            method: "POST",
            headers,
            body: form,
            signal: controller.signal,
          });
        }
        this.inFlight.delete(controller);
        this.confirm(group);
        return;
      } catch (error) {
        this.inFlight.delete(controller);
        this.update();
        this.alive();
        if (error instanceof MfupError && error.retryable === false)
          throw error;
        if (
          error instanceof MfupError &&
          ![0, 408, 409, 429, 500, 502, 503, 504].includes(error.status)
        )
          throw error;
        if (
          error instanceof MfupError &&
          error.status === 409 &&
          !["busy", "range_busy", "stale_epoch"].includes(error.code)
        )
          throw error;
        await this.unpaused();
        try {
          await this.request<Receipt>(this.endpoint(`/batches/${id}`));
          this.confirm(group);
          return;
        } catch (receiptError) {
          if (receiptError instanceof MfupError && receiptError.status !== 404)
            throw receiptError;
        }
        if (attempt >= (this.options.retries ?? 3)) throw error;
        await delay(Math.min(100 * 2 ** attempt, 2000));
      } finally {
        this.inFlight.delete(controller);
        this.controllers.delete(controller);
        this.update({ activeRequests: this.controllers.size });
      }
    }
  }
  async upload(source: Source | FileList | File[]) {
    if (this.running)
      throw new MfupError("busy", "An upload is already running");
    if (!this.ticket || this.snapshot.state === "idle") {
      try {
        await this.connect();
      } catch (error) {
        if (!this.stopping && !this.cancelled) this.fail(error);
        throw error;
      }
    }
    this.alive();
    if (this.snapshot.state === "published") return;
    if (this.snapshot.state === "committed") {
      if (this.options.autoPublish !== false && this.canClientPublish())
        await this.publish();
      return;
    }
    this.running = true;
    this.fatal = null;
    this.cancelled = false;
    this.update({ error: null, errorInfo: null });
    const limits = {
      ...this.ticket!.limits,
      concurrency: Math.max(
        1,
        Math.min(
          this.ticket!.limits.concurrency,
          this.options.concurrency ?? 6,
        ),
      ),
    };
    const maxReady = Math.max(
      limits.maxParts,
      this.options.maxReady ?? limits.maxParts * limits.concurrency,
    );
    const queue: Work[] = [],
      active = new Set<Promise<void>>();
    let ended = false,
      files = 0,
      dirs = 0,
      bytes = 0;
    const seen = new Set<string>();
    const input =
      Symbol.asyncIterator in Object(source) ||
      (Array.isArray(source) && source.length > 0 && "kind" in source[0]) ||
      (Symbol.iterator in Object(source) &&
        !Array.isArray(source) &&
        !(typeof FileList !== "undefined" && source instanceof FileList))
        ? (source as Source)
        : fromFiles(source as ArrayLike<File>);
    const push = async (work: Work) => {
      await this.unpaused();
      while (queue.length >= maxReady) {
        await this.wait();
        await this.unpaused();
      }
      queue.push(work);
      this.wake();
    };
    const producer = (async () => {
      for await (const entry of input) {
        await this.unpaused();
        if (seen.has(entry.path))
          throw new MfupError("duplicate_path", entry.path);
        seen.add(entry.path);
        if (entry.kind === "directory") {
          dirs++;
          await push({ dir: entry.path });
          continue;
        }
        const file = entry.file;
        if (!file) throw new MfupError("missing_file", entry.path);
        const old = this.remoteFiles.get(entry.path);
        if (old && (old.size !== file.size || old.mtime !== file.lastModified))
          throw new MfupError("file_changed", entry.path);
        files++;
        bytes += file.size;
        this.update({ discovered: files, totalBytes: bytes });
        for (
          let offset = 0;
          offset < file.size || offset === 0;
          offset += limits.partBytes
        ) {
          const length = Math.min(limits.partBytes, file.size - offset);
          if (old?.offsets.includes(offset)) continue;
          await push({
            file,
            part: [entry.path, file.size, file.lastModified, offset, length],
          });
        }
      }
      ended = true;
      this.update({ scanDone: true });
    })().catch((error) => {
      this.fatal = error;
      ended = true;
      this.wake();
    });
    try {
      while (!ended || queue.length || active.size) {
        this.alive();
        await this.unpaused();
        if (queue.length && active.size < limits.concurrency) {
          if (!ended && queue.length < maxReady)
            await delay(this.options.batchDelayMs ?? 8);
          const groups = baskets(
            queue,
            limits.concurrency - active.size,
            limits,
          );
          this.wake();
          for (const group of groups) {
            let task: Promise<void>;
            task = this.send(group)
              .catch((error) => {
                this.fatal ??= error;
                for (const c of this.controllers) c.abort();
              })
              .finally(() => {
                active.delete(task);
                this.wake();
              });
            active.add(task);
          }
        } else if (!ended || active.size) await this.wait();
      }
      await producer;
      this.alive();
      const committed = await this.post<RemoteState>("/commit", {
        files,
        dirs,
        bytes,
      });
      this.apply(committed);
      this.alive();
      if (this.getSnapshot().state === "published") return;
      this.update({
        state: committed.state === "published" ? "published" : "committed",
      });
      if (committed.state === "published") return;
      if (this.options.autoPublish !== false && this.canClientPublish())
        await this.publish();
    } catch (error) {
      if (!this.cancelled && !this.stopping) this.fail(error);
      this.fatal ??= error;
      for (const controller of this.controllers) controller.abort();
      this.wake();
      await Promise.allSettled([...active]);
      await producer;
      throw error;
    } finally {
      this.running = false;
    }
  }
  private canClientPublish() {
    return (
      this.snapshot.clientPublish !== false &&
      ["none", "done"].includes(this.snapshot.processing ?? "none")
    );
  }
  async publish(): Promise<RemoteState> {
    while (true) {
      this.alive();
      if (this.snapshot.overwriteRequired) {
        this.update({ state: "waiting" });
        await delay(250);
        const remote = await this.refresh();
        if (remote.state === "published") {
          this.update({ state: "published" });
          return remote;
        }
        continue;
      }
      try {
        const done = await this.post<RemoteState>("/publish");
        this.apply(done);
        this.update({ state: "published" });
        return done;
      } catch (error) {
        if (!(error instanceof MfupError && error.code === "answers_required"))
          throw error;
        await this.refresh();
      }
    }
  }
}
