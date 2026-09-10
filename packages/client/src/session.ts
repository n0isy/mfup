import {
  PROTOCOL,
  MfupError,
  type Choice,
  type Entry,
  type Limits,
  type Manifest,
  type Receipt,
  type RemoteState,
  type Snapshot,
  type Source,
  type Ticket,
} from "./types.js";
import { fromFiles } from "./ingestion.js";
import { baskets, type Work } from "./scheduler.js";
import { uploadMultipart } from "./upload.js";
import {
  DEFAULT_RETRIES,
  isRetryable,
  retryDelay,
  validateRetryOptions,
  type RetryOptions,
} from "./retry.js";

export interface SessionOptions extends RetryOptions {
  serverUrl?: string;
  targetDir?: string;
  meta?: unknown;
  ticket?: Ticket;
  concurrency?: number;
  batchDelayMs?: number;
  maxReady?: number;
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
  private controlControllers = new Set<AbortController>();
  private inFlight = new Map<AbortController, number>();
  private progressTimer?: ReturnType<typeof setTimeout>;
  private needsStatus = false;
  private recovering = 0;
  private ws?: WebSocket;
  private wsTimer?: ReturnType<typeof setTimeout>;
  private disposed = false;
  private paused = false;
  private cancelled = false;
  private stopping = false;
  private connection?: Promise<Ticket>;
  private resumption?: Promise<void>;
  private fatal: unknown = null;
  private running = false;
  private completions = new Set<{
    resolve: () => void;
    reject: (error: unknown) => void;
  }>();
  private completion() {
    return new Promise<void>((resolve, reject) =>
      this.completions.add({ resolve, reject }),
    );
  }
  private settle(error?: unknown) {
    for (const waiter of this.completions)
      error ? waiter.reject(error) : waiter.resolve();
    this.completions.clear();
  }
  private async recover<T>(operation: () => Promise<T>): Promise<T> {
    while (true) {
      await this.unpaused();
      try {
        return await operation();
      } catch (error) {
        if (this.cancelled || this.stopping || this.disposed) throw error;
        this.fail(error);
      }
    }
  }
  readonly options: SessionOptions;
  constructor(options: SessionOptions = {}) {
    validateRetryOptions(options);
    if (
      options.maxReady !== undefined &&
      (!Number.isSafeInteger(options.maxReady) ||
        options.maxReady < 1 ||
        options.maxReady > 10000)
    )
      throw new MfupError(
        "bad_options",
        "maxReady must be between 1 and 10000",
      );
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
    if (controller) this.controlControllers.add(controller);
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
      if (controller) this.controlControllers.delete(controller);
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
    this.settle(this.fatal);
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
    if (this.ticket && remote.epoch < this.ticket.epoch) return;
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
      confirmedBytes: Math.max(
        this.snapshot.confirmedBytes,
        remote.confirmedBytes ?? 0,
      ),
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
      !this.resumption &&
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
  async connect(): Promise<Ticket> {
    if (this.connection) return this.connection;
    this.connection = this.retryRequest(() => this.connectImpl());
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
      this.needsStatus = true;
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
      this.apply(
        await this.retryRequest(() =>
          this.post<RemoteState>("/properties", { overwrite }),
        ),
      );
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
    if (this.resumption) return this.resumption;
    this.resumption = Promise.resolve().then(() => this.resumeImpl());
    try {
      await this.resumption;
    } finally {
      this.resumption = undefined;
    }
  }
  private async resumeImpl() {
    if (!this.ticket) {
      this.fatal = null;
      this.paused = false;
      this.update({ state: "connecting", error: null, errorInfo: null });
      return;
    }
    this.paused = true;
    this.fatal = null;
    this.stopping = false;
    this.update({ error: null, errorInfo: null });
    const remote = await this.retryRequest(() =>
      this.post<RemoteState & { limits: Limits }>("/resume"),
    );
    this.ticket.epoch = remote.epoch;
    this.ticket.limits = remote.limits;
    this.needsStatus = true;
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
    this.settle(new MfupError("cancelled", "Upload stopped"));
    for (const controller of this.controlControllers) controller.abort();
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
    if (this.cancelled || this.stopping || this.disposed) this.alive();
    while (this.paused || this.fatal) {
      await this.wait();
      if (this.cancelled || this.stopping || this.disposed) this.alive();
    }
  }
  private confirm(receipt: Receipt) {
    this.update({
      confirmedBytes: Math.max(
        this.snapshot.confirmedBytes,
        receipt.confirmedBytes,
      ),
      batches: this.snapshot.batches + 1,
    });
  }
  private async backoff(attempt: number, respectPause = true) {
    const until = Date.now() + retryDelay(attempt, this.options);
    while (Date.now() < until) {
      this.alive();
      if (respectPause && this.paused) {
        await this.unpaused();
        return;
      }
      await new Promise<void>((resolve) => {
        const wake = () => {
          clearTimeout(timer);
          this.notifyWaiters.delete(wake);
          resolve();
        };
        const timer = setTimeout(
          wake,
          Math.min(until - Date.now(), 2147483647),
        );
        this.notifyWaiters.add(wake);
      });
    }
    this.alive();
  }
  private async retryRequest<T>(operation: () => Promise<T>): Promise<T> {
    for (let attempt = 0; ; attempt++) {
      this.alive();
      try {
        return await operation();
      } catch (error) {
        this.alive();
        if (
          !isRetryable(error) ||
          attempt >= (this.options.retries ?? DEFAULT_RETRIES)
        )
          throw error;
        this.recovering++;
        try {
          await this.backoff(attempt, false);
        } finally {
          this.recovering--;
          this.wake();
        }
      }
    }
  }
  private async send(group: Work[]) {
    let id = crypto.randomUUID();
    let statusEpoch = this.needsStatus ? 0 : this.ticket!.epoch;
    let pending = group;
    for (let attempt = 0; ; attempt++) {
      await this.unpaused();
      const attemptEpoch = this.ticket!.epoch;
      const controller = new AbortController();
      this.controllers.add(controller);
      const active = this.controllers.size;
      this.update({
        activeRequests: active,
        maxActiveRequests: Math.max(active, this.snapshot.maxActiveRequests),
      });
      let retry: unknown;
      try {
        if (statusEpoch !== this.ticket!.epoch) {
          const status = await this.post<{
            received: boolean[];
            confirmedBytes: number;
          }>("/status", {
            epoch: this.ticket!.epoch,
            files: group.flatMap((w) => (w.part ? [w.part] : [])),
          });
          let n = 0;
          pending = group.filter((w) => !w.part || !status.received[n++]);
          id = crypto.randomUUID();
          this.update({
            confirmedBytes: Math.max(
              this.snapshot.confirmedBytes,
              status.confirmedBytes,
            ),
          });
          statusEpoch = this.ticket!.epoch;
          if (!pending.length) return;
        }
        const form = new FormData(),
          manifest: Manifest = {
            files: pending.flatMap((w) => (w.part ? [w.part] : [])),
            dirs: pending.flatMap((w) => (w.dir ? [w.dir] : [])),
          };
        form.append("manifest", JSON.stringify(manifest));
        let n = 0;
        for (const work of pending)
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
        let receipt: Receipt;
        if (
          this.options.trackUploadProgress &&
          !this.options.fetch &&
          typeof XMLHttpRequest !== "undefined"
        ) {
          const pendingBytes = pending.reduce(
            (sum, w) => sum + (w.part?.[4] ?? 0),
            0,
          );
          receipt = await uploadMultipart(
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
        } else
          receipt = await this.request<Receipt>(url, {
            method: "POST",
            headers,
            body: form,
            signal: controller.signal,
          });
        this.inFlight.delete(controller);
        this.confirm(receipt);
        return;
      } catch (error) {
        this.inFlight.delete(controller);
        this.alive();
        if (this.paused || attemptEpoch !== this.ticket!.epoch) {
          attempt--;
          continue;
        }
        if (!isRetryable(error)) throw error;
        // A failed receipt probe is part of this attempt, not a separate retry budget.
        try {
          const receipt = await this.request<Receipt>(
            this.endpoint(`/batches/${id}`),
          );
          this.confirm(receipt);
          return;
        } catch (receiptError) {
          if (
            !(
              receiptError instanceof MfupError && receiptError.status === 404
            ) &&
            !isRetryable(receiptError)
          )
            throw receiptError;
        }
        if (attempt >= (this.options.retries ?? DEFAULT_RETRIES)) throw error;
        retry = error;
      } finally {
        this.inFlight.delete(controller);
        this.controllers.delete(controller);
        this.update({ activeRequests: this.controllers.size });
      }
      if (retry) {
        this.recovering++;
        try {
          await this.backoff(attempt);
        } finally {
          this.recovering--;
          this.wake();
        }
      }
    }
  }
  async retry() {
    if (!this.running)
      throw new MfupError("not_running", "There is no retained upload");
    const completion = this.completion();
    void this.resume().catch((error) => this.fail(error));
    return completion;
  }
  upload(source: Source | FileList | File[]): Promise<void> {
    if (this.running)
      return Promise.reject(
        new MfupError("busy", "An upload is already running"),
      );
    this.running = true;
    const result = this.completion();
    void this.uploadImpl(source).then(
      () => {
        this.running = false;
        this.settle();
      },
      (error) => {
        this.running = false;
        this.settle(error);
      },
    );
    return result;
  }
  private async uploadImpl(source: Source | FileList | File[]) {
    if (!this.ticket || this.snapshot.state === "idle") {
      try {
        await this.recover(() => this.connect());
      } catch (error) {
        if (!this.stopping && !this.cancelled) this.fail(error);
        throw error;
      }
    }
    this.alive();
    if (this.snapshot.state === "published") return;
    if (this.snapshot.state === "committed") {
      if (this.options.autoPublish !== false && this.canClientPublish())
        await this.recover(() => this.publish());
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
    const maxReady = this.options.maxReady ?? 10000;
    const lowReady = Math.floor(maxReady / 2);
    let pendingCount = 0;
    let gated = false;
    const queue: Work[] = [],
      active = new Set<Promise<void>>();
    let ended = false,
      files = 0,
      dirs = 0,
      bytes = 0;
    let scanUpdateAt = 0;
    const input =
      Symbol.asyncIterator in Object(source) ||
      (Array.isArray(source) && source.length > 0 && "kind" in source[0]) ||
      (Symbol.iterator in Object(source) &&
        !Array.isArray(source) &&
        !(typeof FileList !== "undefined" && source instanceof FileList))
        ? (source as Source)
        : fromFiles(source as ArrayLike<File>);
    const capacity = async () => {
      await this.unpaused();
      if (pendingCount >= maxReady) gated = true;
      while (this.recovering || (gated && pendingCount > lowReady)) {
        await this.wait();
        await this.unpaused();
      }
      gated = false;
    };
    const push = async (work: Work) => {
      await capacity();
      queue.push(work);
      pendingCount++;
      this.wake();
    };
    const producer = (async () => {
      const iterator =
        Symbol.asyncIterator in Object(input)
          ? (input as AsyncIterable<Entry>)[Symbol.asyncIterator]()
          : (input as Iterable<Entry>)[Symbol.iterator]();
      let finished = false;
      try {
        while (true) {
          await capacity();
          const next = await iterator.next();
          if (this.cancelled || this.stopping || this.disposed) {
            this.alive();
          }
          if (next.done) {
            finished = true;
            break;
          }
          const entry = next.value;
          if (entry.kind === "directory") {
            dirs++;
            await push({ dir: entry.path });
            continue;
          }
          const file = entry.file;
          if (!file) throw new MfupError("missing_file", entry.path);
          files++;
          bytes += file.size;
          if (Date.now() >= scanUpdateAt) {
            this.update({ discovered: files, totalBytes: bytes });
            scanUpdateAt = Date.now() + 50;
          }
          for (
            let offset = 0;
            offset < file.size || offset === 0;
            offset += limits.partBytes
          ) {
            const length = Math.min(limits.partBytes, file.size - offset);
            await push({
              file,
              part: [entry.path, file.size, file.lastModified, offset, length],
            });
          }
        }
      } finally {
        if (!finished) await iterator.return?.();
      }
      ended = true;
      this.update({ scanDone: true, discovered: files, totalBytes: bytes });
    })().catch((error) => {
      if (!this.cancelled && !this.stopping && !this.disposed) this.fail(error);
      this.fatal = error;
      ended = true;
      this.wake();
    });
    try {
      while (!ended || queue.length || active.size) {
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
            task = this.recover(() => this.send(group))
              .catch((error) => {
                this.fatal ??= error;
                for (const c of this.controllers) c.abort();
              })
              .finally(() => {
                pendingCount -= group.length;
                active.delete(task);
                this.wake();
              });
            active.add(task);
          }
        } else if (!ended || active.size) await this.wait();
      }
      await producer;
      this.alive();
      const committed = await this.recover(() =>
        this.retryRequest(() =>
          this.post<RemoteState>("/commit", { files, dirs, bytes }),
        ),
      );
      this.apply(committed);
      this.alive();
      if (this.getSnapshot().state === "published") return;
      this.update({
        state: committed.state === "published" ? "published" : "committed",
      });
      if (committed.state === "published") return;
      if (this.options.autoPublish !== false && this.canClientPublish())
        await this.recover(() => this.publish());
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
        const remote = await this.retryRequest(() => this.refresh());
        if (remote.state === "published") {
          this.update({ state: "published" });
          return remote;
        }
        continue;
      }
      try {
        const done = await this.retryRequest(() =>
          this.post<RemoteState>("/publish"),
        );
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
