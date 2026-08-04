// MFUP/2 control channel — WebSocket wrapper

import type {
  ClientControlMsg,
  ServerControlMsg,
  HelloMsg,
  ResumeMsg,
  HelloOkMsg,
  ResumeOkMsg,
  ServerLimits,
} from "./protocol.js";
import { PROTOCOL_VERSION } from "./protocol.js";
import {
  MfupError,
  wsConnectFailed,
  wsHandshakeFailed,
  wsClosedUnexpected,
  wsMessageParse,
} from "./errors.js";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------
export type ControlEventMap = {
  hello_ok: HelloOkMsg;
  resume_ok: ResumeOkMsg;
  file_ack: ServerControlMsg & { t: "FILE_ACK" };
  nack_chunk: ServerControlMsg & { t: "NACK_CHUNK" };
  flow: ServerControlMsg & { t: "FLOW" };
  prune_node: ServerControlMsg & { t: "PRUNE_NODE" };
  reject_file: ServerControlMsg & { t: "REJECT_FILE" };
  session_abort: ServerControlMsg & { t: "SESSION_ABORT" };
  commit_ok: ServerControlMsg & { t: "COMMIT_OK" };
  commit_retry: ServerControlMsg & { t: "COMMIT_RETRY" };
  probe_ack: ServerControlMsg & { t: "PROBE_ACK" };
  ask: ServerControlMsg & { t: "ASK" };
  error: MfupError;
  close: { code: number; reason: string };
};

type Listener<T> = (ev: T) => void;

export interface ControlChannelOpts {
  url: string;            // ws(s)://host/mfup/control
  sessionId: string;
  resumeToken: string;
  legId: string;
  /** Relative path where files should be placed on the server */
  targetDir: string;
  /** Arbitrary JSON attached to the session (HELLO.meta) */
  meta?: unknown;
  /** If set, we send RESUME instead of HELLO */
  lastKnownEpoch?: number | null;
}

// ---------------------------------------------------------------------------
// ControlChannel
// ---------------------------------------------------------------------------
export class ControlChannel {
  private ws: WebSocket | null = null;
  private listeners = new Map<string, Set<Listener<any>>>();
  private _epoch: number = 0;
  private _limits: ServerLimits | null = null;
  private _expiresAt: string | null = null;
  private _ready: Promise<void>;
  private _resolveReady!: () => void;
  private _rejectReady!: (e: Error) => void;
  private _readySettled = false;

  constructor(private opts: ControlChannelOpts) {
    this._ready = new Promise((res, rej) => {
      this._resolveReady = res;
      this._rejectReady = rej;
    });
  }

  get epoch(): number { return this._epoch; }
  get limits(): ServerLimits | null { return this._limits; }
  get expiresAt(): string | null { return this._expiresAt; }

  /** Wait until HELLO_OK or RESUME_OK has been received */
  ready(): Promise<void> { return this._ready; }

  // -- lifecycle --------------------------------------------------------

  open(): void {
    const ws = new WebSocket(this.opts.url);
    this.ws = ws;

    ws.onopen = () => {
      if (this.opts.lastKnownEpoch != null) {
        this.sendResume();
      } else {
        this.sendHello();
      }
    };

    ws.onmessage = (ev) => {
      try {
        const msg: ServerControlMsg = JSON.parse(ev.data as string);
        this.dispatch(msg);
      } catch (err) {
        this.emit("error", wsMessageParse(String(ev.data).slice(0, 200), err));
      }
    };

    ws.onerror = () => {
      this.emit("error", wsConnectFailed(this.opts.url));
    };

    ws.onclose = (ev) => {
      this.emit("close", { code: ev.code, reason: ev.reason });
      if (ev.code !== 1000) {
        this.emit("error", wsClosedUnexpected(ev.code, ev.reason));
      }
      this.settleReady(new Error(`WebSocket closed: ${ev.code} ${ev.reason}`));
    };
  }

  close(code = 1000, reason = ""): void {
    this.ws?.close(code, reason);
  }

  send(msg: ClientControlMsg): void {
    this.ws?.send(JSON.stringify(msg));
  }

  sendAbort(code: string, reason: string): void {
    this.send({ t: "CLIENT_ABORT", code, reason });
  }

  sendAction(action: "merge_overwrite" | "cancel"): void {
    this.send({ t: "ACTION", action });
  }

  // -- event bus -------------------------------------------------------

  on<K extends keyof ControlEventMap>(event: K, fn: Listener<ControlEventMap[K]>): () => void {
    let set = this.listeners.get(event);
    if (!set) { set = new Set(); this.listeners.set(event, set); }
    set.add(fn);
    return () => { set!.delete(fn); };
  }

  private emit<K extends keyof ControlEventMap>(event: K, data: ControlEventMap[K]): void {
    const set = this.listeners.get(event);
    if (set) for (const fn of set) fn(data);
  }

  private settleReady(err?: Error): void {
    if (this._readySettled) return;
    this._readySettled = true;
    if (err) { this._rejectReady(err); } else { this._resolveReady(); }
  }

  // -- internal --------------------------------------------------------

  private sendHello(): void {
    // No resume_token here: the token is SERVER-issued and arrives in
    // HELLO_OK. (opts.resumeToken is only used for RESUME.)
    const msg: HelloMsg = {
      t: "HELLO",
      v: PROTOCOL_VERSION,
      session_id: this.opts.sessionId,
      leg_id: this.opts.legId,
      target_dir: this.opts.targetDir,
    };
    if (this.opts.meta !== undefined) {
      msg.meta = this.opts.meta;
    }
    this.send(msg);
  }

  private sendResume(): void {
    const msg: ResumeMsg = {
      t: "RESUME",
      session_id: this.opts.sessionId,
      resume_token: this.opts.resumeToken,
      leg_id: this.opts.legId,
      last_known_epoch: this.opts.lastKnownEpoch ?? null,
    };
    this.send(msg);
  }

  private dispatch(msg: ServerControlMsg): void {
    switch (msg.t) {
      case "HELLO_OK":
        this._epoch = msg.epoch;
        this._limits = msg.limits;
        this._expiresAt = msg.expires_at;
        this.emit("hello_ok", msg);
        this.settleReady();
        break;
      case "RESUME_OK":
        this._epoch = msg.epoch;
        this._expiresAt = msg.expires_at;
        this.emit("resume_ok", msg);
        this.settleReady();
        break;
      case "FILE_ACK":
        this.emit("file_ack", msg);
        break;
      case "NACK_CHUNK":
        this.emit("nack_chunk", msg);
        break;
      case "FLOW":
        this.emit("flow", msg);
        break;
      case "PRUNE_NODE":
        this.emit("prune_node", msg);
        break;
      case "REJECT_FILE":
        this.emit("reject_file", msg);
        break;
      case "SESSION_ABORT":
        this.emit("session_abort", msg);
        this.emit("error", wsHandshakeFailed(msg.code, msg.reason));
        // Reject ready() with the typed FATAL error so the reconnect loop
        // can distinguish "server refused this session forever" (not_found,
        // auth_failed, bad_version) from a transient network failure.
        this.settleReady(wsHandshakeFailed(msg.code, msg.reason));
        break;
      case "COMMIT_OK":
        this.emit("commit_ok", msg);
        break;
      case "COMMIT_RETRY":
        this.emit("commit_retry", msg);
        break;
      case "PROBE_ACK":
        this.emit("probe_ack", msg);
        break;
      case "ASK":
        this.emit("ask", msg);
        break;
    }
  }
}
