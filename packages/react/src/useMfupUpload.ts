// The main hook: one upload lifecycle per start() call.
//
// Design notes (why it looks like this):
// - The session is created inside start(), never in an effect — StrictMode's
//   double-invoked effects therefore cannot spawn ghost sessions.
// - Option callbacks are read through a ref, so passing inline lambdas does
//   not re-subscribe anything and never misses an event.
// - autoPublish waits for settleAsks(): the transfer keeps running while a
//   conflict dialog is open (the point of the non-blocking control channel),
//   and publish happens only after the user's verdict — or never, on cancel.

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  MfupSession,
  type MfupAsk,
  type MfupError,
  type MfupSessionSnapshot,
  type UploadSource,
} from "@mfup/client";
import { useMfupConfig } from "./context.js";
import { useMfupSession } from "./useMfupSession.js";

const TERMINAL = ["committed", "aborted", "failed"];

export interface UseMfupUploadOptions {
  /** Server base URL. Falls back to the nearest <MfupProvider>. */
  serverUrl?: string;
  targetDir?: string;
  /** Session meta for the server-side authorize/map_file hooks. */
  meta?: unknown;
  chunkSize?: number;
  maxReconnectAttempts?: number | null;
  /** Publish automatically after COMMIT_OK once every ask is settled and
   * none was answered "cancel". Default true. */
  autoPublish?: boolean;
  /** Abort a live upload when the component unmounts. Default false —
   * uploads usually should survive route changes. */
  abortOnUnmount?: boolean;
  onCommitted?(ev: { files: number; bytes: number }): void;
  onPublished?(files: string[]): void;
  onAsk?(ask: MfupAsk): void;
  onError?(err: MfupError): void;
}

export interface UseMfupUploadResult {
  /** The live session, or null before the first start(). */
  session: MfupSession | null;
  /** Reactive snapshot (progress, state, asks, errors), or null. */
  snapshot: MfupSessionSnapshot | null;
  /** Unanswered interactive questions — render a dialog, call ask.respond(). */
  pendingAsks: readonly MfupAsk[];
  /** True while an upload is running (until committed/aborted/failed). */
  busy: boolean;
  /** Connect, scan, stream, commit — and auto-publish unless disabled.
   * Rejects on fatal errors; resolves after commit (and publish). */
  start(source: UploadSource | FileList | File[]): Promise<void>;
  abort(): void;
  /** Manual publish (for autoPublish: false flows). */
  publish(): Promise<{ published: string[] }>;
  /** Forget the finished session so start() can run again. */
  reset(): void;
}

export function useMfupUpload(options: UseMfupUploadOptions = {}): UseMfupUploadResult {
  const ctx = useMfupConfig();
  const [session, setSession] = useState<MfupSession | null>(null);
  const snapshot = useMfupSession(session);

  const optsRef = useRef(options);
  optsRef.current = options;
  const ctxRef = useRef(ctx);
  ctxRef.current = ctx;
  const sessionRef = useRef<MfupSession | null>(null);

  const start = useCallback(async (source: UploadSource | FileList | File[]) => {
    const opts = optsRef.current;
    const base = ctxRef.current;
    const serverUrl = opts.serverUrl ?? base?.serverUrl;
    if (!serverUrl) {
      throw new Error("useMfupUpload: serverUrl missing — pass it in options or via <MfupProvider>");
    }
    const prev = sessionRef.current;
    if (prev && !TERMINAL.includes(prev.state)) {
      throw new Error("useMfupUpload: an upload is already running — abort() it or wait");
    }

    const s = new MfupSession({
      serverUrl,
      targetDir: opts.targetDir ?? base?.targetDir,
      meta: opts.meta ?? base?.meta,
      chunkSize: opts.chunkSize ?? base?.chunkSize,
      maxReconnectAttempts: opts.maxReconnectAttempts ?? base?.maxReconnectAttempts,
    });
    s.on("committed", (ev) => optsRef.current.onCommitted?.(ev));
    s.on("published", (ev) => optsRef.current.onPublished?.(ev.published));
    s.on("ask", (ask) => optsRef.current.onAsk?.(ask));
    s.on("error", (err) => optsRef.current.onError?.(err));

    sessionRef.current = s;
    setSession(s);

    await s.connect();
    await s.upload(source);
    const verdict = await s.settleAsks();
    if ((optsRef.current.autoPublish ?? true) && verdict !== "cancel" && s.state === "committed") {
      await s.publish();
    }
  }, []);

  const abort = useCallback(() => {
    sessionRef.current?.abort();
  }, []);

  const publish = useCallback(async () => {
    const s = sessionRef.current;
    if (!s) throw new Error("useMfupUpload: no session to publish");
    return s.publish();
  }, []);

  const reset = useCallback(() => {
    const s = sessionRef.current;
    if (s && !TERMINAL.includes(s.state)) {
      s.abort("client_reset", "hook reset");
    }
    sessionRef.current = null;
    setSession(null);
  }, []);

  useEffect(() => {
    return () => {
      const s = sessionRef.current;
      if (optsRef.current.abortOnUnmount && s && !TERMINAL.includes(s.state)) {
        s.abort("client_unmount", "component unmounted");
      }
    };
  }, []);

  const pendingAsks = useMemo(
    () => (snapshot ? snapshot.asks.filter((a) => a.answered === null) : []),
    [snapshot],
  );
  const busy = snapshot !== null && !TERMINAL.includes(snapshot.state);

  return { session, snapshot, pendingAsks, busy, start, abort, publish, reset };
}
