// MFUP/2 streaming probe — empirical detection of duplex:"half" support

import type { ControlChannel } from "./control.js";

export interface ProbeResult {
  streaming: boolean;
  latencyMs: number;
}

/**
 * Detect if the browser supports true streaming uploads (duplex:"half").
 *
 * Sends a probe POST with a ReadableStream body. Enqueues chunk A,
 * waits up to `timeoutMs` for a PROBE_ACK message on the control channel.
 * If PROBE_ACK arrives before timeout, streaming is confirmed.
 * Then enqueues chunk B and closes the stream.
 *
 * If the browser throws on duplex:"half" or ReadableStream body, returns { streaming: false }.
 */
export async function probeStreaming(opts: {
  baseUrl: string;
  sessionId: string;
  resumeToken: string;
  control: ControlChannel;
  signal?: AbortSignal;
  timeoutMs?: number;
}): Promise<ProbeResult> {
  const { baseUrl, sessionId, control, signal, timeoutMs = 1500 } = opts;
  const url = `${baseUrl}/mfup/probe/${sessionId}`;
  const CHUNK_SIZE = 1024; // 1 KB

  // Quick pre-check: does the browser support duplex:"half" streaming bodies?
  //
  // Strategy: try constructing Request WITHOUT duplex, then WITH duplex.
  // - Chrome: without → TypeError ("duplex member must be specified"); with → OK
  // - Firefox: without → may or may not throw (not duplex-related); with → throws or stringifies
  //
  // Streaming is supported only if: without-duplex THROWS and with-duplex SUCCEEDS.
  {
    const mk = () => new ReadableStream({ start(c) { c.close(); } });
    let withoutThrew = false;
    let withSucceeded = false;
    try { new Request("http://x", { method: "POST", body: mk() }); }
    catch { withoutThrew = true; }
    if (withoutThrew) {
      try {
        // @ts-expect-error — duplex not in TS lib types
        new Request("http://x", { method: "POST", body: mk(), duplex: "half" });
        withSucceeded = true;
      } catch { /* still no streaming */ }
    }
    if (!withoutThrew || !withSucceeded) {
      return { streaming: false, latencyMs: 0 };
    }
  }

  let controller: ReadableStreamDefaultController<Uint8Array> | null = null;

  const stream = new ReadableStream<Uint8Array>({
    start(ctrl) {
      controller = ctrl;
    },
  });

  // Try to open the streaming fetch — browsers that don't support duplex:"half"
  // will throw synchronously or reject immediately.
  let fetchPromise: Promise<Response>;
  try {
    fetchPromise = fetch(url, {
      method: "POST",
      headers: { "X-MFUP-Token": opts.resumeToken },
      body: stream,
      // @ts-expect-error — duplex: "half" is not yet in the TS lib types
      duplex: "half",
      signal,
    });
  } catch {
    return { streaming: false, latencyMs: 0 };
  }

  // Enqueue chunk A
  const chunkA = new Uint8Array(CHUNK_SIZE); // zeros
  try {
    controller!.enqueue(chunkA);
  } catch {
    return { streaming: false, latencyMs: 0 };
  }

  const start = performance.now();

  // Race: PROBE_ACK on control channel vs the fetch settling vs timeout.
  // The server includes first_chunk_bytes in PROBE_ACK so we can verify the
  // browser actually sent our binary data (not "[object ReadableStream]" = 23 bytes).
  const streaming = await new Promise<boolean>((resolve) => {
    let settled = false;
    const settle = (verdict: boolean) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      unsub();
      resolve(verdict);
    };

    const timer = setTimeout(() => settle(false), timeoutMs);

    const unsub = control.on("probe_ack", (msg) => {
      // Verify server received real binary data, not a stringified ReadableStream
      settle((msg.first_chunk_bytes ?? 0) >= CHUNK_SIZE);
    });

    // Chrome on plain HTTP/1.1 rejects streaming bodies IMMEDIATELY
    // (ERR_H2_OR_QUIC_REQUIRED) — without listening to the fetch, the probe
    // sat out the full timeout before falling back to batch. An early
    // response (proxy buffered the whole body → non-duplex) is a "no" too.
    fetchPromise.then(
      (resp) => { if (!resp.ok) settle(false); },
      () => settle(false),
    );
  });

  const latencyMs = streaming ? Math.round(performance.now() - start) : 0;

  // Enqueue chunk B and close stream
  const chunkB = new Uint8Array(CHUNK_SIZE);
  try {
    controller!.enqueue(chunkB);
    controller!.close();
  } catch {
    // Stream may already be errored if fetch was aborted — that's fine.
  }

  // Await fetch response (we don't care about the result)
  try {
    await fetchPromise;
  } catch {
    // Ignore — probe response doesn't matter.
  }

  return { streaming, latencyMs };
}
