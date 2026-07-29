// MFUP/2 data channel — dual-mode: streaming (duplex:"half") or batch (sequential POSTs)

import type { DataFrame } from "./protocol.js";
import { encodeFrame } from "./protocol.js";
import {
  dataOpenFailed,
  dataWriteFailed,
  dataHttpError,
  dataChannelClosed,
  type MfupError,
} from "./errors.js";

export interface DataChannelOpts {
  /** Base URL, e.g. "https://host" */
  baseUrl: string;
  sessionId: string;
  legId: string;
  /** Resume token for data channel auth (sent as X-MFUP-Token header). */
  resumeToken: string;
  /** Current session epoch — used to reject stale requests server-side. */
  epoch: number;
  /** AbortSignal so the session can tear down the request */
  signal?: AbortSignal;
  /** Enable streaming mode (one long POST with duplex:"half") */
  streaming?: boolean;
  /** Batch flush threshold in bytes (default 2 MiB). Only used in batch mode. */
  flushBytes?: number;
}

const DEFAULT_FLUSH_BYTES = 2 * 1024 * 1024; // 2 MiB

/**
 * DataChannel wraps data upload for a single leg.
 *
 * Two modes:
 * - **Streaming** (`streaming: true`): A single long-lived POST with a
 *   ReadableStream body (duplex:"half"). Frames are enqueued directly.
 * - **Batch** (`streaming: false`, default): Frames are buffered in memory and
 *   flushed as sequential POST requests when the buffer exceeds `flushBytes`
 *   or the channel is closed.
 */
export interface DataCommitResult {
  files: number;
  bytes: number;
}

export class DataChannel {
  private _closed = false;
  private _failed = false;
  private _bytesSent = 0;
  private _onError: ((err: MfupError) => void) | null = null;
  private readonly _url: string;
  private readonly _streaming: boolean;
  private readonly _flushBytes: number;

  /** Commit result parsed from the final data POST response. */
  commitResult: DataCommitResult | null = null;

  // --- Streaming mode state ---
  private _streamController: ReadableStreamDefaultController<Uint8Array> | null = null;
  private _streamFetchPromise: Promise<Response> | null = null;
  /** Resolves when the stream's internal buffer drains below highWaterMark. */
  private _drainResolve: (() => void) | null = null;

  // --- Batch mode state ---
  private _batchBuffer: Uint8Array[] = [];
  private _batchBufferBytes = 0;
  private _seq = 0;
  /** Chain of in-flight POST promises — ensures serialisation. */
  private _flushChain: Promise<void> = Promise.resolve();
  /** Resolves when close() has flushed everything. */
  private _doneResolve: (() => void) | null = null;
  private _donePromise: Promise<void> | null = null;

  constructor(private opts: DataChannelOpts) {
    this._url = `${opts.baseUrl}/mfup/data/${opts.sessionId}/${opts.legId}`;
    this._streaming = opts.streaming ?? false;
    this._flushBytes = opts.flushBytes ?? DEFAULT_FLUSH_BYTES;
  }

  get closed(): boolean { return this._closed; }
  get failed(): boolean { return this._failed; }
  get bytesSent(): number { return this._bytesSent; }

  /** Register an error callback for async errors (HTTP response, network). */
  onError(fn: (err: MfupError) => void): void { this._onError = fn; }

  // -------------------------------------------------------------------------
  // open()
  // -------------------------------------------------------------------------

  /**
   * Open the data channel.
   *
   * - Streaming: returns a promise that resolves with the fetch Response when
   *   the upload body is fully sent or the server closes.
   * - Batch: returns a promise that resolves when close() finishes all flushes.
   */
  open(): Promise<Response | void> {
    if (this._streaming) {
      return this._openStreaming();
    }
    return this._openBatch();
  }

  private _openStreaming(): Promise<Response> {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const self = this;
    const stream = new ReadableStream<Uint8Array>({
      start(ctrl) {
        self._streamController = ctrl;
      },
      pull() {
        // Consumer (fetch/network) is ready for more data — unblock drain().
        if (self._drainResolve) {
          self._drainResolve();
          self._drainResolve = null;
        }
      },
    }, new ByteLengthQueuingStrategy({ highWaterMark: 4 * 1024 * 1024 }));

    try {
      this._streamFetchPromise = fetch(`${this._url}?seq=0&final=1&epoch=${this.opts.epoch}`, {
        method: "POST",
        headers: { "Content-Type": "application/x-mfup", "X-MFUP-Token": this.opts.resumeToken },
        body: stream,
        // @ts-expect-error — duplex: "half" is required for streaming uploads
        duplex: "half",
        signal: this.opts.signal,
      });
    } catch (err) {
      const mfupErr = dataOpenFailed(this._url, err);
      this._onError?.(mfupErr);
      throw mfupErr;
    }

    return this._streamFetchPromise;
  }

  private _openBatch(): Promise<void> {
    this._donePromise = new Promise<void>((resolve) => {
      this._doneResolve = resolve;
    });
    return this._donePromise;
  }

  // -------------------------------------------------------------------------
  // write()
  // -------------------------------------------------------------------------

  /**
   * Enqueue a data frame. Always synchronous.
   *
   * - Streaming: encodes and enqueues into the ReadableStream controller.
   * - Batch: encodes and appends to the internal buffer.
   */
  write(frame: DataFrame): void {
    if (this._closed) {
      const err = dataChannelClosed();
      this._onError?.(err);
      throw err;
    }

    const encoded = encodeFrame(frame);

    if (this._streaming) {
      this._writeStreaming(encoded);
    } else {
      this._writeBatch(encoded);
    }
  }

  private _writeStreaming(encoded: Uint8Array): void {
    if (!this._streamController) {
      const err = dataWriteFailed("DataChannel not opened yet");
      this._onError?.(err);
      throw err;
    }
    try {
      this._streamController.enqueue(encoded);
      this._bytesSent += encoded.byteLength;
    } catch (cause) {
      const err = dataWriteFailed("enqueue failed", cause);
      this._onError?.(err);
      throw err;
    }
  }

  private _writeBatch(encoded: Uint8Array): void {
    this._batchBuffer.push(encoded);
    this._batchBufferBytes += encoded.byteLength;
  }

  // -------------------------------------------------------------------------
  // drain()
  // -------------------------------------------------------------------------

  /**
   * Backpressure gate.
   *
   * - Streaming: waits when the ReadableStream's internal buffer exceeds its
   *   highWaterMark (desiredSize <= 0). Resumes when the pull() callback fires,
   *   meaning the network has consumed enough data. This prevents memory from
   *   growing unboundedly when the disk is faster than the network.
   * - Batch: if the buffer exceeds flushBytes, flushes it as a POST.
   */
  async drain(): Promise<void> {
    if (this._streaming) {
      const ctrl = this._streamController;
      if (ctrl && ctrl.desiredSize !== null && ctrl.desiredSize <= 0) {
        await new Promise<void>((resolve) => {
          this._drainResolve = resolve;
        });
      }
      return;
    }
    if (this._batchBufferBytes >= this._flushBytes) {
      await this._flushBatch(false);
    }
  }

  // -------------------------------------------------------------------------
  // close()
  // -------------------------------------------------------------------------

  /**
   * Gracefully close the data channel.
   *
   * - Streaming: closes the ReadableStream controller.
   * - Batch: flushes remaining buffer with final=1, resolves when last POST completes.
   */
  async close(): Promise<void> {
    if (this._closed) return;
    this._closed = true;
    // Unblock any pending drain() so pump loop can finish
    this._drainResolve?.();
    this._drainResolve = null;

    if (this._streaming) {
      this._streamController?.close();
    } else {
      await this._flushBatch(true);
      this._doneResolve?.();
    }
  }

  // -------------------------------------------------------------------------
  // abort()
  // -------------------------------------------------------------------------

  /**
   * Abort the data channel.
   */
  abort(reason?: string): void {
    if (this._closed) return;
    this._closed = true;
    // Unblock any pending drain()
    this._drainResolve?.();
    this._drainResolve = null;

    if (this._streaming) {
      this._streamController?.error(new Error(reason ?? "aborted"));
    } else {
      // Clear buffer; resolve done promise so open() doesn't hang.
      this._batchBuffer = [];
      this._batchBufferBytes = 0;
      this._doneResolve?.();
    }
  }

  // -------------------------------------------------------------------------
  // Batch internals
  // -------------------------------------------------------------------------

  /**
   * Concatenate the buffer and send it as a single POST.
   * POSTs are serialised via _flushChain — never two in flight.
   */
  private _flushBatch(final: boolean): Promise<void> {
    const chunks = this._batchBuffer;
    const totalBytes = this._batchBufferBytes;
    this._batchBuffer = [];
    this._batchBufferBytes = 0;

    // Nothing to send and not final — skip.
    if (totalBytes === 0 && !final) return Promise.resolve();

    const body = this._concatChunks(chunks, totalBytes);
    const seq = this._seq++;
    const finalFlag = final ? 1 : 0;
    const url = `${this._url}?seq=${seq}&final=${finalFlag}&epoch=${this.opts.epoch}`;

    this._flushChain = this._flushChain.then(() => this._postWithRetry(url, body, seq, final));

    return this._flushChain;
  }

  /**
   * POST one batch with transport-level retries.
   *
   * The server processes buffered batch POSTs atomically and advances its
   * seq counter only after full processing, so retrying a failed POST with
   * the SAME seq is safe. Retry on network errors and 5xx; a 409
   * seq_mismatch with expected == seq + 1 means the previous attempt was in
   * fact fully delivered — treat as success. Without this, frames inside a
   * failed POST (e.g. NODE metadata killed by a flaky proxy) were silently
   * lost, which is exactly the hole that produced COMMIT_OK on an
   * incomplete tree.
   */
  private async _postWithRetry(url: string, body: Uint8Array, seq: number, final: boolean): Promise<void> {
    const MAX_ATTEMPTS = 4;
    const RETRY_DELAYS = [500, 1000, 2000];
    let lastErr: MfupError | null = null;

    for (let attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
      if (this.opts.signal?.aborted) break;
      if (attempt > 0) {
        await new Promise((r) => setTimeout(r, RETRY_DELAYS[attempt - 1] ?? 2000));
        if (this.opts.signal?.aborted) break;
      }
      try {
        const resp = await fetch(url, {
          method: "POST",
          headers: { "Content-Type": "application/x-mfup", "X-MFUP-Token": this.opts.resumeToken },
          body: body as unknown as BodyInit,
          signal: this.opts.signal,
        });

        if (resp.ok) {
          if (final) {
            // Parse commit result from final POST response
            try {
              const json = await resp.json();
              if (json.commit) {
                this.commitResult = { files: json.commit.files, bytes: json.commit.bytes };
              }
              if (json.error) {
                this._onError?.(dataHttpError(url, resp.status, "commit_error", json.error));
              }
            } catch { /* response parse failure — commit_ok via WS is the fallback */ }
          }
          this._bytesSent += body.byteLength;
          return;
        }

        const text = await resp.text().catch(() => "");
        if (resp.status === 409 && attempt > 0) {
          // Did our previous (failed-looking) attempt actually land?
          try {
            const json = JSON.parse(text);
            if (json.error === "seq_mismatch" && json.expected === seq + 1) {
              this._bytesSent += body.byteLength;
              return; // already delivered
            }
          } catch { /* not JSON — fall through */ }
        }

        lastErr = dataHttpError(url, resp.status, resp.statusText, text);
        if (resp.status >= 500 && attempt < MAX_ATTEMPTS - 1) {
          continue; // transient server/proxy error — retry same seq
        }
        break; // 4xx/409 — not retryable
      } catch (cause) {
        lastErr = dataWriteFailed(`POST to ${url} failed`, cause);
        // network error — retry
      }
    }

    this._failed = true;
    if (lastErr) this._onError?.(lastErr);
  }

  private _concatChunks(chunks: Uint8Array[], totalBytes: number): Uint8Array {
    if (chunks.length === 1) return chunks[0];
    const out = new Uint8Array(totalBytes);
    let offset = 0;
    for (const c of chunks) {
      out.set(c, offset);
      offset += c.byteLength;
    }
    return out;
  }
}
