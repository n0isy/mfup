// MFUP/2 progress tracker — merges scan work + accepted body bytes into a
// monotonic progress value with exact counters.

export interface ProgressSnapshot {
  /** Rolling scan units discovered so far */
  scanDoneUnits: bigint;
  /** Estimated total scan units (grows as scan progresses) */
  scanEstUnits: bigint;
  /** Bytes accepted by the server (from FILE_ACK) */
  bodyDoneBytes: bigint;
  /** Bytes handed to the transport (chunks written). Runs ahead of
   * bodyDoneBytes; drives smooth in-flight progress between server ACKs. */
  bodySentBytes: bigint;
  /** Estimated total body bytes (from size hints) */
  bodyEstBytes: bigint;
  /** Number of files fully accepted */
  acceptedFiles: number;
  /** Number of files skipped (pruned + rejected) */
  skippedFiles: number;
  /** Whether scan is complete */
  scanSealed: boolean;
  /** Session resume available until */
  expiresAt: string | null;
  /**
   * Blended 0–1 progress. Monotonic: never decreases even if estimates grow.
   * null before any data.
   */
  fraction: number | null;
}

export type ProgressListener = (snap: ProgressSnapshot) => void;

export class ProgressTracker {
  // Scan counters
  private _scanDone = 0n;
  private _scanEst = 0n;
  private _scanSealed = false;

  // Body counters
  private _bodyDone = 0n;
  private _bodySent = 0n;
  private _bodyEst = 0n;

  // Send-progress notifications are rate-limited (a chunk writer can tick
  // hundreds of times a second); trailing timer so the last value lands.
  private static readonly SENT_NOTIFY_MS = 200; // ≤5 updates/sec
  private _lastSentNotify = 0;
  private _sentTimer: ReturnType<typeof setTimeout> | null = null;

  // File counters
  private _acceptedFiles = 0;
  private _skippedFiles = 0;

  // Monotonicity
  private _lastFraction = 0;

  private _expiresAt: string | null = null;
  private _listeners = new Set<ProgressListener>();

  on(fn: ProgressListener): () => void {
    this._listeners.add(fn);
    return () => { this._listeners.delete(fn); };
  }

  // -- mutators (called by session orchestrator) ----------------------------

  updateScan(done: bigint, est: bigint, sealed: boolean): void {
    this._scanDone = done;
    this._scanEst = est;
    this._scanSealed = sealed;
    this.notify();
  }

  addBodyEstimate(bytes: bigint): void {
    this._bodyEst += bytes;
    this.notify();
  }

  setBodyAccepted(bytes: bigint): void {
    if (bytes > this._bodyDone) {
      this._bodyDone = bytes;
      this.notify();
    }
  }

  /** Cumulative bytes written to the transport. Fires listeners at most
   * every SENT_NOTIFY_MS so per-chunk calls stay cheap and UIs get a smooth
   * ~5 Hz motion instead of either a firehose or 2 MiB jumps. */
  setBodySent(total: bigint): void {
    if (total <= this._bodySent) return;
    this._bodySent = total;
    const now = Date.now();
    const elapsed = now - this._lastSentNotify;
    if (elapsed >= ProgressTracker.SENT_NOTIFY_MS) {
      this._lastSentNotify = now;
      this.notify();
    } else if (this._sentTimer === null) {
      this._sentTimer = setTimeout(() => {
        this._sentTimer = null;
        this._lastSentNotify = Date.now();
        this.notify();
      }, ProgressTracker.SENT_NOTIFY_MS - elapsed);
    }
  }

  /** Increment body-done by delta from a FILE_ACK */
  advanceBody(nodeId: number, acceptedOffset: bigint): void {
    // We track aggregate only; per-file tracking is in the session.
    // This is called with the absolute accepted offset, so the session
    // must compute the delta before calling.
  }

  acceptFile(): void { this._acceptedFiles++; this.notify(); }
  skipFile(): void { this._skippedFiles++; this.notify(); }

  /** Bulk-set file counts (used during resume to restore from server state) */
  setFileCounts(accepted: number, skipped: number): void {
    this._acceptedFiles = accepted;
    this._skippedFiles = skipped;
    this.notify();
  }

  setExpiresAt(v: string | null): void { this._expiresAt = v; this.notify(); }

  /** Bulk-set from a RESUME_OK root_summary */
  setFromRootSummary(s: { scan_done_units: number; scan_est_units: number; body_done_bytes: number; body_est_bytes: number; sealed: boolean }): void {
    this._scanDone = BigInt(s.scan_done_units);
    this._scanEst = BigInt(s.scan_est_units);
    this._bodyDone = BigInt(s.body_done_bytes);
    this._bodyEst = BigInt(s.body_est_bytes);
    this._scanSealed = s.sealed;
    this.notify();
  }

  // -- snapshot ------------------------------------------------------------

  snapshot(): ProgressSnapshot {
    return {
      scanDoneUnits: this._scanDone,
      scanEstUnits: this._scanEst,
      bodyDoneBytes: this._bodyDone,
      bodySentBytes: this._bodySent,
      bodyEstBytes: this._bodyEst,
      acceptedFiles: this._acceptedFiles,
      skippedFiles: this._skippedFiles,
      scanSealed: this._scanSealed,
      expiresAt: this._expiresAt,
      fraction: this.computeFraction(),
    };
  }

  // -- internal ------------------------------------------------------------

  private computeFraction(): number | null {
    if (this._scanEst === 0n && this._bodyEst === 0n) return null;

    // Blend: 10% for scan progress, 90% for body progress
    let scanFrac = 0;
    if (this._scanEst > 0n) {
      scanFrac = Number(this._scanDone * 10000n / this._scanEst) / 10000;
    } else if (this._scanSealed) {
      scanFrac = 1;
    }

    let bodyFrac = 0;
    if (this._bodyEst > 0n) {
      // Blend in transport progress: the bar moves WHILE a chunk is being
      // sent, not only when the server ACKs a whole 2 MiB batch. Sent bytes
      // are clamped to the estimate (retries can resend) and never move the
      // bar backwards (monotonic clamp below).
      const sent = this._bodySent > this._bodyEst ? this._bodyEst : this._bodySent;
      const ahead = sent > this._bodyDone ? sent : this._bodyDone;
      bodyFrac = Number(ahead * 10000n / this._bodyEst) / 10000;
    }

    const raw = 0.1 * scanFrac + 0.9 * bodyFrac;
    // Enforce monotonicity
    const clamped = Math.max(this._lastFraction, Math.min(1, raw));
    this._lastFraction = clamped;
    return clamped;
  }

  private notify(): void {
    const snap = this.snapshot();
    for (const fn of this._listeners) fn(snap);
  }
}
