// MFUP/2 progress tracker — merges scan work + accepted body bytes into a
// monotonic progress value with exact counters.

export interface ProgressSnapshot {
  /** Rolling scan units discovered so far */
  scanDoneUnits: bigint;
  /** Estimated total scan units (grows as scan progresses) */
  scanEstUnits: bigint;
  /** Bytes accepted by the server (from FILE_ACK) */
  bodyDoneBytes: bigint;
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
  private _bodyEst = 0n;

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
      bodyFrac = Number(this._bodyDone * 10000n / this._bodyEst) / 10000;
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
