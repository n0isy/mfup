import { MfupError } from "./types.js";

export interface RetryOptions {
  /** Additional attempts after the initial request. */
  retries?: number;
  /** First retry delay, in milliseconds. */
  retryDelayMs?: number;
  /** Maximum delay between retries, in milliseconds. */
  retryMaxDelayMs?: number;
}

export const DEFAULT_RETRIES = 1000;
export const DEFAULT_RETRY_DELAY_MS = 1000;
export const DEFAULT_RETRY_MAX_DELAY_MS = 36000;

export function validateRetryOptions(options: RetryOptions) {
  for (const value of [
    options.retries,
    options.retryDelayMs,
    options.retryMaxDelayMs,
  ])
    if (value !== undefined && (!Number.isSafeInteger(value) || value < 0))
      throw new MfupError(
        "bad_options",
        "Retry options must be nonnegative safe integers",
      );
}

/** The exponent is bounded before multiplication, including very large retry budgets. */
export function retryDelay(attempt: number, options: RetryOptions): number {
  const first = options.retryDelayMs ?? DEFAULT_RETRY_DELAY_MS;
  const cap = options.retryMaxDelayMs ?? DEFAULT_RETRY_MAX_DELAY_MS;
  return Math.min(cap, first * 2 ** Math.min(attempt, 53));
}

export function isRetryable(error: unknown): boolean {
  if (!(error instanceof MfupError)) return false;
  if (error.retryable === false) return false;
  return (
    [0, 408, 429, 500, 502, 503, 504].includes(error.status) ||
    (error.status === 409 &&
      ["busy", "range_busy", "stale_epoch"].includes(error.code))
  );
}
