import { MfupError, type Receipt } from "./types.js";

/** The browser still serializes native File/Blob parts; JS only observes byte counts. */
export function uploadMultipart(
  url: string,
  body: FormData,
  headers: Record<string, string>,
  signal: AbortSignal,
  progress: (fraction: number) => void,
): Promise<Receipt> {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    let finished = false;
    const abort = () => xhr.abort();
    const finish = (error?: unknown, receipt?: Receipt) => {
      if (finished) return;
      finished = true;
      signal.removeEventListener("abort", abort);
      if (error) reject(error);
      else resolve(receipt!);
    };
    if (signal.aborted) {
      finish(new DOMException("Upload aborted", "AbortError"));
      return;
    }
    xhr.upload.onprogress = (event) => {
      if (event.lengthComputable && event.total > 0)
        progress(Math.min(1, event.loaded / event.total));
    };
    xhr.upload.onload = () => progress(1);
    xhr.onload = () => {
      const data = xhr.response;
      if (xhr.status < 200 || xhr.status >= 300)
        finish(
          new MfupError(
            data?.error ?? "http_error",
            data?.message ?? xhr.statusText,
            xhr.status,
            data?.retryable,
            data?.phase,
          ),
        );
      else if (!data || typeof data.id !== "string")
        finish(new MfupError("bad_response", "Invalid upload receipt", 502));
      else finish(undefined, data);
    };
    xhr.onerror = () =>
      finish(new MfupError("network_error", "Upload connection failed"));
    xhr.ontimeout = () =>
      finish(new MfupError("request_timeout", "Upload timed out", 408));
    xhr.onabort = () =>
      finish(new DOMException("Upload aborted", "AbortError"));
    try {
      xhr.open("POST", url);
      xhr.responseType = "json";
      for (const [name, value] of Object.entries(headers))
        xhr.setRequestHeader(name, value);
      signal.addEventListener("abort", abort, { once: true });
      xhr.send(body);
    } catch (error) {
      finish(error);
    }
  });
}
