import { MfupError, type SessionFailure } from "@mfup/client";
export function failureMessage(
  error: Pick<SessionFailure, "code" | "phase">,
): string {
  const messages: Record<string, string> = {
    processing_failed:
      "The files were received, but server processing failed. Contact the service owner.",
    storage_full:
      "The server ran out of space. Retry the upload after space is available.",
    storage_unavailable:
      "The server could not read or write data. Retry after storage is available.",
    server_error:
      "The server could not complete the operation. Try again later.",
    network_error:
      "The connection was interrupted. Select the same files to resume when it is restored.",
    request_timeout:
      "The server did not respond in time. Check the connection and try again.",
    http_error: "The server is temporarily unavailable. Try again later.",
    bad_response: "The server returned an invalid response. Try again later.",
    quota_exceeded: "This upload exceeds the file count or size limit.",
    mapping_error:
      "The server could not prepare the file destinations. Contact the service owner.",
    processing_required: "The files are awaiting server processing.",
    server_publish_only: "Publication of this upload is managed by the server.",
    file_changed: "The source files have changed. Start a new upload.",
    denied:
      "The server did not allow this upload. Reload the page and try again.",
  };
  const detail =
    messages[error.code] ?? "The upload could not be completed. Try again.";
  return error.phase === "cancel"
    ? `Cancellation has not been confirmed by the server. ${detail}`
    : detail;
}
export function errorMessage(error: unknown): string {
  if (error instanceof MfupError)
    return failureMessage({ code: error.code, phase: error.phase ?? "upload" });
  return error instanceof Error &&
    ["Could not open the session", "Could not refresh the file list"].includes(
      error.message,
    )
    ? error.message
    : "The operation failed. Check the connection and try again.";
}
