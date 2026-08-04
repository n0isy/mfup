// @mfup/react — React bindings for the MFUP/2 upload client.

export { MfupProvider, useMfupConfig } from "./context.js";
export type { MfupConfigValue } from "./context.js";

export { useMfupSession } from "./useMfupSession.js";

export { useMfupUpload } from "./useMfupUpload.js";
export type { UseMfupUploadOptions, UseMfupUploadResult } from "./useMfupUpload.js";

export { useMfupDropzone } from "./useMfupDropzone.js";
export type { UseMfupDropzoneOptions, MfupDropzoneRootProps } from "./useMfupDropzone.js";

// Re-export the client types a React app touches constantly, so simple apps
// need only one import.
export type {
  MfupAsk,
  MfupAskAction,
  MfupError,
  MfupFileRef,
  MfupSession,
  MfupSessionSnapshot,
  ProgressSnapshot,
  SessionState,
  UploadSource,
} from "@mfup/client";
