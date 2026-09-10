export const PROTOCOL = "MFUP/3";
export interface Limits {
  concurrency: number;
  maxParts: number;
  batchBytes: number;
  partBytes: number;
}
export interface Ticket {
  id: string;
  token: string;
  epoch: number;
  limits: Limits;
}
export type Choice = "overwrite" | "cancel";
export interface Question {
  id: string;
  path?: string;
  message: string;
  choices: Choice[];
  answer: Choice | null;
}
export type ProcessingState =
  "none" | "pending" | "running" | "done" | "failed";
export interface SessionFailure {
  code: string;
  status: number;
  phase: string;
  retryable: boolean;
}
export interface RemoteState {
  overwrite?: boolean;
  overwriteRequired?: boolean;
  error?: SessionFailure | null;
  clientPublish?: boolean;
  processing?: ProcessingState;
  id: string;
  epoch: number;
  state: "uploading" | "committed" | "publishing" | "published" | "cancelled";
  files: number;
  bytes: number;
  asks: Question[];
  published: string[];
}
export interface ResumeFile {
  path: string;
  size: number;
  mtime: number;
  offsets: number[];
}
/** path, full size, lastModified, range offset, range length */
export type FilePart = [string, number, number, number, number];
export interface Manifest {
  files: FilePart[];
  dirs: string[];
}
export interface Receipt {
  id: string;
  parts: number;
  bytes: number;
}
export interface Entry {
  path: string;
  kind: "file" | "directory";
  file?: File;
}
export type Source = AsyncIterable<Entry> | Iterable<Entry>;
export interface Snapshot {
  overwrite?: boolean;
  overwriteRequired?: boolean;
  errorInfo?: SessionFailure | null;
  processing?: ProcessingState;
  clientPublish?: boolean;
  state:
    | "idle"
    | "connecting"
    | "uploading"
    | "paused"
    | "waiting"
    | "committed"
    | "published"
    | "cancelled"
    | "cancelling"
    | "failed";
  discovered: number;
  confirmedBytes: number;
  /** Confirmed payload plus an estimate of payload sent in active requests. */
  sentBytes: number;
  totalBytes: number;
  activeRequests: number;
  maxActiveRequests: number;
  batches: number;
  scanDone: boolean;
  asks: Question[];
  error: string | null;
}
export class MfupError extends Error {
  constructor(
    public code: string,
    message: string,
    public status = 0,
    public retryable?: boolean,
    public phase?: string,
  ) {
    super(message);
    this.name = "MfupError";
  }
}
