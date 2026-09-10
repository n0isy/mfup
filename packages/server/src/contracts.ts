export type Awaitable<T> = T | Promise<T>;
export interface RequestContext {
  client?: string;
  query?: Record<string, string>;
}
export interface AuthRequest extends RequestContext {
  sessionId: string;
  headers: Record<string, string>;
  targetDir: string;
  meta: unknown;
}
export interface AuthResult {
  baseDir?: string;
  targetDir?: string;
  maxFiles?: number;
  maxTotalBytes?: number;
  context?: Record<string, unknown>;
  /** Automatically attempt publication after successful processing. */
  autoPublish?: boolean;
  /** Allow the browser to request publication. Backend publish remains available. */
  clientPublish?: boolean;
}
export interface CommitEvent {
  sessionId: string;
  targetDir: string;
  baseDir: string;
  stagingDir: string;
  files: number;
  bytes: number;
  context: Record<string, unknown>;
  meta: unknown;
}
export interface FileMapRequest {
  sessionId: string;
  path: string;
  name: string;
  size: number;
  targetDir: string;
  context: Record<string, unknown>;
  meta: unknown;
}
export interface StagedFile {
  path: string;
  size: number;
  mtime: number;
  localPath: string;
}
export interface Limits {
  concurrency: number;
  maxParts: number;
  batchBytes: number;
  partBytes: number;
}
export interface Options {
  baseDir: string;
  limits?: Partial<Limits>;
  ttlMs?: number;
  sweepIntervalMs?: number;
  maxMetaBytes?: number;
  maxContextBytes?: number;
  prefix?: string;
  autoPublish?: boolean;
  clientPublish?: boolean;
  authorize: (request: AuthRequest) => Awaitable<AuthResult | null>;
  mapFile?: (file: FileMapRequest) => Awaitable<string | null>;
  /** true publishes, false keeps staged; undefined uses the session autoPublish setting. */
  onCommitted?: (event: CommitEvent) => Awaitable<boolean | void>;
  onError?: (event: {
    hook: string;
    sessionId: string;
    error: unknown;
  }) => void;
}
