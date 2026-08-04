/**
 * MFUP/2 extension hooks — consumer policy surface.
 *
 * In JS a hook is always a callable (no dotted-path loading needed):
 *
 *     createMfup({ authorize: async (req) => ({ baseDir: ..., targetDir: ... }) })
 *
 * Contracts mirror server/mfup-core/mfup_core/hooks.py.
 */

/** Everything the server knows about an upload attempt at HELLO time. */
export interface AuthRequest {
  sessionId: string;
  targetDir: string;
  /** HTTP headers of the WebSocket handshake (cookies, authorization, …).
   * Keys are lowercase. */
  headers: Record<string, string>;
  /** Client address ("ip:port" or ""). */
  client: string;
  /** Query parameters of the WebSocket URL. */
  query: Record<string, string>;
  /** Arbitrary JSON the CONSUMER'S FRONTEND attached to the session
   * (MfupSessionConfig.meta → HELLO.meta). Untrusted client input — the
   * hook validates it. Typical use: upload scope/purpose ("avatars",
   * {album_id: 123}) that authorization and per-file mapping key on. */
  meta?: unknown;
}

/** Permission plus per-session constraints. All limits optional. */
export interface AuthResult {
  /** Cap on total accepted payload bytes; exceeding aborts the session
   * with SESSION_ABORT(quota_exceeded). undefined = unlimited. */
  maxTotalBytes?: number;
  /** Cap on the number of file nodes; exceeding aborts likewise. */
  maxFiles?: number;
  /** Per-session BASE directory (absolute path) — e.g. the user's home.
   * Overrides the global baseDir for this session: the staging dir is
   * created inside it (publish stays a same-filesystem rename even when
   * homes live on their own mount), relative targetDir resolves against it,
   * and the containment check confines the session to it. Created if
   * missing. */
  baseDir?: string;
  /** Optional override of the client-requested targetDir (pin or rewrite:
   * `targetDir: "incoming/" + req.targetDir`). Escapes are impossible
   * regardless: the resolved target must stay within the session's baseDir
   * or HELLO is refused (bad_target_dir). */
  targetDir?: string;
  /** Free-form bag to correlate sessions with users; delivered to the
   * mapFile/onCommitted hooks, never sent to the client. Should be
   * JSON-serializable to survive restarts. */
  context?: Record<string, unknown>;
}

/**
 * Called once per HELLO, before the session is created. Return:
 *   - AuthResult — allow, optionally constraining the session;
 *   - null/undefined — deny → the client gets SESSION_ABORT(auth_failed).
 * Throwing is treated as a deny (logged server-side, generic reason sent).
 *
 * If no authorize hook is configured the server runs ALLOW-ALL and logs a
 * warning at startup — acceptable for development, not for production.
 */
export type AuthorizeHook = (
  req: AuthRequest,
) => Promise<AuthResult | null | undefined> | AuthResult | null | undefined;

/** One file about to be published — input to the mapFile hook. */
export interface FileMapRequest {
  sessionId: string;
  /** Path of the file inside the uploaded tree, "/"-separated, as the
   * client sent it (e.g. "photos/2024/img_001.jpg"). */
  path: string;
  /** Basename convenience (last segment of `path`). */
  name: string;
  /** Actual size on disk, bytes. */
  size: number;
  /** The session's targetDir (already authorized/mapped at HELLO). */
  targetDir: string;
  /** Client-attached session meta (see AuthRequest.meta). Untrusted. */
  meta: unknown;
  /** AuthResult.context from the authorize hook. */
  context: Record<string, unknown>;
}

/**
 * Runs once per file at PUBLISH time. Return:
 *   - string — new path RELATIVE to targetDir (e.g. "media/img_001.jpg");
 *   - null/undefined — keep the client's layout for this file.
 * Two files mapping to the same destination is a consumer bug → publish
 * fails with mapping_conflict; escaping segments ("..", absolute, "\\")
 * fail publish likewise. Because it runs at publish it does not need to be
 * deterministic across transfer retries.
 */
export type MapFileHook = (
  req: FileMapRequest,
) => Promise<string | null | undefined> | string | null | undefined;

/** A session just committed — input to the onCommitted hook. */
export interface CommitEvent {
  sessionId: string;
  /** The session's (authorized/mapped) targetDir. */
  targetDir: string;
  /** The session's base directory (per-user home or the global base). */
  baseDir: string;
  /** Absolute staging directory holding the committed payload. */
  stagingDir: string;
  /** Committed file count / total payload bytes (as sent in COMMIT_OK). */
  files: number;
  bytes: number;
  /** Client-attached session meta (see AuthRequest.meta). Untrusted. */
  meta: unknown;
  /** AuthResult.context from the authorize hook. */
  context: Record<string, unknown>;
}

/**
 * Fires right after a session commits. Return:
 *   - "publish" — the server publishes immediately (server-side decision:
 *     scan passed, billing ok, …). The browser's own publish call, if any,
 *     will find the session gone (404) — harmless.
 *   - null/undefined — do nothing; publish stays client-driven (or the
 *     consumer backend calls engine.publish() later).
 * Throwing is logged and treated as null — a broken consumer hook must not
 * strand committed sessions.
 */
export type OnCommittedHook = (
  ev: CommitEvent,
) => Promise<string | null | undefined> | string | null | undefined;
