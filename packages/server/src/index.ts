/**
 * @mfup/server — MFUP/2 upload server for Node.
 *
 *     import { createMfup } from "@mfup/server";
 *
 *     const mfup = createMfup({ baseDir: "/srv/uploads", authorize });
 *     app.use("/api/uploads", mfup.middleware);   // express / connect / vite
 *     mfup.attach(app.listen(8090));              // WebSocket upgrade
 */

// The universal handler
export { createMfup, type Mfup } from "./handler.js";

// Options + engine (programmatic publish, sweep, registry)
export {
  MfupEngine,
  resolveOptions,
  type MfupOptions,
  type ResolvedMfupOptions,
  PublishError,
  SessionNotFound,
  NotCommitted,
  TargetEscapes,
  MapFileHookError,
  reconcileOrphans,
  isSafeTarget,
} from "./engine.js";

// Hook contracts
export type {
  AuthRequest,
  AuthResult,
  AuthorizeHook,
  FileMapRequest,
  MapFileHook,
  CommitEvent,
  OnCommittedHook,
} from "./hooks.js";

// Session store
export { MemoryStore, resolveStore, type SessionStore, type SessionMeta } from "./store.js";
export { RedisStore } from "./store-redis.js";

// Protocol primitives (for tests / advanced integrations)
export {
  PROTOCOL_VERSION,
  ROOT_NODE_ID,
  CRC32C_IMPL,
  FrameTag,
  NodeKind,
  ChecksumKind,
  SessionState,
  NodeStatus,
  FrameReader,
  decodeFramePayload,
  crc32c,
  type Frame,
  type NodeFrame,
  type SummaryFrame,
  type FileOpenFrame,
  type FileChunkFrame,
  type FileCloseFrame,
  type DirCloseFrame,
  type SessionEndFrame,
  type ClientAbortFrame,
} from "./protocol.js";

// Storage / publish building blocks (consumer backends, tests)
export {
  SessionDB,
  DEFAULT_STAGING_PREFIX,
  stagingDir,
  ensureStaging,
  openSessionDb,
  validateNodeName,
  resolvePayloadPath,
} from "./storage.js";
export {
  ConflictError,
  MappingError,
  listPayloadFiles,
  publishSession,
  publishSessionMapped,
  validateMappedPath,
  detectConflicts,
} from "./publish.js";
export {
  LiveSession,
  SessionRegistry,
  FileWriter,
  MAX_COMMIT_RETRIES,
  SessionUnknownError,
  BadTokenError,
  BadStateError,
  type ControlSocket,
} from "./session.js";

export { setLogLevel, setLogSink, type LogLevel, type LogSink } from "./logger.js";
