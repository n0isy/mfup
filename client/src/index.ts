// MFUP/2 client library — public API

export { PROTOCOL_VERSION, ROOT_NODE_ID } from "./protocol.js";

// Protocol types
export type {
  DataFrame,
  NodeFrame,
  SummaryFrame,
  FileOpenFrame,
  FileChunkFrame,
  FileCloseFrame,
  DirCloseFrame,
  SessionEndFrame,
  ClientAbortFrame,
  ClientControlMsg,
  ServerControlMsg,
  HelloMsg,
  ResumeMsg,
  HelloOkMsg,
  ResumeOkMsg,
  FileAckMsg,
  NackChunkMsg,
  FlowMsg,
  PruneNodeMsg,
  RejectFileMsg,
  SessionAbortMsg,
  CommitOkMsg,
  ServerLimits,
  RootSummary,
  ResumeFileStatus,
  SessionState,
} from "./protocol.js";

export {
  FrameTag,
  NodeKind,
  ChecksumKind,
  crc32c,
  encodeFrame,
  encodeNodeFrame,
  encodeSummaryFrame,
  encodeFileOpenFrame,
  encodeFileChunkFrame,
  encodeFileCloseFrame,
  encodeDirCloseFrame,
  encodeSessionEndFrame,
  encodeClientAbortFrame,
} from "./protocol.js";

// Control channel
export { ControlChannel } from "./control.js";
export type { ControlChannelOpts, ControlEventMap } from "./control.js";

// Data channel
export { DataChannel } from "./data-channel.js";
export type { DataChannelOpts, DataCommitResult } from "./data-channel.js";

// Ingestion adapters
export {
  NodeIdAllocator,
  ingestFromHandles,
  ingestFromEntries,
  ingestFromFileList,
  ingestFromFiles,
} from "./ingestion.js";
export type { DiscoveredNode, IngestCallback, IngestFilter } from "./ingestion.js";

// Progress
export { ProgressTracker } from "./progress.js";
export type { ProgressSnapshot, ProgressListener } from "./progress.js";

// Probe
export { probeStreaming, type ProbeResult } from "./probe.js";

// Errors
export { MfupError, MfupErrorCode, MfupErrorLayer } from "./errors.js";

// Session (main entry point)
export { MfupSession } from "./session.js";
export type { MfupSessionConfig, MfupSessionEvents } from "./session.js";
