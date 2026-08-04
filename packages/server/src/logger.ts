/** Minimal leveled logger. Consumers may replace the sink (e.g. pino). */

export type LogLevel = "debug" | "info" | "warn" | "error";
export type LogSink = (level: LogLevel, name: string, message: string) => void;

const LEVEL_ORDER: Record<LogLevel, number> = { debug: 10, info: 20, warn: 30, error: 40 };

let minLevel: LogLevel =
  (process.env.MFUP_LOG_LEVEL as LogLevel | undefined) &&
  LEVEL_ORDER[process.env.MFUP_LOG_LEVEL as LogLevel] !== undefined
    ? (process.env.MFUP_LOG_LEVEL as LogLevel)
    : "info";

let sink: LogSink = (level, name, message) => {
  const line = `${new Date().toISOString()} ${name} ${level.toUpperCase()} ${message}`;
  if (level === "error") console.error(line);
  else if (level === "warn") console.warn(line);
  else console.log(line);
};

export function setLogSink(s: LogSink): void {
  sink = s;
}

export function setLogLevel(level: LogLevel): void {
  minLevel = level;
}

export interface Logger {
  debug(msg: string): void;
  info(msg: string): void;
  warn(msg: string): void;
  error(msg: string): void;
}

export function createLogger(name: string): Logger {
  const emit = (level: LogLevel, msg: string) => {
    if (LEVEL_ORDER[level] >= LEVEL_ORDER[minLevel]) sink(level, name, msg);
  };
  return {
    debug: (m) => emit("debug", m),
    info: (m) => emit("info", m),
    warn: (m) => emit("warn", m),
    error: (m) => emit("error", m),
  };
}
