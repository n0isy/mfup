import * as path from "node:path";
import { pathToFileURL } from "node:url";
import type { Options } from "./contracts.js";

export async function configFromEnv(
  env: NodeJS.ProcessEnv = process.env,
): Promise<Options> {
  async function hook(spec: string | undefined, required = false) {
    if (!spec) {
      if (required) throw new Error("MFUP_AUTHORIZE is required");
      return undefined;
    }
    const at = spec.lastIndexOf("#");
    const module = at < 0 ? spec : spec.slice(0, at);
    const name = at < 0 ? "default" : spec.slice(at + 1);
    const location =
      module.startsWith(".") || path.isAbsolute(module)
        ? pathToFileURL(path.resolve(module)).href
        : module;
    const value = (await import(location))[name];
    if (typeof value !== "function")
      throw new Error(`Hook is not callable: ${spec}`);
    return value;
  }
  function number(name: string, fallback: number) {
    const n = env[name] === undefined ? fallback : Number(env[name]);
    if (!Number.isSafeInteger(n) || n < 0) throw new Error(`Invalid ${name}`);
    return n;
  }
  function boolean(name: string, fallback: boolean) {
    const value = env[name]?.toLowerCase();
    if (value === undefined) return fallback;
    if (!["true", "false", "1", "0"].includes(value))
      throw new Error(`Invalid ${name}`);
    return value === "true" || value === "1";
  }
  return {
    baseDir: env.MFUP_BASE_DIR ?? "./data",
    authorize: await hook(env.MFUP_AUTHORIZE, true),
    mapFile: await hook(env.MFUP_MAP_FILE),
    onCommitted: await hook(env.MFUP_ON_COMMITTED),
    prefix: env.MFUP_PREFIX ?? "",
    ttlMs: number("MFUP_TTL_MS", 86400000),
    sweepIntervalMs: number("MFUP_SWEEP_INTERVAL_MS", 60000),
    maxMetaBytes: number("MFUP_MAX_META_BYTES", 16384),
    maxContextBytes: number("MFUP_MAX_CONTEXT_BYTES", 65536),
    autoPublish: boolean("MFUP_AUTO_PUBLISH", false),
    clientPublish: boolean("MFUP_CLIENT_PUBLISH", true),
    limits: {
      concurrency: number("MFUP_CONCURRENCY", 6),
      maxParts: number("MFUP_MAX_PARTS", 128),
      batchBytes: number("MFUP_BATCH_BYTES", 32 * 1024 ** 2),
      partBytes: number("MFUP_PART_BYTES", 16 * 1024 ** 2),
    },
  };
}
