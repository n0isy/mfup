import { spawn } from "node:child_process";
import { createServer } from "node:http";
import { fileURLToPath } from "node:url";
import path from "node:path";
import fs from "node:fs";
const root = fileURLToPath(new URL("../", import.meta.url));
const backend = process.env.MFUP_BACKEND ?? "node";
const port = process.env.MFUP_TEST_PORT ?? "20063";
const data =
  process.env.MFUP_DATA_DIR ?? path.join(root, ".tmp", `e2e-${backend}`);
const env = {
  ...process.env,
  MFUP_TEST: "1",
  MFUP_DATA_DIR: data,
  PORT: port,
  HOST: "127.0.0.1",
  MFUP_PART_BYTES: "1048576",
  MFUP_BATCH_BYTES: "8388608",
  MFUP_MAX_PARTS: "32",
  PYTHONPATH: [
    path.join(root, "server/mfup-core"),
    path.join(root, "server/mfup-fastapi"),
  ].join(path.delimiter),
};
const localPython = path.join(
  root,
  ".venv",
  process.platform === "win32" ? "Scripts/python.exe" : "bin/python",
);
const python =
  process.env.PYTHON_BIN ??
  (fs.existsSync(localPython) ? localPython : "python");
const children = new Set();
function child(command, args, extra = {}) {
  const p = spawn(command, args, {
    cwd: root,
    env: { ...env, ...extra },
    stdio: "inherit",
  });
  children.add(p);
  p.on("error", (e) => console.error(e));
  p.on("exit", () => children.delete(p));
  return p;
}
function start() {
  if (backend === "python")
    child(python, [
      "-m",
      "uvicorn",
      "scripts.python_dev:app",
      "--host",
      "127.0.0.1",
      "--port",
      "20064",
      "--log-level",
      "warning",
    ]);
  child(
    process.execPath,
    ["--import", "tsx", "scripts/dev.ts"],
    backend === "python" ? { MFUP_BACKEND_URL: "http://127.0.0.1:20064" } : {},
  );
}
async function stop() {
  await Promise.all(
    [...children].map(
      (p) =>
        new Promise((resolve) => {
          p.once("exit", resolve);
          p.kill("SIGTERM");
          setTimeout(() => {
            p.kill("SIGKILL");
            resolve();
          }, 5000).unref();
        }),
    ),
  );
}
async function ready() {
  for (let i = 0; i < 100; i++) {
    try {
      const r = await fetch(`http://127.0.0.1:${port}/mfup/health`);
      if (r.ok) return;
    } catch {}
    await new Promise((r) => setTimeout(r, 100));
  }
  throw new Error("Backend did not start");
}
let restarting = false;
const control = createServer(async (req, res) => {
  if (req.method !== "POST" || req.url !== "/restart" || restarting) {
    res.writeHead(404);
    res.end();
    return;
  }
  restarting = true;
  try {
    await stop();
    start();
    await ready();
    res.end("{}");
  } catch (e) {
    res.writeHead(500);
    res.end(String(e));
  } finally {
    restarting = false;
  }
});
control.listen(20065, "127.0.0.1");
start();
async function close() {
  control.close();
  await stop();
  process.exit(0);
}
process.on("SIGTERM", close);
process.on("SIGINT", close);
