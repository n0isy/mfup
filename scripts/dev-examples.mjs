import { spawn } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";
const root = fileURLToPath(new URL("../", import.meta.url));
const backend = process.env.MFUP_BACKEND ?? "node";
const apiPort = process.env.EXAMPLE_API_PORT ?? "3001";
const children = new Set();
function child(command, args, env = {}) {
  const p = spawn(command, args, {
    cwd: root,
    env: { ...process.env, ...env },
    stdio: "inherit",
  });
  children.add(p);
  p.on("error", (error) => {
    console.error(error.message);
    void close(1);
  });
  p.on("exit", (code) => {
    children.delete(p);
    if (!closing) void close(code ?? 1);
  });
}
const data =
  process.env.MFUP_EXAMPLE_DATA ??
  path.join(root, "data", `example-${backend}`);
const localPython = path.join(
  root,
  ".venv",
  process.platform === "win32" ? "Scripts/python.exe" : "bin/python",
);
let closing = false;
if (backend === "python") {
  child(
    process.env.PYTHON_BIN ??
      (fs.existsSync(localPython) ? localPython : "python"),
    [
      "-m",
      "uvicorn",
      "app:create_example",
      "--factory",
      "--app-dir",
      "examples/multiuser-scopes/server",
      "--host",
      "127.0.0.1",
      "--port",
      apiPort,
      "--log-level",
      "warning",
    ],
    { DEMO_DATA_DIR: data },
  );
} else {
  child(
    process.execPath,
    [
      ...(process.env.MFUP_EXAMPLE_TEST ? [] : ["--watch"]),
      "examples/multiuser-scopes-node/server/server.mjs",
    ],
    { PORT: apiPort, HOST: "127.0.0.1", DEMO_DATA_DIR: data },
  );
}
const require = createRequire(
  new URL("../examples/multiuser-scopes/client/package.json", import.meta.url),
);
const vite = path.join(
  path.dirname(require.resolve("vite/package.json")),
  "bin/vite.js",
);
child(
  process.execPath,
  [
    vite,
    "examples/multiuser-scopes/client",
    "--config",
    "examples/multiuser-scopes/client/vite.config.ts",
  ],
  { EXAMPLE_BACKEND_URL: `http://127.0.0.1:${apiPort}` },
);
async function close(code = 0) {
  if (closing) return;
  closing = true;
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
  process.exit(code);
}
process.on("SIGINT", () => void close());
process.on("SIGTERM", () => void close());
