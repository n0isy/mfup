import { mkdtemp, readdir, writeFile, rm, copyFile } from "node:fs/promises";
import { spawnSync } from "node:child_process";
import path from "node:path";
import os from "node:os";
const npm = process.platform === "win32" ? "npm.cmd" : "npm";
function run(command, args, cwd) {
  const result = spawnSync(command, args, {
    cwd,
    stdio: "inherit",
    shell: process.platform === "win32",
  });
  if (result.status !== 0) throw new Error(`${command} failed`);
}
const temp = await mkdtemp(path.join(os.tmpdir(), "mfup3-pack-"));
try {
  run(
    npm,
    [
      "pack",
      "-w",
      "@mfup/client",
      "-w",
      "@mfup/react",
      "-w",
      "@mfup/server",
      "--pack-destination",
      temp,
    ],
    process.cwd(),
  );
  await writeFile(
    path.join(temp, "package.json"),
    JSON.stringify({ name: "mfup3-smoke", private: true, type: "module" }),
  );
  const tarballs = (await readdir(temp))
    .filter((name) => name.endsWith(".tgz"))
    .map((name) => path.join(temp, name));
  run(
    npm,
    ["install", "--no-audit", "--no-fund", ...tarballs, "react@18"],
    temp,
  );
  run(
    process.env.PYTHON_BIN ?? "python",
    [path.resolve("scripts/check-artifacts.py"), temp],
    process.cwd(),
  );
  await copyFile(
    "scripts/consumer-node-check.mjs",
    path.join(temp, "check.mjs"),
  );
  await copyFile(
    "examples/multiuser-scopes-node/server/server.mjs",
    path.join(temp, "example-server.mjs"),
  );
  run(process.execPath, ["check.mjs"], temp);
} finally {
  await rm(temp, { recursive: true, force: true });
}
