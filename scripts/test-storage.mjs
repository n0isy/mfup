import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
const root = fileURLToPath(new URL("../", import.meta.url));
if (process.platform !== "linux")
  throw Error("Real filesystem scenarios require Linux and Docker");
const common = [
  "run",
  "--rm",
  "--user",
  "1000:1000",
  "--tmpfs",
  "/storage:rw,size=4m,mode=1777",
  "--tmpfs",
  "/readonly:ro,size=1m,mode=755",
  "-v",
  `${root}:/repo:ro`,
  "-w",
  "/tmp",
];
const jobs = [
  ["node:24-bookworm-slim", "node", "/repo/scripts/storage-node-check.mjs"],
  [
    "python:3.12-slim",
    "sh",
    "-c",
    "pip install --target /tmp/deps /repo/server/mfup-core /repo/server/mfup-fastapi httpx > /tmp/install.log 2>&1 && PYTHONPATH=/tmp/deps python /repo/scripts/storage-python-check.py",
  ],
];
for (const job of jobs) {
  const result = spawnSync("docker", [...common, ...job], { stdio: "inherit" });
  if (result.status !== 0) process.exit(result.status ?? 1);
}
