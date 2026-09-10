import { defineConfig } from "@playwright/test";
import defaults from "./playwright.config.js";
import path from "node:path";
export default defineConfig({
  ...defaults,
  testDir: "./tests/examples",
  use: { ...defaults.use, baseURL: "http://127.0.0.1:20067" },
  webServer: {
    command: "node scripts/dev-examples.mjs",
    url: "http://127.0.0.1:20067/api/mfup/health",
    timeout: 60000,
    reuseExistingServer: false,
    gracefulShutdown: { signal: "SIGTERM", timeout: 10000 },
    env: {
      MFUP_EXAMPLE_TEST: "1",
      PORT: "20067",
      EXAMPLE_API_PORT: "20068",
      HOST: "127.0.0.1",
      MFUP_EXAMPLE_DATA: path.resolve(
        ".tmp",
        `examples-e2e-${process.env.MFUP_BACKEND ?? "node"}`,
      ),
    },
  },
});
