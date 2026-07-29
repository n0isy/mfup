import { defineConfig, devices } from "@playwright/test";

export default defineConfig({
  testDir: "./tests",
  timeout: 240_000,
  retries: 0,
  workers: 1, // sequential — tests share the server + uploads dir
  reporter: [["list"]],
  use: {
    baseURL: process.env.MFUP_BASE_URL ?? "http://localhost:20060",
    ignoreHTTPSErrors: true,
  },
  projects: [
    { name: "chromium", use: { ...devices["Desktop Chrome"] } },
    { name: "firefox", use: { ...devices["Desktop Firefox"] } },
    { name: "webkit", use: { ...devices["Desktop Safari"] } },
  ],
});
