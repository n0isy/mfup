import { defineConfig, devices } from "@playwright/test";
export default defineConfig({
  testDir: "./tests/browser",
  timeout: 60000,
  expect: { timeout: 15000 },
  workers: 1,
  retries: process.env.CI ? 1 : 0,
  use: {
    baseURL: process.env.MFUP_E2E_URL ?? "http://127.0.0.1:20063",
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
  },
  webServer: process.env.MFUP_E2E_URL
    ? undefined
    : {
        command: "node scripts/test-stack.mjs",
        url: "http://127.0.0.1:20063/mfup/health",
        timeout: 60000,
        reuseExistingServer: false,
        gracefulShutdown: { signal: "SIGTERM", timeout: 10000 },
      },
  projects: [
    { name: "chromium", use: { ...devices["Desktop Chrome"] } },
    { name: "firefox", use: { ...devices["Desktop Firefox"] } },
    { name: "webkit", use: { ...devices["Desktop Safari"] } },
    ...(process.env.MFUP_EXTENDED
      ? [
          {
            name: "chrome",
            use: { ...devices["Desktop Chrome"], channel: "chrome" },
          },
          {
            name: "edge",
            use: { ...devices["Desktop Edge"], channel: "msedge" },
          },
          { name: "android-emulation", use: { ...devices["Pixel 7"] } },
          { name: "ios-emulation", use: { ...devices["iPhone 13"] } },
        ]
      : []),
  ],
  reporter: process.env.CI ? [["list"], ["html", { open: "never" }]] : "list",
});
