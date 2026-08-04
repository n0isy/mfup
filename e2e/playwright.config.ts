import { defineConfig, devices } from "@playwright/test";

export default defineConfig({
  testDir: "./tests",
  // Functional tests must finish well under a minute; a hang should fail fast,
  // not burn 4 minutes. Chaos/resume tests raise their own timeout in-body.
  timeout: 60_000,
  retries: 0,
  workers: 1, // sequential — tests share the server + uploads dir
  reporter: [["list"]],
  use: {
    baseURL: process.env.MFUP_BASE_URL ?? "http://localhost:20060",
    ignoreHTTPSErrors: true,
    // Never let a stuck navigation/action hang the whole test timeout.
    navigationTimeout: 30_000,
    actionTimeout: 30_000,
  },
  projects: [
    // Chromium runs everything, including @chromium-only chaos/conflict tests.
    { name: "chromium", use: { ...devices["Desktop Chrome"] } },
    // Other engines skip @chromium-only at COLLECTION time (grepInvert), so
    // those tests never instantiate a page fixture on webkit/firefox — no hang.
    { name: "firefox", use: { ...devices["Desktop Firefox"] }, grepInvert: /@chromium-only/ },
    // CI-only retry: the macOS runner's cold start sporadically serves the
    // first page load slower than any sane timeout (observed twice, different
    // specs each time, never reproducible). One retry absorbs exactly that;
    // a real regression still fails twice.
    { name: "webkit", use: { ...devices["Desktop Safari"] }, grepInvert: /@chromium-only/, retries: process.env.CI ? 1 : 0 },
  ],
});
