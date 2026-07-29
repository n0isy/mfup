import { defineConfig } from "vite";
import path from "path";

// Proxy targets default to docker-compose service names; CI jobs without
// Docker (e.g. the macOS WebKit runner) override them with localhost URLs.
const BACKEND = process.env.MFUP_BACKEND_URL ?? "http://backend:8070";
const TRIVIAL = process.env.MFUP_TRIVIAL_URL ?? "http://trivial:8071";

// Shared by the dev server and `vite preview` (static build).
const PROXY = {
  "/mfup/control": { target: BACKEND, changeOrigin: true, ws: true },
  "/mfup": { target: BACKEND, changeOrigin: true },
  "/health": { target: BACKEND, changeOrigin: true },
  "/trivial": {
    target: TRIVIAL,
    changeOrigin: true,
    rewrite: (p: string) => p.replace(/^\/trivial/, ""),
  },
} as const;

export default defineConfig({
  root: ".",
  publicDir: "public",
  resolve: {
    alias: {
      "@mfup/client": path.resolve(__dirname, "../client/src"),
    },
  },
  build: {
    rollupOptions: {
      input: {
        main: path.resolve(__dirname, "index.html"),
        compare: path.resolve(__dirname, "compare.html"),
        e2e: path.resolve(__dirname, "e2e.html"),
      },
    },
  },
  server: {
    host: "0.0.0.0",
    port: 3000,
    strictPort: true,
    allowedHosts: true,
    fs: {
      // repo root (native runs) + the docker-compose mount points
      allow: [path.resolve(__dirname, ".."), "/client", "/app"],
    },
    proxy: PROXY,
  },
  // `vite preview` serves the built dist statically (used by the macOS CI job
  // instead of the dev server — webkit + the HMR dev server hangs on the
  // frozen macOS webkit build; static files match what Linux/Caddy serves).
  preview: {
    host: "0.0.0.0",
    port: 20060,
    strictPort: true,
    allowedHosts: true,
    proxy: PROXY,
  },
});
