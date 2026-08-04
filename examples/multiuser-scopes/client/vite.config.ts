import { defineConfig } from "vite";

// The consumer backend (examples/multiuser-scopes/server) — one prefix
// carries both the app's own /api routes and the mounted MFUP router
// (/api/mfup/...). ws:true forwards the control-WebSocket upgrade.
const BACKEND = process.env.EXAMPLE_BACKEND_URL ?? "http://localhost:8090";

export default defineConfig({
  server: {
    host: "0.0.0.0",
    port: 20061,
    strictPort: true,
    allowedHosts: true,
    proxy: {
      "/api": { target: BACKEND, changeOrigin: true, ws: true },
    },
  },
  preview: {
    host: "0.0.0.0",
    port: 20061,
    strictPort: true,
    allowedHosts: true,
    proxy: {
      "/api": { target: BACKEND, changeOrigin: true, ws: true },
    },
  },
});
