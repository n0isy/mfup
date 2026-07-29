import { defineConfig } from "vite";
import path from "path";

// Proxy targets default to docker-compose service names; CI jobs without
// Docker (e.g. the macOS WebKit runner) override them with localhost URLs.
const BACKEND = process.env.MFUP_BACKEND_URL ?? "http://backend:8070";
const TRIVIAL = process.env.MFUP_TRIVIAL_URL ?? "http://trivial:8071";

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
    proxy: {
      "/mfup/control": {
        target: BACKEND,
        changeOrigin: true,
        ws: true,
      },
      "/mfup": {
        target: BACKEND,
        changeOrigin: true,
      },
      "/health": {
        target: BACKEND,
        changeOrigin: true,
      },
      "/trivial": {
        target: TRIVIAL,
        changeOrigin: true,
        rewrite: (path: string) => path.replace(/^\/trivial/, ""),
      },
    },
  },
});
