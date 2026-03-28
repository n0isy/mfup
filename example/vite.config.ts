import { defineConfig } from "vite";
import path from "path";

export default defineConfig({
  root: ".",
  publicDir: "public",
  resolve: {
    alias: {
      "@mfup/client": path.resolve("/client/src"),
    },
  },
  server: {
    host: "0.0.0.0",
    port: 3000,
    strictPort: true,
    allowedHosts: true,
    fs: {
      allow: ["/client", "/app"],
    },
    proxy: {
      "/mfup/control": {
        target: "http://backend:8070",
        changeOrigin: true,
        ws: true,
      },
      "/mfup": {
        target: "http://backend:8070",
        changeOrigin: true,
      },
      "/health": {
        target: "http://backend:8070",
        changeOrigin: true,
      },
    },
  },
});
