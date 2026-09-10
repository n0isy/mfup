import { defineConfig } from "vite";
const base = process.env.EXAMPLE_BASE ?? "/";
const backend = process.env.EXAMPLE_BACKEND_URL ?? "http://127.0.0.1:3001";
export default defineConfig({
  base,
  cacheDir: `../../../.tmp/vite-${base.replace(/[^a-z0-9]/gi, "_")}-${process.env.PORT ?? "3000"}`,
  server: {
    host: process.env.HOST ?? "0.0.0.0",
    port: Number(process.env.PORT ?? 3000),
    strictPort: true,
    allowedHosts: true,
    proxy: {
      [base + "api"]: {
        target: backend,
        ws: true,
        rewrite: (pathname) => pathname.slice(base.length - 1),
      },
    },
  },
});
