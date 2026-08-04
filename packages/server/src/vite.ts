/**
 * Vite plugin — a full MFUP/2 upload server inside `vite dev` / `vite preview`.
 *
 *     // vite.config.ts
 *     import { mfupDev } from "@mfup/server/vite";
 *
 *     export default defineConfig({
 *       plugins: [mfupDev({ baseDir: "./uploads" })],
 *     });
 *
 * The browser client then talks to the page's own origin:
 *
 *     new MfupSession({ serverUrl: location.origin })
 *
 * The plugin mounts the universal middleware on the dev server's connect
 * stack and claims the /mfup/control WebSocket upgrade — carefully leaving
 * Vite's own HMR socket alone (it matches by path, and HMR lives elsewhere).
 */

import type { Server } from "node:http";
import type { IncomingMessage, ServerResponse } from "node:http";

import { createMfup, type Mfup } from "./handler.js";
import type { MfupOptions } from "./engine.js";

// Structural slices of Vite's types — avoids a hard dependency on vite.
interface ViteDevServerLike {
  middlewares: {
    use(fn: (req: IncomingMessage, res: ServerResponse, next: (err?: unknown) => void) => void): void;
  };
  httpServer?: Server | null;
}

interface VitePluginLike {
  name: string;
  configureServer?(server: ViteDevServerLike): void;
  configurePreviewServer?(server: ViteDevServerLike): void;
}

export function mfupDev(options: MfupOptions): VitePluginLike {
  let mfup: Mfup | null = null;

  const wire = (server: ViteDevServerLike): void => {
    mfup ??= createMfup(options);
    const m = mfup;
    server.middlewares.use((req, res, next) => m.middleware(req, res, next));
    if (server.httpServer) {
      m.attach(server.httpServer);
      server.httpServer.on("close", () => {
        void m.close();
        mfup = null;
      });
    }
  };

  return {
    name: "mfup",
    configureServer: wire,
    configurePreviewServer: wire,
  };
}

export type { MfupOptions } from "./engine.js";
