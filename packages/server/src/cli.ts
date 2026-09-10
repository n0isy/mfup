#!/usr/bin/env node
import { createServer } from "node:http";
import { configFromEnv } from "./config.js";
import { createMfup } from "./http.js";

const mfup = createMfup(await configFromEnv());
const server = createServer(async (req, res) => {
  if (!(await mfup.handle(req, res))) {
    res.writeHead(404);
    res.end();
  }
});
mfup.attach(server);
server.listen(
  Number(process.env.MFUP_PORT ?? 3001),
  process.env.MFUP_HOST ?? "127.0.0.1",
);
let closing = false;
async function close() {
  if (closing) return;
  closing = true;
  server.close();
  await mfup.close();
  server.closeAllConnections();
}
process.once("SIGINT", close);
process.once("SIGTERM", close);
