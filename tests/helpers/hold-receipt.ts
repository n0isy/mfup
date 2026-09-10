import { createServer, request, type ClientRequest } from "node:http";
/** Forward native browser multipart unchanged; delay only the response body. */
export async function holdReceipts(target: string) {
  let release!: () => void;
  const gate = new Promise<void>((r) => (release = r));
  const upstreams = new Set<ClientRequest>();
  const server = createServer((req, res) => {
    const cors = {
      "access-control-allow-origin": req.headers.origin ?? target,
      "access-control-allow-methods": "POST, OPTIONS",
      "access-control-allow-headers": "authorization,content-type,x-mfup-epoch",
    };
    if (req.method === "OPTIONS") {
      res.writeHead(204, cors);
      res.end();
      return;
    }
    const dest = new URL(req.url ?? "/", target);
    const upstream = request(
      dest,
      { method: req.method, headers: { ...req.headers, host: dest.host } },
      (response) => {
        const chunks: Buffer[] = [];
        response.on("data", (chunk) => chunks.push(chunk));
        response.on("error", () => res.destroy());
        response.on("end", () => {
          void (async () => {
            await gate;
            if (res.destroyed) return;
            const headers = { ...response.headers, ...cors };
            delete headers["transfer-encoding"];
            delete headers["content-length"];
            delete headers.connection;
            res.writeHead(response.statusCode ?? 502, headers);
            res.end(Buffer.concat(chunks));
          })();
        });
      },
    );
    upstreams.add(upstream);
    upstream.once("close", () => upstreams.delete(upstream));
    upstream.on("error", () => {
      if (!res.destroyed) {
        res.writeHead(502, cors);
        res.end();
      }
    });
    req.on("aborted", () => upstream.destroy());
    req.pipe(upstream);
  });
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
  return {
    origin: `http://127.0.0.1:${(server.address() as any).port}`,
    release,
    async close() {
      release();
      for (const req of upstreams) req.destroy();
      server.closeAllConnections();
      await new Promise<void>((r) => server.close(() => r()));
    },
  };
}
