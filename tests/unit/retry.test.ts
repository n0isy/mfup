import { afterEach, expect, it, vi } from "vitest";
import { MfupSession, type Entry } from "../../packages/client/src/index.js";
import { retryDelay } from "../../packages/client/src/retry.js";

const limits = {
  concurrency: 6,
  maxParts: 128,
  partBytes: 1024,
  batchBytes: 4096,
};
function endpoint() {
  let epoch = 1,
    bytes = 0,
    state = "uploading";
  const snapshot = () => ({
    id: "test",
    epoch,
    state,
    confirmedBytes: bytes,
    asks: [],
    published: [],
    processing: "none",
    clientPublish: true,
  });
  return async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = String(input);
    if (url.endsWith("/sessions"))
      return Response.json({ id: "test", token: "token", epoch, limits });
    if (url.endsWith("/resume")) {
      epoch++;
      return Response.json({ ...snapshot(), limits });
    }
    if (url.endsWith("/status")) {
      const data = JSON.parse(String(init?.body));
      return Response.json({
        received: data.files.map(() => false),
        confirmedBytes: bytes,
      });
    }
    if (url.includes("/batches/")) {
      if (init?.method !== "POST")
        return Response.json({ error: "not_found" }, { status: 404 });
      const manifest = JSON.parse(
        String((init.body as FormData).get("manifest")),
      );
      const size = manifest.files.reduce(
        (n: number, p: number[]) => n + p[4],
        0,
      );
      bytes += size;
      return Response.json({
        id: url.split("/").at(-1),
        parts: manifest.files.length,
        bytes: size,
        confirmedBytes: bytes,
      });
    }
    if (url.endsWith("/commit")) state = "committed";
    if (url.endsWith("/publish")) state = "published";
    if (url.endsWith("/cancel")) state = "cancelled";
    return Response.json(snapshot());
  };
}
afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

it.each([undefined, 1000])(
  "keeps %s retries alive for almost ten hours, including failed receipt probes",
  async (retries) => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    vi.stubGlobal("WebSocket", undefined);
    const server = endpoint(),
      attempts: number[] = [];
    let probes = 0;
    const s = new MfupSession({
      serverUrl: "http://test",
      retries,
      fetch: async (input, init) => {
        if (String(input).includes("/batches/")) {
          if (init?.method === "POST") attempts.push(Date.now());
          else probes++;
          throw new TypeError("offline");
        }
        return server(input, init);
      },
    });
    try {
      const result = s.upload([new File(["x"], "a")]).catch((e) => e);
      await vi.runAllTimersAsync();
      expect((await result).code).toBe("network_error");
      expect(attempts).toHaveLength(1001);
      expect(probes).toBe(1001);
      expect(attempts.at(-1)! - attempts[0]).toBe(35_847_000);
      expect(s.getSnapshot().state).toBe("failed");
    } finally {
      s.dispose();
    }
  },
);

it("bounds very large budgets and validates retry options", () => {
  expect(retryDelay(1000, {})).toBe(36000);
  expect(retryDelay(Number.MAX_SAFE_INTEGER, {})).toBe(36000);
  for (const value of [-1, NaN, Infinity, 1.5])
    expect(() => new MfupSession({ retries: value })).toThrow();
  expect(retryDelay(0, { retryDelayMs: 50, retryMaxDelayMs: 100 })).toBe(50);
  expect(retryDelay(100, { retryDelayMs: 50, retryMaxDelayMs: 100 })).toBe(100);
});

it("retains its stream after exhaustion and retries without a source argument", async () => {
  vi.stubGlobal("WebSocket", undefined);
  let offline = true,
    produced = 0,
    accepted = 0,
    maximum = 0,
    requests = 0;
  const server = endpoint();
  const s = new MfupSession({
    serverUrl: "http://test",
    retries: 0,
    batchDelayMs: 0,
    fetch: async (input, init) => {
      if (String(input).includes("/batches/") && init?.method === "POST") {
        requests++;
        if (offline) throw new TypeError("offline");
        const manifest = JSON.parse(
          String((init.body as FormData).get("manifest")),
        );
        accepted += manifest.files.length;
      }
      return server(input, init);
    },
  });
  async function* source(): AsyncGenerator<Entry> {
    for (let i = 0; i < 20000; i++) {
      produced++;
      maximum = Math.max(maximum, produced - accepted);
      yield {
        path: `f${i}`,
        kind: "file",
        file: new File(["x"], `f${i}`, { lastModified: 1 }),
      };
    }
  }
  try {
    await expect(s.upload(source())).rejects.toMatchObject({
      code: "network_error",
    });
    const held = produced;
    await new Promise((r) => setTimeout(r, 20));
    expect(produced).toBe(held);
    expect(produced).toBeLessThanOrEqual(10000);
    offline = false;
    await s.retry();
    expect(accepted).toBe(20000);
    expect(maximum).toBeLessThanOrEqual(10000);
    expect(requests).toBeLessThan(1000);
    expect(s.getSnapshot()).toMatchObject({
      state: "published",
      confirmedBytes: 20000,
      discovered: 20000,
    });
  } finally {
    s.dispose();
  }
}, 30000);

it("streams one million files while the consumer supplies no retained list", async () => {
  vi.stubGlobal("WebSocket", undefined);
  let produced = 0,
    accepted = 0,
    maximum = 0;
  const server = endpoint();
  const s = new MfupSession({
    serverUrl: "http://test",
    batchDelayMs: 0,
    fetch: async (input, init) => {
      if (String(input).includes("/batches/") && init?.method === "POST") {
        accepted += JSON.parse(String((init.body as FormData).get("manifest")))
          .files.length;
      }
      return server(input, init);
    },
  });
  async function* source(): AsyncGenerator<Entry> {
    for (let i = 0; i < 1000000; i++) {
      maximum = Math.max(maximum, ++produced - accepted);
      yield {
        path: `f${i}`,
        kind: "file",
        file: new File([], `f${i}`, { lastModified: 1 }),
      };
    }
  }
  try {
    await s.upload(source());
    expect(accepted).toBe(1000000);
    expect(maximum).toBeLessThanOrEqual(10000);
  } finally {
    s.dispose();
  }
}, 120000);

it("cancels immediately during the maximum backoff delay", async () => {
  vi.useFakeTimers();
  vi.stubGlobal("WebSocket", undefined);
  const server = endpoint();
  let posts = 0;
  const s = new MfupSession({
    serverUrl: "http://test",
    retryDelayMs: 36000,
    fetch: async (input, init) => {
      if (String(input).includes("/batches/") && init?.method === "POST") {
        posts++;
        throw new TypeError("offline");
      }
      return server(input, init);
    },
  });
  const result = s.upload([new File(["x"], "a")]).catch((e) => e);
  await vi.advanceTimersByTimeAsync(100);
  expect(posts).toBe(1);
  await s.cancel();
  expect((await result).code).toBe("cancelled");
  expect(posts).toBe(1);
  s.dispose();
});

it("charges offline range-status probes to the same finite retry budget", async () => {
  vi.useFakeTimers();
  vi.stubGlobal("WebSocket", undefined);
  const server = endpoint();
  let probes = 0;
  const s = new MfupSession({
    serverUrl: "http://test",
    ticket: { id: "test", token: "token", epoch: 1, limits },
    retries: 2,
    fetch: async (input, init) => {
      if (String(input).endsWith("/status")) {
        probes++;
        throw new TypeError("offline");
      }
      return server(input, init);
    },
  });
  const result = s.upload([new File(["x"], "a")]).catch((e) => e);
  await vi.runAllTimersAsync();
  expect((await result).code).toBe("network_error");
  expect(probes).toBe(3);
  s.dispose();
});

it("does not count active XHR payload twice when snapshots precede out-of-order receipts", async () => {
  vi.useFakeTimers();
  vi.stubGlobal("WebSocket", undefined);
  const server = endpoint();
  vi.stubGlobal("fetch", server);
  const requests: XHR[] = [];
  class XHR {
    upload: any = {};
    status = 200;
    response: any;
    onload?: () => void;
    onabort?: () => void;
    url = "";
    body!: FormData;
    open(_method: string, url: string) {
      this.url = url;
    }
    setRequestHeader() {}
    send(body: FormData) {
      this.body = body;
      requests.push(this);
    }
    abort() {
      this.onabort?.();
    }
    progress(fraction: number) {
      this.upload.onprogress({
        lengthComputable: true,
        loaded: fraction * 100,
        total: 100,
      });
    }
    async accept() {
      this.response = await (
        await server(this.url, { method: "POST", body: this.body })
      ).json();
    }
  }
  vi.stubGlobal("XMLHttpRequest", XHR);
  const s = new MfupSession({
    serverUrl: "http://test",
    trackUploadProgress: true,
  });
  const pending = s.upload([
    new File([new Uint8Array(3072)], "a", { lastModified: 1 }),
  ]);
  try {
    await vi.advanceTimersByTimeAsync(10);
    expect(requests).toHaveLength(3);
    requests[0].progress(1);
    requests[1].progress(1);
    requests[2].progress(0.5);
    await requests[0].accept();
    await requests[1].accept();
    await s.refresh();
    expect(s.getSnapshot()).toMatchObject({
      confirmedBytes: 2048,
      sentBytes: 2560,
      totalBytes: 3072,
    });
    requests[1].onload!();
    await vi.advanceTimersByTimeAsync(1);
    requests[0].onload!();
    await vi.advanceTimersByTimeAsync(1);
    expect(s.getSnapshot().sentBytes).toBe(2560);
    requests[2].progress(1);
    await requests[2].accept();
    requests[2].onload!();
    await vi.runAllTimersAsync();
    await pending;
    expect(s.getSnapshot()).toMatchObject({
      confirmedBytes: 3072,
      sentBytes: 3072,
      state: "published",
    });
  } finally {
    s.dispose();
  }
});
