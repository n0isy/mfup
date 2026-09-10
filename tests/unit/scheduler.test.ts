import { it, expect } from "vitest";
import { baskets, type Work } from "../../packages/client/src/scheduler.js";
import { fromEntries, fromFiles } from "../../packages/client/src/ingestion.js";
it("balances a ready list across six bounded baskets", () => {
  const queue: Work[] = Array.from({ length: 60 }, (_, i) => ({
    part: [`f${i}`, 10, 0, 0, 10],
  }));
  const result = baskets(queue, 6, {
    concurrency: 6,
    maxParts: 10,
    batchBytes: 100,
    partBytes: 10,
  });
  expect(result.map((x) => x.length)).toEqual([10, 10, 10, 10, 10, 10]);
  expect(queue).toHaveLength(0);
});
it("groups a few small files into a single request", () => {
  const queue: Work[] = Array.from({ length: 3 }, (_, i) => ({
    part: [`f${i}`, 10, 0, 0, 10],
  }));
  expect(
    baskets(queue, 6, {
      concurrency: 6,
      maxParts: 128,
      batchBytes: 1024,
      partBytes: 1024,
    }).map((x) => x.length),
  ).toEqual([3]);
});
it("handles paginated directory entries incrementally and preserves empty dirs", async () => {
  let calls = 0;
  const directory = {
    name: "root",
    isDirectory: true,
    isFile: false,
    createReader: () => ({
      readEntries: (ok: (entries: any[]) => void) => {
        calls++;
        ok(
          calls === 1
            ? [
                {
                  name: "empty",
                  isDirectory: true,
                  isFile: false,
                  createReader: () => ({
                    readEntries: (cb: (x: any[]) => void) => cb([]),
                  }),
                },
              ]
            : [],
        );
      },
    }),
  };
  const iterator = fromEntries([directory as any]);
  expect((await iterator.next()).value?.path).toBe("root");
  expect(calls).toBe(0);
  expect((await iterator.next()).value?.path).toBe("root/empty");
  expect(calls).toBe(1);
  expect((await iterator.next()).done).toBe(true);
  expect(calls).toBe(2);
});
