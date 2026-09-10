import type { FilePart, Limits } from "./types.js";
export interface Work {
  part?: FilePart;
  file?: File;
  dir?: string;
}
/** Drain only ready work; least-loaded baskets spread bytes and zero-byte metadata. */
export function baskets(
  queue: Work[],
  slots: number,
  limits: Limits,
): Work[][] {
  const readyBytes = queue.reduce(
    (sum, item) => sum + (item.part?.[4] ?? 0),
    0,
  );
  const usefulSlots =
    readyBytes === 0
      ? Math.ceil(queue.length / limits.maxParts)
      : Math.max(
          Math.ceil(queue.length / Math.min(32, limits.maxParts)),
          Math.ceil(readyBytes / limits.partBytes),
        );
  slots = Math.min(slots, Math.max(1, usefulSlots));
  const groups: Work[][] = Array.from({ length: slots }, () => []),
    sizes = groups.map(() => 0);
  while (queue.length) {
    const item = queue[0],
      bytes = item.part?.[4] ?? 0;
    let best = -1;
    for (let i = 0; i < groups.length; i++) {
      if (
        groups[i].length >= limits.maxParts ||
        sizes[i] + bytes > limits.batchBytes
      )
        continue;
      if (
        best < 0 ||
        sizes[i] < sizes[best] ||
        (sizes[i] === sizes[best] && groups[i].length < groups[best].length)
      )
        best = i;
    }
    if (best < 0) break;
    groups[best].push(queue.shift()!);
    sizes[best] += bytes;
  }
  return groups.filter((group) => group.length);
}
