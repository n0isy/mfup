/**
 * Wire-protocol tests — mirrors server/tests/test_protocol.py: frames are
 * hand-assembled byte by byte to pin the exact on-wire layout, plus a
 * cross-check against the @mfup/client encoders (codec symmetry).
 */

import { describe, expect, it } from "vitest";

import {
  ChecksumKind,
  FrameReader,
  FrameTag,
  NodeKind,
  crc32c,
  type FileChunkFrame,
  type NodeFrame,
  type SessionEndFrame,
} from "../src/index.js";
import {
  encodeFileChunkFrame,
  encodeNodeFrame,
  encodeSessionEndFrame,
  crc32c as clientCrc32c,
  FrameTag as ClientFrameTag,
} from "@mfup/client";

/** [u32 BE length][u8 tag][payload] — length covers tag + payload. */
function buildFrame(tag: number, payload: Uint8Array): Uint8Array {
  const out = new Uint8Array(4 + 1 + payload.length);
  new DataView(out.buffer).setUint32(0, 1 + payload.length);
  out[4] = tag;
  out.set(payload, 5);
  return out;
}

class Writer {
  private parts: number[] = [];
  u8(v: number) {
    this.parts.push(v);
    return this;
  }
  u16(v: number) {
    this.parts.push((v >> 8) & 0xff, v & 0xff);
    return this;
  }
  u32(v: number) {
    this.parts.push((v >>> 24) & 0xff, (v >>> 16) & 0xff, (v >>> 8) & 0xff, v & 0xff);
    return this;
  }
  u64(v: number) {
    const b = new Uint8Array(8);
    new DataView(b.buffer).setBigUint64(0, BigInt(v));
    this.parts.push(...b);
    return this;
  }
  bytes(b: Uint8Array) {
    this.parts.push(...b);
    return this;
  }
  str(s: string) {
    const b = new TextEncoder().encode(s);
    return this.u16(b.length).bytes(b);
  }
  build(): Uint8Array {
    return new Uint8Array(this.parts);
  }
}

describe("crc32c", () => {
  it("empty input is 0", () => {
    expect(crc32c(new Uint8Array(0))).toBe(0);
  });

  it("known vector: crc32c('hello') == 0x9A71BB4C", () => {
    expect(crc32c(new TextEncoder().encode("hello"))).toBe(0x9a71bb4c);
  });

  it("matches the client implementation", () => {
    const data = new TextEncoder().encode("The quick brown fox jumps over the lazy dog");
    expect(crc32c(data)).toBe(clientCrc32c(data));
  });
});

describe("decodeFramePayload", () => {
  it("decodes a hand-built NODE frame", () => {
    const payload = new Writer()
      .u32(42) // node_id
      .u32(1) // parent_id
      .u8(NodeKind.FILE)
      .str("test.txt")
      .u8(1) // has_size
      .u64(1024)
      .u8(0) // no mtime
      .build();
    const reader = new FrameReader();
    reader.feed(buildFrame(FrameTag.NODE, payload));
    const frames = reader.drain();
    expect(frames).toHaveLength(1);
    const f = frames[0] as NodeFrame;
    expect(f.tag).toBe(FrameTag.NODE);
    expect(f.nodeId).toBe(42);
    expect(f.parentId).toBe(1);
    expect(f.kind).toBe(NodeKind.FILE);
    expect(f.name).toBe("test.txt");
    expect(f.sizeHint).toBe(1024);
    expect(f.mtimeMs).toBeNull();
  });

  it("decodes a FILE_CHUNK frame with matching checksum", () => {
    const body = new TextEncoder().encode("chunk-payload-bytes");
    const ck = crc32c(body);
    const payload = new Writer()
      .u32(7)
      .u64(0)
      .u32(body.length)
      .u8(ChecksumKind.CRC32C)
      .u32(ck)
      .bytes(body)
      .build();
    const reader = new FrameReader();
    reader.feed(buildFrame(FrameTag.FILE_CHUNK, payload));
    const [f] = reader.drain() as [FileChunkFrame];
    expect(f.tag).toBe(FrameTag.FILE_CHUNK);
    expect(f.nodeId).toBe(7);
    expect(f.offset).toBe(0);
    expect(f.length).toBe(body.length);
    expect(Array.from(f.payload)).toEqual(Array.from(body));
    expect(f.checksum).toBe(ck);
    expect(crc32c(f.payload)).toBe(f.checksum);
  });

  it("decodes DIR_CLOSE", () => {
    const reader = new FrameReader();
    reader.feed(buildFrame(FrameTag.DIR_CLOSE, new Writer().u32(5).build()));
    const [f] = reader.drain();
    expect(f.tag).toBe(FrameTag.DIR_CLOSE);
    expect((f as { nodeId: number }).nodeId).toBe(5);
  });

  it("decodes SESSION_END", () => {
    const payload = new Writer().u64(100).u64(100).u64(5000).u64(5000).u8(1).build();
    const reader = new FrameReader();
    reader.feed(buildFrame(FrameTag.SESSION_END, payload));
    const [f] = reader.drain() as [SessionEndFrame];
    expect(f.tag).toBe(FrameTag.SESSION_END);
    expect(f.scanDoneUnits).toBe(100);
    expect(f.bodyDoneBytes).toBe(5000);
    expect(f.sealed).toBe(true);
  });

  it("rejects an unknown tag", () => {
    const reader = new FrameReader();
    reader.feed(buildFrame(0x7f, new Uint8Array(4)));
    expect(() => reader.drain()).toThrow(/unknown frame tag/);
  });
});

describe("FrameReader", () => {
  it("incremental feed: no partial frames, none lost", () => {
    const frame = buildFrame(FrameTag.DIR_CLOSE, new Writer().u32(9).build());
    const reader = new FrameReader();
    for (let i = 0; i < frame.length - 1; i++) {
      reader.feed(frame.subarray(i, i + 1));
      expect(reader.drain()).toHaveLength(0);
    }
    reader.feed(frame.subarray(frame.length - 1));
    const frames = reader.drain();
    expect(frames).toHaveLength(1);
    expect((frames[0] as { nodeId: number }).nodeId).toBe(9);
  });

  it("two frames in one feed drain in order", () => {
    const f1 = buildFrame(FrameTag.DIR_CLOSE, new Writer().u32(1).build());
    const f2 = buildFrame(FrameTag.DIR_CLOSE, new Writer().u32(2).build());
    const both = new Uint8Array(f1.length + f2.length);
    both.set(f1, 0);
    both.set(f2, f1.length);
    const reader = new FrameReader();
    reader.feed(both);
    const frames = reader.drain();
    expect(frames.map((f) => (f as { nodeId: number }).nodeId)).toEqual([1, 2]);
  });

  it("enforces the max declared frame length", () => {
    const evil = new Uint8Array(4);
    new DataView(evil.buffer).setUint32(0, 512 * 1024 * 1024); // 512 MiB claim
    const reader = new FrameReader();
    reader.feed(evil);
    expect(() => reader.drain()).toThrow(/exceeds limit/);
  });
});

describe("codec symmetry with @mfup/client", () => {
  it("client-encoded NODE decodes identically", () => {
    const encoded = encodeNodeFrame({
      tag: ClientFrameTag.NODE,
      nodeId: 3,
      parentId: 1,
      kind: 1,
      name: "файл — тест ❤.txt",
      sizeHint: 2048n,
      mtimeMs: 1720000000000n,
    });
    const reader = new FrameReader();
    reader.feed(encoded);
    const [f] = reader.drain() as [NodeFrame];
    expect(f.nodeId).toBe(3);
    expect(f.parentId).toBe(1);
    expect(f.name).toBe("файл — тест ❤.txt");
    expect(f.sizeHint).toBe(2048);
    expect(f.mtimeMs).toBe(1720000000000);
  });

  it("client-encoded FILE_CHUNK round-trips payload + checksum", () => {
    const body = new Uint8Array(1000).map((_, i) => (i * 31) & 0xff);
    const encoded = encodeFileChunkFrame({
      tag: ClientFrameTag.FILE_CHUNK,
      nodeId: 12,
      offset: 65536n,
      length: body.length,
      checksumKind: 1,
      checksum: clientCrc32c(body),
      payload: body,
    });
    const reader = new FrameReader();
    reader.feed(encoded);
    const [f] = reader.drain() as [FileChunkFrame];
    expect(f.offset).toBe(65536);
    expect(crc32c(f.payload)).toBe(f.checksum);
  });

  it("client-encoded SESSION_END decodes", () => {
    const encoded = encodeSessionEndFrame({
      tag: ClientFrameTag.SESSION_END,
      rootSummary: {
        scanDoneUnits: 65n,
        scanEstUnits: 65n,
        bodyDoneBytes: 2560458n,
        bodyEstBytes: 2560458n,
        sealed: true,
      },
    });
    const reader = new FrameReader();
    reader.feed(encoded);
    const [f] = reader.drain() as [SessionEndFrame];
    expect(f.scanDoneUnits).toBe(65);
    expect(f.bodyDoneBytes).toBe(2560458);
    expect(f.sealed).toBe(true);
  });
});
