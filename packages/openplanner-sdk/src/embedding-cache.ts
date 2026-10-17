export interface EmbeddingCacheEntry {
  embedding: number[];
  cachedAt: number;
}

import { createHash, randomUUID } from "node:crypto";
import { closeSync, fstatSync, openSync, readSync } from "node:fs";
import { mkdir, open, rename, rm } from "node:fs/promises";
import { dirname } from "node:path";
import { StringDecoder } from "node:string_decoder";

const maxCacheRecordBytes = 8 * 1024 * 1024;

export function makeEmbeddingCacheKey(params: {
  model: string;
  text: string;
  dimensions?: number;
}): string {
  const digest = createHash("sha256").update(params.text, "utf8").digest("hex");
  const tuple = JSON.stringify([params.model, params.dimensions ?? null, digest]);
  // A versioned key deliberately misses ambiguous historical cache entries.
  return "embedding-cache:v2:" + createHash("sha256").update(tuple, "utf8").digest("hex");
}

function ownedEmbedding(value: unknown): number[] {
  if (!Array.isArray(value) || value.length === 0) {
    throw new Error("cache embedding must be a nonempty finite number array");
  }
  for (const coordinate of value) {
    if (typeof coordinate !== "number" || !Number.isFinite(coordinate)) {
      throw new Error("cache embedding must contain finite numbers");
    }
  }
  return [...value];
}

export class PersistentEmbeddingCache {
  private map = new Map<string, number[]>();
  private readonly maxEntries = 10000;
  private flushTimer?: ReturnType<typeof setTimeout>;
  private flushQueue: Promise<void> = Promise.resolve();

  constructor(private _cachePath?: string) {
    if (!_cachePath) return;
    try { this.load(_cachePath); }
    catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") return;
      throw error;
    }
  }

  private load(cachePath: string): void {
    const descriptor = openSync(cachePath, "r");
    try {
      const size = fstatSync(descriptor).size;
      const chunk = Buffer.alloc(64 * 1024);
      const decoder = new StringDecoder("utf8");
      let format: "legacy" | "jsonl" | undefined;
      let pending = "";
      const skip = (): void => {
        this.map.clear();
        console.warn("Oversized embedding cache skipped; starting with an empty cache");
      };
      const consume = (text: string): boolean => {
        pending += text;
        if (format === undefined) {
          const first = pending.trimStart();
          const next = first.startsWith("[") ? first.slice(1).trimStart() : first;
          if (next.length > 0) {
            format = first.startsWith("[") && (next.startsWith("[") || next.startsWith("]"))
              ? "legacy" : "jsonl";
          }
        }
        // Legacy arrays are migrated only when small enough to parse safely.
        if (format === "legacy" && size > maxCacheRecordBytes) {
          skip();
          return false;
        }
        if (format === "jsonl") {
          let newline: number;
          while ((newline = pending.indexOf("\n")) !== -1) {
            const line = pending.slice(0, newline);
            pending = pending.slice(newline + 1);
            if (Buffer.byteLength(line, "utf8") > maxCacheRecordBytes) {
              skip();
              return false;
            }
            if (line.trim().length > 0) this.loadEntry(JSON.parse(line));
          }
        }
        if (Buffer.byteLength(pending, "utf8") > maxCacheRecordBytes) {
          skip();
          return false;
        }
        return true;
      };
      let count: number;
      while ((count = readSync(descriptor, chunk, 0, chunk.length, null)) > 0) {
        if (!consume(decoder.write(chunk.subarray(0, count)))) return;
      }
      if (!consume(decoder.end())) return;
      if (format === "legacy") {
        const saved: unknown = JSON.parse(pending);
        if (!Array.isArray(saved)) throw new Error("Invalid persistent embedding cache");
        for (const entry of saved) this.loadEntry(entry);
      } else if (pending.trim().length > 0) {
        this.loadEntry(JSON.parse(pending));
      }
    } finally {
      closeSync(descriptor);
    }
  }

  private loadEntry(entry: unknown): void {
    if (!Array.isArray(entry) || entry.length !== 2 || typeof entry[0] !== "string"
      || !Array.isArray(entry[1]) || entry[1].length === 0
      || !entry[1].every((value: unknown) => typeof value === "number" && Number.isFinite(value))) {
      throw new Error("Invalid persistent embedding cache entry");
    }
    this.map.delete(entry[0]);
    this.map.set(entry[0], ownedEmbedding(entry[1]));
    this.trimToLimit();
  }

  async getMany(keys: string[]): Promise<Map<string, number[]>> {
    const result = new Map<string, number[]>();
    for (const key of keys) {
      const entry = this.map.get(key);
      if (entry) {
        result.set(key, [...entry]);
        this.map.delete(key);
        this.map.set(key, entry);
      }
    }
    return result;
  }

  private trimToLimit(): void {
    while (this.map.size > this.maxEntries) {
      const firstKey = this.map.keys().next().value;
      if (firstKey !== undefined) this.map.delete(firstKey);
    }
  }

  set(key: string, value: EmbeddingCacheEntry): void {
    const vector = ownedEmbedding(value.embedding);
    this.map.delete(key);
    this.map.set(key, vector);
    this.trimToLimit();
    this.scheduleFlush();
  }

  has(key: string): boolean {
    return this.map.has(key);
  }

  delete(key: string): void {
    this.map.delete(key);
    this.scheduleFlush();
  }

  clear(): void {
    this.map.clear();
    this.scheduleFlush();
  }

  get size(): number {
    return this.map.size;
  }

  async putMany(entries: Array<{ key: string; vector: number[] }>): Promise<void> {
    // Validate and copy the whole batch before mutating any retained entry.
    const owned = entries.map(({ key, vector }) => ({ key, vector: ownedEmbedding(vector) }));
    for (const { key, vector } of owned) {
      this.map.delete(key);
      this.map.set(key, vector);
    }
    this.trimToLimit();
    this.scheduleFlush();
  }

  private scheduleFlush(): void {
    if (!this._cachePath || this.flushTimer) return;
    this.flushTimer = setTimeout(() => {
      this.flushTimer = undefined;
      void this.flush().catch(() => console.warn("Embedding cache persistence failed"));
    }, 5_000);
    this.flushTimer.unref();
  }

  /** Explicitly await an atomic durable snapshot; ordinary batches only schedule it. */
  flush(): Promise<void> {
    if (!this._cachePath) return Promise.resolve();
    if (this.flushTimer) clearTimeout(this.flushTimer);
    this.flushTimer = undefined;
    const snapshot = [...this.map];
    const cachePath = this._cachePath;
    const next = this.flushQueue.catch(() => {}).then(() => this.persist(cachePath, snapshot));
    this.flushQueue = next;
    return next;
  }

  private async persist(cachePath: string, snapshot: Array<[string, number[]]>): Promise<void> {
    const directory = dirname(cachePath);
    await mkdir(directory, { recursive: true, mode: 0o700 });
    const temporary = `${cachePath}.${process.pid}.${randomUUID()}.tmp`;
    let file: Awaited<ReturnType<typeof open>> | undefined;
    try {
      file = await open(temporary, "wx", 0o600);
      // Each entry can be reloaded independently without one giant JSON string.
      for (const entry of snapshot) {
        await file.writeFile(JSON.stringify(entry) + "\n");
      }
      await file.sync();
      await file.close();
      file = undefined;
      await rename(temporary, cachePath);
      const parent = await open(directory, "r");
      try { await parent.sync(); } finally { await parent.close(); }
    } finally {
      if (file !== undefined) await file.close();
      await rm(temporary, { force: true });
    }
  }
}
