export interface EmbeddingCacheEntry {
  embedding: number[];
  cachedAt: number;
}

import { createHash, randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import { mkdir, open, rename, rm } from "node:fs/promises";
import { dirname } from "node:path";

export function makeEmbeddingCacheKey(params: {
  model: string;
  text: string;
}): string {
  const digest = createHash("sha256").update(params.text, "utf8").digest("hex");
  return `${params.model}::${digest}`;
}

export class PersistentEmbeddingCache {
  private map = new Map<string, number[]>();
  private readonly maxEntries = 10000;
  private flushTimer?: ReturnType<typeof setTimeout>;
  private flushQueue: Promise<void> = Promise.resolve();

  constructor(private _cachePath?: string) {
    if (!_cachePath) return;
    let saved: unknown;
    try { saved = JSON.parse(readFileSync(_cachePath, "utf8")); }
    catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") return;
      throw error;
    }
    if (!Array.isArray(saved)) throw new Error("Invalid persistent embedding cache");
    for (const entry of saved) {
      if (!Array.isArray(entry) || entry.length !== 2 || typeof entry[0] !== "string"
        || !Array.isArray(entry[1]) || entry[1].length === 0
        || !entry[1].every((value: unknown) => typeof value === "number" && Number.isFinite(value))) {
        throw new Error("Invalid persistent embedding cache entry");
      }
      this.map.set(entry[0], entry[1]);
    }
    this.trimToLimit();
  }

  async getMany(keys: string[]): Promise<Map<string, number[]>> {
    const result = new Map<string, number[]>();
    for (const key of keys) {
      const entry = this.map.get(key);
      if (entry) {
        result.set(key, entry);
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
    this.map.delete(key);
    this.map.set(key, value.embedding);
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
    for (const { key, vector } of entries) {
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
      // Keep the existing array format without allocating or stringifying one
      // giant snapshot on the provider's completion path.
      await file.writeFile("[");
      for (let i = 0; i < snapshot.length; i++) {
        await file.writeFile((i === 0 ? "" : ",") + JSON.stringify(snapshot[i]));
      }
      await file.writeFile("]");
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
