export interface EmbeddingCacheEntry {
  embedding: number[];
  cachedAt: number;
}

import { createHash } from "node:crypto";

export function makeEmbeddingCacheKey(params: {
  model: string;
  text: string;
}): string {
  const digest = createHash("sha256").update(params.text, "utf8").digest("hex");
  return `${params.model}::${digest}`;
}

export class PersistentEmbeddingCache {
  private map = new Map<string, number[]>();
  private flushing = false;
  private readonly maxEntries = 10000;

  constructor(private _cachePath: string) {}

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
  }

  has(key: string): boolean {
    return this.map.has(key);
  }

  delete(key: string): void {
    this.map.delete(key);
  }

  clear(): void {
    this.map.clear();
  }

  get size(): number {
    return this.map.size;
  }

  async putMany(entries: Array<{ key: string; vector: number[] }>): Promise<void> {
    if (this.flushing) return;
    this.flushing = true;
    try {
      for (const { key, vector } of entries) {
        this.map.delete(key);
        this.map.set(key, vector);
      }
      this.trimToLimit();
    } finally {
      this.flushing = false;
    }
  }

  async flush(): Promise<void> {
    // no-op stub
  }
}
