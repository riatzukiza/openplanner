export interface EmbeddingCacheEntry {
  embedding: number[];
  cachedAt: number;
}

import { createHash, randomUUID } from "node:crypto";
import { readFileSync, mkdirSync, openSync, writeFileSync, fsyncSync, closeSync, renameSync, rmSync } from "node:fs";
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
    for (const { key, vector } of entries) {
      this.map.delete(key);
      this.map.set(key, vector);
    }
    this.trimToLimit();
    await this.flush();
  }

  /** Atomically persist this process's bounded snapshot; no distributed writer lock. */
  async flush(): Promise<void> {
    if (!this._cachePath) return;
    const directory = dirname(this._cachePath);
    mkdirSync(directory, { recursive: true, mode: 0o700 });
    const temporary = `${this._cachePath}.${process.pid}.${randomUUID()}.tmp`;
    let file: number | undefined;
    try {
      file = openSync(temporary, "wx", 0o600);
      writeFileSync(file, JSON.stringify([...this.map]));
      fsyncSync(file);
      closeSync(file);
      file = undefined;
      renameSync(temporary, this._cachePath);
      const parent = openSync(directory, "r");
      try { fsyncSync(parent); } finally { closeSync(parent); }
    } finally {
      if (file !== undefined) closeSync(file);
      rmSync(temporary, { force: true });
    }
  }
}
