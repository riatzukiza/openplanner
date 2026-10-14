import { open as openFile, readlink, realpath, type FileHandle } from "node:fs/promises";
import { constants } from "node:fs";
import path from "node:path";
import { open } from "lmdb";
import {
  cacheGet,
  cachePut,
  createLayeredCache,
  createLmdbCache,
  createMemoryLruCache,
  documentCacheKey,
  documentNeedsHydration,
  hydrateDocumentRow,
} from "@open-hax/openplanner-document-hydration";
import type { CacheHandle } from "@open-hax/openplanner-document-hydration";

type OwnedHydrationCache = { cache: CacheHandle; close: () => Promise<void> };
let hydrationCachePromise: Promise<OwnedHydrationCache> | null = null;
let hydrationClosePromise: Promise<void> | null = null;

function parseJsonObject(value: unknown): Record<string, unknown> {
  if (typeof value === "object" && value !== null && !Array.isArray(value)) return value as Record<string, unknown>;
  if (typeof value !== "string" || value.trim().length === 0) return {};
  try {
    const parsed = JSON.parse(value) as unknown;
    return typeof parsed === "object" && parsed !== null && !Array.isArray(parsed) ? parsed as Record<string, unknown> : {};
  } catch {
    return {};
  }
}

function nonBlankString(value: unknown): string | undefined {
  if (typeof value !== "string") return undefined;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

async function createHydrationCache(): Promise<OwnedHydrationCache> {
  const ttlMs = Number(process.env.OPENPLANNER_HYDRATION_CACHE_TTL_MS ?? 5 * 60 * 60 * 1000);
  if (!Number.isSafeInteger(ttlMs) || ttlMs <= 0) throw new Error("hydration cache TTL must be a positive safe integer");
  const layers: CacheHandle[] = [createMemoryLruCache({ maxEntries: 1024, defaultTtlMs: ttlMs })];

  let close: () => Promise<void> = async () => {};
  const lmdbPath = process.env.OPENPLANNER_HYDRATION_LMDB_PATH;
  if (lmdbPath) {
    const db = open({ path: lmdbPath });
    close = async () => { await db.close(); };
    layers.push(createLmdbCache({ db, prefix: "hydration:", defaultTtlMs: ttlMs }));
  }

  return { cache: layers.length === 1 ? layers[0]! : createLayeredCache(layers), close };
}

export async function getHydrationCache(): Promise<CacheHandle> {
  if (hydrationClosePromise) await hydrationClosePromise;
  if (!hydrationCachePromise) {
    const pending = createHydrationCache().catch(error => {
      if (hydrationCachePromise === pending) hydrationCachePromise = null;
      throw error;
    });
    hydrationCachePromise = pending;
  }
  return (await hydrationCachePromise).cache;
}

/** Explicit process-level cache lifecycle. Concurrent closes share one operation. */
export async function closeHydrationCache(): Promise<void> {
  if (hydrationClosePromise) return hydrationClosePromise;
  const owned = hydrationCachePromise;
  if (!owned) return;
  hydrationCachePromise = null;
  hydrationClosePromise = owned.then(resource => resource.close()).finally(() => { hydrationClosePromise = null; });
  return hydrationClosePromise;
}

let sdkCacheOwners = 0;

/** SDK instances share the process cache; only the last owner's release closes it. */
export function acquireHydrationCacheOwner(): () => Promise<void> {
  sdkCacheOwners++;
  let released = false;
  return async () => {
    if (released) return;
    released = true;
    if (--sdkCacheOwners === 0) await closeHydrationCache();
  };
}

export function sourceRoot(): string | undefined {
  const root = process.env.OPENPLANNER_SOURCE_ROOT?.trim();
  return root ? root : undefined;
}

export function sourcePathFromRow(row: Record<string, unknown>): string | undefined {
  const extra = parseJsonObject(row.extra);
  const metadata = parseJsonObject(extra.metadata);
  const migration2 = parseJsonObject(extra.migration_2);
  return nonBlankString(extra.source_path)
    ?? nonBlankString(extra.path)
    ?? nonBlankString(metadata.path)
    ?? nonBlankString(metadata.file_id)
    ?? nonBlankString(migration2.source_path);
}

function withinRoot(root: string, candidate: string): boolean {
  const relative = path.relative(root, candidate);
  return !path.isAbsolute(relative) && relative !== ".." && !relative.startsWith(`..${path.sep}`);
}

function sourceFilePathWithinRoot(row: Record<string, unknown>, configuredRoot: string): string | undefined {
  const rawPath = sourcePathFromRow(row);
  if (!rawPath) return undefined;

  const root = path.resolve(configuredRoot);
  const candidate = path.isAbsolute(rawPath) ? path.resolve(rawPath) : path.resolve(root, rawPath);
  return withinRoot(root, candidate) ? candidate : undefined;
}

export function safeSourceFilePath(row: Record<string, unknown>): string | undefined {
  const root = sourceRoot();
  return root ? sourceFilePathWithinRoot(row, root) : undefined;
}

export async function loadHydrationSourceText(row: Record<string, unknown>): Promise<string | null> {
  const configuredRoot = sourceRoot();
  if (!configuredRoot) return null;
  let cacheKey: string | null = null;
  try {
    cacheKey = documentCacheKey(row);
  } catch {
    return null;
  }
  if (!cacheKey) return null;

  const candidate = sourceFilePathWithinRoot(row, configuredRoot);
  if (!candidate) return null;
  // Linux exposes the kernel's opened-file identity through procfs. Hosts without
  // that capability fail closed rather than falling back to a pathname race.
  if (process.platform !== "linux") return null;
  let rootHandle: FileHandle | undefined;
  let fileHandle: FileHandle | undefined;
  try {
    const [root, filePath] = await Promise.all([realpath(configuredRoot), realpath(candidate)]);
    if (!withinRoot(root, filePath)) return null;
    rootHandle = await openFile(root, constants.O_RDONLY | constants.O_DIRECTORY);
    fileHandle = await openFile(filePath, constants.O_RDONLY | constants.O_NONBLOCK);
    const [openedRoot, openedFile, fileStat] = await Promise.all([
      readlink(`/proc/self/fd/${rootHandle.fd}`),
      readlink(`/proc/self/fd/${fileHandle.fd}`),
      fileHandle.stat(),
    ]);
    if (!fileStat.isFile() || openedRoot.endsWith(" (deleted)") || openedFile.endsWith(" (deleted)")
        || openedRoot !== root || openedFile !== filePath || !withinRoot(openedRoot, openedFile)) return null;
    // Validate and read the same descriptor. A later path swap cannot retarget it.
    const fileCacheKey = "openplanner:hydration-file:v5:" + JSON.stringify([
      cacheKey, openedFile, fileStat.dev, fileStat.ino, fileStat.size, fileStat.mtimeMs,
    ]);
    const hydrationCache = await getHydrationCache();
    const cached = await cacheGet(hydrationCache, fileCacheKey);
    if (typeof cached === "string") return cached;
    const text = await fileHandle.readFile("utf8");
    try { await cachePut(hydrationCache, fileCacheKey, text); }
    catch { /* Optional cache persistence cannot discard a validated read. */ }
    return text;
  } catch {
    return null;
  } finally {
    try { await fileHandle?.close(); }
    finally { await rootHandle?.close(); }
  }
}

export async function hydrateRowFromSourceCache(row: Record<string, unknown>): Promise<Record<string, unknown>> {
  if (!documentNeedsHydration(row)) return row;
  const sourceText = await loadHydrationSourceText(row);
  return hydrateDocumentRow(row, sourceText).row as Record<string, unknown>;
}
