import crypto from "node:crypto";
import type { ClientSession, Collection, Filter } from "mongodb";
import type { IEmbeddingFunction } from "./embeddings.js";
import { formatEmbeddingQueryText, formatEmbeddingPassageText } from "./embedding-text.js";
import { reconcileManagedTtl } from "./mongodb.js";
import { batchPreparedChunks, isContextOverflowError, prepareIndexDocument } from "./indexing.js";
import type { MongoConnection, MongoVectorDocument, MongoVectorPartitionDocument } from "./mongodb.js";
import { OPENPLANNER_SCHEMA_TARGETS, vectorChunkMigrationState } from "./schema-versions.js";
import { loadHydrationSourceText } from "./source-hydration.js";
import { assertNonblankIdentifier } from "./types.js";

export type MongoVectorTier = "hot" | "compact";

type RawQueryResult = {
  ids: string[][];
  documents: string[][];
  metadatas: Array<Array<Record<string, unknown> | null>>;
  distances: Array<Array<number | null>>;
  include: ["documents", "metadatas", "distances"];
  partial?: boolean;
  discarded_invalid_timestamp_count?: number;
  unavailable_partitions?: string[];
  queried_partition_count?: number;
};

export class VectorQueryUnavailableError extends Error {
  constructor(readonly unavailablePartitions: string[], phase: "embedding" | "execution" | "mixed" = "embedding") {
    super(phase === "embedding" ? "vector query embedding unavailable for all partitions"
      : phase === "execution" ? "vector query execution unavailable for all partitions"
        : "vector query unavailable for all partitions");
    this.name = "Error";
  }
}

export type MongoVectorEntry = {
  id: string;
  parentId: string;
  text: string;
  embedding: number[];
  metadata: Record<string, unknown>;
};

type SourceRef = {
  source_path?: string;
  url?: string;
  hostname?: string;
  lake?: string;
  content_hash?: string;
};

const VECTOR_SEARCH_INDEX_NAME = "vs_embedding";
const FILTERABLE_PATHS = ["source", "kind", "project", "session", "visibility", "quality_label", "parent_id", "embedding_model"] as const;
const FILTERABLE_PATH_SET = new Set<string>(FILTERABLE_PATHS);
const VEXX_BASE_URL = String(process.env.VEXX_BASE_URL ?? "").trim();
const VEXX_API_KEY = String(process.env.VEXX_API_KEY ?? "").trim();
const VEXX_DEVICE = String(process.env.VEXX_DEVICE ?? "AUTO").trim() || "AUTO";
const VEXX_REQUIRE_ACCEL = /^(1|true|yes|on)$/i.test(String(process.env.VEXX_REQUIRE_ACCEL ?? ""));
const VEXX_ENFORCE = /^(1|true|yes|on)$/i.test(String(process.env.VEXX_ENFORCE ?? ""));
const VEXX_TIMEOUT_MS = (() => {
  const parsed = Number(process.env.VEXX_TIMEOUT_MS ?? "1500");
  return Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed) : 1500;
})();
const VEXX_MIN_CANDIDATES = (() => {
  const parsed = Number(process.env.VEXX_MIN_CANDIDATES ?? "256");
  return Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed) : 256;
})();
const VECTOR_SCAN_BATCH_SIZE = 256;

function emptyResult(): RawQueryResult {
  return { ids: [[]], documents: [[]], metadatas: [[]], distances: [[]], include: ["documents", "metadatas", "distances"], queried_partition_count: 0 };
}

function getFlatCollection(mongo: MongoConnection, tier: MongoVectorTier): Collection<MongoVectorDocument> {
  return tier === "compact" ? mongo.compactVectors : mongo.hotVectors;
}

function asDate(value: unknown): Date {
  if (value === undefined) return new Date();
  if (!(value instanceof Date) && typeof value !== "string" && typeof value !== "number") {
    throw new Error("vector metadata.ts must be a valid timestamp when supplied");
  }
  const parsed = value instanceof Date ? value : new Date(value);
  if (!Number.isFinite(parsed.getTime())) throw new Error("vector metadata.ts must be a valid timestamp when supplied");
  return parsed;
}

function toStringOrNull(value: unknown): string | null {
  if (value === undefined || value === null) return null;
  if (typeof value !== "string") throw new Error("vector metadata fields must be strings or null");
  const next = value;
  return next.length > 0 ? next : null;
}

function toNumberOrNull(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim().length > 0) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function toStringArray(value: unknown): string[] {
  if (value === undefined || value === null) return [];
  if (!Array.isArray(value) || value.some(entry => typeof entry !== "string" || !entry.trim())) {
    throw new Error("vector labels must be an array of nonblank strings");
  }
  return [...new Set(value.map(entry => entry.trim()))];
}

function validateVectorMetadata(metadata: Record<string, unknown>): void {
  asDate(metadata.ts);
  for (const field of ["source", "kind", "project", "session", "visibility", "author", "role", "model", "quality_label", "embedding_model", "title", "seed_id"]) {
    toStringOrNull(metadata[field]);
  }
  toStringArray(metadata.labels);
}

function parsePositiveIntEnv(name: string): number {
  const parsed = Number.parseInt(process.env[name] ?? "0", 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : 0;
}

function ttlExpiryFromNow(ttlSeconds: number, now: Date): Date | undefined {
  return ttlSeconds > 0 ? new Date(now.getTime() + ttlSeconds * 1000) : undefined;
}

function nonBlankString(value: unknown): string | undefined {
  if (typeof value !== "string") return undefined;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function objectValue(value: unknown): Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value) ? value as Record<string, unknown> : {};
}

function sourceRefFromExtra(extra: Record<string, unknown> | undefined): SourceRef | null {
  const metadata = objectValue(extra?.metadata);
  const sourcePath = nonBlankString(extra?.source_path)
    ?? nonBlankString(extra?.path)
    ?? nonBlankString(metadata.path)
    ?? nonBlankString(metadata.file_id);
  const url = nonBlankString(extra?.url) ?? nonBlankString(metadata.url);
  const hostname = nonBlankString(extra?.hostname) ?? nonBlankString(metadata.hostname);
  const lake = nonBlankString(extra?.lake) ?? nonBlankString(metadata.lake);
  const contentHash = nonBlankString(extra?.content_hash)
    ?? nonBlankString(metadata.content_hash)
    ?? nonBlankString(objectValue(extra?.migration_2).text_hash_sha256);

  // Hydration currently supports filesystem paths only. Keep URL-only text inline.
  if (!sourcePath) return null;
  return {
    ...(sourcePath ? { source_path: sourcePath } : {}),
    ...(url ? { url } : {}),
    ...(hostname ? { hostname } : {}),
    ...(lake ? { lake } : {}),
    ...(contentHash ? { content_hash: contentHash } : {}),
  };
}

export async function hydrateVectorDocumentText(doc: MongoVectorDocument): Promise<string> {
  if (doc.text || doc.source_text_redacted !== true) return doc.text ?? "";
  const ref = objectValue(doc.source_ref);
  const sourceText = await loadHydrationSourceText({
    id: doc.parent_id ?? doc._id,
    text: "",
    project: nonBlankString(ref.lake) ?? doc.project,
    extra: {
      source_path: nonBlankString(ref.source_path) ?? nonBlankString(ref.sourcePath),
      url: nonBlankString(ref.url),
      hostname: nonBlankString(ref.hostname),
      lake: nonBlankString(ref.lake) ?? doc.project,
      // Indexing computed this source hash; caller metadata must not alias two files.
      content_hash: nonBlankString(doc.text_hash_sha256) ?? nonBlankString(ref.content_hash) ?? nonBlankString(ref.contentHash),
    },
  });
  if (!sourceText) return "";
  if (doc.text_hash_sha256 && /^[0-9a-f]{64}$/i.test(doc.text_hash_sha256)
    && sha256Text(sourceText) !== doc.text_hash_sha256.toLowerCase()) return "";
  const start = typeof doc.char_start === "number" && doc.char_start >= 0 ? doc.char_start : null;
  const end = typeof doc.char_end === "number" && doc.char_end >= 0 ? doc.char_end : null;
  if (start !== null && end !== null && end >= start) {
    if (doc.source_coordinate_space === "normalized") {
      const normalized = prepareIndexDocument({ parentId: doc.parent_id ?? doc._id, text: sourceText }).normalizedText;
      const chunk = normalized.slice(start, end);
      return end <= normalized.length && doc.chunk_text_hash_sha256 && sha256Text(chunk) === doc.chunk_text_hash_sha256 ? chunk : "";
    }
    const rawChunk = sourceText.slice(start, end);
    if (!doc.chunk_text_hash_sha256 || sha256Text(rawChunk) === doc.chunk_text_hash_sha256) return rawChunk;
    // Historical rows recorded offsets after normalization. Validate that candidate
    // against the saved chunk hash before returning it; never return a shifted slice.
    const normalized = prepareIndexDocument({ parentId: doc.parent_id ?? doc._id, text: sourceText }).normalizedText;
    const normalizedChunk = normalized.slice(start, end);
    return sha256Text(normalizedChunk) === doc.chunk_text_hash_sha256 ? normalizedChunk : "";
  }
  return doc.chunk_text_hash_sha256 && sha256Text(sourceText) !== doc.chunk_text_hash_sha256 ? "" : sourceText;
}

function sha256Text(text: string): string {
  return crypto.createHash("sha256").update(text, "utf8").digest("hex");
}

function sourceRedactionMetadata(params: {
  extra?: Record<string, unknown>;
  rawText: string;
  normalizedSource: () => string;
  chunkText: string;
  charStart?: number | null;
  charEnd?: number | null;
}): Record<string, unknown> {
  const sourceRef = sourceRefFromExtra(params.extra);
  if (!sourceRef || typeof params.charStart !== "number" || typeof params.charEnd !== "number"
    || params.charStart < 0 || params.charEnd < params.charStart) return {};
  const rawMatches = params.charEnd <= params.rawText.length
    && params.rawText.slice(params.charStart, params.charEnd) === params.chunkText;
  // Only redact normalized references that can be reconstructed from the saved
  // source itself. Caller-supplied alternative HTML is not a hydration authority.
  const normalized = rawMatches ? null : params.normalizedSource();
  if (!rawMatches && (!normalized || params.charEnd > normalized.length
    || normalized.slice(params.charStart, params.charEnd) !== params.chunkText)) return {};
  return {
    source_text_redacted: true,
    source_coordinate_space: rawMatches ? "raw" : "normalized",
    source_ref: sourceRef,
    text_hash_sha256: sha256Text(params.rawText),
    chunk_text_hash_sha256: sha256Text(params.chunkText),
    char_start: params.charStart ?? null,
    char_end: params.charEnd ?? null,
  };
}

function normalizeScalarFilterValue(value: unknown): unknown {
  if (typeof value === "string" || typeof value === "boolean" || typeof value === "number" && Number.isFinite(value)) return value;
  if (value instanceof Date && Number.isFinite(value.getTime())) return value;
  return undefined;
}

function buildMongoFilter(where: Record<string, unknown> | undefined): Filter<MongoVectorDocument> {
  const filter: Filter<MongoVectorDocument> = {};
  if (where === undefined) return filter;
  if (!where || typeof where !== "object" || Array.isArray(where)) throw new Error("vector filter must be an object");

  for (const [key, rawValue] of Object.entries(where)) {
    if (!FILTERABLE_PATH_SET.has(key)) throw new Error(`unsupported vector filter field: ${key}`);

    const scalar = normalizeScalarFilterValue(rawValue);
    if (scalar !== undefined) {
      (filter as Record<string, unknown>)[key] = scalar;
      continue;
    }

    if (typeof rawValue === "object" && rawValue !== null && !Array.isArray(rawValue)) {
      const record = rawValue as Record<string, unknown>;
      if (Object.keys(record).length !== 1) throw new Error(`unsupported vector filter value for ${key}`);
      if (Array.isArray(record.$in) && record.$in.length > 0
          && record.$in.every(value => normalizeScalarFilterValue(value) !== undefined)) {
        (filter as Record<string, unknown>)[key] = { $in: record.$in };
        continue;
      }
      if (Object.prototype.hasOwnProperty.call(record, "$eq")) {
        const normalized = normalizeScalarFilterValue(record.$eq);
        if (normalized === undefined) throw new Error(`unsupported vector filter value for ${key}`);
        (filter as Record<string, unknown>)[key] = normalized;
        continue;
      }
      if (Object.prototype.hasOwnProperty.call(record, "$ne")) {
        const normalized = normalizeScalarFilterValue(record.$ne);
        if (normalized === undefined) throw new Error(`unsupported vector filter value for ${key}`);
        (filter as Record<string, unknown>)[key] = { $ne: normalized };
        continue;
      }
      throw new Error(`unsupported vector filter value for ${key}`);
    }

    throw new Error(`unsupported vector filter value for ${key}`);
  }

  return filter;
}

function dot(left: readonly number[], right: readonly number[]): number {
  if (left.length !== right.length) {
    throw new RangeError(`embedding length mismatch: left=${left.length} right=${right.length}`);
  }
  let total = 0;
  for (let index = 0; index < left.length; index += 1) {
    total += left[index]! * right[index]!;
  }
  return total;
}

function magnitude(input: readonly number[]): number {
  return Math.sqrt(dot(input, input));
}

function cosineSimilarity(left: readonly number[], right: readonly number[]): number {
  if (left.length === 0 || left.length !== right.length) return Number.NEGATIVE_INFINITY;
  const leftMagnitude = magnitude(left);
  const rightMagnitude = magnitude(right);
  if (leftMagnitude === 0 || rightMagnitude === 0) return Number.NEGATIVE_INFINITY;
  return dot(left, right) / (leftMagnitude * rightMagnitude);
}

function vexxRequiredError(context: string): Error {
  return new Error(`vexx_required:${context}`);
}

async function queryPartitionWithVexxTopK(params: {
  candidates: MongoVectorDocument[];
  queryEmbedding: number[];
  k: number;
  minimumAlreadySatisfied?: boolean;
}): Promise<Array<{ doc: MongoVectorDocument; score: number }> | null> {
  if (!VEXX_BASE_URL) return null;
  const validCandidates = params.candidates.filter(
    (doc) => Array.isArray(doc.embedding) && doc.embedding.length === params.queryEmbedding.length,
  );
  if (validCandidates.length === 0) return [];
  if (!VEXX_ENFORCE && !params.minimumAlreadySatisfied
    && validCandidates.length < Math.max(params.k, VEXX_MIN_CANDIDATES)) return null;

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), VEXX_TIMEOUT_MS);
  let response: Response;
  try {
    response = await fetch(`${VEXX_BASE_URL.replace(/\/$/, "")}/v1/cosine/topk`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...(VEXX_API_KEY ? { Authorization: `Bearer ${VEXX_API_KEY}` } : {}),
      },
      body: JSON.stringify({
        query: params.queryEmbedding,
        candidates: validCandidates.map((doc) => ({ id: doc._id, embedding: doc.embedding })),
        k: Math.max(1, params.k),
        device: VEXX_DEVICE,
        requireAccel: VEXX_REQUIRE_ACCEL,
      }),
      signal: controller.signal,
    });
  } finally {
    clearTimeout(timeout);
  }

  if (!response.ok) return null;

  const payload = await response.json() as { matches?: unknown } | null;
  if (!payload || !Array.isArray(payload.matches) || payload.matches.length !== validCandidates.length) return null;
  const docsById = new Map(validCandidates.map((doc) => [doc._id, doc]));
  if (docsById.size !== validCandidates.length) return null;
  const seen = new Set<string>();
  const matches: Array<{ doc: MongoVectorDocument; score: number }> = [];
  for (const value of payload.matches) {
    if (!value || typeof value !== "object") return null;
    const match = value as { id?: unknown; score?: unknown };
    if (typeof match.id !== "string" || typeof match.score !== "number" || !Number.isFinite(match.score)
      || seen.has(match.id)) return null;
    const doc = docsById.get(match.id);
    if (!doc) return null;
    seen.add(match.id);
    matches.push({ doc, score: match.score });
  }
  return matches;
}

// Admit Date or legacy date strings before ranking/parent limits and metadata conversion.
function normalizeQueryTimestamp(doc: MongoVectorDocument, onInvalid: (id: string) => void): MongoVectorDocument | null {
  const value: unknown = doc.ts;
  const ms = value instanceof Date ? value.getTime() : typeof value === "string" ? Date.parse(value) : NaN;
  if (!Number.isFinite(ms)) { onInvalid(doc._id); return null; }
  return { ...doc, ts: new Date(ms) };
}

function toMetadata(doc: MongoVectorDocument): Record<string, unknown> {
  return {
    ts: doc.ts.toISOString(),
    source: doc.source,
    kind: doc.kind,
    project: doc.project ?? "",
    session: doc.session ?? "",
    author: doc.author ?? "",
    role: doc.role ?? "",
    model: doc.model ?? "",
    visibility: doc.visibility ?? "",
    quality_label: doc.quality_label ?? "",
    labels: doc.labels ?? [],
    title: doc.title ?? "",
    embedding_model: doc.embedding_model ?? "",
    embedding_dimensions: doc.embedding_dimensions ?? null,
    search_tier: doc.search_tier,
    parent_id: doc.parent_id,
    chunk_id: doc.chunk_id ?? doc._id,
    chunk_index: doc.chunk_index ?? 0,
    chunk_count: doc.chunk_count ?? 1,
    normalized_format: doc.normalized_format ?? null,
    normalized_estimated_tokens: doc.normalized_estimated_tokens ?? null,
    raw_estimated_tokens: doc.raw_estimated_tokens ?? null,
    seed_id: doc.seed_id ?? null,
    member_count: doc.member_count ?? null,
    char_count: doc.char_count ?? null,
    source_text_redacted: doc.source_text_redacted ?? false,
    source_coordinate_space: doc.source_coordinate_space ?? null,
    source_ref: doc.source_ref ?? null,
    text_hash_sha256: doc.text_hash_sha256 ?? null,
    chunk_text_hash_sha256: doc.chunk_text_hash_sha256 ?? null,
    char_start: doc.char_start ?? null,
    char_end: doc.char_end ?? null,
    schema_version: doc.schema_version ?? null,
    migration_state: doc.migration_state ?? null,
  };
}

function vectorTtlSeconds(mongo: MongoConnection, tier: MongoVectorTier): number {
  return tier === "hot"
    ? mongo.retention?.eventsTtlSeconds ?? parsePositiveIntEnv("MONGODB_EVENTS_TTL_SECONDS")
    : mongo.retention?.compactedTtlSeconds ?? parsePositiveIntEnv("MONGODB_COMPACTED_TTL_SECONDS");
}

async function ensureVectorTtlIndex(collection: Collection<MongoVectorDocument>, tier: MongoVectorTier, ttlSeconds: number): Promise<void> {
  const name = tier === "hot" ? "hot_vectors_ttl" : "compact_vectors_ttl";
  await reconcileManagedTtl(collection, name, ttlSeconds);
}

function sanitizeModelName(model: string): string {
  return model.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "").slice(0, 48) || "model";
}

function hashModelName(model: string): string {
  return crypto.createHash("sha1").update(model).digest("hex").slice(0, 10);
}

function makePartitionCollectionName(baseCollectionName: string, model: string, dimensions: number): string {
  return `${baseCollectionName}__${sanitizeModelName(model)}__d${dimensions}__${hashModelName(model)}`;
}

function makePartitionKey(model: string, dimensions: number): string {
  return `${model}::${dimensions}`;
}

async function ensurePartitionSupportIndexes(collection: Collection<MongoVectorDocument>, tier: MongoVectorTier, ttlSeconds: number): Promise<void> {
  await collection.createIndex({ parent_id: 1, chunk_index: 1 });
  await collection.createIndex({ ts: -1 });
  await collection.createIndex({ source: 1, ts: -1 });
  await collection.createIndex({ kind: 1, ts: -1 });
  await collection.createIndex({ project: 1, ts: -1 });
  await collection.createIndex({ session: 1, ts: -1 });
  await collection.createIndex({ visibility: 1, ts: -1 });
  await collection.createIndex({ quality_label: 1, ts: -1 });
  await collection.createIndex({ labels: 1, ts: -1 });
  await collection.createIndex({ embedding_model: 1, embedding_dimensions: 1, ts: -1 });
  await collection.createIndex({ schema_version: 1, ts: -1 });
  await ensureVectorTtlIndex(collection, tier, ttlSeconds);
}

async function ensurePartitionVectorSearchIndex(
  mongo: MongoConnection,
  partition: MongoVectorPartitionDocument,
): Promise<MongoVectorPartitionDocument["searchIndexStatus"]> {
  const collection = mongo.db.collection<MongoVectorDocument>(partition.collectionName);
  try {
    const existing = await collection.listSearchIndexes(partition.searchIndexName).toArray();
    if (existing.length === 0) {
      await collection.createSearchIndex({
        name: partition.searchIndexName,
        type: "vectorSearch",
        definition: {
          fields: [
            {
              type: "vector",
              path: "embedding",
              numDimensions: partition.dimensions,
              similarity: "cosine",
            },
            ...FILTERABLE_PATHS.map((path) => ({ type: "filter", path })),
          ],
        },
      });
    }

    // One readiness observation per attempt. Building indexes retain the scan
    // fallback rather than holding ingestion for a minute.
    const index = existing[0] as Record<string, unknown> | undefined;
    const nativeStatus = String(index?.status ?? "UNKNOWN").toUpperCase();
    if (nativeStatus === "FAILED" || nativeStatus === "DOES_NOT_EXIST") {
      throw new Error(`vector search index ${partition.searchIndexName} failed: ${nativeStatus}`);
    }
    const status = nativeStatus === "READY" && index?.queryable === true ? "ready" : "pending";

    await mongo.vectorPartitions.updateOne(
      { _id: partition._id },
      {
        $set: {
          searchIndexStatus: status,
          lastError: null,
          updatedAt: new Date(),
        },
      },
    );
    return status;
  } catch (error) {
    const detail = error instanceof Error ? error.message : String(error);
    await mongo.vectorPartitions.updateOne(
      { _id: partition._id },
      {
        $set: {
          searchIndexStatus: "error",
          lastError: detail,
          updatedAt: new Date(),
        },
      },
      { upsert: true },
    );
    return "error";
  }
}

async function materializeVectorPartition(
  mongo: MongoConnection,
  tier: MongoVectorTier,
  model: string,
  dimensions: number,
): Promise<{ partition: MongoVectorPartitionDocument; collection: Collection<MongoVectorDocument> }> {
  const baseCollection = getFlatCollection(mongo, tier);
  const collectionName = makePartitionCollectionName(baseCollection.collectionName, model, dimensions);
  const partitionId = `${tier}:${model}:${dimensions}`;
  const now = new Date();

  await mongo.vectorPartitions.updateOne(
    { _id: partitionId },
    {
      $set: {
        tier,
        model,
        dimensions,
        collectionName,
        searchIndexName: VECTOR_SEARCH_INDEX_NAME,
        updatedAt: now,
      },
      $setOnInsert: {
        searchIndexStatus: "pending",
        lastError: null,
        createdAt: now,
      },
    },
    { upsert: true },
  );

  const partition = await mongo.vectorPartitions.findOne({ _id: partitionId });
  if (!partition) {
    throw new Error(`failed to materialize vector partition ${partitionId}`);
  }

  const collection = mongo.db.collection<MongoVectorDocument>(collectionName);
  await ensurePartitionSupportIndexes(collection, tier, vectorTtlSeconds(mongo, tier));
  return { partition: { ...partition, collectionName }, collection };
}

const partitionSetup = new WeakMap<MongoConnection, Map<string, Promise<{ partition: MongoVectorPartitionDocument; collection: Collection<MongoVectorDocument> }>>>();

type SearchReadiness = {
  status: MongoVectorPartitionDocument["searchIndexStatus"];
  retryAfter: number;
  pending?: Promise<MongoVectorPartitionDocument["searchIndexStatus"]>;
};
const partitionSearchReadiness = new WeakMap<MongoConnection, Map<string, SearchReadiness>>();

async function ensureVectorPartition(mongo: MongoConnection, tier: MongoVectorTier, model: string, dimensions: number) {
  let cache = partitionSetup.get(mongo);
  if (!cache) { cache = new Map(); partitionSetup.set(mongo, cache); }
  const key = JSON.stringify([tier, model, dimensions]);
  let pending = cache.get(key);
  if (!pending) {
    const owner = cache;
    pending = materializeVectorPartition(mongo, tier, model, dimensions).catch(error => {
      // Retry actual storage failures; unavailable search does not invalidate
      // completed collection/index/retention setup.
      if (owner.get(key) === pending) owner.delete(key);
      throw error;
    });
    cache.set(key, pending);
  }
  const result = await pending;
  const partition = await refreshPartitionReadiness(mongo, result.partition);
  return { ...result, partition };
}

async function refreshPartitionReadiness(mongo: MongoConnection, partition: MongoVectorPartitionDocument): Promise<MongoVectorPartitionDocument> {
  if (partition.searchIndexStatus === "ready") return partition;
  const key = JSON.stringify([partition.tier, partition.model, partition.dimensions]);
  let states = partitionSearchReadiness.get(mongo);
  if (!states) { states = new Map(); partitionSearchReadiness.set(mongo, states); }
  let state = states.get(key);
  if (!state) { state = { status: partition.searchIndexStatus, retryAfter: 0 }; states.set(key, state); }
  if (state.status !== "ready" && !state.pending && Date.now() >= state.retryAfter) {
    const current = state;
    current.pending = ensurePartitionVectorSearchIndex(mongo, partition).then(status => {
      current.status = status;
      return status;
    }).finally(() => {
      current.retryAfter = Date.now() + 5_000;
      current.pending = undefined;
    });
  }
  if (state.pending) await state.pending;
  return { ...partition, searchIndexStatus: state.status };
}

async function withMongoTransaction<T>(
  mongo: MongoConnection,
  work: (session: ClientSession | undefined) => Promise<T>,
): Promise<T> {
  const session = mongo.client.startSession();
  try {
    return await session.withTransaction(async () => work(session));
  } finally {
    await session.endSession();
  }
}

async function bulkWriteVectorDocuments(
  collection: Collection<MongoVectorDocument>,
  documents: ReadonlyArray<MongoVectorDocument>,
  now: Date,
  session?: ClientSession,
): Promise<void> {
  if (documents.length === 0) return;
  await collection.bulkWrite(
    documents.map((doc) => {
      const { createdAt, ...docWithoutCreatedAt } = doc;
      return {
        updateOne: {
          filter: { _id: doc._id },
          update: {
            $set: { ...docWithoutCreatedAt, updatedAt: now },
            $setOnInsert: { createdAt },
          },
          upsert: true,
        },
      };
    }),
    { ordered: false, ...(session ? { session } : {}) },
  );
}

async function deletePartitionDocuments(
  mongo: MongoConnection,
  partitionCollectionNames: ReadonlyArray<string>,
  filter: Filter<MongoVectorDocument>,
  session?: ClientSession,
): Promise<void> {
  if (session) {
    for (const collectionName of partitionCollectionNames) {
      await mongo.db.collection<MongoVectorDocument>(collectionName).deleteMany(filter, { session });
    }
    return;
  }

  await Promise.all(
    partitionCollectionNames.map((collectionName) => mongo.db.collection<MongoVectorDocument>(collectionName).deleteMany(filter)),
  );
}

export async function listMongoVectorPartitions(
  mongo: MongoConnection,
  tier: MongoVectorTier,
): Promise<MongoVectorPartitionDocument[]> {
  return mongo.vectorPartitions.find({ tier }).sort({ updatedAt: -1 }).toArray();
}

function toMongoVectorDocument(entry: MongoVectorEntry, tier: MongoVectorTier, now: Date, ttlSeconds: number): MongoVectorDocument {
  assertNonblankIdentifier(entry.id, "vector id");
  assertNonblankIdentifier(entry.parentId, "vector parentId");
  if (!Array.isArray(entry.embedding) || entry.embedding.length === 0) throw new Error("embedding must be a nonempty finite number array");
  for (const value of entry.embedding) {
    if (typeof value !== "number" || !Number.isFinite(value)) throw new Error("embedding must contain finite numbers");
  }
  validateVectorMetadata(entry.metadata);
  const ts = asDate(entry.metadata.ts);
  const sourceTextRedacted = entry.metadata.source_text_redacted === true;
  const labels = toStringArray(entry.metadata.labels);
  const hasSourceExpiry = Object.hasOwn(entry.metadata, "source_expires_at");
  const sourceExpiry = entry.metadata.source_expires_at;
  if (hasSourceExpiry && sourceExpiry !== null
      && (!(sourceExpiry instanceof Date) || !Number.isFinite(sourceExpiry.getTime()))) {
    throw new Error("source_expires_at must be a valid Date or null");
  }
  return {
    _id: entry.id,
    parent_id: entry.parentId,
    text: sourceTextRedacted ? "" : entry.text,
    embedding: entry.embedding,
    ts,
    source: toStringOrNull(entry.metadata.source) ?? "",
    kind: toStringOrNull(entry.metadata.kind) ?? "",
    project: toStringOrNull(entry.metadata.project),
    session: toStringOrNull(entry.metadata.session),
    author: toStringOrNull(entry.metadata.author),
    role: toStringOrNull(entry.metadata.role),
    model: toStringOrNull(entry.metadata.model),
    visibility: toStringOrNull(entry.metadata.visibility),
    quality_label: toStringOrNull(entry.metadata.quality_label),
    labels,
    title: toStringOrNull(entry.metadata.title),
    embedding_model: toStringOrNull(entry.metadata.embedding_model),
    embedding_dimensions: entry.embedding.length,
    search_tier: tier,
    chunk_id: toStringOrNull(entry.metadata.chunk_id) ?? entry.id,
    chunk_index: toNumberOrNull(entry.metadata.chunk_index),
    chunk_count: toNumberOrNull(entry.metadata.chunk_count),
    normalized_format: toStringOrNull(entry.metadata.normalized_format),
    normalized_estimated_tokens: toNumberOrNull(entry.metadata.normalized_estimated_tokens),
    raw_estimated_tokens: toNumberOrNull(entry.metadata.raw_estimated_tokens),
    seed_id: toStringOrNull(entry.metadata.seed_id),
    member_count: toNumberOrNull(entry.metadata.member_count),
    char_count: toNumberOrNull(entry.metadata.char_count),
    source_text_redacted: sourceTextRedacted,
    source_coordinate_space: entry.metadata.source_coordinate_space === "raw" || entry.metadata.source_coordinate_space === "normalized"
      ? entry.metadata.source_coordinate_space : null,
    source_ref: sourceTextRedacted ? objectValue(entry.metadata.source_ref) : null,
    text_hash_sha256: toStringOrNull(entry.metadata.text_hash_sha256),
    chunk_text_hash_sha256: toStringOrNull(entry.metadata.chunk_text_hash_sha256),
    char_start: toNumberOrNull(entry.metadata.char_start),
    char_end: toNumberOrNull(entry.metadata.char_end),
    schema_version: OPENPLANNER_SCHEMA_TARGETS.vectorChunk,
    migration_state: vectorChunkMigrationState(now),
    expiresAt: hasSourceExpiry
      ? sourceExpiry as Date | null
      : labels.length > 0 ? null : ttlExpiryFromNow(ttlSeconds, now) ?? null,
    createdAt: now,
    updatedAt: now,
  };
}

function assertUniqueVectorIds(entries: ReadonlyArray<MongoVectorEntry>): void {
  const ids = new Set<string>();
  for (const entry of entries) {
    assertNonblankIdentifier(entry.id, "vector id");
    if (ids.has(entry.id)) throw new Error("Duplicate vector id in batch");
    ids.add(entry.id);
  }
}

export async function upsertMongoVectorDocuments(
  mongo: MongoConnection,
  tier: MongoVectorTier,
  entries: ReadonlyArray<MongoVectorEntry>,
): Promise<void> {
  if (entries.length === 0) return;

  assertUniqueVectorIds(entries);
  const now = new Date();
  const documents = entries.map((entry) => toMongoVectorDocument(entry, tier, now, vectorTtlSeconds(mongo, tier)));
  const flatCollection = getFlatCollection(mongo, tier);

  const groups = new Map<string, { model: string; dimensions: number; documents: MongoVectorDocument[] }>();
  for (const doc of documents) {
    const model = doc.embedding_model ?? "unknown-model";
    const dimensions = doc.embedding.length;
    const key = makePartitionKey(model, dimensions);
    const existing = groups.get(key) ?? { model, dimensions, documents: [] };
    existing.documents.push(doc);
    groups.set(key, existing);
  }

  const partitionCollections = new Map<string, Collection<MongoVectorDocument>>();
  for (const [key, group] of groups.entries()) {
    const { collection } = await ensureVectorPartition(mongo, tier, group.model, group.dimensions);
    partitionCollections.set(key, collection);
  }

  await withMongoTransaction(mongo, async (session) => {
    const existingDocs = await flatCollection.find(
      { _id: { $in: documents.map((doc) => doc._id) } },
      session ? { session } : undefined,
    ).project<{ _id: string; embedding_model?: string | null; embedding_dimensions?: number | null }>({
      _id: 1,
      embedding_model: 1,
      embedding_dimensions: 1,
    }).toArray();

    const stalePartitionIds = new Map<string, { model: string; dimensions: number; ids: string[] }>();
    const nextPartitionById = new Map(documents.map((doc) => [doc._id, makePartitionKey(doc.embedding_model ?? "unknown-model", doc.embedding.length)]));
    for (const existing of existingDocs) {
      const previousModel = existing.embedding_model ?? "unknown-model";
      const previousDimensions = Number(existing.embedding_dimensions ?? 0);
      if (!Number.isFinite(previousDimensions) || previousDimensions <= 0) continue;
      const previousKey = makePartitionKey(previousModel, previousDimensions);
      if (previousKey === nextPartitionById.get(existing._id)) continue;
      const stale = stalePartitionIds.get(previousKey) ?? { model: previousModel, dimensions: previousDimensions, ids: [] };
      stale.ids.push(existing._id);
      stalePartitionIds.set(previousKey, stale);
    }

    for (const stale of stalePartitionIds.values()) {
      await mongo.db.collection<MongoVectorDocument>(makePartitionCollectionName(flatCollection.collectionName, stale.model, stale.dimensions)).deleteMany(
        { _id: { $in: stale.ids } },
        session ? { session } : undefined,
      );
    }

    await bulkWriteVectorDocuments(flatCollection, documents, now, session);
    for (const [key, group] of groups.entries()) {
      const collection = partitionCollections.get(key);
      if (!collection) continue;
      await bulkWriteVectorDocuments(collection, group.documents, now, session);
    }
  });
}

async function deleteMongoVectorEntriesByParent(
  mongo: MongoConnection,
  tier: MongoVectorTier,
  parentId: string,
  session?: ClientSession,
): Promise<void> {
  await getFlatCollection(mongo, tier).deleteMany({ parent_id: parentId }, session ? { session } : undefined);
  const partitions = await listMongoVectorPartitions(mongo, tier);
  await deletePartitionDocuments(
    mongo,
    partitions.map((partition) => partition.collectionName),
    { parent_id: parentId },
    session,
  );
}

export async function deleteMongoVectorEntriesByFilter(
  mongo: MongoConnection,
  tier: MongoVectorTier,
  where: Record<string, unknown>,
): Promise<void> {
  const filter = buildMongoFilter(where);
  if (Object.keys(filter).length === 0) {
    throw new Error("deleteMongoVectorEntriesByFilter requires at least one supported filter field");
  }
  const partitions = await listMongoVectorPartitions(mongo, tier);
  await withMongoTransaction(mongo, async (session) => {
    await getFlatCollection(mongo, tier).deleteMany(filter, session ? { session } : undefined);
    await deletePartitionDocuments(
      mongo,
      partitions.map((partition) => partition.collectionName),
      filter,
      session,
    );
  });
}

async function updatePartitionVectorLabels(
  mongo: MongoConnection,
  tier: MongoVectorTier,
  parentId: string,
  update: Record<string, unknown> | Record<string, unknown>[],
  requiredLabel?: string,
): Promise<void> {
  const partitions = await listMongoVectorPartitions(mongo, tier);
  await Promise.all(partitions.map((partition) => (
    mongo.db.collection<MongoVectorDocument>(partition.collectionName).updateMany({ parent_id: parentId, ...(requiredLabel === undefined ? {} : { labels: requiredLabel }) }, update)
  )));
}

export async function addMongoVectorParentLabel(
  mongo: MongoConnection,
  parentId: string,
  label: string,
): Promise<void> {
  assertNonblankIdentifier(parentId, "vector parentId");
  const normalized = label.trim();
  if (!normalized) return;
  const update = [
    {
      $set: {
        labels: {
          $setUnion: [
            { $cond: [{ $isArray: "$labels" }, "$labels", []] },
            [normalized],
          ],
        },
        expiresAt: "$$REMOVE",
        updatedAt: new Date(),
      },
    },
  ];
  await Promise.all([
    mongo.hotVectors.updateMany({ parent_id: parentId }, update),
    mongo.compactVectors.updateMany({ parent_id: parentId }, update),
    updatePartitionVectorLabels(mongo, "hot", parentId, update),
    updatePartitionVectorLabels(mongo, "compact", parentId, update),
  ]);
}

export async function removeMongoVectorParentLabel(
  mongo: MongoConnection,
  parentId: string,
  label: string,
): Promise<void> {
  assertNonblankIdentifier(parentId, "vector parentId");
  const normalized = label.trim();
  if (!normalized) return;
  const now = new Date();
  const updateForTier = (tier: MongoVectorTier) => {
    const ttlSeconds = vectorTtlSeconds(mongo, tier);
    const expiresAt = ttlExpiryFromNow(ttlSeconds, now) ?? null;
    return [
      {
        $set: {
          labels: {
            $filter: {
              input: { $cond: [{ $isArray: "$labels" }, "$labels", []] },
              as: "existingLabel",
              cond: { $ne: ["$$existingLabel", normalized] },
            },
          },
          updatedAt: now,
        },
      },
      {
        $set: {
          expiresAt: {
            $cond: [
              { $gt: [{ $size: "$labels" }, 0] },
              "$$REMOVE",
              expiresAt,
            ],
          },
        },
      },
    ];
  };
  const hotUpdate = updateForTier("hot");
  const compactUpdate = updateForTier("compact");
  await Promise.all([
    mongo.hotVectors.updateMany({ parent_id: parentId, labels: normalized }, hotUpdate),
    mongo.compactVectors.updateMany({ parent_id: parentId, labels: normalized }, compactUpdate),
    updatePartitionVectorLabels(mongo, "hot", parentId, hotUpdate, normalized),
    updatePartitionVectorLabels(mongo, "compact", parentId, compactUpdate, normalized),
  ]);
}

export async function replaceMongoVectorEntries(
  mongo: MongoConnection,
  tier: MongoVectorTier,
  parentId: string,
  entries: ReadonlyArray<MongoVectorEntry>,
): Promise<void> {
  await replaceMongoVectorParents(mongo, tier, [parentId], entries);
}

async function replaceMongoVectorParents(
  mongo: MongoConnection,
  tier: MongoVectorTier,
  parentIds: ReadonlyArray<string>,
  entries: ReadonlyArray<MongoVectorEntry>,
): Promise<void> {
  for (const parentId of parentIds) assertNonblankIdentifier(parentId, "vector parentId");
  assertUniqueVectorIds(entries);
  const parents = new Set(parentIds);
  if (entries.some(entry => !parents.has(entry.parentId))) throw new Error("replacement entry parentId must belong to the requested parents");
  // Empty graph cleanup is a no-op only after inspecting every storage copy.
  // Actual destructive replacements still require the transaction below.
  if (entries.length === 0) {
    const filter = parentIds.length === 1 ? { parent_id: parentIds[0] } : { parent_id: { $in: [...parentIds] } };
    let owned = await getFlatCollection(mongo, tier).findOne(filter, { projection: { _id: 1 } });
    if (!owned) {
      for (const partition of await listMongoVectorPartitions(mongo, tier)) {
        owned = await mongo.db.collection<MongoVectorDocument>(partition.collectionName).findOne(filter, { projection: { _id: 1 } });
        if (owned) break;
      }
    }
    if (!owned) return;
  }
  const now = new Date();
  const documents = entries.map((entry) => toMongoVectorDocument(entry, tier, now, vectorTtlSeconds(mongo, tier)));
  const groups = new Map<string, { model: string; dimensions: number; documents: MongoVectorDocument[] }>();
  for (const doc of documents) {
    const model = doc.embedding_model ?? "unknown-model";
    const dimensions = doc.embedding.length;
    const key = makePartitionKey(model, dimensions);
    const existing = groups.get(key) ?? { model, dimensions, documents: [] };
    existing.documents.push(doc);
    groups.set(key, existing);
  }

  const partitionCollections = new Map<string, Collection<MongoVectorDocument>>();
  for (const [key, group] of groups.entries()) {
    const { collection } = await ensureVectorPartition(mongo, tier, group.model, group.dimensions);
    partitionCollections.set(key, collection);
  }

  await withMongoTransaction(mongo, async (session) => {
    const filter = parentIds.length === 1 ? { parent_id: parentIds[0] } : { parent_id: { $in: [...parentIds] } };
    await getFlatCollection(mongo, tier).deleteMany(filter, session ? { session } : undefined);
    const partitions = await listMongoVectorPartitions(mongo, tier);
    await deletePartitionDocuments(mongo, partitions.map(partition => partition.collectionName), filter, session);
    await bulkWriteVectorDocuments(getFlatCollection(mongo, tier), documents, now, session);
    for (const [key, group] of groups.entries()) {
      const collection = partitionCollections.get(key);
      if (!collection) continue;
      await bulkWriteVectorDocuments(collection, group.documents, now, session);
    }
  });
}

export async function indexTextInMongoVectors(params: {
  mongo: MongoConnection;
  tier: MongoVectorTier;
  parentId: string;
  text: string;
  extra?: Record<string, unknown>;
  metadata: Record<string, unknown>;
  embeddingFunction: IEmbeddingFunction;
}): Promise<void> {
  validateVectorMetadata(params.metadata);
  let normalizedSource: string | undefined;
  const getNormalizedSource = () => normalizedSource ??=
    prepareIndexDocument({ parentId: "source-reference", text: params.text }).normalizedText;
  const tryIndex = async (forceChunking: boolean): Promise<void> => {
    const prepared = prepareIndexDocument({
      parentId: params.parentId,
      text: params.text,
      extra: params.extra,
      forceChunking,
    });

    const entries: MongoVectorEntry[] = [];
    let expectedDimensions: number | undefined;

    for (const batch of batchPreparedChunks(prepared.chunks)) {
      const texts = batch.map((chunk) => formatEmbeddingPassageText(chunk.text));
      const embeddings = await params.embeddingFunction.generate(texts);
      if (!Array.isArray(embeddings) || embeddings.length !== batch.length) {
        throw new Error(`embedding batch size mismatch: expected ${batch.length}, got ${Array.isArray(embeddings) ? embeddings.length : 0}`);
      }
      const dimensions = Array.isArray(embeddings[0]) ? embeddings[0].length : 0;
      if (dimensions <= 0) {
        throw new Error(`embedding function returned an empty vector batch for ${params.parentId}`);
      }
      expectedDimensions ??= dimensions;
      if (dimensions !== expectedDimensions) throw new Error("embedding dimensions changed across indexing batches");
      batch.forEach((chunk, index) => {
        const embedding = embeddings[index];
        if (!Array.isArray(embedding) || embedding.length !== dimensions || embedding.some((value) => typeof value !== "number" || !Number.isFinite(value))) {
          throw new Error(`invalid embedding vector for ${chunk.id} at batch index ${index}`);
        }
        entries.push({
          id: chunk.id,
          parentId: params.parentId,
          text: chunk.text,
          embedding,
          metadata: {
            ...params.metadata,
            ...sourceRedactionMetadata({
              extra: params.extra,
              rawText: params.text,
              normalizedSource: getNormalizedSource,
              chunkText: chunk.text,
              charStart: chunk.charStart,
              charEnd: chunk.charEnd,
            }),
            chunk_id: chunk.id,
            chunk_index: chunk.chunkIndex,
            chunk_count: chunk.chunkCount,
            normalized_format: prepared.normalizedFormat,
            normalized_estimated_tokens: prepared.normalizedEstimatedTokens,
            raw_estimated_tokens: prepared.rawEstimatedTokens,
          },
        });
      });
    }

    await replaceMongoVectorEntries(params.mongo, params.tier, params.parentId, entries);
  };

  try {
    await tryIndex(false);
  } catch (error) {
    if (!isContextOverflowError(error)) throw error;
    await tryIndex(true);
  }
}

function queryFilter(where: Record<string, unknown> | undefined, excludeParentIds: string[] = []): Filter<MongoVectorDocument> {
  const filter = buildMongoFilter(where);
  if (excludeParentIds.length > 0) {
    const current = filter.parent_id;
    filter.parent_id = {
      ...(typeof current === "object" && current !== null ? current : current !== undefined ? { $eq: current } : {}),
      $nin: excludeParentIds,
    };
  }
  return filter;
}

/** Rank the bounded candidate set by distinct parent, never by chunk slots. */
function uniqueParentRows(rows: Array<{ doc: MongoVectorDocument; score: number }>, k: number): Array<{ doc: MongoVectorDocument; score: number }> {
  const seen = new Set<string>();
  return [...rows]
    .sort((left, right) => right.score - left.score || left.doc._id.localeCompare(right.doc._id))
    .filter(entry => {
      const parent = entry.doc.parent_id ?? entry.doc._id;
      if (seen.has(parent)) return false;
      seen.add(parent);
      return true;
    })
    .slice(0, Math.max(1, k));
}

// A failed redacted-source read is not a successful vector match. Hydrate in
// rank order before parent admission/limit, allowing a lower accessible chunk.
async function hydratedParentRows(rows: Array<{ doc: MongoVectorDocument; score: number }>, k: number, onInvalidTimestamp: (id: string) => void): Promise<Array<{ doc: MongoVectorDocument; score: number }>> {
  const seen = new Set<string>();
  const admitted: Array<{ doc: MongoVectorDocument; score: number }> = [];
  for (let entry of [...rows].sort((left, right) => right.score - left.score || left.doc._id.localeCompare(right.doc._id))) {
    const doc = normalizeQueryTimestamp(entry.doc, onInvalidTimestamp);
    if (!doc) continue;
    entry = { ...entry, doc };
    const parent = entry.doc.parent_id ?? entry.doc._id;
    if (seen.has(parent)) continue;
    let text: string;
    try { text = await hydrateVectorDocumentText(entry.doc); } catch { continue; }
    if (!text) continue;
    seen.add(parent);
    admitted.push({ doc: { ...entry.doc, text }, score: entry.score });
    if (admitted.length >= Math.max(1, k)) break;
  }
  return admitted;
}

async function queryPartitionWithNativeVectorSearch(params: {
  onInvalidTimestamp: (id: string) => void;
  collection: Collection<MongoVectorDocument>;
  partition: MongoVectorPartitionDocument;
  queryEmbedding: number[];
  k: number;
  where?: Record<string, unknown>;
  excludeParentIds?: string[];
}): Promise<Array<{ doc: MongoVectorDocument; score: number }>> {
  const filter = queryFilter(params.where, params.excludeParentIds);
  const pipeline: Record<string, unknown>[] = [
    {
      $vectorSearch: {
        index: params.partition.searchIndexName,
        path: "embedding",
        queryVector: params.queryEmbedding,
        numCandidates: Math.max(params.k * 20, 100),
        limit: Math.max(params.k * 20, 100),
        ...(Object.keys(filter).length > 0 ? { filter } : {}),
      },
    },
    {
      $project: {
        _id: 1,
        parent_id: 1,
        text: 1,
        embedding: 1,
        ts: 1,
        source: 1,
        kind: 1,
        project: 1,
        session: 1,
        author: 1,
        role: 1,
        model: 1,
        visibility: 1,
        quality_label: 1,
        labels: 1,
        title: 1,
        embedding_model: 1,
        embedding_dimensions: 1,
        search_tier: 1,
        chunk_id: 1,
        chunk_index: 1,
        chunk_count: 1,
        normalized_format: 1,
        normalized_estimated_tokens: 1,
        raw_estimated_tokens: 1,
        seed_id: 1,
        member_count: 1,
        char_count: 1,
        source_text_redacted: 1,
        source_coordinate_space: 1,
        source_ref: 1,
        text_hash_sha256: 1,
        chunk_text_hash_sha256: 1,
        char_start: 1,
        char_end: 1,
        createdAt: 1,
        updatedAt: 1,
        score: { $meta: "vectorSearchScore" },
      },
    },
  ];

  const rows = await params.collection.aggregate(pipeline).toArray() as Array<MongoVectorDocument & { score?: number }>;
  const rescored = await queryPartitionWithVexxTopK({
    candidates: rows,
    queryEmbedding: params.queryEmbedding,
    k: rows.length,
  }).catch((error) => {
    if (VEXX_ENFORCE) throw error;
    return null;
  });
  if (!rescored && VEXX_ENFORCE) {
    throw vexxRequiredError(`native_vector_search:${params.partition.collectionName}`);
  }
  // Native Atlas metadata is normalized; recompute from its projected vectors
  // so native, Vexx and cosine fallback partitions share raw cosine distance.
  const scored = rescored ?? rows
    .map((doc) => ({ doc, score: cosineSimilarity(params.queryEmbedding, doc.embedding ?? []) }))
    .filter((entry) => Number.isFinite(entry.score));
  const admitted = await hydratedParentRows(scored, params.k, params.onInvalidTimestamp);
  // Exhaustion at the native window may hide usable rows. The caller's full
  // cosine scan supplies additional candidates rather than empty top hits.
  if (admitted.length < params.k && rows.length >= Math.max(params.k * 20, 100)) {
    throw new Error("native vector candidate window exhausted after hydration");
  }
  return admitted;
}

async function queryPartitionWithCosineScan(params: {
  onInvalidTimestamp: (id: string) => void;
  collection: Collection<MongoVectorDocument>;
  queryEmbedding: number[];
  k: number;
  where?: Record<string, unknown>;
  excludeParentIds?: string[];
}): Promise<Array<{ doc: MongoVectorDocument; score: number }>> {
  const filter = queryFilter(params.where, params.excludeParentIds);
  const limit = Math.max(1, params.k);
  type ScoredRow = { doc: MongoVectorDocument; score: number };
  const compare = (left: ScoredRow, right: ScoredRow) =>
    right.score - left.score || left.doc._id.localeCompare(right.doc._id);

  const scan = async (accelerated: boolean): Promise<ScoredRow[] | null> => {
    const retained = new Map<string, ScoredRow>();
    const admit = async (entry: ScoredRow) => {
      const doc = normalizeQueryTimestamp(entry.doc, params.onInvalidTimestamp);
      if (!doc) return;
      entry = { ...entry, doc };
      if (!Number.isFinite(entry.score)) return;
      const parent = entry.doc.parent_id ?? entry.doc._id;
      const current = retained.get(parent);
      if (current && compare(entry, current) >= 0) return;
      if (!current && retained.size >= limit) {
        const worst = [...retained.values()].sort(compare).at(-1)!;
        if (compare(entry, worst) >= 0) return;
      }
      let text: string;
      try { text = await hydrateVectorDocumentText(entry.doc); } catch { return; }
      if (!text) return;
      retained.set(parent, { doc: { ...entry.doc, text }, score: entry.score });
      if (retained.size > limit) {
        const worst = [...retained.values()].sort(compare).at(-1)!;
        retained.delete(worst.doc.parent_id ?? worst.doc._id);
      }
    };
    let batch: MongoVectorDocument[] = [];
    let minimumAlreadySatisfied = false;
    const consumeBatch = async (): Promise<boolean> => {
      const scored = await queryPartitionWithVexxTopK({
        candidates: batch, queryEmbedding: params.queryEmbedding, k: batch.length,
        minimumAlreadySatisfied,
      }).catch((error) => {
        if (VEXX_ENFORCE) throw error;
        return null;
      });
      if (!scored) {
        if (VEXX_ENFORCE) throw vexxRequiredError(`cosine_scan:${params.collection.collectionName}`);
        return false;
      }
      minimumAlreadySatisfied = true;
      for (const entry of scored) await admit(entry);
      batch = [];
      return true;
    };

    // Score the entire filtered partition without retaining it. The driver
    // buffer and accelerator payload are bounded; only k usable parents survive.
    // Returning/throwing from this loop closes its async Mongo cursor.
    for await (const doc of params.collection.find(filter).batchSize(VECTOR_SCAN_BATCH_SIZE)) {
      if (accelerated) {
        batch.push(doc);
        if (batch.length === VECTOR_SCAN_BATCH_SIZE && !await consumeBatch()) return null;
      } else {
        await admit({ doc, score: cosineSimilarity(params.queryEmbedding, doc.embedding ?? []) });
      }
    }
    if (batch.length > 0 && !await consumeBatch()) return null;
    return [...retained.values()].sort(compare);
  };

  if (VEXX_BASE_URL) {
    const accelerated = await scan(true);
    if (accelerated) return accelerated;
    // A later optional batch failure must discard all accelerator scores and
    // rescan with one local score scale, including candidates already visited.
  } else if (VEXX_ENFORCE) {
    throw vexxRequiredError(`cosine_scan:${params.collection.collectionName}`);
  }
  return (await scan(false))!;
}

export async function queryMongoVectorsByText(params: {
  mongo: MongoConnection;
  tier: MongoVectorTier;
  q: string;
  k: number;
  where?: Record<string, unknown>;
  excludeParentIds?: string[];
  getEmbeddingFunctionForModel: (model: string) => IEmbeddingFunction;
}): Promise<RawQueryResult> {
  if (!Number.isSafeInteger(params.k)) throw new Error("vector k must be a finite safe integer");
  const queryText = formatEmbeddingQueryText(params.q);
  const partitions = await listMongoVectorPartitions(params.mongo, params.tier);
  if (partitions.length === 0) {
    return emptyResult();
  }

  const rows: Array<{ doc: MongoVectorDocument; score: number }> = [];
  const queryEmbeddingsByPartitionKey = new Map<string, number[]>();
  const invalidTimestampRows = new Set<string>();
  const unavailablePartitions: string[] = [];
  let availablePartitions = 0;
  const unavailablePhases = new Set<"embedding" | "execution">();

  for (const storedPartition of partitions) {
    let phase: "embedding" | "execution" = "execution";
    try {
      const partition = await refreshPartitionReadiness(params.mongo, storedPartition);
      const collection = params.mongo.db.collection<MongoVectorDocument>(partition.collectionName);
      const cacheKey = `${partition.model}:${partition.dimensions}`;
      let queryEmbedding = queryEmbeddingsByPartitionKey.get(cacheKey);
      if (!queryEmbedding) {
        phase = "embedding";
        const embeddingFunction = params.getEmbeddingFunctionForModel(partition.model);
        const [generatedEmbedding] = await embeddingFunction.generate([queryText]);
        if (!Array.isArray(generatedEmbedding) || generatedEmbedding.length !== partition.dimensions
          || generatedEmbedding.length === 0 || generatedEmbedding.some(value => typeof value !== "number" || !Number.isFinite(value))) {
          throw new Error("invalid partition query embedding");
        }
        queryEmbedding = generatedEmbedding;
        queryEmbeddingsByPartitionKey.set(cacheKey, queryEmbedding);
      }
      phase = "execution";

      let partitionRows: Array<{ doc: MongoVectorDocument; score: number }>;
      if (partition.searchIndexStatus === "ready") {
        try {
          partitionRows = await queryPartitionWithNativeVectorSearch({
            collection, partition, queryEmbedding, k: params.k, where: params.where, excludeParentIds: params.excludeParentIds,
            onInvalidTimestamp: id => invalidTimestampRows.add(JSON.stringify([partition.collectionName, id])),
          });
        } catch {
          partitionRows = await queryPartitionWithCosineScan({
            collection, queryEmbedding, k: params.k, where: params.where, excludeParentIds: params.excludeParentIds,
            onInvalidTimestamp: id => invalidTimestampRows.add(JSON.stringify([partition.collectionName, id])),
          });
        }
      } else {
        partitionRows = await queryPartitionWithCosineScan({
          collection, queryEmbedding, k: params.k, where: params.where, excludeParentIds: params.excludeParentIds,
            onInvalidTimestamp: id => invalidTimestampRows.add(JSON.stringify([partition.collectionName, id])),
        });
      }

      rows.push(...partitionRows);
      availablePartitions++;
    } catch {
      // A corrupt or unreadable partition cannot discard healthy hits. Expose
      // storage identities and a fixed phase only, never private diagnostics.
      unavailablePartitions.push(storedPartition.collectionName);
      unavailablePhases.add(phase);
    }
  }

  if (availablePartitions === 0 && unavailablePartitions.length > 0) {
    throw new VectorQueryUnavailableError(unavailablePartitions,
      unavailablePhases.size > 1 ? "mixed" : unavailablePhases.has("execution") ? "execution" : "embedding");
  }

  const sorted = uniqueParentRows(rows, params.k);
  const documents = await Promise.all(sorted.map((entry) => hydrateVectorDocumentText(entry.doc)));

  return {
    ids: [sorted.map((entry) => entry.doc._id)],
    documents: [documents],
    metadatas: [sorted.map((entry) => toMetadata(entry.doc))],
    distances: [sorted.map((entry) => Number.isFinite(entry.score) ? 1 - entry.score : null)],
    include: ["documents", "metadatas", "distances"],
    ...(unavailablePartitions.length > 0 ? { partial: true, unavailable_partitions: [...new Set(unavailablePartitions)] } : {}),
    ...(invalidTimestampRows.size > 0 ? { partial: true, discarded_invalid_timestamp_count: invalidTimestampRows.size } : {}),
    queried_partition_count: availablePartitions,
  };
}

export function buildMongoVectorDeleteFilter(where: Record<string, unknown>): Filter<MongoVectorDocument> {
  return buildMongoFilter(where);
}

// ============================================================================
// GPU-Saturating Batch Indexing
// ============================================================================

/**
 * Configuration for parallel batch indexing
 */
export type BatchIndexConfig = {
  /** Number of Mongo replacement batches to process in parallel (default: 16) */
  concurrency?: number;
  /** Number of chunks to embed in a single batch (default: 256) */
  embeddingBatchSize?: number;
  /** Number of documents to write to MongoDB in a single batch (default: 100) */
  mongoBatchSize?: number;
  /** Callback for progress reporting */
  onProgress?: (phase: string, completed: number, total: number) => void;
};

/**
 * Optimized batch indexer for GPU saturation during backfill.
 *
 * Key optimizations:
 * 1. Pipelines embedding generation with MongoDB writes
 * 2. Uses larger batch sizes for GPU efficiency
 * 3. Processes multiple documents concurrently
 * 4. Fire-and-forget caching to avoid blocking
 */
export async function batchIndexTextsInMongoVectors(params: {
  mongo: MongoConnection;
  tier: MongoVectorTier;
  items: Array<{
    id: string;
    text: string;
    extra?: Record<string, unknown>;
    metadata: Record<string, unknown>;
  }>;
  embeddingFunction: IEmbeddingFunction;
  config?: BatchIndexConfig;
}): Promise<{ indexed: number; failed: Array<{ id: string; error: string }> }> {
  const { items, embeddingFunction, config = {} } = params;
  const concurrency = config.concurrency ?? 16;
  const embeddingBatchSize = config.embeddingBatchSize ?? 256;
  const mongoBatchSize = config.mongoBatchSize ?? 100;
  for (const [name, value] of Object.entries({ concurrency, embeddingBatchSize, mongoBatchSize })) {
    if (!Number.isSafeInteger(value) || value <= 0) throw new Error(`${name} must be a positive integer`);
  }

  const indexed: string[] = [];
  const failed: Array<{ id: string; error: string }> = [];

  // Duplicate parents would reuse chunk identities and pair another text's vector.
  const seenParentIds = new Set<string>();
  for (const item of items) {
    validateVectorMetadata(item.metadata);
    if (seenParentIds.has(item.id)) throw new Error(`Duplicate vector parent ID: ${item.id}`);
    seenParentIds.add(item.id);
  }

  // Prepare all documents first
  const preparedItems: Array<{
    id: string;
    prepared: ReturnType<typeof prepareIndexDocument>;
    rawText: string;
    extra?: Record<string, unknown>;
    metadata: Record<string, unknown>;
  }> = [];

  for (const item of items) {
    try {
      const prepared = prepareIndexDocument({
        parentId: item.id,
        text: item.text,
        extra: item.extra,
      });
      preparedItems.push({ id: item.id, prepared, rawText: item.text, extra: item.extra, metadata: item.metadata });
    } catch (error) {
      failed.push({ id: item.id, error: error instanceof Error ? error.message : String(error) });
    }
  }

  // Collect all chunks for batch embedding
  const allChunks: Array<{ chunkId: string; parentId: string; text: string; metadata: Record<string, unknown> }> = [];
  for (const item of preparedItems) {
    let normalizedSource: string | undefined;
    const getNormalizedSource = () => normalizedSource ??=
      prepareIndexDocument({ parentId: "source-reference", text: item.rawText }).normalizedText;
    for (const chunk of item.prepared.chunks) {
      allChunks.push({
        chunkId: chunk.id,
        parentId: item.id,
        text: chunk.text,
        metadata: {
          ...item.metadata,
          ...sourceRedactionMetadata({
            extra: item.extra,
            rawText: item.rawText,
            normalizedSource: getNormalizedSource,
            chunkText: chunk.text,
            charStart: chunk.charStart,
            charEnd: chunk.charEnd,
          }),
          chunk_id: chunk.id,
          chunk_index: chunk.chunkIndex,
          chunk_count: chunk.chunkCount,
          normalized_format: item.prepared.normalizedFormat,
          normalized_estimated_tokens: item.prepared.normalizedEstimatedTokens,
          raw_estimated_tokens: item.prepared.rawEstimatedTokens,
        },
      });
    }
  }

  config.onProgress?.("embedding", 0, allChunks.length);

  // Generate embeddings in large batches for GPU saturation
  const embeddings = new Map<string, number[]>();
  const embeddingErrors = new Map<string, string>();
  let expectedDimensions: number | undefined;

  for (let offset = 0; offset < allChunks.length; offset += embeddingBatchSize) {
    const batch = allChunks.slice(offset, offset + embeddingBatchSize);
    const texts = batch.map((c) => formatEmbeddingPassageText(c.text));

    try {
      const vectors = await embeddingFunction.generate(texts);
      if (!Array.isArray(vectors) || vectors.length !== batch.length) {
        throw new Error(`Embedding batch size mismatch: expected ${batch.length}, got ${Array.isArray(vectors) ? vectors.length : 0}`);
      }
      for (const vec of vectors) {
        if (!Array.isArray(vec) || !vec.length || vec.some(value => typeof value !== "number" || !Number.isFinite(value))) {
          throw new Error("Invalid embedding vector");
        }
        expectedDimensions ??= vec.length;
        if (vec.length !== expectedDimensions) throw new Error("embedding dimensions changed across indexing batches");
      }
      batch.forEach((chunk, index) => {
        const vec = vectors[index];
        if (Array.isArray(vec) && vec.length > 0 && vec.every((v) => typeof v === "number" && Number.isFinite(v))) {
          embeddings.set(chunk.chunkId, vec);
        } else {
          embeddingErrors.set(chunk.chunkId, "Invalid embedding vector");
        }
      });
    } catch (error) {
      const errorMsg = error instanceof Error ? error.message : String(error);
      batch.forEach((chunk) => embeddingErrors.set(chunk.chunkId, errorMsg));
    }

    config.onProgress?.("embedding", Math.min(offset + embeddingBatchSize, allChunks.length), allChunks.length);
  }

  const failedParents = new Set<string>();
  for (const chunk of allChunks) {
    const error = embeddingErrors.get(chunk.chunkId);
    if (error && !failedParents.has(chunk.parentId)) {
      failedParents.add(chunk.parentId);
      failed.push({ id: chunk.parentId, error });
    }
  }

  // Group by parent for MongoDB upsert
  const entriesByParent = new Map<string, MongoVectorEntry[]>();
  for (const chunk of allChunks) {
    const embedding = embeddings.get(chunk.chunkId);
    if (failedParents.has(chunk.parentId) || !embedding) continue;

    const existing = entriesByParent.get(chunk.parentId) ?? [];
    existing.push({
      id: chunk.chunkId,
      parentId: chunk.parentId,
      text: chunk.text,
      embedding,
      metadata: chunk.metadata,
    });
    entriesByParent.set(chunk.parentId, existing);
  }

  config.onProgress?.("indexing", 0, entriesByParent.size);

  // Each transaction replaces an entire bounded parent batch, including stale copies.
  const parentIds = Array.from(entriesByParent.keys());
  const queue: string[][] = [];
  for (let offset = 0; offset < parentIds.length; offset += mongoBatchSize) {
    queue.push(parentIds.slice(offset, offset + mongoBatchSize));
  }
  let completedCount = 0;
  const workers = Array.from({ length: Math.min(concurrency, queue.length) }, async () => {
    while (queue.length > 0) {
      const batch = queue.shift()!;
      try {
        await replaceMongoVectorParents(params.mongo, params.tier, batch, batch.flatMap(id => entriesByParent.get(id)!));
        indexed.push(...batch);
      } catch (error) {
        const detail = error instanceof Error ? error.message : String(error);
        failed.push(...batch.map(id => ({ id, error: detail })));
      }
      completedCount += batch.length;
      config.onProgress?.("indexing", completedCount, parentIds.length);
    }
  });

  await Promise.all(workers);

  config.onProgress?.("complete", indexed.length, items.length);

  return { indexed: indexed.length, failed };
}
