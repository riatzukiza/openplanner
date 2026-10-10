/**
 * Event ingest pipeline, extracted from the REST API's POST /v1/events handler
 * so it can run in-process anywhere the SDK is embedded (the API server is one
 * consumer; knoxx's direct-mongo client is another). Metrics and Kafka
 * publishing are intentionally NOT here — they are API-server concerns layered
 * on by the route.
 */
import { createHash } from "node:crypto";
import { createProtocols } from "./protocol-adapters.js";
import { upsertEvent, upsertGraphEdges, upsertGraphNodeEmbeddings } from "./mongodb.js";
import type { MongoConnection } from "./mongodb.js";
import { prepareIndexDocument } from "./indexing.js";
import { indexTextInMongoVectors, replaceMongoVectorEntries } from "./mongo-vectors.js";
import type { EventEnvelopeV1 } from "./types.js";
import { splitSentences, deduplicateByHash, computeTextHash } from "./sentence-split.js";
import { formatEmbeddingPassageText } from "./embedding-text.js";
import { eventMigrationState, OPENPLANNER_SCHEMA_TARGETS } from "./schema-versions.js";
import type { EmbeddingRuntime } from "./embedding-runtime.js";

export interface IngestLogger {
  warn(obj: Record<string, unknown>, msg: string): void;
}

export interface IngestContext {
  mongo: MongoConnection;
  embeddingRuntime: EmbeddingRuntime;
  log?: IngestLogger;
}

export interface IngestResult {
  ok: true;
  count: number;
  ids: string[];
  projectedGraphEdges: number;
  ftsEnabled: true;
  storageBackend: "mongodb";
  indexed: true;
  indexing: "queued" | "skipped";
  queuedEventVectors: number;
  queuedGraphNodeEmbeddings: number;
  /** Events that passed validation, for downstream fan-out (e.g. Kafka). */
  acceptedEvents: EventEnvelopeV1[];
  /**
   * Settles when detached vector/embedding work finishes. Callers that need
   * read-your-writes (tests, batch jobs) can await it; the API route does not.
   */
  backgroundIndexing: Promise<void>;
}

function norm(v: any): string | null {
  if (v === undefined || v === null) return null;
  return String(v);
}

export function validateEvent(ev: EventEnvelopeV1) {
  if (!ev || ev.schema !== "openplanner.event.v1") throw new Error("event.schema must be openplanner.event.v1");
  if (!ev.id) throw new Error("event.id required");
  if (typeof ev.ts !== "string" || !ev.ts.trim() || !Number.isFinite(Date.parse(ev.ts))) throw new Error("event.ts must be a valid timestamp (ISO)");
  if (!ev.source) throw new Error("event.source required");
  if (!ev.kind) throw new Error("event.kind required");
  if (ev.kind === "graph.node" && ev.text !== undefined && typeof ev.text !== "string") {
    throw new Error("graph.node event text must be a string when supplied");
  }
}

function hasIndexableEventText(ev: EventEnvelopeV1): boolean {
  return typeof ev.text === "string" && ev.text.trim().length > 0;
}

function labelSlug(value: string): string {
  return value.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "").slice(0, 96) || "label";
}

function graphLabelId(tenantId: string, label: string): string {
  return `label:${tenantId}:${labelSlug(label)}:${createHash("sha256").update(label, "utf8").digest("hex")}`;
}

function eventLabels(extra: Record<string, unknown>): string[] {
  const labels = (extra.openplanner_labels as any)?.labels;
  if (!Array.isArray(labels)) return [];
  return [...new Set(labels.map((label) => String(label ?? "").trim()).filter(Boolean))];
}

export function shouldIndexEventHotVectors(ev: EventEnvelopeV1): boolean {
  if (!hasIndexableEventText(ev)) return false;

  // graph.node receives dedicated node-embedding materialization below, and
  // graph.edge text is mostly structural glue (e.g. mentions_web URLs). Running
  // both through the generic hot vector path just burns response time and can
  // hold /v1/events open long enough for upstream header timeouts.
  if (ev.kind === "graph.node" || ev.kind === "graph.edge") return false;

  return true;
}

async function withTimeout<T>(promise: Promise<T>, timeoutMs: number, label: string): Promise<T> {
  let timer: NodeJS.Timeout | null = null;
  let timedOut = false;
  try {
    return await Promise.race([
      promise,
      new Promise<T>((_resolve, reject) => {
        timer = setTimeout(() => { timedOut = true; reject(new Error(`${label} timed out after ${timeoutMs}ms`)); }, timeoutMs);
      }),
    ]);
  } catch (error) {
    if (timedOut) {
      // Provider cancellation is not available on every embedding implementation.
      // Retain ownership until the underlying operation settles; expiry is not termination.
      try { await promise; } catch { /* Preserve the original timeout outcome. */ }
    }
    throw error;
  } finally {
    if (timer) clearTimeout(timer);
  }
}

const noopLogger: IngestLogger = { warn: () => {} };

// SDK connection-local ownership. A replacement waits for earlier detached
// writes to terminate; this is not a distributed writer or fencing protocol.
const ingestTails = new WeakMap<MongoConnection, Map<string, Promise<void>>>();

export async function ingestEvents(ctx: IngestContext, events: EventEnvelopeV1[]): Promise<IngestResult> {
  const unique = new Map<string, EventEnvelopeV1>();
  const payloads = new Map<string, string>();
  for (const event of events) {
    validateEvent(event);
    const payload = JSON.stringify(event);
    const previous = payloads.get(event.id);
    if (previous !== undefined && previous !== payload) {
      throw new Error(`conflicting event replacements in one batch: ${event.id}`);
    }
    payloads.set(event.id, payload);
    unique.set(event.id, event);
  }
  const tails = ingestTails.get(ctx.mongo) ?? new Map<string, Promise<void>>();
  ingestTails.set(ctx.mongo, tails);
  const predecessors = [...unique.keys()].map(id => tails.get(id)).filter((tail): tail is Promise<void> => tail !== undefined);
  let complete!: () => void;
  const terminal = new Promise<void>(resolve => { complete = resolve; });
  // Reserve the complete batch without yielding, avoiding overlapping-batch
  // lock ordering. Unrelated parents remain independent.
  for (const id of unique.keys()) tails.set(id, terminal);
  const release = () => {
    for (const id of unique.keys()) if (tails.get(id) === terminal) tails.delete(id);
    complete();
  };
  try {
    await Promise.all(predecessors);
    const result = await ingestReservedEvents(ctx, [...unique.values()]);
    void result.backgroundIndexing.then(release, release);
    return result;
  } catch (error) {
    release();
    throw error;
  }
}

async function ingestReservedEvents(ctx: IngestContext, events: EventEnvelopeV1[]): Promise<IngestResult> {
  const { mongo, embeddingRuntime } = ctx;
  const log = ctx.log ?? noopLogger;
  const protocols = createProtocols({ mongo });

  // Refuse the whole batch before starting event, projection or vector writes.
  for (const ev of events) validateEvent(ev);
  const batchPayloads = new Map<string, string>();
  for (const ev of events) {
    const payload = JSON.stringify(ev);
    const previous = batchPayloads.get(ev.id);
    if (previous !== undefined && previous !== payload) {
      throw new Error(`conflicting event replacements in one batch: ${ev.id}`);
    }
    batchPayloads.set(ev.id, payload);
  }
  // Old endpoint-keyed edges can lack causal ownership. Refuse replacement
  // rather than mutate the event and retain stale or delete another owner.
  for (const ev of events) {
    const previous = await mongo.events.findOne({ _id: ev.id });
    if (previous?.kind === "graph.edge") {
      const old = (previous.extra as Record<string, unknown> | undefined) ?? {};
      const source = norm(old.source_node_id)?.trim();
      const target = norm(old.target_node_id)?.trim();
      const kind = (norm(old.edge_type) ?? norm(old.edge_kind))?.trim();
      if (source && target && kind) {
        const legacy = await mongo.graphEdges.findOne({ _id: `${source}||${target}||${kind}` });
        if (legacy && legacy.data?.source_event_id !== ev.id) {
          throw new Error(`graph projection ownership unavailable for event ${ev.id}`);
        }
      }
    }
  }

  const ids: string[] = [];
  const acceptedEvents: EventEnvelopeV1[] = [];
  const eventVectorTasks: Array<() => Promise<void>> = [];
  const projectedGraphEdges: Array<{
    source_node_id: string;
    target_node_id: string;
    edge_kind: string;
    layer?: string | null;
    project?: string | null;
    source?: string | null;
    data?: Record<string, unknown> | null;
    updated_at?: Date;
  }> = [];
  const graphNodeEmbeddingInputs = new Map<string, {
    node_id: string;
    source_event_id: string;
    project?: string | null;
    text: string;
    source_text_hash_sha256: string;
    expiresAt: Date | null;
    chunk_count: number;
  }>();
  const sourceExpiries = new Map<string, Date | null>();

  const derivedGraphNodeOps = new Map<string, any>();
  const graphLabelNodeOps: any[] = [];
  const queuedGraphLabelIds = new Set<string>();
  const now = new Date();

  const queueDerivedGraphNodeEvent = (params: {
    id: string;
    ts: Date;
    project?: string | null;
    nodeId: string;
    nodeKind: string;
    label: string;
    preview: string;
    retentionLabels: string[];
    sourceEventId: string;
    extra?: Record<string, unknown>;
  }): void => {
    derivedGraphNodeOps.set(params.id, {
      updateOne: {
        filter: { _id: params.id },
        update: {
          $set: {
            id: params.id,
            ts: params.ts,
            source: "openplanner-derive",
            kind: "graph.node",
            project: params.project ?? null,
            session: null,
            message: params.label,
            role: null,
            author: null,
            model: null,
            tags: null,
            text: "",
            attachments: null,
            extra: {
              ...(params.extra ?? {}),
              node_id: params.nodeId,
              node_kind: params.nodeKind,
              label: params.label,
              preview: params.preview,
              content_hash: computeTextHash(params.preview),
              lake: params.project ?? undefined,
              source_event_id: params.sourceEventId,
              openplanner_labels: {
                ...((params.extra?.openplanner_labels as Record<string, unknown> | undefined) ?? {}),
                labels: params.retentionLabels,
              },
            },
            schema_version: OPENPLANNER_SCHEMA_TARGETS.event,
            migration_state: eventMigrationState(now),
            updatedAt: now,
          },
          $setOnInsert: {
            createdAt: now,
          },
        },
        upsert: true,
      },
    });
  };

  const queueNodeEmbedding = (params: {
    nodeId: string;
    sourceEventId: string;
    project?: string | null;
    text: string;
    chunkCount?: number;
    sourceText?: string;
  }): void => {
    const normalized = formatEmbeddingPassageText(params.text);
    if (!normalized) return;
    graphNodeEmbeddingInputs.set(params.nodeId, {
      node_id: params.nodeId,
      source_event_id: params.sourceEventId,
      project: params.project ?? null,
      text: normalized,
      source_text_hash_sha256: createHash("sha256").update(params.sourceText ?? params.text, "utf8").digest("hex"),
      expiresAt: sourceExpiries.get(params.sourceEventId) ?? null,
      chunk_count: params.chunkCount ?? 1,
    });
  };

  for (const ev of events) {
    const projectionStart = projectedGraphEdges.length;
    acceptedEvents.push(ev);

    const sr = ev.source_ref ?? {};
    const meta = ev.meta ?? {};
    const extra = (ev.extra as Record<string, unknown> | undefined) ?? {};
    const role = norm((meta as any).role);
    const author = norm((meta as any).author);
    const model = norm((meta as any).model);
    const tags = (meta as any).tags;
    const project = norm((sr as any).project);

    // Protocol-based event admission
    await protocols.eventAdmission.appendEvent({
      id: ev.id,
      ts: new Date(ev.ts),
      source: ev.source,
      kind: ev.kind,
      project,
      session: norm((sr as any).session),
      message: norm((sr as any).message),
      role,
      author,
      model,
      tags: tags ?? null,
      text: norm(ev.text ?? ""),
      attachments: ev.attachments ?? null,
      extra: ev.extra ?? null,
      schema_version: ev.schema_version,
      migration_state: ev.migration_state as any,
    });

    ids.push(ev.id);
    const admittedSource = await mongo.events.findOne({ _id: ev.id });
    sourceExpiries.set(ev.id, admittedSource?.expiresAt instanceof Date ? admittedSource.expiresAt : null);

    const labels = eventLabels(extra);
    for (const label of labels) {
      const tenantId = String((extra.openplanner_labels as any)?.tenant_id ?? (extra as any).tenant_id ?? "default").trim() || "default";
      const labelId = graphLabelId(tenantId, label);
      if (!queuedGraphLabelIds.has(labelId)) {
        queuedGraphLabelIds.add(labelId);
        graphLabelNodeOps.push({
          updateOne: {
            filter: { label_id: labelId },
            update: {
              $set: {
                _id: labelId,
                label_id: labelId,
                label,
                emoji: null,
                description: `Auto-derived event label: ${label}`,
                color: null,
                tenant_id: tenantId,
                project,
                embedding_model: null,
                embedding_dimensions: 0,
                embedding: null,
                created_by: "event-ingest",
                updatedAt: now,
              },
              $setOnInsert: { createdAt: now },
            },
            upsert: true,
          },
        });
      }

      projectedGraphEdges.push({
        source_node_id: ev.id,
        target_node_id: labelId,
        edge_kind: "has_label",
        layer: null,
        project,
        source: ev.source,
        data: {
          applied_at: new Date(ev.ts).toISOString(),
          confidence: 1,
          label,
          claim_system: (extra.openplanner_labels as any)?.claim_system ?? null,
          source_event_id: ev.id,
        },
        updated_at: new Date(ev.ts),
      });
    }

    if (ev.kind === "graph.edge") {
      const sourceNodeId = norm(extra.source_node_id)?.trim() ?? "";
      const targetNodeId = norm(extra.target_node_id)?.trim() ?? "";
      const edgeKind = (norm(extra.edge_type) ?? norm(extra.edge_kind) ?? "").trim();
      if (sourceNodeId && targetNodeId && edgeKind && sourceNodeId !== targetNodeId) {
        projectedGraphEdges.push({
          source_node_id: sourceNodeId,
          target_node_id: targetNodeId,
          edge_kind: edgeKind,
          layer: norm(extra.layer),
          project,
          source: ev.source,
          data: extra,
          updated_at: new Date(ev.ts),
        });
      }
    }

    if (ev.kind === "graph.node") {
      const nodeId = norm(extra.node_id)?.trim() ?? norm((sr as any).message)?.trim() ?? "";
      const preview = norm(extra.preview)?.trim() ?? "";
      const directText = norm(ev.text)?.trim() ?? "";
      const body = directText || preview;
      if (nodeId && body) {
        const label = String(extra.label ?? extra.path ?? (sr as any).message ?? nodeId).trim() || nodeId;
        const prepared = prepareIndexDocument({
          parentId: nodeId,
          text: body,
          extra,
          forceChunking: false,
          targetChunkTokens: 32_000,
          targetChunkChars: 180_000,
          overlapChars: 1_000,
        });

        if (prepared.chunkCount <= 1) {
          queueNodeEmbedding({
            nodeId,
            sourceEventId: ev.id,
            project,
            text: prepared.normalizedText,
            sourceText: ev.text ?? "",
            chunkCount: 1,
          });
        } else {
          for (const chunk of prepared.chunks) {
            const chunkLabel = `${label} [chunk ${chunk.chunkIndex + 1}/${chunk.chunkCount}]`;
            const chunkPreview = chunk.text.slice(0, 800);
            const chunkEventId = `graph.node:doc_chunk:${chunk.id}`;

            queueDerivedGraphNodeEvent({
              id: chunkEventId,
              ts: new Date(ev.ts),
              project,
              nodeId: chunk.id,
              nodeKind: "doc_chunk",
              label: chunkLabel,
              preview: chunkPreview,
              retentionLabels: labels,
              sourceEventId: ev.id,
              extra: {
                parent_node_id: nodeId,
                chunk_index: chunk.chunkIndex,
                chunk_count: chunk.chunkCount,
              },
            });

            projectedGraphEdges.push({
              source_node_id: nodeId,
              target_node_id: chunk.id,
              edge_kind: "contains_chunk",
              layer: "derived",
              project,
              source: "openplanner-derive",
              data: {
                parent_node_id: nodeId,
                chunk_index: chunk.chunkIndex,
                chunk_count: chunk.chunkCount,
              },
              updated_at: new Date(ev.ts),
            });

            queueNodeEmbedding({
              nodeId: chunk.id,
              sourceEventId: ev.id,
              project,
              text: chunk.text,
              sourceText: ev.text ?? "",
              chunkCount: chunk.chunkCount,
            });
          }
        }

        const sentenceHashesInDoc = new Set<string>();
        const sentenceNodeIdsQueued = new Set<string>();

        const sentenceSources = prepared.chunkCount <= 1
          ? [{ text: prepared.normalizedText }]
          : prepared.chunks.map((chunk) => ({ text: chunk.text }));

        for (const sourceChunk of sentenceSources) {
          const sentences = splitSentences(sourceChunk.text);
          const uniqueSentences = deduplicateByHash(sentences);

          for (const [hash, sent] of uniqueSentences) {
            if (sent.tokens <= 3) continue;
            if (sentenceHashesInDoc.has(hash)) continue;
            sentenceHashesInDoc.add(hash);

            const sentenceNodeId = `sentence:${createHash("sha256").update(JSON.stringify([project ?? null, ev.id, hash]), "utf8").digest("hex")}`;
            const sentenceEventId = `graph.node:sentence:${sentenceNodeId}`;

            if (!sentenceNodeIdsQueued.has(sentenceNodeId)) {
              sentenceNodeIdsQueued.add(sentenceNodeId);
              queueDerivedGraphNodeEvent({
                id: sentenceEventId,
                ts: new Date(ev.ts),
                project,
                nodeId: sentenceNodeId,
                nodeKind: "sentence",
                label: sent.sentence.length > 120 ? sent.sentence.slice(0, 117) + "..." : sent.sentence,
                preview: sent.sentence,
                retentionLabels: labels,
                sourceEventId: ev.id,
                extra: {
                  derived_from_node_id: nodeId,
                },
              });
            }

            projectedGraphEdges.push({
              source_node_id: nodeId,
              target_node_id: sentenceNodeId,
              edge_kind: "contains_sentence",
              layer: "derived",
              project,
              source: "openplanner-derive",
              data: {
                sentence_hash: hash,
              },
              updated_at: new Date(ev.ts),
            });

            queueNodeEmbedding({
              nodeId: sentenceNodeId,
              sourceEventId: ev.id,
              project,
              text: sent.sentence,
              sourceText: ev.text ?? "",
              chunkCount: 1,
            });
          }
        }
      }
    }

    // Auto-materialize arbitrary event kinds as graph nodes so the graph
    // export and graph-weaver can see them without requiring every producer
    // to emit `kind: "graph.node"`. We preserve the original kind in
    // `extra.node_kind` and use the event id as the node id.
    if (ev.kind !== "graph.node" && ev.kind !== "graph.edge" && hasIndexableEventText(ev)) {
      const nodeId = ev.id;
      const preview = norm(ev.text)?.trim() ?? "";
      const derivedEventId = `graph.node:derive:${ev.id}`;
      const label = String(
        extra.label ?? (sr as any).message ?? (preview.length > 80 ? `${preview.slice(0, 77)}...` : preview) ?? nodeId,
      ).trim() || nodeId;

      queueDerivedGraphNodeEvent({
        id: derivedEventId,
        ts: new Date(ev.ts),
        project,
        nodeId,
        nodeKind: ev.kind,
        label,
        preview,
        retentionLabels: labels,
        sourceEventId: ev.id,
        extra: {
          lake: project ?? undefined,
          entity_key: ev.id,
          source_event_id: ev.id,
          source_kind: ev.kind,
          ...extra,
        },
      });

      queueNodeEmbedding({
        nodeId,
        sourceEventId: ev.id,
        project,
        text: preview,
        sourceText: ev.text!,
        chunkCount: 1,
      });
    }

    for (let index = projectionStart; index < projectedGraphEdges.length; index++) {
      const edge = projectedGraphEdges[index]!;
      edge.data = { ...edge.data, source_event_id: ev.id };
    }

    if (shouldIndexEventHotVectors(ev)) {
      eventVectorTasks.push(async () => {
        try {
          const embeddingScope = {
            source: ev.source,
            kind: ev.kind,
            project: project ?? undefined,
          };

          const embeddingFunction = embeddingRuntime.hot.getBackgroundEmbeddingFunction(embeddingScope);
          const embeddingModel = embeddingRuntime.hot.getModel(embeddingScope);
          await withTimeout(indexTextInMongoVectors({
            mongo,
            tier: "hot",
            parentId: ev.id,
            text: ev.text!,
            extra,
            metadata: {
              ts: ev.ts,
              source: ev.source,
              kind: ev.kind,
              project: (sr as any).project,
              session: (sr as any).session,
              author: author ?? "",
              role: role ?? "",
              model: model ?? "",
              embedding_model: embeddingModel ?? "",
              search_tier: "hot",
              visibility: extra.visibility ?? "internal",
              quality_label: ((extra.openplanner_labels as any)?.quality ?? ""),
              labels,
              title: extra.title ?? (sr as any).message ?? ev.id,
            },
            embeddingFunction,
          }), 30_000, `event vector index ${ev.id}`);
        } catch (err) {
          log.warn({ err, eventId: ev.id }, "Failed to index event into MongoDB vectors; preserving base event without embeddings");
        }
      });
    } else {
      // An empty or structural replacement still owns deletion of its old
      // hot vectors, including retention-exempt rows and model partitions.
      await replaceMongoVectorEntries(mongo, "hot", ev.id, []);
    }
  }

  if (derivedGraphNodeOps.size > 0) {
    for (const operation of derivedGraphNodeOps.values()) {
      await upsertEvent(mongo.events, operation.updateOne.update.$set, mongo.retention?.eventsTtlSeconds);
    }
  }

  // Reconcile only explicitly owned derived rows. Legacy rows without causal
  // ownership are retained for an independently qualified migration.
  for (const sourceEventId of new Set(ids)) {
    const derivedIds = [...derivedGraphNodeOps.values()]
      .map((operation) => operation.updateOne.update.$set)
      .filter((row) => row.extra.source_event_id === sourceEventId)
      .map((row) => row.id);
    const inputs = [...graphNodeEmbeddingInputs.values()].filter((row) => row.source_event_id === sourceEventId);
    await mongo.events.deleteMany({ source: "openplanner-derive", "extra.source_event_id": sourceEventId, id: { $nin: derivedIds } });
    await mongo.graphNodeEmbeddings.deleteMany({ source_event_id: sourceEventId, node_id: { $nin: inputs.map((row) => row.node_id) } });
    for (const input of inputs) {
      await mongo.graphNodeEmbeddings.deleteMany({
        source_event_id: sourceEventId,
        node_id: input.node_id,
        source_text_hash_sha256: { $ne: input.source_text_hash_sha256 },
      });
    }
  }

  if (graphLabelNodeOps.length > 0) {
    const batchSize = 1000;
    for (let i = 0; i < graphLabelNodeOps.length; i += batchSize) {
      await mongo.graphLabelNodes.bulkWrite(graphLabelNodeOps.slice(i, i + batchSize), { ordered: false });
    }
  }

  // Replacing an admitted source removes its old owned projection, including
  // when the replacement produces no edges. Unrelated owners remain intact.
  if (ids.length > 0) await mongo.graphEdges.deleteMany({ "data.source_event_id": { $in: ids } });
  if (projectedGraphEdges.length > 0) {
    await upsertGraphEdges(mongo.graphEdges, projectedGraphEdges);
  }

  const graphNodeEmbeddingTask = graphNodeEmbeddingInputs.size > 0
    ? (async () => {
        try {
          type GraphNodeEmbeddingInput = {
            node_id: string;
            source_event_id: string;
            project?: string | null;
            text: string;
            source_text_hash_sha256: string;
            expiresAt: Date | null;
            chunk_count: number;
          };
          const groupedByModel = new Map<string, GraphNodeEmbeddingInput[]>();

          for (const input of graphNodeEmbeddingInputs.values()) {
            const model = embeddingRuntime.hot.getModel({
              source: "graph-event",
              kind: "graph.node",
              project: input.project ?? undefined,
            });
            const rows = groupedByModel.get(model) ?? [];
            rows.push(input);
            groupedByModel.set(model, rows);
          }

          for (const [model, rows] of groupedByModel) {
            const embeddingFunction = embeddingRuntime.hot.getBackgroundEmbeddingFunctionForModel(model);
            const nodeIds = rows.map((row) => row.node_id);
            const existing = await mongo.graphNodeEmbeddings
              .find({ node_id: { $in: nodeIds }, embedding_model: model })
              .project({ node_id: 1, text: 1, source_text_hash_sha256: 1, source_event_id: 1, project: 1, chunk_count: 1, expiresAt: 1 })
              .toArray();
            const existingById = new Map(existing.map((row: any) => [String(row.node_id), row] as const));

            const toEmbed = rows.filter((row) => {
              const previous = existingById.get(row.node_id);
              return !previous || previous.text !== row.text || previous.source_text_hash_sha256 !== row.source_text_hash_sha256;
            });

            for (const row of rows) {
              const previous = existingById.get(row.node_id);
              if (previous && previous.text === row.text && previous.source_text_hash_sha256 === row.source_text_hash_sha256
                  && (previous.source_event_id !== row.source_event_id || (previous.project ?? null) !== (row.project ?? null)
                      || previous.chunk_count !== row.chunk_count
                      || (previous.expiresAt?.getTime() ?? null) !== (row.expiresAt?.getTime() ?? null))) {
                await mongo.graphNodeEmbeddings.updateMany({
                  node_id: row.node_id, embedding_model: model, text: row.text,
                  source_text_hash_sha256: row.source_text_hash_sha256,
                  source_event_id: previous.source_event_id, project: previous.project ?? null,
                }, { $set: { source_event_id: row.source_event_id, project: row.project ?? null,
                  chunk_count: row.chunk_count, expiresAt: row.expiresAt, updated_at: new Date(), updatedAt: new Date() } });
              }
            }

            if (toEmbed.length === 0) continue;

            const embeddings = await withTimeout(
              embeddingFunction.generate(toEmbed.map((row) => row.text)) as Promise<number[][]>,
              30_000,
              `graph node embedding batch ${model}`,
            );

            const storedRows = toEmbed.flatMap((row, idx) => {
              const embedding = embeddings[idx];
              if (!Array.isArray(embedding) || embedding.length === 0) return [];
              return [{
                node_id: row.node_id,
                source_event_id: row.source_event_id,
                project: row.project ?? null,
                embedding_model: model,
                embedding_dimensions: embedding.length,
                embedding,
                chunk_count: row.chunk_count ?? 1,
                text: row.text,
                source_text_hash_sha256: row.source_text_hash_sha256,
                expiresAt: row.expiresAt,
                updated_at: new Date(),
              }];
            });

            if (storedRows.length > 0) {
              await upsertGraphNodeEmbeddings(mongo.graphNodeEmbeddings, storedRows);
            }
          }
        } catch (err) {
          log.warn({ err, count: graphNodeEmbeddingInputs.size }, "Failed to materialize graph node embeddings during event ingest");
        }
      })()
    : Promise.resolve();

  const eventVectorsTask = eventVectorTasks.length > 0
    ? Promise.allSettled(eventVectorTasks.map(task => task())).then((results) => {
        const rejected = results.filter((result) => result.status === "rejected").length;
        if (rejected > 0) {
          log.warn({ rejected, queued: eventVectorTasks.length }, "Detached event vector indexing completed with rejected tasks");
        }
      })
    : Promise.resolve();

  // All work must terminate even if a task's logger throws: a first rejection
  // cannot release parent ownership while another task is still writing.
  const backgroundIndexing = Promise.allSettled([graphNodeEmbeddingTask, eventVectorsTask]).then(() => undefined);
  // Detached by default, mirroring the original route behavior.
  void backgroundIndexing;

  return {
    ok: true,
    count: ids.length,
    ids,
    projectedGraphEdges: projectedGraphEdges.length,
    ftsEnabled: true,
    storageBackend: "mongodb",
    indexed: true,
    indexing: eventVectorTasks.length > 0 || graphNodeEmbeddingInputs.size > 0 ? "queued" : "skipped",
    queuedEventVectors: eventVectorTasks.length,
    queuedGraphNodeEmbeddings: graphNodeEmbeddingInputs.size,
    acceptedEvents,
    backgroundIndexing,
  };
}
