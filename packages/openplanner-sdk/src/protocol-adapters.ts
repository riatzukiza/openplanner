/**
 * Protocol adapters for OpenPlanner routes.
 *
 * These adapters wrap MongoDB (or REST) operations behind protocol interfaces,
 * allowing routes to delegate to protocols instead of direct DB access.
 *
 * env var PROTOCOL_IMPL: "mongo" (default) | "rest"
 */

import { createHash } from "node:crypto";
import { upsertEvent, eventRetentionExpiry, type MongoConnection } from "./mongodb.js";
import { assertNonblankIdentifier } from "./types.js";

/**
 * The minimal context protocol implementations need. The REST API passes its
 * Fastify-decorated connection; library consumers (knoxx, workers) pass the
 * connection from createOpenPlannerSdk / openMongoDB directly.
 */
export interface ProtocolContext {
  mongo: MongoConnection;
}

export interface EventAdmission {
  appendEvent(event: any): Promise<any>;
  appendEvents(events: any[]): Promise<any[]>;
  queryEvents(filter: any): Promise<any[]>;
}

/** Undefined is unscoped; null selects only projectless markers. */
export interface SessionScope { project?: string | null; }

export interface SessionManagement {
  createSession(opts: any): Promise<any>;
  getSession(sessionId: string, opts?: SessionScope): Promise<any>;
  updateSession(sessionId: string, updates: any, opts?: SessionScope): Promise<any>;
  closeSession(sessionId: string, opts?: SessionScope): Promise<void>;
  listSessions(opts: { project?: string; limit: number; offset: number }): Promise<{ rows: any[]; total: number }>;
  getSessionEvents(sessionId: string, opts: any): Promise<any[]>;
}

export interface TenantManagement {
  listTenants(): Promise<any[]>;
  getTenant(tenantId: string): Promise<any | null>;
  createTenant(tenant: any): Promise<any>;
  updateTenant(tenantId: string, updates: any): Promise<any | null>;
  deleteTenant(tenantId: string): Promise<boolean>;
  getPolicy(tenantId: string): Promise<any | null>;
  setPolicy(tenantId: string, policy: any): Promise<any>;
}

const PROTOCOL_IMPL = process.env.PROTOCOL_IMPL ?? "mongo";

function assertOptionalSessionString(value: unknown, field: string): void {
  if (value !== undefined && typeof value !== "string") {
    throw new Error(`${field} session filter must be a string`);
  }
}

function sessionPageInteger(value: unknown, fallback: number): number {
  if (value === undefined) return fallback;
  if (!Number.isSafeInteger(value)) throw new Error("session pagination values must be finite safe integers");
  return value as number;
}

// ── MongoDB Implementations ─────────────────────────────────────────

class MongoEventAdmission implements EventAdmission {
  constructor(private ctx: ProtocolContext) {}

  async appendEvent(event: any): Promise<any> {
    assertNonblankIdentifier(event?.id, "event.id");
    await upsertEvent(this.ctx.mongo.events, event, this.ctx.mongo.retention?.eventsTtlSeconds);
    return event;
  }

  async appendEvents(events: any[]): Promise<any[]> {
    for (const event of events) assertNonblankIdentifier(event?.id, "event.id");
    for (const event of events) await this.appendEvent(event);
    return events;
  }

  async queryEvents(filter: any): Promise<any[]> {
    return this.ctx.mongo.events.find(filter).sort({ ts: -1 }).toArray();
  }
}

class MongoSessionManagement implements SessionManagement {
  constructor(private ctx: ProtocolContext) {}

  async createSession(opts: any): Promise<any> {
    if (typeof opts?.session !== "string" || !opts.session.trim()) throw new Error("session must be a nonblank string");
    assertOptionalSessionString(opts.project, "project");
    const { expiresAt: _callerExpiry, _id: _callerId, ...content } = opts;
    const now = new Date();
    const expiresAt = eventRetentionExpiry(content.extra, this.ctx.mongo.retention?.eventsTtlSeconds ?? 0, now);
    const existing = await this.ctx.mongo.events.findOne({ session: content.session, kind: "session", project: content.project ?? null });
    if (existing) return existing; // Preserve an already stored legacy marker on retry.
    const markerId = "session-marker:" + createHash("sha256").update(JSON.stringify(["session", content.project ?? null, content.session])).digest("hex");
    const doc = { ...content, _id: markerId, kind: "session", createdAt: now, updatedAt: now,
      ...(expiresAt ? { expiresAt } : {}) };
    // Mongo's unique _id makes concurrent new marker creation converge atomically.
    await this.ctx.mongo.events.updateOne({ _id: markerId }, { $setOnInsert: doc }, { upsert: true });
    const stored = await this.ctx.mongo.events.findOne({ _id: markerId });
    if (!stored || stored.kind !== "session" || stored.session !== content.session || (stored.project ?? null) !== (content.project ?? null)) {
      throw new Error("session marker identity conflicts with stored document");
    }
    return stored;
  }

  async getSession(sessionId: string, opts: SessionScope = {}): Promise<any> {
    assertNonblankIdentifier(sessionId, "session");
    if (opts.project !== null) assertOptionalSessionString(opts.project, "project");
    const filter = { session: sessionId, kind: "session", ...(opts.project === undefined ? {} : { project: opts.project }) };
    const first = await this.ctx.mongo.events.findOne(filter);
    if (first && opts.project === undefined) {
      const matches = await this.ctx.mongo.events.find(filter).limit(2).toArray();
      if (matches.length > 1) throw new Error("ambiguous session identity; specify project");
    }
    return first;
  }

  async updateSession(sessionId: string, updates: any, opts: SessionScope = {}): Promise<any> {
    const stored = await this.getSession(sessionId, opts);
    if (!stored) return null;
    const { _id, session, project: _project, kind, createdAt, expiresAt: _callerExpiry, ...mutable } = updates;
    const now = new Date();
    const retentionUpdate: Record<string, unknown> = {};
    if (["extra", "extra.openplanner_labels", "extra.openplanner_labels.labels", "expiresAt"]
      .some(key => Object.hasOwn(updates, key))) {
      let extra = Object.hasOwn(mutable, "extra") ? mutable.extra : stored?.extra;
      if (Object.hasOwn(mutable, "extra.openplanner_labels")) {
        extra = { ...extra, openplanner_labels: mutable["extra.openplanner_labels"] };
      }
      if (Object.hasOwn(mutable, "extra.openplanner_labels.labels")) {
        extra = { ...extra, openplanner_labels: { ...extra?.openplanner_labels,
          labels: mutable["extra.openplanner_labels.labels"] } };
      }
      const managedExpiry = eventRetentionExpiry(extra, this.ctx.mongo.retention?.eventsTtlSeconds ?? 0, now);
      if (["extra", "extra.openplanner_labels", "extra.openplanner_labels.labels"]
        .some(key => Object.hasOwn(mutable, key))) {
        // Persist one canonical parent path; Mongo refuses parent/child $set collisions.
        mutable.extra = extra;
        delete mutable["extra.openplanner_labels"];
        delete mutable["extra.openplanner_labels.labels"];
      }
      if (managedExpiry) mutable.expiresAt = managedExpiry;
      else retentionUpdate.$unset = { expiresAt: "" };
    }
    await this.ctx.mongo.events.updateOne(
      { _id: stored._id },
      { $set: { ...mutable, session: sessionId, kind: "session", updatedAt: now }, ...retentionUpdate }
    );
    return this.getSession(sessionId, { project: stored.project ?? null });
  }

  async closeSession(sessionId: string, opts: SessionScope = {}): Promise<void> {
    const stored = await this.getSession(sessionId, opts);
    if (stored) await this.ctx.mongo.events.deleteMany({ _id: stored._id });
  }

  async listSessions(opts: { project?: string; limit: number; offset: number }): Promise<{ rows: any[]; total: number }> {
    assertOptionalSessionString(opts.project, "project");
    const requestedLimit = sessionPageInteger(opts.limit, 50);
    const limit = Math.min(requestedLimit > 0 ? requestedLimit : 50, 500);
    const offset = Math.max(0, sessionPageInteger(opts.offset, 0));
    const match: Record<string, unknown> = { session: { $type: "string", $ne: "" } };
    if (opts.project !== undefined) match.project = opts.project;

    const groupedStages = [
      { $match: match },
      {
        $group: {
          _id: { project: { $ifNull: ["$project", null] }, session: "$session" },
          last_ts: { $max: "$ts" },
          event_count: { $sum: 1 },
        },
      },
      {
        $project: {
          _id: 0,
          project: "$_id.project",
          session: "$_id.session",
          last_ts: "$last_ts",
          event_count: "$event_count",
        },
      },
      { $sort: { last_ts: -1, project: 1, session: 1 } },
    ];

    const [rawRows, totalRows] = await Promise.all([
      this.ctx.mongo.events.aggregate([...groupedStages, { $skip: offset }, { $limit: limit }]).toArray(),
      this.ctx.mongo.events.aggregate([...groupedStages, { $count: "total" }]).toArray(),
    ]);

    const rows = rawRows.map((row: any) => ({
      ...row,
      last_ts: row.last_ts instanceof Date ? row.last_ts.toISOString() : row.last_ts,
      event_count: typeof row.event_count === "bigint" ? Number(row.event_count) : row.event_count,
    }));

    const total = Number(totalRows[0]?.total ?? 0);

    return { rows, total };
  }

  async getSessionEvents(sessionId: string, opts: any): Promise<any[]> {
    assertNonblankIdentifier(sessionId, "session");
    if (opts.project !== null) assertOptionalSessionString(opts.project, "project");
    assertOptionalSessionString(opts.kind, "kind");
    const requestedLimit = sessionPageInteger(opts.limit, 1000);
    if (opts.mode !== undefined && opts.mode !== "full") throw new Error("session event mode must be full when supplied");
    const limit = Math.min(requestedLimit > 0 ? requestedLimit : 1000, opts.mode === "full" ? 100001 : 1000);
    const filter: Record<string, unknown> = { session: sessionId };
    if (opts.project === undefined) {
      const scopes = await this.ctx.mongo.events.aggregate([
        { $match: { session: sessionId } },
        { $group: { _id: { $ifNull: ["$project", null] } } },
        { $limit: 2 },
      ]).toArray();
      if (scopes.length > 1) throw new Error("ambiguous session identity; specify project");
      const project = scopes[0]?._id ?? null;
      if (project !== null) assertOptionalSessionString(project, "project");
      filter.project = project;
    } else filter.project = opts.project;
    if (opts.kind) filter.kind = opts.kind;

    let cursor = this.ctx.mongo.events.find(filter);
    if (opts.projection) cursor = cursor.project(opts.projection);
    return cursor
      .sort({ ts: opts.sort === "asc" ? 1 : -1, _id: opts.sort === "asc" ? 1 : -1 })
      .limit(limit)
      .toArray();
  }
}

class MongoTenantManagement implements TenantManagement {
  constructor(private ctx: ProtocolContext) {}

  private collection() {
    return this.ctx.mongo.db.collection("tenants");
  }

  private policiesCollection() {
    return this.ctx.mongo.db.collection("tenant_policies");
  }

  async listTenants(): Promise<any[]> {
    return this.collection().find({}).sort({ created_at: -1 }).toArray();
  }

  async getTenant(tenantId: string): Promise<any | null> {
    assertNonblankIdentifier(tenantId, "tenant_id");
    return this.collection().findOne({ tenant_id: tenantId });
  }

  async createTenant(tenant: any): Promise<any> {
    if (typeof tenant?.tenant_id !== "string" || !tenant.tenant_id.trim()) {
      throw new Error("tenant_id must be a nonblank string");
    }
    const now = new Date().toISOString();
    const doc = {
      ...tenant,
      created_at: now,
      updated_at: now,
    };
    await this.collection().insertOne(doc);
    return doc;
  }

  async updateTenant(tenantId: string, updates: any): Promise<any | null> {
    assertNonblankIdentifier(tenantId, "tenant_id");
    const { _id: _storageId, tenant_id: _tenantId, created_at: _createdAt, ...mutable } = updates;
    const result = await this.collection().findOneAndUpdate(
      { tenant_id: tenantId },
      { $set: { ...mutable, updated_at: new Date().toISOString() } },
      { returnDocument: "after" }
    );
    return result;
  }

  async deleteTenant(tenantId: string): Promise<boolean> {
    assertNonblankIdentifier(tenantId, "tenant_id");
    await this.ctx.mongo.db.collection("km_labels").deleteMany({ tenant_id: tenantId });
    await this.policiesCollection().deleteOne({ tenant_id: tenantId });
    const result = await this.collection().deleteOne({ tenant_id: tenantId });
    return result.deletedCount > 0;
  }

  async getPolicy(tenantId: string): Promise<any | null> {
    assertNonblankIdentifier(tenantId, "tenant_id");
    return this.policiesCollection().findOne({ tenant_id: tenantId });
  }

  async setPolicy(tenantId: string, policy: any): Promise<any> {
    assertNonblankIdentifier(tenantId, "tenant_id");
    const now = new Date();
    const doc = {
      ...policy,
      tenant_id: tenantId,
      updated_at: now,
    };
    delete (doc as Record<string, unknown>).created_at;
    delete (doc as Record<string, unknown>)._id;
    await this.policiesCollection().updateOne(
      { tenant_id: tenantId },
      { $set: doc, $setOnInsert: { created_at: now } },
      { upsert: true }
    );
    return doc;
  }
}

// ── Factory ─────────────────────────────────────────────────────────

export function createProtocols(ctx: ProtocolContext) {
  if (PROTOCOL_IMPL === "rest") {
    throw new Error("REST protocol implementation not yet available");
  }

  return {
    eventAdmission: new MongoEventAdmission(ctx),
    sessionManagement: new MongoSessionManagement(ctx),
    tenantManagement: new MongoTenantManagement(ctx),
  };
}

export type Protocols = ReturnType<typeof createProtocols>;
