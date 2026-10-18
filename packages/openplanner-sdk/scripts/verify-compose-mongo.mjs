// SPDX-License-Identifier: GPL-3.0-or-later
// Execute the checked-in Mongo topology only in a uniquely owned disposable fixture.
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import { appendFileSync, copyFileSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { MongoClient } from "mongodb";
import { loadConfig } from "../dist/config.js";
import { reconcileManagedTtl, upsertEvent, upsertCompactedMemory } from "../dist/mongodb.js";
import { listMongoVectorPartitions, replaceMongoVectorEntries, upsertMongoVectorDocuments } from "../dist/mongo-vectors.js";

const root = fileURLToPath(new URL("../../../", import.meta.url));
const fixtureEnv = { PATH: process.env.PATH, HOME: process.env.HOME };
const docker = (...args) => execFileSync("docker", args, { env: fixtureEnv, encoding: "utf8", timeout: 15_000 }).trim();
const compose = JSON.parse(docker("compose", "--env-file", "/dev/null", "-f", `${root}docker-compose.yml`, "--profile", "mongodb", "config", "--format", "json"));
const service = compose.services.mongodb;
assert.ok(Array.isArray(service.command), "documented Mongo service must declare a transactional replica topology");
assert.deepEqual(service.command, ["mongod", "--replSet", "openplanner", "--bind_ip_all"]);
assert.equal(compose.services.openplanner.environment.MONGODB_URI, "mongodb://mongodb:27017/?replicaSet=openplanner");
assert.equal(service.healthcheck.test[0], "CMD");
const documented = readFileSync(new URL("../../../.env.example", import.meta.url), "utf8").match(/^MONGODB_URI=(.+)$/m)?.[1];
assert.equal(documented, "mongodb://localhost:27017/?directConnection=true");
// Render the documented copied-.env setup without reading a private project .env.
const envDirectory = mkdtempSync(join(tmpdir(), "openplanner-compose-env-proof-"));
try {
  const legacy = join(envDirectory, "legacy.env");
  const legacyUri = "mongodb://legacy-external.invalid:27017/?replicaSet=legacy";
  for (const composeOverride of [undefined, "", "mongodb://explicit-external.invalid:27017/?replicaSet=explicit"]) {
    writeFileSync(legacy, `MONGODB_URI=${legacyUri}\n${composeOverride === undefined ? "" : `COMPOSE_MONGODB_URI=${composeOverride}\n`}`);
    const rendered = JSON.parse(docker("compose", "--env-file", legacy, "-f", `${root}docker-compose.yml`, "--profile", "mongodb", "config", "--format", "json"));
    assert.equal(rendered.services.openplanner.environment.MONGODB_URI, composeOverride || legacyUri,
      "custom legacy Mongo URIs must survive unless a nonempty Compose-specific override is explicit");
  }
  const copied = join(envDirectory, ".env");
  copyFileSync(new URL("../../../.env.example", import.meta.url), copied);
  appendFileSync(copied, "\nOPENPLANNER_STORAGE_BACKEND=mongodb\n");
  const fromCopied = JSON.parse(docker("compose", "--env-file", copied, "-f", `${root}docker-compose.yml`, "--profile", "mongodb", "config", "--format", "json"));
  assert.equal(fromCopied.services.openplanner.environment.OPENPLANNER_STORAGE_BACKEND, "mongodb");
  assert.equal(fromCopied.services.openplanner.environment.MONGODB_URI, "mongodb://mongodb:27017/?replicaSet=openplanner",
    "copied .env must keep the application container on the Mongo service URI");
  const override = "mongodb://owned-external.invalid:27017/?replicaSet=owned";
  appendFileSync(copied, `COMPOSE_MONGODB_URI=${override}\n`);
  const fromOverride = JSON.parse(docker("compose", "--env-file", copied, "-f", `${root}docker-compose.yml`, "--profile", "mongodb", "config", "--format", "json"));
  assert.equal(fromOverride.services.openplanner.environment.MONGODB_URI, override,
    "an explicit Compose-specific external Mongo URI must survive unchanged");
} finally { rmSync(envDirectory, { recursive: true, force: true }); }
const previousUri = process.env.MONGODB_URI;
delete process.env.MONGODB_URI;
try { assert.equal(loadConfig().mongodb.uri, documented); }
finally { if (previousUri === undefined) delete process.env.MONGODB_URI; else process.env.MONGODB_URI = previousUri; }

// Refuse a missing image instead of pulling or touching any existing service.
const image = JSON.parse(docker("image", "inspect", service.image))[0].Id;
const name = `openplanner-compose-proof-${randomUUID()}`;
let container;
let client;
try {
  container = docker("run", "--detach", "--pull=never", "--name", name, "--hostname", "mongodb",
    "--user", "999:999", "--read-only", "--cap-drop=ALL", "--security-opt=no-new-privileges",
    "--cpus=1", "--memory=1g", "--pids-limit=128",
    "--tmpfs", "/data/db:rw,nosuid,nodev,size=512m,uid=999,gid=999",
    "--tmpfs", "/tmp:rw,nosuid,nodev,size=32m,uid=999,gid=999",
    "--publish", "127.0.0.1::27017", "--entrypoint", service.command[0], image,
    ...service.command.slice(1), "--wiredTigerCacheSizeGB", "0.25");
  let healthy = false;
  for (let attempt = 0; attempt < 50; attempt++) {
    try { docker("exec", container, ...service.healthcheck.test.slice(1)); healthy = true; break; }
    catch (error) {
      if (error.status !== 1) throw error;
      await new Promise(resolve => setTimeout(resolve, 250));
    }
  }
  assert.ok(healthy, "actual configured health must reach a writable primary");
  docker("exec", container, ...service.healthcheck.test.slice(1));
  const address = docker("port", container, "27017/tcp");
  assert.match(address, /^127\.0\.0\.1:\d+$/);
  const uri = new URL(documented);
  uri.port = address.split(":")[1]; // Only the unique fixture port differs from the documented client URI.
  client = new MongoClient(uri.href, { serverSelectionTimeoutMS: 1_500, connectTimeoutMS: 1_500 });
  await client.connect();
  const db = client.db("owned_compose_proof");
  const hello = await db.admin().command({ hello: 1 });
  assert.equal(hello.setName, "openplanner");
  assert.equal(hello.isWritablePrimary, true);
  const callerTime = new Date(0);
  const callerFields = [
    ["identity", { _id: "caller-owned-id" }],
    ["creation-time", { createdAt: callerTime }],
    ["round-trip", { _id: "caller-owned-id", createdAt: callerTime, updatedAt: callerTime }],
  ];
  const callerFieldFailures = [];
  for (const [kind, write] of [["event", upsertEvent], ["compacted", upsertCompactedMemory]]) {
    const collection = db.collection(`owned_${kind}_caller_fields`);
    for (const [index, [scenario, fields]] of callerFields.entries()) {
      const id = `${kind}-${scenario}`;
      try {
        await write(collection, { id, text: "first", extra: {}, ...fields }, 0);
        const inserted = await collection.findOne({ _id: id });
        assert.equal(inserted._id, id);
        assert.ok(inserted.createdAt instanceof Date && inserted.createdAt > callerTime);
        const createdAt = inserted.createdAt.getTime();
        await write(collection, { ...inserted, ...fields, text: "round-tripped" }, 0);
        const updated = await collection.findOne({ _id: id });
        assert.equal(updated._id, id);
        assert.equal(updated.createdAt.getTime(), createdAt);
        assert.ok(updated.updatedAt instanceof Date && updated.updatedAt >= inserted.updatedAt);
        assert.equal(updated.text, "round-tripped");
        assert.equal(await collection.countDocuments(), index + 1);
      } catch (error) {
        callerFieldFailures.push({ kind, scenario, error: error.name, code: error.code, message: error.message });
      }
    }
  }
  assert.deepEqual(callerFieldFailures, [], "actual Mongo event/memory writes must ignore caller-managed identity and timestamps");
  const retained = db.collection("owned_retention_cleanup");
  await retained.insertMany([
    ...Array.from({ length: 1000 }, (_, id) => ({ _id: `retained-${id}`, value: id })),
    { _id: "dated", expiresAt: new Date("2099-01-01T00:00:00Z") },
    { _id: "explicit-null", expiresAt: null },
    { _id: "non-date", expiresAt: "unchanged" },
  ]);
  await reconcileManagedTtl(retained, "owned_ttl", 0);
  const cleanupIndex = (await retained.indexes()).find(index => index.name === "owned_ttl_cleanup");
  assert.ok(cleanupIndex, "disabled retention must retain a non-TTL date cleanup index");
  assert.deepEqual(cleanupIndex.partialFilterExpression, { expiresAt: { $type: "date" } });
  assert.equal(Object.hasOwn(cleanupIndex, "expireAfterSeconds"), false);
  assert.equal(await retained.countDocuments(), 1003);
  assert.equal(Object.hasOwn(await retained.findOne({ _id: "dated" }), "expiresAt"), false);
  assert.equal((await retained.findOne({ _id: "explicit-null" })).expiresAt, null);
  assert.equal((await retained.findOne({ _id: "non-date" })).expiresAt, "unchanged");
  const cleanupPlan = await db.command({
    explain: { update: retained.collectionName, updates: [
      { q: { expiresAt: { $type: "date" } }, u: { $unset: { expiresAt: "" } }, multi: true },
    ] }, verbosity: "executionStats",
  });
  const winningPlan = JSON.stringify(cleanupPlan.queryPlanner.winningPlan);
  assert.ok(winningPlan.includes('"indexName":"owned_ttl_cleanup"'), "repeat startup cleanup must use the partial index");
  assert.ok(!winningPlan.includes('"stage":"COLLSCAN"'));
  assert.equal(cleanupPlan.executionStats.totalDocsExamined, 0);
  await reconcileManagedTtl(retained, "owned_ttl", 0);
  assert.deepEqual((await retained.indexes()).find(index => index.name === cleanupIndex.name), cleanupIndex);
  await reconcileManagedTtl(retained, "owned_ttl", 60);
  const enabledIndexes = await retained.indexes();
  assert.ok(!enabledIndexes.some(index => index.name === "owned_ttl_cleanup"));
  assert.equal(enabledIndexes.find(index => index.name === "owned_ttl").expireAfterSeconds, 0);
  const mongo = { db, client, hotVectors: db.collection("hot_vectors"), compactVectors: db.collection("compact_vectors"),
    vectorPartitions: db.collection("vector_partitions"), retention: { eventsTtlSeconds: 0, compactedTtlSeconds: 0 } };
  const entry = id => ({ id, parentId: "owned-parent", text: "outside encounter", embedding: [1, 0], metadata: { embedding_model: "owned-fixture" } });
  await upsertMongoVectorDocuments(mongo, "hot", [entry("first")]);
  await replaceMongoVectorEntries(mongo, "hot", "owned-parent", [entry("replacement")]);
  const partitions = await listMongoVectorPartitions(mongo, "hot");
  assert.equal(partitions.length, 1);
  const partition = db.collection(partitions[0].collectionName);
  const ids = async collection => (await collection.find({}).toArray()).map(row => row._id).sort();
  assert.deepEqual(await ids(mongo.hotVectors), ["replacement"]);
  assert.deepEqual(await ids(partition), ["replacement"]);
  const original = mongo.hotVectors.bulkWrite;
  mongo.hotVectors.bulkWrite = async (_operations, options) => {
    assert.ok(options.session, "actual SDK replacement must retain its transaction session");
    throw new Error("owned replacement failure after deletes");
  };
  try { await assert.rejects(replaceMongoVectorEntries(mongo, "hot", "owned-parent", [entry("failed")]), /owned replacement failure/); }
  finally { mongo.hotVectors.bulkWrite = original; }
  assert.deepEqual(await ids(mongo.hotVectors), ["replacement"]);
  assert.deepEqual(await ids(partition), ["replacement"]);
  console.log(JSON.stringify({ passed: true, image, replica: hello.setName, health_idempotent: true,
    actual_sdk_replacement_and_rollback: true, adjusted_only_client_port: true,
    caller_identity_and_timestamp_round_trips: 6,
    disabled_retention_indexed_cleanup_and_reenable: true, repeat_cleanup_docs_examined: 0 }));
} finally {
  try { if (client) await client.close(); }
  finally { if (container) docker("rm", "--force", container); }
}
