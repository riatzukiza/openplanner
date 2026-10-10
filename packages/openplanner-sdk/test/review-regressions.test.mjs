// SPDX-License-Identifier: LGPL-3.0-or-later
import test, { mock } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { execFile as execFileCallback } from 'node:child_process';
import { promisify } from 'node:util';
import { createHash } from 'node:crypto';
import { MongoClient } from 'mongodb';
import { createProtocols } from '../dist/protocol-adapters.js';
import { openMongoDB, ilikeSearch } from '../dist/mongodb.js';
import { makeEmbeddingCacheKey, PersistentEmbeddingCache } from '../dist/embedding-cache.js';
import { EmbedProviderFunction } from '../dist/embeddings.js';
import { formatEmbeddingQueryText, formatEmbeddingPassageText } from '../dist/embedding-text.js';
import { ftsSearchWithQuality } from '../dist/search-core.js';
import { batchIndexTextsInMongoVectors, upsertMongoVectorDocuments, hydrateVectorDocumentText, removeMongoVectorParentLabel } from '../dist/mongo-vectors.js';
import { prepareIndexDocument } from '../dist/indexing.js';
import { ingestEvents } from '../dist/ingest.js';
import { queryCollectionResponse } from '../dist/mongo-browse.js';

function collection(name, initial = []) {
  const rows = new Map(initial.map(r => [r._id ?? r.id, r]));
  const calls = { indexes: [], writes: [], drops: [], searches: [], deletes: [], setups: 0 };
  const cursor = (values) => ({
    sort() { values.sort((a,b) => new Date(b.ts) - new Date(a.ts)); return this; },
    project() { return this; }, limit(n) { values = values.slice(0,n); return this; },
    async toArray() { return values; },
  });
  return {
    collectionName: name, rows, calls, failSetup: false, failText: false,
    async createIndex(keys, opts = {}) { calls.indexes.push({keys, opts}); return opts.name; },
    async indexes() { return name === 'semantic_graph_runs' ? [{name:'graph_version_1',key:{graph_version:1},unique:true}] : []; },
    async dropIndex(name) { calls.drops.push(name); },
    async updateOne(filter, update) {
      if (this.failSetup) { this.failSetup = false; throw new Error('temporary partition failure'); }
      calls.writes.push({filter, update});
      const key = filter._id ?? filter.tenant_id;
      const old = rows.get(key);
      const next = {...(!old ? update.$setOnInsert : {}), ...old, ...filter, ...update.$set};
      for (const field of Object.keys(update.$unset ?? {})) delete next[field];
      rows.set(key, next); return {modifiedCount:1};
    },
    async insertOne(row) { rows.set(`auto-${rows.size}`, {...row}); },
    async insertMany(items) { for (const row of items) await this.insertOne(row); },
    async findOne(filter) { return rows.get(filter._id ?? filter.tenant_id) ?? null; },
    find(filter) {
      calls.searches.push(filter);
      if (filter.$text && this.failText) throw new Error('text index unavailable');
      return cursor([...rows.values()].filter(r => {
        const q = filter['extra.openplanner_labels.quality'];
        if (q === 'good' && r.extra?.openplanner_labels?.quality !== 'good') return false;
        if (q?.$ne === 'bad' && r.extra?.openplanner_labels?.quality === 'bad') return false;
        if (filter.id?.$nin?.includes(r.id)) return false;
        return true;
      }));
    },
    async deleteMany(filter) { calls.deletes.push(filter); },
    async updateMany(filter, update) { calls.writes.push({filter,update}); },
    async bulkWrite(ops) { for (const op of ops) await this.updateOne(op.updateOne.filter, op.updateOne.update); },
    listSearchIndexes() { calls.setups++; return cursor([{status:'READY',queryable:true}]); },
  };
}
function mongoFixture() {
  const collections = new Map();
  const db = {collection(name) { if (!collections.has(name)) collections.set(name,collection(name)); return collections.get(name); }};
  const client = {startSession() { return {withTransaction: async f => f(), endSession: async () => {}}; }};
  return {db, client, collections, events:db.collection('events'), compacted:db.collection('compacted'),
    hotVectors:db.collection('hot_vectors'), compactVectors:db.collection('compact_vectors'), vectorPartitions:db.collection('vector_partitions'),
    retention:{eventsTtlSeconds:60,compactedTtlSeconds:120}};
}
const item = (id, text = 'ordinary document', extra) => ({id,text,extra,metadata:{embedding_model:'fixture',ts:'2026-01-01T00:00:00Z'}});
const entry = (id = 'chunk', labels = []) => ({id,parentId:'parent',text:'chunk',embedding:[1,2],metadata:{embedding_model:'fixture',labels}});
async function withEnv(values, f) {
  const saved = Object.fromEntries(Object.keys(values).map(k => [k,process.env[k]]));
  Object.assign(process.env,values);
  try { return await f(); } finally { for (const [k,v] of Object.entries(saved)) if (v === undefined) delete process.env[k]; else process.env[k] = v; }
}
async function withFetch(fixture, f) { const original = globalThis.fetch; globalThis.fetch = fixture; try { return await f(); } finally { globalThis.fetch = original; } }
const response = vectors => new Response(JSON.stringify({data:vectors.map(embedding=>({embedding}))}),{status:200});

for (const batch of [false,true]) test(`event ${batch ? 'batch' : 'single'} admission is replay-safe and uses configured retention`, async () => {
  const mongo = mongoFixture(); const admission = createProtocols({mongo}).eventAdmission;
  const row = {id:'stable-id',text:'encounter',extra:{},ts:new Date('2026-01-01')};
  await withEnv({MONGODB_EVENTS_TTL_SECONDS:'0'},async () => {
    if (batch) { await admission.appendEvents([row,row]); } else { await admission.appendEvent(row); await admission.appendEvent(row); }
  });
  assert.equal(mongo.events.rows.size,1);
  const stored = mongo.events.rows.get('stable-id'); assert.ok(stored);
  assert.equal(stored._id,'stable-id'); assert.ok(stored.createdAt instanceof Date); assert.ok(stored.schema_version);
  assert.equal(stored.expiresAt - stored.updatedAt,60_000);
  await admission.appendEvent({...row,extra:{openplanner_labels:{labels:['keep']}}});
  assert.equal(mongo.events.rows.get('stable-id').expiresAt,undefined);
});
test('tenant policy payload cannot replace the authoritative tenant ID',async () => {
  const mongo = mongoFixture(); const doc = await createProtocols({mongo}).tenantManagement.setPolicy('tenant-a',{tenant_id:'tenant-b',retention_days:4});
  assert.equal(doc.tenant_id,'tenant-a'); assert.equal(mongo.db.collection('tenant_policies').rows.get('tenant-a').tenant_id,'tenant-a');
});
test('policy updates preserve stored creation time even when the payload supplies it',async()=>{
  const mongo=mongoFixture();const policies=mongo.db.collection('tenant_policies');
  const created=new Date('2025-01-01T00:00:00Z');
  policies.rows.set('tenant-a',{tenant_id:'tenant-a',created_at:created});
  await createProtocols({mongo}).tenantManagement.setPolicy('tenant-a',{created_at:new Date('2024-01-01'),retention_days:4});
  assert.equal(policies.rows.get('tenant-a').created_at,created);
  const update=policies.calls.writes[0].update;
  assert.equal(Object.hasOwn(update.$set,'created_at'),false);
  assert.ok(update.$setOnInsert.created_at instanceof Date);
  assert.ok(update.$set.updated_at instanceof Date);
});
test('all embedding-cache methods share entries and support invalidation',async()=>{
  const cache=new PersistentEmbeddingCache();
  cache.set('single',{embedding:[1,2],cachedAt:1});
  assert.equal(cache.has('single'),true);
  assert.deepEqual((await cache.getMany(['single'])).get('single'),[1,2]);
  await cache.putMany([{key:'batch',vector:[3,4]}]);
  assert.equal(cache.has('batch'),true);
  cache.delete('batch');cache.delete('single');
  assert.equal(cache.has('batch'),false);
  assert.equal(cache.size,0);
  assert.equal((await cache.getMany(['batch','single'])).size,0);
});
test('an invalid later ingest event prevents every event write and embedding call',async()=>{
  const mongo=mongoFixture();let embedded=0;
  const valid={schema:'openplanner.event.v1',id:'valid-first',ts:'2026-01-01T00:00:00Z',source:'fixture',kind:'message',text:'memory'};
  const embeddingRuntime={hot:{getBackgroundEmbeddingFunction(){embedded++;return {generate:async texts=>texts.map(()=>[1,2])};},getModel(){return 'fixture';}}};
  await assert.rejects(ingestEvents({mongo,embeddingRuntime},[valid,{...valid,id:''}]),/id/i);
  assert.equal(mongo.events.calls.writes.length,0);
  assert.equal(mongo.events.rows.size,0);
  assert.equal(embedded,0);
});
for(const filter of [{$where:'true'},{$and:[{nested:{$function:{body:'return true',args:[],lang:'js'}}}]},{nested:{$accumulator:{init:'function() {}'}}}])test('raw browse refuses server-side JavaScript before accessing a collection',async()=>{
  let accessed=0;
  const mongo={db:{collection(){accessed++;return {countDocuments:async()=>0,find(){throw new Error('fixture database reached');}};}}};
  await assert.rejects(queryCollectionResponse({mongo},{collection:'events',filter}),/server-side JavaScript/);
  assert.equal(accessed,0);
});
test('permitted raw browse filters carry time limits to count and find',async()=>{
  const calls=[];const filter={$and:[{kind:'message'},{count:{$gt:1}}]};
  const cursor={sort(){return this;},skip(){return this;},limit(){return this;},async toArray(){return [{id:'one'}];}};
  const mongo={db:{collection:()=>({async countDocuments(query,options){calls.push({query,options});return 1;},find(query,options){calls.push({query,options});return cursor;}})}};
  const result=await queryCollectionResponse({mongo},{collection:'events',filter,projection:{id:1}});
  assert.deepEqual(result.rows,[{id:'one'}]);
  assert.equal(calls.length,2);
  for(const call of calls){assert.equal(call.query,filter);assert.ok(call.options?.maxTimeMS>0);assert.ok(call.options.maxTimeMS<=5000);}
  assert.deepEqual(calls[1].options.projection,{id:1});
});
test('embedding keys distinguish suffixes and models',() => {
  const prefix = 'common-header-'.repeat(12);
  assert.notEqual(makeEmbeddingCacheKey({model:'m',text:prefix+'one'}),makeEmbeddingCacheKey({model:'m',text:prefix+'two'}));
  assert.notEqual(makeEmbeddingCacheKey({model:'m',text:prefix}),makeEmbeddingCacheKey({model:'n',text:prefix}));
});
test('same-prefix inputs reach the provider separately and retain their own cached vectors',async () => {
  const calls=[]; const texts=['x'.repeat(128)+'one','x'.repeat(128)+'two'];
  await withFetch(async (_,opts) => { const {input}=JSON.parse(opts.body); calls.push(input); return response(input.map(t=>t.endsWith('one')?[1,0]:[0,1])); },async () => {
    const provider = new EmbedProviderFunction('fixture','http://fixture.invalid',{batchWindowMs:1,cache:new PersistentEmbeddingCache()});
    assert.deepEqual(await provider.generate(texts),[[1,0],[0,1]]);
    assert.deepEqual(await provider.generate(texts),[[1,0],[0,1]]);
    assert.deepEqual(calls.flat(),texts);
  });
});
for (const status of [401,503]) test(`overflow fallback propagates ${status} and retries later`,async () => {
  let attempts=0;
  await withFetch(async () => { attempts++; if(attempts===1)return new Response('context window exceeded',{status:400}); if(attempts===2)return new Response('fixture unavailable',{status}); return response([[1,2]]); },async () => {
    const provider=new EmbedProviderFunction('fixture','http://fixture.invalid',{batchWindowMs:1,cache:new PersistentEmbeddingCache()});
    await assert.rejects(provider.generate(['retry me']),new RegExp(String(status)));
    assert.deepEqual(await provider.generate(['retry me']),[[1,2]]); assert.equal(attempts,3);
  });
});
test('invalid provider vectors fail without poisoning the cache',async () => {
  let attempts=0;
  await withFetch(async () => new Response(JSON.stringify({embeddings:++attempts===1?[[]]:[[1,2]]}),{status:200}),async () => {
    const provider=new EmbedProviderFunction('fixture','http://fixture.invalid',{batchWindowMs:1,cache:new PersistentEmbeddingCache()});
    await assert.rejects(provider.generate(['retry invalid']),/invalid|empty|unavailable/i);
    assert.deepEqual(await provider.generate(['retry invalid']),[[1,2]]);
  });
});
test('embedding POST carries a timeout cancellation signal',async () => {
  await withFetch(async (_,opts) => { assert.ok(opts.signal instanceof AbortSignal); return response([[1,2]]); },async () => {
    const provider=new EmbedProviderFunction('fixture','http://fixture.invalid',{batchWindowMs:1});
    assert.deepEqual(await provider.generate(['bounded']),[[1,2]]);
  });
});
test('template substitution preserves dollar sequences literally',async () => {
  await withEnv({EMBED_QUERY_TEMPLATE:'prefix:{query}:end',EMBED_PASSAGE_TEMPLATE:'prefix:{text}:end'},async () => {
    const value="$& $' $`"; assert.equal(formatEmbeddingQueryText(value),`prefix:${value}:end`); assert.equal(formatEmbeddingPassageText(value),`prefix:${value}:end`);
  });
});
for (const tier of ['hot','compact']) test(`${tier} vector expiry uses connection configuration instead of process env`,async () => {
  const mongo=mongoFixture(); await withEnv({MONGODB_EVENTS_TTL_SECONDS:'0',MONGODB_COMPACTED_TTL_SECONDS:'0'},async()=>upsertMongoVectorDocuments(mongo,tier,[entry()]));
  const stored=(tier==='hot'?mongo.hotVectors:mongo.compactVectors).rows.get('chunk');
  assert.equal(stored.expiresAt-stored.updatedAt,tier==='hot'?60_000:120_000);
  const partition=[...mongo.collections.values()].find(c=>c.collectionName.includes('__'));
  assert.ok(partition.calls.indexes.some(i=>i.keys.expiresAt===1));
});
test('configured Mongo retention and partial graph-version index survive opening the connection',async () => {
  const mongo=mongoFixture(); const connect=mock.method(MongoClient.prototype,'connect',async function(){return this;}); const db=mock.method(MongoClient.prototype,'db',()=>mongo.db);
  try {
    const opened=await openMongoDB({uri:'mongodb://fixture.invalid',dbName:'fixture',eventsCollection:'events',compactedCollection:'compacted',vectorHotCollection:'hot_vectors',vectorCompactCollection:'compact_vectors',graphLayoutCollection:'layout',graphNodeEmbeddingCollection:'embeddings',eventsTtlSeconds:60,compactedTtlSeconds:120});
    assert.deepEqual(opened.retention,{eventsTtlSeconds:60,compactedTtlSeconds:120});
    const graph=mongo.db.collection('semantic_graph_runs');
    assert.deepEqual(graph.calls.indexes.find(i=>i.keys.graph_version)?.opts.partialFilterExpression,{graph_version:{$type:'string'}});
    assert.ok(graph.calls.drops.includes('graph_version_1'));
  } finally {connect.mock.restore();db.mock.restore();}
});
test('substring fallback escapes all regular-expression metacharacters',async()=>{
  const c=collection('events'); const q='c++ [a].*(foo) $ ^ ? \\'; await ilikeSearch(c,q);
  const regex=new RegExp(c.calls.searches[0].text.$regex,'i'); assert.ok(regex.test('prefix '+q+' suffix')); assert.ok(!regex.test('unrelated abc'));
});
for (const tier of ['hot','compact','both']) test(`full-text search respects ${tier} tier, labels rows, and merges within limit`,async()=>{
  const mongo=mongoFixture(); mongo.events=collection('events',[{id:'hot',text:'h',ts:'2026-01-01'}]);mongo.compacted=collection('compacted',[{id:'compact',text:'c',ts:'2026-01-02'}]);
  const r=await ftsSearchWithQuality({mongo},{q:'memory',tier,quality:'any',limit:2});
  assert.deepEqual(r.rows.map(r=>r.id),tier==='hot'?['hot']:tier==='compact'?['compact']:['compact','hot']);
  assert.deepEqual(r.rows.map(r=>r.tier),tier==='both'?['compact','hot']:[tier]);
  assert.equal(mongo.events.calls.searches.length,tier==='compact'?0:1); assert.equal(mongo.compacted.calls.searches.length,tier==='hot'?0:1);
});
test('full-text fallback handles compact tier and preserves good-first ordering across both tiers',async()=>{
  const mongo=mongoFixture(); mongo.events=collection('events',[{id:'hot',ts:'2026-02-01',extra:{openplanner_labels:{quality:'good'}}}]);mongo.compacted=collection('compacted',[{id:'compact',ts:'2026-03-01',extra:{openplanner_labels:{quality:'good'}}},{id:'other',ts:'2026-04-01'}]);mongo.compacted.failText=true;
  const r=await ftsSearchWithQuality({mongo},{q:'[literal]',tier:'both',limit:3});
  assert.deepEqual(r.rows.map(r=>r.id),['compact','hot','other']);assert.equal(r.ftsEnabled,false);assert.equal(new Set(r.rows.map(r=>r.id)).size,3);
});
for (const partial of [false,true]) test(`${partial?'partial':'total'} embedding failure reports parent without replacing any of its vectors`,async()=>{
  const mongo=mongoFixture();const text=partial?'a'.repeat(35_000):'small';let n=0;
  const result=await batchIndexTextsInMongoVectors({mongo,tier:'hot',items:[item('failed-parent',text)],embeddingFunction:{generate:async ts=>ts.map(()=>partial&&n++===0?[1,2]:[])},config:{embeddingBatchSize:1}});
  assert.equal(result.indexed,0);assert.equal(result.failed.length,1);assert.equal(result.failed[0].id,'failed-parent');assert.match(result.failed[0].error,/embedding/i);
  assert.equal(mongo.hotVectors.calls.deletes.length,0);assert.equal(mongo.hotVectors.calls.writes.length,0);
});
test('thrown embedding batch errors report each affected parent',async()=>{
  const mongo=mongoFixture();const r=await batchIndexTextsInMongoVectors({mongo,tier:'hot',items:[item('a'),item('b')],embeddingFunction:{generate:async()=>{throw new Error('transport failure');}}});
  assert.equal(r.indexed,0);assert.deepEqual(r.failed.map(f=>f.id),['a','b']);assert.ok(r.failed.every(f=>f.error.includes('transport failure')));
});
for(const text of ['  leading\r\n\ttext\n\n\nend  ','<html><body><p>Hello memory</p></body></html>'])test('normalized chunks stay inline when raw coordinates do not match',async()=>{
  const mongo=mongoFixture(); const prepared=prepareIndexDocument({parentId:'normalized',text});
  await batchIndexTextsInMongoVectors({mongo,tier:'hot',items:[item('normalized',text,{source_path:'source.txt'})],embeddingFunction:{generate:async ts=>ts.map(()=>[1,2])}});
  const doc=[...mongo.hotVectors.rows.values()][0]; assert.equal(doc.text,prepared.chunks[0].text);assert.notEqual(doc.source_text_redacted,true);
});
test('legacy normalized coordinates hydrate the matching chunk rather than a shifted raw slice',async()=>{
  const dir=await mkdtemp(join(tmpdir(),'openplanner-hydration-'));const text='  source\r\n\tcontent  ';const prepared=prepareIndexDocument({parentId:'legacy',text});const chunk=prepared.chunks[0];
  try {await writeFile(join(dir,'source.txt'),text);await withEnv({OPENPLANNER_SOURCE_ROOT:dir},async()=>{
    const doc={_id:'legacy',parent_id:'legacy',text:'',source_text_redacted:true,source_ref:{source_path:'source.txt'},char_start:chunk.charStart,char_end:chunk.charEnd,chunk_text_hash_sha256:createHash('sha256').update(chunk.text).digest('hex')};
    assert.equal(await hydrateVectorDocumentText(doc),chunk.text);
  });}finally{await rm(dir,{recursive:true,force:true});}
});
test('partition setup is reused per connection and failed setup can be retried',async()=>{
  const mongo=mongoFixture(); await Promise.all([upsertMongoVectorDocuments(mongo,'hot',[entry('a')]),upsertMongoVectorDocuments(mongo,'hot',[entry('b')])]);
  const part=[...mongo.collections.values()].find(c=>c.collectionName.includes('__'));const setups=part.calls.indexes.length;await upsertMongoVectorDocuments(mongo,'hot',[entry('c')]);assert.equal(part.calls.indexes.length,setups);assert.equal(part.calls.setups,2);
  const other=mongoFixture();other.vectorPartitions.failSetup=true;await assert.rejects(upsertMongoVectorDocuments(other,'hot',[entry()]),/temporary/);await upsertMongoVectorDocuments(other,'hot',[entry()]);assert.ok(other.hotVectors.rows.has('chunk'));
});

test('hydration-cache initialization can retry after an actual LMDB path failure',async()=>{
  const dir=await mkdtemp(join(tmpdir(),'openplanner-cache-init-'));
  const script=`import assert from 'node:assert/strict'; import {writeFile,rm} from 'node:fs/promises';
    const blocked=process.argv[1]+'/cache'; await writeFile(blocked,'not a directory');
    process.env.OPENPLANNER_HYDRATION_LMDB_PATH=blocked;
    const {getHydrationCache}=await import(process.argv[2]);
    await assert.rejects(getHydrationCache()); await rm(blocked);
    assert.ok(await getHydrationCache()); process.exit(0);`;
  try {await promisify(execFileCallback)(process.execPath,['--input-type=module','-e',script,dir,new URL('../dist/source-hydration.js',import.meta.url).href],{timeout:10_000});}
  finally {await rm(dir,{recursive:true,force:true});}
});

test('one-character overflow fails terminally instead of recursively retrying unchanged input',async()=>{
  let attempts=0;
  await withFetch(async()=>{attempts++;return new Response(attempts>5?'fixture recursion guard':'context window exceeded',{status:attempts>5?503:400});},async()=>{
    const provider=new EmbedProviderFunction('fixture','http://fixture.invalid',{batchWindowMs:1});
    await assert.rejects(provider.generate(['x']),/context window/);assert.equal(attempts,2);
  });
});

test('removing the last vector label uses the same configured retention and timestamp',async()=>{
  const mongo=mongoFixture();await withEnv({MONGODB_EVENTS_TTL_SECONDS:'0',MONGODB_COMPACTED_TTL_SECONDS:'0'},()=>removeMongoVectorParentLabel(mongo,'parent','keep'));
  for(const [collection,seconds] of [[mongo.hotVectors,60],[mongo.compactVectors,120]]){
    const update=collection.calls.writes[0].update;
    assert.equal(update[1].$set.expiresAt.$cond[2]-update[0].$set.updatedAt,seconds*1000);
  }
});
