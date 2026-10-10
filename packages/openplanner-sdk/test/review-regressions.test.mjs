// SPDX-License-Identifier: LGPL-3.0-or-later
import test, { mock } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, writeFile, readFile, rm, symlink, mkdir } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { execFile as execFileCallback } from 'node:child_process';
import { promisify } from 'node:util';
import { createHash } from 'node:crypto';
import { MongoClient } from 'mongodb';
import { createProtocols } from '../dist/protocol-adapters.js';
import { openMongoDB, ilikeSearch, reconcileManagedTtl } from '../dist/mongodb.js';
import { makeEmbeddingCacheKey, PersistentEmbeddingCache } from '../dist/embedding-cache.js';
import { safeSourceFilePath, loadHydrationSourceText } from '../dist/source-hydration.js';
import { EmbedProviderFunction } from '../dist/embeddings.js';
import { formatEmbeddingQueryText, formatEmbeddingPassageText } from '../dist/embedding-text.js';
import { ftsSearchWithQuality, vectorSearchWithQuality } from '../dist/search-core.js';
import { batchIndexTextsInMongoVectors, upsertMongoVectorDocuments, hydrateVectorDocumentText, removeMongoVectorParentLabel, indexTextInMongoVectors, queryMongoVectorsByText } from '../dist/mongo-vectors.js';
import { createOpenPlannerSdk } from '../dist/sdk.js';
import { prepareIndexDocument } from '../dist/indexing.js';
import { ingestEvents } from '../dist/ingest.js';
import { queryCollectionResponse } from '../dist/mongo-browse.js';
import { getSessionResponse } from '../dist/sessions-core.js';
import { mergeTieredVectorHits } from '../dist/vector-search.js';

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
      const key = filter._id ?? filter.tenant_id ?? filter.label_id;
      const old = rows.get(key);
      const next = {...(!old ? update.$setOnInsert : {}), ...old, ...filter, ...update.$set};
      for (const field of Object.keys(update.$unset ?? {})) delete next[field];
      rows.set(key, next); return {modifiedCount:1};
    },
    async findOneAndUpdate(filter, update) { await this.updateOne(filter,update); return this.findOne(filter); },
    async insertOne(row) { rows.set(`auto-${rows.size}`, {...row}); },
    async insertMany(items) { for (const row of items) await this.insertOne(row); },
    async findOne(filter) { return rows.get(filter._id ?? filter.tenant_id ?? filter.label_id) ?? null; },
    find(filter) {
      calls.searches.push(filter);
      if (filter.$text && this.failText) throw new Error('text index unavailable');
      return cursor([...rows.values()].filter(r => {
        const q = filter['extra.openplanner_labels.quality'];
        if (q === 'good' && r.extra?.openplanner_labels?.quality !== 'good') return false;
        if (q?.$ne === 'bad' && r.extra?.openplanner_labels?.quality === 'bad') return false;
        if (filter.id?.$nin?.includes(r.id)) return false;
        if (filter.tier && r.tier !== filter.tier) return false;
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
    graphNodeEmbeddings:db.collection('embeddings'),graphEdges:db.collection('graph_edges'),graphLabelNodes:db.collection('graph_label_nodes'),
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
for(const text of ['  leading\r\n\ttext\n\n\nend  ','<html><body><p>Hello memory</p></body></html>'])test('normalized source chunks remain references and hydrate exactly',async()=>{
  const dir=await mkdtemp(join(tmpdir(),'openplanner-normalized-ref-'));const mongo=mongoFixture();const prepared=prepareIndexDocument({parentId:'normalized',text});
  try{await writeFile(join(dir,'source.txt'),text);await withEnv({OPENPLANNER_SOURCE_ROOT:dir},async()=>{
    await batchIndexTextsInMongoVectors({mongo,tier:'hot',items:[item('normalized',text,{source_path:'source.txt'})],embeddingFunction:{generate:async ts=>ts.map(()=>[1,2])}});
    const doc=[...mongo.hotVectors.rows.values()][0];assert.equal(doc.text,'');assert.equal(doc.source_text_redacted,true);assert.equal(doc.source_coordinate_space,'normalized');assert.equal(doc.text_hash_sha256,createHash('sha256').update(text).digest('hex'));assert.equal(await hydrateVectorDocumentText(doc),prepared.chunks[0].text);
  });}finally{await rm(dir,{recursive:true,force:true});}
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
  const part=[...mongo.collections.values()].find(c=>c.collectionName.includes('__'));const setups=part.calls.indexes.length;await upsertMongoVectorDocuments(mongo,'hot',[entry('c')]);assert.equal(part.calls.indexes.length,setups);assert.equal(part.calls.setups,1);
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

test('disabled retention removes managed flat and existing partition TTLs without dropping unrelated indexes',async()=>{
  const mongo=mongoFixture();const managed=[['events','events_ttl'],['compacted','compacted_ttl'],['hot_vectors','hot_vectors_ttl'],['compact_vectors','compact_vectors_ttl'],['hot_existing','hot_vectors_ttl'],['compact_existing','compact_vectors_ttl']];
  for(const [name,index] of managed) mongo.db.collection(name).indexes=async()=>[{name:index,expireAfterSeconds:0},{name:'unrelated_ttl',expireAfterSeconds:1}];
  mongo.vectorPartitions.rows.set('hot',{tier:'hot',collectionName:'hot_existing'});
  mongo.vectorPartitions.rows.set('compact',{tier:'compact',collectionName:'compact_existing'});
  const connect=mock.method(MongoClient.prototype,'connect',async function(){return this;});const db=mock.method(MongoClient.prototype,'db',()=>mongo.db);
  try{
    await openMongoDB({uri:'mongodb://fixture.invalid',dbName:'fixture',eventsCollection:'events',compactedCollection:'compacted',vectorHotCollection:'hot_vectors',vectorCompactCollection:'compact_vectors',graphLayoutCollection:'layout',graphNodeEmbeddingCollection:'embeddings',eventsTtlSeconds:0,compactedTtlSeconds:0});
    for(const [name,index] of managed){const c=mongo.db.collection(name);assert.deepEqual(c.calls.drops,[index],name);assert.ok(c.calls.writes.some(w=>w.update.$unset?.expiresAt===''),name);}
  }finally{connect.mock.restore();db.mock.restore();}
});
test('opening the SDK preserves customized default tenant and policy settings',async()=>{
  const mongo=mongoFixture();const tenant={tenant_id:'knoxx-session',name:'Personal creator',status:'inactive',domains:['kept.invalid']};const policy={tenant_id:'knoxx-session',retention_days:3,pii_rules:{reject:true},rate_limits:{tokens_per_day:1}};
  mongo.db.collection('tenants').rows.set('knoxx-session',tenant);mongo.db.collection('tenant_policies').rows.set('knoxx-session',policy);
  const connect=mock.method(MongoClient.prototype,'connect',async function(){return this;});const db=mock.method(MongoClient.prototype,'db',()=>mongo.db);
  try{await openMongoDB({uri:'mongodb://fixture.invalid',dbName:'fixture',eventsCollection:'events',compactedCollection:'compacted',vectorHotCollection:'hot_vectors',vectorCompactCollection:'compact_vectors',graphLayoutCollection:'layout',graphNodeEmbeddingCollection:'embeddings'});await new Promise(resolve=>setImmediate(resolve));
    assert.deepEqual(mongo.db.collection('tenants').rows.get('knoxx-session'),tenant);assert.deepEqual(mongo.db.collection('tenant_policies').rows.get('knoxx-session'),policy);
  }finally{connect.mock.restore();db.mock.restore();}
});
test('tenant updates cannot change storage identity, tenant ID or creation time',async()=>{
  const mongo=mongoFixture();const tenants=mongo.db.collection('tenants');tenants.rows.set('tenant-a',{_id:'stored-id',tenant_id:'tenant-a',created_at:'original'});
  const result=await createProtocols({mongo}).tenantManagement.updateTenant('tenant-a',{_id:'foreign-id',tenant_id:'tenant-b',created_at:'replaced',name:'new name'});
  assert.equal(result._id,'stored-id');assert.equal(result.tenant_id,'tenant-a');assert.equal(result.created_at,'original');assert.equal(result.name,'new name');
});
test('invalid later timestamp prevents all ingest effects including graph projections',async()=>{
  const mongo=mongoFixture();let embedded=0;const event={schema:'openplanner.event.v1',id:'valid',ts:'2026-01-01T00:00:00Z',source:'fixture',kind:'message',text:'memory',extra:{openplanner_labels:{labels:['keep']}}};
  const embeddingRuntime={hot:{getBackgroundEmbeddingFunction(){embedded++;return {generate:async texts=>texts.map(()=>[1,2])};},getModel(){return 'fixture';}}};
  await assert.rejects(ingestEvents({mongo,embeddingRuntime},[event,{...event,id:'invalid',ts:'not-a-date'}]),/ts|timestamp|Invalid time/i);
  assert.equal([...mongo.collections.values()].reduce((n,c)=>n+c.calls.writes.length,0),0);assert.equal(embedded,0);
});
test('label nodes distinguish punctuation, Unicode and long common prefixes and keep replay IDs',async()=>{
  const mongo=mongoFixture();const labels=['C++','C#','!!!','???','海','空','a'.repeat(100)+'one','a'.repeat(100)+'two'];const event={schema:'openplanner.event.v1',id:'labeled',ts:'2026-01-01T00:00:00Z',source:'fixture',kind:'graph.edge',extra:{openplanner_labels:{labels}}};
  await ingestEvents({mongo,embeddingRuntime:{}},[event]);const rows=[...mongo.db.collection('graph_label_nodes').rows.values()];assert.equal(rows.length,labels.length);assert.deepEqual(rows.map(r=>r.label),labels);const ids=rows.map(r=>r.label_id);assert.equal(new Set(ids).size,labels.length);
  await ingestEvents({mongo,embeddingRuntime:{}},[event]);assert.deepEqual([...mongo.db.collection('graph_label_nodes').rows.values()].map(r=>r.label_id),ids);
});
test('embedding cache flush is durable and invalidations survive a new process',async()=>{
  const dir=await mkdtemp(join(tmpdir(),'openplanner-persistent-'));const path=join(dir,'nested','embeddings.json');
  try{const cache=new PersistentEmbeddingCache(path);await cache.putMany([{key:'kept',vector:[1,2]},{key:'removed',vector:[3,4]}]);await cache.flush();assert.ok((await readFile(path,'utf8')).length>0);
    const script=`import assert from 'node:assert/strict';const {PersistentEmbeddingCache}=await import(process.argv[2]);const cache=new PersistentEmbeddingCache(process.argv[1]);assert.deepEqual((await cache.getMany(['kept'])).get('kept'),[1,2]);cache.delete('removed');await cache.flush();`;
    await promisify(execFileCallback)(process.execPath,['--input-type=module','-e',script,path,new URL('../dist/embedding-cache.js',import.meta.url).href],{timeout:10_000});
    const restored=new PersistentEmbeddingCache(path);assert.equal(restored.has('removed'),false);assert.equal(restored.has('kept'),true);
  }finally{await rm(dir,{recursive:true,force:true});}
});
test('absolute in-root hydration paths remain absolute while escape paths are rejected',async()=>{
  await withEnv({OPENPLANNER_SOURCE_ROOT:'/owned/source'},async()=>{
    assert.equal(safeSourceFilePath({extra:{source_path:'/owned/source/a.md'}}),'/owned/source/a.md');assert.equal(safeSourceFilePath({extra:{source_path:'a.md'}}),'/owned/source/a.md');assert.equal(safeSourceFilePath({extra:{source_path:'/foreign/a.md'}}),undefined);assert.equal(safeSourceFilePath({extra:{source_path:'../escape.md'}}),undefined);
  });
});
for(const extra of [{url:'https://outside.invalid/a'},{hostname:'outside.invalid'}])test('URL-only sources keep vector text inline',async()=>{
  const mongo=mongoFixture();const result=await batchIndexTextsInMongoVectors({mongo,tier:'hot',items:[item('url-only','outside content',extra)],embeddingFunction:{generate:async texts=>texts.map(()=>[1,2])}});
  assert.equal(result.indexed,1);const doc=[...mongo.hotVectors.rows.values()][0];assert.equal(doc.text,'outside content');assert.notEqual(doc.source_text_redacted,true);
});
for(const batch of [false,true])test(`${batch?'batch':'single'} vector indexing formats provider passages but keeps stored chunk text`,async()=>{
  const mongo=mongoFixture();const texts=[];await withEnv({EMBED_PASSAGE_TEMPLATE:'passage: {text}'},async()=>{
    const params={mongo,tier:'hot',embeddingFunction:{generate:async inputs=>{texts.push(...inputs);return inputs.map(()=>[1,2]);}}};
    if(batch)await batchIndexTextsInMongoVectors({...params,items:[item('formatted','outside content')]});else{const {indexTextInMongoVectors}=await import('../dist/mongo-vectors.js');await indexTextInMongoVectors({...params,parentId:'formatted',text:'outside content',metadata:{embedding_model:'fixture'}});}
  });assert.deepEqual(texts,['passage: outside content']);assert.equal([...mongo.hotVectors.rows.values()][0].text,'outside content');
});
test('background indexing cannot settle after a timeout while its underlying task can still write',async()=>{
  const mongo=mongoFixture();let release;let entered;const started=new Promise(resolve=>{entered=resolve;});const held=new Promise(resolve=>{release=resolve;});let settled=false;let warned=0;
  const embeddingRuntime={hot:{getModel(){return 'fixture';},getBackgroundEmbeddingFunction(){return {generate:async()=>{entered();await held;return [[1,2]];}};},getBackgroundEmbeddingFunctionForModel(){return {generate:async texts=>texts.map(()=>[1,2])};}}};
  const originalTimer=globalThis.setTimeout;
  const timer=mock.method(globalThis,'setTimeout',(fn,ms,...args)=>originalTimer(fn,ms===30_000?5:ms,...args));
  try{const result=await ingestEvents({mongo,embeddingRuntime,log:{warn(){warned++;}}},[{schema:'openplanner.event.v1',id:'slow',ts:'2026-01-01T00:00:00Z',source:'fixture',kind:'message',text:'memory'}]);result.backgroundIndexing.then(()=>{settled=true;});await started;await new Promise(resolve=>originalTimer(resolve,25));const premature=settled;release();await result.backgroundIndexing;assert.equal(premature,false);assert.ok(warned>0);assert.ok(mongo.hotVectors.calls.writes.length>0);
  }finally{release?.();timer.mock.restore();}
});

test('ordinary graph embedding persistence binds the exact untrimmed Unicode source text',async()=>{
  const mongo=mongoFixture();let calls=0;
  const provider={generate:async texts=>{calls++;return texts.map(()=>[1,2]);}};
  const embeddingRuntime={hot:{getModel(){return 'fixture';},getBackgroundEmbeddingFunction(){return provider;},getBackgroundEmbeddingFunctionForModel(){return provider;}}};
  const event={schema:'openplanner.event.v1',id:'bound-text',ts:'2026-01-01T00:00:00Z',source:'fixture',kind:'message',text:'  海 memory  '};
  const first=await ingestEvents({mongo,embeddingRuntime},[event]);await first.backgroundIndexing;
  const row=[...mongo.graphNodeEmbeddings.rows.values()][0];assert.equal(row.source_text_hash_sha256,createHash('sha256').update(event.text,'utf8').digest('hex'));assert.ok(row.text.includes('海 memory'));
  const previous=calls;const next={...event,text:'海 memory'};const second=await ingestEvents({mongo,embeddingRuntime},[next]);await second.backgroundIndexing;
  assert.ok(calls>previous);assert.equal([...mongo.graphNodeEmbeddings.rows.values()][0].source_text_hash_sha256,createHash('sha256').update(next.text,'utf8').digest('hex'));
});

test('explicit graph-node embeddings bind the authoritative raw event text',async()=>{
  const mongo=mongoFixture();const provider={generate:async texts=>texts.map(()=>[1,2])};
  const embeddingRuntime={hot:{getModel(){return 'fixture';},getBackgroundEmbeddingFunctionForModel(){return provider;}}};
  const event={schema:'openplanner.event.v1',id:'explicit-node',ts:'2026-01-01T00:00:00Z',source:'fixture',kind:'graph.node',text:'  <p>海 memory</p>  ',extra:{node_id:'explicit-node'}};
  const result=await ingestEvents({mongo,embeddingRuntime},[event]);await result.backgroundIndexing;
  assert.equal([...mongo.graphNodeEmbeddings.rows.values()][0].source_text_hash_sha256,createHash('sha256').update(event.text,'utf8').digest('hex'));
});
test('session updates retain immutable identity and creation time',async()=>{
  const mongo=mongoFixture();const original={_id:'stored',session:'session-a',kind:'session',createdAt:'original'};let stored={...original};
  mongo.events.updateOne=async(filter,update)=>{assert.deepEqual(filter,{session:'session-a',kind:'session'});Object.assign(stored,update.$set);};
  mongo.events.findOne=async filter=>stored.session===filter.session&&stored.kind===filter.kind?stored:null;
  const result=await createProtocols({mongo}).sessionManagement.updateSession('session-a',{_id:'foreign',session:'session-b',kind:'message',createdAt:'replaced',title:'changed'});
  assert.deepEqual({...result,updatedAt:undefined},{...original,title:'changed',updatedAt:undefined});
});
test('event replacement removes only its previously projected edges',async()=>{
  const mongo=mongoFixture();const edges=mongo.graphEdges;
  edges.deleteMany=async filter=>{edges.calls.deletes.push(filter);for(const [id,row] of edges.rows)if(filter['data.source_event_id']?.$in?.includes(row.data?.source_event_id))edges.rows.delete(id);};
  const event={schema:'openplanner.event.v1',id:'edge-source',ts:'2026-01-01T00:00:00Z',source:'fixture',kind:'graph.edge',extra:{source_node_id:'a',target_node_id:'b',edge_kind:'related',openplanner_labels:{labels:['old']}}};
  await ingestEvents({mongo,embeddingRuntime:{}},[event]);
  edges.rows.set('unrelated',{source_node_id:'x',target_node_id:'y',data:{source_event_id:'another-source'}});
  await ingestEvents({mongo,embeddingRuntime:{}},[{...event,extra:{source_node_id:'a',target_node_id:'c',edge_kind:'related'}}]);
  const owned=[...edges.rows.values()].filter(r=>r.data?.source_event_id==='edge-source');
  assert.equal(owned.length,1);assert.equal(owned[0].target_node_id,'c');assert.ok(edges.rows.has('unrelated'));assert.equal([...edges.rows.values()].some(r=>r.target_node_id==='b'||r.edge_kind==='has_label'),false);
  await ingestEvents({mongo,embeddingRuntime:{}},[{...event,kind:'message',text:'',extra:{}}]);
  assert.equal([...edges.rows.values()].filter(r=>r.data?.source_event_id==='edge-source').length,0);
});
test('two source events with equal endpoints retain independent projection ownership',async()=>{
  const mongo=mongoFixture();const edges=mongo.graphEdges;
  edges.deleteMany=async filter=>{for(const [id,row] of edges.rows)if(filter['data.source_event_id']?.$in?.includes(row.data?.source_event_id))edges.rows.delete(id);};
  const event={schema:'openplanner.event.v1',id:'owner-a',ts:'2026-01-01T00:00:00Z',source:'fixture',kind:'graph.edge',extra:{source_node_id:'a',target_node_id:'b',edge_kind:'related'}};
  await ingestEvents({mongo,embeddingRuntime:{}},[event,{...event,id:'owner-b'}]);
  assert.equal(edges.rows.size,2);
  await ingestEvents({mongo,embeddingRuntime:{}},[{...event,extra:{source_node_id:'a',target_node_id:'c',edge_kind:'related'}}]);
  assert.equal(edges.rows.size,2);assert.equal([...edges.rows.values()].find(r=>r.data?.source_event_id==='owner-b').target_node_id,'b');
});
test('replacement refuses unattributed legacy edge ownership before event mutation',async()=>{
  const mongo=mongoFixture();const previous={_id:'legacy',id:'legacy',kind:'graph.edge',extra:{source_node_id:'a',target_node_id:'b',edge_kind:'related'}};
  mongo.events.rows.set('legacy',previous);mongo.graphEdges.rows.set('a||b||related',{source_node_id:'a',target_node_id:'b',edge_kind:'related',data:{}});
  await assert.rejects(ingestEvents({mongo,embeddingRuntime:{}},[{schema:'openplanner.event.v1',id:'legacy',ts:'2026-01-01T00:00:00Z',source:'fixture',kind:'graph.edge',extra:{source_node_id:'a',target_node_id:'c',edge_kind:'related'}}]),/projection ownership/i);
  assert.equal(mongo.events.calls.writes.length,0);assert.deepEqual(mongo.events.rows.get('legacy'),previous);assert.equal(mongo.graphEdges.calls.deletes.length,0);
});
test('conflicting replacements for one event in a batch refuse before any effects',async()=>{
  const mongo=mongoFixture();const event={schema:'openplanner.event.v1',id:'conflict',ts:'2026-01-01T00:00:00Z',source:'fixture',kind:'graph.edge',extra:{source_node_id:'a',target_node_id:'b',edge_kind:'related'}};
  await assert.rejects(ingestEvents({mongo,embeddingRuntime:{}},[event,{...event,extra:{source_node_id:'a',target_node_id:'c',edge_kind:'related'}}]),/conflicting event replacements/i);
  assert.equal(mongo.events.calls.writes.length,0);assert.equal(mongo.graphEdges.calls.deletes.length,0);assert.equal(mongo.graphEdges.calls.writes.length,0);
});
test('embedding uniqueness admits separate chunks and replaces only the obsolete owned index',async()=>{
  const mongo=mongoFixture();const c=mongo.graphNodeEmbeddings;
  c.indexes=async()=>[{name:'node_id_1_embedding_model_1_embedding_dimensions_1',key:{node_id:1,embedding_model:1,embedding_dimensions:1},unique:true},{name:'unrelated_unique',key:{project:1},unique:true}];
  const connect=mock.method(MongoClient.prototype,'connect',async function(){return this;});const db=mock.method(MongoClient.prototype,'db',()=>mongo.db);
  try{await openMongoDB({uri:'mongodb://fixture.invalid',dbName:'fixture',eventsCollection:'events',compactedCollection:'compacted',vectorHotCollection:'hot_vectors',vectorCompactCollection:'compact_vectors',graphLayoutCollection:'layout',graphNodeEmbeddingCollection:'embeddings'});
    assert.ok(c.calls.indexes.some(i=>i.opts.unique&&i.keys.chunk_index===1));
    assert.deepEqual(c.calls.drops,['node_id_1_embedding_model_1_embedding_dimensions_1']);
  }finally{connect.mock.restore();db.mock.restore();}
});
test('unknown session detail modes refuse before reading history',async()=>{
  const mongo=mongoFixture();let reads=0;mongo.events.find=()=>{reads++;throw new Error('history should not be read');};
  for(const mode of ['visiblity','',{},42])await assert.rejects(getSessionResponse({mongo},'session-a',{mode}),/mode.*full.*resume.*visibility/i);
  assert.equal(reads,0);
});
test('derived graph-node event content follows resolved retention and label exemptions',async()=>{
  const mongo=mongoFixture();const provider={generate:async texts=>texts.map(()=>[1,2])};const embeddingRuntime={hot:{getModel(){return 'fixture';},getBackgroundEmbeddingFunction(){return provider;},getBackgroundEmbeddingFunctionForModel(){return provider;}}};
  const event={schema:'openplanner.event.v1',id:'retained-source',ts:'2026-01-01T00:00:00Z',source:'fixture',kind:'message',text:'outside retained content'};
  const first=await ingestEvents({mongo,embeddingRuntime},[event]);await first.backgroundIndexing;
  const derived=mongo.events.rows.get('graph.node:derive:retained-source');assert.ok(derived.expiresAt instanceof Date);assert.equal(derived.expiresAt-derived.updatedAt,60_000);
  const second=await ingestEvents({mongo,embeddingRuntime},[{...event,extra:{openplanner_labels:{labels:['keep']}}}]);await second.backgroundIndexing;
  assert.equal(mongo.events.rows.get('graph.node:derive:retained-source').expiresAt,undefined);
});
test('RRF contributes one vote per parent per tier while retaining its best displayed chunk',()=>{
  const hit=(id,parent,rank,distance,tier='hot')=>({id,tier,rank,distance,document:id,metadata:{parent_id:parent}});
  const result=mergeTieredVectorHits([[hit('a1','a',0,.4),hit('a2','a',1,.1),hit('b1','b',2,.2)],[hit('b2','b',0,.2,'compact')]],2);
  assert.deepEqual(result.ids,[['b','a']]);assert.equal(result.metadatas[0][1].best_match_id,'a2');assert.equal(result.metadatas[0][1].rrf_score,Number((1/61).toFixed(8)));
});
test('derived sentence and chunk events inherit the owning source retention labels',async()=>{
  const provider={generate:async texts=>texts.map(()=>[1,2])};
  const embeddingRuntime={hot:{getModel(){return 'fixture';},getBackgroundEmbeddingFunctionForModel(){return provider;}}};
  for(const text of ['Outside information changes remembered creative choices.', 'Outside information changes remembered creative choices. '.repeat(4000)]){
    const mongo=mongoFixture();
    const event={schema:'openplanner.event.v1',id:'retention-owner',ts:'2026-01-01T00:00:00Z',source:'fixture',kind:'graph.node',text,extra:{node_id:'retention-owner',openplanner_labels:{labels:['keep']}}};
    const result=await ingestEvents({mongo,embeddingRuntime},[event]);await result.backgroundIndexing;
    const derived=[...mongo.events.rows.values()].filter(row=>row.source==='openplanner-derive');
    assert.ok(derived.some(row=>row.extra.node_kind==='sentence'));
    if(text.length>180000)assert.ok(derived.some(row=>row.extra.node_kind==='doc_chunk'));
    for(const row of derived){assert.deepEqual(row.extra.openplanner_labels?.labels,['keep']);assert.equal(row.expiresAt,undefined);}
  }
});
test('explicit graph-node chunk and sentence embeddings bind their authoritative raw source',async()=>{
  const mongo=mongoFixture();const provider={generate:async texts=>texts.map(()=>[1,2])};
  const embeddingRuntime={hot:{getModel(){return 'fixture';},getBackgroundEmbeddingFunctionForModel(){return provider;}}};
  const event={schema:'openplanner.event.v1',id:'chunk-source',ts:'2026-01-01T00:00:00Z',source:'fixture',kind:'graph.node',text:'Outside information changes remembered creative choices. '.repeat(4000),extra:{node_id:'chunk-source'}};
  const result=await ingestEvents({mongo,embeddingRuntime},[event]);await result.backgroundIndexing;
  const rows=[...mongo.graphNodeEmbeddings.rows.values()];assert.ok(rows.length>1);
  for(const row of rows){assert.equal(row.source_event_id,event.id);assert.equal(row.source_text_hash_sha256,createHash('sha256').update(event.text,'utf8').digest('hex'));}
});

function ownedProjectionFixture() {
  const mongo=mongoFixture();
  const matches=(row,filter)=>Object.entries(filter).every(([key,value])=>{
    const actual=key.split('.').reduce((object,part)=>object?.[part],row);
    if(value&&typeof value==='object'){
      if('$in' in value)return value.$in.includes(actual);
      if('$nin' in value)return !value.$nin.includes(actual);
      if('$ne' in value)return actual!==value.$ne;
    }
    return actual===value;
  });
  for(const c of [mongo.events,mongo.graphNodeEmbeddings]){
    c.deleteMany=async filter=>{c.calls.deletes.push(filter);for(const [key,row] of c.rows)if(matches(row,filter))c.rows.delete(key);};
    c.updateMany=async(filter,update)=>{c.calls.writes.push({filter,update});for(const row of c.rows.values())if(matches(row,filter))Object.assign(row,update.$set);};
  }
  return mongo;
}
test('malformed graph-node text refuses the complete batch before admission',async()=>{
  for(const text of [42,false,{},null]){
    const mongo=mongoFixture();const valid={schema:'openplanner.event.v1',id:'first',ts:'2026-01-01T00:00:00Z',source:'fixture',kind:'graph.edge',extra:{source_node_id:'a',target_node_id:'b',edge_kind:'related'}};
    await assert.rejects(ingestEvents({mongo,embeddingRuntime:{}},[valid,{...valid,id:'invalid',kind:'graph.node',text,extra:{node_id:'invalid'}}]),/graph.node.*text.*string/i);
    assert.equal(mongo.events.calls.writes.length,0);assert.equal(mongo.graphEdges.calls.deletes.length,0);
  }
});
test('empty replacement removes owned derived content and embeddings without TTL',async()=>{
  const mongo=ownedProjectionFixture();mongo.retention.eventsTtlSeconds=0;
  const provider={generate:async texts=>texts.map(()=>[1,2])};const embeddingRuntime={hot:{getModel(){return 'fixture';},getBackgroundEmbeddingFunction(){return provider;},getBackgroundEmbeddingFunctionForModel(){return provider;}}};
  const event={schema:'openplanner.event.v1',id:'content-owner',ts:'2026-01-01T00:00:00Z',source:'fixture',kind:'message',text:'Outside content retained by its source.'};
  const first=await ingestEvents({mongo,embeddingRuntime},[event]);await first.backgroundIndexing;
  assert.ok(mongo.events.rows.has('graph.node:derive:content-owner'));assert.ok([...mongo.graphNodeEmbeddings.rows.values()].some(row=>row.source_event_id===event.id));
  mongo.events.rows.set('foreign-derived',{id:'foreign-derived',source:'openplanner-derive',extra:{source_event_id:'different-owner'}});
  mongo.graphNodeEmbeddings.rows.set('foreign-index',{node_id:'foreign-index',source_event_id:'different-owner'});
  const replacement=await ingestEvents({mongo,embeddingRuntime},[{...event,text:''}]);await replacement.backgroundIndexing;
  assert.equal(mongo.events.rows.has('graph.node:derive:content-owner'),false);assert.equal([...mongo.graphNodeEmbeddings.rows.values()].some(row=>row.source_event_id===event.id),false);
  assert.ok(mongo.events.rows.has('content-owner'));assert.ok(mongo.events.rows.has('foreign-derived'));assert.ok(mongo.graphNodeEmbeddings.rows.has('foreign-index'));
});
test('changed graph-node text reconciles obsolete owned chunks and sentences',async()=>{
  const mongo=ownedProjectionFixture();const provider={generate:async texts=>texts.map(()=>[1,2])};const embeddingRuntime={hot:{getModel(){return 'fixture';},getBackgroundEmbeddingFunctionForModel(){return provider;}}};
  const event={schema:'openplanner.event.v1',id:'changed-owner',ts:'2026-01-01T00:00:00Z',source:'fixture',kind:'graph.node',text:'Outside information changes remembered creative choices. '.repeat(4000),extra:{node_id:'changed-owner'}};
  const first=await ingestEvents({mongo,embeddingRuntime},[event]);await first.backgroundIndexing;
  const oldDerived=[...mongo.events.rows.values()].filter(row=>row.source==='openplanner-derive');assert.ok(oldDerived.some(row=>row.extra.node_kind==='doc_chunk'));assert.ok(oldDerived.some(row=>row.extra.node_kind==='sentence'));
  const oldIds=oldDerived.map(row=>row.id);const oldNodes=[...mongo.graphNodeEmbeddings.rows.values()].map(row=>row.node_id);
  const next=await ingestEvents({mongo,embeddingRuntime},[{...event,text:'A distinct later encounter reshapes this memory.'}]);await next.backgroundIndexing;
  for(const id of oldIds)assert.equal(mongo.events.rows.has(id),false);
  for(const node of oldNodes)assert.equal([...mongo.graphNodeEmbeddings.rows.values()].some(row=>row.node_id===node),false);
  const derived=[...mongo.events.rows.values()].filter(row=>row.source==='openplanner-derive');assert.ok(derived.length>0);assert.ok(derived.every(row=>row.extra.source_event_id===event.id));
});
test('equal graph-node text refreshes source and project without regenerating vectors',async()=>{
  const mongo=ownedProjectionFixture();let calls=0;const provider={generate:async texts=>{calls++;return texts.map(()=>[1,2]);}};const embeddingRuntime={hot:{getModel(){return 'fixture';},getBackgroundEmbeddingFunctionForModel(){return provider;}}};
  const event={schema:'openplanner.event.v1',id:'source-a',ts:'2026-01-01T00:00:00Z',source:'fixture',kind:'graph.node',text:'shared text',source_ref:{project:'first'},extra:{node_id:'shared-node'}};
  const first=await ingestEvents({mongo,embeddingRuntime},[event]);await first.backgroundIndexing;const previousCalls=calls;
  const second=await ingestEvents({mongo,embeddingRuntime},[{...event,id:'source-b',source_ref:{project:'second'}}]);await second.backgroundIndexing;
  const row=[...mongo.graphNodeEmbeddings.rows.values()].find(row=>row.node_id==='shared-node');assert.equal(row.source_event_id,'source-b');assert.equal(row.project,'second');assert.deepEqual(row.embedding,[1,2]);assert.equal(calls,previousCalls);
});

// Native Codex review 5479596524: actual public operations and owned collection fixtures.
const openingConfig = {uri:'mongodb://fixture.invalid',dbName:'fixture',eventsCollection:'events',compactedCollection:'compacted',vectorHotCollection:'hot_vectors',vectorCompactCollection:'compact_vectors',graphLayoutCollection:'layout',graphNodeEmbeddingCollection:'embeddings'};
test('edge uniqueness includes source ownership and migrates only the exact legacy SDK index',async()=>{
  const mongo=mongoFixture();const c=mongo.graphEdges;
  c.indexes=async()=>[{name:'source_node_id_1_target_node_id_1_edge_kind_1',key:{source_node_id:1,target_node_id:1,edge_kind:1},unique:true},{name:'unrelated_unique',key:{project:1},unique:true}];
  const connect=mock.method(MongoClient.prototype,'connect',async function(){return this;});const db=mock.method(MongoClient.prototype,'db',()=>mongo.db);
  try{await openMongoDB(openingConfig);
    assert.ok(c.calls.indexes.some(i=>i.opts.unique&&i.keys['data.source_event_id']===1));
    assert.deepEqual(c.calls.drops,['source_node_id_1_target_node_id_1_edge_kind_1']);
  }finally{connect.mock.restore();db.mock.restore();}
});
test('arbitrary source metadata cannot replace derived identity or content',async()=>{
  const mongo=mongoFixture();const provider={generate:async texts=>texts.map(()=>[1,2])};const embeddingRuntime={hot:{getModel(){return 'fixture';},getBackgroundEmbeddingFunction(){return provider;},getBackgroundEmbeddingFunctionForModel(){return provider;}}};
  const text='Authoritative outside information.';const event={schema:'openplanner.event.v1',id:'reserved-owner',ts:'2026-01-01T00:00:00Z',source:'fixture',kind:'message',text,source_ref:{project:'owned-project'},extra:{node_id:'spoof',node_kind:'spoof',preview:'spoof',label:'spoof',lake:'spoof',content_hash:'spoof',source_event_id:'spoof',custom:'retained'}};
  const result=await ingestEvents({mongo,embeddingRuntime},[event]);await result.backgroundIndexing;
  const row=mongo.events.rows.get('graph.node:derive:reserved-owner');assert.equal(row.extra.node_id,event.id);assert.equal(row.extra.node_kind,'message');assert.equal(row.extra.preview,text);assert.equal(row.extra.lake,'owned-project');assert.notEqual(row.extra.content_hash,'spoof');assert.equal(row.extra.source_event_id,event.id);assert.equal(row.extra.custom,'retained');
});
test('equal sentences from different sources keep independent retention and provenance in either order',async()=>{
  const provider={generate:async texts=>texts.map(()=>[1,2])};const embeddingRuntime={hot:{getModel(){return 'fixture';},getBackgroundEmbeddingFunctionForModel(){return provider;}}};
  for(const reversed of [false,true]){
    const mongo=ownedProjectionFixture();const event={schema:'openplanner.event.v1',ts:'2026-01-01T00:00:00Z',source:'fixture',kind:'graph.node',text:'Outside information changes remembered creative choices.',source_ref:{project:'same-project'}};
    const rows=[{...event,id:'ordinary-owner',extra:{node_id:'ordinary-owner'}},{...event,id:'retained-owner',extra:{node_id:'retained-owner',openplanner_labels:{labels:['keep']}}}];
    const result=await ingestEvents({mongo,embeddingRuntime},reversed?rows.reverse():rows);await result.backgroundIndexing;
    const sentences=[...mongo.events.rows.values()].filter(row=>row.extra?.node_kind==='sentence');assert.equal(sentences.length,2);
    const ordinary=sentences.find(row=>row.extra.source_event_id==='ordinary-owner');const retained=sentences.find(row=>row.extra.source_event_id==='retained-owner');assert.ok(ordinary.expiresAt instanceof Date);assert.equal(retained.expiresAt,undefined);assert.notEqual(ordinary.extra.node_id,retained.extra.node_id);
  }
});
test('Atlas setup errors remain retryable on the same connection',async()=>{
  const mongo=mongoFixture();const original=mongo.db.collection;let attempts=0;
  mongo.db.collection=function(name){const c=original.call(this,name);if(name.includes('__'))c.listSearchIndexes=()=>({toArray:async()=>{attempts++;if(attempts===1)throw new Error('transient Atlas failure');return [{status:'READY',queryable:true}];}});return c;};
  const clock=mock.method(Date,'now',()=>1000);
  try{await upsertMongoVectorDocuments(mongo,'hot',[entry('first')]);
    assert.equal([...mongo.vectorPartitions.rows.values()][0].searchIndexStatus,'error');assert.ok(mongo.hotVectors.rows.has('first'));
    clock.mock.mockImplementation(()=>6001);
    await upsertMongoVectorDocuments(mongo,'hot',[entry('retry')]);assert.equal([...mongo.vectorPartitions.rows.values()][0].searchIndexStatus,'ready');assert.ok(attempts>1);
  }finally{clock.mock.restore();}
});
test('invalid embedding batch sizes refuse before provider or database work',async()=>{
  const script=`import assert from 'node:assert/strict';const {batchIndexTextsInMongoVectors}=await import(process.argv[1]);
    for(const value of [0,-1,0.5,NaN,Infinity]){let calls=0;
      await assert.rejects(batchIndexTextsInMongoVectors({mongo:{},tier:'hot',items:[{id:'invalid',text:'outside information',metadata:{embedding_model:'fixture'}}],embeddingFunction:{generate:async()=>{calls++;throw new Error('provider must not be reached');}},config:{embeddingBatchSize:value}}),/embeddingBatchSize.*positive integer/i);
      assert.equal(calls,0);}`;
  await promisify(execFileCallback)(process.execPath,['--input-type=module','-e',script,new URL('../dist/mongo-vectors.js',import.meta.url).href],{timeout:3000});
});
test('embedding flush scheduling drains every queued completion under saturation',async()=>{
  let release;const gate=new Promise(resolve=>release=resolve);let started;const admitted=new Promise(resolve=>started=resolve);
  await withFetch(async request=>{started();await gate;return response([[1,2]]);},async()=>{
    const provider=new EmbedProviderFunction('fixture','http://fixture.invalid',{maxConcurrentBatches:1,maxBatchItems:1});
    const result=provider.generate(['one','two','three','four','five']);await admitted;release();assert.equal((await result).length,5);
    // Exercise the compiled scheduler completion under saturation, including queued no-op callbacks.
    const completions=Array.from({length:5},()=>provider.scheduleFlush());
    const all=Promise.all(completions).then(()=>true);
    assert.equal(await Promise.race([all,new Promise(resolve=>setTimeout(()=>resolve(false),100))]),true);
  });
});
test('hydration refuses file and directory symlinks escaping the configured root',async()=>{
  const dir=await mkdtemp(join(tmpdir(),'openplanner-symlink-'));const root=join(dir,'root');await mkdir(root);await writeFile(join(dir,'secret.txt'),'outside secret');await symlink(join(dir,'secret.txt'),join(root,'linked-file'));await symlink(dir,join(root,'linked-dir'));await writeFile(join(root,'inside.txt'),'inside content');
  try{await withEnv({OPENPLANNER_SOURCE_ROOT:root},async()=>{
    assert.equal(await loadHydrationSourceText({id:'file-link',extra:{source_path:'linked-file'}}),null);
    assert.equal(await loadHydrationSourceText({id:'dir-link',extra:{source_path:'linked-dir/secret.txt'}}),null);
    assert.equal(await loadHydrationSourceText({id:'inside',extra:{source_path:'inside.txt'}}),'inside content');
  });}finally{await rm(dir,{recursive:true,force:true});}
});
test('owning hydration cache exposes idempotent LMDB close and can reopen persisted entries',async()=>{
  const dir=await mkdtemp(join(tmpdir(),'openplanner-close-hydration-'));
  const script=`import assert from 'node:assert/strict';process.env.OPENPLANNER_HYDRATION_LMDB_PATH=process.argv[1];
    const h=await import(process.argv[2]);const {cachePut,cacheGet}=await import('@open-hax/openplanner-document-hydration');
    assert.equal(typeof h.closeHydrationCache,'function');const first=await h.getHydrationCache();await cachePut(first,'persisted','kept');
    await Promise.all([h.closeHydrationCache(),h.closeHydrationCache()]);const second=await h.getHydrationCache();assert.notEqual(first,second);assert.equal(await cacheGet(second,'persisted'),'kept');await h.closeHydrationCache();`;
  try{await promisify(execFileCallback)(process.execPath,['--input-type=module','-e',script,dir,new URL('../dist/source-hydration.js',import.meta.url).href],{timeout:10_000});}finally{await rm(dir,{recursive:true,force:true});}
});
test('failed Mongo initialization closes its owned client and preserves the original error',async()=>{
  const mongo=mongoFixture();const original=new Error('index initialization failed');mongo.events.createIndex=async()=>{throw original;};let closes=0;
  const connect=mock.method(MongoClient.prototype,'connect',async function(){return this;});const db=mock.method(MongoClient.prototype,'db',()=>mongo.db);const close=mock.method(MongoClient.prototype,'close',async()=>{closes++;});
  try{await assert.rejects(openMongoDB(openingConfig),error=>error===original);assert.equal(closes,1);}finally{connect.mock.restore();db.mock.restore();close.mock.restore();}
});

// Native Codex review 5479630941 on e3: retained source hashes and public SDK behavior.
function vectorDeletionFixture(){
  const mongo=ownedProjectionFixture();const original=mongo.db.collection;
  mongo.db.collection=function(name){const c=original.call(this,name);c.deleteMany=async filter=>{c.calls.deletes.push(filter);for(const [id,row] of c.rows)if(row.parent_id===filter.parent_id)c.rows.delete(id);};return c;};
  mongo.db.collection('hot_vectors');return mongo;
}
const ordinaryEvent=(id,text)=>({schema:'openplanner.event.v1',id,ts:'2026-01-01T00:00:00Z',source:'fixture',kind:'message',text,extra:{openplanner_labels:{labels:['keep']}}});
const eventRuntime=provider=>({hot:{getModel(){return 'fixture';},getBackgroundEmbeddingFunction(){return provider;},getBackgroundEmbeddingFunctionForModel(){return provider;}}});
test('non-indexable source replacements remove all owned hot-vector copies without TTL',async()=>{
  for(const kind of ['message','graph.node','graph.edge']){
    const mongo=vectorDeletionFixture();mongo.retention.eventsTtlSeconds=0;const embeddingRuntime=eventRuntime({generate:async texts=>texts.map(()=>[1,2])});
    const initial=await ingestEvents({mongo,embeddingRuntime},[ordinaryEvent('hot-owner','Old outside information.')]);await initial.backgroundIndexing;assert.ok(mongo.hotVectors.rows.size>0);
    const changed=await ingestEvents({mongo,embeddingRuntime},[{...ordinaryEvent('hot-owner',''),kind,extra:{node_id:'hot-owner'}}]);await changed.backgroundIndexing;
    assert.equal(mongo.hotVectors.rows.size,0);for(const c of mongo.collections.values())if(c.collectionName.includes('__'))assert.equal(c.rows.size,0);
  }
});
test('successive parent ingests cannot let an older detached embedding overwrite the current text',async()=>{
  const mongo=vectorDeletionFixture();let release;const gate=new Promise(resolve=>release=resolve);
  const embeddingRuntime=eventRuntime({generate:async texts=>{if(texts.some(text=>text.includes('first version')))await gate;return texts.map(()=>[1,2]);}});
  const first=await ingestEvents({mongo,embeddingRuntime},[ordinaryEvent('serial-parent','The first version stays pending.')]);
  const secondPromise=ingestEvents({mongo,embeddingRuntime},[ordinaryEvent('serial-parent','The second version is current.')]);setTimeout(release,20);
  const second=await secondPromise;await second.backgroundIndexing;await first.backgroundIndexing;
  const rows=[...mongo.hotVectors.rows.values()];assert.ok(rows.length>0);assert.ok(rows.every(row=>row.text.includes('second version')));
});
test('same path reindexing uses the computed source hash to avoid stale hydration cache entries',async()=>{
  const dir=await mkdtemp(join(tmpdir(),'openplanner-revision-hydration-'));const mongo=mongoFixture();const path=join(dir,'source.txt');
  try{await withEnv({OPENPLANNER_SOURCE_ROOT:dir},async()=>{
    for(const text of ['Earlier outside information.','Updated outside information.']){
      await writeFile(path,text);await indexTextInMongoVectors({mongo,tier:'hot',parentId:'same-source',text,extra:{source_path:path},metadata:{embedding_model:'fixture'},embeddingFunction:{generate:async ts=>ts.map(()=>[1,2])}});
      const doc=[...mongo.hotVectors.rows.values()].at(-1);assert.equal(await hydrateVectorDocumentText(doc),text);
    }
  });}finally{await rm(dir,{recursive:true,force:true});}
});
test('identical event payloads fan out once while conflicting IDs still refuse',async()=>{
  const mongo=mongoFixture();const event={...ordinaryEvent('duplicate',''),kind:'graph.edge',extra:{source_node_id:'a',target_node_id:'b',edge_kind:'related'}};
  const result=await ingestEvents({mongo,embeddingRuntime:{}},[event,event]);await result.backgroundIndexing;
  assert.equal(result.count,1);assert.deepEqual(result.ids,['duplicate']);assert.equal(result.acceptedEvents.length,1);assert.equal(result.projectedGraphEdges,1);
});
function queryFixture(){
  const mongo=mongoFixture();const row={_id:'healthy-chunk',parent_id:'healthy',embedding:[1,0],embedding_model:'healthy-model',embedding_dimensions:2,text:'outside information',labels:['keep'],ts:new Date()};
  mongo.vectorPartitions.rows.set('healthy',{_id:'healthy',tier:'hot',model:'healthy-model',dimensions:2,collectionName:'healthy_vectors',searchIndexName:'fixture_index',searchIndexStatus:'ready'});
  const c=mongo.db.collection('healthy_vectors');c.rows.set(row._id,row);
  c.aggregate=pipeline=>({toArray:async()=>{const projection=pipeline[1].$project;return [{...Object.fromEntries(Object.keys(projection).filter(k=>k in row).map(k=>[k,row[k]])),score:1}];}});
  return mongo;
}
test('native Atlas vector projection preserves retention labels in returned metadata',async()=>{
  const mongo=queryFixture();const result=await queryMongoVectorsByText({mongo,tier:'hot',q:'outside',k:2,getEmbeddingFunctionForModel:()=>({generate:async()=>[[1,0]]})});assert.deepEqual(result.metadatas[0][0].labels,['keep']);
});
test('round-trip policy update excludes immutable Mongo storage identity',async()=>{
  const mongo=mongoFixture();const policies=mongo.db.collection('tenant_policies');policies.rows.set('tenant-a',{_id:'stored-id',tenant_id:'tenant-a',created_at:new Date('2025-01-01'),retention_days:4});const p=createProtocols({mongo}).tenantManagement;
  const original=await p.getPolicy('tenant-a');await p.setPolicy('tenant-a',{...original,retention_days:5});const write=policies.calls.writes.at(-1);assert.equal(Object.hasOwn(write.update.$set,'_id'),false);assert.equal(write.update.$set.retention_days,5);
});
test('an unavailable historical model preserves healthy results and reports incomplete partition coverage',async()=>{
  const mongo=queryFixture();mongo.vectorPartitions.rows.set('obsolete',{_id:'obsolete',tier:'hot',model:'obsolete-model',dimensions:2,collectionName:'obsolete_vectors',searchIndexStatus:'ready'});
  const result=await queryMongoVectorsByText({mongo,tier:'hot',q:'outside',k:2,getEmbeddingFunctionForModel:model=>({generate:async()=>{if(model==='obsolete-model')throw new Error('PRIVATE provider unavailable');return [[1,0]];}})});
  assert.deepEqual(result.ids,[['healthy-chunk']]);assert.deepEqual(result.unavailable_partitions,['obsolete_vectors']);assert.equal(result.partial,true);assert.ok(!JSON.stringify(result).includes('PRIVATE'));
});
test('visibility detail mode cannot exceed the bounded session row limit',async()=>{
  const mongo=mongoFixture();let observed;mongo.events.find=()=>({sort(){return this;},limit(n){observed=n;return this;},project(){return this;},toArray:async()=>[]});await getSessionResponse({mongo},'session-a',{mode:'visibility',limit:999999999});assert.equal(observed,1000);
});

test('when every query model is unavailable, search reports a named failure',async()=>{
  const mongo=queryFixture();
  await assert.rejects(queryMongoVectorsByText({mongo,tier:'hot',q:'outside',k:2,getEmbeddingFunctionForModel:()=>{throw new Error('PRIVATE credentials must not be exposed');}}),/^Error: vector query embedding unavailable for all partitions$/);
});
test('failed base projection starts no detached provider work',async()=>{
  const mongo=vectorDeletionFixture();let calls=0;
  mongo.graphEdges.deleteMany=async()=>{throw new Error('owned projection failed');};
  const provider={generate:async texts=>{calls++;return texts.map(()=>[1,2]);}};
  await assert.rejects(ingestEvents({mongo,embeddingRuntime:eventRuntime(provider)},[ordinaryEvent('base-failure','Outside information.')]),/owned projection failed/);
  assert.equal(calls,0);
});

// Native Codex 5479730893: confirmed on 0065680, before these four production fixes.
test('source expiry and exemption propagate to graph node embeddings and their owned TTL index',async()=>{
  const mongo=ownedProjectionFixture();const provider={generate:async texts=>texts.map(()=>[1,2])};
  const event={...ordinaryEvent('graph-retention','Outside information changes creative choices.'),kind:'graph.node',extra:{node_id:'graph-retention'}};
  for(const labels of [[],['keep'],[]]){
    const result=await ingestEvents({mongo,embeddingRuntime:eventRuntime(provider)},[{...event,extra:{...event.extra,openplanner_labels:{labels}}}]);await result.backgroundIndexing;
    const source=mongo.events.rows.get(event.id);const rows=[...mongo.graphNodeEmbeddings.rows.values()];assert.ok(rows.length>0);
    for(const row of rows){if(labels.length)assert.equal(row.expiresAt,null);else{assert.ok(source.expiresAt instanceof Date);assert.equal(row.expiresAt?.getTime(),source.expiresAt.getTime());}}
  }
  const connect=mock.method(MongoClient.prototype,'connect',async function(){return this;});const db=mock.method(MongoClient.prototype,'db',()=>mongo.db);
  try{await openMongoDB({...openingConfig,eventsTtlSeconds:60});assert.ok(mongo.graphNodeEmbeddings.calls.indexes.some(i=>i.keys.expiresAt===1&&i.opts.name==='graph_node_embeddings_ttl'&&i.opts.expireAfterSeconds===0));}
  finally{connect.mock.restore();db.mock.restore();}
});
test('browse projections reject recursive server Javascript before collection access',async()=>{
  for(const projection of [{bad:{$function:{body:'return 1',args:[],lang:'js'}}},{nested:{$cond:[true,{$accumulator:{}},0]}},{nested:[{$where:'true'}]}]){
    let calls=0;await assert.rejects(queryCollectionResponse({mongo:{db:{collection(){calls++;throw new Error('collection must not be reached');}}}},{collection:'events',projection}),/Unsupported server-side JavaScript operator/);assert.equal(calls,0);
  }
});
test('tier and quality fusion retain incomplete vector partition coverage',async()=>{
  const mongo=queryFixture();mongo.vectorPartitions.rows.set('obsolete',{_id:'obsolete',tier:'hot',model:'obsolete-model',dimensions:2,collectionName:'obsolete_vectors',searchIndexStatus:'ready'});
  const providerFor=model=>({generate:async()=>{if(model==='obsolete-model')throw new Error('PRIVATE provider error');return [[1,0]];}});
  const result=await vectorSearchWithQuality({mongo,embeddingRuntime:{hot:{getEmbeddingFunctionForModel:providerFor},compact:{getEmbeddingFunctionForModel:providerFor}}},{q:'outside',k:2,tier:'both',quality:'good_then_not_bad'});
  assert.deepEqual(result.result.ids,[['healthy']]);assert.equal(result.result.partial,true);assert.deepEqual(result.result.unavailable_partitions,['obsolete_vectors']);
});

test('an empty second tier cannot turn all unavailable query models into success',async()=>{
  const mongo=queryFixture();const unavailable=()=>{throw new Error('PRIVATE provider unavailable');};
  await assert.rejects(vectorSearchWithQuality({mongo,embeddingRuntime:{hot:{getEmbeddingFunctionForModel:unavailable},compact:{getEmbeddingFunctionForModel:unavailable}}},{q:'outside',tier:'both',quality:'any'}),/vector query embedding unavailable for all partitions/);
});


test('provider vectors survive failed or pending optional cache persistence',async()=>{
  for(const persist of [async()=>{throw new Error('read-only cache');},()=>new Promise(()=>{})]){
    const cache={getMany:async()=>new Map(),putMany:persist};
    await withFetch(async()=>response([[1,2]]),async()=>{
      const provider=new EmbedProviderFunction('fixture','http://fixture.invalid',{batchWindowMs:1,cache});
      const result=await Promise.race([provider.generate(['outside information']),new Promise(resolve=>setTimeout(()=>resolve('blocked'),80))]);
      assert.deepEqual(result,[[1,2]]);
    });
  }
});
test('cache batch returns before persistence while explicit flush remains durable',async()=>{
  const dir=await mkdtemp(join(tmpdir(),'openplanner-debounced-'));const path=join(dir,'cache.json');
  try{const cache=new PersistentEmbeddingCache(path);await cache.putMany([{key:'one',vector:[1,2]}]);
    await assert.rejects(readFile(path),{code:'ENOENT'});await cache.flush();
    assert.deepEqual((await new PersistentEmbeddingCache(path).getMany(['one'])).get('one'),[1,2]);
  }finally{await rm(dir,{recursive:true,force:true});}
});
test('SDK runtime construction failures occur before opening Mongo',async()=>{
  const dir=await mkdtemp(join(tmpdir(),'openplanner-sdk-init-'));const path=join(dir,'invalid.json');await writeFile(path,'invalid JSON');let connected=0;
  const mongo=mongoFixture();const connect=mock.method(MongoClient.prototype,'connect',async function(){connected++;return this;});const db=mock.method(MongoClient.prototype,'db',()=>mongo.db);
  try{await assert.rejects(createOpenPlannerSdk({config:{embedProviderCachePath:path,mongodb:openingConfig}}));assert.equal(connected,0);}finally{connect.mock.restore();db.mock.restore();await rm(dir,{recursive:true,force:true});}
});
test('SDK protocol setup rejection closes the newly connected client',async()=>{
  const script=`import assert from 'node:assert/strict';const {MongoClient}=await import('mongodb');
    const module=await import(process.argv[1]);const collections=new Map();const collection=()=>({createIndex:async()=>'',indexes:async()=>[],dropIndex:async()=>{},updateMany:async()=>{},updateOne:async()=>{},findOne:async()=>null,find:()=>({sort(){return this;},toArray:async()=>[]}),listSearchIndexes:()=>({toArray:async()=>[{status:'READY',queryable:true}]})});
    let closed=0;MongoClient.prototype.connect=async function(){return this;};MongoClient.prototype.db=()=>({collection:n=>{if(!collections.has(n))collections.set(n,collection());return collections.get(n);}});MongoClient.prototype.close=async()=>{closed++;};
    await assert.rejects(module.createOpenPlannerSdk({config:{embedProviderCachePath:undefined}}),/REST protocol/);assert.equal(closed,1);`;
  await promisify(execFileCallback)(process.execPath,['--input-type=module','-e',script,new URL('../dist/sdk.js',import.meta.url).href],{cwd:new URL('..',import.meta.url),env:{...process.env,PROTOCOL_IMPL:'rest',EMBED_PROVIDER_CACHE_PATH:'/fixture-cache-unused'},timeout:5000});
});
test('duplicate vector parent IDs refuse the complete batch before provider or writes',async()=>{
  const mongo=mongoFixture();let embedded=0;
  await assert.rejects(batchIndexTextsInMongoVectors({mongo,tier:'hot',items:[item('same','first text'),item('same','different text')],embeddingFunction:{generate:async texts=>{embedded++;return texts.map(()=>[1,2]);}}}),/duplicate.*parent/i);
  assert.equal(embedded,0);assert.equal(mongo.hotVectors.calls.writes.length,0);
});
test('owned graph edges and delayed hot vectors retain exact source expiry',async()=>{
  const mongo=mongoFixture();const expiry=new Date('2026-01-01T00:01:00Z');const original=mongo.events.findOne;
  mongo.events.findOne=async filter=>({...await original.call(mongo.events,filter),expiresAt:expiry});
  const rows=[{...ordinaryEvent('message-ttl','Outside information.'),extra:{}},{...ordinaryEvent('edge-ttl',''),kind:'graph.edge',extra:{source_node_id:'a',target_node_id:'b',edge_kind:'related'}}];
  const result=await ingestEvents({mongo,embeddingRuntime:eventRuntime({generate:async texts=>texts.map(()=>[1,2])})},rows);await result.backgroundIndexing;
  assert.ok(mongo.hotVectors.rows.size);for(const row of mongo.hotVectors.rows.values())assert.equal(row.expiresAt?.getTime(),expiry.getTime());
  assert.ok(mongo.graphEdges.rows.size);for(const row of mongo.graphEdges.rows.values())assert.equal(row.expiresAt?.getTime(),expiry.getTime());
});
test('graph model failures do not skip independently healthy groups',async()=>{
  const mongo=mongoFixture();const models=[];const runtime={hot:{getModel:scope=>scope.project==='failed-project'?'failed':'healthy',getBackgroundEmbeddingFunctionForModel:model=>({generate:async texts=>{models.push(model);if(model==='failed')throw new Error('model unavailable');return texts.map(()=>[1,2]);}})}};
  const rows=['failed','healthy'].map(id=>({schema:'openplanner.event.v1',id,ts:'2026-01-01T00:00:00Z',source:'fixture',kind:'graph.node',text:'Outside information.',source_ref:{project:id+'-project'},extra:{node_id:id}}));
  const result=await ingestEvents({mongo,embeddingRuntime:runtime},rows);await result.backgroundIndexing;
  assert.ok(models.includes('healthy'));assert.ok([...mongo.graphNodeEmbeddings.rows.values()].some(row=>row.embedding_model==='healthy'));
});
test('TTL disable reconciles stored dates without rewriting explicit nonexpiring rows',async()=>{
  const c=collection('retention');await reconcileManagedTtl(c,'managed',0);assert.deepEqual(c.calls.writes[0].filter,{expiresAt:{$type:'date'}});
});
test('opening Mongo indexes source-owned cleanup and retained graph edges',async()=>{
  const mongo=mongoFixture();const connect=mock.method(MongoClient.prototype,'connect',async function(){return this;});const db=mock.method(MongoClient.prototype,'db',()=>mongo.db);
  try{await openMongoDB({...openingConfig,eventsTtlSeconds:60});const edges=mongo.db.collection('graph_edges');
    assert.ok(mongo.events.calls.indexes.some(x=>x.keys['extra.source_event_id']===1 && x.keys.source===1));
    assert.ok(edges.calls.indexes.some(x=>x.keys['data.source_event_id']===1));assert.ok(edges.calls.indexes.some(x=>x.opts.name==='graph_edges_ttl'));
  }finally{connect.mock.restore();db.mock.restore();}
});
test('unavailable Atlas retries do not repeat partition storage setup or probe every write',async()=>{
  const mongo=mongoFixture();const original=mongo.db.collection;let probes=0;
  mongo.db.collection=function(name){const c=original.call(this,name);if(name.includes('__'))c.listSearchIndexes=()=>({toArray:async()=>{probes++;throw new Error('unsupported search');}});return c;};
  await upsertMongoVectorDocuments(mongo,'hot',[entry('one')]);const c=[...mongo.collections.values()].find(c=>c.collectionName.includes('__'));const count=c.calls.indexes.length;
  await upsertMongoVectorDocuments(mongo,'hot',[entry('two')]);assert.equal(c.calls.indexes.length,count);assert.equal(probes,1);assert.ok(c.rows.has('two'));
});

test('building Atlas indexes preserve writes and become ready without repeating storage setup',async()=>{
  const mongo=mongoFixture();const original=mongo.db.collection;let probes=0;let ready=false;
  mongo.db.collection=function(name){const c=original.call(this,name);if(name.includes('__'))c.listSearchIndexes=()=>({toArray:async()=>{probes++;return [{status:ready?'READY':'BUILDING',queryable:ready}];}});return c;};
  const clock=mock.method(Date,'now',()=>1000);
  try{const outcome=await Promise.race([upsertMongoVectorDocuments(mongo,'hot',[entry('building')]).then(()=>true),new Promise(resolve=>setTimeout(()=>resolve(false),80))]);
    assert.equal(outcome,true);assert.equal([...mongo.vectorPartitions.rows.values()][0].searchIndexStatus,'pending');assert.ok(mongo.hotVectors.rows.has('building'));
    const part=[...mongo.collections.values()].find(c=>c.collectionName.includes('__'));const indexes=part.calls.indexes.length;
    await upsertMongoVectorDocuments(mongo,'hot',[entry('pending')]);assert.equal(probes,1);
    ready=true;clock.mock.mockImplementation(()=>6001);await upsertMongoVectorDocuments(mongo,'hot',[entry('ready')]);
    assert.equal([...mongo.vectorPartitions.rows.values()][0].searchIndexStatus,'ready');assert.equal(probes,2);assert.equal(part.calls.indexes.length,indexes);assert.ok(part.rows.has('ready'));
  }finally{clock.mock.restore();}
});

// Exact-head Codex review 5479951707: executable regressions before repair.
test('read queries refresh a pending Atlas index once per bounded cadence without another write',async()=>{
  const mongo=mongoFixture();const original=mongo.db.collection;let ready=false,probes=0,queries=0;
  mongo.db.collection=function(name){const c=original.call(this,name);if(name.includes('__')){
    c.listSearchIndexes=()=>({toArray:async()=>{probes++;return [{status:ready?'READY':'BUILDING',queryable:ready}];}});
    c.aggregate=()=>({toArray:async()=>{queries++;return [...c.rows.values()].map(doc=>({...doc,score:1}));}});
  }return c;};
  const clock=mock.method(Date,'now',()=>1000);
  try{await upsertMongoVectorDocuments(mongo,'hot',[entry('read-ready')]);ready=true;clock.mock.mockImplementation(()=>6001);
    const query=()=>queryMongoVectorsByText({mongo,tier:'hot',q:'outside',k:1,getEmbeddingFunctionForModel:()=>({generate:async()=>[[1,2]]})});
    await Promise.all([query(),query()]);assert.equal(probes,2);assert.equal(queries,2);assert.equal([...mongo.vectorPartitions.rows.values()][0].searchIndexStatus,'ready');
  }finally{clock.mock.restore();}
});
test('embedding batches overlap up to the configured cap and all queued waiters complete',async()=>{
  let release;const gate=new Promise(resolve=>release=resolve);let active=0,maximum=0,started=0;
  await withFetch(async(_,opts)=>{started++;active++;maximum=Math.max(maximum,active);await gate;active--;return response(JSON.parse(opts.body).input.map(()=>[1,2]));},async()=>{
    const provider=new EmbedProviderFunction('fixture','http://fixture.invalid',{maxConcurrentBatches:2,maxBatchItems:1});
    const result=provider.generate(['one','two','three','four']);
    try{await new Promise(resolve=>setImmediate(resolve));assert.equal(started,2);assert.equal(maximum,2);}
    finally{release();assert.equal((await result).length,4);}
    assert.equal(started,4);assert.equal(maximum,2);
  });
});
test('embedding provider errors never consume or expose arbitrary error bodies',async()=>{
  let textReads=0;const logs=[];const logger=mock.method(console,'error',(...values)=>logs.push(values.map(String).join(' ')));
  try{await withFetch(async()=>({ok:false,status:503,statusText:'PRIVATE status',text:async()=>{textReads++;return 'PRIVATE submitted text and credential';},body:{cancel:async()=>{}}}),async()=>{
    const provider=new EmbedProviderFunction('fixture','http://fixture.invalid',{maxBatchItems:1});
    await assert.rejects(provider.generate(['PRIVATE submitted text']),error=>/503/.test(error.message)&&!error.message.includes('PRIVATE'));
  });assert.equal(textReads,0);assert.ok(!logs.join(' ').includes('PRIVATE'));}
  finally{logger.mock.restore();}
});
test('non-string and whitespace event identities refuse the complete batch before writes or indexing',async()=>{
  for(const field of ['id','source','kind'])for(const value of [123,{},true,'   ']){
    const mongo=mongoFixture();let calls=0;const provider={generate:async()=>{calls++;return [[1,2]];}};
    const row=ordinaryEvent('valid','Outside information');
    await assert.rejects(ingestEvents({mongo,embeddingRuntime:eventRuntime(provider)},[row,{...row,[field]:value}]),new RegExp(`event\\.${field}`));
    assert.equal(mongo.events.rows.size,0);assert.equal(calls,0);
  }
});
for(const ttl of [60,0])test(`legacy relative TTL remains owned by its retained writer when SDK TTL is ${ttl}`,async()=>{
  const c=collection('legacy');c.indexes=async()=>[{name:'events_ttl',key:{createdAt:1},expireAfterSeconds:60},{name:'events_ttl_absolute',key:{expiresAt:1},expireAfterSeconds:0}];
  await reconcileManagedTtl(c,'events_ttl',ttl);assert.ok(!c.calls.drops.includes('events_ttl'));
  assert.ok(!c.calls.indexes.some(i=>i.opts.name==='events_ttl'));
  if(ttl>0)assert.ok(c.calls.indexes.some(i=>i.opts.name==='events_ttl_absolute'&&i.keys.expiresAt===1));
  else assert.ok(c.calls.drops.includes('events_ttl_absolute'));
});
test('enforced Vexx ranks small nonempty partitions and accepts empty search results',async()=>{
  const script=`import assert from 'node:assert/strict';
    process.env.VEXX_BASE_URL='http://fixture.invalid';process.env.VEXX_ENFORCE='true';process.env.VEXX_MIN_CANDIDATES='256';
    const {queryMongoVectorsByText}=await import(process.argv[1]);let calls=0,rows=[{_id:'one',parent_id:'one',text:'outside',embedding:[1,0],embedding_model:'fixture',ts:new Date()}];
    globalThis.fetch=async(_,options)=>{calls++;const body=JSON.parse(options.body);assert.equal(body.candidates.length,1);return new Response(JSON.stringify({matches:[{id:'one',score:1}]}),{status:200});};
    const part={tier:'hot',model:'fixture',dimensions:2,collectionName:'small',searchIndexName:'fixture',searchIndexStatus:'ready'};
    const mongo={vectorPartitions:{find:()=>({sort(){return this;},toArray:async()=>[part]})},db:{collection:()=>({collectionName:'small',aggregate:()=>({toArray:async()=>rows}),find:()=>({sort(){return this;},limit(){return this;},toArray:async()=>rows})})}};
    const query=()=>queryMongoVectorsByText({mongo,tier:'hot',q:'outside',k:10,getEmbeddingFunctionForModel:()=>({generate:async()=>[[1,0]]})});
    assert.deepEqual((await query()).ids,[['one']]);assert.equal(calls,1);rows=[];assert.deepEqual((await query()).ids,[[]]);assert.equal(calls,1);`;
  await promisify(execFileCallback)(process.execPath,['--input-type=module','-e',script,new URL('../dist/mongo-vectors.js',import.meta.url).href],{timeout:5000});
});
test('mongoBatchSize groups complete parent replacements into bounded transactions',async()=>{
  const mongo=mongoFixture();let transactions=0;mongo.client.startSession=()=>({withTransaction:async f=>{transactions++;await f();},endSession:async()=>{}});
  const result=await batchIndexTextsInMongoVectors({mongo,tier:'hot',items:Array.from({length:5},(_,n)=>item(`batch-parent-${n}`)),embeddingFunction:{generate:async texts=>texts.map(()=>[1,2])},config:{mongoBatchSize:2,concurrency:2}});
  assert.deepEqual(result,{indexed:5,failed:[]});assert.equal(transactions,3);
  assert.deepEqual(mongo.hotVectors.calls.deletes.map(f=>f.parent_id.$in.length).sort(),[1,2,2]);
  assert.ok(mongo.hotVectors.rows.size>=5);
});
