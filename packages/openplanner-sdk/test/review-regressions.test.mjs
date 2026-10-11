// SPDX-License-Identifier: LGPL-3.0-or-later
import test, { mock } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, writeFile, readFile, rm, symlink, mkdir } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { execFile as execFileCallback } from 'node:child_process';
import { promisify } from 'node:util';
import { createHash } from 'node:crypto';
import { createServer } from 'node:http';
import { MongoClient, ObjectId } from 'mongodb';
import TurndownService from 'turndown';
import { createProtocols } from '../dist/protocol-adapters.js';
import { openMongoDB, ftsSearch, ilikeSearch, upsertGraphLayoutOverrides, reconcileManagedTtl, upsertEvent, upsertCompactedMemory, upsertGraphNodeEmbeddings, upsertGraphSemanticEdges, upsertGraphSemanticForceSamples, upsertGraphEdges, enqueueMigrationJob } from '../dist/mongodb.js';
import { makeEmbeddingCacheKey, PersistentEmbeddingCache } from '../dist/embedding-cache.js';
import { sourceRoot, safeSourceFilePath, loadHydrationSourceText, getHydrationCache, closeHydrationCache } from '../dist/source-hydration.js';
import { EmbedProviderFunction, ParallelEmbeddingPool } from '../dist/embeddings.js';
import { cacheGet, cachePut } from '@open-hax/openplanner-document-hydration';
import { splitSentences, deduplicateByHash } from '../dist/sentence-split.js';
import { formatEmbeddingQueryText, formatEmbeddingPassageText } from '../dist/embedding-text.js';
import { ftsSearchWithQuality, vectorSearchWithQuality } from '../dist/search-core.js';
import { batchIndexTextsInMongoVectors, upsertMongoVectorDocuments, hydrateVectorDocumentText, addMongoVectorParentLabel, removeMongoVectorParentLabel, indexTextInMongoVectors, queryMongoVectorsByText, replaceMongoVectorEntries, deleteMongoVectorEntriesByFilter, listMongoVectorPartitions } from '../dist/mongo-vectors.js';
import { createOpenPlannerSdk } from '../dist/sdk.js';
import { createEmbeddingRuntime } from '../dist/embedding-runtime.js';
import { prepareIndexDocument } from '../dist/indexing.js';
import { ingestEvents } from '../dist/ingest.js';
import { queryCollectionResponse } from '../dist/mongo-browse.js';
import { getSessionResponse, listSessionsResponse } from '../dist/sessions-core.js';
import { mergeTieredVectorHits } from '../dist/vector-search.js';
import { loadConfig } from '../dist/config.js';

// CodeRabbit5481190788/4239586317: complete recall with bounded cursor/batch storage.
function streamingQueryFixture(count, rowAt) {
  const mongo=parentQueryFixture('cosine',[]);const collection=mongo.db.collection('healthy_vectors');
  const observed={scans:0,yielded:0,closed:0,batchSizes:[]};
  collection.find=filter=>{
    collection.calls.searches.push(filter);observed.scans++;
    return {
      batchSize(value){observed.batchSizes.push(value);assert.equal(value,256);return this;},
      async toArray(){throw new Error('full partition materialization is forbidden');},
      async *[Symbol.asyncIterator](){try{for(let i=0;i<count;i++){observed.yielded++;yield rowAt(i);}}finally{observed.closed++;}},
    };
  };
  return {mongo,observed};
}
const streamingQuery=(vectors,mongo,k=1)=>vectors.queryMongoVectorsByText({mongo,tier:'hot',q:'outside',k,getEmbeddingFunctionForModel:()=>({generate:async()=>[[1,0]]})});

for(const invalid of [NaN,Infinity,-Infinity,1.5,Number.MAX_SAFE_INTEGER+1,'3'])test(`streaming query rejects invalid retention k ${String(invalid)} before partition/provider IO`,async()=>{
  const {mongo,observed}=streamingQueryFixture(601,i=>vectorRow('row-'+i,'parent-'+i,.2));let generated=0;
  await assert.rejects(queryMongoVectorsByText({mongo,tier:'hot',q:'outside',k:invalid,getEmbeddingFunctionForModel:()=>({generate:async()=>{generated++;return [[1,0]];}})}),/k.*safe integer|safe integer.*k/i);
  assert.equal(mongo.vectorPartitions.calls.searches.length,0);assert.equal(observed.scans,0);assert.equal(observed.yielded,0);assert.equal(generated,0);
});
for(const k of [0,-1,2])test(`streaming query preserves existing safe integer retention k ${k}`,async()=>{
  const mongo=parentQueryFixture('cosine',[vectorRow('best','best-parent',1),vectorRow('second','second-parent',.8)]);
  const result=await streamingQuery({queryMongoVectorsByText},mongo,k);assert.deepEqual(result.ids,[k===2?['best','second']:['best']]);
});

test('streaming cosine scans every row and retains a late best match without toArray',async()=>{
  const {mongo,observed}=streamingQueryFixture(1201,i=>vectorRow('row-'+i,'parent-'+i,i===1200?1:.2));
  const result=await streamingQuery({queryMongoVectorsByText},mongo);
  assert.deepEqual(result.ids,[['row-1200']]);assert.equal(observed.yielded,1201);assert.equal(observed.closed,1);assert.deepEqual(observed.batchSizes,[256]);
});
test('streaming cosine admits lower usable chunks of duplicate parents and stable score ties',async()=>{
  const rows=[{...vectorRow('missing','same',1),text:'',source_text_redacted:true,source_ref:{source_path:'/not-admitted/missing'},text_hash_sha256:'0'.repeat(64)},vectorRow('z-tie','other',.8),vectorRow('a-tie','same',.8),vectorRow('late-best','same',.9),vectorRow('third','third',.7)];
  const {mongo,observed}=streamingQueryFixture(rows.length,i=>rows[i]);
  const result=await streamingQuery({queryMongoVectorsByText},mongo,2);
  assert.deepEqual(result.ids,[['late-best','z-tie']]);assert.equal(observed.yielded,rows.length);assert.equal(observed.closed,1);
  const tied=streamingQueryFixture(2,i=>[vectorRow('z-tie','other',.8),vectorRow('a-tie','same',.8)][i]);
  assert.deepEqual((await streamingQuery({queryMongoVectorsByText},tied.mongo)).ids,[['a-tie']]);
});
test('streaming Vexx bounds all batches including a short tail and ranks the complete partition',async()=>{
  const {mongo,observed}=streamingQueryFixture(601,i=>vectorRow('row-'+i,'parent-'+i,.2));const sizes=[];
  await withEnv({VEXX_BASE_URL:'https://vexx.fixture.invalid',VEXX_MIN_CANDIDATES:'256',VEXX_ENFORCE:'false'},async()=>{
    const vectors=await import(new URL('../dist/mongo-vectors.js?streaming-vexx-complete',import.meta.url).href);
    await withFetch(async(_url,init)=>{const request=JSON.parse(init.body);sizes.push(request.candidates.length);assert.ok(request.candidates.length<=256);assert.equal(observed.yielded,sizes.reduce((a,b)=>a+b,0));
      return {ok:true,json:async()=>({matches:request.candidates.map(row=>({id:row.id,score:row.id==='row-600'?.95:.1}))})};
    },async()=>{const result=await streamingQuery(vectors,mongo);assert.deepEqual(result.ids,[['row-600']]);assert.ok(Math.abs(result.distances[0][0]-.05)<1e-12);});
  });assert.deepEqual(sizes,[256,256,89]);assert.equal(observed.scans,1);assert.equal(observed.closed,1);
});
test('streaming optional Vexx later failure discards prior scores and rescans with local cosine',async()=>{
  const {mongo,observed}=streamingQueryFixture(601,i=>vectorRow('row-'+i,'parent-'+i,i===600?.9:.2));let calls=0;
  await withEnv({VEXX_BASE_URL:'https://vexx.fixture.invalid',VEXX_MIN_CANDIDATES:'1',VEXX_ENFORCE:'false'},async()=>{
    const vectors=await import(new URL('../dist/mongo-vectors.js?streaming-vexx-fallback',import.meta.url).href);
    await withFetch(async(_url,init)=>{calls++;const request=JSON.parse(init.body);assert.ok(request.candidates.length<=256);return {ok:true,json:async()=>({matches:calls===1?request.candidates.map(row=>({id:row.id,score:1})):[]})};},async()=>{
      const result=await streamingQuery(vectors,mongo);assert.deepEqual(result.ids,[['row-600']]);assert.ok(Math.abs(result.distances[0][0]-.1)<1e-12);
    });
  });assert.equal(calls,2);assert.equal(observed.scans,2);assert.equal(observed.closed,2);assert.equal(observed.yielded,512+601);
});
test('streaming enforced Vexx later failure refuses and closes the cursor without a local rescan',async()=>{
  const {mongo,observed}=streamingQueryFixture(601,i=>vectorRow('row-'+i,'parent-'+i,.2));let calls=0;
  await withEnv({VEXX_BASE_URL:'https://vexx.fixture.invalid',VEXX_MIN_CANDIDATES:'1',VEXX_ENFORCE:'true'},async()=>{
    const vectors=await import(new URL('../dist/mongo-vectors.js?streaming-vexx-enforced',import.meta.url).href);
    await withFetch(async(_url,init)=>{calls++;const request=JSON.parse(init.body);return {ok:true,json:async()=>({matches:calls===1?request.candidates.map(row=>({id:row.id,score:.5})):[]})};},async()=>assert.rejects(streamingQuery(vectors,mongo),/unavailable/i));
  });assert.equal(calls,2);assert.equal(observed.scans,1);assert.equal(observed.closed,1);assert.equal(observed.yielded,512);
});

function collection(name, initial = []) {
  const rows = new Map(initial.map(r => [r._id ?? r.id, r]));
  const calls = { indexes: [], writes: [], drops: [], searches: [], deletes: [], setups: 0 };
  const cursor = (values) => ({
    sort() { values.sort((a,b) => new Date(b.ts) - new Date(a.ts)); return this; },
    project() { return this; }, limit(n) { values = values.slice(0,n); return this; },
    async toArray() { return values; },
    batchSize() { return this; },
    async *[Symbol.asyncIterator]() { yield* values; },
  });
  return {
    collectionName: name, rows, calls, failSetup: false, failText: false,
    async createIndex(keys, opts = {}) { calls.indexes.push({keys, opts}); return opts.name; },
    async indexes() { return name === 'semantic_graph_runs' ? [{name:'graph_version_1',key:{graph_version:1},unique:true}] : []; },
    async dropIndex(name) { calls.drops.push(name); },
    async updateOne(filter, update) {
      if (this.failSetup) { this.failSetup = false; throw new Error('temporary partition failure'); }
      calls.writes.push({filter, update});
      const tupleFields = ['node_id','embedding_model','embedding_dimensions','chunk_index'];
      const key = filter._id ?? filter.tenant_id ?? filter.label_id ??
        (tupleFields.every(field=>Object.hasOwn(filter,field))
          ? [...rows.entries()].find(([,row])=>tupleFields.every(field=>row[field]===filter[field]))?.[0] ?? update.$setOnInsert?._id
          : undefined);
      const old = rows.get(key);
      const next = {...(!old ? update.$setOnInsert : {}), ...old, ...filter, ...update.$set};
      for (const field of Object.keys(update.$unset ?? {})) delete next[field];
      rows.set(key, next); return {modifiedCount:1};
    },
    async findOneAndUpdate(filter, update) { await this.updateOne(filter,update); return this.findOne(filter); },
    async insertOne(row) { rows.set(`auto-${rows.size}`, {...row}); },
    async insertMany(items) { for (const row of items) await this.insertOne(row); },
    async findOne(filter) {
      // The native empty-cleanup query uses parent ownership, not an _id lookup.
      if(Object.hasOwn(filter,'parent_id'))return [...rows.values()].find(row=>
        Array.isArray(filter.parent_id?.$in)?filter.parent_id.$in.includes(row.parent_id):row.parent_id===filter.parent_id)??null;
      return rows.get(filter._id ?? filter.tenant_id ?? filter.label_id) ?? null;
    },
    aggregate(pipeline) {
      // Native bounded session-scope discovery groups absent/null projects.
      assert.deepEqual(pipeline[1],{$group:{_id:{$ifNull:['$project',null]}}});
      assert.deepEqual(pipeline[2],{$limit:2});
      const session=pipeline[0].$match.session;
      const projects=[...new Set([...rows.values()].filter(row=>row.session===session).map(row=>row.project??null))];
      return {toArray:async()=>projects.slice(0,2).map(project=>({_id:project}))};
    },
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
    searchDefinition: {fields:[{type:'vector',path:'embedding',numDimensions:1024,similarity:'cosine'}]},
    async updateSearchIndex(_name,definition) { this.searchDefinition=definition; },
    listSearchIndexes() { calls.setups++; return cursor([{status:'READY',queryable:true,latestDefinition:this.searchDefinition}]); },
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
const response = vectors => new Response(JSON.stringify({data:vectors.map((embedding,index)=>({embedding,index}))}),{status:200});

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
  mongo.events.updateOne=async(filter,update)=>{assert.deepEqual(filter,{_id:'stored'});Object.assign(stored,update.$set);};
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
  const oldIds=oldDerived.map(row=>row.id);const oldNodes=[...mongo.graphNodeEmbeddings.rows.values()].map(row=>row.node_id).filter(id=>id!==event.id);
  const oldHash=createHash('sha256').update(event.text).digest('hex');
  const next=await ingestEvents({mongo,embeddingRuntime},[{...event,text:'A distinct later encounter reshapes this memory.'}]);await next.backgroundIndexing;
  for(const id of oldIds)assert.equal(mongo.events.rows.has(id),false);
  for(const node of oldNodes)assert.equal([...mongo.graphNodeEmbeddings.rows.values()].some(row=>row.node_id===node),false);
  assert.equal([...mongo.graphNodeEmbeddings.rows.values()].some(row=>row.source_text_hash_sha256===oldHash),false);
  const parent=[...mongo.graphNodeEmbeddings.rows.values()].find(row=>row.node_id===event.id);
  assert.ok(parent);assert.equal(parent.source_event_id,event.id);assert.equal(parent.source_text_hash_sha256,createHash('sha256').update('A distinct later encounter reshapes this memory.').digest('hex'));
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
    const module=await import(process.argv[1]);const collections=new Map();const collection=()=>({createIndex:async()=>'',indexes:async()=>[],dropIndex:async()=>{},updateMany:async()=>{},updateOne:async()=>{},findOne:async()=>null,find:()=>({sort(){return this;},toArray:async()=>[]}),listSearchIndexes:()=>({toArray:async()=>[{status:'READY',queryable:true,latestDefinition:{fields:[{type:'vector',path:'embedding',numDimensions:1024}]}}]})});
    let closed=0;MongoClient.prototype.connect=async function(){return this;};MongoClient.prototype.db=()=>({collection:n=>{if(!collections.has(n))collections.set(n,collection());return collections.get(n);}});MongoClient.prototype.close=async()=>{closed++;};
    await assert.rejects(module.createOpenPlannerSdk({config:{embedProviderCachePath:undefined}}),/REST protocol/);assert.equal(closed,1);`;
  await promisify(execFileCallback)(process.execPath,['--input-type=module','-e',script,new URL('../dist/sdk.js',import.meta.url).href],{cwd:new URL('..',import.meta.url),env:{...process.env,PROTOCOL_IMPL:'rest',EMBED_PROVIDER_CACHE_PATH:'/fixture-cache-unused'},timeout:5000});
});
test('SDK close persists recent embeddings and deletions before closing Mongo',async()=>{
  const dir=await mkdtemp(join(tmpdir(),'openplanner-sdk-cache-close-'));const path=join(dir,'cache.json');
  const removed=makeEmbeddingCacheKey({model:'fixture',text:'obsolete outside information'});
  const added=makeEmbeddingCacheKey({model:'fixture',text:'new outside information'});
  await writeFile(path,JSON.stringify([[removed,[3,4]]]));
  const mongo=mongoFixture();let cache,closes=0;
  const put=PersistentEmbeddingCache.prototype.putMany;
  const capture=mock.method(PersistentEmbeddingCache.prototype,'putMany',async function(entries){cache=this;return put.call(this,entries);});
  const connect=mock.method(MongoClient.prototype,'connect',async function(){return this;});
  const db=mock.method(MongoClient.prototype,'db',()=>mongo.db);
  const close=mock.method(MongoClient.prototype,'close',async()=>{
    assert.deepEqual((await readFile(path,'utf8')).trim().split('\n').map(line=>JSON.parse(line)),[[added,[1,2]]]);closes++;
  });
  try{
    const sdk=await createOpenPlannerSdk({config:{embedProviderCachePath:path,embedProviderBatchWindowMs:1,mongodb:openingConfig}});
    await withFetch(async()=>response([[1,2]]),async()=>{
      assert.deepEqual(await sdk.embeddingRuntime.hot.getEmbeddingFunctionForModel('fixture').generate(['new outside information']),[[1,2]]);
    });
    assert.ok(cache);cache.delete(removed);
    await sdk.close();assert.equal(closes,1);
    const restored=new PersistentEmbeddingCache(path);
    assert.equal(restored.has(removed),false);assert.deepEqual((await restored.getMany([added])).get(added),[1,2]);
  }finally{
    if(cache)await cache.flush();capture.mock.restore();connect.mock.restore();db.mock.restore();close.mock.restore();await rm(dir,{recursive:true,force:true});
  }
});
test('SDK close still closes Mongo after a cache flush failure and reports only a fixed warning',async()=>{
  const mongo=mongoFixture();let closes=0,flushes=0;const warnings=[];
  const connect=mock.method(MongoClient.prototype,'connect',async function(){return this;});
  const db=mock.method(MongoClient.prototype,'db',()=>mongo.db);
  const close=mock.method(MongoClient.prototype,'close',async()=>{closes++;});
  const flush=mock.method(PersistentEmbeddingCache.prototype,'flush',async()=>{flushes++;throw new Error('PRIVATE cache path and credential');});
  const warning=mock.method(console,'warn',(...args)=>warnings.push(args));
  try{
    const sdk=await createOpenPlannerSdk({config:{embedProviderCachePath:undefined,mongodb:openingConfig}});
    await sdk.close();assert.equal(flushes,1);assert.equal(closes,1);
    assert.deepEqual(warnings,[['Embedding cache persistence failed']]);
  }finally{connect.mock.restore();db.mock.restore();close.mock.restore();flush.mock.restore();warning.mock.restore();}
});
test('duplicate vector parent IDs refuse the complete batch before provider or writes',async()=>{
  const mongo=mongoFixture();let embedded=0;
  await assert.rejects(batchIndexTextsInMongoVectors({mongo,tier:'hot',items:[item('same','first text'),item('same','different text')],embeddingFunction:{generate:async texts=>{embedded++;return texts.map(()=>[1,2]);}}}),/duplicate.*parent/i);
  assert.equal(embedded,0);assert.equal(mongo.hotVectors.calls.writes.length,0);
});
test('owned graph edges and delayed hot vectors retain exact source expiry',async()=>{
  const mongo=mongoFixture();const expiry=new Date('2026-01-01T00:01:00Z');const original=mongo.events.findOne;
  mongo.events.findOne=async filter=>{const row=await original.call(mongo.events,filter);return row?{...row,expiresAt:expiry}:null;};
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
  assert.deepEqual(mongo.hotVectors.calls.deletes.map(f=>typeof f.parent_id==='string'?1:f.parent_id.$in.length).sort(),[1,2,2]);
  assert.ok(mongo.hotVectors.rows.size>=5);
});

for(const name of ['events_ttl','compacted_ttl'])test(`fresh SDK retention leaves ${name} available to the retained writer`,async()=>{
  const c=collection('fresh-retention');await reconcileManagedTtl(c,name,60);
  assert.ok(c.calls.indexes.some(i=>i.opts.name===`${name}_absolute`&&i.keys.expiresAt===1));
  assert.ok(!c.calls.indexes.some(i=>i.opts.name===name));
});

// Native Codex review 5480078194 on a8: failure isolation and boundary admission.
for(const failure of ['embedding','storage'])test(`failed ${failure} replacement removes obsolete hot vectors across partitions`,async()=>{
  const mongo=vectorDeletionFixture();mongo.retention.eventsTtlSeconds=0;
  const provider={generate:async texts=>texts.map(()=>[1,2])};
  const first=await ingestEvents({mongo,embeddingRuntime:eventRuntime(provider)},[ordinaryEvent('failed-replacement','Obsolete outside information.')]);await first.backgroundIndexing;
  assert.ok(mongo.hotVectors.rows.size>0);
  const unrelated={_id:'unrelated',parent_id:'other-parent',text:'Keep this information',embedding:[1,2]};
  mongo.hotVectors.rows.set(unrelated._id,unrelated);
  if(failure==='embedding')provider.generate=async()=>{throw new Error('fixture provider unavailable');};
  else {
    mongo.client.startSession=()=>({withTransaction:async f=>{
      const saved=new Map([...mongo.collections].map(([name,c])=>[name,new Map(c.rows)]));
      try{return await f();}catch(error){for(const [name,rows]of saved){const c=mongo.collections.get(name);c.rows.clear();for(const [id,row]of rows)c.rows.set(id,row);}throw error;}
    },endSession:async()=>{}});
    const original=mongo.hotVectors.bulkWrite;let fail=true;mongo.hotVectors.bulkWrite=async ops=>{if(fail){fail=false;throw new Error('fixture vector write failed');}return original.call(mongo.hotVectors,ops);};
  }
  const changed=await ingestEvents({mongo,embeddingRuntime:eventRuntime(provider)},[ordinaryEvent('failed-replacement','Replacement outside information.')]);await changed.backgroundIndexing;
  assert.equal(mongo.events.rows.get('failed-replacement').text,'Replacement outside information.');
  assert.deepEqual([...mongo.hotVectors.rows.keys()],['unrelated']);
  for(const c of mongo.collections.values())if(c.collectionName.includes('__'))assert.ok([...c.rows.values()].every(row=>row.parent_id!=='failed-replacement'));
});

for(const ready of [false,true])test(`failed ${ready?'native and fallback':'pending scan'} partition preserves healthy vector hits`,async()=>{
  const mongo=queryFixture();
  mongo.vectorPartitions.rows.set('broken',{_id:'broken',tier:'hot',model:'healthy-model',dimensions:2,collectionName:'broken_vectors',searchIndexStatus:ready?'ready':'pending',searchIndexCheckedAt:new Date()});
  const broken=mongo.db.collection('broken_vectors');broken.aggregate=()=>{throw new Error('PRIVATE native failure');};broken.find=()=>{throw new Error('PRIVATE scan failure');};
  const result=await queryMongoVectorsByText({mongo,tier:'hot',q:'outside',k:2,getEmbeddingFunctionForModel:()=>({generate:async()=>[[1,0]]})});
  assert.deepEqual(result.ids,[['healthy-chunk']]);assert.deepEqual(result.unavailable_partitions,['broken_vectors']);assert.equal(result.partial,true);assert.equal(result.queried_partition_count,1);assert.ok(!JSON.stringify(result).includes('PRIVATE'));
});
test('all failed partition executions produce a named unavailable error without provider diagnostics',async()=>{
  const mongo=queryFixture();const c=mongo.db.collection('healthy_vectors');c.aggregate=()=>{throw new Error('PRIVATE native');};c.find=()=>{throw new Error('PRIVATE scan');};
  await assert.rejects(queryMongoVectorsByText({mongo,tier:'hot',q:'outside',k:2,getEmbeddingFunctionForModel:()=>({generate:async()=>[[1,0]]})}),error=>error.unavailablePartitions?.includes('healthy_vectors')&&!error.message.includes('PRIVATE'));
});
test('fractional environment batch limits never normalize to zero',async()=>{
  await withEnv({EMBED_PROVIDER_MAX_BATCH_ITEMS:'0.5',EMBED_PROVIDER_BATCH_WINDOW_MS:'0.5'},async()=>{
    const config=loadConfig();assert.ok(Number.isInteger(config.embedProviderMaxBatchItems)&&config.embedProviderMaxBatchItems>0);assert.ok(Number.isInteger(config.embedProviderBatchWindowMs)&&config.embedProviderBatchWindowMs>0);
  });
});
for(const field of ['maxBatchItems','maxConcurrentBatches'])test(`fractional ${field} cannot stall the provider drain`,async()=>{
  const script=`import assert from 'node:assert/strict';const {EmbedProviderFunction}=await import(process.argv[1]);globalThis.fetch=async(_,options)=>new Response(JSON.stringify({embeddings:JSON.parse(options.body).input.map(()=>[1,2])}),{status:200});const provider=new EmbedProviderFunction('fixture','http://fixture.invalid',{[process.argv[2]]:0.5,batchWindowMs:1});assert.deepEqual(await provider.generate(['outside information']),[[1,2]]);`;
  await promisify(execFileCallback)(process.execPath,['--input-type=module','-e',script,new URL('../dist/embeddings.js',import.meta.url).href,field],{timeout:2000});
});


// Native Codex review 5480207159: shutdown ownership and public data boundaries.
test('SDK shutdown drains admitted background indexing before cache flush and Mongo close',async()=>{
  const mongo=mongoFixture();let release,entered,closed=false;
  const gate=new Promise(resolve=>release=resolve);const started=new Promise(resolve=>entered=resolve);
  const connect=mock.method(MongoClient.prototype,'connect',async function(){return this;});
  const db=mock.method(MongoClient.prototype,'db',()=>mongo.db);
  const close=mock.method(MongoClient.prototype,'close',async()=>{closed=true;});
  const bulk=mongo.hotVectors.bulkWrite;mongo.hotVectors.bulkWrite=async ops=>{assert.equal(closed,false);return bulk.call(mongo.hotVectors,ops);};
  let sdk,result;
  try{await withFetch(async(_,options)=>{entered();await gate;return response(JSON.parse(options.body).input.map(()=>[1,2]));},async()=>{
    sdk=await createOpenPlannerSdk({config:{embedProviderCachePath:undefined,embedProviderBatchWindowMs:1,embeddingModels:{defaultModel:'fixture',bySource:{},byKind:{},byProject:{}},mongodb:openingConfig},log:{warn(){}}});
    sdk.mongo.client.startSession=mongo.client.startSession;
    result=await sdk.ingestEvents([ordinaryEvent('shutdown-owner','Outside information must finish indexing.')]);await started;
    const closing=sdk.close();await new Promise(resolve=>setImmediate(resolve));const premature=closed;
    release();await closing;await result.backgroundIndexing;
    assert.equal(premature,false);assert.equal(closed,true);assert.ok(mongo.hotVectors.rows.size>0);
  });}finally{release?.();await result?.backgroundIndexing;await sdk?.close();connect.mock.restore();db.mock.restore();close.mock.restore();}
});
test('every event kind rejects supplied non-string text before admitting any batch row',async()=>{
  for(const kind of ['message','graph.edge','session'])for(const text of [42,false,{},null,[]]){
    const mongo=mongoFixture();const row=ordinaryEvent('invalid-text','valid');
    await assert.rejects(ingestEvents({mongo,embeddingRuntime:eventRuntime({generate:async texts=>texts.map(()=>[1,2])})},[row,{...row,id:'malformed',kind,text}]),/text.*string/i);
    assert.equal(mongo.events.rows.size,0);assert.equal(mongo.graphEdges.calls.deletes.length,0);
  }
});
for(const search of ['fts','vector'])test(`${search} search refuses non-finite or fractional limits before any storage/provider work`,async()=>{
  for(const value of [NaN,Infinity,-Infinity,'not a number',{},1.5,'',true]){
    const mongo=mongoFixture();let touched=0;mongo.events.find=()=>{touched++;throw new Error('must validate first');};mongo.vectorPartitions.find=()=>{touched++;throw new Error('must validate first');};
    const ctx={mongo,embeddingRuntime:{}};
    await assert.rejects(search==='fts'?ftsSearchWithQuality(ctx,{q:'outside',limit:value}):vectorSearchWithQuality(ctx,{q:'outside',k:value}),/finite.*integer/i);
    assert.equal(touched,0);
  }
});
for(const kind of ['event','compacted'])for(const ttl of [0,60])test(`${kind} retention ignores caller expiry without conflicting update paths (${ttl})`,async()=>{
  const c=collection('expiry');const row={id:'retained',text:'outside',expiresAt:new Date(0),extra:{openplanner_labels:{labels:['keep']}}};
  await (kind==='event'?upsertEvent(c,row,ttl):upsertCompactedMemory(c,row,ttl));
  const update=c.calls.writes[0].update;assert.equal(Object.hasOwn(update.$set,'expiresAt'),false);assert.equal(update.$unset.expiresAt,'');
  await (kind==='event'?upsertEvent(c,{...row,id:'ordinary',extra:{}},ttl):upsertCompactedMemory(c,{...row,id:'ordinary',extra:{}},ttl));
  const ordinary=c.calls.writes[1].update;
  if(ttl===0){assert.equal(Object.hasOwn(ordinary.$set,'expiresAt'),false);assert.equal(ordinary.$unset.expiresAt,'');}
  else{assert.equal(ordinary.$set.expiresAt-ordinary.$set.updatedAt,ttl*1000);assert.equal(ordinary.$unset,undefined);}
});
test('embedding provider base URLs accept ordinary trailing slash configuration',async()=>{
  for(const suffix of ['','/','///'])await withFetch(async(url)=>{assert.equal(url,'http://fixture.invalid/v1/embeddings');return response([[1,2]]);},async()=>{
    assert.deepEqual(await new EmbedProviderFunction('fixture','http://fixture.invalid'+suffix,{batchWindowMs:1}).generate(['outside']),[[1,2]]);
  });
});
for(const [ttl,labels] of [[60,[]],[0,[]],[60,['keep']]])test(`session creation applies event retention and ignores caller expiry (${ttl}/${labels.length})`,async()=>{
  const mongo=mongoFixture();mongo.retention.eventsTtlSeconds=ttl;
  const row=await createProtocols({mongo}).sessionManagement.createSession({session:'owned-session',expiresAt:new Date(0),extra:{openplanner_labels:{labels}},metadata:{kept:true}});
  assert.equal(row.kind,'session');assert.deepEqual(row.metadata,{kept:true});
  if(ttl>0&&!labels.length)assert.equal(row.expiresAt-row.createdAt,ttl*1000);else assert.equal(Object.hasOwn(row,'expiresAt'),false);
  assert.deepEqual([...mongo.events.rows.values()][0],row);
});
test('declared model dimension changes refresh unchanged graph text and bypass the old persistent cache',async()=>{
  const dir=await mkdtemp(join(tmpdir(),'openplanner-model-contract-'));const path=join(dir,'cache.json');
  const mongo=ownedProjectionFixture();const config={...loadConfig(),embedProviderCachePath:path,embedProviderBatchWindowMs:1,embeddingModels:{defaultModel:'fixture',bySource:{},byKind:{},byProject:{},dimensionsByModel:{fixture:2}}};
  const event={schema:'openplanner.event.v1',id:'dimension-owner',ts:'2026-01-01T00:00:00Z',source:'fixture',kind:'graph.node',text:'Stable outside information.',extra:{node_id:'dimension-owner'}};
  let dimensions=2,calls=0,firstRuntime,secondRuntime;
  try{await withFetch(async(_,options)=>{calls++;return response(JSON.parse(options.body).input.map(()=>Array(dimensions).fill(1)));},async()=>{
    firstRuntime=createEmbeddingRuntime(config);const first=await ingestEvents({mongo,embeddingRuntime:firstRuntime},[event]);await first.backgroundIndexing;await firstRuntime.flushCaches();
    const previous=calls;dimensions=3;secondRuntime=createEmbeddingRuntime({...config,embeddingModels:{...config.embeddingModels,dimensionsByModel:{fixture:3}}});
    const next=await ingestEvents({mongo,embeddingRuntime:secondRuntime},[event]);await next.backgroundIndexing;
    assert.ok(calls>previous);const rows=[...mongo.graphNodeEmbeddings.rows.values()];assert.ok(rows.length>0);assert.ok(rows.every(row=>row.embedding_dimensions===3&&row.embedding.length===3));
    const stable=calls;const replay=await ingestEvents({mongo,embeddingRuntime:secondRuntime},[event]);await replay.backgroundIndexing;assert.equal(calls,stable);
  });}finally{await firstRuntime?.flushCaches();await secondRuntime?.flushCaches();await rm(dir,{recursive:true,force:true});}
});
test('declared embedding dimensions reject a provider vector that violates the model contract',async()=>{
  await withFetch(async()=>response([[1,2]]),async()=>{
    const runtime=createEmbeddingRuntime({...loadConfig(),embedProviderCachePath:undefined,embedProviderBatchWindowMs:1,embeddingModels:{defaultModel:'fixture',bySource:{},byKind:{},byProject:{},dimensionsByModel:{fixture:3}}});
    await assert.rejects(runtime.hot.getBackgroundEmbeddingFunctionForModel('fixture').generate(['outside']),/dimension/i);
  });
});
test('dimension configuration preserves positive integer model contracts and refuses malformed declarations',async()=>{
  await withEnv({EMBED_PROVIDER_MODEL_DIMENSIONS:'{"fixture":3,"other":"4"}'},async()=>assert.deepEqual(loadConfig().embeddingModels.dimensionsByModel,{fixture:3,other:4}));
  for(const raw of ['{"fixture":0}','{"fixture":1.5}','{"fixture":"bad"}'])await withEnv({EMBED_PROVIDER_MODEL_DIMENSIONS:raw},async()=>assert.throws(()=>loadConfig(),/dimension/i));
});

// Explicit dimension contracts cannot be partially admitted by a forgiving model-map parser.
for (const raw of ["bad", "[]", "fixture=", "fixture=3;bad", "=3", ";"]) {
  test(`dimension pair configuration rejects silently discarded declaration ${JSON.stringify(raw)}`, async () => {
    await withEnv({ EMBED_PROVIDER_MODEL_DIMENSIONS: raw }, async () => {
      assert.throws(() => loadConfig(), /dimension/i);
    });
  });
}

test("dimension pair configuration preserves own prototype-named model contract", async () => {
  await withEnv({ EMBED_PROVIDER_MODEL_DIMENSIONS: "fixture=3;__proto__=4" }, async () => {
    const dimensions = loadConfig().embeddingModels.dimensionsByModel;
    assert.equal(dimensions.fixture, 3);
    assert.equal(Object.hasOwn(dimensions, "__proto__"), true);
    assert.equal(dimensions.__proto__, 4);
    assert.equal(Object.getPrototypeOf(dimensions), Object.prototype);
  });
});


// Native Codex5480380498 and CodeRabbit5480383537: causal regressions on a27d70e.
for (const [oldLabels, updates, expectedExpiry] of [
  [[], {extra:{openplanner_labels:{labels:['keep']}}}, false],
  [['keep'], {extra:{openplanner_labels:{labels:[]}}}, true],
  [['keep'], {expiresAt:new Date(0)}, false],
  [[], {'extra.openplanner_labels':{labels:['keep']}}, false],
]) test(`session update reconciles managed retention ${JSON.stringify(updates)}`, async () => {
  const mongo=mongoFixture();let stored={_id:'stored',session:'owned',kind:'session',createdAt:new Date(0),extra:{openplanner_labels:{labels:oldLabels}},...(oldLabels.length?{}:{expiresAt:new Date(1)})};
  mongo.events.findOne=async()=>({...stored});
  mongo.events.updateOne=async(filter,update)=>{mongo.events.calls.writes.push({filter,update});for(const [key,value] of Object.entries(update.$set??{})){if(key==='extra.openplanner_labels')stored.extra.openplanner_labels=value;else stored[key]=value;}for(const key of Object.keys(update.$unset??{}))delete stored[key];};
  const before=Date.now();const row=await createProtocols({mongo}).sessionManagement.updateSession('owned',updates);
  assert.equal(Object.hasOwn(row,'expiresAt'),expectedExpiry);
  if(expectedExpiry){assert.ok(row.expiresAt>=new Date(before+60_000));assert.ok(row.expiresAt<=new Date(Date.now()+60_000));}
  const write=mongo.events.calls.writes.at(-1);assert.equal(Object.hasOwn(write.update.$set,'expiresAt')&&Object.hasOwn(write.update.$unset??{},'expiresAt'),false);
});

const models=await import('../dist/embedding-models.js');
for(const scope of [{project:'constructor'},{source:'toString'},{kind:'__proto__'}])test(`embedding scope ignores inherited model ${JSON.stringify(scope)}`,()=>{
  assert.equal(models.resolveEmbeddingModel({defaultModel:'default',byProject:{},bySource:{},byKind:{}},scope),'default');
});
test('model precedence accepts only own nonempty string entries',()=>{
  const config={defaultModel:'default',byProject:{p:42},bySource:{s:''},byKind:{k:'kind'}};
  assert.equal(models.resolveEmbeddingModel(config,{project:'p',source:'s',kind:'k'}),'kind');
  assert.equal(models.resolveEmbeddingModel({...config,byProject:{p:'project'},bySource:{s:'source'}},{project:'p',source:'s',kind:'k'}),'project');
});
for(const raw of ['__proto__=fixture','{"__proto__":"fixture"}'])test(`explicit prototype-named model map remains own ${raw}`,()=>{
  const map=models.parseModelMap(raw);assert.equal(Object.hasOwn(map,'__proto__'),true);assert.equal(map.__proto__,'fixture');assert.equal(Object.getPrototypeOf(map),Object.prototype);
});

function parentQueryFixture(mode, rows) {
  const mongo=queryFixture();const c=mongo.db.collection('healthy_vectors');c.rows.clear();for(const row of rows)c.rows.set(row._id,row);
  const matching=filter=>rows.filter(r=>(filter.quality_label!=='good'||r.quality_label==='good')&&(!(filter.quality_label?.$ne==='bad')||r.quality_label!=='bad')&&(!(filter.parent_id?.$nin?.includes(r.parent_id)))&&(!(filter.parent_id?.$eq)||r.parent_id===filter.parent_id.$eq));
  c.find=filter=>{c.calls.searches.push(filter);let selected=matching(filter);return {sort(){return this;},limit(n){selected=selected.slice(0,n);return this;},batchSize(){return this;},async *[Symbol.asyncIterator](){yield* selected;},toArray:async()=>selected};};
  c.aggregate=pipeline=>({toArray:async()=>{if(mode==='cosine')throw new Error('native unavailable');const query=pipeline[0].$vectorSearch;c.calls.searches.push(query.filter??{});return matching(query.filter??{}).slice(0,query.limit);}});
  return mongo;
}
const vectorRow=(id,parent,score,quality='good')=>({_id:id,parent_id:parent,text:'outside encounter',embedding:[score,Math.sqrt(1-score*score)],embedding_model:'healthy-model',embedding_dimensions:2,quality_label:quality,score,ts:new Date()});
for(const mode of ['native','cosine'])test(`vector ${mode} fills unique parents before final limit`,async()=>{
  const mongo=parentQueryFixture(mode,[vectorRow('a-1','a',1),vectorRow('a-2','a',0.99),vectorRow('a-3','a',0.98),vectorRow('b-1','b',0.9)]);
  const result=await queryMongoVectorsByText({mongo,tier:'hot',q:'outside',k:2,getEmbeddingFunctionForModel:()=>({generate:async()=>[[1,0]]})});
  assert.deepEqual(result.metadatas[0].map(x=>x.parent_id),['a','b']);assert.deepEqual(result.ids,[['a-1','b-1']]);
});
for(const mode of ['native','cosine'])test(`vector ${mode} quality backfill excludes already selected good parents`,async()=>{
  const mongo=parentQueryFixture(mode,[vectorRow('a-1','a',1),vectorRow('a-2','a',0.99),vectorRow('b-1','b',0.9,'neutral'),vectorRow('c-1','c',0.8,'neutral')]);
  const providerFor=()=>({generate:async()=>[[1,0]]});const result=await vectorSearchWithQuality({mongo,embeddingRuntime:{hot:{getEmbeddingFunctionForModel:providerFor},compact:{getEmbeddingFunctionForModel:providerFor}}},{q:'outside',k:3,tier:'hot',quality:'good_then_not_bad'});
  assert.deepEqual(result.result.ids,[['a','b','c']]);assert.ok(mongo.db.collection('healthy_vectors').calls.searches.some(filter=>filter.parent_id?.$nin?.includes('a')));
});
for(const name of ['ftsSearch','ilikeSearch'])test(`${name} snippets truncate at Unicode code point boundaries`,async()=>{
  const storage=await import('../dist/mongodb.js');const text='a'.repeat(239)+'海🙂';let observed;
  const c={find(filter,options){observed=options.projection.text;const [operator,args]=Object.entries(observed)[0];const value=operator==='$substrCP'?Array.from(text).slice(args[1],args[1]+args[2]).join(''):Buffer.from(text).subarray(args[1],args[1]+args[2]).toString('utf8');return {sort(){return this;},limit(){return this;},toArray:async()=>[{id:'unicode',text:value}]};}};
  const rows=await storage[name](c,'a');assert.deepEqual(observed,{$substrCP:['$text',0,240]});assert.equal(rows[0].snippet,'a'.repeat(239)+'海');assert.ok(!rows[0].snippet.includes('\uFFFD'));
});


// Native Codex review5480466640: identity, numeric, time and recall bindings.
test('generated chunk identities cannot alias another parent', async () => {
  const text='First parent outside information. '.repeat(1800);
  const prepared=prepareIndexDocument({parentId:'a',text});assert.ok(prepared.chunkCount>1);
  const other=prepareIndexDocument({parentId:prepared.chunks[0].id,text:'Second parent outside information.'});
  const ids=[...prepared.chunks,...other.chunks].map(row=>row.id);
  assert.equal(new Set(ids).size,ids.length);
});
test('batch indexing preserves distinct parents and their own vectors when an ID looks like a generated chunk', async () => {
  const text='First parent outside information. '.repeat(1800);
  const collision=prepareIndexDocument({parentId:'a',text}).chunks[0].id;
  const mongo=mongoFixture();const result=await batchIndexTextsInMongoVectors({mongo,tier:'hot',items:[item('a',text),item(collision,'Second parent outside information.')],embeddingFunction:{generate:async texts=>texts.map(t=>t.includes('Second parent')?[2,0]:[1,0])}});
  assert.equal(result.failed.length,0);
  const rows=[...mongo.hotVectors.rows.values()];assert.ok(rows.some(row=>row.parent_id==='a'));assert.ok(rows.some(row=>row.parent_id===collision));
  for(const row of rows)assert.deepEqual(row.embedding,row.parent_id===collision?[2,0]:[1,0]);
  assert.equal(rows.length,prepareIndexDocument({parentId:'a',text}).chunkCount+1);
});
for(const input of [{limit:1.5},{skip:0.5},{limit:Infinity},{skip:Infinity}])test('raw browse rejects noninteger pagination before Mongo effects '+JSON.stringify(input),async()=>{
  let effects=0;const cursor={sort(){return this;},skip(value){if(!Number.isSafeInteger(value))throw new Error('fixture driver requires an integer');return this;},limit(value){if(!Number.isSafeInteger(value))throw new Error('fixture driver requires an integer');return this;},toArray:async()=>[]};
  const mongo={db:{collection:()=>({countDocuments:async()=>{effects++;return 0;},find(){effects++;return cursor;}})}};
  await assert.rejects(queryCollectionResponse({mongo},{collection:'events',...input}),/pagination|finite integer/i);assert.equal(effects,0);
});
test('mixed native and fallback vector partitions use one cosine score and distance scale',async()=>{
  const mongo=queryFixture();const native=mongo.db.collection('healthy_vectors');const nativeRow={...vectorRow('orthogonal','native-parent',0),score:0.5};
  native.aggregate=()=>({toArray:async()=>[nativeRow]});
  mongo.vectorPartitions.rows.set('fallback',{_id:'fallback',tier:'hot',model:'fallback-model',dimensions:2,collectionName:'fallback_vectors',searchIndexName:'fallback_index',searchIndexStatus:'ready'});
  const fallback=mongo.db.collection('fallback_vectors');fallback.rows.set('similar',vectorRow('similar','fallback-parent',0.4));fallback.aggregate=()=>{throw new Error('fixture native index unavailable');};
  const result=await queryMongoVectorsByText({mongo,tier:'hot',q:'outside',k:2,getEmbeddingFunctionForModel:()=>({generate:async()=>[[1,0]]})});
  assert.deepEqual(result.ids,[['similar','orthogonal']]);assert.ok(Math.abs(result.distances[0][0]-0.6)<1e-12);assert.equal(result.distances[0][1],1);
});
for(const ts of ['0','2020','01/02/2026','2021-02-30','2021-02-30T00:00:00Z','2026-01-01T24:00:00Z'])test('ingest refuses non-ISO or rollover timestamp before any effects '+ts,async()=>{
  const mongo=mongoFixture();let calls=0;const event={...ordinaryEvent('bad-time','Outside information'),ts};
  await assert.rejects(ingestEvents({mongo,embeddingRuntime:eventRuntime({generate:async()=>{calls++;return [[1,0]];}})},[event]),/timestamp.*ISO/i);
  assert.equal(calls,0);assert.equal(mongo.events.calls.writes.length,0);assert.equal(mongo.graphEdges.calls.writes.length,0);
});
for(const text of [undefined,'','  '])test('preview-only graph metadata cannot create an authority-hashed embedding '+String(text),async()=>{
  const mongo=mongoFixture();const event={...ordinaryEvent('preview-only',text),kind:'graph.node',text,extra:{node_id:'preview-only',preview:'Unadmitted preview-only information'}};
  const result=await ingestEvents({mongo,embeddingRuntime:eventRuntime({generate:async texts=>texts.map(()=>[1,0])})},[event]);await result.backgroundIndexing;
  assert.equal(mongo.graphNodeEmbeddings.rows.size,0);
});
test('explicit graph embedding normalization uses admitted event text rather than alternative HTML metadata',async()=>{
  const mongo=mongoFixture();const text='Admitted outside information.';const event={...ordinaryEvent('html-authority',text),kind:'graph.node',extra:{node_id:'html-authority',html:'<html><body>Hidden unadmitted alternative content.</body></html>'}};
  const result=await ingestEvents({mongo,embeddingRuntime:eventRuntime({generate:async texts=>texts.map(()=>[1,0])})},[event]);await result.backgroundIndexing;
  const row=[...mongo.graphNodeEmbeddings.rows.values()].find(row=>row.node_id===event.id);assert.ok(row);assert.ok(row.text.includes(text));assert.ok(!row.text.includes('Hidden unadmitted'));assert.equal(row.source_text_hash_sha256,createHash('sha256').update(text).digest('hex'));
});
test('long explicit graph nodes materialize an admitted parent vector from bounded chunk embeddings',async()=>{
  const mongo=mongoFixture();const text='Outside memory with several meaningful words. '.repeat(6000);const event={...ordinaryEvent('long-authority',text),kind:'graph.node',extra:{node_id:'long-authority'}};const inputs=[];
  const result=await ingestEvents({mongo,embeddingRuntime:eventRuntime({generate:async texts=>{inputs.push(...texts);return texts.map(()=>[1,0]);}})},[event]);await result.backgroundIndexing;
  const row=[...mongo.graphNodeEmbeddings.rows.values()].find(row=>row.node_id===event.id&&row.source_event_id===event.id);
  assert.ok(row,'parent vector admitted by the scoped recall reader must exist');assert.deepEqual(row.embedding,[1,0]);assert.ok(row.chunk_count>1);assert.equal(row.source_text_hash_sha256,createHash('sha256').update(text).digest('hex'));
  assert.ok(inputs.length>1);assert.ok(inputs.every(value=>value.length<180_100),'provider receives bounded chunks, never the entire long source');
});

// Supplemental controls for the repaired long-source parent, not new causal RED claims.
test('graph parent aggregates every bound chunk and can be reconstructed from reused vectors',async()=>{
  const mongo=ownedProjectionFixture();let calls=0;const provider={generate:async texts=>{calls++;return texts.map((_,i)=>[i+1,2*(i+1)]);}};
  const event={...ordinaryEvent('mean-parent','Distinct outside encounter information. '.repeat(6000)),kind:'graph.node',extra:{node_id:'mean-parent'}};
  const context={mongo,embeddingRuntime:eventRuntime(provider)};const first=await ingestEvents(context,[event]);await first.backgroundIndexing;
  const rows=[...mongo.graphNodeEmbeddings.rows.values()];const parent=rows.find(row=>row.node_id===event.id);
  const chunkIds=new Set([...mongo.events.rows.values()].filter(row=>row.extra?.node_kind==='doc_chunk'&&row.extra?.source_event_id===event.id).map(row=>row.extra.node_id));
  const chunks=rows.filter(row=>chunkIds.has(row.node_id));assert.equal(chunks.length,chunkIds.size);
  assert.ok(chunks.length>1);assert.equal(parent.chunk_count,chunks.length);
  for(let dimension=0;dimension<2;dimension++)assert.ok(Math.abs(parent.embedding[dimension]-chunks.reduce((sum,row)=>sum+row.embedding[dimension],0)/chunks.length)<1e-10);
  const expected=[...parent.embedding];for(const [key,row] of mongo.graphNodeEmbeddings.rows)if(row.node_id===event.id)mongo.graphNodeEmbeddings.rows.delete(key);
  const before=calls;const replay=await ingestEvents(context,[event]);await replay.backgroundIndexing;
  assert.equal(calls,before);assert.deepEqual([...mongo.graphNodeEmbeddings.rows.values()].find(row=>row.node_id===event.id).embedding,expected);
});
test('missing bound graph chunk never materializes a partially aggregated parent',async()=>{
  const mongo=mongoFixture();const warnings=[];const event={...ordinaryEvent('missing-parent','Distinct outside encounter information. '.repeat(6000)),kind:'graph.node',extra:{node_id:'missing-parent'}};
  const result=await ingestEvents({mongo,log:{warn:(data,message)=>warnings.push(String(data.err?.message)+' '+message)},embeddingRuntime:eventRuntime({generate:async texts=>texts.slice(1).map(()=>[1,0])})},[event]);await result.backgroundIndexing;
  assert.equal([...mongo.graphNodeEmbeddings.rows.values()].some(row=>row.node_id===event.id),false);
  assert.ok(warnings.some(value=>value.includes('Graph embedding batch size mismatch')));
});
test('calendar-valid ISO leap day and timezone offsets remain admissible',async()=>{
  for(const ts of ['2024-02-29T23:59:59.999Z','2026-01-01T00:00:00+05:30','2026-01-01T00:00:00-06:00']){
    const mongo=mongoFixture();const result=await ingestEvents({mongo,embeddingRuntime:eventRuntime({generate:async texts=>texts.map(()=>[1,0])})},[{...ordinaryEvent('valid-iso', 'Outside information.'),ts}]);await result.backgroundIndexing;
    assert.ok(mongo.events.calls.writes.length>0);
  }
});

// Native Codex5480582857: causal RED at084b377, before the six production repairs.
for(const label of [0,{},false,null,'  '])test('malformed retention labels refuse the complete ingest batch '+JSON.stringify(label),async()=>{
  const mongo=mongoFixture();let providers=0;const invalid={...ordinaryEvent('bad-label','Outside information.'),extra:{openplanner_labels:{labels:[label]}}};
  await assert.rejects(ingestEvents({mongo,embeddingRuntime:eventRuntime({generate:async texts=>{providers++;return texts.map(()=>[1,0]);}})},[ordinaryEvent('prior','Earlier valid encounter.'),invalid]),/label.*nonblank string/i);
  assert.equal(mongo.events.calls.writes.length,0);assert.equal(mongo.graphEdges.calls.writes.length,0);assert.equal(providers,0);
});
for(const [kind,field] of [['graph.edge','source_node_id'],['graph.edge','target_node_id'],['graph.edge','edge_kind'],['graph.edge','edge_type'],['graph.node','node_id']])for(const value of [42,{},false])test('graph identifier refuses coercion before admission '+kind+'/'+field+'/'+JSON.stringify(value),async()=>{
  const mongo=mongoFixture();let providers=0;const extra=kind==='graph.edge'?{source_node_id:'a',target_node_id:'b',edge_kind:'supports'}:{node_id:'node'};
  const invalid={...ordinaryEvent('bad-graph','Outside information.'),kind,extra:{...extra,[field]:value}};
  await assert.rejects(ingestEvents({mongo,embeddingRuntime:eventRuntime({generate:async texts=>{providers++;return texts.map(()=>[1,0]);}})},[ordinaryEvent('prior','Earlier encounter.'),invalid]),/graph.*identifier.*nonblank string/i);
  assert.equal(mongo.events.calls.writes.length,0);assert.equal(mongo.graphEdges.calls.writes.length,0);assert.equal(providers,0);
});
test('session creation retry preserves one marker and the original stored state',async()=>{
  const mongo=mongoFixture();const sessions=createProtocols({mongo}).sessionManagement;
  const first=await sessions.createSession({session:'stable-session',metadata:{first:true},extra:{openplanner_labels:{labels:['keep']}}});
  const repeated=await sessions.createSession({session:'stable-session',metadata:{first:false},extra:{openplanner_labels:{labels:[]}}});
  assert.equal(mongo.events.rows.size,1);assert.deepEqual(repeated,first);assert.equal(Object.hasOwn(repeated,'expiresAt'),false);
});
for(const name of ['ftsSearchWithQuality','vectorSearchWithQuality'])test(name+' refuses whitespace before storage and provider work',async()=>{
  let effects=0;const c={find(){effects++;throw new Error('unexpected storage access');}};
  const provider={generate:async()=>{effects++;return [[1,0]];}};
  const context={mongo:{events:c,compacted:c,vectorPartitions:c},embeddingRuntime:{hot:{getEmbeddingFunctionForModel(){effects++;return provider;}}}};
  await assert.rejects((name==='ftsSearchWithQuality'?ftsSearchWithQuality:vectorSearchWithQuality)(context,{q:' \t\n ',quality:'any'}),/q is required/i);assert.equal(effects,0);
});

// Native Codex5480976255 exactdfa287c: causal lifecycle/metadata/source/filter/cosine regressions.
for (const field of ['project','session','message']) for (const invalid of [42,{},false])
  test(`source reference ${field} refuses malformed identity ${JSON.stringify(invalid)} before batch IO`,async()=>{
    const mongo=mongoFixture();let providers=0;
    const event={schema:'openplanner.event.v1',id:'source-ref',ts:'2026-01-01T00:00:00Z',source:'fixture',kind:'message',text:'outside'};
    const embeddingRuntime={hot:{getBackgroundEmbeddingFunction(){providers++;return {generate:async texts=>texts.map(()=>[1,0])};},getModel(){return 'fixture';}}};
    await assert.rejects(ingestEvents({mongo,embeddingRuntime},[{...event,id:'first'},{...event,source_ref:{[field]:invalid}}]),/source.reference|source_ref/i);
    assert.equal(mongo.events.calls.writes.length,0);assert.equal(providers,0);
  });
for (const field of ['source','kind','project','session','visibility']) for(const invalid of [{$ne:'expected'},42,false]) for(const fallback of [false,true])
  test(`FTS ${fallback?'fallback':'native'} rejects malformed exact ${field} filter ${JSON.stringify(invalid)} before IO`,async()=>{
    const mongo=mongoFixture();mongo.events.failText=fallback;
    await assert.rejects(ftsSearchWithQuality({mongo},{q:'outside',tier:'hot',quality:'any',[field]:invalid}),/filter.*string|string.*filter/i);
    assert.equal(mongo.events.calls.searches.length,0);
  });
for(const dotted of [
  {'extra.openplanner_labels':{labels:['keep'],quality:'good'}},
  {'extra.openplanner_labels.labels':['keep']},
  {'extra.openplanner_labels':{labels:[],quality:'good'},'extra.openplanner_labels.labels':['keep']},
])test(`session parent/child metadata updates normalize before Mongo ${JSON.stringify(dotted)}`,async()=>{
  const mongo=mongoFixture();let stored={_id:'session-marker',session:'owned',kind:'session',extra:{original:true},expiresAt:new Date(0)};
  mongo.events.findOne=async()=>({...stored});
  mongo.events.updateOne=async(filter,update)=>{
    const keys=Object.keys(update.$set??{});
    if(keys.some(a=>keys.some(b=>b.startsWith(a+'.'))))throw new Error('MongoDB parent and child update path conflict');
    mongo.events.calls.writes.push({filter,update});Object.assign(stored,update.$set);
    for(const field of Object.keys(update.$unset??{}))delete stored[field];
  };
  const result=await createProtocols({mongo}).sessionManagement.updateSession('owned',{extra:{preserved:'outside',openplanner_labels:{labels:[],quality:'neutral'}},...dotted});
  assert.equal(result.extra.preserved,'outside');assert.deepEqual(result.extra.openplanner_labels.labels,['keep']);
  assert.equal(Object.hasOwn(result,'expiresAt'),false);assert.equal(mongo.events.calls.writes.length,1);
  assert.equal(Object.keys(mongo.events.calls.writes[0].update.$set).some(k=>k.startsWith('extra.')),false);
});
for(const ready of [false,true])test(`cosine ${ready?'native-unavailable':'pending-index'} fallback scores older matching documents across the full filtered partition`,async()=>{
  const rows=Array.from({length:1001},(_,i)=>({...vectorRow('new-'+String(i).padStart(4,'0'),'new-parent-'+i,0.1),ts:new Date('2026-10-10T00:00:00Z')}));
  rows.push({...vectorRow('old-perfect','old-perfect-parent',1),ts:new Date('2020-01-01T00:00:00Z')});
  rows.push({...vectorRow('old-next','old-next-parent',0.9),ts:new Date('2019-01-01T00:00:00Z')});
  const mongo=parentQueryFixture('cosine',rows);const partition=mongo.vectorPartitions.rows.get('healthy');
  assert.ok(partition);partition.searchIndexStatus=ready?'ready':'pending';
  const result=await queryMongoVectorsByText({mongo,tier:'hot',q:'outside',k:2,getEmbeddingFunctionForModel:()=>({generate:async()=>[[1,0]]})});
  assert.deepEqual(result.ids,[['old-perfect','old-next']]);assert.deepEqual(result.metadatas[0].map(x=>x.parent_id),['old-perfect-parent','old-next-parent']);
  assert.equal(result.distances[0][0],0);
});

// Native CodeRabbit5480988982 exactdfa: normalization, terminal polling and exact checkout.
for(const batch of [false,true])test(`source redaction normalizes saved HTML once per ${batch?'batch item':'single document'}`,async(t)=>{
  const html='<article>'+Array.from({length:1800},(_,i)=>'<p>Outside encounter '+i+' changes creative perspective with distinct observations.</p>').join('')+'</article>';
  const mongo=mongoFixture();let conversions=0;const original=TurndownService.prototype.turndown;
  t.mock.method(TurndownService.prototype,'turndown',function(...args){conversions++;return original.apply(this,args);});
  const embeddingFunction={generate:async texts=>texts.map(()=>[1,0])};
  const params={mongo,tier:'hot',embeddingFunction};
  if(batch)await batchIndexTextsInMongoVectors({...params,items:[item('html-source',html,{source_path:'fixture-source.html'})]});
  else await indexTextInMongoVectors({...params,parentId:'html-source',text:html,extra:{source_path:'fixture-source.html'},metadata:{embedding_model:'fixture'}});
  const rows=[...mongo.hotVectors.rows.values()];assert.ok(rows.length>2);
  assert.ok(rows.every(row=>row.source_text_redacted&&row.source_coordinate_space==='normalized'));
  // One normalization for embedding chunks and one independent raw-source memo,
  // preserving refusal to treat caller-provided alternative HTML as saved source.
  assert.equal(conversions,2);
});
for(const status of ['FAILED','DOES_NOT_EXIST'])test(`Mongo bootstrap stops graph-index polling immediately at ${status}`,async(t)=>{
  const mongo=mongoFixture();let polls=0,sleeps=0,clock=0;const warnings=[];
  mongo.graphNodeEmbeddings.listSearchIndexes=()=>({toArray:async()=>{polls++;return [{name:'embedding_vector',status,queryable:false}];}});
  t.mock.method(MongoClient.prototype,'connect',async function(){return this;});t.mock.method(MongoClient.prototype,'db',()=>mongo.db);
  t.mock.method(Date,'now',()=>{clock+=10_000;return clock;});
  t.mock.method(globalThis,'setTimeout',resolve=>{sleeps++;queueMicrotask(resolve);return {unref(){},ref(){}};});
  t.mock.method(console,'warn',(...values)=>warnings.push(values.join(' ')));
  await openMongoDB(openingConfig);
  assert.equal(polls,2);assert.equal(sleeps,0);assert.ok(warnings.some(value=>value.includes('unavailable: '+status)));
});
for(const event of ['pull_request','workflow_dispatch'])test(`scoped workflow binds and refuses mismatched actual Git checkout for ${event}`,async()=>{
  const execFile=promisify(execFileCallback);
  const file=new URL('../../../.github/workflows/scoped-graph-recall.yml',import.meta.url);
  const parsed=await execFile('python3',['-c','import yaml,json,sys;w=yaml.safe_load(open(sys.argv[1]));print(json.dumps(w["jobs"]["verify"]["steps"]))',file.pathname]);
  const steps=JSON.parse(parsed.stdout);const checkout=steps.find(step=>String(step.uses??'').startsWith('actions/checkout@'));
  const expected='${{ github.event.pull_request.head.sha || github.sha }}';assert.equal(checkout.with.ref,expected);
  const guard=steps.find(step=>step.name==='Verify selected checkout');assert.ok(guard);assert.equal(guard.env.EXPECTED_HEAD,expected);
  const dir=await mkdtemp(join(tmpdir(),'openplanner-checkout-law-'));
  try{
    await execFile('git',['init','--quiet',dir]);await execFile('git',['-C',dir,'-c','user.name=Fixture','-c','user.email=fixture@example.invalid','commit','--quiet','--allow-empty','-m','selected head']);
    const selected=(await execFile('git',['-C',dir,'rev-parse','HEAD'])).stdout.trim();
    await execFile('git',['-C',dir,'-c','user.name=Fixture','-c','user.email=fixture@example.invalid','commit','--quiet','--allow-empty','-m','different checkout']);
    const different=(await execFile('git',['-C',dir,'rev-parse','HEAD'])).stdout.trim();
    const actualExpected=event==='pull_request'?selected:different;
    await execFile('git',['-C',dir,'checkout','--quiet',actualExpected]);
    const env={PATH:process.env.PATH,EXPECTED_HEAD:actualExpected};
    await execFile('bash',['-e','-c',guard.run],{cwd:dir,env});
    await assert.rejects(execFile('bash',['-e','-c',guard.run],{cwd:dir,env:{...env,EXPECTED_HEAD:event==='pull_request'?different:selected}}),error=>error.code===1);
  }finally{await rm(dir,{recursive:true,force:true});}
});


// Native Codex5481067260 exact62d9fa7: derived-ID ownership and available vector candidates.
for(const reverse of [false,true])test(`derived event collision refuses complete source batch before IO reverse=${reverse}`,async()=>{
  const mongo=mongoFixture();let calls=0;const provider={generate:async texts=>{calls++;return texts.map(()=>[1,0]);}};
  const first=ordinaryEvent('collision-owner','Owned outside encounter.');const second=ordinaryEvent('graph.node:derive:collision-owner','Caller owned source must survive.');
  await assert.rejects(ingestEvents({mongo,embeddingRuntime:eventRuntime(provider)},reverse?[second,first]:[first,second]),/derived.*collision|ownership/i);
  assert.equal(mongo.events.calls.writes.length,0);assert.equal(mongo.graphEdges.calls.writes.length,0);assert.equal(calls,0);
});
for(const generatedFirst of [false,true])test(`derived event collision with persisted source or projection refuses replacement generatedFirst=${generatedFirst}`,async()=>{
  const mongo=mongoFixture();const provider={generate:async texts=>texts.map(()=>[1,0])};const ctx={mongo,embeddingRuntime:eventRuntime(provider)};
  const ordinary=ordinaryEvent('persisted-owner','Outside encounter creates its projection.');const caller=ordinaryEvent('graph.node:derive:persisted-owner','Caller supplied source identity.');
  const before=await ingestEvents(ctx,[generatedFirst?ordinary:caller]);await before.backgroundIndexing;
  const saved=structuredClone([...mongo.events.rows.values()]);const writes=mongo.events.calls.writes.length;
  await assert.rejects(ingestEvents(ctx,[generatedFirst?caller:ordinary]),/derived.*collision|ownership/i);
  assert.equal(mongo.events.calls.writes.length,writes);assert.deepEqual([...mongo.events.rows.values()],saved);
});
test('connection-local derived ID reservation serializes colliding concurrent source identities',async()=>{
  const mongo=mongoFixture();const provider={generate:async texts=>texts.map(()=>[1,0])};const ctx={mongo,embeddingRuntime:eventRuntime(provider)};
  const results=await Promise.allSettled([ingestEvents(ctx,[ordinaryEvent('parallel-owner','First source owns the projection.')]),ingestEvents(ctx,[ordinaryEvent('graph.node:derive:parallel-owner','Second caller-owned source.')])]);
  for(const result of results)if(result.status==='fulfilled')await result.value.backgroundIndexing;
  assert.equal(results.filter(r=>r.status==='fulfilled').length,1);assert.equal(results.filter(r=>r.status==='rejected').length,1);
  assert.equal(mongo.events.rows.get('graph.node:derive:parallel-owner').source,'openplanner-derive');
  assert.equal(mongo.events.rows.get('graph.node:derive:parallel-owner').extra.source_event_id,'parallel-owner');
});
for(const mode of ['native','cosine'])for(const failure of ['missing','escape','changed-hash'])test(`vector ${mode} backfills usable documents after ${failure} hydration`,async()=>{
  const dir=await mkdtemp(join(tmpdir(),'openplanner-query-hydration-'));
  try{await writeFile(join(dir,'source.txt'),'Changed source payload');await withEnv({OPENPLANNER_ROOT:dir},async()=>{
    const unavailable={...vectorRow('unavailable','unavailable-parent',1),text:'',source_text_redacted:true,source_ref:{source_path:failure==='missing'?'absent.txt':failure==='escape'?'../escape.txt':'source.txt'},text_hash_sha256:createHash('sha256').update('Original source payload').digest('hex')};
    const accessible={...vectorRow('accessible','accessible-parent',.9),text:'Accessible independent encounter.'};
    const rows=[unavailable,accessible];const result=await queryMongoVectorsByText({mongo:parentQueryFixture(mode,rows),tier:'hot',q:'outside',k:1,getEmbeddingFunctionForModel:()=>({generate:async()=>[[1,0]]})});
    assert.deepEqual(result.ids,[['accessible']]);assert.deepEqual(result.documents,[['Accessible independent encounter.']]);assert.equal(result.metadatas[0][0].parent_id,'accessible-parent');
  });}finally{await rm(dir,{recursive:true,force:true});}
});
test('native hydration exhaustion backfills beyond initial native candidate window',async()=>{
  const rows=Array.from({length:101},(_,i)=>({...vectorRow('unavailable-'+i,'missing-'+i,1),text:'',source_text_redacted:true,source_ref:{source_path:'/path-not-admitted/missing-'+i},text_hash_sha256:'0'.repeat(64)}));rows.push({...vectorRow('backfilled','accessible-parent',.9),text:'Accessible beyond native candidates.'});
  const result=await queryMongoVectorsByText({mongo:parentQueryFixture('native',rows),tier:'hot',q:'outside',k:1,getEmbeddingFunctionForModel:()=>({generate:async()=>[[1,0]]})});assert.deepEqual(result.ids,[['backfilled']]);assert.deepEqual(result.documents,[['Accessible beyond native candidates.']]);
});
for(const mode of ['native','cosine'])for(const malformed of ['empty','truncated','duplicate','unknown','nonfinite','null-match','non-array'])test(`optional Vexx ${mode} ${malformed} response falls back to complete cosine ranking`,async()=>{
  const rows=[vectorRow('best','best-parent',1),vectorRow('second','second-parent',.8)];let calls=0;
  await withEnv({VEXX_BASE_URL:'https://vexx.fixture.invalid',VEXX_API_KEY:'',VEXX_MIN_CANDIDATES:'1',VEXX_ENFORCE:'false'},async()=>{
    const vectors=await import(new URL('../dist/mongo-vectors.js?'+mode+'-'+malformed,import.meta.url).href);
    await withFetch(async(_url,init)=>{calls++;const request=JSON.parse(init.body);assert.equal(request.candidates.length,2);
      const matches=malformed==='empty'?[]:malformed==='truncated'?[{id:'second',score:1}]:malformed==='duplicate'?[{id:'second',score:1},{id:'second',score:.9}]:malformed==='unknown'?[{id:'best',score:.1},{id:'not-submitted',score:1}]:malformed==='nonfinite'?[{id:'best',score:Infinity},{id:'second',score:1}]:malformed==='null-match'?[null,{id:'second',score:1}]:{};
      return {ok:true,json:async()=>({matches})};
    },async()=>{
      const result=await vectors.queryMongoVectorsByText({mongo:parentQueryFixture(mode,rows),tier:'hot',q:'outside',k:1,getEmbeddingFunctionForModel:()=>({generate:async()=>[[1,0]]})});assert.deepEqual(result.ids,[['best']]);assert.equal(result.distances[0][0],0);
    });
  });assert.ok(calls>0);
});
for(const mode of ['native','cosine'])test(`enforced Vexx ${mode} incomplete response fails explicitly`,async()=>{
  await withEnv({VEXX_BASE_URL:'https://vexx.fixture.invalid',VEXX_API_KEY:'',VEXX_MIN_CANDIDATES:'1',VEXX_ENFORCE:'true'},async()=>{
    const vectors=await import(new URL('../dist/mongo-vectors.js?enforced-'+mode,import.meta.url).href);
    await withFetch(async()=>({ok:true,json:async()=>({matches:[]})}),async()=>assert.rejects(vectors.queryMongoVectorsByText({mongo:parentQueryFixture(mode,[vectorRow('best','best-parent',1),vectorRow('second','second-parent',.8)]),tier:'hot',q:'outside',k:1,getEmbeddingFunctionForModel:()=>({generate:async()=>[[1,0]]})}),/unavailable/i));
  });
});
for(const mode of ['native','cosine'])test(`complete Vexx ${mode} matches remain eligible`,async()=>{
  await withEnv({VEXX_BASE_URL:'https://vexx.fixture.invalid',VEXX_API_KEY:'',VEXX_MIN_CANDIDATES:'1',VEXX_ENFORCE:'false'},async()=>{
    const vectors=await import(new URL('../dist/mongo-vectors.js?complete-'+mode,import.meta.url).href);
    await withFetch(async()=>({ok:true,json:async()=>({matches:[{id:'best',score:.25},{id:'second',score:.75}]})}),async()=>{
      const result=await vectors.queryMongoVectorsByText({mongo:parentQueryFixture(mode,[vectorRow('best','best-parent',1),vectorRow('second','second-parent',.8)]),tier:'hot',q:'outside',k:1,getEmbeddingFunctionForModel:()=>({generate:async()=>[[1,0]]})});assert.deepEqual(result.ids,[['second']]);assert.equal(result.distances[0][0],.25);
    });
  });
});


// Native Codex5481127319: direct SDK admission and provider identity boundaries.
for(const field of ['project','kind'])for(const value of [{$ne:'expected'},['expected'],17,true])test('native a021 session filters refuse '+field+' '+JSON.stringify(value),async()=>{
  const mongo=mongoFixture();let effects=0;const find=mongo.events.find.bind(mongo.events);mongo.events.find=filter=>{effects++;return find(filter);};
  await assert.rejects(createProtocols({mongo}).sessionManagement.getSessionEvents('owned-session',{[field]:value}),/project|kind|string/i);assert.equal(effects,0);
});
test('native a021 session listing refuses operator project before aggregate',async()=>{
  const mongo=mongoFixture();let effects=0;mongo.events.aggregate=()=>{effects++;return {toArray:async()=>[]};};
  await assert.rejects(createProtocols({mongo}).sessionManagement.listSessions({project:{$ne:'expected'},limit:50,offset:0}),/project.*string/i);assert.equal(effects,0);
});
for(const field of ['limit','offset'])for(const value of [1.5,Infinity,NaN,Number.MAX_SAFE_INTEGER+1])test('native a021 session pagination refuses '+field+' '+String(value),async()=>{
  const mongo=mongoFixture();let effects=0;mongo.events.aggregate=()=>{effects++;return {toArray:async()=>[]};};
  await assert.rejects(createProtocols({mongo}).sessionManagement.listSessions({limit:50,offset:0,[field]:value}),/pagination|finite.*integer/i);assert.equal(effects,0);
});
test('native a021 session pagination bounds limits and keeps exact filters',async()=>{
  const mongo=mongoFixture();const pipelines=[];mongo.events.aggregate=pipeline=>{pipelines.push(pipeline);return {toArray:async()=>[]};};const sessions=createProtocols({mongo}).sessionManagement;
  for(const [limit,offset,expected] of [[-2,-2,50],[0,0,50],[1000,0,500],[3,2,3]]){
    await sessions.listSessions({project:'expected',limit,offset});const page=pipelines.at(-2);
    assert.deepEqual(page[0].$match.project,'expected');assert.equal(page.at(-1).$limit,expected);assert.equal(page.at(-2).$skip,Math.max(0,offset));
  }
  const filterCalls=mongo.events.calls.searches;await sessions.getSessionEvents('owned-session',{project:'expected',kind:'message',limit:1});assert.deepEqual(filterCalls.at(-1),{session:'owned-session',project:'expected',kind:'message'});
});
for(const value of [undefined,null,'','   ',false,{$ne:'expected'}])test('native a021 tenant identity rejects '+String(value),async()=>{
  const mongo=mongoFixture();let effects=0;const tenant=mongo.db.collection('tenants');const insert=tenant.insertOne.bind(tenant);tenant.insertOne=doc=>{effects++;return insert(doc);};
  await assert.rejects(createProtocols({mongo}).tenantManagement.createTenant({tenant_id:value,label:'retained'}),/tenant_id.*nonblank.*string/i);assert.equal(effects,0);assert.equal(tenant.rows.size,0);
});
test('native a021 tenant admission preserves a valid identity and metadata',async()=>{
  const mongo=mongoFixture();const tenant=await createProtocols({mongo}).tenantManagement.createTenant({tenant_id:'owned-tenant',label:'retained'});assert.equal(tenant.tenant_id,'owned-tenant');assert.equal(tenant.label,'retained');assert.equal(mongo.db.collection('tenants').rows.size,1);
});
for(const style of ['embeddings','data'])test('native a021 mixed provider dimensions reject the whole '+style+' batch without cached partials',async()=>{
  let calls=0;await withFetch(async()=>new Response(JSON.stringify(style==='embeddings'?{embeddings:++calls===1?[[1,0],[1,0,0]]:[[1,0],[0,1]]}:{data:(++calls===1?[[1,0],[1,0,0]]:[[1,0],[0,1]]).map((embedding,index)=>({embedding,index}))}),{status:200}),async()=>{
    const provider=new EmbedProviderFunction('native-a021-mixed-'+style,'http://fixture.invalid',{batchWindowMs:1,cache:new PersistentEmbeddingCache()});await assert.rejects(provider.generate(['first','second']),/dimension/i);assert.deepEqual(await provider.generate(['first','second']),[[1,0],[0,1]]);assert.equal(calls,2);
  });
});
test('native a021 provider response indices retain text identity through reordered responses and cache',async()=>{
  let calls=0;await withFetch(async()=>{calls++;return new Response(JSON.stringify({data:[{index:1,embedding:[0,1]},{index:0,embedding:[1,0]}]}),{status:200});},async()=>{
    const provider=new EmbedProviderFunction('native-a021-reordered','http://fixture.invalid',{batchWindowMs:1,cache:new PersistentEmbeddingCache()});assert.deepEqual(await provider.generate(['first','second']),[[1,0],[0,1]]);assert.deepEqual(await provider.generate(['first','second']),[[1,0],[0,1]]);assert.equal(calls,1);
  });
});
for(const indices of [[0,0],[0,2],[-1,1],[0,.5],[0,'1'],[0,undefined]])test('native a021 malformed provider indices reject '+JSON.stringify(indices),async()=>{
  await withFetch(async()=>new Response(JSON.stringify({data:indices.map((index,i)=>({index,embedding:i?[0,1]:[1,0]}))}),{status:200}),async()=>{
    const provider=new EmbedProviderFunction('native-a021-indices','http://fixture.invalid',{batchWindowMs:1});await assert.rejects(provider.generate(['first','second']),/index|indices/i);
  });
});
for(const alias of ['content_hash','contentHash'])test('native a021 hydration uses each computed hash before reused caller '+alias,async()=>{
  const dir=await mkdtemp(join(tmpdir(),'native-a021-hydration-'));try{await withEnv({OPENPLANNER_SOURCE_ROOT:dir},async()=>{
    for(const [name,text] of [['first.txt','First independently verified source.'],['second.txt','Second independently verified source.']]){
      await writeFile(join(dir,name),text);const doc={_id:'native-a021-'+alias+'-'+name,parent_id:'native-a021-'+alias+'-'+name,text:'',source_text_redacted:true,source_ref:{source_path:name,[alias]:'native-a021-reused-caller-hash-'+alias},text_hash_sha256:createHash('sha256').update(text).digest('hex')};assert.equal(await hydrateVectorDocumentText(doc),text);
    }
  });}finally{await rm(dir,{recursive:true,force:true});}
});
test('native a021 graph embedding identity keeps delimiter-bearing tuples distinct and updates the same tuple',async()=>{
  const c=collection('native-a021-graph-embeddings');const row=(node_id,embedding_model,embedding)=>({node_id,embedding_model,embedding,embedding_dimensions:2,source_event_id:'source-'+node_id,chunk_index:0,chunk_count:1});
  await upsertGraphNodeEmbeddings(c,[row('a::b','c',[1,0]),row('a','b::c',[0,1])]);assert.equal(c.rows.size,2);assert.deepEqual([...c.rows.values()].map(r=>r.embedding),[[1,0],[0,1]]);
  await upsertGraphNodeEmbeddings(c,[row('a::b','c',[.5,.5])]);assert.equal(c.rows.size,2);assert.deepEqual([...c.rows.values()].find(r=>r.node_id==='a::b').embedding,[.5,.5]);assert.deepEqual([...c.rows.values()].find(r=>r.node_id==='a').embedding,[0,1]);
});
test('native a021 graph expiration declaration permits the actual null wire value and safe narrowing',async()=>{
  const dir=await mkdtemp(join(tmpdir(),'native-a021-graph-types-'));try{
    const graphTypes=new URL('../../graph/graph-claim-core/index.d.ts',import.meta.url).pathname;const fixture=join(dir,'consumer.mts');await writeFile(fixture,`import type { NormalizedEdgeClaimInput } from ${JSON.stringify(graphTypes)};\nconst none: NormalizedEdgeClaimInput['valid_until_ms'] = null;\nexport function format(claim: NormalizedEdgeClaimInput) { return claim.valid_until_ms == null ? 'none' : claim.valid_until_ms.toFixed(0); }\n`);
    await promisify(execFileCallback)(process.execPath,[new URL('../node_modules/typescript/bin/tsc',import.meta.url).pathname,'--noEmit','--strict','--skipLibCheck','--module','NodeNext','--moduleResolution','NodeNext',fixture]);
    // The matching null wire assertion runs in graph-claim-core itself; SDK tests need no built graph runtime.
  }finally{await rm(dir,{recursive:true,force:true});}
});

// Identity repair must preserve the installed tuple index and legacy stored IDs.
test('native a021 graph embedding identity updates a legacy tuple without duplicating its unique index',async()=>{
  const rows=[{_id:'legacy::fixture::2::0',node_id:'legacy',embedding_model:'fixture',embedding_dimensions:2,chunk_index:0,embedding:[1,0]}];
  const fields=['node_id','embedding_model','embedding_dimensions','chunk_index'];
  const c={async bulkWrite(ops){for(const {updateOne:{filter,update}} of ops){const old=rows.find(row=>Object.entries(filter).every(([key,value])=>row[key]===value));const next={...(!old?update.$setOnInsert:{}),...old,...filter,...update.$set};if(!old&&rows.some(row=>fields.every(key=>row[key]===next[key])))throw new Error('fixture unique graph_node_chunk_identity rejects a duplicate tuple');if(old)Object.assign(old,next);else rows.push(next);}}};
  await upsertGraphNodeEmbeddings(c,[{node_id:'legacy',embedding_model:'fixture',embedding_dimensions:2,chunk_index:0,source_event_id:'legacy-source',chunk_count:1,embedding:[0,1]}]);assert.equal(rows.length,1);assert.equal(rows[0]._id,'legacy::fixture::2::0');assert.deepEqual(rows[0].embedding,[0,1]);
});
test('native a021 graph embedding identity preserves null versus empty model tuple values',async()=>{
  const c=collection('native-a021-model-identities');await upsertGraphNodeEmbeddings(c,[{node_id:'same-node',embedding_model:null,embedding_dimensions:2,chunk_index:0,source_event_id:'null-model',chunk_count:1,embedding:[1,0]},{node_id:'same-node',embedding_model:'',embedding_dimensions:2,chunk_index:0,source_event_id:'empty-model',chunk_count:1,embedding:[0,1]}]);assert.equal(c.rows.size,2);assert.deepEqual([...c.rows.values()].map(row=>row.embedding_model),[null,'']);
});

// Native Codex5481241594/5481299578: all sixteen imported-package boundaries.
for (const generated of ['chunk','sentence']) for (const ownership of ['batch','persisted'])
  test(`all generated ${generated} IDs refuse ${ownership} caller ownership before writes`,async()=>{
    const mongo=mongoFixture();let providers=0;
    const text=generated==='chunk'?'A long outside encounter carries many distinct words. '.repeat(5000):'An outside encounter contains enough distinct words.';
    const ev={...ordinaryEvent('generated-source',text),kind:'graph.node',source_ref:{project:'lake'},extra:{node_id:'generated-parent'}};
    const prepared=prepareIndexDocument({parentId:'generated-parent',text,targetChunkTokens:32000,targetChunkChars:180000,overlapChars:1000});
    const [hash]=[...deduplicateByHash(splitSentences(prepared.normalizedText))].find(([,s])=>s.tokens>3);
    const id=generated==='chunk'?`graph.node:doc_chunk:${prepared.chunks[0].id}`:`graph.node:sentence:sentence:${createHash('sha256').update(JSON.stringify(['lake',ev.id,hash])).digest('hex')}`;
    const caller=ordinaryEvent(id,'Caller-owned content remains intact.');
    if(ownership==='persisted')mongo.events.rows.set(id,{...caller,_id:id});
    await assert.rejects(ingestEvents({mongo,embeddingRuntime:eventRuntime({generate:async texts=>{providers++;return texts.map(()=>[1,0]);}})},ownership==='batch'?[ev,caller]:[ev]),/derived.*identity.*collision/i);
    assert.equal(mongo.events.calls.writes.length,0);assert.equal(providers,0);
    if(ownership==='persisted')assert.equal(mongo.events.rows.get(id).text,caller.text);
  });

test('generic hydration keeps distinct real files separate despite a reused caller hash',async()=>{
  const dir=await mkdtemp(join(tmpdir(),'hydration-file-identity-'));
  try{await closeHydrationCache();await withEnv({OPENPLANNER_SOURCE_ROOT:dir},async()=>{
    for(const [name,text]of [['first','First actual file.'],['second','Second actual file.']]){
      await writeFile(join(dir,name),text);
      assert.equal(await loadHydrationSourceText({id:name,project:'lake',text:'',extra:{source_path:name,content_hash:'reused-untrusted-hash'}}),text);
    }
  });}finally{await closeHydrationCache();await rm(dir,{recursive:true,force:true});}
});

// A typed-tuple fixture enforces Mongo's actual unique tuple rule, including legacy IDs.
function semanticTupleCollection(fields,initial=[]){
  const rows=initial.map(row=>({...row}));
  return {rows,async bulkWrite(ops){for(const {updateOne:{filter,update}}of ops){
    const old=rows.find(row=>Object.entries(filter).every(([k,v])=>row[k]===v));
    const next={...(!old?update.$setOnInsert:{}),...old,...filter,...update.$set};
    if(!old&&rows.some(row=>fields.every(k=>row[k]===next[k])))throw new Error('fixture unique semantic tuple rejects duplicate identity');
    if(old)Object.assign(old,next);else rows.push(next);
  }}};
}
for(const force of [false,true])test(`semantic ${force?'force':'edge'} typed tuples avoid delimiter collisions and preserve legacy identities`,async()=>{
  const fields=['source_node_id','target_node_id',...(force?['field_profile','embedding_model']:[])];
  const c=semanticTupleCollection(fields);const write=force?upsertGraphSemanticForceSamples:upsertGraphSemanticEdges;
  const row=(source_node_id,target_node_id)=>({source_node_id,target_node_id,similarity:.5,field_profile:'layout.v1',embedding_model:'fixture'});
  await write(c,[row('a','b||c'),row('a||b','c')]);assert.equal(c.rows.length,2);assert.notEqual(c.rows[0]._id,c.rows[1]._id);
  const legacy=semanticTupleCollection(fields,[{_id:'legacy-retained',...row('old-a','old-b')}]);
  await write(legacy,[{...row('old-b','old-a'),similarity:.9}]);assert.equal(legacy.rows.length,1);assert.equal(legacy.rows[0]._id,'legacy-retained');assert.equal(legacy.rows[0].similarity,.9);
});
test('semantic force model null and empty remain distinct typed identities',async()=>{
  const c=semanticTupleCollection(['source_node_id','target_node_id','field_profile','embedding_model']);
  await upsertGraphSemanticForceSamples(c,[null,''].map(embedding_model=>({source_node_id:'a',target_node_id:'b',field_profile:'layout.v1',embedding_model,similarity:.5})));
  assert.equal(c.rows.length,2);assert.notEqual(c.rows[0]._id,c.rows[1]._id);
});

for(const limit of [NaN,Infinity,-Infinity,1.5,Number.MAX_SAFE_INTEGER+1,'3',null])test(`session event limit refuses ${String(limit)} before a cursor`,async()=>{
  let effects=0;const mongo=mongoFixture();mongo.events.find=()=>{effects++;throw new Error('unexpected cursor');};
  await assert.rejects(createProtocols({mongo}).sessionManagement.getSessionEvents('session',{limit}),/pagination|finite.*integer/i);assert.equal(effects,0);
});
for(const [value,want]of [[undefined,1000],[0,1000],[-1,1000],[2000,1000],[2,2]])test(`session event limit ${String(value)} retains bounded semantics`,async()=>{
  let limit;const mongo=mongoFixture();mongo.events.find=()=>({sort(){return this;},limit(n){limit=n;return this;},toArray:async()=>[]});
  await createProtocols({mongo}).sessionManagement.getSessionEvents('session',{limit:value});assert.equal(limit,want);
});
for(const where of [{project:{$gt:'owned'}},{project:{$eq:{}}},{project:{$in:['owned',{}]}},{project:{$in:[]}},{project:{$eq:'owned',$ne:'other'}},{'project.secret':'owned'},{$or:[]},{unknown:'owned'},['owned']])test(`vector where refuses unsupported scope ${JSON.stringify(where)} before IO`,async()=>{
  const mongo=mongoFixture();let effects=0;mongo.vectorPartitions.find=()=>{effects++;throw new Error('unexpected partition');};
  await assert.rejects(vectorSearchWithQuality({mongo,embeddingRuntime:{}},{q:'outside',quality:'any',where}),/vector.*filter|vector.*where/i);assert.equal(effects,0);
});
for(const [where,want]of [[{project:{$eq:'owned'}},{project:'owned'}],[{project:{$ne:'other'}},{project:{$ne:'other'}}],[{project:{$in:['owned']}},{project:{$in:['owned']}}]])test(`vector where preserves supported operator ${JSON.stringify(where)}`,async()=>{
  const mongo=parentQueryFixture('cosine',[vectorRow('outside','parent',1)]);
  const runtime={hot:{getEmbeddingFunctionForModel:()=>({generate:async()=>[[1,0]]})}};
  await vectorSearchWithQuality({mongo,embeddingRuntime:runtime},{q:'outside',tier:'hot',quality:'any',where});
  assert.deepEqual(mongo.db.collection('healthy_vectors').calls.searches.at(-1),want);
});

for(const labels of [[{}],[17],[true],[''],[' '],['keep',null],{},17])test(`vector labels refuse malformed retention ${JSON.stringify(labels)} before provider or writes`,async()=>{
  const mongo=mongoFixture();let providers=0;
  await assert.rejects(indexTextInMongoVectors({mongo,tier:'hot',parentId:'owned',text:'Outside information.',metadata:{labels},embeddingFunction:{generate:async texts=>{providers++;return texts.map(()=>[1,0]);}}}),/labels?.*nonblank string/i);
  assert.equal(providers,0);assert.equal(mongo.hotVectors.calls.writes.length,0);assert.equal(mongo.vectorPartitions.calls.writes.length,0);
});
for(const field of ['project','session','visibility','source','kind','embedding_model'])for(const value of [{},17,true])test(`vector metadata rejects ${field} ${JSON.stringify(value)} before embedding`,async()=>{
  const mongo=mongoFixture();let providers=0;
  await assert.rejects(indexTextInMongoVectors({mongo,tier:'hot',parentId:'owned',text:'Outside information.',metadata:{[field]:value},embeddingFunction:{generate:async texts=>{providers++;return texts.map(()=>[1,0]);}}}),/metadata.*string|scope.*string/i);
  assert.equal(providers,0);assert.equal(mongo.hotVectors.calls.writes.length,0);
});
for(const batch of [false,true])test(`vector ${batch?'batch':'single'} indexing refuses changing dimensions across provider batches before replacement`,async()=>{
  const mongo=mongoFixture();let calls=0;const text='Outside words carry a long encounter. '.repeat(12000);
  const embeddingFunction={generate:async texts=>texts.map(()=>++calls===1?[1,0]:[1,0,0])};
  if(batch){const result=await batchIndexTextsInMongoVectors({mongo,tier:'hot',items:[item('long-parent',text)],embeddingFunction,config:{embeddingBatchSize:1}});assert.equal(result.indexed,0);assert.equal(result.failed.length,1);assert.match(result.failed[0].error,/dimension/i);}
  else await assert.rejects(indexTextInMongoVectors({mongo,tier:'hot',parentId:'long-parent',text,metadata:{embedding_model:'fixture'},embeddingFunction:{generate:async texts=>{calls++;return texts.map(()=>calls===1?[1,0]:[1,0,0]);}}}),/dimension|invalid embedding/i);
  assert.ok(calls>1);assert.equal(mongo.hotVectors.calls.deletes.length,0);assert.equal(mongo.hotVectors.calls.writes.length,0);
});
test('unsupported Mongo transactions refuse unsafe vector replacement without an untransactional replay',async()=>{
  const mongo=mongoFixture();const previous={...entry('old'),_id:'old',parent_id:'parent'};mongo.hotVectors.rows.set('old',previous);let ended=0;
  mongo.client.startSession=()=>({withTransaction:async()=>{throw new Error('Transaction numbers are only allowed on a replica set member or mongos');},endSession:async()=>{ended++;}});
  await assert.rejects(replaceMongoVectorEntries(mongo,'hot','parent',[entry('new')]),/transaction|replica set/i);
  assert.equal(ended,1);assert.equal(mongo.hotVectors.calls.deletes.length,0);assert.equal(mongo.hotVectors.calls.writes.length,0);assert.deepEqual([...mongo.hotVectors.rows.values()],[previous]);
});
for(const ttl of ['not-a-number','Infinity','0','-1','1.5','9007199254740992'])test(`hydration cache refuses invalid TTL ${ttl} before opening a cache`,async()=>{
  await closeHydrationCache();try{await withEnv({OPENPLANNER_HYDRATION_CACHE_TTL_MS:ttl},async()=>assert.rejects(getHydrationCache(),/hydration.*TTL.*positive|TTL.*safe integer/i));}finally{await closeHydrationCache();}
});

function exactSessionFixture(){
  const mongo=mongoFixture();const c=mongo.events;
  const matches=(row,filter)=>Object.entries(filter).every(([k,v])=>v===null?row[k]==null:row[k]===v);
  c.findOne=async filter=>[...c.rows.values()].find(row=>matches(row,filter))??null;
  c.find=filter=>{let rows=[...c.rows.values()].filter(row=>matches(row,filter));return {limit(n){rows=rows.slice(0,n);return this;},toArray:async()=>rows};};
  c.deleteMany=async filter=>{for(const [id,row]of c.rows)if(matches(row,filter))c.rows.delete(id);};
  return mongo;
}
test('session markers and CRUD isolate identical session strings by project and refuse ambiguous unscoped calls',async()=>{
  const mongo=exactSessionFixture();const s=createProtocols({mongo}).sessionManagement;
  const a=await s.createSession({session:'shared',project:'a'});const b=await s.createSession({session:'shared',project:'b'});
  assert.notEqual(a._id,b._id);assert.equal((await s.getSession('shared',{project:'b'})).project,'b');
  await assert.rejects(s.getSession('shared'),/ambiguous.*project/i);
  await assert.rejects(s.updateSession('shared',{mood:'wrong'}),/ambiguous.*project/i);
  await assert.rejects(s.closeSession('shared'),/ambiguous.*project/i);
  await s.updateSession('shared',{project:'a',mood:'curious'},{project:'b'});
  assert.equal(mongo.events.rows.get(a._id).mood,undefined);assert.equal(mongo.events.rows.get(b._id).project,'b');assert.equal(mongo.events.rows.get(b._id).mood,'curious');
  await s.closeSession('shared',{project:'b'});assert.equal(mongo.events.rows.size,1);assert.equal((await s.getSession('shared')).project,'a');
});
for(const forceChunking of [false,true])test(`token-heavy blocks respect both budgets with forceChunking ${forceChunking}`,()=>{
  const text=Array(400).fill('a').join(' ');const prepared=prepareIndexDocument({parentId:'token-heavy',text,targetChunkTokens:100,targetChunkChars:10000,overlapChars:40,forceChunking});
  assert.ok(prepared.chunkCount>1);for(const c of prepared.chunks){assert.ok(Math.ceil((c.text.match(/\S+/g)?.length??0)*1.35)<=100);assert.ok(c.text.length<=10000);assert.equal(prepared.normalizedText.slice(c.charStart,c.charEnd),c.text);}
});
for(const tier of ['hot','compact'])test(`label removal in ${tier} changes expiry only when it removes the last retained label`,async()=>{
  const mongo=mongoFixture();const c=tier==='hot'?mongo.hotVectors:mongo.compactVectors;const old=new Date('2026-01-01T00:00:00Z');const row={labels:[],expiresAt:old};
  // Execute the native pipeline's array/filter/conditional expressions against original stage values.
  const evaluate=(value,state,vars={})=>{
    if(typeof value==='string'){if(value.startsWith('$$'))return vars[value.slice(2)]??value;if(value.startsWith('$'))return state[value.slice(1)];return value;}
    if(Array.isArray(value))return value.map(v=>evaluate(v,state,vars));if(!value||typeof value!=='object'||value instanceof Date)return value;
    const [op,args]=Object.entries(value)[0];const a=()=>evaluate(args,state,vars);
    if(op==='$isArray')return Array.isArray(a());if(op==='$cond'){const [q,t,f]=args;return evaluate(evaluate(q,state,vars)?t:f,state,vars);}
    if(op==='$size')return a().length;if(op==='$gt'){const [x,y]=a();return x>y;}if(op==='$ne'){const [x,y]=a();return x!==y;}
    if(op==='$filter')return evaluate(args.input,state,vars).filter(item=>evaluate(args.cond,state,{...vars,[args.as]:item}));
    throw new Error('fixture unsupported expression '+op);
  };
  c.updateMany=async(filter,stages)=>{if(filter.labels!==undefined&&!row.labels.includes(filter.labels))return;for(const stage of stages){const updates=Object.fromEntries(Object.entries(stage.$set).map(([k,v])=>[k,evaluate(v,{...row})]));for(const [k,v]of Object.entries(updates)){if(v==='$$REMOVE')delete row[k];else row[k]=v;}}};
  await removeMongoVectorParentLabel(mongo,'parent','keep');assert.equal(row.expiresAt,old);
  row.labels=['keep'];await removeMongoVectorParentLabel(mongo,'parent','keep');const expiry=row.expiresAt;assert.ok(expiry>old);assert.deepEqual(row.labels,[]);
  await removeMongoVectorParentLabel(mongo,'parent','keep');assert.equal(row.expiresAt,expiry);
});
test('SDK shutdown retains the shared hydration cache until the last SDK owner closes',async()=>{
  await closeHydrationCache();const mongo=mongoFixture();const connect=mock.method(MongoClient.prototype,'connect',async function(){return this;});const db=mock.method(MongoClient.prototype,'db',()=>mongo.db);const close=mock.method(MongoClient.prototype,'close',async()=>{});const sdks=[];
  const dir=await mkdtemp(join(tmpdir(),'sdk-owned-hydration-'));
  try{await withEnv({OPENPLANNER_HYDRATION_LMDB_PATH:dir},async()=>{
    for(let i=0;i<2;i++)sdks.push(await createOpenPlannerSdk({config:{embedProviderCachePath:undefined,mongodb:openingConfig}}));
    const before=await getHydrationCache();await cachePut(before,'owned-proof','persistent');
    await sdks[0].close();assert.equal(await getHydrationCache(),before);assert.equal(await cacheGet(before,'owned-proof'),'persistent');
    await sdks[1].close();assert.notEqual(await getHydrationCache(),before);
  });}finally{for(const sdk of sdks)await sdk.close();await closeHydrationCache();connect.mock.restore();db.mock.restore();close.mock.restore();await rm(dir,{recursive:true,force:true});}
});
for(const count of [0,-1,NaN,Infinity,1.5,Number.MAX_SAFE_INTEGER+1,'2'])test(`embedding pool refuses invalid worker count ${String(count)}`,()=>{
  assert.throws(()=>new ParallelEmbeddingPool('fixture','https://fixture.invalid',{workerCount:count}),/workerCount.*positive.*integer/i);
});
test('embedding pool keeps the default and positive worker rotation',()=>{
  const pool=new ParallelEmbeddingPool('fixture','https://fixture.invalid',{workerCount:2});const a=pool.getWorker(),b=pool.getWorker();assert.notEqual(a,b);assert.equal(pool.getWorker(),a);
  assert.ok(new ParallelEmbeddingPool('fixture','https://fixture.invalid').getWorker());
});

// Native Codex5481359467: ten follow-up public storage boundaries.
test('full session history preserves more than one thousand ascending events',async()=>{
  const mongo=mongoFixture();const rows=Array.from({length:2002},(_,i)=>({_id:'event-'+i,session:'history',ts:new Date(1767225600000+i),text:'encounter '+i}));
  mongo.events.find=()=>{let found=[...rows];return {sort(spec){found.sort((a,b)=>(a.ts-b.ts)*spec.ts);return this;},limit(n){found=found.slice(0,n);return this;},toArray:async()=>found};};
  const result=await getSessionResponse({mongo},'history',{mode:'full'});
  assert.equal(result.rows.length,2002);assert.equal(result.rows[0]._id,'event-0');assert.equal(result.rows.at(-1)._id,'event-2001');
});

test('projectless session markers remain exactly addressable alongside a scoped marker',async()=>{
  const mongo=exactSessionFixture();const sessions=createProtocols({mongo}).sessionManagement;
  const projectless=await sessions.createSession({session:'shared-projectless'});
  const scoped=await sessions.createSession({session:'shared-projectless',project:'owned'});
  await assert.rejects(sessions.getSession('shared-projectless'),/ambiguous/i);
  assert.equal((await sessions.getSession('shared-projectless',{project:null}))._id,projectless._id);
  await sessions.updateSession('shared-projectless',{mood:'curious'},{project:null});
  assert.equal(mongo.events.rows.get(projectless._id).mood,'curious');assert.equal(mongo.events.rows.get(scoped._id).mood,undefined);
  await sessions.closeSession('shared-projectless',{project:null});
  assert.equal(mongo.events.rows.has(projectless._id),false);assert.equal(mongo.events.rows.has(scoped._id),true);
});

for(const unsupported of [{parent_id:{$gt:'x'}},{parent_id:{$eq:'x',$ne:'y'}},{parent_id:{$in:[{$ne:'x'}]}},{parent_id:{$in:[]}},{parent_id:null},{parent_id:NaN},{unknown:'x'},{$where:'true'},{'extra.scope':'x'}])
  test('bulk vector deletion refuses the whole lossy predicate '+JSON.stringify(unsupported),async()=>{
    const mongo=mongoFixture();let accessed=0;const original=mongo.db.collection;
    mongo.db.collection=name=>{accessed++;return original(name);};
    await assert.rejects(deleteMongoVectorEntriesByFilter(mongo,'hot',{project:'owned',...unsupported}),/filter|unsupported/i);
    assert.equal(accessed,0);assert.equal(mongo.hotVectors.calls.deletes.length,0);assert.equal(mongo.compactVectors.calls.deletes.length,0);
  });
test('bulk vector deletion retains every admitted conjunct',async()=>{
  const mongo=mongoFixture();const filter={project:'owned',parent_id:{$in:['first','second']},kind:{$ne:'excluded'}};
  await deleteMongoVectorEntriesByFilter(mongo,'hot',filter);assert.deepEqual(mongo.hotVectors.calls.deletes[0],filter);assert.equal(mongo.compactVectors.calls.deletes.length,0);
});

for(const id of [{$ne:'victim'},42,null,undefined,'','   '])for(const batch of [false,true])
  test(`public event admission rejects selector identity ${JSON.stringify(id)} batch=${batch} before any write`,async()=>{
    const mongo=mongoFixture();const admission=createProtocols({mongo}).eventAdmission;
    const bad={...ordinaryEvent('bad','outside'),id};
    await assert.rejects(batch?admission.appendEvents([ordinaryEvent('good','outside'),bad]):admission.appendEvent(bad),/id.*nonblank.*string/i);
    assert.equal(mongo.events.calls.writes.length,0);assert.equal(mongo.events.rows.size,0);
  });

for(const method of ['getTenant','updateTenant','deleteTenant','getPolicy','setPolicy'])for(const id of [{$ne:'victim'},42,null,undefined,'','   '])
  test(`tenant ${method} rejects selector identity ${JSON.stringify(id)} before collection access`,async()=>{
    const mongo=mongoFixture();let accessed=0;mongo.db.collection=()=>{accessed++;throw new Error('collection accessed before identity admission');};
    await assert.rejects(createProtocols({mongo}).tenantManagement[method](id,{name:'change'}),/tenant.*nonblank.*string/i);assert.equal(accessed,0);
  });

test('embedding cache distinguishes colliding model and dimension tuple spellings',async()=>{
  const first=makeEmbeddingCacheKey({model:'m::dimensions:3',text:'same encounter'});
  const second=makeEmbeddingCacheKey({model:'m',dimensions:3,text:'same encounter'});
  const cache=new PersistentEmbeddingCache();await cache.putMany([{key:first,vector:[1,0,0]},{key:second,vector:[0,1,0]}]);
  assert.notEqual(first,second);assert.equal(cache.size,2);assert.deepEqual((await cache.getMany([first,second])).get(first),[1,0,0]);
  assert.deepEqual((await cache.getMany([first,second])).get(second),[0,1,0]);
});

function migrationIdentityFixture(){
  const rows=new Map();return {rows,async updateOne(filter,update){
    const old=[...rows.values()].find(row=>Object.entries(filter).every(([key,value])=>row[key]===value));
    const next={...(!old?update.$setOnInsert:{}),...old,...filter,...update.$set};rows.set(next._id,next);
  }};
}
test('migration queue preserves distinct tuples containing delimiters and retry state',async()=>{
  const collection=migrationIdentityFixture();const a={entity:'a',object_id:'b:c',trigger:'d'};const b={entity:'a:b',object_id:'c',trigger:'d'};
  await enqueueMigrationJob(collection,a);await enqueueMigrationJob(collection,b);assert.equal(collection.rows.size,2);
  const before=[...collection.rows.values()].find(row=>row.entity==='a');before.status='running';before.attempts=3;
  await enqueueMigrationJob(collection,{...a,priority:4});assert.equal(collection.rows.size,2);
  const after=[...collection.rows.values()].find(row=>row.entity==='a');assert.equal(after._id,before._id);assert.equal(after.createdAt,before.createdAt);assert.equal(after.status,'running');assert.equal(after.attempts,3);
});
test('migration enqueue retains a stored legacy job ID and creation time',async()=>{
  const collection=migrationIdentityFixture();const legacy={_id:'legacy:opaque:id',entity:'a',object_id:'b',trigger:'c',createdAt:new Date(0),attempts:5,status:'failed'};
  collection.rows.set(legacy._id,legacy);await enqueueMigrationJob(collection,{entity:'a',object_id:'b',trigger:'c',priority:2});
  assert.equal(collection.rows.size,1);assert.equal(collection.rows.get(legacy._id).createdAt,legacy.createdAt);assert.equal(collection.rows.get(legacy._id).attempts,5);
});
test('migration tuple selectors refuse operator or blank components before Mongo access',async()=>{
  let writes=0;const storage={async updateOne(){writes++;throw new Error('unadmitted migration storage access');}};
  for(const field of ['entity','object_id','trigger'])for(const value of [{$ne:'victim'},null,'   ']) {
    await assert.rejects(enqueueMigrationJob(storage,{entity:'a',object_id:'b',trigger:'c',[field]:value}),/migration.*nonblank.*string/i);
    assert.equal(writes,0);
  }
});

test('session pages use stable project/session ordering when every timestamp ties',async()=>{
  const mongo=mongoFixture();let calls=0;
  const all=[{project:'b',session:'one'},{project:'a',session:'two'},{project:'a',session:'one'},{project:'c',session:'one'}].map(row=>({...row,last_ts:new Date(0),event_count:1}));
  mongo.events.aggregate=pipeline=>({toArray:async()=>{
    if(pipeline.some(stage=>stage.$count))return [{total:all.length}];
    let rows=calls++%2?[...all].reverse():[...all];const spec=pipeline.find(stage=>stage.$sort).$sort;
    rows.sort((a,b)=>{for(const [key,direction]of Object.entries(spec)){const comparison=a[key]instanceof Date?a[key]-b[key]:String(a[key]).localeCompare(String(b[key]));if(comparison)return comparison*direction;}return 0;});
    const offset=pipeline.find(stage=>stage.$skip)?.$skip??0;const limit=pipeline.find(stage=>stage.$limit).$limit;return rows.slice(offset,offset+limit);
  }});
  const sessions=createProtocols({mongo}).sessionManagement;
  const first=await sessions.listSessions({limit:2,offset:0}),second=await sessions.listSessions({limit:2,offset:2});
  assert.deepEqual([...first.rows,...second.rows].map(row=>[row.project,row.session]),[['a','one'],['a','two'],['b','one'],['c','one']]);
  assert.equal(new Set([...first.rows,...second.rows].map(row=>JSON.stringify([row.project,row.session]))).size,4);
});

for(const timestamp of ['not-a-date','',null,false,{},new Date(NaN)])for(const operation of ['single','batch','direct'])
  test(`vector ${operation} rejects supplied invalid timestamp ${JSON.stringify(timestamp)} before provider/storage`,async()=>{
    const mongo=mongoFixture();let generated=0;const embeddingFunction={generate:async texts=>{generated++;return texts.map(()=>[1,2]);}};
    const metadata={embedding_model:'fixture',ts:timestamp};
    const result=operation==='single'?indexTextInMongoVectors({mongo,tier:'hot',parentId:'parent',text:'outside',metadata,embeddingFunction})
      :operation==='batch'?batchIndexTextsInMongoVectors({mongo,tier:'hot',items:[{...item('parent'),metadata}],embeddingFunction})
      :upsertMongoVectorDocuments(mongo,'hot',[{...entry(),metadata}]);
    await assert.rejects(result,/timestamp|metadata.ts/i);assert.equal(generated,0);assert.equal(mongo.hotVectors.calls.writes.length,0);assert.equal(mongo.compactVectors.calls.writes.length,0);
  });

test('generated ownership preflight batches a large document and rejects a late collision before writes',async()=>{
  const mongo=mongoFixture();const text=Array.from({length:310},(_,i)=>`Outside encounter number ${i} contains enough distinct creative words.`).join(' ');
  const ev={...ordinaryEvent('batch-source',text),kind:'graph.node',source_ref:{project:'lake'},extra:{node_id:'batch-parent'}};
  const prepared=prepareIndexDocument({parentId:'batch-parent',text,targetChunkTokens:32000,targetChunkChars:180000,overlapChars:1000});
  const sentences=[...deduplicateByHash(splitSentences(prepared.normalizedText))].filter(([,s])=>s.tokens>3);assert.ok(sentences.length>256);
  const hash=sentences.at(-1)[0];const conflict=`graph.node:sentence:sentence:${createHash('sha256').update(JSON.stringify(['lake',ev.id,hash])).digest('hex')}`;
  mongo.events.rows.set(conflict,{_id:conflict,source:'caller',text:'protected outside ownership'});
  let scalarGeneratedReads=0,providers=0;const batches=[];const original=mongo.events.findOne;
  mongo.events.findOne=async filter=>{if(filter._id!==ev.id)scalarGeneratedReads++;return original(filter);};
  mongo.events.find=filter=>{assert.ok(Array.isArray(filter._id?.$in));batches.push([...filter._id.$in]);const rows=[...mongo.events.rows.values()].filter(row=>filter._id.$in.includes(row._id));return {project(){return this;},toArray:async()=>rows};};
  await assert.rejects(ingestEvents({mongo,embeddingRuntime:eventRuntime({generate:async texts=>{providers++;return texts.map(()=>[1,0]);}})},[ev]),/derived.*ownership.*collision/i);
  assert.equal(scalarGeneratedReads,0);assert.ok(batches.length>=2);assert.ok(batches.every(batch=>batch.length>0&&batch.length<=256));assert.equal(new Set(batches.flat()).size,batches.flat().length);
  assert.equal(mongo.events.calls.writes.length,0);assert.equal(providers,0);assert.equal(mongo.events.rows.get(conflict).text,'protected outside ownership');
});

// CodeRabbit5481374361: empty no-op cleanup and explicit full-history cap.
test('standalone empty vector cleanup lets every graph/empty event and deferred projection finish',async()=>{
  const mongo=mongoFixture();let started=0;
  mongo.client.startSession=()=>{started++;return {withTransaction:async()=>{throw Object.assign(new Error('Transaction numbers are only allowed on a replica set member or mongos'),{code:20});},endSession:async()=>{}};};
  const edge={...ordinaryEvent('edge-without-vectors',''),kind:'graph.edge',extra:{source_node_id:'a',target_node_id:'b',edge_kind:'related'}};
  const empty=ordinaryEvent('empty-without-vectors','');
  const result=await ingestEvents({mongo,embeddingRuntime:{}},[edge,empty]);await result.backgroundIndexing;
  assert.equal(result.count,2);assert.ok(mongo.events.rows.has(edge.id));assert.ok(mongo.events.rows.has(empty.id));
  // The ordinary fixture also owns its keep-label projection. Both deferred
  // relations must survive cleanup; counting only the explicit edge hid it.
  assert.equal(result.projectedGraphEdges,2);assert.equal(mongo.graphEdges.rows.size,2);
  const relations=[...mongo.graphEdges.rows.values()];
  assert.ok(relations.some(row=>row.source_node_id==='a'&&row.target_node_id==='b'&&row.edge_kind==='related'&&row.data.source_event_id===edge.id));
  assert.ok(relations.some(row=>row.source_node_id===empty.id&&row.edge_kind==='has_label'&&row.data.label==='keep'&&row.data.source_event_id===empty.id&&typeof row.target_node_id==='string'&&row.target_node_id.length>0));
  assert.equal(started,0);
});
for(const location of ['flat','partition'])test(`owned vector cleanup in ${location} retains the required transaction and old row on standalone refusal`,async()=>{
  const mongo=mongoFixture();let started=0;const row={_id:'owned-chunk',parent_id:'owned-parent',text:'existing'};
  const collection=location==='flat'?mongo.hotVectors:mongo.db.collection('owned-partition');collection.rows.set(row._id,row);
  if(location==='partition')mongo.vectorPartitions.rows.set('partition',{tier:'hot',collectionName:'owned-partition',updatedAt:new Date(0)});
  mongo.client.startSession=()=>{started++;return {withTransaction:async()=>{throw Object.assign(new Error('Transaction numbers are only allowed on a replica set member or mongos'),{code:20});},endSession:async()=>{}};};
  await assert.rejects(replaceMongoVectorEntries(mongo,'hot','owned-parent',[]),/Transaction.*replica/i);
  assert.equal(started,1);assert.equal(collection.rows.get(row._id),row);assert.equal(collection.calls.deletes.length,0);
});
test('full history signals actual rows beyond its compatible cap using one extra observation',async()=>{
  const mongo=mongoFixture();const row={session:'history',ts:new Date(0),text:'encounter'};let observedLimit;
  mongo.events.find=()=>({sort(){return this;},limit(n){observedLimit=n;return this;},toArray:async()=>Array(Math.min(observedLimit,100001)).fill(row)});
  const result=await getSessionResponse({mongo},'history',{mode:'full'});
  assert.equal(result.rows.length,100000);assert.equal(result.has_more,true);assert.equal(result.limit,100000);assert.equal(observedLimit,100001);
  mongo.events.find=()=>({sort(){return this;},limit(){return this;},toArray:async()=>Array(100000).fill(row)});
  const complete=await getSessionResponse({mongo},'history',{mode:'full'});assert.equal(complete.has_more,false);assert.equal(complete.rows.length,100000);
});


// Actual Codex5481432710 on64d5b84: eight scoped read/write boundaries.
function scopedHistoryFixture(rows){
  const mongo=mongoFixture();for(const row of rows)mongo.events.rows.set(row._id,row);
  let invocation=0;mongo.events.find=filter=>{
    let values=[...rows].filter(row=>row.session===filter.session&&(!Object.hasOwn(filter,'project')||(filter.project===null?row.project==null:row.project===filter.project))&&(!filter.kind||row.kind===filter.kind));
    if(invocation++%2)values.reverse();
    return {project(){return this;},sort(spec){values.sort((a,b)=>{for(const [key,direction]of Object.entries(spec)){const x=a[key],y=b[key];const compare=x instanceof Date?x-y:String(x).localeCompare(String(y));if(compare)return compare*direction;}return 0;});return this;},limit(n){values=values.slice(0,n);return this;},toArray:async()=>values};
  };return mongo;
}
const historyRows=()=>[['lake','a'],['forest','b'],[null,'c'],['','d']].map(([project,_id])=>({_id,session:'shared',project,ts:new Date(0),kind:'knoxx.message',text:_id}));
for(const mode of ['full','resume','visibility'])test(`Codex64d: ${mode} refuses ambiguous unscoped history`,async()=>{
  const mongo=scopedHistoryFixture(historyRows());await assert.rejects(getSessionResponse({mongo},'shared',{mode}),/ambiguous.*project/i);
});
test('Codex64d: direct history refuses cross-project mixing and exact null/empty selectors isolate rows',async()=>{
  const mongo=scopedHistoryFixture(historyRows()),sessions=createProtocols({mongo}).sessionManagement;
  await assert.rejects(sessions.getSessionEvents('shared',{}),/ambiguous.*project/i);
  for(const [project,id]of [['lake','a'],['forest','b'],[null,'c'],['','d']]){
    assert.deepEqual((await sessions.getSessionEvents('shared',{project})).map(x=>x._id),[id]);
    assert.deepEqual((await getSessionResponse({mongo},'shared',{mode:'full',project})).rows.map(x=>x._id),[id]);
  }
});
test('Codex64d: unscoped history with one actual project remains usable without session markers',async()=>{
  const mongo=scopedHistoryFixture(historyRows().filter(row=>row.project==='lake'));
  assert.deepEqual((await getSessionResponse({mongo},'shared',{mode:'resume'})).rows.map(x=>x._id),['a']);
});
for(const direction of ['asc','desc'])test(`Codex64d: ${direction} history ties retain the same bounded rows on repeated reads`,async()=>{
  const rows=['z','a','m','b'].map(_id=>({_id,session:'ties',project:'lake',ts:new Date(0)}));const mongo=scopedHistoryFixture(rows),sessions=createProtocols({mongo}).sessionManagement;
  const expected=direction==='asc'?['a','b']:['z','m'];
  for(let i=0;i<3;i++)assert.deepEqual((await sessions.getSessionEvents('ties',{project:'lake',limit:2,sort:direction})).map(x=>x._id),expected);
});
test('Codex64d: ownerless graph edges preserve complete project and delimiter identities',async()=>{
  const mongo=mongoFixture(),rows=[{source_node_id:'a',target_node_id:'b',edge_kind:'related',project:'lake'},{source_node_id:'a',target_node_id:'b',edge_kind:'related',project:'forest'},{source_node_id:'a',target_node_id:'b',edge_kind:'related',project:null},{source_node_id:'a',target_node_id:'b',edge_kind:'related',project:''},{source_node_id:'a||b',target_node_id:'c',edge_kind:'related',project:'lake'},{source_node_id:'a',target_node_id:'b||c',edge_kind:'related',project:'lake'}];
  const c=mongo.graphEdges;c.bulkWrite=async ops=>{for(const {updateOne:{filter,update}} of ops){c.calls.writes.push({filter,update});let old;
    if(filter._id)old=c.rows.get(filter._id);else old=[...c.rows.values()].find(row=>['source_node_id','target_node_id','edge_kind','project'].every(key=>(row[key]??null)===(filter[key]??null))&&(row.data?.source_event_id??null)===filter['data.source_event_id']);
    const next={...(!old?update.$setOnInsert:{}),...old,...update.$set};next._id=old?._id??filter._id??next._id;c.rows.set(next._id,next);
  }};
  await upsertGraphEdges(c,rows);assert.equal(c.rows.size,6);assert.equal(new Set([...c.rows.values()].map(row=>JSON.stringify([row.project,row.source_node_id,row.target_node_id,row.edge_kind]))).size,6);
  const saved=[...c.rows.values()].find(row=>row.project==='lake'&&row.source_node_id==='a'&&row.target_node_id==='b');c.rows.delete(saved._id);saved._id='existing-ownerless-legacy';c.rows.set(saved._id,saved);
  await upsertGraphEdges(c,[{...rows[0],source:'refreshed'}]);assert.equal(c.rows.size,6);assert.equal(c.rows.get('existing-ownerless-legacy').source,'refreshed');
});
for(const filter of ['scope',[],null,undefined,42,false,new Date(0),new Map(),/scope/])test(`Codex64d: browse rejects explicitly malformed filter ${String(filter)}`,async()=>{
  let accessed=0;const cursor={sort(){return this;},skip(){return this;},limit(){return this;},toArray:async()=>[]};const mongo={db:{collection(){accessed++;return {countDocuments:async()=>0,find:()=>cursor};}}};
  await assert.rejects(queryCollectionResponse({mongo},{collection:'events',filter}),/filter.*plain object|plain object.*filter/i);assert.equal(accessed,0);
});
for(const embedding of [[],[NaN],[Infinity],[-Infinity],[1,'2'],Array(2)])for(const tier of ['hot','compact'])test(`Codex64d: ${tier} direct vector admission rejects nonfinite/empty/sparse embedding ${JSON.stringify(embedding)}`,async()=>{
  const mongo=mongoFixture();await assert.rejects(upsertMongoVectorDocuments(mongo,tier,[entry('good'),{...entry('bad'),embedding}]),/embedding.*finite|embedding.*nonempty/i);
  assert.equal(mongo.vectorPartitions.calls.writes.length,0);assert.equal(mongo.hotVectors.calls.writes.length,0);assert.equal(mongo.compactVectors.calls.writes.length,0);assert.equal(mongo.vectorPartitions.calls.searches.length,0);
});
test('Codex64d: mismatched replacement parent cannot delete its victim or set up foreign partitions',async()=>{
  const mongo=mongoFixture();let started=0;mongo.hotVectors.rows.set('protected',{_id:'protected',parent_id:'victim'});mongo.client.startSession=()=>{started++;return {withTransaction:async f=>f(),endSession:async()=>{}};};
  await assert.rejects(replaceMongoVectorEntries(mongo,'hot','victim',[{...entry('own'),parentId:'victim'},{...entry('foreign'),parentId:'other'}]),/parent/i);
  assert.equal(started,0);assert.equal(mongo.vectorPartitions.calls.writes.length,0);assert.equal(mongo.hotVectors.calls.deletes.length,0);assert.equal(mongo.hotVectors.rows.get('protected').parent_id,'victim');
});
for(const parentId of [{$ne:'victim'},[],42,false,null,' '])for(const operation of [addMongoVectorParentLabel,removeMongoVectorParentLabel])test(`Codex64d: ${operation.name} rejects malformed parent before any storage access ${JSON.stringify(parentId)}`,async()=>{
  const mongo=mongoFixture();let accessed=0;const hot=mongo.hotVectors;Object.defineProperty(mongo,'hotVectors',{get(){accessed++;return hot;}});
  await assert.rejects(operation(mongo,parentId,'keep'),/parent.*nonblank string|nonblank string.*parent/i);assert.equal(accessed,0);assert.equal(hot.calls.writes.length,0);assert.equal(mongo.compactVectors.calls.writes.length,0);assert.equal(mongo.vectorPartitions.calls.searches.length,0);
});
for(const tenant of [{},[],42,false,null,' '])for(const field of ['nested','fallback'])test(`Codex64d: batch label projection rejects malformed ${field} tenant before writes ${JSON.stringify(tenant)}`,async()=>{
  const mongo=mongoFixture();let provider=0;const bad=ordinaryEvent('bad-tenant','');if(field==='nested')bad.extra.openplanner_labels.tenant_id=tenant;else bad.extra.tenant_id=tenant;
  await assert.rejects(ingestEvents({mongo,embeddingRuntime:eventRuntime({generate:async texts=>{provider++;return texts.map(()=>[1,0]);}})},[ordinaryEvent('preceding-valid',''),bad]),/tenant.*nonblank string|nonblank string.*tenant/i);
  assert.equal(provider,0);assert.equal(mongo.events.calls.writes.length,0);assert.equal(mongo.graphEdges.calls.writes.length,0);assert.equal(mongo.graphLabelNodes.calls.writes.length,0);
});

// Companion checks protect the typed selectors introduced by these repairs.
test('Codex64d companion: typed ownerless graph selectors reject operator-valued identities before writes',async()=>{
  for(const field of ['source_node_id','target_node_id','edge_kind','project','owner']){
    const mongo=mongoFixture();const row={source_node_id:'a',target_node_id:'b',edge_kind:'related',project:'lake'};
    if(field==='owner')row.data={source_event_id:{$ne:'owner'}};else row[field]={$ne:'protected'};
    await assert.rejects(upsertGraphEdges(mongo.graphEdges,[{source_node_id:'good-a',target_node_id:'good-b',edge_kind:'related'},row]),/graph edge.*string/i);
    assert.equal(mongo.graphEdges.calls.writes.length,0);assert.equal(mongo.graphEdges.rows.size,0);
  }
});
test('Codex64d companion: direct vector identifiers reject operators before any partition setup',async()=>{
  for(const field of ['id','parentId'])for(const value of [{$ne:'protected'},' ',null]){
    const mongo=mongoFixture();await assert.rejects(upsertMongoVectorDocuments(mongo,'hot',[entry('good'),{...entry('bad'),[field]:value}]),/vector.*nonblank string/i);
    assert.equal(mongo.vectorPartitions.calls.writes.length,0);assert.equal(mongo.hotVectors.calls.writes.length,0);assert.equal(mongo.vectorPartitions.calls.searches.length,0);
  }
});
test('Codex64d companion: inferred malformed legacy project never becomes a Mongo query operator',async()=>{
  const row={_id:'protected',session:'legacy',project:{$ne:'lake'},kind:'knoxx.message',ts:new Date(0)};const mongo=scopedHistoryFixture([row]);let reads=0;mongo.events.find=()=>{reads++;throw new Error('unsafe inferred selector was used');};
  await assert.rejects(createProtocols({mongo}).sessionManagement.getSessionEvents('legacy',{}),/project.*string/i);assert.equal(reads,0);assert.equal(mongo.events.rows.get('protected'),row);
});

// Actual native Codex5481490146 on 2b01e5c: exported data boundaries and shutdown.
for(const id of [{$ne:'victim'},null,17,'',' ']) test(`Codex2b: direct event writer refuses malformed ID ${JSON.stringify(id)}`,async()=>{
  const c=collection('direct-events',[{_id:'victim',id:'victim',text:'retained'}]);
  await assert.rejects(upsertEvent(c,{...ordinaryEvent('valid','outside'),id},60),/event.*id|id.*event/i);
  assert.equal(c.calls.writes.length,0);assert.equal(c.rows.get('victim').text,'retained');
});
for(const where of [new Date('2026-01-01'),new Map(),new (class Scope {})()]) test(`Codex2b: vector where refuses nonplain ${where.constructor.name}`,async()=>{
  const mongo=parentQueryFixture('cosine',[vectorRow('outside','parent',1)]);let generated=0;
  const runtime={hot:{getEmbeddingFunctionForModel:()=>({generate:async()=>{generated++;return [[1,0]];}})}};
  await assert.rejects(vectorSearchWithQuality({mongo,embeddingRuntime:runtime},{q:'outside',tier:'hot',quality:'any',where}),/vector.*filter|vector.*where/i);
  assert.equal(generated,0);assert.equal(mongo.vectorPartitions.calls.searches.length,0);
});
for(const projection of [null,'id',['id'],new Date('2026-01-01'),new (class Projection {})()]) test(`Codex2b: browse rejects malformed explicit projection ${JSON.stringify(projection)}`,async()=>{
  let effects=0;const mongo={db:{collection(){effects++;return {countDocuments:async()=>0,find:()=>({sort(){return this;},skip(){return this;},limit(){return this;},toArray:async()=>[]})};}}};
  await assert.rejects(queryCollectionResponse({mongo},{collection:'events',projection}),/projection.*plain|plain.*projection/i);assert.equal(effects,0);
});
test('Codex2b: SDK close drains admitted collection listing and refuses a later listing',async()=>{
  const mongo=mongoFixture();let release,closed=false,starts=0;const gate=new Promise(resolve=>release=resolve);
  mongo.db.listCollections=()=>({toArray:async()=>{starts++;await gate;assert.equal(closed,false,'admitted listing must precede Mongo close');return [{name:'events'}];}});
  const connect=mock.method(MongoClient.prototype,'connect',async function(){return this;});const db=mock.method(MongoClient.prototype,'db',()=>mongo.db);const close=mock.method(MongoClient.prototype,'close',async()=>{closed=true;});let sdk;
  try{sdk=await createOpenPlannerSdk({config:{embedProviderCachePath:undefined,mongodb:openingConfig}});
    const listing=sdk.listCollections();const closing=sdk.close();await new Promise(resolve=>setImmediate(resolve));const premature=closed;
    release();const result=await Promise.allSettled([listing,closing]);assert.equal(premature,false);assert.equal(result[0].status,'fulfilled');assert.deepEqual(result[0].value,['events']);
    await assert.rejects(sdk.listCollections(),/closing/i);assert.equal(starts,1);assert.equal(closed,true);
  }finally{release();if(sdk)await sdk.close();connect.mock.restore();db.mock.restore();close.mock.restore();}
});
for(const project of [{$ne:'owned'},null,17,true,['owned']]) test(`Codex2b: session list refuses malformed supplied project ${JSON.stringify(project)}`,async()=>{
  const mongo=mongoFixture();let aggregates=0;mongo.events.aggregate=()=>{aggregates++;return {toArray:async()=>[]};};
  await assert.rejects(listSessionsResponse({mongo},{project}),/project.*string|string.*project/i);assert.equal(aggregates,0);
});
for(const overlapChars of [0,2,30]) test(`Codex2b: repetitive chunks preserve actual offsets with overlap ${overlapChars}`,()=>{
  const prepared=prepareIndexDocument({parentId:'repetitive',text:'abc '.repeat(50),forceChunking:true,targetChunkChars:20,targetChunkTokens:100,overlapChars});
  assert.ok(prepared.chunks.length>1);let start=0;
  for(const chunk of prepared.chunks){const end=Math.min(start+20,prepared.normalizedText.length);let boundedEnd=end;
    if(end<prepared.normalizedText.length){const boundary=prepared.normalizedText.lastIndexOf(' ',end-1);if(boundary>start)boundedEnd=boundary;}
    const raw=prepared.normalizedText.slice(start,boundedEnd);const expectedStart=start+raw.length-raw.trimStart().length;
    assert.equal(chunk.charStart,expectedStart);assert.equal(chunk.charEnd,expectedStart+raw.trim().length);assert.equal(prepared.normalizedText.slice(chunk.charStart,chunk.charEnd),chunk.text);
    if(boundedEnd===prepared.normalizedText.length)break;start=Math.max(start+1,boundedEnd-Math.min(overlapChars,Math.floor((boundedEnd-start)/2)));
  }
});
for(const search of ['fts','vector'])for(const tier of ['hot ','unknown',null,{},17])test(`Codex2b: ${search} refuses unsupported explicit tier ${JSON.stringify(tier)}`,async()=>{
  const mongo=parentQueryFixture('cosine',[vectorRow('outside','parent',1)]);let effects=0;mongo.events.find=()=>{effects++;throw new Error('unexpected search IO');};mongo.compacted.find=mongo.events.find;
  const runtime={hot:{getEmbeddingFunctionForModel:()=>({generate:async()=>{effects++;return [[1,0]];}})},compact:{getEmbeddingFunctionForModel:()=>({generate:async()=>{effects++;return [[1,0]];}})}};
  await assert.rejects((search==='fts'?ftsSearchWithQuality:vectorSearchWithQuality)({mongo,embeddingRuntime:runtime},{q:'outside',quality:'any',tier}),/tier.*hot|unsupported.*tier/i);assert.equal(effects,0);
});
for(const kind of ['event','compacted'])for(const labels of ['keep',{keep:true},null])test(`Codex2b: ${kind} retention refuses nonarray explicit labels ${JSON.stringify(labels)}`,async()=>{
  const c=collection('retention');const value={...ordinaryEvent('retained','outside'),extra:{openplanner_labels:{labels}}};
  await assert.rejects((kind==='event'?upsertEvent:upsertCompactedMemory)(c,value,60),/retention.*labels|labels.*array/i);assert.equal(c.calls.writes.length,0);
});
for(const field of ['eventsTtlSeconds','compactedTtlSeconds'])for(const ttl of [NaN,Infinity,-1,1.5])test(`Codex2b: Mongo open refuses invalid ${field}=${ttl} before reconciliation`,async()=>{
  const mongo=mongoFixture();let connections=0;const connect=mock.method(MongoClient.prototype,'connect',async function(){connections++;return this;});const db=mock.method(MongoClient.prototype,'db',()=>mongo.db);const close=mock.method(MongoClient.prototype,'close',async()=>{});
  try{await assert.rejects(openMongoDB({...openingConfig,[field]:ttl}),/TTL|retention/i);assert.equal(connections,0);assert.ok([...mongo.collections.values()].every(c=>c.calls.indexes.length===0&&c.calls.writes.length===0&&c.calls.drops.length===0));}
  finally{connect.mock.restore();db.mock.restore();close.mock.restore();}
});

// Native Codex5481523582 on1e7f755: whole-batch identity and provider contracts.
for(const id of [{$ne:'victim'},null,17,'',' '])test(`Codex1e: direct compacted writer refuses malformed ID ${JSON.stringify(id)}`,async()=>{
  const c=collection('direct-compacted',[{_id:'victim',id:'victim',text:'retained'}]);await assert.rejects(upsertCompactedMemory(c,{...ordinaryEvent('valid','outside'),id},60),/memory.*id|id.*memory/i);assert.equal(c.calls.writes.length,0);assert.equal(c.rows.get('victim').text,'retained');
});
for(const [field,value]of [['node_id',{$ne:'victim'}],['node_id',''],['embedding_model',{$ne:null}],['embedding_dimensions',{$gt:0}],['embedding_dimensions',NaN],['chunk_index',{$gte:0}],['chunk_index',-1]])test(`Codex1e: graph embedding refuses malformed tuple ${field}=${JSON.stringify(value)} before whole batch writes`,async()=>{
  const c=collection('embeddings');const valid={node_id:'owned',source_event_id:'event',embedding_model:'fixture',embedding_dimensions:2,embedding:[1,0],chunk_index:0,chunk_count:1};await assert.rejects(upsertGraphNodeEmbeddings(c,[valid,{...valid,[field]:value}]),/graph.*embedding|embedding.*identity|embedding.*tuple/i);assert.equal(c.calls.writes.length,0);
});
for(const field of ['source','kind','project','session','visibility'])test(`Codex1e: FTS preserves explicit empty exact ${field}`,async()=>{
  const mongo=mongoFixture();await ftsSearchWithQuality({mongo,embeddingRuntime:{}},{q:'outside',quality:'any',tier:'hot',[field]:''});assert.equal(mongo.events.calls.searches[0][field==='visibility'?'extra.visibility':field],'');
});
for(const search of ['fts','vector'])for(const quality of ['goodd',null,{},17])test(`Codex1e: ${search} rejects unsupported explicit quality ${JSON.stringify(quality)}`,async()=>{
  const mongo=parentQueryFixture('cosine',[vectorRow('outside','parent',1)]);let effects=0;mongo.events.find=()=>{effects++;throw new Error('unexpected IO');};mongo.compacted.find=mongo.events.find;const runtime={hot:{getEmbeddingFunctionForModel:()=>({generate:async()=>{effects++;return [[1,0]];}})}};
  await assert.rejects((search==='fts'?ftsSearchWithQuality:vectorSearchWithQuality)({mongo,embeddingRuntime:runtime},{q:'outside',tier:'hot',quality}),/quality.*mode|unsupported.*quality/i);assert.equal(effects,0);
});
for(const write of ['upsert','replace'])test(`Codex1e: ${write} rejects duplicate vector IDs before partitions or writes`,async()=>{
  const mongo=mongoFixture();const entries=[{...entry('shared'),embedding:[1,0],metadata:{embedding_model:'model-a'}},{...entry('shared'),embedding:[1,0,0],metadata:{embedding_model:'model-b'}}];let effects=0;mongo.db.collection=()=>{effects++;throw new Error('unexpected storage');};
  await assert.rejects(write==='upsert'?upsertMongoVectorDocuments(mongo,'hot',entries):replaceMongoVectorEntries(mongo,'hot','parent',entries),/duplicate.*id|id.*duplicate/i);assert.equal(effects,0);assert.equal(mongo.vectorPartitions.calls.writes.length,0);
});
for(const raw of ['{"scope":','{"scope":17}','{"scope":"fixture","other":null}','scope=fixture;broken','scope=','["fixture"]'])test(`Codex1e: embedding routing rejects entire malformed declaration ${raw}`,async()=>{
  const {parseModelMap}=await import('../dist/embedding-models.js');assert.throws(()=>parseModelMap(raw),/model.*routing|routing.*model|model.*map/i);
});
test('Codex1e: overflow fragments must have equal dimensions and cannot poison the original cache',async()=>{
  let failing=true,requests=0;await withFetch(async(_url,opts)=>{requests++;const text=JSON.parse(opts.body).input[0];if(!failing)return response([[0,0,1]]);if(text.length>1)return new Response('context window exceeded',{status:400});return response([text==='a'?[1,0]:[0,1,2]]);},async()=>{
    const provider=new EmbedProviderFunction('fixture','http://fixture.invalid',{batchWindowMs:1,cache:new PersistentEmbeddingCache()});await assert.rejects(provider.generate(['ab']),/dimension/i);failing=false;assert.deepEqual(await provider.generate(['ab']),[[0,0,1]]);assert.ok(requests>=5);
  });
});

// Native CodeRabbit5481545352/4239918959: BSON Date precision survives tier merging.
for (const fallback of [false,true]) for (const quality of ['any','good_then_not_bad']) test(`CodeRabbit1e: ${fallback?'substring':'FTS'} ${quality} preserves BSON milliseconds before the limit`,async()=>{
  const mongo=mongoFixture();const row=(id,ts)=>({id,ts:new Date(ts),extra:{openplanner_labels:{quality:'good'}}});
  mongo.events=collection('events',[row('a-older','2026-10-11T00:00:00.100Z')]);mongo.compacted=collection('compacted',[row('z-newer','2026-10-11T00:00:00.900Z')]);mongo.events.failText=fallback;mongo.compacted.failText=fallback;
  const result=await ftsSearchWithQuality({mongo},{q:'outside',tier:'both',quality,limit:1});assert.deepEqual(result.rows.map(row=>row.id),['z-newer']);assert.equal(result.ftsEnabled,!fallback);
});

// Actual native Codex5481584632 on7e: whole-batch identity, scope and provenance admission.
for(const bad of [{$ne:'victim'},[],null,42,'',' ']) {
 test(`layout whole batch refuses malformed identity ${JSON.stringify(bad)}`,async()=>{
  let effects=0;const c={bulkWrite:async()=>{effects++}};
  await assert.rejects(upsertGraphLayoutOverrides(c,[{node_id:'valid',x:1,y:2},{node_id:bad,x:3,y:4}]),/node.*id|identifier/i);assert.equal(effects,0);
 });
 for(const field of ['source_node_id','target_node_id'])for(const writer of [upsertGraphSemanticEdges,upsertGraphSemanticForceSamples])test(`${writer.name} whole batch refuses ${field} ${JSON.stringify(bad)}`,async()=>{
  let effects=0;const c={bulkWrite:async()=>{effects++}};const row={source_node_id:'a',target_node_id:'b',similarity:.5};
  await assert.rejects(writer(c,[row,{...row,[field]:bad}]),/node.*id|identifier/i);assert.equal(effects,0);
 });
}
for(const field of ['field_profile','embedding_model'])for(const bad of [{$ne:'victim'},[],42,false])test(`force tuple refuses ${field} ${JSON.stringify(bad)}`,async()=>{
 let effects=0;const c={bulkWrite:async()=>{effects++}};const row={source_node_id:'a',target_node_id:'b',similarity:.5};
 await assert.rejects(upsertGraphSemanticForceSamples(c,[row,{...row,[field]:bad}]),/profile|model|string/i);assert.equal(effects,0);
});
test('force tuple preserves null-model and explicit empty-model/profile distinctions',async()=>{
 let ops;await upsertGraphSemanticForceSamples({bulkWrite:async x=>{ops=x}},[{source_node_id:'b',target_node_id:'a',similarity:.5},{source_node_id:'a',target_node_id:'b',similarity:.5,embedding_model:'',field_profile:''}]);
 assert.deepEqual(ops.map(x=>x.updateOne.filter),[{source_node_id:'a',target_node_id:'b',field_profile:'layout.v1',embedding_model:null},{source_node_id:'a',target_node_id:'b',field_profile:'',embedding_model:''}]);
});
test('session listing keeps null/missing projects distinct from explicit empty project and exact filters',async()=>{
 const mongo=mongoFixture();const pipelines=[];mongo.events.aggregate=p=>{pipelines.push(p);return{toArray:async()=>[]}};
 const sessions=createProtocols({mongo}).sessionManagement;await sessions.listSessions({project:'',limit:50,offset:0});
 for(const p of pipelines){assert.equal(p[0].$match.project,'');assert.deepEqual(p.find(x=>x.$group).$group._id.project,{$ifNull:['$project',null]});}
});
test('vector query admits legacy ISO timestamps without discarding healthy results',async()=>{
 const rows=[{...vectorRow('legacy','legacy',1),ts:'2026-01-01T00:00:00.123Z'},vectorRow('healthy','healthy',.9)];const{mongo}=streamingQueryFixture(rows.length,i=>rows[i]);
 const result=await streamingQuery({queryMongoVectorsByText},mongo,2);assert.deepEqual(result.ids,[['legacy','healthy']]);assert.equal(result.metadatas[0][0].ts,'2026-01-01T00:00:00.123Z');
});
for(const bad of [undefined,'invalid',new Date(NaN),42,{}])test(`vector query skips unusable timestamp ${String(bad)} before rank limit`,async()=>{
 const rows=[{...vectorRow('invalid','invalid',1),ts:bad},vectorRow('healthy','healthy',.9)];const{mongo}=streamingQueryFixture(rows.length,i=>rows[i]);
 const result=await streamingQuery({queryMongoVectorsByText},mongo,1);assert.deepEqual(result.ids,[['healthy']]);assert.equal(result.partial,true);assert.equal(result.discarded_invalid_timestamp_count,1);
});
for(const search of [ftsSearch,ilikeSearch])test(`direct ${search.name} returns explicitly bound compact provenance`,async()=>{
 const c={collectionName:'custom_compacted',find:()=>({sort(){return this},limit(){return this},toArray:async()=>[{id:'compact',text:'outside'}]})};
 const compact=await search(c,'outside',{tier:'compact'});assert.equal(compact[0].tier,'compact');const hot=await search(c,'outside',{tier:'hot'});assert.equal(hot[0].tier,'hot');
 c.collectionName='compacted_memories';assert.equal((await search(c,'outside'))[0].tier,'compact');
});

// Actual native Codex5481623446 on309: metadata, identity and cross-copy transaction laws.
for(const model of [undefined,null,'',' '])for(const replace of [false,true])test(`direct vector ${replace?'replace':'upsert'} refuses missing model ${String(model)} before effects`,async()=>{
 const mongo=mongoFixture();const row={...entry('missing-model'),metadata:{embedding_model:model}};
 await assert.rejects(replace?replaceMongoVectorEntries(mongo,'hot','parent',[row]):upsertMongoVectorDocuments(mongo,'hot',[row]),/embedding.model|model.*nonblank/i);
 assert.equal(mongo.hotVectors.rows.size,0);assert.equal(mongo.vectorPartitions.rows.size,0);
});
for(const field of ['project','session','visibility'])test(`direct vector scope retains explicit empty ${field}`,async()=>{
 const mongo=mongoFixture();const row=entry('empty-'+field);row.metadata[field]='';await upsertMongoVectorDocuments(mongo,'hot',[row]);assert.equal(mongo.hotVectors.rows.get(row.id)[field],'');
});
for(const fallback of [false,true])test(`legacy FTS identity backfill excludes _id with fallback=${fallback}`,async()=>{
 const mongo=mongoFixture();const rows=[{_id:'legacy',ts:new Date(2),text:'outside',extra:{openplanner_labels:{quality:'good'}}},{id:'other',_id:'storage-other',ts:new Date(1),text:'outside',extra:{openplanner_labels:{quality:'neutral'}}}];
 mongo.events.find=filter=>{if(fallback&&filter.$text)throw Error('FTS absent');let values=rows.filter(r=>filter['extra.openplanner_labels.quality']!=='good'||r.extra.openplanner_labels.quality==='good');values=values.filter(r=>!filter.id?.$nin?.includes(r.id));
  if(filter.$expr){const expr=filter.$expr.$not[0].$in;assert.deepEqual(expr[0].$convert.input,{$ifNull:['$id','$_id']});values=values.filter(r=>!expr[1].$literal.includes(String(r.id??r._id)));}
  return{sort(){return this},limit(n){values=values.slice(0,n);return this},toArray:async()=>values};};
 const result=await ftsSearchWithQuality({mongo},{q:'outside',tier:'hot',quality:'good_then_not_bad',limit:2});assert.deepEqual(result.rows.map(r=>r.id??r._id),['legacy','other']);
});
test('direct session event scope retains explicitly empty kind',async()=>{
 const mongo=mongoFixture();await createProtocols({mongo}).sessionManagement.getSessionEvents('shared',{project:'owned',kind:'',limit:10});assert.equal(mongo.events.calls.searches.at(-1).kind,'');
});
test('native vector projection retains schema metadata identical to cosine reads',async()=>{
 const row={...vectorRow('schema','parent',1),schema_version:'fixture.v2',migration_state:'complete'};const mongo=parentQueryFixture('native',[row]);const c=mongo.db.collection('healthy_vectors');
 c.aggregate=pipeline=>({toArray:async()=>{const projection=pipeline.find(s=>s.$project).$project;const doc=Object.fromEntries(Object.entries(row).filter(([k])=>projection[k]===1));doc.score=1;return[doc];}});
 const native=await streamingQuery({queryMongoVectorsByText},mongo);assert.equal(native.metadatas[0][0].schema_version,'fixture.v2');assert.equal(native.metadatas[0][0].migration_state,'complete');
});
for(const operation of [addMongoVectorParentLabel,removeMongoVectorParentLabel])for(const failAt of [0,1,2,3])test(`${operation.name} rolls back all four copies after write failure ${failAt}`,async()=>{
 const mongo=mongoFixture();const stores=[mongo.hotVectors,mongo.compactVectors,mongo.db.collection('label-hot'),mongo.db.collection('label-compact')];const token={};let ended=0;let active=false;
 mongo.vectorPartitions.rows.set('label-hot',{_id:'label-hot',tier:'hot',collectionName:'label-hot',updatedAt:new Date()});mongo.vectorPartitions.rows.set('label-compact',{_id:'label-compact',tier:'compact',collectionName:'label-compact',updatedAt:new Date()});
 for(const c of stores)c.rows.set('copy',{_id:'copy',parent_id:'parent',labels:['keep'],expiresAt:new Date(100)});
 const before=stores.map(c=>structuredClone([...c.rows]));
 const session={withTransaction:async work=>{active=true;try{return await work();}catch(error){stores.forEach((c,i)=>{c.rows.clear();for(const [id,row]of before[i])c.rows.set(id,row);});throw error;}finally{active=false;}},endSession:async()=>{ended++},token};mongo.client.startSession=()=>session;
 stores.forEach((c,i)=>c.updateMany=async(_filter,_update,opts)=>{if(active)assert.equal(opts?.session,session,'all effects bind the same transaction session');if(i===failAt)throw Error('actual fixture write failure');c.rows.get('copy').labels=operation===addMongoVectorParentLabel?['keep','new']:[];c.rows.get('copy').expiresAt=null;});
 await assert.rejects(operation(mongo,'parent',operation===addMongoVectorParentLabel?'new':'keep'),/actual fixture write failure/);assert.deepEqual(stores.map(c=>[...c.rows]),before);assert.equal(ended,1);
});

for(const operation of [addMongoVectorParentLabel,removeMongoVectorParentLabel])test(`${operation.name} refuses unavailable transactions without any untransactional replay`,async()=>{
 const mongo=mongoFixture();let effects=0,ended=0;mongo.hotVectors.updateMany=mongo.compactVectors.updateMany=async()=>{effects++};
 mongo.client.startSession=()=>({withTransaction:async()=>{throw Error('replica-set transaction unavailable')},endSession:async()=>{ended++}});
 await assert.rejects(operation(mongo,'parent','keep'),/transaction unavailable/);assert.equal(effects,0);assert.equal(ended,1);
});

// Actual native Codex5481674801 on1a: bounded query and projection identities.
for(const k of [201,1000,Number.MAX_SAFE_INTEGER])for(const mode of ['native','cosine'])test(`Codex1a: direct ${mode} vector retention is capped at200 for ${k}`,async()=>{
  const mongo=parentQueryFixture(mode,Array.from({length:250},(_,i)=>vectorRow('bounded-'+i,'parent-'+i,1-i/1000)));
  let nativeBounds;if(mode==='native'){const c=mongo.db.collection('healthy_vectors');const aggregate=c.aggregate;c.aggregate=function(pipeline){nativeBounds=pipeline[0].$vectorSearch;return aggregate.call(this,pipeline);};}
  const result=await streamingQuery({queryMongoVectorsByText},mongo,k);if(mode==='native'){assert.equal(nativeBounds.limit,4000);assert.equal(nativeBounds.numCandidates,4000);}assert.equal(result.ids[0].length,200);assert.equal(new Set(result.metadatas[0].map(row=>row.parent_id)).size,200);
});
for(const field of ['x','y'])for(const bad of [NaN,Infinity,-Infinity,'3',{},null,undefined])test(`Codex1a: whole layout batch refuses ${field}=${String(bad)} before a write`,async()=>{
  let writes=0;const c={bulkWrite:async()=>{writes++;}};
  await assert.rejects(upsertGraphLayoutOverrides(c,[{node_id:'valid',x:1,y:2},{node_id:'invalid',x:3,y:4,[field]:bad}]),/coordinate|finite.*number/i);assert.equal(writes,0);
});
for(const chunked of [false,true])test(`Codex1a: shared explicit ${chunked?'chunked':'single'} node waits for the earlier detached ingest`,async()=>{
  const mongo=vectorDeletionFixture();let release,entered;const gate=new Promise(resolve=>release=resolve),started=new Promise(resolve=>entered=resolve);let secondAdmitted=false;
  const embeddingRuntime=eventRuntime({generate:async texts=>{if(texts.some(text=>text.includes('first version'))){entered();await gate;}return texts.map(()=>[1,2]);}});
  const firstText=chunked?'first version '.repeat(15000):'The first version remains pending.';
  const secondText='The second version is authoritative.';
  const secondNode=chunked?prepareIndexDocument({parentId:'shared-explicit',text:firstText,targetChunkTokens:32000,targetChunkChars:180000,overlapChars:1000}).chunks[0].id:'shared-explicit';
  const node=text=>({...ordinaryEvent(text===firstText?'explicit-first':'explicit-second',text),kind:'graph.node',source_ref:{project:text===firstText?'old-project':'current-project'},extra:{node_id:text===firstText?'shared-explicit':secondNode,openplanner_labels:{labels:['keep']}}});
  const first=await ingestEvents({mongo,embeddingRuntime},[node(firstText)]);await started;
  try{
    const secondPromise=ingestEvents({mongo,embeddingRuntime},[node(secondText)]).then(result=>{secondAdmitted=true;return result;});
    await new Promise(resolve=>setTimeout(resolve,15));const premature=secondAdmitted;release();const second=await secondPromise;await Promise.all([first.backgroundIndexing,second.backgroundIndexing]);
    assert.equal(premature,false);const parent=[...mongo.graphNodeEmbeddings.rows.values()].filter(row=>row.node_id===secondNode);assert.ok(parent.length>0);assert.ok(parent.every(row=>row.source_event_id==='explicit-second'&&row.project==='current-project'&&row.text.includes('second version')));
    if(chunked)assert.ok(prepareIndexDocument({parentId:'shared-explicit',text:firstText,targetChunkTokens:32000,targetChunkChars:180000,overlapChars:1000}).chunks.length>1);
  }finally{release();}
});
test('Codex1a: session updates cannot mutate the public event ID',async()=>{
  const mongo=mongoFixture();mongo.events.rows.set('stored-marker',{_id:'stored-marker',id:'public-marker',session:'owned-session',project:null,kind:'session'});
  mongo.events.findOne=async filter=>[...mongo.events.rows.values()].find(row=>Object.entries(filter).every(([key,value])=>value===null?row[key]==null:row[key]===value))??null;
  const row=await createProtocols({mongo}).sessionManagement.updateSession('owned-session',{id:'other-event',title:'New title'});
  assert.equal(row.id,'public-marker');assert.equal(row._id,'stored-marker');assert.equal(row.title,'New title');assert.equal(Object.hasOwn(mongo.events.calls.writes.at(-1).update.$set,'id'),false);
});
for(const parentId of ['', ' ', '\t'])test(`Codex1a: invalid parent${JSON.stringify(parentId)} refuses before provider/cache or Mongo access`,async()=>{
  const mongo=mongoFixture();let generated=0;await assert.rejects(indexTextInMongoVectors({mongo,tier:'hot',parentId,text:'Outside information.',metadata:{embedding_model:'fixture'},embeddingFunction:{generate:async texts=>{generated++;return texts.map(()=>[1,0]);}}}),/parent.*nonblank|string.*parent/i);
  assert.equal(generated,0);assert.equal(mongo.hotVectors.calls.writes.length,0);assert.equal(mongo.vectorPartitions.calls.searches.length,0);
});
test('Codex1a: labels attach to an explicit graph node rather than its source event',async()=>{
  const mongo=vectorDeletionFixture();const event={...ordinaryEvent('label-source','A new outside encounter informs the character.'),kind:'graph.node',extra:{node_id:'declared-character',openplanner_labels:{labels:['keep']}}};
  const result=await ingestEvents({mongo,embeddingRuntime:eventRuntime({generate:async texts=>texts.map(()=>[1,0])})},[event]);await result.backgroundIndexing;
  const labels=[...mongo.graphEdges.rows.values()].filter(row=>row.edge_kind==='has_label'&&row.data.source_event_id===event.id);assert.equal(labels.length,1);assert.equal(labels[0].source_node_id,'declared-character');
});
for(const mode of ['native','cosine'])for(const score of [-1.01,1.01,2])test(`Codex1a: optional ${mode} Vexx score${score} falls back to local cosine`,async()=>{
  await withEnv({VEXX_BASE_URL:'https://vexx.fixture.invalid',VEXX_API_KEY:'',VEXX_MIN_CANDIDATES:'1',VEXX_ENFORCE:'false'},async()=>{
    const vectors=await import(new URL(`../dist/mongo-vectors.js?bounded-score-${mode}-${score}`,import.meta.url).href);
    await withFetch(async(_url,init)=>{const request=JSON.parse(init.body);return {ok:true,json:async()=>({matches:request.candidates.map(row=>({id:row.id,score:row.id==='second'?score:-score}))})};},async()=>{
      const result=await streamingQuery(vectors,parentQueryFixture(mode,[vectorRow('best','best-parent',1),vectorRow('second','second-parent',.8)]));assert.deepEqual(result.ids,[['best']]);assert.equal(result.distances[0][0],0);
    });
  });
});
for(const mode of ['native','cosine'])test(`Codex1a: enforced ${mode} Vexx out-of-range scores refuse`,async()=>{
  await withEnv({VEXX_BASE_URL:'https://vexx.fixture.invalid',VEXX_API_KEY:'',VEXX_MIN_CANDIDATES:'1',VEXX_ENFORCE:'true'},async()=>{
    const vectors=await import(new URL(`../dist/mongo-vectors.js?bounded-score-enforced-${mode}`,import.meta.url).href);
    await withFetch(async(_url,init)=>({ok:true,json:async()=>({matches:JSON.parse(init.body).candidates.map(row=>({id:row.id,score:2}))})}),async()=>assert.rejects(streamingQuery(vectors,parentQueryFixture(mode,[vectorRow('best','best-parent',1),vectorRow('second','second-parent',.8)])),/unavailable/i));
  });
});

// Native Codex5481717991: legacy retention, scoped admission and cache ownership.
for (const operation of ['create', 'remove-label']) test(`Codex518: legacy session ${operation} uses environment retention`, async () => {
  await withEnv({MONGODB_EVENTS_TTL_SECONDS:'73'}, async () => {
    const mongo=exactSessionFixture(); delete mongo.retention;
    const sessions=createProtocols({mongo}).sessionManagement;
    await sessions.createSession({session:'legacy-retention',extra:operation==='remove-label'?{openplanner_labels:{labels:['keep']}}:{}});
    if (operation==='remove-label') await sessions.updateSession('legacy-retention',{'extra.openplanner_labels.labels':[]});
    const stored=await sessions.getSession('legacy-retention');
    assert.ok(stored.expiresAt instanceof Date);
    assert.equal(stored.expiresAt.getTime()-stored.updatedAt.getTime(),73000);
  });
});
test('Codex518: explicitly disabled connection retention overrides the environment', async () => {
  await withEnv({MONGODB_EVENTS_TTL_SECONDS:'73'}, async () => {
    const mongo=exactSessionFixture(); mongo.retention.eventsTtlSeconds=0;
    const row=await createProtocols({mongo}).sessionManagement.createSession({session:'disabled-retention'});
    assert.equal(row.expiresAt,undefined);
  });
});
for (const similarity of [undefined,null,'0.5',NaN,Infinity,-Infinity,-1.01,1.01]) test(`Codex518: semantic batch rejects similarity ${String(similarity)} before any writes`, async () => {
  let writes=0;
  await assert.rejects(upsertGraphSemanticEdges({bulkWrite:async()=>{writes++;}},[
    {source_node_id:'valid-a',target_node_id:'valid-b',similarity:.5},
    {source_node_id:'invalid-a',target_node_id:'invalid-b',similarity}
  ]),/similarity.*finite|similarity.*range|similarity.*-1/i);
  assert.equal(writes,0);
});
test('Codex518: semantic cosine endpoints remain valid', async () => {
  let ops;
  assert.equal(await upsertGraphSemanticEdges({bulkWrite:async value=>{ops=value;}},[-1,0,1].map((similarity,i)=>({source_node_id:'a'+i,target_node_id:'b'+i,similarity}))),3);
  assert.deepEqual(ops.map(op=>op.updateOne.update.$set.similarity),[-1,0,1]);
});
for (const tier of ['unknown','hot ',undefined,null,{},17]) for (const operation of ['list','upsert','delete','replace','single','query','batch'])
test(`Codex518: direct ${operation} rejects tier ${JSON.stringify(tier)} before storage/provider access`, async () => {
  const mongo=mongoFixture(); let effects=0;
  mongo.vectorPartitions.find=()=>{effects++;throw new Error('unexpected partition access');};
  mongo.hotVectors.findOne=async()=>{effects++;return null;};
  mongo.hotVectors.bulkWrite=async()=>{effects++;};
  const provider={generate:async texts=>{effects++;return texts.map(()=>[1,0]);}};
  const actions={
    list:()=>listMongoVectorPartitions(mongo,tier),
    upsert:()=>upsertMongoVectorDocuments(mongo,tier,[entry()]),
    delete:()=>deleteMongoVectorEntriesByFilter(mongo,tier,{parent_id:'parent'}),
    replace:()=>replaceMongoVectorEntries(mongo,tier,'parent',[]),
    single:()=>indexTextInMongoVectors({mongo,tier,parentId:'parent',text:'outside',metadata:{embedding_model:'fixture'},embeddingFunction:provider}),
    query:()=>queryMongoVectorsByText({mongo,tier,q:'outside',k:1,getEmbeddingFunctionForModel:()=>provider}),
    batch:()=>batchIndexTextsInMongoVectors({mongo,tier,items:[item('parent')],embeddingFunction:provider})
  };
  await assert.rejects(actions[operation](),/tier.*hot|unsupported.*tier/i);
  assert.equal(effects,0);
  assert.ok([...mongo.collections.values()].every(c=>c.calls.writes.length===0&&c.calls.indexes.length===0&&c.calls.deletes.length===0));
});
for (const operation of ['set','putMany']) test(`Codex518: embedding cache ${operation} owns vectors through reads and durable snapshots`, async () => {
  const dir=await mkdtemp(join(tmpdir(),'embedding-owned-'));
  try {
    const path=join(dir,'cache.json'), cache=new PersistentEmbeddingCache(path), vector=[1,0];
    if(operation==='set') cache.set('owned',{embedding:vector,cachedAt:0});
    else await cache.putMany([{key:'owned',vector}]);
    vector[0]=NaN;
    const first=(await cache.getMany(['owned'])).get('owned'); assert.deepEqual(first,[1,0]);
    first[0]=17;
    assert.deepEqual((await cache.getMany(['owned'])).get('owned'),[1,0]);
    await cache.flush(); assert.deepEqual((await readFile(path,'utf8')).trim().split('\n').map(line=>JSON.parse(line)),[['owned',[1,0]]]);
  } finally { await rm(dir,{recursive:true,force:true}); }
});
for (const vector of [[],[NaN],[Infinity],[-Infinity],[1,'2'],Array(2)]) for (const operation of ['set','putMany'])
test(`Codex518: embedding cache ${operation} rejects invalid vectors atomically ${JSON.stringify(vector)}`, async () => {
  const cache=new PersistentEmbeddingCache(); await cache.putMany([{key:'retained',vector:[1,0]}]);
  if(operation==='set') assert.throws(()=>cache.set('invalid',{embedding:vector,cachedAt:0}),/embedding.*finite|embedding.*nonempty/i);
  else await assert.rejects(cache.putMany([{key:'first-new',vector:[0,1]},{key:'invalid',vector}]),/embedding.*finite|embedding.*nonempty/i);
  assert.equal(cache.size,1); assert.deepEqual((await cache.getMany(['retained'])).get('retained'),[1,0]);
});
for (const source_ref of [[],['lake'],'lake',17,false,null,new Date(0)]) test(`Codex518: ingest rejects non-object source reference ${JSON.stringify(source_ref)} before batch effects`, async () => {
  const mongo=mongoFixture(); let providers=0;
  await assert.rejects(ingestEvents({mongo,embeddingRuntime:eventRuntime({generate:async texts=>{providers++;return texts.map(()=>[1,0]);}})},[
    ordinaryEvent('valid-first','Valid outside information.'),{...ordinaryEvent('invalid-source','Invalid scope information.'),source_ref}
  ]),/source_ref.*plain.*object|source.reference.*object/i);
  assert.equal(providers,0); assert.ok([...mongo.collections.values()].every(c=>c.calls.writes.length===0));
});
for (const parentId of ['', ' ', '\t',null,17,{}]) test(`Codex518: batch parent ${JSON.stringify(parentId)} rejects before any provider work`, async () => {
  const mongo=mongoFixture(); let providers=0;
  await assert.rejects(batchIndexTextsInMongoVectors({mongo,tier:'hot',items:[item('valid-first'),item(parentId)],embeddingFunction:{generate:async texts=>{providers++;return texts.map(()=>[1,0]);}}}),/parent.*nonblank|string.*parent/i);
  assert.equal(providers,0); assert.ok([...mongo.collections.values()].every(c=>c.calls.writes.length===0));
});
test('Codex518: conflicting tenant aliases reject the whole event batch before graph writes', async () => {
  const mongo=mongoFixture(); let providers=0;
  const event={...ordinaryEvent('conflicted-tenant','An outside encounter.'),extra:{tenant_id:'tenant-a',openplanner_labels:{tenant_id:'tenant-b',labels:['keep']}}};
  await assert.rejects(ingestEvents({mongo,embeddingRuntime:eventRuntime({generate:async texts=>{providers++;return texts.map(()=>[1,0]);}})},[ordinaryEvent('valid-first','Valid outside information.'),event]),/tenant.*agree|tenant.*conflict/i);
  assert.equal(providers,0); assert.ok([...mongo.collections.values()].every(c=>c.calls.writes.length===0));
});
test('Codex518: matching tenant aliases and plain source reference preserve exact persisted scope', async () => {
  const mongo=vectorDeletionFixture(); const event={...ordinaryEvent('matched-tenant','An outside encounter.'),source_ref:{project:'owned-project'},extra:{tenant_id:'owned-tenant',openplanner_labels:{tenant_id:'owned-tenant',labels:['keep']}}};
  const result=await ingestEvents({mongo,embeddingRuntime:eventRuntime({generate:async texts=>texts.map(()=>[1,0])})},[event]); await result.backgroundIndexing;
  assert.equal(mongo.events.rows.get(event.id).project,'owned-project');
  assert.ok([...mongo.graphLabelNodes.rows.values()].every(row=>row.tenant_id==='owned-tenant'));
});

// Native CodeRabbit5481823938: bounded cache reload and BSON hydration.
test('CodeRabbit0f: JSONL cache reload preserves UTF-8 across chunks and large vectors',async()=>{
  const dir=await mkdtemp(join(tmpdir(),'openplanner-jsonl-reload-'));const path=join(dir,'cache.jsonl');
  const key='outside-🧠'.repeat(12000), vector=Array.from({length:4096},(_,i)=>i/4096);
  const bytes=JSON.stringify([key,vector])+'\n'+JSON.stringify(['second',[0,1]])+'\n';
  try{await writeFile(path,bytes);const cache=new PersistentEmbeddingCache(path);
    assert.equal(cache.size,2);assert.deepEqual((await cache.getMany([key,'second'])).get(key),vector);
    assert.deepEqual((await cache.getMany(['second'])).get('second'),[0,1]);
    assert.equal(await readFile(path,'utf8'),bytes,'loading is read-only');
  }finally{await rm(dir,{recursive:true,force:true});}
});
test('CodeRabbit0f: JSONL reload keeps the bounded retained entry window',async()=>{
  const dir=await mkdtemp(join(tmpdir(),'openplanner-jsonl-bound-'));const path=join(dir,'cache.jsonl');
  try{await writeFile(path,Array.from({length:10003},(_,i)=>JSON.stringify(['key-'+i,[i/10003]])).join('\n')+'\n');
    const cache=new PersistentEmbeddingCache(path);assert.equal(cache.size,10000);
    assert.equal(cache.has('key-0'),false);assert.equal(cache.has('key-2'),false);
    assert.deepEqual((await cache.getMany(['key-3','key-10002'])).get('key-3'),[3/10003]);
    assert.deepEqual((await cache.getMany(['key-10002'])).get('key-10002'),[10002/10003]);
  }finally{await rm(dir,{recursive:true,force:true});}
});
test('CodeRabbit0f: durable snapshots use independent JSONL entries and migrate small legacy arrays',async()=>{
  const dir=await mkdtemp(join(tmpdir(),'openplanner-jsonl-migrate-'));const path=join(dir,'cache.jsonl');
  try{await writeFile(path,JSON.stringify([['legacy',[1,0]]]));const cache=new PersistentEmbeddingCache(path);
    assert.deepEqual((await cache.getMany(['legacy'])).get('legacy'),[1,0]);
    await cache.putMany([{key:'new',vector:[0,1]}]);await cache.flush();
    const entries=(await readFile(path,'utf8')).trim().split('\n').map(line=>JSON.parse(line));
    assert.deepEqual(entries,[['legacy',[1,0]],['new',[0,1]]]);
    const restored=new PersistentEmbeddingCache(path);assert.equal(restored.size,2);
    assert.deepEqual((await restored.getMany(['legacy','new'])).get('new'),[0,1]);
  }finally{await rm(dir,{recursive:true,force:true});}
});
test('CodeRabbit0f: oversized legacy cache is skipped without leaking a private path or blocking startup',async()=>{
  const dir=await mkdtemp(join(tmpdir(),'openplanner-private-large-cache-'));const path=join(dir,'PRIVATE-cache.jsonl');const warnings=[];
  const warning=mock.method(console,'warn',(...args)=>warnings.push(args));
  try{await writeFile(path,JSON.stringify([['legacy',[1,0]]])+' '.repeat(12*1024*1024));
    let cache;assert.doesNotThrow(()=>{cache=new PersistentEmbeddingCache(path);});
    assert.equal(cache.size,0);assert.equal(warnings.length,1);assert.match(warnings[0][0],/cache.*skip|skip.*cache/i);
    assert.equal(warnings.flat().some(x=>String(x).includes(dir)||String(x).includes('PRIVATE')),false);
    await cache.putMany([{key:'fresh',vector:[0,1]}]);await cache.flush();
    assert.deepEqual((await new PersistentEmbeddingCache(path).getMany(['fresh'])).get('fresh'),[0,1]);
  }finally{warning.mock.restore();await rm(dir,{recursive:true,force:true});}
});
test('CodeRabbit0f: real Mongo ObjectId vector identity does not block scoped source hydration',async()=>{
  const dir=await mkdtemp(join(tmpdir(),'openplanner-objectid-hydration-'));const text='An independently retained outside encounter.';
  try{await writeFile(join(dir,'encounter.md'),text);await withEnv({OPENPLANNER_SOURCE_ROOT:dir},async()=>{
    const id=new ObjectId('0123456789abcdef01234567');
    assert.equal(await hydrateVectorDocumentText({_id:id,project:'lake',source_text_redacted:true,source_ref:{source_path:'encounter.md',lake:'lake'}}),text);
  });}finally{closeHydrationCache();await rm(dir,{recursive:true,force:true});}
});


// Native Codex5481827605: numeric forces, whole-batch metadata and deadlines.
for (const [field,values] of [['similarity',[undefined,null,'0.5',NaN,Infinity,-Infinity,-1.01,1.01]],['charge',[null,'0.5',NaN,Infinity,-Infinity,{}]]])
for (const value of values) test(`Codex0f: force batch refuses ${field} ${String(value)} before any write`,async()=>{
  let writes=0;const row={source_node_id:'a',target_node_id:'b',similarity:.5};
  await assert.rejects(upsertGraphSemanticForceSamples({bulkWrite:async()=>{writes++;}},[row,{...row,source_node_id:'c',target_node_id:'d',[field]:value}]),/similarity|charge|finite|range/i);
  assert.equal(writes,0);
});
test('Codex0f: force cosine endpoints and finite charges retain exact values',async()=>{
  let ops;const rows=[-1,0,1].map((similarity,i)=>({source_node_id:'a'+i,target_node_id:'b'+i,similarity,...(i===1?{charge:2}:{})}));
  assert.equal(await upsertGraphSemanticForceSamples({bulkWrite:async x=>{ops=x;}},rows),3);
  assert.deepEqual(ops.map(x=>x.updateOne.update.$set.similarity),[-1,0,1]);
  assert.deepEqual(ops.map(x=>x.updateOne.update.$set.charge),[-1,2,1]);
});
for (const reversed of [false,true]) test(`Codex0f: duplicate undirected semantic identities reject whole batch reversed=${reversed}`,async()=>{
  let writes=0;const row={source_node_id:'a',target_node_id:'b',similarity:.5};
  await assert.rejects(upsertGraphSemanticEdges({bulkWrite:async()=>{writes++;}},[row,{...row,...(reversed?{source_node_id:'b',target_node_id:'a'}:{}),similarity:.9}]),/duplicate.*semantic|semantic.*duplicate/i);
  assert.equal(writes,0);
});
for (const extra of [[],['label'],'metadata',42,false,new Date(0)]) test(`Codex0f: malformed extra ${JSON.stringify(extra)} rejects before any batch effects`,async()=>{
  const mongo=vectorDeletionFixture();let generated=0;
  const runtime=eventRuntime({generate:async texts=>{generated++;return texts.map(()=>[1,0]);}});
  await assert.rejects(ingestEvents({mongo,embeddingRuntime:runtime},[ordinaryEvent('first-valid'),{...ordinaryEvent('second-invalid'),extra}]),/extra.*plain object|plain object.*extra/i);
  assert.equal(generated,0);
  assert.ok([...mongo.collections.values()].every(c=>c.calls.writes.length===0&&c.calls.indexes.length===0&&c.calls.deletes.length===0));
});
for (const mode of ['native','cosine']) for (const enforced of [false,true]) test(`Codex0f: actual stalled HTTP body retains Vexx deadline ${mode} enforced=${enforced}`,async()=>{
  let requests=0;const server=createServer((_request,res)=>{requests++;res.writeHead(200,{'Content-Type':'application/json'});res.flushHeaders();res.write('{"matches":[');});
  await new Promise((resolve,reject)=>{server.once('error',reject);server.listen(0,'127.0.0.1',resolve);});
  let guard;
  try{const address=server.address();await withEnv({VEXX_BASE_URL:`http://127.0.0.1:${address.port}`,VEXX_API_KEY:'',VEXX_MIN_CANDIDATES:'1',VEXX_ENFORCE:String(enforced),VEXX_TIMEOUT_MS:'40'},async()=>{
    const vectors=await import(new URL(`../dist/mongo-vectors.js?codex0f-body-${mode}-${enforced}`,import.meta.url).href);
    const work=vectors.queryMongoVectorsByText({mongo:parentQueryFixture(mode,[vectorRow('best','best-parent',1),vectorRow('second','second-parent',.8)]),tier:'hot',q:'outside',k:1,getEmbeddingFunctionForModel:()=>({generate:async()=>[[1,0]]})});
    const bounded=Promise.race([work,new Promise((_,reject)=>{guard=setTimeout(()=>reject(new Error('fixture body deadline exceeded')),700);})]);
    if(enforced)await assert.rejects(bounded,error=>{assert.equal(error.message,'vector query execution unavailable for all partitions');assert.deepEqual(error.unavailablePartitions,['healthy_vectors']);return true;});
    else assert.deepEqual((await bounded).ids,[['best']]);
    assert.ok(requests>0);
  });}finally{clearTimeout(guard);server.closeAllConnections();await new Promise(resolve=>server.close(resolve));}
});
test('Codex0f: migration-only filesystem provenance hydrates exact retained text',async()=>{
  const dir=await mkdtemp(join(tmpdir(),'openplanner-migration-source-'));const text='Retained migration encounter.';
  try{await writeFile(join(dir,'legacy.md'),text);await withEnv({OPENPLANNER_SOURCE_ROOT:dir},async()=>{
    assert.equal(await loadHydrationSourceText({id:'legacy',project:'lake',text:'',extra:{migration_2:{source_path:'legacy.md'}}}),text);
  });}finally{await closeHydrationCache();await rm(dir,{recursive:true,force:true});}
});
// Native Codex5481938999: typed force tuples, graph owners and truthful query diagnostics.
for(const reversed of [false,true])test(`Codex949: duplicate force identity refuses whole batch reversed=${reversed}`,async()=>{
  const c=collection('force-preflight');const a={source_node_id:'a',target_node_id:'b',similarity:.2,field_profile:'field',embedding_model:'model'};
  const b={...a,source_node_id:reversed?'b':'a',target_node_id:reversed?'a':'b',similarity:.8,charge:-.3};
  await assert.rejects(upsertGraphSemanticForceSamples(c,[a,b]),/duplicate.*force.*identity/i);assert.equal(c.calls.writes.length,0);
});
for(const dimensions of [NaN,Infinity,-Infinity,0,-1,1.5,Number.MAX_SAFE_INTEGER+1,'2',false])test(`Codex949: force dimensions ${String(dimensions)} refuse before any write`,async()=>{
  const c=collection('force-preflight');const good={source_node_id:'a',target_node_id:'b',similarity:.5,embedding_dimensions:2};
  await assert.rejects(upsertGraphSemanticForceSamples(c,[good,{...good,target_node_id:'c',embedding_dimensions:dimensions}]),/force.*dimensions.*positive safe integer/i);assert.equal(c.calls.writes.length,0);
});
test('Codex949: distinct force model/profile tuples and nullable dimensions remain valid',async()=>{
  const c=collection('forces');const a={source_node_id:'a',target_node_id:'b',similarity:.5};
  const rows=[a,{...a,field_profile:'other',embedding_dimensions:null},{...a,embedding_model:'model',embedding_dimensions:2}];
  assert.equal(await upsertGraphSemanticForceSamples(c,rows),3);assert.deepEqual(c.calls.writes.map(x=>x.update.$set.embedding_dimensions),[null,null,2]);
});
for(const ordinary of [false,true])for(const reverse of [false,true])test(`Codex949: conflicting embedding owners refuse all effects ordinary=${ordinary} reverse=${reverse}`,async()=>{
  const mongo=vectorDeletionFixture();let providers=0;const first={...ordinaryEvent('owner-a','First outside information changes creative choices.'),kind:'graph.node',extra:{node_id:'shared-node'}};
  const second=ordinary?ordinaryEvent('shared-node','Another outside encounter changes creative choices.'):{...ordinaryEvent('owner-b','Another outside encounter changes creative choices.'),kind:'graph.node',extra:{node_id:'shared-node'}};
  await assert.rejects(ingestEvents({mongo,embeddingRuntime:eventRuntime({generate:async texts=>{providers++;return texts.map(()=>[1,0]);}})},reverse?[second,first]:[first,second]),/embedding.*ownership.*collision/i);
  assert.equal(providers,0);for(const c of mongo.collections.values()){assert.equal(c.calls.writes.length,0);assert.equal(c.calls.deletes.length,0);}
});
for(const quality of ['any','good_then_not_bad'])test(`Codex949: ${quality} retains lower-level timestamp and partition diagnostics`,async()=>{
  const mongo=parentQueryFixture('cosine',[{...vectorRow('bad-time','bad-time',1),ts:'not-a-date'},vectorRow('healthy','healthy',.9)]);
  const embeddingRuntime=eventRuntime({generate:async()=>[[1,0]]});embeddingRuntime.hot.getEmbeddingFunctionForModel=()=>({generate:async()=>[[1,0]]});
  const response=await vectorSearchWithQuality({mongo,embeddingRuntime},{q:'outside',k:2,tier:'hot',quality});
  assert.equal(response.ok,true);assert.equal(response.storageBackend,'mongodb');assert.equal(response.qualityMode,quality);const result=response.result;
  assert.deepEqual(result.ids,[['healthy']]);assert.equal(result.partial,true);assert.equal(result.discarded_invalid_timestamp_count,quality==='any'?1:2);assert.equal(result.queried_partition_count,quality==='any'?1:2);
});
for(const ts of ['2020','1/2/2020','2021-02-30T00:00:00Z','2026-01-01T24:00:00Z','2026-01-01T00:00:00+24:00'])test(`Codex949: legacy non-ISO timestamp ${ts} is discarded before rank limit`,async()=>{
  const mongo=parentQueryFixture('cosine',[{...vectorRow('bad-time','bad-time',1),ts},vectorRow('healthy','healthy',.9)]);
  const result=await streamingQuery({queryMongoVectorsByText},mongo);assert.deepEqual(result.ids,[['healthy']]);assert.equal(result.partial,true);assert.equal(result.discarded_invalid_timestamp_count,1);
});
for(const ts of ['2024-02-29T00:00:00Z','2026-01-01T00:00:00.123+05:30'])test(`Codex949: strict legacy ISO timestamp ${ts} retains its actual instant`,async()=>{
  const mongo=parentQueryFixture('cosine',[{...vectorRow('legacy','legacy',1),ts}]);const result=await streamingQuery({queryMongoVectorsByText},mongo);
  assert.deepEqual(result.ids,[['legacy']]);assert.equal(result.metadatas[0][0].ts,new Date(ts).toISOString());assert.equal(result.discarded_invalid_timestamp_count,undefined);
});


// Exact current Codex5482017007 and CodeRabbit5482020746: all eight native seams.
for(const ts of ['2020','01/02/2026','2021-02-30T00:00:00Z','2026-01-01T24:00:00Z'])for(const op of ['single','batch','direct'])test(`Codex0b: vector write ${op} rejects non-ISO timestamp ${ts} before IO`,async()=>{
 const mongo=mongoFixture();let providers=0;const embeddingFunction={generate:async texts=>{providers++;return texts.map(()=>[1,2]);}};const metadata={embedding_model:'fixture',ts};
 const promise=op==='single'?indexTextInMongoVectors({mongo,tier:'hot',parentId:'parent',text:'outside',metadata,embeddingFunction}):op==='batch'?batchIndexTextsInMongoVectors({mongo,tier:'hot',items:[{...item('parent'),metadata}],embeddingFunction}):upsertMongoVectorDocuments(mongo,'hot',[{...entry(),metadata}]);
 await assert.rejects(promise,/timestamp|metadata.ts|ISO/i);assert.equal(providers,0);for(const c of mongo.collections.values())assert.equal(c.calls.writes.length,0);
});
for(const ts of [new Date('2024-02-29T00:00:00Z'),1709164800000,'2024-02-29T00:00:00Z'])test(`Codex0b: explicit supported vector timestamp retains instant ${String(ts)}`,async()=>{
 const mongo=mongoFixture();await upsertMongoVectorDocuments(mongo,'hot',[{...entry(),metadata:{embedding_model:'fixture',ts}}]);assert.equal(mongo.hotVectors.rows.get('chunk').ts.getTime(),new Date(ts).getTime());
});
for(const populated of [false,true])for(const where of [null,{unknown:'value'},{project:{$nin:['a']}}])test(`Codex0b: invalid vector predicate rejects before registry/provider populated=${populated} ${JSON.stringify(where)}`,async()=>{
 const mongo=populated?parentQueryFixture('cosine',[vectorRow('healthy','parent',1)]):mongoFixture();let providers=0;
 await assert.rejects(queryMongoVectorsByText({mongo,tier:'hot',q:'outside',k:1,where,getEmbeddingFunctionForModel:()=>({generate:async()=>{providers++;return [[1,0]];}})}),/vector filter|unsupported vector/i);
 assert.equal(mongo.vectorPartitions.calls.searches.length,0);assert.equal(providers,0);
});
for(const owner of [undefined,'source-owner'])test(`Codex0b: duplicate graph edge identity refuses entire unordered batch owner=${owner}`,async()=>{
 const c=collection('edges');const edge={source_node_id:'a',target_node_id:'b',edge_kind:'related',project:'lake',data:owner===undefined?{}:{source_event_id:owner}};
 await assert.rejects(upsertGraphEdges(c,[{...edge,target_node_id:'different'},edge,{...edge,source:'changed-metadata'}]),/duplicate.*graph edge|graph edge.*duplicate/i);assert.equal(c.calls.writes.length,0);
});
test('Codex0b: graph edge ownership/project/direction remain distinct identities',async()=>{
 const c=collection('edges');const a={source_node_id:'a',target_node_id:'b',edge_kind:'related',project:'lake'};
 assert.equal(await upsertGraphEdges(c,[a,{...a,project:'other'},{...a,source_node_id:'b',target_node_id:'a'},{...a,data:{source_event_id:'owner-a'}},{...a,data:{source_event_id:'owner-b'}}]),5);assert.equal(c.calls.writes.length,5);
});
for(const text of [17,true,{},null,undefined])for(const op of ['prepare','single','batch'])test(`Codex0b: ${op} rejects non-string text before normalization/provider ${String(text)}`,async()=>{
 const mongo=mongoFixture();let providers=0;const embeddingFunction={generate:async texts=>{providers++;return texts.map(()=>[1,2]);}};
 if(op==='prepare')assert.throws(()=>prepareIndexDocument({parentId:'parent',text}),/text.*string|string.*text/i);
 else await assert.rejects(op==='single'?indexTextInMongoVectors({mongo,tier:'hot',parentId:'parent',text,metadata:{embedding_model:'fixture'},embeddingFunction}):batchIndexTextsInMongoVectors({mongo,tier:'hot',items:[item('valid-first'),{...item('parent'),text}],embeddingFunction}),/text.*string|string.*text/i);
 assert.equal(providers,0);for(const c of mongo.collections.values())assert.equal(c.calls.writes.length,0);
});
for(const labels of [null,'keep',[null],['']])for(const ttl of [0,60])test(`Codex0b: appendEvents preflights all retention labels ttl=${ttl} ${JSON.stringify(labels)}`,async()=>{
 const mongo=mongoFixture();mongo.retention.eventsTtlSeconds=ttl;const admission=createProtocols({mongo}).eventAdmission;
 await assert.rejects(admission.appendEvents([ordinaryEvent('first','outside'),{...ordinaryEvent('last','outside'),extra:{openplanner_labels:{labels}}}]),/retention.*labels|labels.*array|labels.*strings/i);assert.equal(mongo.events.calls.writes.length,0);
});
for(const separate of [false,true])test(`Codex0b: label graph ownership distinguishes projects separate=${separate}`,async()=>{
 const mongo=mongoFixture();const ctx={mongo,embeddingRuntime:eventRuntime({generate:async()=>{throw new Error('no embedding expected for empty texts');}})};
 const event=(id,project)=>({...ordinaryEvent(id,''),source_ref:{project},extra:{openplanner_labels:{labels:['keep'],tenant_id:'one-tenant'}}});const a=event('owner-a','lake'),b=event('owner-b','river');
 if(separate){await ingestEvents(ctx,[a]);await ingestEvents(ctx,[b]);}else await ingestEvents(ctx,[a,b]);
 const nodes=[...mongo.graphLabelNodes.rows.values()];assert.equal(nodes.length,2);assert.deepEqual(nodes.map(x=>x.project).sort(),['lake','river']);
 const edges=mongo.graphEdges.calls.writes.filter(x=>x.update.$set.edge_kind==='has_label').map(x=>x.update.$set);assert.equal(edges.length,2);assert.notEqual(edges[0].target_node_id,edges[1].target_node_id);
 for(const edge of edges)assert.equal(nodes.find(n=>n.label_id===edge.target_node_id)?.project,edge.project);
});
test('Codex0b: same tenant/project/label reuses stable label identity across sources and replay',async()=>{
 const mongo=mongoFixture();const ctx={mongo,embeddingRuntime:eventRuntime({generate:async()=>{throw new Error('empty texts');}})};const a={...ordinaryEvent('owner-a',''),source_ref:{project:'lake'}},b={...ordinaryEvent('owner-b',''),source_ref:{project:'lake'}};
 await ingestEvents(ctx,[a,b]);const id=[...mongo.graphLabelNodes.rows.keys()][0];assert.equal(mongo.graphLabelNodes.rows.size,1);await ingestEvents(ctx,[a]);assert.deepEqual([...mongo.graphLabelNodes.rows.keys()],[id]);
});
for(const key of ['latestDefinition','definition'])for(const oldDimensions of [2,3])test(`Codex0b: graph vector index reconciles ${key} dimensions ${oldDimensions} to2`,async(t)=>{
 const mongo=mongoFixture();const c=mongo.graphNodeEmbeddings;let definition={fields:[{type:'vector',path:'embedding',numDimensions:oldDimensions,similarity:'cosine'},{type:'filter',path:'project'},{type:'filter',path:'node_id'},{type:'filter',path:'embedding_model'},{type:'filter',path:'embedding_dimensions'}]};const updates=[];
 c.listSearchIndexes=()=>({toArray:async()=>[{name:'embedding_vector',status:'READY',queryable:true,[key]:definition}]});
 c.updateSearchIndex=async(name,next)=>{updates.push({name,next});definition=next;};c.createSearchIndex=async()=>{throw new Error('existing index must not be duplicated');};
 t.mock.method(MongoClient.prototype,'connect',async function(){return this;});t.mock.method(MongoClient.prototype,'db',()=>mongo.db);
 await withEnv({EMBED_PROVIDER_DIMENSIONS:'2'},async()=>openMongoDB(openingConfig));assert.equal(updates.length,oldDimensions===2?0:1);assert.equal(definition.fields.find(f=>f.type==='vector').numDimensions,2);if(updates.length)assert.equal(updates[0].name,'embedding_vector');
});
for(const root of [undefined,'','   '])test(`CodeRabbit0b: absent/blank source root explicitly disables hydration ${String(root)}`,async()=>{
 const saved=process.env.OPENPLANNER_SOURCE_ROOT;
 try{if(root===undefined)delete process.env.OPENPLANNER_SOURCE_ROOT;else process.env.OPENPLANNER_SOURCE_ROOT=root;
  assert.equal(sourceRoot(),undefined);assert.equal(safeSourceFilePath({id:'no-root',extra:{source_path:'outside.txt'}}),undefined);assert.equal(await loadHydrationSourceText({id:'no-root',extra:{source_path:'outside.txt'}}),null);
 }finally{if(saved===undefined)delete process.env.OPENPLANNER_SOURCE_ROOT;else process.env.OPENPLANNER_SOURCE_ROOT=saved;}
});
test('CodeRabbit0b: configured trimmed root hydrates only its actual owned file',async()=>{
 const dir=await mkdtemp(join(tmpdir(),'openplanner-explicit-root-'));
 try{await writeFile(join(dir,'outside.txt'),'outside encounter');await withEnv({OPENPLANNER_SOURCE_ROOT:'  '+dir+'  '},async()=>{
  assert.equal(sourceRoot(),dir);const row={id:'configured',extra:{source_path:'outside.txt'}};assert.equal(safeSourceFilePath(row),join(dir,'outside.txt'));assert.equal(await loadHydrationSourceText(row),'outside encounter');
 });}finally{await closeHydrationCache();await rm(dir,{recursive:true,force:true});}
});

for(const key of ['latestDefinition','definition'])test(`Codex0b: graph index readiness waits for updated dimensions in ${key}`,async(t)=>{
 const mongo=mongoFixture(),c=mongo.graphNodeEmbeddings;let polls=0,updates=0,sleeps=0,clock=0;
 c.listSearchIndexes=()=>({toArray:async()=>{polls++;return [{name:'embedding_vector',status:'READY',queryable:true,[key]:{fields:[{type:'vector',path:'embedding',numDimensions:polls<3?3:2}]}}];}});
 c.updateSearchIndex=async()=>{updates++;};
 t.mock.method(MongoClient.prototype,'connect',async function(){return this;});t.mock.method(MongoClient.prototype,'db',()=>mongo.db);
 t.mock.method(Date,'now',()=>{clock+=1000;return clock;});t.mock.method(globalThis,'setTimeout',resolve=>{sleeps++;queueMicrotask(resolve);return {unref(){},ref(){}};});
 await withEnv({EMBED_PROVIDER_DIMENSIONS:'2'},async()=>openMongoDB(openingConfig));assert.equal(updates,1);assert.equal(polls,3);assert.equal(sleeps,1);
});
