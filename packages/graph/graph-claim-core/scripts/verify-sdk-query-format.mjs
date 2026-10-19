// LGPL-3.0-or-later. Actual owning SDK formatter; held provider and storage only.
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {createRequire} from 'node:module';
import {createScopedMongoRecall} from '../dist/index.js';
import {formatEmbeddingQueryText} from '../../../openplanner-sdk/src/embedding-text.ts';
const {ObjectId}=createRequire(new URL('../../../openplanner-sdk/package.json',import.meta.url))('mongodb');

const scope={'actor-id':'creator','org-id':'org','membership-id':'member','user-id':'user','policy-revision':'policy:1'};
const authority={scope,project:'project',records:[{id:'event',text:'harbor encounter'}]};
const request={version:1,'recall-id':'recall',query:'  harbor  ',k:1,fetch:1,'max-nodes':1,'max-cost':1,feedback:'none'};
const index={source_text_hash_sha256:createHash('sha256').update(authority.records[0].text,'utf8').digest('hex'),_id:'event::held-model::2::0',node_id:'event',source_event_id:'event',project:'project',embedding_model:'held-model',embedding_dimensions:2,embedding:[1,0],chunk_index:0,chunk_count:1};
const collection=rows=>({find(){return {sort(){return this},limit(){return this},maxTimeMS(){return this},async toArray(){return rows}}}});
const captured=[];
const hot={getModel:()=> 'held-model',getEmbeddingFunctionForModel:()=>({async generate(texts){captured.push(...texts);return [[1,0]]}})};
const sdk={mongo:{graphNodeEmbeddings:collection([index]),graphEdges:collection([])},embeddingRuntime:{hot}};
const saved=Object.fromEntries(['EMBED_QUERY_PREFIX','EMBED_QUERY_TEMPLATE'].map(key=>[key,process.env[key]]));
try {
  for(const config of [
    {prefix:'',template:'',expected:'harbor'},
    {prefix:'query: ',template:'',expected:'query: harbor'},
    {prefix:'ignored: ',template:'Question\\n{query} / {query}',expected:'Question\nharbor / harbor'}
  ]) {
    process.env.EMBED_QUERY_PREFIX=config.prefix;
    process.env.EMBED_QUERY_TEMPLATE=config.template;
    const expected=formatEmbeddingQueryText(request.query);
    assert.equal(expected,config.expected,'actual SDK supported formatting');
    captured.length=0;
    const result=await createScopedMongoRecall(sdk,async()=>authority,formatEmbeddingQueryText)(request);
    assert.equal(result.selection.status,'completed');
    assert.deepEqual(captured,[expected],'adapter must pass actual SDK formatted text to provider');
  }
  for(const formatter of [undefined,null,42,()=>'',()=>null,()=>({query:'harbor'}),()=>{throw Error('private formatting failure')}]) {
    captured.length=0;
    const result=await createScopedMongoRecall(sdk,async()=>authority,formatter)(request);
    assert.equal(result.selection.status,'failed','invalid/missing formatter refuses');
    assert.deepEqual(captured,[],'formatter refusal reaches no provider');
    assert.ok(!JSON.stringify(result).includes('private formatting failure'));
  }
  const read=createScopedMongoRecall(sdk,async()=>authority,formatEmbeddingQueryText);
  const originalId=index._id;
  const before=await read(request);
  try {
    index._id=new ObjectId('0123456789abcdef01234567');
    const after=await read(request);
    assert.deepEqual(after,before,'physical BSON storage identity cannot change typed embedding recall');
    index._id=new ObjectId('abcdef0123456789abcdef01');
    assert.deepEqual(await read(request),before,'embedding tuple identity survives physical storage replacement');
    const duplicateSdk={...sdk,mongo:{...sdk.mongo,graphNodeEmbeddings:collection([index,{...index,_id:originalId}])}};
    const duplicate=await createScopedMongoRecall(duplicateSdk,async()=>authority,formatEmbeddingQueryText)(request);
    assert.equal(duplicate.selection.failure.code,'invalid-embedding','duplicate logical tuples still refuse');
    const dimensions=index.embedding_dimensions;
    index.embedding_dimensions='2';
    try { assert.equal((await read(request)).selection.failure.code,'invalid-embedding','malformed tuple still refuses'); }
    finally { index.embedding_dimensions=dimensions; }
  } finally { index._id=originalId; }
  console.log('PASS actual BSON ObjectId, stable typed embedding identity and duplicate/malformed tuple refusals');
  console.log('PASS actual SDK trim/prefix/template/escaped newline and invalid formatter refusals; no live model or Mongo proof');
} finally {
  for(const [key,value] of Object.entries(saved)) {
    if(value===undefined)delete process.env[key];else process.env[key]=value;
  }
}
