// LGPL-3.0-or-later. Pure released-library demonstration; no storage/model calls.
import assert from 'node:assert/strict';
import {mkdtemp, writeFile, readFile, rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {scopedGraphRecallPlan} from '../dist/index.js';

const directory = await mkdtemp(join(tmpdir(), 'openplanner-scoped-recall-'));
try {
  const scope = {'actor-id':'fixture-creator', 'org-id':'fixture-org',
    'membership-id':'fixture-member', 'user-id':'fixture-user', 'policy-revision':'fixture-policy:7'};
  const node = (id, seed, score) => ({id, 'event-id':`event:${id}`, text:`memory:${id}`, 'seed?':seed, score});
  const nodes = [node('outside-encounter', true, 0.8), node('graph-only-neighbor', false, 0.1)];
  const snapshot = {version:1, revision:'fixture-graph:23', 'field-revision':'fixture-field:11',
    'field-owner':'eros-eris-field', scope, 'index-status':'ready', nodes,
    edges:[{id:'edge:encounter:neighbor', source:nodes[0].id, target:nodes[1].id,
      cost:1, 'provenance-node-ids':nodes.map(row => row.id)}], influences:[]};
  const decisions = nodes.map(row => ({'node-id':row.id, 'event-id':row['event-id'],
    'policy-revision':scope['policy-revision'], 'allowed?':true}));
  const request = {version:1, 'recall-id':'fixture-turn:17', query:'harbor', k:6, fetch:18,
    'max-nodes':64, 'max-cost':4, feedback:'none'};
  const file = join(directory, 'fixture.json');
  await writeFile(file, JSON.stringify({snapshot,decisions,request}), {mode:0o600, flag:'wx'});
  const frozen = JSON.parse(await readFile(file,'utf8'));
  const result = scopedGraphRecallPlan(frozen.snapshot,frozen.decisions,frozen.request);
  assert.equal(result.status,'completed');
  assert.deepEqual(result.hits[1].path, ['outside-encounter','graph-only-neighbor']);
  assert.deepEqual(result.hits[1]['path-edge-ids'], ['edge:encounter:neighbor']);
  assert.deepEqual(result.feedback, {status:'not-requested',attempted:0,completed:0});
  const revoked = scopedGraphRecallPlan(snapshot,[],request);
  assert.equal(revoked.status,'denied'); assert.deepEqual(revoked.hits,[]);
  console.log('PASS released ESM export returns a non-seed neighbor with an admitted graph path:');
  console.log(JSON.stringify(result,null,2));
  console.log('PASS missing decisions refuse recall; retrieval schedules zero feedback writes');
  console.log('WARN decisions/revisions are frozen fixtures; no live identity, Mongo, field state or maker prompt was verified');
} finally {
  await rm(directory,{recursive:true,force:true});
  console.log('CLEAN removed only this demonstration\'s dedicated temporary fixture');
}
