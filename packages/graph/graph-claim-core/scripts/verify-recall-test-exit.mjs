// LGPL-3.0-or-later. An isolated process mutates only its loaded function value.
// This verifies the actual Node runner, not a synthetic count parser.
import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';

const target = fileURLToPath(new URL('../target/test.cjs', import.meta.url));
const child = spawnSync(process.execPath, ['-e', `
  require(process.argv[1]);
  const boundary = globalThis.openplanner.graph.recall.boundary;
  const original = boundary.recall_plan_js;
  if (typeof original !== 'function') throw new Error('compiled recall export is missing');
  boundary.recall_plan_js = (...args) => {
    const result = original(...args);
    if (result.status === 'completed') result.status = 'forced-review-failure';
    return result;
  };
  globalThis.openplanner.graph.recall.runner._main();
`, target], {encoding:'utf8', timeout:30000, maxBuffer:1048576});
process.stdout.write(child.stdout ?? '');
process.stderr.write(child.stderr ?? '');
if (child.error) throw child.error;
assert.equal(child.signal, null, 'failure must be a completed test result, not a timeout or signal');
assert.match(child.stdout, /1 failures, 0 errors\./, 'force a real existing boundary assertion to fail');
console.log(`Observed isolated runner exit=${child.status}`);
assert.equal(child.status, 1, 'an actual assertion failure must exit nonzero');
console.log('PASS actual compiled Node runner exits 1 for the forced assertion failure');
