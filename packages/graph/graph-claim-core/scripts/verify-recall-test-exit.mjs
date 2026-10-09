// LGPL-3.0-or-later. An isolated process mutates only its loaded function value.
// This verifies the actual Node runner, not a synthetic count parser.
import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';

const target = fileURLToPath(new URL('../target/test.cjs', import.meta.url));

function assertInjectedOutput(stdout) {
  assert.match(stdout, /1 failures, 0 errors\./, 'force a real existing boundary assertion to fail');
}

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
  console.log('ISOLATED_IN_MEMORY_ASSERTION_FAILURE');
  globalThis.openplanner.graph.recall.runner._main();
`, target], {encoding:'utf8', timeout:30000, maxBuffer:1048576});
process.stdout.write(child.stdout ?? '');
process.stderr.write(child.stderr ?? '');
if (child.error) throw child.error;
assert.equal(child.signal, null, 'failure must be a completed test result, not a timeout or signal');
assertInjectedOutput(child.stdout);
// These mutate the actual observed child output, not a fabricated test run.
// The checker must refuse multiple failures and a missing injected assertion.
for (const badOutput of [
  child.stdout.replace('1 failures, 0 errors.', '11 failures, 0 errors.'),
  child.stdout.replace('1 failures, 0 errors.', '21 failures, 0 errors.'),
  child.stdout.replaceAll('forced-review-failure', 'unrelated-failure'),
  child.stdout.replace('0 failures, 0 errors.', '1 failures, 0 errors.'),
]) {
  assert.notEqual(badOutput, child.stdout, 'negative fixture must change actual output');
  assert.throws(() => assertInjectedOutput(badOutput), 'invalid injection evidence must be rejected');
}
console.log(`Observed isolated runner exit=${child.status}`);
assert.equal(child.status, 1, 'an actual assertion failure must exit nonzero');
console.log('PASS actual compiled Node runner exits 1 for the forced assertion failure');
