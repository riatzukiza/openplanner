// LGPL-3.0-or-later. An isolated process mutates only its loaded function value.
// This verifies the actual Node runner, not a synthetic count parser.
import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';

const target = fileURLToPath(new URL('../target/test.cjs', import.meta.url));

function assertInjectedOutput(stdout) {
  const parts = stdout.split('\nISOLATED_IN_MEMORY_ASSERTION_FAILURE\n');
  assert.equal(parts.length, 2, 'one injection marker must follow the ordinary run');
  const summaries = parts.map(part => [...part.matchAll(
    /^Ran (\d+) tests containing (\d+) assertions\.\r?\n(\d+) failures, (\d+) errors\.$/gm
  )]);
  assert.equal(summaries[0].length, 1, 'one actual ordinary summary is required');
  assert.equal(summaries[1].length, 1, 'one actual injected summary is required');
  const ordinary = summaries[0][0];
  const injected = summaries[1][0];
  assert.deepEqual(ordinary.slice(3, 5), ['0', '0'], 'the ordinary run must pass');
  assert.deepEqual(injected.slice(3, 5), ['1', '0'], 'exactly one injected assertion must fail');
  assert.deepEqual(injected.slice(1, 3), ordinary.slice(1, 3), 'the same suite must run twice');
  assert.match(parts[1], /^FAIL in \(native-boundary-keeps-trace-and-failure-outcomes\)/m);
  assert.ok(parts[1].includes('actual: (not (= "completed" "forced-review-failure"))'),
    'the injected boundary status must cause the actual failure');
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
  child.stdout.replace('ISOLATED_IN_MEMORY_ASSERTION_FAILURE', 'missing-injection'),
  child.stdout + '\nRan 1 tests containing 1 assertions.\n1 failures, 0 errors.\n',
]) {
  assert.notEqual(badOutput, child.stdout, 'negative fixture must change actual output');
  assert.throws(() => assertInjectedOutput(badOutput), 'invalid injection evidence must be rejected');
}
console.log(`Observed isolated runner exit=${child.status}`);
assert.equal(child.status, 1, 'an actual assertion failure must exit nonzero');
console.log('PASS actual compiled Node runner exits 1 for the forced assertion failure');
