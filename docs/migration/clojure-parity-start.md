# Clojure-family migration: implementation checkpoint

## Destination → roadmap → epic → stories

- **Destination:** a complete Clojure-family OpenPlanner implementation with
  portable contracts; nbb for everyday backend work and JVM tooling where CI
  requires it.
- **Roadmap**, parent: destination — migrate in behavior-preserving stages,
  retain reference implementations until parity, and make backend selection
  explicit rather than assuming all runtimes are interchangeable.
- **Epic**, parent: roadmap — JVM-free OpenPlanner backend with behavioral
  parity. The backend is not yet complete or production-selected.
- **Stories**, parent: this epic — OpenAPI HTTP route parity; filesystem blob
  HTTP route parity; Closure hashing compatibility investigation; and auth
  boundary parity, plus a three-route composition checkpoint. These are steps toward the destination, not
  a replacement for a roadmap or evidence of whole-backend completion.

Reference checkout: `07085d6557b75834ce6f50e6c54b8ca47e1c7c08`.
The user selected nbb for backend migration. A side-by-side nbb route now
exists, but no TypeScript implementation has been removed or replaced in the
production startup. This is not a claim that the application or full test
suite passes.

## Ownership and boundaries

- Root `src/` is the Fastify API. `src/app.ts` registers the real auth and Mongo
  plugins and `src/routes/v1/index.ts`; `src/main.ts` starts the server.
- `packages/` contains separate workspace-owned components. Several root API
  shells delegate to the SDK; e.g. `/v1/mongo` delegates to
  `packages/openplanner-sdk/src/mongo-browse.ts`. Root build also depends on
  separately built graph-claim, translation, cache, hydration, publication,
  and SDK packages. A route rewrite does not imply its delegated core is
  rewritten.
- Gitlinks `packages/vexx` (`296905f37fa8ca24bfa08faaad555879a5033d68`) and
  `packages/stores/migrations/openplanner-migration-tools`
  (`a0c7919d448132838c56bd7d13793978c4ef7700`) were not initialized or
  changed. The latter's configured URL is a developer-local `file://` URL.
- The root `AGENTS.md` requires zero-warning compile/test/lint and review of
  all changed files. It names skills but this checkout contained no
  `.agents/skills` files. Do not weaken these gates or delete existing tests.

## Reference HTTP contract

`pnpm test:contract:openapi` runs a ClojureScript test using nbb against an
isolated loopback Fastify server. It registers the existing auth plugin and the
unaltered `openApiRoutes` at `/v1`, with a synthetic key. It checks missing and
invalid credentials and their error bodies; preflight and CORS headers; the
full successful JSON document against `tests/fixtures/openapi-07085d65.json`;
repeat/query behavior; automatic HEAD; and unsupported POST. The snapshot was
captured from the reference above; its SHA-256 is
`76d9ba0e3975416e3e3bff8e9b748d1166cd2d384bff1a26ff960a1ca0f0176a`.
The `OPENPLANNER_OPENAPI_MODULE` environment variable selects an ESM module or
an nbb `.cljs` file exporting `openApiRoutes`. The same test passed first
against the reference, then unchanged against
`src/nbb/openplanner/openapi_route.cljs` via
`pnpm test:contract:openapi:nbb`. It checks the 4,946-byte JSON wire SHA-256
`745dbaa1873009d92b780c47c211e9c05c9c0ca82b10d642e9dc3637f7a0a702` as
well as parsed structure. Node ESM's `nbb/loadFile` also registered the
candidate with Fastify in an isolated probe.

This covers the isolated HTTP route and auth composition, **not** the entire
production `buildApp` startup or the other `openApiSchemas` export consumed by
search. It is not by itself parity evidence for replacing the whole module or
for any Mongo-dependent route. The candidate now consumes shared `.cljc` JSON Schema data, copied from the
TS-owned reference at the checkpoint. Running *both* contract commands remains
necessary to detect reference drift; the TypeScript search/state routes do not
yet consume the candidate declarations. It was registered only in isolated
tests, not selected by production startup. The nbb run does not prove
compilation under shadow-cljs or a typed.cljs check.

## Baseline in this isolated environment

- `pnpm install --frozen-lockfile --ignore-scripts --store-dir /tmp/openplanner-pnpm-store`:
  passed, with two warnings about missing generated `sintel` bins. Build scripts
  were disabled; this is not evidence that native modules were built.
- `pnpm --filter @open-hax/openplanner-sdk build`: passed.
- `node --import tsx --test src/tests/embedding-models.test.ts src/tests/sentence-split.test.ts`:
  16 passed, 0 failed. This is only a focused subset.
- `pnpm test:contract:openapi` and `pnpm test:contract:openapi:nbb`: passed
  against the original route and side-by-side nbb route, respectively.
  Selecting a module without `openApiRoutes` failed as expected.
- `pnpm check:no-new-typescript`: passed; the existing inventory reports 554
  active allowlisted TypeScript-family files, of which 92 are assigned to the
  root API/SDK tranche (including 69 at the root and 22 in the SDK).
- Hosted PR head `5dfdb25f967372fb65308dcd6997e0f0db341f97`: the nbb
  parity workflow passed on Node 22, including the reference SDK build,
  characterized HTTP/schema/composition tests, nbb package contracts and
  focused existing tests. Code Quality passed its no-new-TypeScript,
  deployment-boundary and duplication jobs; it did not run strict lint or
  the full build. These are results for that head, not a whole-app pass.
- `pnpm lint:strict`: failed on the unmodified baseline with 27 errors and
  8,032 warnings; unrelated legacy files were not changed to hide this.
- `pnpm build`: failed in the graph-claim-core shadow-cljs dependency setup,
  before that package compiled. A writable Java home exposed the underlying
  DNS failure reaching `repo1.maven.org`. A scoped escalation attempt did not
  execute: `bwrap` failed creating the policy-denied sessions path as read-only.
  No network restriction was bypassed.
- Full application startup and aggregate tests were not run: required CLJS
  package `dist` artifacts are not checked in and could not be built here;
  the production app also requires Mongo, and neither `mongod` nor a container
  runtime was available. No live Mongo or credentials were used.

## Next gate

Continue nbb development without requiring the JVM for its runtime; retain the
original builds and restore authorized official Maven access when verifying
shadow-cljs or the JVM-based static checker. Provide isolated Mongo for
storage-dependent fixtures. Verify exact `typed.cljs` APIs/version with a real
check; a runtime
Malli contract and a static type check answer different questions. For a
chosen narrow slice, first complete and run its Clojure-family end-to-end
fixture against the reference, including production integration, error and
boundary behavior. Then implement a side-by-side Clojure-family candidate,
run the same fixture unchanged, run relevant existing tests and zero-warning
checks, and obtain a clean review before considering any replacement. Preserve
observable behavior in this parity stage; record suspected defects separately.

## Nbb compatibility observations

Installed nbb 1.6.214 executes the existing pure graph-claims and translation
cores. Small macro, protocol, record, and `deftype` probes worked; the official
README's `deftype` caveat did not match that minimal probe. Actual graph
boundary loading stopped at `goog.crypt.Sha256`, and translation/cache
boundaries at `goog/typeOf`. Malli and typed.cljs were absent from the nbb
classpath; this is not proof of incompatibility. Node ESM can load an nbb
file and call an ordinary CLJS function. An nbb `^:async` function returned
through `loadFile` was not a native function accepted by Fastify's plugin
registration, so this synchronous route uses Fastify's callback-style
registration. This does not mean `^:async`/`await` syntax is unsupported.

A temporary cache diagnostic substituting `object?` passed existing tests but
was **not** equivalent to `goog/typeOf`: Date, Map and other JS objects exposed
counterexamples. Do not apply that substitution as a migration fix. Node
crypto's usual UTF-8 string hashing is likewise not assumed equivalent to
Closure's string handling. Both require reference-backed boundary tests.

## Candidate legacy string-hash adapter

`src/nbb/openplanner/closure_sha.cljs` is an **unwired candidate**, not a
replacement for the graph-claim boundary. Before writing it, the fixed vectors
in `tests/closure_sha_contract.cljs` were run against the official
`google-closure-library` npm package `20230802.0.0`; the candidate then passed
them and 256 deterministic mixed-UTF-16 differential cases against that same
library. The vectors include non-ASCII, NUL, surrogate, and SHA-block boundary
inputs. Default `pnpm test:contract:closure-sha:nbb` runs the fixed vectors
without requiring Closure. To run the optional differential, provide a local,
trusted installation of exactly that official version as
`OPENPLANNER_CLOSURE_REFERENCE=/absolute/package/directory`.

This is narrower than project parity: shadow-cljs 3.4.10 declares
`org.clojure/google-closure-library` `0.0-20250515-f04e4c0e`, a different
Clojure-maintained fork. That exact shadow build could not be run here, so the
2023 result does **not** prove the candidate reproduces every original graph
claim ID. Run the same and broader tests against the actual resolved fork,
including any JavaScript object/type edge cases, before wiring the adapter or
changing existing boundary code. The legacy hash truncates UTF-16 code units
to bytes in the tested Closure implementation; ordinary UTF-8 hashing is not a
safe substitute.

## Blob HTTP story (parent: JVM-free backend parity epic)

`tests/blob_http_contract.cljs` passed first against unchanged
`src/routes/v1/blobs.ts` and then unchanged against the side-by-side
`src/nbb/openplanner/blob_routes.cljs` using `pnpm test:contract:blobs` and
`pnpm test:contract:blobs:nbb`. It uses loopback HTTP, actual Fastify, auth and
multipart, and a disposable real filesystem. It covers binary and empty file
round trips, byte-based SHA-256 and sharded paths, metadata, repeated
no-overwrite behavior, invalid/missing IDs, auth and preflight, no-file and
multipart errors. The candidate is not selected by production startup; this
is not yet proof of full production startup. The additional unchanged
`tests/blob_http_edge_contract.cljs` passed reference first and candidate
second: fallback directories, invalid/encoded IDs, multi-file/filename handling,
HEAD, actual filesystem errors, and a reduced multipart size limit. The
permission tests ran as uid 1000; they do not exercise the full production
200 MiB limit.

Some robustness findings and their detailed reproductions are retained
privately for review. The public fixtures deliberately do not cover every
failure mode; no behavioral fix is implied by this checkpoint.

## Auth contract story (parent: JVM-free backend parity epic)

`tests/auth_http_contract.cljs` passes against the original TS plugin via
`pnpm test:contract:auth`. It checks public-path/query distinctions, synthetic
bearer credentials, CORS, preflight, multiple methods and repeated requests.
An intentionally empty plugin failed as a negative control. The unchanged
fixture also passes against `src/nbb/openplanner/auth_plugin.cljs` via
`pnpm test:contract:auth:nbb`. The original auth plugin remains in production;
independent review and full application integration are separate gates.

## Composed backend slice (parent: JVM-free backend parity epic)

The reference-first `tests/backend_slice_http_contract.cljs` passed against
the original TS auth, OpenAPI, and blob modules in a partial Fastify app.
The unchanged test then passed against `src/nbb/openplanner/application.cljs`,
a reusable nbb builder for those three components. It exercises real HTTP,
auth, multipart, a temporary filesystem and an OpenAPI wire checksum. Run
`pnpm test:contract:backend-slice` and
`pnpm test:contract:backend-slice:nbb` for the two providers. This builder is
not the production application: it omits Mongo, Kafka, tenant handling, root,
and all other routes, and no startup default has been switched.

## Portable JSON Schema story (parent: portable language/checks epic)

`tests/shared_schemas_contract.cljs` first passed against the actual four
TypeScript exports from `openapi.ts` and `state.ts`, then unchanged against
`src/nbb/openplanner/schemas.cljc` through `schemas_boundary.cljs`. It checks
complete schema structure against `tests/fixtures/schemas-07085d65.json`
(SHA-256 `9a487bb475025fb53eaa298adef2ef1ddc0e08bff64be777a613c7a85e3c0151`)
and runs real Fastify loopback validation, defaults, rejected inputs and
response serialization using synthetic handlers. Run
`pnpm test:contract:schemas` and `pnpm test:contract:schemas:nbb`. The nbb
OpenAPI route consumes these same declarations; its original/candidate wire
contract and partial-app composition were rerun after extraction.

The `.cljc` values are JSON Schema shapes, not Malli semantic laws, static type
proofs, or authorization decisions. The real search route could not be loaded
because the shadow-built document-hydration package is missing; Mongo search
and state behavior, JVM-host evaluation and static checking remain unverified.
The original TypeScript schema exports and all original tests remain intact.

## Cache and document-hydration nbb dependency story

Before adaptation, the unchanged cache and document-hydration suites failed to
load under nbb at `goog/typeOf`. A first experimental classifier passed common
cases but independent review found it could misclassify native functions if
`Function.prototype.toString` was modified; that wiring was withdrawn. The
regression is preserved in `tests/closure_object_intrinsic_regression.cljs`.

The revised `packages/stores/cache/src/openplanner/stores/cache/js_compat.cljc`
uses existing locked Lodash 4.18.1's documented `isObjectLike` native `typeof`
extern plus `Array.isArray` on nbb, and retains the original `goog/typeOf`
expression in the Shadow `:cljs` branch. Lodash is declared directly in the
root and cache package because nbb resolves npm modules relative to the entry
script. No handwritten JavaScript or evaluation escape is used.
`tests/closure_object_contract.cljs` ran first against official Google Closure
npm `20230802.0.0`, then against the nbb boundary, including native/SCI
functions, intrinsic mutation, revoked proxies and cross-realm values. The
unchanged suites now pass under nbb: `pnpm test:nbb:cache` (5 tests, 21
assertions) and `pnpm test:nbb:document-hydration` (4 tests, 17 assertions).

`src/nbb/openplanner/cache_exports.cljs` and `hydration_exports.cljs` expose
those existing namespaces through `nbb/loadFile` with the named API surfaces
listed in their Shadow configs, without inventing generated dist. Their
`cache_exports_contract.cljs` and `hydration_exports_contract.cljs` fixtures
read expected names from the checked-in Shadow configs and exercise both
source-derived nbb maps and the candidate maps. These are not executions of
the unavailable compiled Shadow ESM reference artifacts. A plain Node ESM probe also loaded both and exercised a cache
round trip and a hydration predicate without a JVM. These APIs are not yet
wired into the TypeScript SDK's static ESM imports or production startup.

These are nbb source/test results, not a Shadow build pass, proof for every host
value, or evidence against the distinct 2025 Clojure Closure fork. The Shadow
expression is retained but could not be compiled or run here. The SDK still
imports a missing shadow-built hydration `dist` artifact; an isolated Mongo and
whole-service qualification are also still needed.

## Hosted Shadow baseline and dual-host structural check

The PR-only `shadow-baseline.yml` workflow is configured to check out the
immutable original revision and candidate head separately, run the existing
root build, and run four inspected synthetic package suites. It also runs the
same `tests/openplanner/schema_laws_test.cljc` structural assertions under nbb
and JVM Clojure using Shadow's `clj-run`. A failed build or suite remains a
failed job; later package suites are still attempted after a successful install.
The nbb law run passed locally; JVM execution and the new hosted workflow are
not yet established by this note. These assertions do not establish Malli
semantic laws, static typing, authorization, search behavior or full app E2E.
