# OpenPlanner TypeScript → ClojureScript Monorepo Roadmap

Date: 2026-06-01
Status: planning
Epic card: `kanban/openplanner-typescript-to-cljs-epic.md`
Inventory: `docs/architecture/typescript-to-cljs-file-inventory.md`

## Living map — 2026-11-01

The following is the current destination and decision map. IDs are local to
this document, **not** GitHub issue numbers or claims that issues exist. The
June inventory and T0–T9 material below are preserved as historical planning
and useful ownership clues: their 756-file count, worktree scope, shadow-only
standard, and strictly blocking order are not a current baseline or a rule that
all architecture must precede any language-parity work. At reference commit
`07085d6557b75834ce6f50e6c54b8ca47e1c7c08`, the no-new-TypeScript gate reports
554 active allowlisted files. Re-measure at each checkout; the historical
counts are not evidence of removal, and submodules have separate ownership.

### Destination and roadmap

- **D-OP-1 — Destination.** A complete Clojure-family OpenPlanner implementation
  with portable specifications and semantics, nbb-first backend development and
  runtime, and JVM-based CI where useful. Eliminate the active TypeScript
  implementation over time without discarding user behavior or tests. Before
  replacing each TypeScript slice, a complete Clojure-family end-to-end fixture
  must pass against the original, then unchanged against the candidate; retain
  existing tests and the reference until parity is established. All new
  implementation and tests are Clojure-family; minimal manifest/configuration
  changes are permitted. No mass deletion or reorganization is implied.
- **R-OP-1 — Roadmap. Parent: D-OP-1.** Inventory ownership and dependencies;
  build evidence-backed, side-by-side runtime and domain capabilities; qualify
  each against reference and existing tests; compose and qualify the whole
  backend; only then propose a separately authorized transition. The epics
  below are parallel-capable lines of work, not a blanket architectural lock on
  incremental implementation.

**Map, not territory:** when an obstacle or new evidence changes the route,
update the affected story, dependency, status and rationale here; preserve the
original destination instead of silently shrinking it. A runtime contract,
static type check, semantic law and authorization decision are different
claims. Preserve observed behavior during parity; record potential defects and
review any semantic correction separately. Local work here does not authorize
production selection, deployment, merge, remote issue creation, or live-data
changes.

The evidence and precise limits for the current isolated checkout are in the
[implementation checkpoint](../migration/clojure-parity-start.md). In
particular, Node 24 has been exercised, not the Dockerfile's Node 22; full app
startup and aggregate tests have not passed in this environment.

### E-OP-1 — JVM-free backend baseline and parity

**Parent:** R-OP-1. **Status:** in progress; candidates remain side-by-side and
production still selects the TypeScript reference.

#### S1.1 — Establish the baseline

**Parent:** E-OP-1. **Story:** As a maintainer, I want a pinned checkout and
truthful build, test, lint and ownership baseline so that migration results can
be compared without concealing pre-existing failures.

- **Acceptance:** record commit, ownership, commands, passes, failures and
  never-run stages; retain existing tests and disclose environment limits.
- **Evidence/status:** in progress. [Checkpoint baseline](../migration/clojure-parity-start.md#baseline-in-this-isolated-environment): SDK build,
  16 focused tests and no-new-TS gate passed; strict lint failed with 27 errors
  and 8,032 warnings; root shadow build failed at Maven resolution, and full
  application/aggregate tests were not run. These are not a green global gate.
- **Dependencies:** authorized official Maven access for original shadow/JVM
  checks; isolated Mongo and required build artifacts for full-stack tests.

#### S1.2 — OpenAPI route parity

**Parent:** E-OP-1. **Story:** As an API consumer, I want the OpenAPI HTTP route
ported without changing its wire contract so that existing clients can consume
it during migration.

- **Acceptance:** run the same complete route fixture first on the original,
  then unchanged on the nbb candidate, with auth, errors, repeated requests and
  document/wire assertions. Record production integration and shared-schema
  export gaps as downstream S4.1 qualification before replacement.
- **Evidence/status:** in progress. `tests/openapi_http_contract.cljs` passed
  against both the original and `src/nbb/openplanner/openapi_route.cljs` in an
  isolated Fastify server. It does not cover full `buildApp` or the separately
  used `openApiSchemas` export in TypeScript search; candidate declarations are
  shared as `.cljc` but can still drift from the unchanged TS reference. See the
  [OpenAPI checkpoint](../migration/clojure-parity-start.md#reference-http-contract).
- **Candidate-stage dependency:** S1.1. **Downstream handoff:** S2.1 for
  portable shared schemas and S4.1 for full-startup qualification; neither
  blocks collecting side-by-side route evidence.

#### S1.3 — Blob routes parity

**Parent:** E-OP-1. **Story:** As a blob API consumer, I want uploads and reads
to retain their filesystem and HTTP behavior on nbb so that stored content and
clients remain compatible.

- **Acceptance:** unchanged Clojure-family fixtures pass original first and
  candidate second with real isolated filesystem, auth, multipart, binary,
  error, configuration and repeat cases. Hand full production startup and
  actual limits to S4.1 before transition. Track, rather than silently fix,
  observed legacy defects.
- **Evidence/status:** in progress. The two `tests/blob_http*_contract.cljs`
  fixtures passed against both implementations with loopback HTTP and temp
  storage, including a reduced size-limit test. Some robustness findings and
  detailed reproductions are retained privately; the public fixture is not
  exhaustive and no fix is implied. See the
  [blob checkpoint](../migration/clojure-parity-start.md#blob-http-story-parent-jvm-free-backend-parity-epic).
- **Candidate-stage dependencies:** S1.1 and the reference auth boundary.
  **Downstream handoff:** S4.1 for production-sized limits, Node 22 and
  complete startup. Any robustness correction needs explicit review as a
  behavior change.

#### S1.4 — Authentication boundary

**Parent:** E-OP-1. **Story:** As an operator, I want the authentication and
CORS boundary characterized and ported with its existing behavior so that a
runtime change cannot silently widen access.

- **Acceptance:** reference-first Clojure-family fixture covers public-path
  and query distinctions, credential rejection/acceptance, methods, CORS,
  preflight and repeats; the unchanged fixture and a negative control pass as
  appropriate for a candidate. Hand production-composition verification to
  S4.1.
- **Evidence/status:** in progress. `tests/auth_http_contract.cljs` passed the
  original first and then the side-by-side
  `src/nbb/openplanner/auth_plugin.cljs`; a no-op negative control failed as
  expected. Production auth has not been changed or selected.
- **Candidate-stage dependency:** S1.1. **Downstream handoff:** S4.1 for full
  app composition and review.

#### S1.5 — Compose the characterized backend slice

**Parent:** E-OP-1. **Story:** As a backend developer, I want the characterized
nbb authentication, OpenAPI and blob components to start and work together so
that compatibility is tested across their registration and effect boundaries,
not just in isolated handlers.

- **Acceptance:** run the same synthetic end-to-end composition fixture against
  reference wiring first and the nbb partial app second; preserve both and
  describe every omitted capability. Full production startup, the unported
  routes, Mongo, Kafka and tenant behavior remain separately required in S4.1.
- **Evidence/status:** in progress. `tests/backend_slice_http_contract.cljs`
  passed reference composition before `src/nbb/openplanner/application.cljs`
  was written, then passed against that partial app. It is not the complete
  `buildApp` and is not selected in production.
- **Candidate-stage dependencies:** the characterized components in S1.2–S1.4.
  **Downstream handoff:** S4.1 for whole-service qualification and review.

### E-OP-2 — Portable language, contracts and checks

**Parent:** R-OP-1. **Status:** investigation/in progress; this epic may
proceed alongside, rather than block all, reference-first route migration.

#### S2.1 — Dual-host contracts and laws

**Parent:** E-OP-2. **Story:** As a domain maintainer, I want shared portable
contracts and executable laws checked on the intended hosts so that the
specification does not depend on one backend.

- **Acceptance:** identify `.cljc`-appropriate shapes/laws, validate examples
  and boundary cases under nbb/Node and the chosen JVM CI host, and document
  effects and host-specific adapters without equating schema validity with
  semantic truth or authorization.
- **Evidence/status:** in progress, with partial nbb structural evidence only.
  `tests/shared_schemas_contract.cljs` passed first against the four actual
  TypeScript schema exports, then unchanged against
  `src/nbb/openplanner/schemas.cljc` via `schemas_boundary.cljs`. It checks a
  full structural snapshot plus Fastify loopback validation, defaults, error
  and serialization behavior. The nbb OpenAPI route consumes the same portable
  declarations and still passes its exact-wire and composition contracts. This
  does not establish JVM evaluation, Malli laws, static type checking, actual
  search/Mongo E2E, or production replacement. The real search import was blocked
  by missing shadow-built document-hydration artifacts. Some pure graph and
  translation probes work on nbb; Closure-dependent boundaries need adaptation.
  Malli is absent from the current nbb classpath, not established incompatible.
  [Shared-schema evidence](../migration/clojure-parity-start.md#portable-json-schema-story-parent-portable-languagechecks-epic).
- **Dependencies:** initial S3.1 discovery and actual host/dependency
  availability, not completion of all S3.1 children; may run concurrently with
  E-OP-1.

#### S2.2 — Real typed checking

**Parent:** E-OP-2. **Story:** As a maintainer, I want a real, reproducible
static typed-Clojure check with explicit interop boundaries so that type claims
are backed by an executed checker rather than annotations alone.

- **Acceptance:** verify supported `typed.cljs` APIs/version, checker setup,
  representative positive and negative cases and CI behavior; distinguish
  runtime validation and static checking, and document which steps require JVM.
- **Evidence/status:** blocked/unverified. `typed.cljs` is not on the current
  nbb classpath; original shadow dependency resolution cannot reach Maven here.
  No static checker pass is claimed.
- **Dependencies:** authorized dependencies and a supported checker setup;
  S2.1 for relevant contracts where applicable.

#### S2.3 — Closure compatibility and claim IDs

**Parent:** E-OP-2. **Story:** As a graph-data owner, I want the nbb adapter to
reproduce the actual reference Closure hash and object-boundary semantics so
that existing identifiers and input behavior do not silently change.

- **Acceptance:** run reference-backed vectors and differential cases against
  the exact resolved 2025 Clojure Closure fork, including non-ASCII, surrogate,
  NUL, block boundaries and JavaScript object types; only then qualify exact
  fork compatibility or wire the reviewed SHA candidate into graph claims and
  rerun existing graph tests.
- **Evidence/status:** in progress, not complete. The nbb SHA candidate remains
  unwired: it matched 13 vectors and 256 differential cases against the
  distinct official 2023 npm Closure library, not the exact fork. An earlier
  nbb object-predicate attempt was withdrawn after review found divergence when
  same-process code mutates `Function.prototype.toString`; that is a
  compatibility finding, not a claim of remote exploitability. The revised
  cache `js_compat.cljc` retains the original `goog/typeOf` expression on the
  Shadow `:cljs` branch and uses directly declared, already-locked Lodash
  `isObjectLike` plus `Array.isArray` on nbb. A reference-first 2023 Closure
  matrix, including intrinsic mutation, cross-realm values and revoked proxies,
  passed against the revised boundary. The unchanged cache suite now passes
  under nbb (5 tests/21 assertions), as does document hydration (4 tests/17
  assertions). Export fixtures derive expected names from the checked-in
  `shadow-cljs.edn` files and exercise source functions through the nbb export
  maps; a Node ESM smoke test passed. These are not tests of compiled Shadow ESM
  behavior, generated `dist` artifacts or production integration. Exact
  compatibility with the 2025 fork, Shadow compile, SDK static ESM import,
  Mongo and whole-backend behavior remain unproven. UTF-8 and `object?` are not safe blanket substitutes.
  See the [hash evidence](../migration/clojure-parity-start.md#candidate-legacy-string-hash-adapter)
  and [cache/hydration evidence](../migration/clojure-parity-start.md#cache-and-document-hydration-nbb-dependency-story).
- **Dependencies:** access to the exact original dependency/build without
  bypassing network restrictions; S1.1; graph boundary integration tests.

#### S2.4 — Declaration ownership and precedents

**Parent:** E-OP-2. **Story:** As a language-tooling maintainer, I want to
connect portable declarations and laws to Katamorph's existing responsibility
so that intent can guide host-specific implementations without creating a
competing DSL.

- **Acceptance:** compare Katamorph responsibilities with existing
  language-design precedents; record ownership, executable checks, gaps and
  host choices before proposing integration.
- **Evidence/status:** research identified precedents for further investigation;
  no runtime enforcement or static-checking result is claimed here.
- **Dependencies:** S2.1–S2.2 evidence and Katamorph ownership review; no new
  parallel framework or implicit cross-repository change.

### E-OP-3 — Backend and domain migration

**Parent:** R-OP-1. **Status:** inventory and design in progress. The legacy
T1–T9 groups below remain ownership inputs, not automatic authorization to
retire packages or alter external systems.

#### S3.1 — Capability and dependency inventory

**Parent:** E-OP-3. **Story:** As a migration lead, I want every remaining
backend capability assigned an owner and a linked child story so that the
whole implementation, including delegated packages, is accounted for.

- **Acceptance:** reconcile current tracked files and package/submodule
  boundaries, dependencies and observable contracts; create local child
  stories with acceptance and reference fixtures before implementation.
- **Evidence/status:** in progress. Root `src/` owns the Fastify app; SDK,
  graph, storage, translation and other workspace packages own distinct
  components. Both submodules were uninitialized. See the
  [ownership checkpoint](../migration/clojure-parity-start.md#ownership-and-boundaries).
- **Dependencies:** S1.1 and per-checkout inventory. Initial children, each
  parented to S3.1, are listed below; they must be refined as evidence changes.

| Local child | User story | Acceptance and evidence | Status / dependencies |
| --- | --- | --- | --- |
| S3.1a | As an API maintainer, I want every root route and its effects inventoried so that no HTTP capability is lost. | Route-to-contract/dependency map and reference-first fixtures; OpenAPI/blob evidence exists. | In progress; S1.1–S1.5. |
| S3.1b | As an SDK consumer, I want delegated data-plane operations inventoried so that an HTTP shell rewrite does not masquerade as a core rewrite. | SDK/core inventory and unchanged direct/HTTP consumer tests; SDK build passed. | In progress; S1.1 and isolated service fixtures. |
| S3.1c | As a graph and signal consumer, I want package boundaries and runtime interop inventoried so that claim and event semantics remain stable. | Claims, graph, signals and Closure dependencies mapped with tests; pure graph core probe exists. | In progress; S2.3 and package owners. |
| S3.1d | As a workspace maintainer, I want remaining agents, clients, UI, pseudo, archive and gitlink ownership classified so that no active TypeScript is silently omitted. | Reconciled T3–T9 inventory, explicit owner and authorized disposition per package; old counts are historical. | Planned; S1.1 and owning repositories. |

#### S3.2 — Backend-independent storage contracts

**Parent:** E-OP-3. **Story:** As a storage implementer, I want portable store
contracts and isolated backend adapters so that OpenPlanner semantics can move
between backends without changing clients or touching live data.

- **Acceptance:** specify and exercise operations, errors, ordering, retention
  and boundary examples against isolated reference and candidate backends;
  distinguish proven parity from known discrepancies.
- **Evidence/status:** planned; no isolated Mongo daemon/container was
  available here. Do not substitute a mock contract for required end-to-end
  evidence or use Atlas/live credentials.
- **Dependencies:** S2.1, S3.1b, isolated backend availability.

#### S3.3 — Clio event compatibility

**Parent:** E-OP-3. **Story:** As an event consumer, I want OpenPlanner's event
contracts reconciled with Clio so that future interoperability is explicit and
preserves current behavior until a reviewed change is authorized.

- **Acceptance:** map schemas, laws, versioning and event semantics; link the
  work to [Foresight #51](https://github.com/open-hax/foresight/issues/51)
  and [Foresight #52](https://github.com/open-hax/foresight/issues/52), verify
  their actual state and ownership before implementation, and test with
  synthetic events. Record
  duplicate/TTL/order discrepancies separately, not as silent rewrite fixes.
- **Evidence/status:** planned; references are related work identifiers, not a
  claim they are complete or a substitute for source verification.
- **Dependencies:** S2.1, S3.1c, owner review and access to the relevant
  Foresight evidence.

### E-OP-4 — Whole-backend qualification and authorized transition

**Parent:** R-OP-1. **Status:** planned; no production switch is authorized by
isolated test results.

#### S4.1 — Qualify the whole backend

**Parent:** E-OP-4. **Story:** As an operator, I want the complete nbb backend
and its portable contracts exercised with real isolated dependencies so that
readiness reflects end-to-end behavior rather than a collection of route demos.

- **Acceptance:** production-equivalent startup/configuration, auth, all
  required routes and dependency lifecycles pass reference/candidate E2E and
  retained existing tests; test Node 22 as deployed as well as supported
  versions, run actual type/law and zero-warning gates, and obtain review.
- **Evidence/status:** not started as a full-backend qualification; isolated
  route evidence is in E-OP-1, static and global checks remain open.
- **Dependencies:** E-OP-1 through E-OP-3 capability completion, isolated
  service infrastructure and executable CI/toolchain.

#### S4.2 — Review and authorize transition

**Parent:** E-OP-4. **Story:** As the service owner, I want a separately
reviewed transition and rollback decision after qualification so that changing
production behavior or retiring reference code is deliberate and authorized.

- **Acceptance:** present evidence, remaining defects, ownership, rollback and
  explicit authorization before any production selection, deletion, merge or
  deployment; verify the authorized outcome afterward.
- **Evidence/status:** not started and not authorized by this document.
- **Dependencies:** S4.1 and explicit authorization for the proposed external
  state change.

---

## Historical June 2026 plan and inventory

The material below is preserved for context and package ownership. Where it
conflicts with the living map above, the current destination, test-preservation
rules, nbb-first backend choice, separate transition authorization, and
revision rule take precedence.

## Scope

This roadmap covers the active OpenPlanner worktree under `orgs/open-hax/openplanner` and every source TypeScript/TSX declaration/config/test file found outside generated/vendor directories.

Exclusions from the active inventory:

- `node_modules/`
- `dist/`, `build/`, `target/`, `.shadow-cljs/`, `.next/`, `coverage/`, `tmp/`, `vendor/`
- nested `.worktrees/` checkouts, because they are separate worktree state rather than active monorepo source

`archive/` and `pseudo/` are included. Archive code must be either deleted/retired or explicitly ported; it is not ignored.

## Inventory summary

Active TypeScript-family files found: **756**.

| Kind | Count |
| --- | ---: |
| source `.ts` | 448 |
| frontend `.tsx` | 132 |
| tests/specs | 149 |
| tooling/config `.ts` | 12 |
| declarations `.d.ts` | 15 |

## Package inventory

| TS files | Package/root | Package name | Shape | Workspace deps |
| ---: | --- | --- | --- | --- |
| 213 | `packages/agents/knoxx/frontend` | `@open-hax/knoxx-frontend` | src:57, tsx:101, test:49, cfg:5, dts:1 | @open-hax/garden-publication-components |
| 104 | `packages/agents/cephalon/packages/cephalon-ts` | `@promethean-os/cephalon-ts` | src:79, test:21, cfg:3, dts:1 | @promethean-os/event, @promethean-os/openplanner-cljs-client, @open-hax/sintel |
| 104 | `pseudo/clients` | `@open-hax/cli-client` | src:80, test:23, dts:1 | @promethean-os/logger, @promethean-os/ollama-queue, @promethean-os/opencode-interface-plugin, @promethean-os/persistence |
| 68 | `.` | `@open-hax/openplanner` | src:54, test:12, dts:2 | @open-hax/garden-publication-components, @open-hax/openplanner-document-hydration, @open-hax/openplanner-graph-claim-core, @open-hax/openplanner-store-cache, @open-hax/openplanner-translation-core |
| 51 | `archive/persistence` | `@promethean-os/persistence` | src:45, test:6 | @promethean-os/embedding, @promethean-os/logger |
| 32 | `pseudo/workbench` | `@promethean-os/opencode-unified` | src:1, test:30, cfg:1 | @promethean-os/openplanner-cljs-client, @promethean-os/opencode-cljs-client, @promethean-os/openplanner-cljs-client |
| 30 | `packages/signals/sintel` | `@open-hax/sintel` | src:29, test:1 | @open-hax/signal-contracts, @open-hax/signal-radar-core |
| 17 | `pseudo/janus` | `@promethean-os/janus` | src:14, test:3 | @workspace/mcp-oauth |
| 15 | `packages/agents/cephalon/packages/cephalon-ts/src/ui` | `cephalon-ui` | src:2, tsx:12, cfg:1 | — |
| 15 | `packages/agents/knoxx` | `@open-hax/knoxx` | src:3, tsx:12 | — |
| 15 | `packages/graph/graph-weaver` | `@workspace/graph-weaver` | src:15 | @workspace/graph-weaver-aco |
| 12 | `archive/reconstituter` | `@promethean-os/reconstituter` | src:7, test:4, cfg:1 | @promethean-os/opencode-cljs-client, @promethean-os/openplanner-cljs-client |
| 11 | `archive/semantic-graph-builder` | `@workspace/semantic-graph-builder` | src:10, dts:1 | — |
| 11 | `packages/gardens/publication-components` | `@open-hax/garden-publication-components` | src:2, tsx:7, cfg:1, dts:1 | — |
| 10 | `packages/graph/myrmex` | `@workspace/myrmex` | src:10 | @workspace/graph-weaver-aco |
| 10 | `packages/graph/webgl-graph-view` | `@octave-commons/webgl-graph-view` | src:10 | — |
| 9 | `packages/graph/graph-weaver-aco` | `@workspace/graph-weaver-aco` | src:9 | — |
| 7 | `packages/signals/event` | `@promethean-os/event` | src:7 | @promethean-os/test-utils |
| 6 | `packages/graph/eros-eris-field` | `@workspace/eros-eris-field` | src:6 | — |
| 3 | `pseudo/aether` | `@workspace/aether` | src:3 | — |
| 1 | `archive/embedding` | `@promethean-os/embedding` | src:1 | — |
| 1 | `packages/clients/opencode-cljs-client` | `@promethean-os/opencode-cljs-client` | dts:1 | — |
| 1 | `packages/clients/openplanner-cljs-client` | `@promethean-os/openplanner-cljs-client` | dts:1 | — |
| 1 | `packages/graph/eros-eris-field-app` | `@workspace/eros-eris-field-app` | src:1 | @workspace/eros-eris-field |
| 1 | `packages/graph/graph-claim-core` | `@open-hax/openplanner-graph-claim-core` | dts:1 | — |
| 1 | `packages/signals/signal-contracts` | `@open-hax/signal-contracts` | dts:1 | — |
| 1 | `packages/signals/signal-radar-core` | `@open-hax/signal-radar-core` | dts:1 | — |
| 1 | `packages/stores/cache` | `@open-hax/openplanner-store-cache` | dts:1 | — |
| 1 | `packages/stores/document-hydration` | `@open-hax/openplanner-document-hydration` | dts:1 | @open-hax/openplanner-store-cache |
| 1 | `packages/translations/translation-core` | `@open-hax/openplanner-translation-core` | dts:1 | — |
| 1 | `packages/utils/logger` | `@promethean-os/logger` | src:1 | — |
| 1 | `packages/utils/test-utils` | `@promethean-os/test-utils` | src:1 | — |
| 1 | `pseudo/mcp-oauth` | `@workspace/mcp-oauth` | src:1 | — |

## Dependency-ordered task groups

The order below is a blocking order, not a promise that all work is strictly serial. Work in the same level can run in parallel after its blockers are green.

### T0 — Freeze, accounting, and CLJS package standard

**Blocks:** every other task.

**Goal:** make TypeScript retirement enforceable before implementation begins.

**Work:**

- Keep this inventory current in CI.
- Add a no-new-TypeScript gate with explicit allowlist for files still owned by an open migration task.
- Standardize CLJS package skeletons: `shadow-cljs.edn`, `deps.edn`, `src/cljs`, `test/cljs`, ESM exports, Node import smoke checks.
- Document compatibility shim policy: no business logic in TypeScript shims, and every shim has a deletion condition.

**Exit:** CI can report remaining TS files by task group and fail on unowned new TS.

### T1 — Leaf shared packages and generated declaration stubs

**Blocks:** root OpenPlanner app, agents, pseudo clients, and downstream graph/signal consumers.

**Packages/files:**

- `packages/utils/logger` (1)
- `packages/utils/test-utils` (1)
- `packages/stores/cache` (1)
- `packages/stores/document-hydration` (1)
- `packages/translations/translation-core` (1)
- `packages/graph/graph-claim-core` (1)
- `packages/clients/opencode-cljs-client` (1 declaration)
- `packages/clients/openplanner-cljs-client` (1 declaration)
- `packages/signals/signal-contracts` (1 declaration)
- `packages/signals/signal-radar-core` (1 declaration)

**Goal:** remove low-count dependency leaves and replace TS declaration-only facades with CLJS-authored or generated package surfaces.

**Exit:** dependent packages can import CLJS/ESM outputs without TypeScript type shims as source authority.

### T2 — Root OpenPlanner API/runtime CLJS port

**Blocks:** Graph Weaver imported mode, CLI/client parity, Cephalon runtime, workbench, and final archive retirement.

**Files:** `src/**/*.ts` plus root `tests/**/*.ts` (**68 files**).

**Goal:** port canonical OpenPlanner HTTP, Mongo, vector, graph, translation, tenant, label, event, and health paths to CLJS while preserving current runtime behavior.

**Subtasks:**

1. CLJS Fastify app skeleton and route registration parity.
2. Mongo/vector/TTL/label-retention data layer port.
3. Graph/export/layout routes port; define cursor/pagination contract required by Graph Weaver.
4. Translation, documents, events, lakes, sessions, and tenant routes port.
5. Test harness migration from TS tests to CLJS/node-test.

**Exit:** root OpenPlanner service starts from CLJS, health/routes pass, and generated TS output is absent from source.

### T3 — Graph package tranche

**Blocks:** graph-memory workbench truth, Myrmex orchestration, graph UI retirement, P1B/P2 graph roadmap cleanup.

**Order inside group:**

1. `packages/graph/graph-weaver-aco` (9) — leaf ACO engine.
2. `packages/graph/graph-weaver` (15) — Fastify plugin/importable runtime; depends on ACO and root graph export cursor contract.
3. `packages/graph/myrmex` (10) — orchestrator/crawler; depends on ACO and canonical OpenPlanner writes.
4. `packages/graph/webgl-graph-view` (10) — front-end/view layer; depends on Graph Weaver API shape.
5. `packages/graph/eros-eris-field` (6) and `packages/graph/eros-eris-field-app` (1) — field library then app.

**Exit:** Graph Weaver and graph-adjacent packages build from CLJS, standalone/imported modes use the same runtime, and Graph Weaver no longer treats capped exports as full graph truth.

### T4 — Signals/events tranche

**Blocks:** Cephalon TS runtime and any signal-agent consumers.

**Order inside group:**

1. `packages/signals/event` (7) — event contract/runtime leaf.
2. `packages/signals/sintel` (30) — discovery/integration/memory signal runtime, after signal contracts/radar core from T1.

**Exit:** signal packages expose CLJS/ESM APIs and their tests are CLJS-native.

### T5 — Pseudo/service adapters and client tools

**Blocks:** workbench, Janus flows, and any legacy CLIs that currently route through TS persistence/client packages.

**Order inside group:**

1. `pseudo/mcp-oauth` (1) — leaf for Janus.
2. `pseudo/janus` (17) — depends on `mcp-oauth`.
3. `pseudo/aether` (3) — independent small pseudo package.
4. `pseudo/clients` (104) — depends on logger and legacy persistence decision; should be repointed to canonical OpenPlanner CLJS clients rather than archive persistence if possible.
5. `pseudo/workbench` (32) — depends on CLJS clients.

**Exit:** pseudo packages either build from CLJS or have been explicitly deleted/archived as non-product experiments.

### T6 — Agent package tranche

**Blocks:** final TS removal because this is the largest active TS/TSX surface.

**Order inside group:**

1. `packages/agents/knoxx` (15) — package-level pseudo/shell TSX cleanup; backend is already CLJS and remains the reference implementation.
2. `packages/agents/knoxx/frontend` (213) — React/TSX frontend port to CLJS UI stack, after shared UI/component strategy is chosen.
3. `packages/agents/cephalon/packages/cephalon-ts` (104) — migrate or retire after signals, OpenPlanner clients, and graph APIs stabilize.
4. `packages/agents/cephalon/packages/cephalon-ts/src/ui` (15) — UI slice after Cephalon runtime decision.

**Exit:** active agent packages have no TS/TSX source. Knoxx backend CLJS patterns remain canonical for backend/runtime code.

### T7 — UI/component leaf packages

**Blocks:** Knoxx frontend migration if reused as shared UI primitives.

**Packages:**

- `packages/gardens/publication-components` (11)

**Goal:** port shared UI components before high-volume app frontend work consumes them.

**Exit:** garden publication components are CLJS-native or replaced by a CLJS UI package.

### T8 — Archive disposition and legacy dependency breakage

**Blocks:** final all-TS-zero claim; may also block `pseudo/clients` if it still depends on `@promethean-os/persistence`.

**Archive packages:**

- `archive/embedding` (1)
- `archive/persistence` (51)
- `archive/reconstituter` (12)
- `archive/semantic-graph-builder` (11)

**Goal:** do not port dead code by default. For each archive package, choose one of:

1. delete/remove from workspace,
2. keep as inert historical artifact outside active build/source inventory,
3. port only if a live package still depends on it and no better canonical OpenPlanner replacement exists.

**Exit:** archive TS is either removed from active source inventory or has a signed reason to remain outside the no-TS gate.

### T9 — Tooling/config/test final sweep

**Blocks:** final TypeScript-free monorepo assertion.

**Scope:** remaining `.config.ts`, `.d.ts`, TS test files, tsconfig references, tsx/tsup/playwright/ava config files, package `types` fields, and stale devDependencies (`typescript`, `tsx`, `tsup`, `@types/*`) after all ports.

**Exit:** `find` returns zero active `.ts/.tsx/.cts/.mts` source files outside explicitly generated artifacts; CI no-new-TS gate passes with an empty allowlist.

## Critical path

```text
T0 freeze/standard
  -> T1 shared leaves
  -> T2 root OpenPlanner CLJS runtime/API
     -> T3 graph tranche
     -> T5 pseudo/client tools
     -> T6 agent tranche
  -> T8 archive disposition
  -> T9 final sweep
```

Parallel lanes after T1:

```text
T1 -> T4 signals -> T6 Cephalon
T1 -> T7 shared UI -> T6 Knoxx frontend
T1 -> T3 ACO leaf -> T3 Graph Weaver/Myrmex
```

## Definition of done for the epic

- Active inventory count is zero for `.ts`, `.tsx`, `.cts`, `.mts`, and `.d.ts` source files outside generated build output.
- OpenPlanner root service, Graph Weaver imported/standalone modes, Knoxx frontend/backend, signal packages, pseudo clients, and graph packages build/test from CLJS.
- Archive code has explicit disposition and is not part of active source inventory.
- TypeScript dependencies and tsconfig/tooling files are removed unless only used for external generated artifacts with documented reason.
- CI prevents new TypeScript source unless a migration owner updates the roadmap and allowlist.
