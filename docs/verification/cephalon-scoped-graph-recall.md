# Scoped graph selection for the character loop

This is the pure OpenPlanner portion of the accepted B3 milestone, task
`a1e9d6af-0233-4dcb-9677-5c76fa9a2701`. Storage/identity integration and automatic
Knoxx prompt inclusion remain unfinished. Passing this package does not complete
B3, the full recall story, physical field persistence or the separate mood model.

## Run the actual library

From `packages/graph/graph-claim-core`:

```sh
clojure -M:recall-test
pnpm exec shadow-cljs --force-spawn compile test
node target/test.cjs
pnpm exec shadow-cljs --force-spawn release lib
node scripts/verify-scoped-recall.mjs
```

The Node suite retains every existing edge-claim test. The final demonstration
imports the released ESM export, creates one private temporary JSON fixture,
checks a graph-only neighbor and its complete path, exercises absent decisions,
prints the actual result and deletes only its own fixture. It performs no database,
REST, model, field write or social call. Fixture identity/revisions are explicit
examples; they do not establish current stored authority or deployed state.

## Contract and causal boundary

The named Malli registry is serializable Clojure data. Request version1 accepts
query and finite budgets; it accepts no actor/grant fields or feedback writer.
The host must obtain the snapshot and decisions from an actual trusted storage
and identity adapter. A valid schema or an exported function cannot grant access.
The ESM boundary converts canonical kebab-case JSON and validates the result.

Selection applies fresh revision/event-bound decisions before seed ordering or
the k/fetch cuts. A duplicate authority decision denies its node. A duplicate
currently bound durable node fails storage integrity. Compacted nodes require
their own decision and all transitive members; unknown members and cycles refuse
admission. Edges and force/trail/field influences require admitted endpoints and
every declared provenance node before changing a path or cost. Hidden malformed
content is excluded before content validation, so it cannot induce a failure in
otherwise permitted recall. Denied IDs/text are absent from returned diagnostics.

The pure walker returns stable node and edge paths, reasons, graph/field/policy
revisions, inclusion and visit counts and budgets. Empty, denied, indexing-pending,
budget-exhausted and malformed-input outcomes remain distinct. Retrieval performs
zero I/O and schedules zero reinforcement. Motion, field integration and deliberate
feedback persistence belong to their selected owning adapters and kernel; this
module does not implement them or inherit the legacy REST route's implicit writes.

## Source integration and remaining execution

The isolated implementation uses actual upstream
`07085d6557b75834ce6f50e6c54b8ca47e1c7c08`; freshly fetched personal main remains
`f95a53a4da8ed90588b0be320ba8e86c6a03668d`. The local branch
`codex/sync-openplanner-origin-main` preserves that source history. This source
integration is independently unqualified. The much larger upstream history is
not approved by a review of the scoped delta, and cannot inherit another PR's
approvals. The shared primary checkout and its unrelated lockfile dirt remain.

Before B3 delivery, the owning storage adapter must load bounded, scope-admitted
data using current stored actor/source/session authority before candidate caps,
ranking, traversal, aggregate/field influence and effects. It must verify actual
projection/index readiness and revisions, propagate distinct transport/timeout
failures, and enforce compatibility before reaching the legacy REST writer.
The actual automatic Knoxx turn and provider-session prompt require their own
positive/negative tests and served-revision proof after qualified delivery.

The original RED stdout and counts remain in the committed evidence JSON.
The first registry setup error and later malformed-snapshot decoder failure are
recorded as separate observed attempts, not rewritten into a passing history.
The private npm configuration emits a missing-NPM_TOKEN warning; compiler and
scoped lint warning counts are recorded independently. No credential is emitted.

Process documentation: GPL-3.0-or-later.
