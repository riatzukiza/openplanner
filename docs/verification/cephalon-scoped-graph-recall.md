# Scoped graph selection for the character loop

This is the OpenPlanner selection and scoped storage portion of the accepted
B3 milestone, task `a1e9d6af-0233-4dcb-9677-5c76fa9a2701`. The storage adapter is
tested over held SDK collection ports; live identity/storage qualification and
automatic Knoxx prompt delivery remain unfinished. Passing this package does not complete
B3, the full recall story, physical field persistence or the separate mood model.

## Run the actual library

From `packages/graph/graph-claim-core`:

```sh
clojure -M:recall-test
pnpm exec shadow-cljs --force-spawn compile test
node target/test.cjs
node scripts/verify-recall-test-exit.mjs
pnpm exec shadow-cljs --force-spawn release lib
node scripts/verify-scoped-recall.mjs
```

The Node suite retains every existing edge-claim test. The final demonstration
imports the released ESM export, creates one private temporary JSON fixture,
checks a graph-only neighbor and its complete path, exercises absent decisions,
prints the actual result and deletes only its own fixture. It performs no database,
REST, model, field write or social call. Fixture identity/revisions are explicit
examples; they do not establish current stored authority or deployed state.

The exit verifier forces a real existing assertion to fail by changing only a
function value in an isolated process. It requires the child to report exactly
one assertion failure and exit1. That intentional failing child is a passing
runner-integrity check. Both Node entry points use the actual end-run report;
the return value of `run-tests` is not a result summary.
The verifier separates the ordinary and injected runs at its explicit marker,
requires their exact summaries and equal suite counts, and checks the actual
injected status assertion. It rejects mutated actual outputs reporting11/21
failures, missing/wrong injection, an ordinary failure or an extra summary.
Both runs await their actual asynchronous completion event before starting the
next suite; otherwise shared test counters could overlap and corrupt evidence.

## Owning SDK/Mongo recall boundary

`createScopedMongoRecall(sdk, resolveCurrentAuthority, formatQueryText)` binds a trusted SDK
handle and a host callback. Its returned function accepts only the closed
version1 recall request. The callback supplies the exact current principal,
policy revision, project and admitted event IDs/text; neither tool JSON nor
previous admission receipts can supply a grant. Absence denies before index or
embedding access. It refreshes the complete binding after index loading, query
embedding and edge loading, refusing changed scope or text without stale hits.
These are checked observations across awaits, not atomic grant fencing.

The third argument is the owning SDK's `formatEmbeddingQueryText` export from
`@open-hax/openplanner-sdk/embedding-text`. The trusted host passes this function;
request JSON cannot supply or override it. This retains the SDK's actual query
trim, configured prefix/template and escaped-newline behavior before generating
the vector. Missing, malformed or throwing formatters refuse without sending
text to the provider. `node scripts/verify-sdk-query-format.mjs` exercises the
actual SDK source formatter with held storage/provider ports. It proves provider
input parity, not live semantic ranking or model availability.

The adapter uses existing `graphNodeEmbeddings` and `graphEdges` cursor methods,
with a five-second server query limit and finite row limits. It selects the
same `graph-event` / `graph.node` model scope as the SDK indexer. Index rows must
bind `node_id` to `source_event_id` in Mongo **before** the row limit; edges must
have nonempty admitted provenance in Mongo before their limit. Excluded rows
cannot consume the admitted storage budget. Missing admitted indices return
`indexing-pending`; overflowing admitted rows fail without partial results.
Held cursor fixtures test these explicit predicates; they do not establish
compatibility with a live Mongo deployment.

Validated cosine ranking chooses one strongest semantic seed; the existing
pure walker selects associative neighbors with their exact node/edge paths.
`validScopedMongoRecallResult(result)` lets a consumer validate the owning wire
contract, including failure, feedback and path shapes, without copying those
schemas. Storage/provider exceptions return bounded safe codes, including null
or undefined rejections. Retrieval makes no database write and requests no
reinforcement. `field-status: not-loaded` explicitly records the absent physical
field; graph snapshot hashes are not field persistence proof.

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
Duplicate storage detection runs after the complete compact/member admission;
copies of denied compact rows cannot veto permitted recall. Public selection
checks each retained compact row's members even if another row admits its ID;
two independently admitted duplicates still fail storage integrity. Public
selection states depend on admitted data and the safe denial count, never excluded seed
flags. Binary64 influence reduction uses stable influence-ID order before cost
and budget decisions; permutations of the same admitted facts give the same result.

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
# Hosted source verification and review route

The scoped source workflow runs the portable JVM laws, recall namespace lint,
actual compiled Node suites and isolated assertion-failure proof, released ESM,
and the runnable demonstration. Its compiler steps require the actual completed
zero-warning summary. The existing broader Code Quality and Review Resolution
jobs retain their obligations. Local evidence does not establish hosted success.

The evidence review workflow uses the exact shared receiver
`45ec644c2d15ed511e9bc1e797d1b4073b63dbfc` and canonical skill source
`4b4f48dae1b804b2581974e604cca618ecedadf1`. Its own deterministic gates are
diff statistics and hygiene; the separate scoped verification workflow supplies
functional execution. The existing operator App and personal installation are
reused through named encrypted GitHub secrets. No credential contents appear in
source or evidence, and configured secrets do not prove a completed review.

The frozen upstream source `07085d6557b75834ce6f50e6c54b8ca47e1c7c08` is a
separate personal synchronization layer. Review of this bounded recall diff
cannot qualify that larger synchronization diff. Both layers require their own
current native review, check and merge evidence before release or consumption.
