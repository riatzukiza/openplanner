---
uuid: "openplanner-workflow-dsl-investigation"
title: "Spike: investigate portable EDN workflow declarations"
status: incoming
priority: P2
labels: ["spike", "workflow", "edn", "clojure", "architecture"]
created_at: "2026-11-01T15:52:00Z"
source: "user-review:openplanner-pr-4-workflow-follow-up"
category: "architecture"
---
# Spike: investigate portable EDN workflow declarations

**Parent:** R-OP-1 → E-OP-2 → S2.5 in the [living migration roadmap](../docs/architecture/typescript-to-cljs-monorepo-roadmap.md#s25--workflow-declaration-spike-and-provisional-follow-up).
**Status:** incoming; investigation not performed. This card proposes no implementation or change to existing workflows.

## Story and question

As a maintainer, I want to investigate the existing EDN workflow-description work and its applicability to OpenPlanner, so that a future workflow migration can reuse shared language tooling and preserve CI behavior rather than creating another DSL or hand-maintained translation.

Can existing declarations and compiler patterns express OpenPlanner's workflow intent and produce equivalent GitHub Actions configuration, while remaining suitable for other targets? The review identified possible starting points in `open-hax/eta-mu`, `open-hax/shx`, and `octave-commons/muse`; their current content, ownership, accessibility, maturity, and suitability are **not established** by this card. Inspect only material the investigator is authorized to access; avoid exposing private material in public artifacts.

**Hypothesis to test, not a decision:** an existing shared EDN-based representation may express common workflow intent with target-specific adapters, reproducible generation, and explicit escape hatches. The destination is to replace handwritten workflow YAML over time; findings may require extending existing tooling, a staged exception, or a revised route without silently abandoning that destination.

## Investigation and evidence

- Inventory the repository's active workflow YAML, triggers, jobs, dependencies, permissions, secrets references, environments, caches, artifacts, and externally observable status checks; identify owners and critical gates.
- Inspect relevant existing language/tooling candidates and document source revision, ownership, license/access, actual executable capabilities, supported targets and limitations. Compare against a no-new-DSL option; do not assume that similar names mean equivalent implementations.
- Pick representative synthetic, non-deploying workflows—including the nbb parity and Shadow reference/candidate checks—and compare intent, generated configuration, permissions, triggers, dependency order, failure and cancellation semantics, and check names. Prototype only in an isolated branch or fixture; no live secrets, production runs, or deployment changes.
- Record alternatives, risks, maintenance ownership, reproducibility, review ergonomics, rollout/rollback, evidence and unresolved questions. Preserve original workflows until equivalence is demonstrated and change is separately reviewed and authorized.

## Exit tests and deliverables

- [ ] Produce a revision-pinned inventory and decision record, including evidence for or against the hypothesis and any inaccessible sources.
- [ ] Demonstrate reproducible generation on representative fixtures, or document a falsifying result and an alternative. Verify generated output's triggers, job ordering, least-privilege permissions, failure behavior and check names against the reference; syntax validation alone is insufficient.
- [ ] Describe how equivalence would be tested in isolated CI without credentials or deployment, including negative controls and rollback; identify any behavior that cannot be preserved.
- [ ] Revise, split, reorder or discard the provisional follow-up below using spike evidence, with explicit parent links, dependencies, acceptance criteria and ownership. No estimated migration scope is treated as settled before this review.

## Provisional follow-up epic and stories — revise after spike

**Draft epic:** As a maintainer, I want workflow intent expressed through appropriate shared declarations and compiled to supported targets, so that the workflow estate can be maintained consistently without losing provider-specific guarantees. Parent: R-OP-1 → E-OP-2 / S2.5. The intended destination is to replace handwritten workflow YAML across the estate over time. The spike must test feasibility and revise the boundaries, sequence and stories; it does not authorize changing production CI.

- **Draft story A — declarations and ownership:** As a workflow author, I want an agreed source of workflow intent and clear ownership so that changes have one reviewable source. Acceptance and tool choice are to be revised from spike evidence.
- **Draft story B — reference-backed generation:** As a maintainer, I want reproducible target generation and behavior comparison so that generated configuration preserves existing workflows. Revise test scope, target support, failure cases and acceptance after the spike.
- **Draft story C — staged migration:** As an operator, I want an independently reviewed, reversible rollout so that workflow changes preserve security and required checks. Revise migration boundaries, dependencies and rollout/rollback criteria after the spike; no deployment or live credential changes are implied.
