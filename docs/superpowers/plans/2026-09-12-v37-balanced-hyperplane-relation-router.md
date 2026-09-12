# V37 Balanced Hyperplane Relation Router Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Qualify a single-owner balanced hyperplane layout and, only when its exact fourteen-posting ceiling passes, a sparse leaf-to-posting relation router on the frozen one-million-row ReLAION2B cohort.

**Architecture:** A new V37 Rust subsystem trains deterministic quota-balanced hyperplane trees over the authenticated V36 projected-f32 population. It computes an exact unique-owner layout ceiling before evaluating direct best-bin-first routing; a second independent tree and compact relation prefixes are constructed only if the layout is feasible and direct routing fails.

**Tech Stack:** Rust, Rayon, Arrow IPC, Parquet, serde JSON, existing `borsuk-fma` kernels, Python AWS controller, causality EC2 Spot.

**Spec:** `docs/superpowers/specs/2026-09-12-v37-balanced-hyperplane-relation-router-design.md`

## Global Constraints

- Use the frozen one-million-row ReLAION2B cohort A, its registered V36 f32[192] projection, 1,000 development queries, and exact GT@100.
- Construction has no query, truth, validation, holdout, page-body, endpoint, or arbitrary storage capability.
- Stop before relation construction unless the exact unique-owner K=14 ceiling reaches 998,000 ppm aggregate and 800,000 ppm minimum query.
- Never advance a failing cell to page transport, holdout, 10M, or 100M.
- Use Parquet for cross-language tables, Arrow IPC for dense arrays, and sorted compact JSON plus LF for authority and results.
- Add no V36 compatibility reader, alias, migration path, or production default.
- Run narrow gates during TDD; run a full repository gate once only after the stable final diff.

## File Map

- Create `crates/borsuk/src/v37_relation_router.rs`: authority, tree training, codecs, ceiling, direct traversal, relation reduction, and result validation.
- Modify `crates/borsuk/src/lib.rs`: export only the V37 high-level diagnostic boundary required by the example.
- Modify `crates/borsuk/src/v36_prefix_dataset.rs`: expose only the crate-private resident projected-coordinate slice needed by the V37 borrowed trainer.
- Create `crates/borsuk/examples/v37_relation_router.rs`: strict local-file 1M diagnostic CLI with no network/page API.
- Create `scripts/run_v37_relation_router_spot.py`: causality Spot staging, phased capability boundary, pressure monitoring, immutable receipts, and cleanup.
- Create `scripts/test_run_v37_relation_router_spot.py`: launcher mutation, phase-order, stop, and cleanup tests.
- Modify `docs/research/publication-v3-attempt-ledger.md`: evidence only after a terminal scientific result.

### Task 1: Freeze authority and exact arithmetic

**Files:**

- Create: `crates/borsuk/src/v37_relation_router.rs`
- Modify: `crates/borsuk/src/lib.rs`

**Produces:** `V37TreeSpec`, `V37RelationSpec`, `V37ArtifactIdentity`, `V37LayoutDisposition`, checked leaf/quota/RAM/work projections, and canonical manifest bytes.

- [ ] Write REDs named `v37_relation_authority_*` for exact B8192/P123 arithmetic, recursive quotas, allowed seeds and dimensions, artifact roles/algorithms, URI/digest/length bindings, no unknown fields, nonzero lengths, checked overflow, and canonical JSON LF.
- [ ] Run `cargo test -p borsuk --lib v37_relation_authority_ -- --nocapture`; require unresolved V37 types/functions only.
- [ ] Implement the minimal typed authority and checked projections. Make invalid specs unconstructable outside the module.
- [ ] Rerun the identical selector and require every selected test GREEN.
- [ ] Run `cargo fmt --all -- --check`, targeted strict Clippy for `borsuk`, and `git diff --check`.
- [ ] Commit only Task 1 after read-only diff review.

### Task 2: Train the deterministic quota-balanced ownership tree

**Files:**

- Modify: `crates/borsuk/src/v37_relation_router.rs`

**Produces:** `V37BalancedTree`, `train_v37_ownership_tree`, `route_v37_primary_leaf`, and scalar/SIMD node scoring.

- [ ] Write REDs named `v37_relation_tree_*` covering exact uneven quotas, the framed little-endian 4,096-row hash reservoir, preorder node IDs, deterministic two-means initialization/eight rounds/empty repair, source-ordinal ties, boundary score-plus-ordinal replay, query equality behavior, signed zero, nonfinite inputs/intermediates, worker/block/reorder equality, arbitrary dimension tails, and exact single ownership.
- [ ] Add a differential fixture that compares the registered fused backend and scalar diagnostic dot scores for random, tied, subnormal, reversed, and non-lane-multiple dimensions. Require artifact equality only for the same registered backend and mutation-lock backend drift.
- [ ] Run `cargo test -p borsuk --lib v37_relation_tree_ -- --nocapture`; accept missing tree/trainer symbols only.
- [ ] Implement the in-memory reduced-shape trainer with bounded per-node scratch and one parallel score pass. Preserve increasing source ordinal for binary64 reductions.
- [ ] Add Arrow IPC encode/decode with strict field names, nullability, topology, ordering, f32 finiteness, child/leaf cardinality, manifest, exact bytes, and digest mutation coverage.
- [ ] Rerun the focused selector, targeted Clippy, fmt, and diff-check; commit the independently GREEN tree slice.

### Task 3: Prove the layout ceiling before routing

**Files:**

- Modify: `crates/borsuk/src/v37_relation_router.rs`

**Produces:** `V37LayoutCeiling`, `evaluate_v37_unique_owner_ceiling`, and canonical ceiling result bytes.

- [ ] Write REDs named `v37_relation_ceiling_*` that hand-map GT rows to unique postings and independently sum the largest fourteen counts. Cover fewer/more than 100 GT IDs, unknown/duplicate IDs, posting-order ties, fewer than fourteen postings, aggregate/minimum recomputation, and pass/fail precedence.
- [ ] Run the narrow RED; implement only unique-owner ceiling and canonical validation; rerun GREEN.
- [ ] Add canonical `ceiling-passed`/`layout-rejected` receipts; relation-construction admission is tested in Task 7 after the phase boundary exists.
- [ ] Run focused tests, Clippy, fmt, and diff-check; commit.

### Task 4: Evaluate direct best-bin-first routing

**Files:**

- Modify: `crates/borsuk/src/v37_relation_router.rs`

**Produces:** `select_v37_direct_postings` and `V37DirectRoutingResult`.

- [ ] Write REDs named `v37_relation_direct_*` for containing-child priority, max-accumulated normalized squared sibling margin, equality queuing, node/posting ties, exact fourteen unique postings, bounded node visits, backend-bound SIMD behavior, GT-free selection, and separately recomputed containment/ceiling gaps.
- [ ] Run the narrow RED; implement a bounded heap traversal that scores every visited node once and never accesses truth.
- [ ] Rerun GREEN and differential tests; run focused Clippy/fmt/diff and commit.

### Task 5: Build and score independent relation prefixes

**Files:**

- Modify: `crates/borsuk/src/v37_relation_router.rs`

**Produces:** `V37RelationPlane`, `build_v37_relation_plane`, `select_v37_relation_postings`, and prefix Arrow/Parquet codecs.

- [ ] Write REDs named `v37_relation_plane_*` for an independent seed/tree, one quota-membership leaf assignment per row, full pre-truncation counts, `(count desc,posting asc)` order, R16/32/64 exact prefixes, u32 Q24 ties-to-even masses including exact `2^24`, population binding, probe ladder 8/16/32, at most 1,024 visited nodes, checked rank-weight votes, zero-vote primary backfill, short prefixes, candidate shortage, exact fourteen postings, and cell precedence.
- [ ] Include causal fixtures where direct tree routing misses a split-boundary posting but the independent relation succeeds, and where a failed layout ceiling prevents all relation work.
- [ ] Run the narrow RED; implement bounded count reduction and structure-of-arrays Arrow output without per-record serving allocations.
- [ ] Rerun GREEN; validate equivalent outputs across workers, block sizes, row reorder, Parquet round-trip, and scalar/SIMD traversal.
- [ ] Run focused Clippy/fmt/diff and commit.

### Task 6: Add the strict local 1M diagnostic boundary

**Files:**

- Create: `crates/borsuk/examples/v37_relation_router.rs`
- Modify: `crates/borsuk/src/lib.rs`
- Modify: `crates/borsuk/src/v36_prefix_dataset.rs`

**Produces:** `V37LocalRunRequest`, `run_v37_local_request`, and canonical claim-ineligible output.

- [ ] First stage example REDs named `v37_relation_cli_*` for the two fail-fast
      modes `build-ownership` and `evaluate-ceiling`. Build receives only the
      V36 construction authority files, source, V37 construction authority, and
      explicit outputs. Ceiling receives only a separate ceiling authority,
      ownership tree/table, and original V36 GT@100. Reject query/source
      capabilities in the truth phase, GT/query capabilities in construction,
      duplicates, omissions, unknowns, bucket/endpoint/page/D3 flags, and every
      filesystem alias between input and output roles. Add downstream direct
      and relation modes only after the scientific ceiling passes.
- [ ] Run the example RED; implement the smallest parser and high-level call. Keep `main` thin and stdout canonical only.
- [ ] Replace the projected-corpus object field with a versioned SRHT replay
      contract (algorithm, 768-to-192 dimensions, seed 36, frozen replay
      SHA-256); do not confuse semantic replay bytes with an encoded artifact.
- [ ] Change ownership Parquet to format v2 with exact non-null
      `(source_ordinal,feature_row_id,posting_ordinal,posting_local_ordinal)`,
      rejecting the old format, duplicate IDs, gaps, population drift, and
      local-ordinal drift. Add a ceiling authority that cross-binds construction
      authority, tree, ownership, original GT identity, query count, GT@100,
      and K=14.
- [ ] Borrow row slices from the single V36 projected buffer so descriptor
      sorting does not clone coordinates. Bound source decoder working sets and
      reject any source row group above 65,536 rows, 256 MiB compressed, or
      256 MiB uncompressed before decoding. Before Arrow decode, scan compact-
      Thrift footer before metadata construction, capped at 16 MiB with 1 MiB
      binary fields and 65,536-entry collections. Then scan page headers with at
      most 64 KiB scratch per header; reject page claims above 16 MiB, values
      beyond the row-group shape, and dictionary physical bytes beyond the
      declared uncompressed page size. Apply the identical checks to the
      feature-ID-only, ownership, and GT passes, and reject ceiling
      authorities above 1,000 queries before allocating GT vectors. Charge the
      decoder caps plus actual
      descriptor/reservoir/score/assignment/writer/stack scratch;
      release ancestor split scratch before recursion.
- [ ] Add a GT-only scanner that reuses the strict V36 GT Parquet schema and
      maps deliberately nonordinal feature IDs through ownership v2. It must
      reject unknown/duplicate IDs and malformed query/rank/distance ordering
      without opening source or query files.
- [ ] Add coherent reduced-shape local files proving a complete
      build-ownership to evaluate-ceiling flow, canonical bound receipts,
      no-clobber publication, and `layout-rejected` termination before any
      direct/relation file exists.
- [ ] Require the build receipt v2 to bind all six authenticated input
      identities and both generated output identities. Wrap the ceiling in a
      bound result that includes the exact ceiling-authority identity and all
      four prerequisite identities. Stage both construction outputs before
      publication. Never roll back a committed path by name; if the second
      no-clobber commit loses a race, withhold the terminal receipt and leave
      the first file as explicit controller-cleaned scratch.
- [ ] Retain every authenticated input file descriptor and consume only cloned,
      rewound handles thereafter. Mutation-lock parent-directory replacement
      so pathname swaps cannot substitute source, authority, tree, ownership,
      or GT bytes after authentication.
- [ ] Run library and example focused gates, strict workspace Clippy, fmt, and diff-check; commit.

### Task 7: Add Spot orchestration with fast phase stops

**Files:**

- Create: `scripts/run_v37_relation_router_spot.py`
- Create: `scripts/test_run_v37_relation_router_spot.py`

**Produces:** phase-separated construction, query-selection, and truth-evaluation workers with immutable S3 receipts.

- [ ] Stage Python REDs for exact causality profile, Spot-only launch, registered AMI/type/region, IAM profile, explicit subnet, source/binary/input hashes, no prefix listing as authority, one process group, 3 GiB RSS, PSI full 0.75, zero swap growth, 600-second science/720-second wrapper/120-second progress caps, 20,000,000-coordinate/s preflight, terminal preservation, termination, and named scratch cleanup.
- [ ] Make `preflight-training` source-free and output-free. Require the exact
      65,536-row f32[192] generator digest, 50,331,648 production partition
      scores, 15,360 zero-ULP scalar/fused comparisons, and the exact
      2,137,615,120-byte full-build projection in its canonical result.
- [ ] Use one create-only `ATTEMPT_TERMINAL.json` key for both terminal
      dispositions. Before any `build-ownership` EC2 call, authenticate a
      successful preflight terminal plus its bounded exact manifest and result,
      cross-binding source commit/archive, binary, worker count, and V37
      authority to the build manifest. Reject missing or mismatched predecessor
      evidence before launch.
- [ ] Authenticate and phase-bind every manifest before EC2 launch and again
      before guest input downloads. Put Python orchestration and native science
      in one deterministic transient systemd slice with an aggregate 3 GiB and
      zero-swap boundary, monitor that slice, and reject oversized terminal
      objects from metadata before reading their bodies.
- [ ] Require construction phases to reject query/GT inputs, routing phases to reject GT, and truth phases to reject corpus inputs. `build-relations` must authenticate matching `ceiling-passed` and `direct-failed` predecessors bound to the same source, projection, tree, and ownership identities.
- [ ] Run only the affected unittest classes under pinned dependencies; implement the minimal launcher/controller; rerun GREEN.
- [ ] Run Ruff, py_compile, docs validator, and diff-check; commit.

### Task 8: Execute the 1M fail-fast campaign

**Files:**

- Modify only after terminal evidence: `docs/research/publication-v3-attempt-ledger.md`

**Produces:** an authenticated V37 disposition; no larger-scale artifact.

- [ ] Build the exact clean commit once on causality Spot and preserve binary SHA-256/size/receipt.
- [ ] Run one reduced-shape preflight. Stop on numeric drift, insufficient throughput, memory projection, or capability mismatch.
- [ ] Run the 1M ownership/layout ceiling. If it misses 998,000/800,000 ppm, write `layout-rejected`, terminate, and do not execute Tasks 4-5 scientifically.
- [ ] If the ceiling passes, run GT-free direct selection then separate truth evaluation. If direct passes, skip relation construction; otherwise seal `direct-failed`, build relations without query/GT inputs, and run the frozen R16/32/64 by P8/16/32 ladder through separate selection/evaluation phases.
- [ ] Record exact aggregate/minimum recall, per-query distribution, ceiling gap, construction time, query CPU, work counts, RAM/PSI/swap, every artifact digest/URI/length, instance identity, and cleanup.
- [ ] Validate the one-file evidence ledger update and push it fast-forward. Keep transport, holdout, 10M, and 100M fenced unless this task passes.

### Task 9: Final repository assurance

**Files:** all changed paths from Tasks 1-8.

- [ ] Run the complete V37 library selectors and example tests.
- [ ] Run dependency-complete Python discovery once.
- [ ] Run `cargo fmt --all -- --check`, strict locked workspace/all-targets Clippy, then one locked workspace/all-targets test gate as sole pressure-monitored processes.
- [ ] Request independent dual review of the stable diff. For each accepted Critical/Important finding, add a focused RED, make the minimal repair, and rerun only the failing layer before one final affected gate.
- [ ] Verify `HEAD`, `origin/main`, and `ls-remote` equality plus a clean worktree after the final fast-forward push.
