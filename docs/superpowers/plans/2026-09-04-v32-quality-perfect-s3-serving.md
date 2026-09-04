# V32 Quality-Perfect S3 Serving Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship the proven 16-page, perfect-recall PQ route with bounded S3-native storage and a measured low-latency serving tier.

**Architecture:** Replace the rejected page-centroid production path with the authenticated root/routing-microleaf/PQ candidate router. Keep compact Arrow/Parquet metadata resident, fetch exactly 16 Arrow pages concurrently from Standard S3 or a byte-identical same-AZ S3 Express replica, and exact-rerank only those pages.

**Tech Stack:** Rust, Tokio, object_store/AWS S3, Arrow IPC, Parquet, Python 3.12 evidence controllers, Causality EC2 Spot.

**Spec:** `docs/superpowers/specs/2026-09-04-v32-quality-perfect-s3-serving-design.md`

## Global Constraints

- Pre-release schema replacement only: no V30/V31 compatibility reader, alias, or fallback.
- Exactly 16 selected pages, each at most 196,608 encoded bytes and therefore
  at most 3,145,728 fetched bytes in aggregate, 12,288 retained candidates,
  and a frozen scan budget from the 65,536/131,072/262,144 ladder.
- Page and metadata artifacts remain cross-language Arrow IPC or Parquet; canonical JSON binds identities.
- Standard and Express tiers contain byte-identical page objects; one request selects exactly one tier.
- No local corpus, D3 capability, latest-object discovery, or 100M work before the earlier gates pass.

---

### Task 1: Restore the quality-perfect PQ route

**Files:**
- Modify: `crates/borsuk/src/v30_s3_search.rs`
- Modify: `crates/borsuk/src/lib.rs`
- Modify: `crates/borsuk/examples/v30_s3_qualify.rs`
- Modify: `scripts/run_v30_untouched_quality.py`
- Test: existing unit modules in those files

**Interfaces:**
- Produces: `V32SearchArm { root_beam, leaf_beam, scan_budget,
  candidate_depth, page_count }` and `V32Router::select_pages` using bounded
  row-PQ candidates.

- [ ] Add a production-path test whose PQ ranking and page centroids disagree; require the PQ-selected 16-page behavior and reject the centroid choice.
- [ ] Run `cargo test -p borsuk --lib v32_s3_search_ -- --nocapture`; require the intended missing/rejected V32 production boundary RED.
- [ ] Rename the coherent router surface to V32, restore root/leaf/PQ selection
  with the 1M 8/64/65,536/12,288/16 authority, and remove page-centroid
  selection from production.
- [ ] Update the qualifier and independent Python reducer to require nonzero bounded code/candidate work and exact 16-page quality evidence.
- [ ] Rerun the focused Rust and Python selectors; require GREEN with no warnings.
- [ ] Commit the verified routing slice.

### Task 2: Replace the experimental manifest and tier authority

**Files:**
- Modify: `crates/borsuk/src/v30_s3_layout.rs`
- Modify: `crates/borsuk/examples/v30_s3_build.rs`
- Modify: `crates/borsuk/examples/v30_s3_qualify.rs`
- Modify: `scripts/test_run_v32_s3_latency_preflight.py`
- Modify: `scripts/run_v32_s3_latency_preflight.py`

**Interfaces:**
- Produces: schema-v3 manifest plus strict Parquet page-location table and `V32ServingTier::{Standard, Express}`.

- [ ] Add RED mutations for missing/extra/type/order/digest/length/URI/tier fields, cross-tier byte drift, and forbidden implicit fallback.
- [ ] Run only the new manifest/tier selectors and preserve the intended RED.
- [ ] Implement one schema-v3 writer/reader, delete superseded page-centroid
  routing fields and old schema dispatch, and bind byte-identical
  Standard/Express locations. The final schema's routing-microleaf centroid is
  introduced only by Task 4A.
- [ ] Store page digests as fixed-size-binary 32-byte values plus integer
  ordinal/length/row fields. Bind Standard/Express URI prefixes once in the
  manifest; reject per-page URI strings so the 100M resident projection does
  not hide heap-string overhead.
- [ ] Add a pure latency preflight that cross-binds the observed compute/quality
  fields to one registered terminal identity. Profiles carry a measured
  max-of-16 concurrent-GET wave p99, at least 16 parallel slots, and aggregate
  throughput; require Standard-144ms rejection and keep any injected Express
  pass claim-ineligible until a same-AZ wave is measured.
- [ ] Run focused Rust/Python GREEN, Ruff, pycompile, fmt, and diff-check.
- [ ] Commit the verified authority slice.

### Task 3: Remove serving copies and reuse asynchronous resources

**Files:**
- Modify: `crates/borsuk/src/v30_s3_search.rs`
- Modify: `crates/borsuk/examples/v30_s3_qualify.rs`

**Interfaces:**
- Produces: `V32PageStore::read_wave -> Vec<bytes::Bytes>` and one persistent multithreaded Tokio runtime/client.

- [ ] Add RED tests proving response buffers are not copied, one runtime/client serves multiple queries, all 16 reads start before any result is consumed, and cancellation/error cardinality fails closed.
- [ ] Run the narrow store/batch selectors and preserve RED.
- [ ] Change the store boundary to `Bytes`, remove `.to_vec()`, construct one multithread runtime, and retain deterministic output ordering.
- [ ] Add local delayed-store tests for concurrent-wave wall time and 32-query resource reuse.
- [ ] Run focused GREEN, fmt, strict targeted Clippy, and the affected qualifier tests.
- [ ] Commit the verified execution slice.

### Task 4: Fail fast on 1M containment without page reads

**Files:**
- Modify: `crates/borsuk/src/v30_s3_layout.rs`
- Modify: `crates/borsuk/examples/v30_s3_build.rs`
- Modify: `crates/borsuk/examples/v30_s3_qualify.rs`
- Modify: `scripts/run_v32_no_page_containment.py`
- Modify: `scripts/run_v30_s3_campaign.py`
- Test: corresponding Rust example and Python unittest modules

**Interfaces:**
- Produces: authenticated logical-to-source Arrow mapping and canonical page-free containment evidence.

- [ ] Emit `logical-sources.arrow` from construction in logical order using bounded batches; bind its filename, role, length, and SHA-256 in the strict manifest without loading it in normal serving.
- [ ] Freeze the 1M geometry at 128 roots, 4,096 leaves, 32,768 training rows,
  and 480-row pages.
- [ ] In a separate query-enabled, page-blind Spot phase, stream the six authenticated prefix shards one at a time and freeze exact deterministic top-10 Parquet truth for the 32 development queries; do not reuse the incompatible 9.99M neighbor table.
- [ ] Run 32 development queries against ten exact truth IDs each through the
  no-page diagnostic. Require 320/320 containment, exactly 16 selected pages,
  at most 3,145,728 selected-page bytes, the smallest CPU-qualifying frozen
  scan-ladder budget, at most 1,024 rows in any routing microleaf, and zero page
  reads.
- [ ] Use one `causality` Spot worker with the original-session monitor and immediate termination. Stop all later latency/corpus work on failure.
- [ ] Record the claim-ineligible terminal and commit the evidence.

### Task 4A: Bound routing leaves before repeating the 1M gate

This corrective task replaces the leaf-layout portion of Task 2 and supersedes
the failed Task 4 construction artifacts. Only the post-4A schema and rebuilt
logical-source/page evidence are citable; no intermediate schema is retained.

**Files:**
- Modify: `crates/borsuk/src/v30_s3_layout.rs`
- Modify: `crates/borsuk/src/v30_s3_search.rs`
- Modify: `crates/borsuk/examples/v30_s3_build.rs`
- Modify: strict manifest fixtures in the corresponding Rust/Python test modules

**Interfaces:**
- Produces: strict routing-microleaf Arrow rows containing
  `(routing_leaf_ordinal, code_parent_leaf_ordinal, routing_centroid,
  logical_start, row_count, page_start, page_count)`.
- Consumes: unchanged trained root/leaf hierarchy and unchanged 24/48-byte PQ
  code planes whose residuals remain relative to `code_parent_leaf_ordinal`.

- [ ] Add a synthetic RED in `v30_s3_layout.rs` with more than 1,024 rows
  assigned to one trained leaf. Require exactly `ceil(rows/1,024)` nonempty
  deterministic routing microleaves, no microleaf above 1,024 rows, pages at
  most 480 rows and 196,608 encoded bytes, unique complete source coverage,
  and explicit parent binding.
- [ ] Add REDs proving empty trained parents emit no routing row, split and
  unsplit routing centroids are complete-population raw means in one
  squared-L2 geometry, the count-only
  preflight's checked arithmetic rejects 131,073 rows without allocating that
  corpus, and a reduced-shape equivalent is byte-identical under worker counts
  1 and 8.
- [ ] Run only `cargo test -p borsuk --lib v32_routing_microleaf_ -- --nocapture`;
  require failure at the missing microleaf format/partition boundary.
- [ ] Extend the layout range and its strict Arrow codec with
  `code_parent_leaf_ordinal` and non-null fixed-size-list `float16[96]`
  `routing_centroid`. Reject old/extra/nullable fields, nonfinite or zero
  centroids, parent ordinals outside the trained hierarchy, row counts above
  1,024, and zero-row ranges. Do not impose a per-root population cap; report
  fan-out and enforce the global resident/work equations.
- [ ] Generalize the existing deterministic geometric partitioner to a
  1,024-row routing cap. Compute float64-accumulated complete-population raw
  means for every routing row, split or unsplit, and cast them through float32
  to float16. Preserve source-ordinal tie order and map nonempty routing
  ordinals monotonically in trained-leaf order. Add a count-only corpus
  assignment preflight and reject above 131,072
  rows before PQ encoding; raise the obsolete 65,536-row and trained-parent
  page-count limits for the actual build.
- [ ] Run the same focused selector for GREEN, then add mutation RED/GREEN cases
  for centroid/parent/schema drift, duplicate-heavy input, 1,024/1,025
  boundaries, deterministic rebuilds, and worker counts 1 versus 8.
- [ ] Add search REDs proving root filtering and ranking use routing centroids,
  while PQ query tables use the exact stored float16 trained code-parent
  centroid used by construction. Pair leaf beams 64/128/256 with scan budgets
  65,536/131,072/262,144; extend only when the selected population is below
  12,288 and reject if complete ranges cross the paired budget. Cover sparse
  tails and f16 centroid ties.
- [ ] Mutation-lock all router namespace couplings: constructor cardinality and
  arm bounds use routing rows; root membership is derived through
  `code_parent_leaf_ordinal -> leaf_roots`; selected leaves, page ranges, and
  miss classification use routing ordinals; PQ residual lookup alone uses the
  trained parent ordinal.
- [ ] Mutation-lock that the decoded non-null float16 trained-parent centroid is
  byte-identical at construction residual encoding and query-table creation;
  reject f32 substitution or centroid recomputation.
- [ ] Change page construction to pack the globally ordered logical stream
  across microleaf boundaries into at most 480 rows per page, and require every
  encoded Arrow page to be at most 196,608 bytes before sink/upload. Remove the
  single-leaf field from page ranges; map diagnostic logical ordinals through
  routing ranges. Add a 16-page maximum-body test proving the aggregate byte
  bound and retain exact byte validation again after each read.
- [ ] Require the reducer to sum the 16 authenticated page lengths and reject
  the aggregate before calling `read_wave`; add a store observer proving an
  oversized selection performs zero object reads.
- [ ] Add a page-free CPU preflight over scan budgets 65,536, 131,072, and
  262,144. Remove arms that cannot fit the complete 64 ms CPU gate; on the 1M
  development truth choose the smallest surviving 320/320 arm and freeze it for
  both disjoint scale cohorts.
- [ ] Before the 1M envelope decision, build the authenticated 100K development
  cohort with frozen 128-root/4,096-trained-leaf/root-beam-8 geometry and run
  the same page-free 32-query truth containment diagnostic. When that complete
  root frontier contains fewer than 12,288 rows, retain exactly
  `min(12,288, codes_scanned)` candidates for this rank-evidence-only leg;
  preserve 16 pages, 320/320 containment, and zero page reads. From its newly
  produced truth-microleaf rank maximum and the 1M maximum, compute the exact
  two-point power envelope in the spec and round each
  scale projection upward to leaf beam 64/128/256. Reject before a disjoint
  cohort when the projection exceeds 256 or the paired arm failed CPU; never
  refit on the 9.99M or 100M results.
- [ ] Add exact diagnostic stages and margins for root frontier, routing-leaf
  frontier, candidate retention (including the resident truth-code score versus
  rank 12,288), and page reduction.
- [ ] Change `V32Router` construction and routing minimally to consume the new
  layout authority. Run focused layout/search GREEN, fmt, strict targeted
  Clippy, and `git diff --check`; commit the self-contained format/search slice.
- [ ] Re-run one query-blind 1M construction on `causality` Spot. Require
  `maximum_routing_leaf_rows <= 1,024`,
  every trained-parent population at most 131,072 rows, every encoded page at
  most 196,608 bytes, a resident projection below 3 GiB, clean
  terminal/PID/instance termination, and no query/truth inputs. Report the
  page-count change; do not apply an underived percentage threshold.
  Stop immediately on failure.
- [ ] Only after that structural GREEN, run the already-authenticated six-shard
  streaming truth builder and the page-free 32-query containment diagnostic.
  Require 320/320, exactly 16 selected pages, at most 3,145,728 page bytes, at
  most the chosen frozen scan budget, and zero page-body reads before any
  latency run.

### Task 4B: Close the compute budget before provisioning Express

**Files:**
- Modify: `crates/borsuk/src/v30_s3_search.rs`
- Modify: `crates/borsuk/src/v30_s3_pq.rs`
- Create: `crates/borsuk/examples/v32_cpu_preflight.rs`
- Modify: the focused Rust test modules in those files

**Interfaces:**
- Produces: allocation-free block PQ scoring, cached per-parent query tables,
  parallel deterministic page reranking, and a canonical in-memory CPU receipt.
- Produces: a doc-hidden `run_v32_cpu_preflight` boundary used only by the thin
  example. Its exact shape is 1,024 roots, 65,536 trained parents, 163,192
  routing microleaves, 208,334 page identities, one deterministic scan slice,
  and sixteen authenticated 480-row Arrow bodies with disjoint source ordinals;
  it has no object-store or corpus input.

- [ ] Add scalar differential REDs for root/microleaf distance, both PQ widths,
  f16 ties, subnormals, reversed blocks, and per-page exact top-ten merge.
- [ ] Add REDs for exact 100M cardinalities, scan slices 65,536/131,072/262,144,
  five-percent high-width codes, distinct-parent table construction, bounded
  12,288 candidates with the real 45,056-entry prune buffer, 16-page reduction,
  sixteen disjoint 480-row Arrow bodies, and a canonical claim-ineligible
  receipt containing raw outer-elapsed/process-CPU/stage samples, explicit
  unattributed time, and a deterministic query-cohort digest.
- [ ] Implement the preflight using the production root filter, centroid/PQ
  scoring, bounded candidate reducer, page reducer, Arrow validator, and exact
  top-ten merge. Materialize no 100M-row code plane: allocate only the exact
  scan slice and sixteen immutable authenticated page bodies across the 16
  decode/rerank inputs. Treat this cache-local slice as rejection-only; only the
  later authenticated scale leg supplies transferable CPU evidence. A
  reduced-shape unit fixture must prove identical work accounting without
  timing assertions.
- [ ] Cache one query table per distinct `code_parent_leaf_ordinal`, scan
  contiguous code blocks without per-block allocation, and use the repository's
  runtime-detected fused SIMD backend with a scalar oracle.
- [ ] Exact-rerank the 16 already-authenticated decoded pages in parallel,
  producing one deterministic top ten per page and a stable `(distance,
  source_ordinal)` final merge. Do not parallelize S3 authority checks.
- [ ] Run arm 64 first in a pinned process. A 128-sample probe may reject only
  when every total CPU sample exceeds 64 ms; otherwise run 1,024 warmups and
  exactly 10,000 raw observations. Continue serially through arms 128 and 256
  only while the preceding arm stays within the gates. Persist canonical raw-ns
  evidence and require routing plus decode/rerank no-load p99 at most 12 ms and
  total process CPU p99 at most 64 ms. Then run the identical path at fixed
  1,000-query/s offered load with 64 concurrent clients, including queueing and
  any bounded batching wait in latency; require achieved throughput at least
  1,000 queries/s and compute p99 at most 12 ms.
- [ ] Stop before Express when no scan arm satisfies compute and 320/320 1M
  containment. Run focused GREEN, fmt, targeted strict Clippy, and commit the
  compute slice when both gates pass.

### Task 5: Measure Express, qualify 100K, then scale conditionally

**Files:**
- Modify: `scripts/run_v30_s3_campaign.py`
- Modify: `scripts/test_run_v30_s3_campaign.py`
- Create: `scripts/run_v32_s3_express_preflight.py`
- Create: `scripts/test_run_v32_s3_express_preflight.py`
- Modify after each terminal: `docs/research/publication-v3-attempt-ledger.md`

**Interfaces:**
- Consumes: V32 manifest, exact Standard/Express page identities, and the passing latency profile.
- Produces: authenticated quality, latency, throughput, memory, and cleanup evidence.

- [ ] TDD a 16-object same-AZ S3 Express microbenchmark; require a measured full-latency projection at most 15 ms before an end-to-end cell.
- [ ] Add campaign REDs for fixed candidate/page counts 12,288/16, the frozen
  scale-specific root beam and development-chosen scan budget, tier/AZ binding,
  quality gates, latency gates, and immediate Spot termination.
- [ ] Implement the minimal V32 campaign boundary and make the focused controller tests GREEN.
- [ ] Run one 100K Express-backed end-to-end cell in both no-load and fixed
  1,000-query/s, 64-client modes; require 320/320, 32/32 perfect, p99 at most
  15 ms including queueing, achieved throughput at least 1,000 queries/s,
  process CPU p99 at most 64 ms, and RSS at most 3 GiB.
- [ ] If and only if 100K passes, run the disjoint 9.99M cohort with the same frozen source and gates.
- [ ] Before release qualification, preregister a sealed 1,000-query cohort.
  In a separate query-enabled, page-blind `causality` Spot phase with no
  construction/router/layout inputs, stream the authenticated 100M source
  shards and compute exact float32 squared-L2 top ten with deterministic
  `(distance, source_ordinal)` ties. Persist only the canonical 10,000-row
  Parquet truth artifact and a receipt binding cohort, shards, row count,
  algorithm, worker topology, timings, cost, and digest. Require all 1,000
  held-out queries at serving qualification; never promote from the 32
  development queries.
- [ ] If and only if 9.99M passes, run 100M Spot construction and serving qualification, then one full repository assurance gate.
- [ ] Commit every terminal ledger entry and freeze production defaults only after all gates pass.
