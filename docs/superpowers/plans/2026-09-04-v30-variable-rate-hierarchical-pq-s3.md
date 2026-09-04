# V30 Variable-Rate Hierarchical PQ S3 Index Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reproduce the archived variable-rate result, then replace the experimental V28 format with the smallest authenticated hierarchical PQ index that exact-reranks ten immutable S3 Arrow pages.

**Architecture:** First reproduce the archived 24-byte/48-byte PQ8 replacement arm on the frozen 100K fixture because the historical evaluator was not preserved. If it matches, retain V28's query-independent hierarchy and primary-only page ownership, persist one compact code per row selected by an exact five-percent fidelity bitmap, fetch ten pages once, and exact-rerank their f32 vectors.

**Tech Stack:** Rust 2024, Rayon, `borsuk-fma`, Apache Arrow IPC, Parquet, SHA-256, Python 3.12 controllers, AWS S3/EC2 Spot profile `causality`.

**Spec:** `docs/superpowers/specs/2026-09-04-v30-variable-rate-hierarchical-pq-s3-design.md`

## Global Constraints

- V30 is a clean pre-release format; no V28/V29 compatibility reader, alias, migration, or dual writer.
- Exact vectors and IDs remain only in immutable S3 Arrow pages; serving never stages the full corpus.
- Persistent tables use Parquet, typed serving artifacts use Arrow IPC, and authority/result objects use sorted compact JSON plus LF.
- No production codec is selected until Task 0 reproduces the archived result; only the winning fixed interpretation may proceed.
- The reproduction candidate uses 24-by-4D and 48-by-2D PQ8 codebooks with 256 centroids; exactly 50,000 ppm of rows replace 24 base bytes with 48 high-fidelity bytes.
- Hierarchy scale is fixed by row count: 16/256 at 100K, 1,024/32,768 at 9.99M, and 1,024/65,536 at 100M; pages contain at most 512 rows.
- Query work is at most 1,000,000 codes, 12,288 retained candidates, ten pages, one read wave, and 4,587,520 page bytes.
- The provisional replacement 100-million-row resident projection must equal 2,630,588,896 bytes and runtime peak RSS must stay below 3,221,225,472 bytes.
- Construction has no query, truth, prior-result, page-read, or D3 capability.
- Run focused selectors and the 100K gate while iterating; run strict Clippy and the full workspace suite once at the release checkpoint.

---

### Task 0: Reproduce and freeze the variable-rate mechanism

**Files:**
- Create: `scripts/run_v30_variable_rate_reproduction.py`
- Create: `scripts/test_run_v30_variable_rate_reproduction.py`
- Modify after terminal evidence: `docs/research/publication-v3-attempt-ledger.md`

**Interfaces:**
- Consumes: exact registered 100K construction Arrow pages, query/truth Parquet, source identity, and an output prefix.
- Produces: canonical `V30ReproductionResult` plus Parquet per-query evidence for fixed 0/5/10/20-percent PQ8 replacement arms.

- [ ] **Step 1: Write authority REDs.** Require exact URI/SHA-256/length identities, 100,000 unique rows, 32 fixed queries with 320 truth memberships, 16 roots, 256 leaves, 512-row pages, no query/truth capability during training, and no complete-corpus persistence outside the disposable worker.
- [ ] **Step 2: Write interpretation REDs.** Fix 24-by-4D/256-centroid base PQ8 and 48-by-2D/256-centroid replacement PQ8. Require the same deterministic training sample, hierarchy, base-code page order, base-error selection, leaf beam 64, candidate depth 12,288, and ten-page reducer for 0/5/10/20-percent arms.
- [ ] **Step 3: Write result REDs.** Independently recompute hits, aggregate/minimum/perfect counts, work, bytes, and memory components from Parquet evidence. Require claim-ineligible canonical JSON and fail closed if no arm reaches 319/320, 900,000 minimum, and 31/32 perfect.
- [ ] **Step 4: Run the focused RED.** Run `python3 -m unittest scripts.test_run_v30_variable_rate_reproduction`; accept missing controller/evaluator symbols only, then implement the minimum deterministic evaluator.
- [ ] **Step 5: Run the bounded Spot reproduction once.** Use `causality` Spot, stream only the 46.8-MB frozen pages plus small query/truth objects, upload result/evidence/terminal, and terminate. Do not tune from per-query misses.
- [ ] **Step 6: Freeze or reject.** If the five-percent arm reproduces the boundary and is the smallest passing arm, freeze its authenticated receipt and commit the evaluator/evidence. Otherwise reject V30 and do not implement Tasks 1-6.

### Task 1: Add variable-rate residual code authority

**Files:**
- Create: `crates/borsuk/src/v30_s3_pq.rs`
- Modify: `crates/borsuk/src/lib.rs`

**Interfaces:**
- Consumes: normalized `[f32; 96]` leaf residuals and a new fixed-width
  256-centroid ADC backend whose scalar and optimized paths share exact lookup
  tables and reduction order.
- Produces after Task 0 passes: `V30PqCodebooks`, `V30BaseBlock`, `V30HighBlock`, `V30Fidelity`, `fit_v30_codebooks`, `encode_v30_codes`, `score_v30_leaf`, `encode_v30_pq_artifacts`, and `decode_v30_pq_artifacts`.

- [ ] **Step 1: Write the codec REDs.** Add `v30_s3_pq_` unit tests requiring 24 four-dimensional and 48 two-dimensional subquantizers with 256 centroids, 24/48-byte rows, deterministic centroid/source ties, exact-zero residual support, non-finite rejection, and exact five-percent selection by reversed base reconstruction error then source ordinal. Require each logical position to map to exactly one compact plane.
- [ ] **Step 2: Lock scoring and memory arithmetic in RED.** Differential-test scalar and optimized 256-entry-table scoring for both widths in the same f32 domain. Require `project_v30_resident_bytes(100_000_000, 50_000) == 2_630_588_896` with literal component checks; reject per-leaf plane padding, every other fraction/width, overflow, or zero rows.
- [ ] **Step 3: Lock Arrow authority in RED.** Require exact schemas for both codebooks, base blocks, fidelity bitmap/rank offsets, and high blocks; mutate role, digest, length, row count, width, nullability, field names, offsets, padding, fidelity cardinality, and dependency bindings.
- [ ] **Step 4: Run the focused RED.** Run `cargo test -p borsuk --lib v30_s3_pq_ -- --nocapture`; require only unresolved V30 symbols and at least six selected tests.
- [ ] **Step 5: Implement the minimal codec.** Implement deterministic 256-centroid training for both widths, globally pack mutually exclusive planes, and use bounded 256-entry query tables. Store no zero-filled placeholders and expose no compatibility dispatch.
- [ ] **Step 6: Run GREEN and commit.** Run the identical selector, `cargo fmt --all -- --check`, and `git diff --check`; commit only `v30_s3_pq.rs` and `lib.rs`.

### Task 2: Build one-owner variable-rate pages with bounded external selection

**Files:**
- Create: `crates/borsuk/src/v30_s3_layout.rs`
- Modify: `crates/borsuk/src/lib.rs`

**Interfaces:**
- Consumes: `V27Hierarchy`, `V30PqCodebooks`, the strict V27 Arrow page codec, and an ordered normalized corpus stream.
- Produces: `V30LayoutBuilder`, `V30LeafRange`, `V30PageRange`, `V30Layout`, `encode_v30_layout_artifacts`, and `decode_v30_layout_artifacts`.

- [ ] **Step 1: Write the construction REDs.** Require one primary owner per source ordinal, complete source union, leaf-residual encoding, exact five-percent high-error selection, deterministic cutoff ties, and merge order `(leaf,base_code,refined_desc,refinement_code,source)`.
- [ ] **Step 2: Prove bounded construction in RED.** Feed more rows than the configured memory limit, require sorted spill runs, bound resident records and merge heads, forbid a corpus-sized vector/code/error collection, and require scratch cleanup after success and injected failure.
- [ ] **Step 3: Lock page/offset authority in RED.** Require pages of at most 512 rows; monotone base/high block, fidelity-rank, leaf, and page offsets; code-position-to-page equality at every first/last boundary; and exact Arrow/Parquet schema/digest/length bindings.
- [ ] **Step 4: Run the focused RED.** Run `cargo test -p borsuk --lib v30_s3_layout_ -- --nocapture`; require missing V30 layout symbols only.
- [ ] **Step 5: Implement the bounded builder.** Use fixed records and external runs, a bounded error-selection pass, and one deterministic merge that emits code artifacts and page bodies in the same order. Do not retain or emit a row-to-page array.
- [ ] **Step 6: Run GREEN and commit.** Run the identical selector, formatting, and diff-check; commit only `v30_s3_layout.rs` and `lib.rs`.

### Task 3: Route and score the mixed-fidelity leaf frontier

**Files:**
- Create: `crates/borsuk/src/v30_s3_search.rs`
- Modify: `crates/borsuk/src/lib.rs`

**Interfaces:**
- Consumes: authenticated V27 hierarchy, V30 codebooks/layout, and `V30SearchArm`.
- Produces: `V30Router::select_pages`, `V30PageSelection`, `V30RoutingWork`, `V30PageStore`, `V30Index::search`, and exact `V30SearchResult`.

- [ ] **Step 1: Write the routing REDs.** Require normalized-query validation, deterministic root/leaf ties, base/refined scoring in one f32 domain, candidate ties by immutable logical code position, and ten unique pages by page identity. Assert that source IDs become available only after Arrow page decode.
- [ ] **Step 2: Lock boundedness in RED.** Reject more than 1,000,000 scanned base codes, more than 12,288 retained candidates, more than ten pages, or corpus-sized score allocation; use observers to prove unselected leaf and refinement ranges are untouched.
- [ ] **Step 3: Write the fetch/rerank REDs.** Require one `read_wave` call, ten authenticated page identities, at most 4,587,520 encoded bytes, complete-byte authentication before Arrow decode, all-or-nothing errors, and exact f32 `(distance,source)` top ten.
- [ ] **Step 4: Run the focused RED.** Run `cargo test -p borsuk --lib v30_s3_search_ -- --nocapture`; require unresolved router/index symbols only.
- [ ] **Step 5: Implement router and index.** Reuse V28 hierarchy traversal and bounded heap patterns, dispatch per fidelity bit/rank, retain no unbounded intermediate, then call the store once and release decoded page bodies after rerank.
- [ ] **Step 6: Run GREEN and commit.** Run the identical selector, formatting, and diff-check; commit only `v30_s3_search.rs` and `lib.rs`.

### Task 4: Add the fast 100K quality and simulated-S3 gate

**Files:**
- Create: `crates/borsuk/examples/v30_s3_qualify.rs`
- Create: `scripts/run_v30_reduced_quality.py`
- Create: `scripts/test_run_v30_reduced_quality.py`
- Modify: `scripts/check_v26_fast.py`
- Modify: `scripts/test_check_v26_fast.py`

**Interfaces:**
- Consumes: explicit local Arrow/Parquet authority paths, an explicit S3 bucket/key roster or injected local store, and the frozen V30 arm.
- Produces: canonical per-query results/work, exact recall evidence, and injected request/throughput latency projections.

- [ ] **Step 1: Write example/controller REDs.** Require explicit artifacts and identities, exact burned 32-query ordinals for regression and a preregistered disjoint untouched range for qualification, no latest/prefix discovery, no ETag digest, no legacy/version flags, no D3 surface, canonical stdout, and cleanup on every terminal.
- [ ] **Step 2: Write the fail-fast quality RED.** Recompute all 320 truth memberships independently; require at least 319 hits, 900,000-ppm minimum recall, 31 perfect queries, ten GETs, bounded bytes/work/memory, and injected Standard-S3 p50/p95/p99 decomposition without sleeping.
- [ ] **Step 3: Run the narrow REDs.** Run `cargo test -p borsuk --example v30_s3_qualify v30_ -- --nocapture` and `python3 -m unittest scripts.test_run_v30_reduced_quality`; require only missing V30 example/controller boundaries.
- [ ] **Step 4: Implement the thin boundaries.** Keep scientific scoring in Rust, use Python only for exact orchestration/evidence, and keep page bodies in Arrow with query/truth tables in Parquet.
- [ ] **Step 5: Add the fast selector.** Add only V30 focused Rust tests and controller contracts to `check_v26_fast.py`; ensure ordinary per-edit execution does not run full workspace tests or access AWS.
- [ ] **Step 6: Run GREEN and commit.** Run the complete controller file, example/library selectors, scoped Ruff, `py_compile`, formatting, and diff-check; commit only Task 4 paths.

### Task 5: Qualify one untouched 9.99M candidate on Spot

**Files:**
- Create: `scripts/run_v30_s3_campaign.py`
- Create: `scripts/test_run_v30_s3_campaign.py`
- Modify after terminal evidence: `docs/research/publication-v3-attempt-ledger.md`

**Interfaces:**
- Consumes: the committed V30 source/binary, frozen construction manifest, ordered training Parquet shards, untouched query/truth ordinals, and exact S3 output prefix.
- Produces: immutable construction, quality, CPU, cold-S3, resource, and terminal receipts.

- [ ] **Step 1: Write campaign REDs.** Require `causality` Spot, one attempt per cell, interruption discard/restart rules, 30-second health observations, RSS/PSI/swap/wall stops, exact terminal upload, and immediate instance termination.
- [ ] **Step 2: Implement and verify the launcher.** Run `python3 -m unittest scripts.test_run_v30_s3_campaign`, pinned Ruff, `py_compile`, and diff-check. The launcher must stage only registered objects and must not download the complete vector corpus to the controller/devbox.
- [ ] **Step 3: Construct once without evaluation capability.** Stream ordered training Parquet shards on one disposable builder, emit content-addressed Arrow/Parquet/page artifacts, verify exact source union/fidelity count/projection, upload terminal, and terminate.
- [ ] **Step 4: Evaluate one untouched cohort.** On a fresh Spot worker, run the exact frozen arm once. Require 995,000-ppm aggregate recall, 997,500-ppm floor compliance, 800,000-ppm minimum, ten pages, bounded work/bytes/RSS, 15-ms CPU p99, 100-ms cold-S3 p99, and no cold sample above 150 ms.
- [ ] **Step 5: Preserve the disposition.** Stop without tuning on any failed gate; report 100-percent counts separately; validate the ledger and commit only launcher/evidence paths.

### Task 6: Final assurance and 100M authorization decision

**Files:**
- Create only after a passing Task 5: `docs/research/v30-s3-production-authority.json`
- Modify only from passing evidence: `docs/production-readiness.md`
- Modify only from passing evidence: `README.md`

**Interfaces:**
- Consumes: the frozen passing V30 source, binary, arm, manifests, and Task 5 receipts.
- Produces: one release-candidate assurance receipt and an explicit authorize/reject decision for 100M construction.

- [ ] **Step 1: Run proportional affected gates.** Run the grouped V30 library/example/controller tests, formatting, scoped Python checks, and `git diff --check`.
- [ ] **Step 2: Run milestone assurance once.** On a pressure-qualified host, run `cargo clippy --locked --workspace --all-targets -- -D warnings`, then only if GREEN `cargo test --locked --workspace --all-targets`; preserve the original processes and results.
- [ ] **Step 3: Publish only supported claims.** If Task 5 and assurance pass, record V30's exact scope and authorize a separately designed 100M campaign. Do not claim guaranteed perfect recall, cold 15-ms Standard-S3 latency, or competitor superiority without paired reproduction.
- [ ] **Step 4: Commit and push.** Fetch `origin/main`, require it to be an ancestor, commit the exact verified paths with configured identity/no attribution, push fast-forward to `origin/main`, verify `HEAD==origin/main==ls-remote`, clean the worktree, and confirm no live benchmark instance.
