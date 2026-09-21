# Bounded Reader Next-Result Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Package and measure the strongest unchanged bounded BORSUK reader on frozen ReLAION-1M, authenticate an S3 Vectors service preflight before any second matched attempt, and publish either matched evidence or an exact decision record within 24 hours.

**Architecture:** Keep the selected V78 native one-wave SQ8 reader unchanged: hierarchical resident row-code routing selects rows/pages, coalesced ranged GETs read one immutable S3 object, and exact SQ8 scan returns top-100. Add typed evidence and an independent Python reducer around that reader, then run one immutable Spot cell over all 1,000 development queries twice. Separately prove the repaired S3 Vectors dependency and API lifecycle with a tiny authenticated local service preflight before deciding whether the second paid matched attempt is admissible.

**Tech Stack:** Rust, Tokio, Rayon, `object_store`, Arrow/Parquet, Serde typed structs, Python 3.12, boto3 S3 Vectors, Causality AWS Spot.

**Spec:** Active native goal `24-hour next-result phase`; historical method authority in `docs/research/algorithm-first-page-layout-ledger.md` V70–V85.

## Global Constraints

- Never reopen V108, create V109, or tune a new member of the closed router family.
- The BORSUK algorithm and selected V78 operating point remain fixed: regions 256, shortlist 512, gap 2, 128 ranged-GET concurrency.
- Use the frozen ReLAION-1M development corpus: 1,000,000 rows × 768 float32, 1,000 queries, exact Euclidean GT100.
- Call the two sequential passes `first_connection_pass` and `connection_reuse_pass`; do not claim service-cache state.
- Emit per-query returned feature IDs in Parquet so recall can be recomputed independently.
- Use typed Serde structs; do not construct production results with `serde_json::json!`.
- No S3 Vectors benchmark launch until pinned imports, local launcher tests, and an authenticated create/index/put/query/delete service preflight pass.
- Turbopuffer remains access-blocked unless a credential and tenant/namespace are actually available.
- A new architecture is optional and limited to one two-hour 100k pilot only after a concrete sub-3-GiB worksheet; historical decisive falsifiers may justify selecting zero.

## Review Focus

- Feature-row IDs, not physical row positions, must be present in every returned-neighbor sample and recall intersection.
- First/reused-connection labels must be fixed before execution and must not imply an opaque vendor cache state.
- Throughput must count successful queries only and report errors at every admitted-worker point.
- The reader must authenticate the manifest and immutable SQ8 object identity before science; ETag is not a digest.
- S3 Vectors cleanup must attempt both index and bucket deletion after every preflight outcome.

---

### Task 1: Typed Native Reader Evidence

**Files:**
- Modify: `crates/borsuk-v71/src/main.rs`
- Test: `crates/borsuk-v71/src/main.rs`

**Interfaces:**
- Consumes: existing `search(...) -> SearchOutcome` and frozen manifest truth/query arrays.
- Produces: canonical `BoundedReaderResult` JSON and `samples.parquet` with returned IDs for two 1,000-query passes.

- [ ] **Step 1: Write failing result-contract tests**

Add tests that construct two small pass sample sets and require typed aggregation of R@10, average/p05/worst R@100, p50/p95/p99 latency, GETs and bytes. Add a serializer test that rejects nonfinite values and asserts no `serde_json::Value` result construction is needed.

- [ ] **Step 2: Verify RED**

Run `cargo test -p borsuk-v71 --bin v71_native_reader bounded_evidence_ -- --nocapture` and require unresolved typed evidence/aggregation symbols only.

- [ ] **Step 3: Implement the minimal evidence types**

Define Serde structs `QueryEvidence`, `PassAggregate`, `ThroughputCell`, and `BoundedReaderResult`; compute exact integer recall units from ordered returned/truth identifiers; write Parquet fields `pass_label`, `query_ordinal`, `latency_ns`, `recall10_ppm`, `recall100_ppm`, `requests`, `bytes`, and fixed-size-list `returned_feature_row_ids[100]`.

- [ ] **Step 4: Add fixed two-pass execution**

Run all registered queries sequentially as `first_connection_pass`, immediately repeat as `connection_reuse_pass`, then run the unchanged throughput ladder. Serialize the typed result with one trailing LF and bind the samples SHA-256/length.

- [ ] **Step 5: Verify and commit**

Run the focused test, `cargo fmt --all -- --check`, `cargo clippy -p borsuk-v71 --all-targets -- -D warnings`, and `git diff --check`; commit the reader evidence slice.

### Task 2: Independent Parquet Reducer

**Files:**
- Create: `scripts/reduce_bounded_reader_result.py`
- Create: `scripts/test_reduce_bounded_reader_result.py`

**Interfaces:**
- Consumes: typed reader result, samples Parquet, and frozen ground-truth Parquet.
- Produces: `reduce_bounded_reader_result(result, samples, truth) -> ReductionReceipt` and canonical independent receipt bytes.

- [ ] **Step 1: Write failing reducer tests**

Create synthetic Arrow fixtures whose feature IDs differ from row offsets. Require exact recomputation of both recall cutoffs, p05/worst, latency percentiles, GETs/bytes and samples identity; mutate returned IDs, pass labels, schema, digest and query order independently.

- [ ] **Step 2: Verify RED**

Run `uv run --offline --python 3.12 --with-requirements scripts/requirements-format-bench.txt python -m unittest scripts.test_reduce_bounded_reader_result` and require only the missing reducer boundary.

- [ ] **Step 3: Implement and verify GREEN**

Read Parquet using strict concrete Arrow schemas, recompute every aggregate from samples plus immutable truth, compare against the producer result, and emit sorted compact typed-dataclass JSON. Run unittest, Ruff, py_compile and diff-check; commit.

### Task 3: Immutable BORSUK Spot Cell

**Files:**
- Create: `scripts/launch_bounded_reader_1m_spot.py`
- Create: `scripts/run_bounded_reader_1m_remote.sh`
- Create: `scripts/test_launch_bounded_reader_1m_spot.py`

**Interfaces:**
- Consumes: source archive, frozen source/query/truth/layout identities, V70 SQ8 URI/length/full SHA-256, and the Task 1 reader.
- Produces: create-only reservation/launch/terminal receipts binding result, samples, reduction, resources and worker log.

- [ ] **Step 1: Stage launcher REDs**

Require one-time c7i.12xlarge Spot across registered eu-central-1 zones, one attempt, 48-GiB virtual-memory cap, PSI/swap/interrupt stop, full SHA-256 authentication before science, two-pass 1,000-query command, `/usr/bin/time -v` RSS, Spot cost and unconditional termination.

- [ ] **Step 2: Verify RED, implement, and verify GREEN**

Run the exact launcher unittest under pinned Python; implement the minimum launcher/runner; rerun unittest, Ruff, py_compile, bash syntax and diff-check; commit and push a clean fast-forward revision.

- [ ] **Step 3: Execute once and reduce**

Hash the 780,000,000-byte SQ8 object on the Spot worker before science, preserve its immutable identity in the terminal, run exactly one cell, independently reduce terminal-bound evidence, terminate immediately, and verify no instance remains.

### Task 4: Authenticated S3 Vectors Preflight

**Files:**
- Modify: `scripts/benchmark_s3_vectors_parquet.py`
- Create: `scripts/preflight_s3_vectors_service.py`
- Create: `scripts/test_preflight_s3_vectors_service.py`
- Modify: `scripts/requirements-s3-vectors-match.txt`

**Interfaces:**
- Consumes: pinned NumPy/PyArrow/boto3 environment and `causality` profile.
- Produces: `run_service_preflight(client, bucket, index) -> ServicePreflightReceipt` for one tiny create/index/put/query/delete lifecycle.

- [ ] **Step 1: Write failing lifecycle tests**

Require import/vector-conversion preflight before mutation, one unique bucket/index, deterministic two-dimensional vectors and keys, exact query order, API model/version evidence, cleanup after every failure boundary, and canonical claim-ineligible receipt.

- [ ] **Step 2: Verify RED, implement, and verify locally**

Run the narrow unittest under `scripts/requirements-s3-vectors-match.txt`; implement only the preflight; rerun tests and statics. Then execute the tiny authenticated service lifecycle locally, store its receipt under the registered research prefix, and verify service resources are absent.

- [ ] **Step 3: Decide the matched attempt**

Only if Task 4 Step 2 is GREEN and Task 3 launcher tests are GREEN may one second matched S3 Vectors Spot attempt be launched. Otherwise record the exact failed prerequisite and spend nothing.

### Task 5: Competitor Disposition and 24-Hour Delivery

**Files:**
- Modify: `docs/research/algorithm-first-page-layout-ledger.md`

**Interfaces:**
- Consumes: terminal BORSUK evidence, optional terminal S3 Vectors evidence, Turbopuffer access audit, and prior V60/V61 graph falsifiers.
- Produces: one pushed decision record or release-candidate tag with exact next action.

- [ ] **Step 1: Record the strongest honest mode**

Record dataset/split, R@10, average/p05/worst R@100, both-pass p50/p95/p99, throughput/error point, GETs, bytes/query, RSS, cost, commit, terminal/result/reduction hashes and limitations.

- [ ] **Step 2: Record competitors without invented parity**

Record matched S3 Vectors only if terminal evidence exists; otherwise record the authenticated preflight plus exact benchmark blocker. Record Turbopuffer as access-blocked unless valid credential/tenant evidence exists; keep vendor figures context-only.

- [ ] **Step 3: Apply the non-router pilot gate**

Treat V60/V61 as the existing graph-family 1M pilot: returned recall passed, but the 11,061-byte/row graph and 64-page BFS-packing failure disprove a plausible blob-native 100M path. Select zero new hypotheses unless a genuinely different candidate has a complete sub-3-GiB worksheet and a two-hour 100k falsifier.

- [ ] **Step 4: Validate, review, and publish**

Run focused tests, scoped statics, research-doc validation and one proportional repository gate; perform one final dual critique only at this promotion boundary; fix Critical/Important findings once; fast-forward push and annotate either a release-candidate or next-result decision tag. Verify HEAD/origin/ls-remote, clean tree, no benchmark compute and no temporary service resources.
