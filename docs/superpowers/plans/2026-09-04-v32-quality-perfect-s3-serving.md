# V32 Quality-Perfect S3 Serving Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship the proven 16-page, perfect-recall PQ route with bounded S3-native storage and a measured low-latency serving tier.

**Architecture:** Replace the rejected page-centroid production path with the authenticated root/leaf/PQ candidate router. Keep compact Arrow/Parquet metadata resident, fetch exactly 16 Arrow pages concurrently from Standard S3 or a byte-identical same-AZ S3 Express replica, and exact-rerank only those pages.

**Tech Stack:** Rust, Tokio, object_store/AWS S3, Arrow IPC, Parquet, Python 3.12 evidence controllers, Causality EC2 Spot.

**Spec:** `docs/superpowers/specs/2026-09-04-v32-quality-perfect-s3-serving-design.md`

## Global Constraints

- Pre-release schema replacement only: no V30/V31 compatibility reader, alias, or fallback.
- Exactly 16 selected pages, at most 3,145,728 fetched bytes, 12,288 retained candidates, and 1,000,000 scanned codes.
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
- Produces: `V32SearchArm { root_beam, leaf_beam, candidate_depth, page_count }` and `V32Router::select_pages` using bounded row-PQ candidates.

- [ ] Add a production-path test whose PQ ranking and page centroids disagree; require the PQ-selected 16-page behavior and reject the centroid choice.
- [ ] Run `cargo test -p borsuk --lib v32_s3_search_ -- --nocapture`; require the intended missing/rejected V32 production boundary RED.
- [ ] Rename the coherent router surface to V32, restore root/leaf/PQ selection with fixed 8/64/12,288/16 authority, and remove centroid selection from production.
- [ ] Update the qualifier and independent Python reducer to require nonzero bounded code/candidate work and exact 16-page quality evidence.
- [ ] Rerun the focused Rust and Python selectors; require GREEN with no warnings.
- [ ] Commit the verified routing slice.

### Task 2: Replace the experimental manifest and tier authority

**Files:**
- Modify: `crates/borsuk/src/v30_s3_layout.rs`
- Modify: `crates/borsuk/examples/v30_s3_build.rs`
- Modify: `crates/borsuk/examples/v30_s3_qualify.rs`
- Create: `scripts/test_run_v32_s3_latency_preflight.py`
- Create: `scripts/run_v32_s3_latency_preflight.py`

**Interfaces:**
- Produces: schema-v3 manifest plus strict Parquet page-location table and `V32ServingTier::{Standard, Express}`.

- [ ] Add RED mutations for missing/extra/type/order/digest/length/URI/tier fields, cross-tier byte drift, and forbidden implicit fallback.
- [ ] Run only the new manifest/tier selectors and preserve the intended RED.
- [ ] Implement one schema-v3 writer/reader, delete centroid fields and old schema dispatch, and bind byte-identical Standard/Express locations.
- [ ] Add a pure latency preflight with literal profiles and the frozen formula; require Standard-144ms rejection and an injected qualifying profile pass.
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

### Task 4: Measure the serving tier before corpus work

**Files:**
- Create: `scripts/run_v32_s3_express_preflight.py`
- Create: `scripts/test_run_v32_s3_express_preflight.py`
- Modify after terminal: `docs/research/publication-v3-attempt-ledger.md`

**Interfaces:**
- Produces: canonical preflight JSON and non-null Parquet samples for the exact selected page identities.

- [ ] TDD a controller that creates one `causality` Spot worker in a supported Frankfurt Express AZ, copies only the 16 registered 100K page objects byte-for-byte, runs warmup plus at least 10,000 timed read waves, and deletes the directory bucket objects/bucket after terminal evidence.
- [ ] Reject source/result identity drift, cross-AZ compute, page-body expansion, RSS/PSI/swap growth, and incomplete cleanup.
- [ ] Run the pure controller tests and static gates.
- [ ] Execute one bounded preflight; require p99 request and bandwidth inputs whose full latency projection is at most 15 ms.
- [ ] Record evidence and commit the preflight result. Stop here if the latency projection fails.

### Task 5: Qualify 100K, then scale conditionally

**Files:**
- Modify: `scripts/run_v30_s3_campaign.py`
- Modify: `scripts/test_run_v30_s3_campaign.py`
- Modify after each terminal: `docs/research/publication-v3-attempt-ledger.md`

**Interfaces:**
- Consumes: V32 manifest, exact Standard/Express page identities, and the passing latency profile.
- Produces: authenticated quality, latency, throughput, memory, and cleanup evidence.

- [ ] Add campaign REDs for the fixed 8/64/12,288/16 arm, tier/AZ binding, quality gates, latency gates, and immediate Spot termination.
- [ ] Implement the minimal V32 campaign boundary and make the focused controller tests GREEN.
- [ ] Run one 100K Express-backed end-to-end cell; require 320/320, 32/32 perfect, p99 at most 15 ms, process CPU p99 at most 64 ms, and RSS at most 3 GiB.
- [ ] If and only if 100K passes, run the disjoint 9.99M cohort with the same frozen source and gates.
- [ ] If and only if 9.99M passes, run 100M Spot construction and serving qualification, then one full repository assurance gate.
- [ ] Commit every terminal ledger entry and freeze production defaults only after all gates pass.

