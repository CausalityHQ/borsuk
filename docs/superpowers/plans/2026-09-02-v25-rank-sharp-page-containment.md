# V25 Rank-Sharp Page Containment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fail fast on real data by proving rank-sharp page containment before building a bounded V25 row router, then qualify only passing designs through sealed scale gates.

**Architecture:** A claim-ineligible decomposition first evaluates exact best-row-per-page selection on the existing authenticated layout. If and only if that control passes, a capacity-bounded 16,384-list hierarchy and 12-byte residual sign-code scorer supply bounded candidate rows to the same fixed reducer. Reduced and sealed Parquet cohorts precede any 10-million-row or 100-million-row construction.

**Tech Stack:** Rust 2024, Arrow/Parquet 58.3, Arrow IPC, `half`, `rayon`, architecture-specific fused f32 SIMD, Python 3.12 standard library, pinned Ruff, static scientific binaries, AWS EC2 Spot through profile `causality`.

**Spec:** `docs/superpowers/specs/2026-09-02-v25-rank-sharp-page-containment-design.md`

## Global Constraints

- V25 is a clean format: no V24 reader, migration, alias, duplicate write path, or version dispatch.
- Bulk cross-language artifacts are Parquet or Arrow IPC; JSON contains only small authority, progress, receipt, policy, and aggregate objects.
- The fixed reducer is minimum row distance per primary/replica page followed by `(distance, page_ordinal)` ordering; mass voting is forbidden.
- Individual fixes run one named RED/GREEN test; grouped gates run once per coherent slice; strict Clippy and the full workspace run only at a milestone.
- The open 262,144-row screen and sealed 1,048,576-row sentry must pass before any full construction.
- Training sees corpus vectors only; it cannot read query, neighbor, page-quality, sentry, development, holdout, or prior-result roles.
- Corpus-derived pseudoqueries exclude their own row and primary/replica pages from both selector and oracle; own-page-included output is sensitivity evidence only.
- Scientific work uses `causality` EC2 Spot with multi-AZ fallback; no DGX, On-Demand default, page-body client, or devbox corpus persistence.
- Serving memory is at most 2,811,172,872 bytes, measured RSS is below 3 GiB, and warm p99 is below 12 ms before the 15 ms release gate.
- Page budgets 8, 12, and 16 are reported without selection on reporting cohorts; the smallest passing fixed budget is frozen.
- A terminal scientific cell never restarts. Interrupted nonterminal work may restart only from authenticated immutable inputs.

---

## File Structure

- Create `crates/borsuk-v25/Cargo.toml` and `crates/borsuk-v25/src/lib.rs`: small internal crate for V25 identities, manifests, exact page reducer, evidence rows, metrics, gates, causal classification, and canonical serializers.
- Create `crates/borsuk/src/v25_containment_local.rs`: strict Parquet/Arrow readers, streaming exact controls, local request boundary, progress, and known-file cleanup contract.
- Modify `crates/borsuk/src/lib.rs` and `crates/borsuk/Cargo.toml` only at the local-runner integration milestone.
- Create `crates/borsuk/examples/v25_page_containment.rs`: thin offline CLI with no storage/page client.
- Create `scripts/run_v25_page_containment.py`: one-process monitor, resource stops, terminal preservation, and explicit cleanup.
- Create `scripts/stage_v25_page_containment.py`: credentialed exact-object staging and offline inventory receipt.
- Create `scripts/launch_v25_qualification_spot.py`: Spot-only multi-AZ phase launcher and immediate termination.
- Create matching `scripts/test_*.py` files.
- Conditionally create `crates/borsuk/src/v25_router.rs` and `crates/borsuk/src/v25_router_local.rs` only after the exact-global control passes.
- Update `docs/research/publication-v3-attempt-ledger.md` only after authenticated terminals.

### Task 1: V25 authority, fixed reducer, and result contract

**Files:**
- Create: `crates/borsuk-v25/Cargo.toml`
- Create: `crates/borsuk-v25/src/lib.rs`
- Modify: root `Cargo.toml` workspace members
- Test: `crates/borsuk-v25/src/lib.rs`

**Interfaces:**
- Produces: `V25ObjectIdentity`, `V25Control`, `V25ContainmentSample`, `V25ContainmentResult`, `V25Disposition`, `select_v25_rank_sharp_pages`, `canonical_v25_containment_result_bytes`.

- [ ] **Step 1: Write the reducer RED.** Add `v25_containment_rank_sharp_pages_use_first_row_not_mass` with rows whose dense page has more mediocre candidates than the sparse page holding the nearest row. Require exactly eight unique pages, primary and replica support, minimum-distance replacement, and ties by ascending page ordinal.

```rust
let pages = select_v25_rank_sharp_pages(&ranked_rows, &assignments, 8)?;
assert_eq!(pages, vec![2, 7, 11, 13, 17, 19, 23, 29]);
```

- [ ] **Step 2: Write the authority/result RED.** Add `v25_containment_result_recomputes_samples_gates_and_identities`. Mutation-lock schema, source/archive/index/generation, query/control ordering, page cardinality, finite distances, hits, recall, oracle hits, oracle attainment, candidate count, aggregates, minimums, page-budget ladder, disposition, claim eligibility, URI, digest algorithm, digest, length, and input/output role disjointness.
- [ ] **Step 3: Run only the focused RED.** Run `cargo test -p borsuk-v25 v25_containment_ -- --nocapture`. Expected: unresolved V25 boundary symbols only.
- [ ] **Step 4: Implement the minimal pure boundary.** Use `f32::total_cmp` then page ordinal for total ordering. Recompute all metrics from samples; require `claim_eligible=false`; serialize sorted compact JSON with one trailing newline.
- [ ] **Step 5: Run focused GREEN and mechanical checks.** Run the same selector, then `cargo fmt --all -- --check` and `git diff --check`. Record compile and test wall separately; the named test must not build `borsuk`. Commit only the small crate, root workspace member, spec, and plan with message `Add fast V25 rank-sharp containment contracts`.

### Task 2: Strict local artifacts and exact-global decomposition

**Files:**
- Create: `crates/borsuk/src/v25_containment_local.rs`
- Modify: `crates/borsuk/src/lib.rs`
- Test: `crates/borsuk/src/v25_containment_local.rs`

**Interfaces:**
- Consumes: authenticated construction rows, page assignments, pseudoqueries, and exact truth.
- Produces: `V25ContainmentLocalPaths`, `V25ContainmentLocalRunRequest`, `run_v25_containment_local_request`, evidence Parquet, canonical result JSON.

- [ ] **Step 1: Write schema/identity REDs.** Require exact production field names/order/types/nullability, `FixedSizeList<element: Float32 non-null, 96> non-null`, nonzero offsets rejected, finite nonzero normalized vectors, contiguous unique `u64` source ordinals, one primary plus at most one replica page, exact query/truth cardinality, and full URI/SHA-256/length/generation bindings.
- [ ] **Step 2: Write an exact-global streaming RED.** Use a 257-row fixture partitioned three ways. For every query, stream every construction row once, update a bounded `Vec<(f32,u64)>` indexed by page ordinal, exclude the pseudoquery source ordinal, and require byte-identical evidence across batch sizes and partition order.

```rust
let result = run_v25_containment_local_request(request)?;
assert_eq!(result.scanned_rows, 257);
assert_eq!(result.page_body_reads, 0);
assert_eq!(result.samples[0].selected_pages, expected_pages);
```

- [ ] **Step 3: Write evidence Parquet REDs.** Require rows ordered `(query_ordinal, control_ordinal, page_budget)`, `selected_pages: FixedSizeList<element: UInt32 non-null, 16> non-null` plus `selected_page_count`, exact scalar fields, pinned writer settings, canonical metadata, and mutation rejection for every field and identity. Unused page slots contain `u32::MAX` and never enter metrics.
- [ ] **Step 4: Run focused RED.** Run `cargo test -p borsuk --lib v25_containment_local_ -- --nocapture`.
- [ ] **Step 5: Implement streaming readers and controls.** Authenticate bytes before parse, scan fixed record batches, retain only query vectors/truth and per-page minima, write evidence, re-read it, and derive JSON through Task 1's serializer. Add no corpus-sized vector plane or storage client.
- [ ] **Step 6: Run focused GREEN, fmt/diff, and commit.** Commit with message `Evaluate exact V25 page containment locally`.

### Task 3: Authentic boundary smoke and thin executable

**Files:**
- Create: `crates/borsuk/examples/v25_page_containment.rs`
- Modify: `crates/borsuk/src/lib.rs`
- Test: example-local `#[cfg(test)]` module and `v25_containment_local.rs`

**Interfaces:**
- Produces: CLI flags `--manifest`, `--input-dir`, `--output-dir`, `--evaluate-containment`, `--execute`.

- [ ] **Step 1: Write CLI REDs.** Require each flag exactly once and reject missing, duplicate, unknown, malformed, V24, bucket, endpoint, prefix, AWS, page-body, D3, and compatibility flags. Require nonempty input and empty output directories.
- [ ] **Step 2: Write authentic-smoke RED.** Build a small fixture with the production Parquet schemas, first/last row-group shapes, exact authority graph, and one query. Run the non-test entry boundary and require canonical stdout, zero page reads, explicit outputs only, and completion below the registered 90-second cap.
- [ ] **Step 3: Run example compile RED.** Run `cargo test -p borsuk --example v25_page_containment v25_containment_cli_ -- --nocapture`.
- [ ] **Step 4: Implement only the thin CLI.** Parse strict flags, validate directory inventory, call `run_v25_containment_local_request`, write returned canonical bytes to stdout, and exit nonzero on error. Do not add network/storage dependencies.
- [ ] **Step 5: Run example and smoke GREEN.** Run the named example selector and authentic-smoke node, then fmt/diff. Commit with message `Add V25 containment smoke runner`.

### Task 4: Fail-fast monitor, staging, and Spot launcher

**Files:**
- Create: `scripts/run_v25_page_containment.py`
- Create: `scripts/test_run_v25_page_containment.py`
- Create: `scripts/stage_v25_page_containment.py`
- Create: `scripts/test_stage_v25_page_containment.py`
- Create: `scripts/launch_v25_qualification_spot.py`
- Create: `scripts/test_launch_v25_qualification_spot.py`

**Interfaces:**
- Produces: `stage_exact_inputs`, `monitor_process_group`, `cleanup_known_files`, `build_v25_spot_plan`, `run_v25_spot_phase`.

- [ ] **Step 1: Write Python REDs.** Cover full digest/length/generation verification rather than ETag, stripped scientific environment, direct static binary, one process group, RSS/swap/PSI/progress/wall stops, TERM then bounded KILL, original exit preservation, terminal no-clobber, named cleanup, Spot-only requests, ordered multi-AZ capacity fallback, and termination on every path.

```python
self.assertEqual(plan.profile, "causality")
self.assertTrue(plan.market_options["MarketType"] == "spot")
self.assertNotIn("AWS_ACCESS_KEY_ID", child_env)
self.assertFalse(any("page" in flag for flag in scientific_flags))
```

- [ ] **Step 2: Run focused RED.** Run `python3 -m unittest scripts.test_run_v25_page_containment scripts.test_stage_v25_page_containment scripts.test_launch_v25_qualification_spot`.
- [ ] **Step 3: Implement credentialed parent/offline child separation.** Stage only registered files, invoke the static child without credentials/proxy variables, upload explicit output/evidence/progress/terminal files, and terminate the instance immediately.
- [ ] **Step 4: Run focused GREEN and static checks.** Run the same unittest selector, pinned Ruff, `python3 -m py_compile` on the three production/test pairs, and `git diff --check`. Commit with message `Launch fail-fast V25 containment screens`.

### Task 5: Open 262,144-row causal screen

**Files:**
- Create: `docs/research/v25-containment-open-manifest.json`
- Modify only Task 1--4 files when a preserved focused RED proves a defect
- Update: `docs/research/publication-v3-attempt-ledger.md` after terminal authentication

**Interfaces:**
- Produces: one authenticated claim-ineligible open-screen result and causal disposition.

- [ ] **Step 1: Freeze the open manifest.** Bind exactly 262,144 SplitMix-ranked real rows, 512 leave-self-out pseudoqueries, exactly eight pages, exact truth work `12,884,901,888` dimensions, source/archive/index identities, schemas, gates, resource stops, binary/source identities, output prefix, and no-restart semantics.
- [ ] **Step 2: Run the authentic boundary smoke.** Use one fresh `causality` Spot instance and the exact production binary. A non-GREEN smoke terminates before the screen.
- [ ] **Step 3: Run controls sequentially.** Compute layout then exact-global. Stop immediately if layout is below 975,000 ppm, exact-global aggregate is below 975,000 ppm, or exact-global oracle attainment is below 995,000 ppm. Record 995,000 ppm layout recall as the target, not a hard gate. Do not train a hierarchy or codebook in this task.
- [ ] **Step 4: Authenticate and classify the terminal.** Recompute every metric from evidence Parquet, verify progress completed exactly, record RSS/PSI/swap/wall/CPU, terminate the instance, and explicitly clean staged files.
- [ ] **Step 5: Commit evidence.** If rejected, record the closure and stop this plan. If passed, record `rank-reducer-candidate` and authorize Task 6. Commit with message `Record V25 containment causal screen`.

### Task 6: Capacity-bounded hierarchy and 12-byte residual row evidence

**Condition:** Execute only after Task 5 exact-global passes.

**Files:**
- Create: `crates/borsuk/src/v25_router.rs`
- Create: `crates/borsuk/src/v25_router_local.rs`
- Modify: `crates/borsuk/src/lib.rs`
- Test: both new modules

**Interfaces:**
- Produces: `V25Hierarchy`, `V25ResidualQuantizer`, `V25Router`, Arrow codecs, deterministic training/assignment/encoding, `select_v25_pages`.

- [ ] **Step 1: Write hierarchy REDs.** Require 16,384 capacity-bounded lists at 100M, at most 6,104 rows per list, fixed 128-list probing, deterministic total ties, complete source inventory, partition/order/worker invariance, and bounded external runs. Reduced fixtures use 4,096 lists of exactly 64 rows and fixed 32-list probing.
- [ ] **Step 2: Write code REDs.** Require one registered orthogonal 96-by-96 rotation, exactly 12 residual sign bytes plus one f16 residual norm and one f16 alignment denominator per row, finite parameters, deterministic training, scalar/SIMD distance ordering, and exact Arrow metadata/identity.
- [ ] **Step 3: Write resident selector REDs.** Require 128 of 16,384 lists, at most 781,312 codes and 0.782% of rows, bounded top 4,096 rows, the Task 1 reducer, exact page equality between scalar and fused SIMD, no exhaustive fallback, no cell above 2% scan, and no storage/page capability.
- [ ] **Step 4: Run focused REDs.** Run `cargo test -p borsuk --lib v25_router_ -- --nocapture`.
- [ ] **Step 5: Implement minimal deterministic production.** Use fixed-size heaps and checked offsets; store codes, primary pages, and replica pages in coarse-list order with `u64` list offsets. Measure allocation rather than inferring it.
- [ ] **Step 6: Run focused GREEN and a separate-process reduced determinism harness.** Require byte-identical hierarchy, code, and result digests at one and four workers. Run fmt/diff and commit with message `Add bounded V25 row router`.

### Task 7: Complete the open screen through bounded routing

**Condition:** Execute only after Task 6 focused and authentic-smoke gates pass.

**Files:**
- Update: `docs/research/v25-containment-open-manifest.json`
- Update: `docs/research/publication-v3-attempt-ledger.md` after terminal authentication

**Interfaces:**
- Produces: exact-contained, coded-contained, bounded, timing, and memory controls on the same open cohort.

- [ ] **Step 1: Freeze all remaining parameters before execution.** No cell sweep is allowed on reporting evidence. Bind the one hierarchy, codebook, probe, candidate, reducer, and page-budget configuration selected from reduced synthetic tests.
- [ ] **Step 2: Run exact-contained.** Reject if aggregate is below 975,000 ppm or oracle attainment is below 995,000 ppm.
- [ ] **Step 3: Run coded-contained.** Reject if aggregate is below 975,000 ppm or oracle attainment is below 995,000 ppm.
- [ ] **Step 4: Run bounded selector and resource preflight.** Reject below 975,000 ppm aggregate, 995,000 ppm oracle attainment, 12 ms warm p99, the 2,811,172,872-byte projection, or a scan above 2% of rows.
- [ ] **Step 5: Authenticate, terminate, record, and commit.** Persist the first failing causal class or `bounded-router-candidate`; commit with message `Record bounded V25 open screen`.

### Task 8: Sealed 1,048,576-row sentry

**Files:**
- Create: `docs/research/v25-containment-sentry-authority.json`
- Update: evidence ledger after terminal authentication

**Interfaces:**
- Produces: one one-shot sentry disposition for one committed architecture version.

- [ ] **Step 1: Freeze authority without opening cohort bytes.** Bind 1,048,576 disjoint rows, 4,096 capacity-bounded lists, fixed 32-list probing, 1,024 pseudoqueries, source/binary/artifact identities, gates, Spot zones/types, stops, and output prefix.
- [ ] **Step 2: Run exactly one sentry attempt.** Require aggregate at least 975,000 ppm, oracle attainment at least 995,000 ppm, oracle-relative minimum at least 800,000 ppm, p99 below 12 ms, and peak RSS below 3 GiB; report 995,000 ppm oracle-relative minimum as the quality target.
- [ ] **Step 3: Burn the version at terminal.** A rejection may inform a new committed version but cannot be tuned and rerun. Authenticate evidence, terminate compute, clean named scratch, and commit the result.

### Task 9: Milestone assurance and full-scale authorization

**Condition:** Execute only after the sealed sentry passes.

**Files:**
- Modify only focused files on preserved failures
- Create the frozen 10M authority and later the 100M authority

**Interfaces:**
- Produces: one immutable release candidate and the smallest passing fixed page budget.

- [ ] **Step 1: Run milestone assurance once.** Execute fmt, strict locked workspace/all-target Clippy, locked workspace/all-target tests, dependency-complete Python discovery, docs validator, and diff-check serially. On failure, repair only the failing layer through a named RED/GREEN and resume from that layer; rerun the complete assurance once after repairs settle.
- [ ] **Step 2: Build full artifacts in parallel on Spot.** Assignment/encoding workers consume disjoint Parquet shards and emit content-addressed sorted runs. The deterministic merge proves complete source ordinals and byte invariance before evaluation.
- [ ] **Step 3: Run 10M pseudoqueries.** Report fixed page budgets 8, 12, and 16 without choosing on the reporting cohort. A failure stops before 100M.
- [ ] **Step 4: Run 100M only after the 10M pass.** Freeze the smallest passing page budget, then measure aggregate/oracle-relative recall, measured RSS, warm selector p99, and end-to-end page fetch/decode/rerank p99 under an equivalent disclosed competitor condition.
- [ ] **Step 5: Seal holdout and release evidence.** Require at least 975,000 ppm aggregate, 995,000 ppm oracle attainment, 800,000 ppm oracle-relative minimum, selector p99 at most 15 ms, RSS below 3 GiB, and a passing end-to-end latency gate; retain 995,000 ppm oracle-relative minimum as the quality target. D3 opens only after this terminal.
