# V24 Witness-to-Page Router Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build and qualify a prerelease witness-graph router that preserves original dataset identity and selects the smallest passing set of 8, 16, 32, or 64 pages.

**Architecture:** A deterministic one-million-row corpus sample becomes a packed f16 HNSW witness graph. One authenticated page stream builds capped witness-to-page postings; development and sealed holdout queries retrieve witnesses, fuse posting evidence, and select pages without any V23 compatibility layer.

**Tech Stack:** Rust 2024, Arrow/Parquet 58.3, Arrow IPC, `half`, `rayon`, architecture-specific fused f32 SIMD, Python 3.12 standard library, static MUSL scientific binaries, AWS EC2 Spot.

**Spec:** `docs/superpowers/specs/2026-09-01-v24-witness-page-router-design.md`

## Global Constraints

- V24 rejects V23 artifacts; do not add readers, aliases, migrations, or version dispatch.
- Preserve original `u64` dataset row ordinals in every construction and posting boundary.
- Bulk cross-language data is Parquet or Arrow IPC; JSON is only small authority/evidence.
- Training sees no queries, neighbors, page labels, or prior results; posting sees no queries or neighbors.
- Use exactly 1,048,576 witnesses and HNSW `M=16`; only `ef=128/256/512`, selected witnesses `8/16/32`, posting caps `16/32/64`, and page budgets `8/16/32/64` are legal.
- Recall gates remain 975,000 aggregate, 800,000 minimum-query, and 995,000 oracle-attainment ppm.
- Serving projection remains below 3 GiB and warm selector p99 remains at most 15,000,000 ns.
- Construction may use 32 GiB RSS and 500 GiB scratch; scientific work stops on swap growth, PSI full avg10 above 0.50, missing progress for 20 minutes, or two hours wall.
- Use direct static binaries and explicit files. Do not add `ldd`, loader discovery, custom roots, mounts, or scientific network/storage clients.
- Every behavior change follows one preserved focused RED, minimal GREEN, review, and commit.

---

## File Structure

- Create `crates/borsuk/src/v24_witness.rs`: registered constants, identities, phase manifests, receipts, source-row authority, and high-level local requests.
- Create `crates/borsuk/src/v24_witness_graph.rs`: deterministic sampling, packed f16 witnesses, graph codec, build, and search.
- Create `crates/borsuk/src/v24_witness_postings.rs`: canonical page-record IDs, one-pass row assignment, bounded posting accumulation, and Arrow codec.
- Create `crates/borsuk/src/v24_witness_eval.rs`: query/neighbor truth, page fusion, exact control, timing, gates, and causal classification.
- Modify `crates/borsuk/src/lib.rs`: private modules plus one doc-hidden local run boundary.
- Create `crates/borsuk/examples/v24_witness_page_router.rs`: strict local phase CLI.
- Create `scripts/run_v24_witness_page_router.py`: phase-local monitor and explicit cleanup.
- Create `scripts/stage_v24_witness_inputs.py`: credentialed exact-object staging.
- Create corresponding `scripts/test_*.py` files and frozen research authority JSON before any allocation.
- Update `docs/research/publication-v3-attempt-ledger.md` only after authenticated terminals.

### Task 1: V24 identity and source-ordinal authority

**Files:**
- Create: `crates/borsuk/src/v24_witness.rs`
- Modify: `crates/borsuk/src/lib.rs`
- Test: `crates/borsuk/src/v24_witness.rs`

**Interfaces:**
- Produces: `V24ObjectIdentity`, `V24Phase`, `V24SourceRow`, `V24Receipt`, `validate_v24_identity`, `canonical_v24_receipt_bytes`, `parse_v24_decimal_source_ordinal`.

- [ ] **Step 1: Write the authority RED.** Add `v24_witness_authority_rejects_positional_identity_and_v23_schemas` and `v24_witness_receipt_binds_phase_inputs_outputs_and_resources`. Require decimal IDs `0` or nonzero-leading ASCII digits, exact role-specific SHA-256, unique URI/role, V24-only schemas, concrete types, canonical newline JSON, phase-legal parent digests, and input/output disjointness.
- [ ] **Step 2: Run focused RED.** Run `cargo test -p borsuk --lib v24_witness_authority_ -- --nocapture`. Expected: unresolved V24 types/functions only.
- [ ] **Step 3: Implement the minimal strict boundary.** Use `u64` ordinals directly; reject a positional counter, hexadecimal ID, leading zero, V23 schema, digest/length/URI drift, duplicate role, and input/output overlap.
- [ ] **Step 4: Run focused GREEN.** Run `cargo test -p borsuk --lib v24_witness_authority_ -- --nocapture && cargo fmt --all -- --check && git diff --check`. Expected: two tests pass, no warnings.
- [ ] **Step 5: Commit.** Commit only `v24_witness.rs` and `lib.rs` with message `Add V24 witness authority boundary`.

### Task 2: Deterministic witness sample and Arrow codec

**Files:**
- Create: `crates/borsuk/src/v24_witness_graph.rs`
- Modify: `crates/borsuk/src/v24_witness.rs`
- Test: `crates/borsuk/src/v24_witness_graph.rs`

**Interfaces:**
- Produces: `V24Witness`, `V24WitnessSampler`, `write_v24_witnesses`, `read_v24_witnesses`.

- [ ] **Step 1: Write sampler/codec REDs.** Add `v24_witness_sample_is_order_partition_and_thread_invariant` and `v24_witness_arrow_rejects_schema_identity_and_vector_drift`. Use a reduced 257-row fixture and compare one partition, reversed partitions, and four Rayon workers. Mutation-lock field name/order/type/nullability, f16 child `element`, duplicate/nonmonotone ordinals, nonfinite/zero vectors, digest, length, URI, and row count.
- [ ] **Step 2: Run focused RED.** Run `cargo test -p borsuk --lib v24_witness_sample_ -- --nocapture`. Expected: missing sampler/codec symbols.
- [ ] **Step 3: Implement bounded sampling and exact Arrow IPC.** Retain the smallest `(SplitMix64(ordinal xor seed), ordinal)` keys in a fixed max-heap, merge by the same total order, normalize in fused f32, round once to f16, sort final witnesses by witness ordinal, and authenticate bytes before decoding.
- [ ] **Step 4: Run focused GREEN and commit.** Run the same selector, fmt, and diff-check; commit with message `Build deterministic V24 witnesses`.

### Task 3: Packed deterministic witness graph

**Files:**
- Modify: `crates/borsuk/src/v24_witness_graph.rs`
- Test: `crates/borsuk/src/v24_witness_graph.rs`

**Interfaces:**
- Produces: `V24WitnessGraph`, `build_v24_witness_graph`, `search_v24_witness_graph`, `write_v24_witness_graph`, `read_v24_witness_graph`.

- [ ] **Step 1: Write graph REDs.** Add `v24_witness_graph_is_byte_deterministic_and_bounded` and `v24_witness_graph_search_matches_scalar_control_on_reduced_fixture`. Require `M=16`, deterministic levels from witness ordinal and seed, sorted unique adjacency, no self edges, bounded offsets, exact scalar/fused distance ordering, and byte-identical builds at one/four workers.
- [ ] **Step 2: Run focused RED.** Run `cargo test -p borsuk --lib v24_witness_graph_ -- --nocapture`. Expected: missing graph symbols.
- [ ] **Step 3: Implement a packed graph, not `Vec<Vec<f32>>`.** Store one contiguous f16 vector plane, `u64` offsets, and `u32` neighbors. Use the existing centroid-HNSW algorithm only as a behavioral reference; do not reuse its f32/heap-heavy representation. Construction ties are `(distance_bits, witness_ordinal)` and search returns exact-reranked unique witnesses.
- [ ] **Step 4: Add reduced exhaustive differential.** For random, ties, subnormals, reversed insertion, and disconnected-mutation fixtures, compare graph results at `ef >= row_count` with full scalar sorting.
- [ ] **Step 5: Run GREEN, fmt/diff, and commit.** Commit with message `Add packed V24 witness graph`.

### Task 4: One-pass witness-to-page postings

**Files:**
- Create: `crates/borsuk/src/v24_witness_postings.rs`
- Modify: `crates/borsuk/src/v24_witness.rs`
- Test: `crates/borsuk/src/v24_witness_postings.rs`

**Interfaces:**
- Produces: `V24PostingRecord`, `V24PostingPlane`, `build_v24_witness_postings`, `write_v24_postings`, `read_v24_postings`.

- [ ] **Step 1: Write page-identity RED.** Add `v24_postings_bind_decimal_dataset_ids_not_page_or_leaf_position`. Construct pages whose physical row order differs from IDs and prove postings remain keyed by the canonical decimal IDs; positional interpretation must fail.
- [ ] **Step 2: Write one-pass/bounds RED.** Add `v24_postings_stream_pages_once_and_keep_exact_top64`. Require one decode per page, one primary per ID, at most one replica, exactly two nearest witness assignments per unique row, no replica double-counting, checked `u32` mass, top-64 order `(-mass,page)`, and prefix equivalence for 16/32/64.
- [ ] **Step 3: Run focused RED.** Run `cargo test -p borsuk --lib v24_witness_postings_ -- --nocapture`.
- [ ] **Step 4: Implement bounded external accumulation.** Partition by witness high bits into 256 scratch runs, sort fixed `(witness,page,mass)` records, merge sums, emit canonical Arrow batches, and unlink each consumed run explicitly. Never retain page bodies.
- [ ] **Step 5: Run GREEN, fmt/diff, and commit.** Commit with message `Build V24 witness page postings`.

### Task 5: Query fusion, causal controls, and gates

**Files:**
- Create: `crates/borsuk/src/v24_witness_eval.rs`
- Modify: `crates/borsuk/src/v24_witness.rs`
- Test: `crates/borsuk/src/v24_witness_eval.rs`

**Interfaces:**
- Produces: `V24Cell`, `V24QuerySample`, `V24Evaluation`, `V24Disposition`, `evaluate_v24_cell`, `classify_v24_ladder`, `canonical_v24_result_bytes`.

- [ ] **Step 1: Write fusion/evidence REDs.** Add `v24_witness_page_fusion_uses_registered_integer_score_and_exact_ties` and `v24_witness_result_recomputes_every_sample_aggregate_gate_and_identity`. Mutation-lock witness order, posting cap, integer division, mass, page tie, exactly selected pages, hits, recall, oracle, aggregate/minimum, SIMD equality, p99 raw samples, memory projection, disposition, and all identities.
- [ ] **Step 2: Write causal-control RED.** Add `v24_witness_exact_control_separates_graph_from_posting_failure`. Require classifications `witness-postings-rejected`, `graph-retrieval-rejected`, `page-integration-rejected`, and `witness-router-candidate` from independently recomputed evidence.
- [ ] **Step 3: Run focused RED.** Run `cargo test -p borsuk --lib v24_witness_eval_ -- --nocapture`.
- [ ] **Step 4: Implement the exact fixed ladder.** Iterate lexicographically over page budget 8/16/32/64, ef 128/256/512, selected witnesses 8/16/32, and cap 16/32/64; seal the first complete pass. Exact witness scan is diagnostic only and cannot be selected as serving.
- [ ] **Step 5: Run GREEN, grouped V24 tests, fmt/diff, and commit.** Commit with message `Evaluate V24 witness page routing`.

### Task 6: Direct local executable

**Files:**
- Create: `crates/borsuk/examples/v24_witness_page_router.rs`
- Modify: `crates/borsuk/src/lib.rs`
- Test: example-local `#[cfg(test)]` module

**Interfaces:**
- Produces: doc-hidden `V24LocalRunRequest`, `run_v24_local_request`, and CLI modes `--train-witnesses`, `--build-postings`, `--evaluate-development`, `--bind-holdout`, `--evaluate-holdout`.

- [ ] **Step 1: Write CLI REDs.** Require one explicit manifest, one input directory, one output directory, one phase flag, and `--execute`; reject duplicates, unknown flags, V23 flags, bucket/endpoint/page-prefix/storage flags, AWS variables, and nonempty output.
- [ ] **Step 2: Run compile RED.** Run `cargo test -p borsuk --example v24_witness_page_router v24_witness_cli_ -- --nocapture`.
- [ ] **Step 3: Implement the thin boundary.** `main` parses only the registered flags, authenticates the full directory inventory, calls one high-level request, writes canonical stdout, and exits nonzero on any error. Add no network or storage type.
- [ ] **Step 4: Run example GREEN, grouped V24 lib tests, fmt/diff, strict Clippy, and commit.** Commit with message `Add V24 local witness runner`.

### Task 7: Phase staging, monitoring, and cleanup

**Files:**
- Create: `scripts/run_v24_witness_page_router.py`
- Create: `scripts/test_run_v24_witness_page_router.py`
- Create: `scripts/stage_v24_witness_inputs.py`
- Create: `scripts/test_stage_v24_witness_inputs.py`

**Interfaces:**
- Produces: `stage_manifest`, `validate_inventory`, `build_phase_command`, `monitor_process_group`, `run_phase`, `cleanup_known_files`.

- [ ] **Step 1: Write Python REDs.** Cover exact URI/generation/length/digest staging; complete inventory; no ETag authority; direct static binary; stripped AWS environment; RSS/PSI/swap/progress/wall stop; one TERM then KILL; original exit preservation; and explicit named-file cleanup. Assert the source contains no `ldd`, loader search, mount, chroot, pivot, recursive removal, or scientific S3 client.
- [ ] **Step 2: Run focused RED.** Run `python3 -m unittest scripts.test_stage_v24_witness_inputs scripts.test_run_v24_witness_page_router`.
- [ ] **Step 3: Implement minimal credentialed parent/offline child split.** The parent stages and later uploads; the child receives no credentials and uses the static binary. Each phase uses a fresh Spot worker and terminates after its terminal marker.
- [ ] **Step 4: Run GREEN, pinned Ruff, py_compile, shell syntax, diff-check, and commit.** Commit with message `Orchestrate V24 witness phases`.

### Task 8: Reduced determinism and resource preflight

**Files:**
- Modify only focused V24 files on observed REDs
- Create: `docs/research/v24-witness-router-manifest.json`
- Create: `docs/research/v24-witness-router-spot-authority.json`

**Interfaces:**
- Produces: one claim-ineligible reduced receipt and one frozen Spot authority.

- [ ] **Step 1: Run separate-process reduced builds.** Use 65,536 corpus rows, 4,096 witnesses, 64 pages, and identical inputs with one and four workers. Require identical witness/graph/posting/result hashes, zero unexpected files, no swap, and projected 100M memory exactly 1,644,167,168 bytes.
- [ ] **Step 2: Run CPU preflight.** Measure graph search and posting fusion with 1,024 warmups plus at least 10,000 raw native samples; persist the canonical p99 artifact. Reject any projection above 15 ms before page integration.
- [ ] **Step 3: Run final repository assurance once.** Execute fmt, strict locked workspace/all-target Clippy, locked workspace/all-target tests, dependency-complete Python discovery, docs validator, and diff-check serially. Repair only failing layers via focused TDD, then rerun the final progression once.
- [ ] **Step 4: Freeze and commit exact authority.** Bind source commit/archive, static binary, AWS account/region/AMI/Spot type, all input objects, schemas, arms, gates, stops, scratch names, output prefix, and no-restart rule. Push fast-forward only after proving `origin/main` is an ancestor.

### Task 9: One qualification campaign and production decision

**Files:**
- Modify: `docs/research/publication-v3-attempt-ledger.md` after terminals only
- Create production integration spec only if holdout passes

**Interfaces:**
- Consumes: frozen Task 8 authority.
- Produces: authenticated pseudoquery, development, holdout, and disposition receipts.

- [ ] **Step 1: Run one tree/witness training phase on Spot.** Monitor only health/progress/resources/terminal; terminate immediately. No restart after a scientific terminal.
- [ ] **Step 2: Run one posting phase on a fresh Spot worker.** Stream the authenticated D2 pages once; preserve and authenticate outputs; terminate immediately.
- [ ] **Step 3: Run the unbiased pseudoquery screen.** It may reject but cannot select. On rejection, record evidence and stop.
- [ ] **Step 4: Run burned development only after pseudoquery pass.** Seal the first passing lexicographic cell. Do not expose holdout bytes earlier.
- [ ] **Step 5: Bind and evaluate holdout once on a fresh worker.** Recompute truth from canonical decimal page IDs. Any failure rejects the architecture without tuning.
- [ ] **Step 6: Record evidence and decide.** On pass, write a separate V24 production page-body integration design covering warm end-to-end fetch/decode/exact-rerank p99 and freeze the MVP only after it passes. On failure, record the causal class and do not rerun the same representation.
- [ ] **Step 7: Validate, commit, and push only the ledger.** Prove HEAD, `origin/main`, and `ls-remote` equality and a clean worktree.

## Self-Review Record

- Every spec requirement maps to Tasks 1--9.
- V24 identity originates once; no V23 artifact crosses the boundary.
- Original dataset ordinals are present before any page-truth or posting logic.
- Serving RAM arithmetic is locked by a reduced-shape projection test.
- Query leakage is prevented by phase-specific inputs and digest-chained receipts.
- Bulk tables remain Arrow/Parquet; JSON remains small authority/evidence.
- The plan contains no compatibility, hidden fallback, dynamic-loader, or D3 task.
