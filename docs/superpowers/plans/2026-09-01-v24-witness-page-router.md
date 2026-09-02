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
- The corpus-uniform pseudoquery phase uses exactly SplitMix ranks `[1,048,576, 1,049,600)`, scans all 9,990,000 construction rows for self-excluded exact top-10 truth, and rejects only when no complete cell reaches 975,000 aggregate recall and 995,000 oracle attainment ppm; it cannot select, prune, reorder, or seal a cell. An eight-page oracle below eight hits is impossible for ten valid nonempty assignments and remains an authority failure.
- One query-independent preparation worker converts exactly 58 authenticated corpus shards and 28,282 authenticated historical pages into deterministic V24 Parquet; no historical reader enters the V24 scientific binary.
- Recall is recall@10. Each development or holdout truth boundary computes the exact optimal eight-page cover as a structural layout gate, then independently recomputes the exact cover for each registered cell's page budget with lexicographic page-list ties; an eight-page oracle below eight hits is a structural rejection, not an exception.
- `V24ObjectIdentity.generation` is logical campaign authority. S3 version IDs are optional staging metadata and never replace that value.
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
- Create `crates/borsuk/src/v24_witness_pseudoquery.rs`: deterministic disjoint corpus split, bounded exact top-10 scan, page-assignment binding, bulk evidence Parquet, and one-way screen result.
- Modify `crates/borsuk/src/lib.rs`: private modules plus one doc-hidden local run boundary.
- Create `crates/borsuk/examples/v24_witness_page_router.rs`: strict local phase CLI.
- Create `scripts/run_v24_witness_page_router.py`: phase-local monitor and explicit cleanup.
- Create `scripts/stage_v24_witness_inputs.py`: credentialed exact-object staging.
- Create `crates/borsuk/src/v24_witness_prepare.rs`: deterministic consolidation of immutable dataset shards and historical page objects into strict V24 Parquet.
- Create `crates/borsuk/examples/v24_prepare_witness_inputs.rs`: strict offline preparation CLI with no query, neighbor, evaluation, or storage surface.
- Create `scripts/launch_v24_qualification_spot.py`: multi-zone `causality` Spot preparation and phase launcher with authenticated terminals and immediate termination.
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

- [ ] **Step 1: Write graph REDs.** Add `v24_witness_graph_is_byte_deterministic_and_bounded` and `v24_witness_graph_search_matches_scalar_control_on_reduced_fixture`. Require `M=16`, deterministic levels from witness ordinal and seed, sorted unique adjacency, no self edges, bounded offsets, and exact scalar/fused distance ordering. The build is byte-identical across repeated executions; do not claim worker-count invariance for the sequential insertion authority.
- [ ] **Step 2: Run focused RED.** Run `cargo test -p borsuk --lib v24_witness_graph_ -- --nocapture`. Expected: missing graph symbols.
- [ ] **Step 3: Implement a packed graph, not `Vec<Vec<f32>>`.** Store one contiguous f16 vector plane, `u64` offsets, and `u32` neighbors. Use the existing centroid-HNSW algorithm only as a behavioral reference; do not reuse its f32/heap-heavy representation. Construction ties are `(distance_bits, witness_ordinal)` and search returns exact-reranked unique witnesses.
- [ ] **Step 4: Add two non-tautological reduced controls.** Compare the exhaustive branch at `ef >= row_count` with full scalar sorting for random, ties, and subnormals. Separately force `ef < row_count`, prove real traversal is deterministic and exact-reranked within its visited candidate set, and reject disconnected adjacency. Reversed witness order is invalid authority rather than a build mode.
- [ ] **Step 5: Run GREEN, fmt/diff, and commit.** Commit with message `Add packed V24 witness graph`.

### Task 4: One-pass witness-to-page postings

**Files:**
- Create: `crates/borsuk/src/v24_witness_postings.rs`
- Modify: `crates/borsuk/src/v24_witness.rs`
- Test: `crates/borsuk/src/v24_witness_postings.rs`

**Interfaces:**
- Produces: `V24PostingRecord`, `V24PostingPlane`, `build_v24_witness_postings`, `write_v24_postings`, `read_v24_postings`.

- [ ] **Step 1: Write page-identity RED.** Add `v24_postings_bind_decimal_dataset_ids_not_page_or_leaf_position`. Construct pages whose physical row order differs from IDs and prove postings remain keyed by the canonical decimal IDs; positional interpretation must fail.
- [ ] **Step 2: Write one-pass/bounds RED.** Add `v24_postings_stream_pages_once_and_keep_exact_top64`. Require one decode per page, one primary per ID, at most one replica, the two best exact-reranked witnesses from fixed `ef_assignment=128` per unique row, no replica double-counting, checked `u32` mass, top-64 order `(-mass,page)`, and prefix equivalence for 16/32/64. Bind source ordinals to the registered construction row count/generation and persist exact witness/unique/physical counts in Arrow schema metadata.
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

### Task 9: Deterministic full-scale input preparation

**Files:**
- Create: `crates/borsuk/src/v24_witness_prepare.rs`
- Modify: `crates/borsuk/src/lib.rs`
- Create: `crates/borsuk/examples/v24_prepare_witness_inputs.rs`
- Create: `scripts/test_prepare_v24_witness_inputs.py`
- Modify: `docs/superpowers/specs/2026-09-01-v24-witness-page-router-design.md`

**Interfaces:**
- Consumes: a frozen V24-only preparation manifest containing the exact 58 corpus-shard authorities copied once from the immutable dataset evidence, the authenticated 28,282-page roster, and its content-addressed page objects. Production never parses a V23 manifest.
- Produces: `V24PreparationRunRequest`, `run_v24_preparation_request`, `construction-rows.parquet`, `page-rows.parquet`, `preparation-receipt.json`, and a direct offline CLI.

- [ ] **Step 1: Write the preparation authority REDs.** Require 58 contiguous shard roles and ordinal intervals over `[0, 9,990,000)`, exact input `emb: FixedSizeList<element: Float32 non-null, 96> non-null`, ascending unique page ordinals, canonical decimal record IDs, exactly 9,990,000 unique primary IDs, exactly 18,620,111 physical rows, at most one replica per ID, and complete input URI/digest/length authority. Cross-bind the exact dataset ID, index ID, source-archive SHA-256, D1-report SHA-256, and page-namespace URI from the authenticated roster; require each page URI to be the namespace plus `pages/` plus its registered BLAKE3 digest. Reject any query, neighbor, development, holdout, `AWS_*`, endpoint, page-prefix, or recursive-cleanup surface.
- [ ] **Step 2: Write deterministic Parquet REDs.** In separate processes, prepare a reduced two-shard/four-page fixture twice and require byte-identical SHA-256 values. Mutation-lock column order, child name, types, nullability, `(page_ordinal, replica, numeric record_id)` row order, exactly one primary and at most one replica per source ordinal, exact decoded primary/replica f16-code equality, pinned row-group/data-page/compression/statistics/writer settings, construction digest metadata, and logical generation. Add nonzero-offset FixedSizeList mutations to every V24 reader. Retain SHA-256 for dataset/roster inputs, registered BLAKE3 for immutable page bodies, and SHA-256 for all new V24 outputs.
- [ ] **Step 3: Write bounded-streaming REDs.** Require one sequential read of every shard and page object, bounded row-group buffers plus at most 256 ordinal-range scratch runs of fixed `(record_id, page_ordinal, replica, 192-byte code)` records, checked primary/physical counts and primary/replica code equality, authenticated progress across shard/page/run units, and explicit known scratch cleanup. Prove the preparer has no query/truth input and cannot emit development or holdout authority.
- [ ] **Step 4: Run focused REDs.** Run `cargo test -p borsuk --lib v24_preparation_ -- --nocapture` and `cargo test -p borsuk --example v24_prepare_witness_inputs v24_prepare_ -- --nocapture`. Expected: only unresolved preparation/truth symbols.
- [ ] **Step 5: Implement the minimal offline preparer.** Stage historical objects outside the child, use a standalone codec for the one frozen immutable page format only inside the preparer, stream each input once, write explicit known output names exclusively, and remove only explicit scratch runs. Do not add a legacy reader, alias, or version dispatch to production. The scientific V24 runner remains unchanged and contains no historical reader.
- [ ] **Step 6: Run focused GREEN and commit.** Run the same Rust selectors, the Python preparation tests, fmt, strict targeted Clippy, pycompile, docs validator, and diff-check. Commit with message `Prepare authenticated V24 full inputs`.

### Task 10: Full qualification Spot launcher and staging integration

**Files:**
- Create: `scripts/launch_v24_qualification_spot.py`
- Create: `scripts/test_launch_v24_qualification_spot.py`
- Modify: `scripts/stage_v24_witness_inputs.py`
- Modify: `scripts/test_stage_v24_witness_inputs.py`

**Interfaces:**
- Consumes: content-addressed source archive/static binary, one exact phase manifest, optional S3 transport version metadata, and the three registered `eu-central-1` Spot targets.
- Produces: `build_v24_spot_plan`, `build_v24_launch_specs`, `run_v24_spot_phase`, authenticated `ATTEMPT_COMPLETE.json` or `ATTEMPT_FAILED.json`, and immediate instance termination.

- [ ] **Step 1: Write staging-integration REDs.** Stage at least four fake S3 objects with one shared logical generation and distinct or absent S3 version IDs, feed the resulting directory to the real phase inventory validator, and require exact SHA-256/length/URI authentication. Prove ETag is never authority and an overwritten object fails by digest.
- [ ] **Step 2: Write exact truth-binding REDs.** For each ten-neighbor assignment set, use an exact 10-bit dynamic program to maximize covered neighbors under the eight-page structural gate and each registered cell's page budget, with the lexicographically smallest sorted page list on ties. Recompute this independently in development and holdout binding, prove selected hits cannot exceed the budget-matched oracle hits, and encode an eight-page oracle below eight hits as a structural layout rejection. Never accept preregistered `oracle_pages` as truth authority.
- [ ] **Step 3: Write Spot-launch REDs.** Require AWS profile `causality`, Spot-only one-time requests, ordered `eu-central-1c/b/a` fallback, no fallback after a non-capacity error, one fresh instance per phase, exact source/binary/manifest identities, terminal no-clobber, health/progress/resource monitoring, and termination on every terminal/error/timeout path.
- [ ] **Step 4: Run focused RED.** Run `python3 -m unittest scripts.test_stage_v24_witness_inputs scripts.test_launch_v24_qualification_spot` plus the focused Rust truth-binding selector.
- [ ] **Step 5: Implement the minimal launcher and truth boundary.** Use the credentialed stager in the parent process, run the static child with stripped AWS/proxy environment, upload only explicit output/receipt/progress files, publish one canonical terminal conditionally, verify it from the controller, and terminate the instance. Replace input-authored oracle pages with the independently recomputed exact cover in each isolated cohort boundary.
- [ ] **Step 6: Run GREEN and commit.** Run the same tests, pinned Ruff, pycompile, shell syntax, docs validator, and diff-check. Commit with message `Launch V24 qualification on Spot`.

### Task 11: Query-independent corpus-uniform pseudoquery qualification phase

**Files:**
- Create: `crates/borsuk/src/v24_witness_pseudoquery.rs`
- Modify: `crates/borsuk/src/v24_witness.rs`
- Modify: `crates/borsuk/src/v24_witness_local.rs`
- Modify: `crates/borsuk/src/lib.rs`
- Modify: `crates/borsuk/examples/v24_witness_page_router.rs`
- Modify: `scripts/run_v24_witness_page_router.py`
- Modify: `scripts/stage_v24_witness_inputs.py`
- Modify: `scripts/launch_v24_qualification_spot.py`
- Test: focused Rust modules and the affected V24 Python files

**Interfaces:**
- Consumes: authenticated `posting-result.json`, `witness-graph.arrow`, `witness-postings.arrow`, `construction-rows.parquet`, and `page-rows.parquet` from one logical generation.
- Produces: `V24PseudoquerySplit`, `V24PseudoqueryTruth`, `V24PseudoqueryResult`, `select_v24_pseudoqueries`, `scan_v24_pseudoquery_truth`, `canonical_v24_pseudoquery_evidence_parquet`, `canonical_v24_pseudoquery_result_bytes`, a digest-only `pseudoquery-pass-receipt`, local phase `EvaluatePseudoqueries`, CLI flag `--evaluate-pseudoqueries`, and one Spot phase terminal.

- [ ] **Step 1: Write split and exact-truth REDs.** Add `v24_pseudoquery_split_is_disjoint_rank_exact_and_partition_invariant` and `v24_pseudoquery_truth_scans_every_row_excludes_self_and_matches_scalar`. A reduced fixture uses 64 rows, 16 witnesses, and 8 pseudoqueries; require ranks `[16,24)`, exact witness-vector binding, partition/order invariance, self exclusion, ten-neighbor `f32::total_cmp(distance)` then source-ordinal ties, fixed-block bounded storage, scalar/fused ordering equality within the registered numeric delta, and rejection of an incomplete scan.
- [ ] **Step 2: Run core RED.** Run `cargo test -p borsuk --lib v24_pseudoquery_ -- --nocapture`. Expected: unresolved pseudoquery split/truth symbols only.
- [ ] **Step 3: Implement bounded split and truth.** Reuse the registered SplitMix total order and fused distance backend. Validate witnesses against the first-ranked source rows, retain a 1,024-entry non-witness heap, stream construction row groups, and keep only ten neighbors per pseudoquery. Do not allocate query-by-corpus pairs or accept a nonfused scientific fallback.
- [ ] **Step 4: Write page/evidence/result REDs.** Add `v24_pseudoquery_pages_bind_complete_primary_replica_stream` and `v24_pseudoquery_result_recomputes_all_cells_and_cannot_select`. Require a complete ordered page-table scan, exact query primary/optional-replica assignments, a budget-matched oracle returning pages plus hits, all 108 cells in registered order, deterministic Parquet schema/bytes, own-page and rank-one sensitivity evidence, recomputed sample/cell metrics, and `selected_cell: null`. Gate only on whether any complete cell reaches aggregate recall 975,000 ppm and oracle attainment 995,000 ppm; minimum recall is evidence only, while an eight-page oracle below eight hits remains an authority failure. Mutation-lock every input/output identity and prove no pseudoquery cell or metric enters development.
- [ ] **Step 5: Run evidence RED and implement GREEN.** Run the same `v24_pseudoquery_` selector, then implement only the page binder and canonical Parquet/screen-specific JSON boundaries. Rows must be `(cell_ordinal,pseudoquery_ordinal)` ordered. Memoize four budget oracles per query after an independent equality check. Keep all per-query/scalar/timing arrays out of JSON.
- [ ] **Step 6: Write phase-authority/local CLI REDs.** Add `v24_witness_receipt_roles_are_phase_specific`, extend the local tests with `v24_witness_local_pseudoquery_authenticates_corpus_only_inputs`, and extend the example test with `v24_witness_cli_exposes_only_offline_pseudoquery_phase`. Require the exact five-file inventory, parent posting-result binding, source-ordinal-list SHA-256, no query/neighbor/page-body/storage/AWS/D3 surface, progress over split/truth/page/evaluation work, a digest-only pass receipt, and explicit output cleanup. Require development to authenticate the same graph/posting digests through that receipt while rejecting pseudoquery metrics.
- [ ] **Step 7: Implement the local phase and controller.** Add `V24Phase::PseudoqueryEvaluation`, `V24LocalPhase::EvaluatePseudoqueries`, phase-specific receipt role allowlists, strict manifest parsing, `--evaluate-pseudoqueries`, phase-specific staging, one fresh `causality` Spot worker, terminal no-clobber, and immediate termination. Bulk evidence stays Parquet; JSON contains only authority and aggregates. Keep the 3 GiB RSS cap and 4,096-row truth blocks; do not add a corpus-resident vector plane.
- [ ] **Step 8: Verify and commit.** Run focused Rust RED/GREEN selectors, affected example/Python tests, grouped `v24_witness_`, fmt, strict locked Clippy, dependency-complete Python discovery, docs validator, and diff-check serially. Commit with message `Screen V24 with unbiased pseudoqueries` and push only as a fast-forward.

### Task 12: One qualification campaign and production decision

**Files:**
- Modify: `docs/research/publication-v3-attempt-ledger.md` after terminals only
- Create production integration spec only if holdout passes

**Interfaces:**
- Consumes: frozen Task 8 authority plus the Task 9 preparation receipt and Task 10 Spot launcher.
- Produces: authenticated pseudoquery, development, holdout, and disposition receipts.

- [ ] **Step 1: Run one query-independent preparation phase on Spot.** Stage the 58 corpus shards, page roster, and 28,282 page objects, execute the offline deterministic preparer, authenticate both Parquet outputs and the exact 9,990,000/18,620,111 row counts, and terminate immediately. An interrupted attempt may restart; no terminal attempt may restart.
- [ ] **Step 2: Run one tree/witness training phase on Spot.** Monitor only health/progress/resources/terminal; terminate immediately. No restart after a scientific terminal.
- [ ] **Step 3: Run one posting phase on a fresh Spot worker.** Stream the authenticated prepared page rows once; preserve and authenticate outputs; terminate immediately.
- [ ] **Step 4: Run the corpus-uniform pseudoquery screen on a fresh Spot worker.** Authenticate ranks `[1,048,576,1,049,600)`, the source-list SHA-256, exact self-excluded top-10 truth over all 9,990,000 rows, the complete page-assignment scan, own-page sensitivity, all 108 cells, the Parquet evidence, and canonical result. Apply only the preregistered any-cell aggregate/oracle catastrophe rule. Stop on reject; on pass stage only the digest-bound pass receipt and unchanged router into development, never the per-cell table.
- [ ] **Step 5: Run burned development only after pseudoquery pass.** Seal the first passing lexicographic cell. Do not expose holdout bytes earlier.
- [ ] **Step 6: Bind and evaluate holdout once on a fresh worker.** Recompute the exact recall@10 eight-page structural oracle and the sealed cell's budget-matched oracle from canonical decimal page IDs. Any failure rejects the architecture without tuning.
- [ ] **Step 7: Record evidence and decide.** On pass, write a separate V24 production page-body integration design covering warm end-to-end fetch/decode/exact-rerank p99 and freeze the MVP only after it passes. On failure, record the causal class and do not rerun the same representation.
- [ ] **Step 8: Validate, commit, and push only the ledger.** Prove HEAD, `origin/main`, and `ls-remote` equality and a clean worktree.

## Self-Review Record

- Every spec requirement maps to Tasks 1--12.
- V24 identity originates once; no V23 artifact crosses the boundary.
- Original dataset ordinals are present before any page-truth or posting logic.
- Serving RAM arithmetic is locked by a reduced-shape projection test.
- Query leakage is prevented by phase-specific inputs and digest-chained receipts.
- Bulk tables remain Arrow/Parquet; JSON remains small authority/evidence.
- The plan contains no compatibility, hidden fallback, dynamic-loader, or D3 task.
