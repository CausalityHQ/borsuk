# V35 Dimension-Independent S3 Routing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build and qualify a breaking high-dimensional ANN format whose hot routing state stays below 3 GiB while full-dimensional codes and vectors remain selective immutable S3 objects.

**Architecture:** Project corpus rows into a fixed 64/128/192-dimensional routing space, persist compact quantized low-rank leaf patches in Arrow, traverse them with an exact-over-stored 16-way hierarchy, and fetch only admitted remote code ranges and exact-vector pages. Stream construction and updates through immutable Parquet/Arrow segments; use fast synthetic and public-1M gates before any 10M or 100M Spot campaign.

**Tech Stack:** Rust, Arrow IPC, Parquet, serde JSON, SHA-256/BLAKE3, architecture-specific SIMD with scalar authority, Python 3.12, unittest, boto3, AWS profile `causality`, EC2 Spot, S3.

**Spec:** `docs/superpowers/specs/2026-09-05-v35-dimension-independent-s3-routing-design.md`

## Global Constraints

- V35 is a new format with no V34 reader, alias, migration, or duplicate write path.
- Source dimension `D`, routing dimension `M`, and remote bits per dimension are independent authenticated fields.
- Development may choose `M` only from 64, 128, and 192; failure at 192 rejects the representation rather than opening an outcome-triggered wider arm.
- Exact vectors and full row-code planes stay in S3; a serving process never downloads or persists the corpus.
- The complete checked 192-wide admission projection is `2,792,129,536 B`, strictly below `3,221,225,472 B`.
- The 201,326,592-B shared-cache term is fixed at 25,000,000 B of active/retiring one-bit liveness planes, 16,777,216 B of directories, and 159,549,376 B of code/page object data.
- Arrow IPC is canonical for resident routing generations, Parquet for bulk vectors/query/truth/evidence, and newline canonical JSON for small manifests and receipts.
- Query construction/training cannot read queries or truth; holdout is unreadable until the development format is committed.
- Optimized traversal must be byte-identical to exhaustive scoring of the same decoded V35 representation.
- Warm router p95 is at most 5 ms and p99 at most 8 ms; cold S3 has no 15-ms promise and must beat the paired baseline on GETs or returned bytes without latency/throughput regression.
- Sealed qualification requires at least 995,000-ppm aggregate Recall@10, 800,000-ppm minimum-query recall, and 999,000-ppm truth-owner containment.
- Use AWS profile `causality` and Spot by default; terminate compute at every terminal result.

---

### Task 1: Freeze V35 Authority and Checked Projections

**Files:**
- Create: `crates/borsuk/src/v35_authority.rs`
- Create: `crates/borsuk/tests/v35_authority.rs`
- Modify: `crates/borsuk/src/lib.rs`

**Interfaces:**
- Consumes: dataset/source identities and immutable artifact descriptors.
- Produces: `V35Dimensions`, `V35ProjectionArm`, `V35RemoteCodeRate`, `V35ArtifactIdentity`, `V35GenerationManifest`, `V35ServingProjection`, `validate_v35_manifest`, and `project_v35_serving_memory`.

- [ ] **Step 1: Write the authority RED**

  Add `v35_authority_` tests requiring positive source `D`, `M in {64,128,192}`, `M<=D`, concrete finite scales, exact role/digest algorithms, unique URIs, one active plus one retiring generation, and no legacy format field. Include 384/768/960/1536/3072D qualification cases without making those the API bounds. Mutate every field family, extra/missing/null fields, roles, algorithms, digests, lengths, source identity, dimensions, projection arm, patch arm, and remote code rate.

- [ ] **Step 2: Write exact projection REDs**

  Pin `8*M+128` bytes per one-patch leaf and the exact one-generation values `265_024_000`, `477_043_200`, and `689_062_400` for 414,100 leaves. Pin the 192-wide complete admission sum at `2_792_129_536` and hard limit at `3_221_225_472`. Make the 64-MiB delta term explicit: 32,000,000 mutation-directory bytes, 16,000,000 posting/reference bytes, 6,890,624 leaf bytes, 223,680 tree bytes, 4,194,304 four-run overhead, and 7,800,256 reserved bytes. Cover multiplication/addition overflow, boundary equality rejection, a two-patch overflow, and a reduced leaf count that legitimately admits two patches.

- [ ] **Step 3: Run the narrow RED**

  Run: `cargo test -p borsuk --test v35_authority -- --nocapture`

  Expected: compilation fails only at the missing V35 authority/projection symbols.

- [ ] **Step 4: Implement minimal typed authority**

  Define:

  ```rust
  pub struct V35Dimensions { pub source: u32, pub routing: u16 }
  pub enum V35ProjectionArm { Pca, Srht }
  pub struct V35RemoteCodeRate { pub bits_per_dimension: u8, pub bytes_per_row: u32 }
  pub fn project_v35_serving_memory(manifest: &V35GenerationManifest) -> Result<V35ServingProjection>;
  pub fn validate_v35_manifest(bytes: &[u8], registered: &V35ArtifactIdentity) -> Result<V35GenerationManifest>;
  ```

  Parse strict canonical newline JSON, authenticate exact bytes before semantic use, use checked `u64` arithmetic, and reject totals at or above 3 GiB.

- [ ] **Step 5: Run GREEN and static checks**

  Run the same test target, `cargo fmt --all -- --check`, strict Clippy for the affected library, and `git diff --check`.

- [ ] **Step 6: Commit the authority slice**

  Commit only `v35_authority.rs`, `tests/v35_authority.rs`, and `lib.rs`; fetch, require `origin/main` is an ancestor, push `HEAD:main`, and verify the full remote SHA.

### Task 2: Implement Deterministic Projection and SIMD Kernels

**Files:**
- Create: `crates/borsuk/src/v35_projection.rs`
- Create: `crates/borsuk/tests/v35_projection.rs`
- Modify: `crates/borsuk/src/lib.rs`

**Interfaces:**
- Consumes: bounded source-row blocks, `V35Dimensions`, source-ordinal training sample, and deterministic seed.
- Produces: `V35Projection`, `V35ProjectionTrainingSpec`, `V35ProjectionLimits`, `train_v35_pca`, `build_v35_srht`, `encode_v35_projection_arrow`, `decode_v35_projection_arrow`, `project_v35_query_scalar`, and `project_v35_query_simd`.

- [ ] **Step 1: Write projection authority REDs**

  Require exactly one Arrow record batch with `D` rows and one non-null `coefficients: FixedSizeList<item: Float32 not null, M>` field in source-major order. Require deterministic source-ordinal sample membership, stable post-f32 signs, canonical positive zero, decoded f32 Gram element error at most `5e-4`, exact logical checksum, complete-file SHA-256, dimension/source/sample/seed/trainer binding, finite inputs, and allocation admission before decode. The logical checksum covers a domain tag, arm, `D`, `M`, seed, an empty sample slot for the control, the complete training descriptor, and source-major little-endian f32 bits. Reject query/truth roles in the construction capability and reject unknown/missing metadata, extra batches, transposed layout, nullability/type drift, or encoded/decoded duplicates outside the declared limit. Require both arms to have the same decoded numeric byte count and ensure every downstream projection uses only the decoded f32 basis after the training f64 state is dropped.

- [ ] **Step 2: Write scalar/SIMD differential REDs**

  For every supported `D` and `M`, compare architecture-specific SIMD to increasing-dimension scalar f64 authority on zeros, subnormals, alternating magnitudes, cancellation, random finite vectors, and maximum finite f32 values. Store coefficients source-major, widen coefficient/query f32 values before multiplication, vectorize across output coordinates, and require bit-identical f64 outputs with no horizontal source-dimension reduction. Require identical wrong-length/nonfinite rejection and canonical positive zero. Record backend identity; never silently substitute a different scientific kernel.

- [ ] **Step 3: Run RED**

  Run: `cargo test -p borsuk --test v35_projection -- --nocapture`

  Expected: missing V35 projection/kernel boundary only.

- [ ] **Step 4: Implement streaming PCA and SRHT control**

  Read one registered source-ordinal sample in blocks of at most 2,048 rows and implement uncentered streamed second-moment subspace iteration with `ell=min(D,M+16)`, a domain-separated Rademacher sketch, one initial application, exactly two iterations, and fixed-order Rayleigh--Ritz: exactly four registered-sample passes. Consume unique ordinals in increasing order; parallelize independent columns, not ordinal reductions. Canonicalize repeated eigenspaces and rank-deficient completion from increasing source axes; never emit a zero row. Sample extraction, when needed, is a separately receipted streaming pass; generation encoding is one subsequent full source pass. Do not allocate `D*D`, retain `sample_rows*D`, or expose queries/truth to training.

  Implement `srht-prefix-orthonormalized-v1` by generating seeded restricted Hadamard candidates directly in a seeded full-row permutation and accepting them after two increasing-row/increasing-coordinate f64 modified-Gram--Schmidt passes. The dependence threshold is exactly `1e-12`; record skipped row indices and fail rather than changing seeds if `M` rows cannot be accepted. Persist the resulting dense source-major f32 basis and apply the same decoded-basis validation and query kernel as PCA. Do not claim standard SRHT embedding guarantees.

- [ ] **Step 5: Implement fused kernels and run GREEN**

  Provide AVX2/FMA and NEON/FMA paths that vectorize independent outputs while each lane consumes source dimensions in identical order; scalar authority remains a test/control path only. Serving applies `Pq` without PCA-mean subtraction or implicit normalization. Encode/decode the strict Arrow basis, authenticate the projection descriptor/checksum and complete-file identity, then run focused tests, affected Clippy, fmt, and diff-check; commit and fast-forward push.

### Task 3: Persist Compact Quantized Leaf Patches

**Files:**
- Create: `crates/borsuk/src/v35_patch.rs`
- Create: `crates/borsuk/tests/v35_patch.rs`
- Modify: `crates/borsuk/src/lib.rs`

**Interfaces:**
- Consumes: projected streaming leaf blocks and `V35Projection`.
- Produces: `V35PatchArm`, `V35LeafPatch`, `V35RoutingGeneration`, `build_v35_leaf_patch`, `score_v35_leaf_patch`, `encode_v35_generation_arrow`, and `decode_v35_generation_arrow`.

- [ ] **Step 1: Write patch codec and algebra REDs**

  Pin one-patch size to `8*M+128`, int8/u8 round-to-nearest tie behavior, per-plane f32 scales, six residual directions, nonnegative weights, decoded trace/trace-square/spectral values, and `E_l=mean(max(0,||x||^2-||Px||^2))`. Assert the literal score `D_p+t-a*sqrt(2*h+4*u_sigma_u)+(sqrt(E_q)-sqrt(E_l))^2`, node `E_min/E_max` interval bound, logical ordering, group bounds, and generation identities. Cover constant/singleton leaves, saturated coefficients, singular covariance, nonorthogonal decoded directions, negative scores, and nonfinite rejection. Require SIMD score intervals to order only when disjoint and overlapping intervals to return the increasing-dimension f64 authority score.

- [ ] **Step 2: Write control REDs**

  Require byte-accounted one-patch, two-patch, and equal-byte centroid arms on the identical leaf layout. Recursive patch split uses population-weighted projected variance with dimension/ordinal ties. The centroid control uses f32 centers, the same recursive split, and minimum-center-distance scoring inside an exact equal-byte envelope. A two-patch result cannot be compared against a cheaper control. SRHT and PCA generations cannot share an identity.

  Pin one 192-wide triangular f64 co-moment to
  `192*193/2*8 == 148_224` bytes and require resident moment state to be at most
  `worker_count*148_224` plus the declared row/merge buffers. A reader that
  exposes a second leaf per worker must fail before allocation.

- [ ] **Step 3: Run RED**

  Run: `cargo test -p borsuk --test v35_patch -- --nocapture`

  Expected: missing patch/codec symbols only.

- [ ] **Step 4: Implement bounded patch construction**

  Emit bounded `(Morton key,source ordinal,projected row,source row)` runs, merge them in exact key/ordinal order, and keep only one at-most-256-row leaf accumulator live while computing six components at seal. Quantize once, recompute every cached scalar from decoded coefficients, and release the leaf. Never retain the corpus, all leaf co-moments, or a dense source-dimensional covariance.

- [ ] **Step 5: Implement strict Arrow generation**

  Emit one non-null record batch in logical leaf order with exact fixed-size-list widths derived from `M`. Authenticate bytes before decoding, reject all schema and semantic mutations, and construct immutable serving state only after validation.

- [ ] **Step 6: Run GREEN and commit**

  Run the patch target, projection regressions, affected Clippy, fmt, and diff-check; commit/push the coherent patch slice.

### Task 4: Generalize Exact Hierarchical Routing to V35

**Files:**
- Create: `crates/borsuk/src/v35_route.rs`
- Create: `crates/borsuk/tests/v35_route.rs`
- Modify: `crates/borsuk/src/lib.rs`
- Read for reference: `crates/borsuk/src/v34_route.rs`

**Interfaces:**
- Consumes: authenticated `V35RoutingGeneration`, projected query, group storage descriptors, and fixed budgets.
- Produces: `V35RouteTree`, `V35RouteBudget`, `V35RoutePrefix`, `build_v35_route_tree`, `exhaustive_v35_route`, and `hierarchical_v35_route`.

- [ ] **Step 1: Write exhaustive admission REDs**

  Pin minimum patch score per group, `(score,group,leaf)` total order, complete-prefix group/row/code-byte budgets, exact first overflow, checked sums, and no group skipping. Candidate and page caps are later deterministic stages and cannot retroactively change this prefix.

- [ ] **Step 2: Write tree/bound REDs**

  Freeze deterministic 16-way construction in projected space. Pin the node layout to at most `M+128` bytes with an int8 center plus f32 scale, and pin `69_905*320 == 22_369_600` bytes at `M=192`; include decoded-center quantization error in the outward radius. Test every node bound against all descendant decoded scores for random, tie, singular, subnormal, huge-finite, quantization-boundary, repeated-mean, one/two-patch, and complement-energy cases.

- [ ] **Step 3: Write optimized/exhaustive differential REDs**

  For each `M` in `{64,128,192}`, require byte-identical optimized-versus-exhaustive selected groups, scores, rows, bytes, first overflow, and object identities. Different `M` values are separate representations and need not match each other. Cover exact ties, reversed leaf input, early all-group completion, worst-case no pruning, and mutated generation/tree digest rejection.

- [ ] **Step 4: Run RED**

  Run: `cargo test -p borsuk --test v35_route -- --nocapture`

  Expected: V35 route symbols missing, with V34 tests unaffected.

- [ ] **Step 5: Implement without compatibility dispatch**

  Port only the mathematical tree/traversal pattern, parameterized by authenticated `M`; do not port V34's f32 node representation. Quantize each node center once, enlarge its radius by the exact decoded-center error, and validate every cached bound from decoded bytes. Keep a V35-specific format and digest; do not add a V34/V35 runtime alias. Cache group row totals at generation construction, return immediately after all groups are admitted, and expand ambiguous bounds.

- [ ] **Step 6: Run GREEN and commit**

  Run V35 route tests, V34 route regressions, affected Clippy, fmt, and diff-check; commit/push.

### Task 5: Add Selective Remote Code and Exact-Vector Execution

**Files:**
- Create: `crates/borsuk/src/v35_remote.rs`
- Create: `crates/borsuk/tests/v35_remote.rs`
- Modify: `crates/borsuk/src/lib.rs`

**Interfaces:**
- Consumes: `V35RoutePrefix`, immutable object directory, range-capable object reader, and query.
- Produces: `V35ResidualSqDescriptor`, `V35RemotePlan`, `V35Candidate`, `V35SearchResult`, `build_v35_residual_sq_descriptor`, `plan_v35_remote_reads`, `scan_v35_code_ranges`, and `rerank_v35_exact_pages`.

- [ ] **Step 1: Write capability and planning REDs**

  Require selected-role-only GET/range authorization, coalescing only contiguous independently authenticated chunks in one versioned object, exact interval/digest and requested/returned byte counters, bounded retries, candidate heap cap, first-distinct exact pages, and rejection of an unselected object, whole-code-plane request, corpus path, endpoint override, or persisted page body. Pin one query workspace to 32 MiB: 4 MiB route state, 3 MiB code buffers so one authenticated encoded chunk and its decoded blocks may coexist, 1 MiB candidate/page state, 8 MiB sequential encoded/decoded page buffers, 1 MiB query/result state, and 15 MiB allocator/SDK headroom. Reject chunks above 1 MiB encoded/2 MiB decoded and pages above 256 rows or 4 MiB encoded/decoded.

- [ ] **Step 2: Write remote code-rate REDs**

  Pin per-group affine residual SQ4/SQ8: increasing-row f64 mean rounded to f16, population sigma computed relative to that decoded f16 center, `scale=8*sigma/(2^bits-1)`, center plus/minus four-sigma saturation, zero-sigma code zero, SQ4 high-nibble-first, and SQ8 source-order bytes. Pin SQ4 packed-code payload arithmetic before envelopes at 19.2/38.4/76.8/153.6 GB for 100M rows at 384/768/1536/3072D and exact doubling for SQ8. Separately account for odd-dimension padding, per-group f16 centers/f32 scales, u64 IDs/sequences, u32 primary/replica pages, chunk manifests, and object envelopes. Require bytes-per-row to agree with registered rate and `D`.

  Pin the total returned code-object budget to 8 MiB including every envelope. Before envelopes, SQ4 admits at most 43,690/21,845/10,922/5,461 rows at 384/768/1536/3072D; SQ8 admits half as many. The candidate heap is `min(12_288,scanned_rows)`.

- [ ] **Step 3: Write deterministic candidate/rerank REDs**

  Use in-memory range fixtures to require scalar/SIMD source-order ADC equality against the decoded group center/scale, including saturation and zero variance; a 12,288-row heap tied by source ordinal; first eight distinct primary/replica pages; snapshot visibility both before candidate-heap admission and for every row decoded from an exact page; ID deduplication by greatest sequence before final top-k; full-dimensional exact distance; and no read outside the plan. Include a page with a tombstone, an old replacement whose live group is unselected, and duplicate primary/replica copies.

- [ ] **Step 4: Run RED**

  Run: `cargo test -p borsuk --test v35_remote -- --nocapture`

  Expected: missing remote-plan/execution boundary only; no network is opened.

- [ ] **Step 5: Implement minimal reader-independent core**

  Define a narrow versioned range-reader trait with no list/discovery method. Authenticate each registered chunk before decoding, stream structure-of-arrays blocks directly into the bounded heap, release buffers, fetch only the first eight selected exact pages, and expose complete work/network counters.

- [ ] **Step 6: Run GREEN and commit**

  Run remote tests, Miri/property tests for range arithmetic where configured, affected Clippy, fmt, and diff-check; commit/push.

### Task 6: Build the Streaming Writer and Delta Lifecycle

**Files:**
- Create: `crates/borsuk/src/v35_build.rs`
- Create: `crates/borsuk/src/v35_delta.rs`
- Create: `crates/borsuk/tests/v35_build.rs`
- Modify: `crates/borsuk/src/lib.rs`

**Interfaces:**
- Consumes: ordered Parquet/Arrow source blocks, projection, and bounded object sink.
- Produces: `V35BuildRequest`, `V35BuildReceipt`, `V35DeltaManifest`, `build_v35_generation`, `seal_v35_delta`, and `compact_v35_generation`.

- [ ] **Step 1: Write streaming-bound REDs**

  Use a reader that panics if more than two registered blocks are alive. Require deterministic outputs across block sizes/workers, sample selection of the sixteen highest-variance projected coordinates with coordinate ties, 255 f32 quantile boundaries per selected coordinate, equality-to-lower-bucket, and a 128-bit most-significant-bit-first Morton interleave. Require `(key,source ordinal)` ordering, at-most-256-row leaves, and storage groups capped by both 8-MiB final encoded bytes and 64-MiB live builder bytes including source/projected rows, IDs, references, moments, and allocator capacity. Require bounded S3 scratch runs with 64-MiB local buffers, exact source ordinals, no query/truth access, no corpus materialization, and complete scratch/final digest/length binding.

- [ ] **Step 2: Write delta semantics REDs**

  Require conditional publication, base-plus-ordered-delta pinning, a complete snapshot mutation directory, `(id,sequence)` latest-wins suppression before heap admission even when the replacement/tombstone route is unselected, four-run/one-million-row admission, checked 32-byte directory entries, reader-safe retirement, and deterministic compaction. Pin one-bit immutable active/retiring base-row liveness planes inside the exact 25,000,000-B cache component; replacement/delete publication changes these planes rather than rewriting full base code or vector pages. Require the same snapshot liveness decision before candidate-heap admission and after exact-page decode; stale rows may charge route work but never enter either result stage. Pin the single 64-MiB delta reservation across all old/new pinned directories, construction copies, hash-table capacity, run metadata, delta leaves, and delta trees; an old-reader publication race must backpressure before allocating an over-budget copy.

- [ ] **Step 3: Run RED**

  Run: `cargo test -p borsuk --test v35_build -- --nocapture`

  Expected: missing builder/delta boundary only.

- [ ] **Step 4: Implement bounded construction**

  Project registered source blocks, emit `(key,source ordinal,projected row,source row)` scratch runs, merge them in exact order, create at-most-256-row leaves and capped consecutive storage groups, build the Task 5 per-group SQ4/SQ8 descriptors from one bounded group buffer, and emit immutable code/vector objects and assignment directories. Publish only after all content digests are known. Keep one bounded block per worker, one leaf accumulator, one at-most-64-MiB group buffer, and declared 64-MiB merge buffers; lifecycle-tag and clean only registered scratch objects after terminal publication.

- [ ] **Step 5: Implement delta/compaction state machine**

  Use immutable segments, snapshot-bound chunked liveness planes, a snapshot-pinned latest-sequence directory, and conditional manifest replacement. Aggregate all simultaneously pinned active/retiring bitmap and mutation-directory bytes under their single declared terms and backpressure before copying. Compaction streams affected objects, writes a new generation, atomically publishes it, and deletes nothing or drops covered directory entries until all reader pins release.

- [ ] **Step 6: Run GREEN and commit**

  Run builder tests, affected Clippy, fmt, and diff-check; commit/push.

### Task 7: Create the Fast High-Dimensional Qualification Harness

**Files:**
- Create: `crates/borsuk/examples/v35_synthetic_gate.rs`
- Create: `scripts/run_v35_qualification.py`
- Create: `scripts/test_run_v35_qualification.py`
- Modify: `scripts/requirements-format-bench.txt`

**Interfaces:**
- Consumes: frozen V35 binary, registered manifests, phase-specific local/S3 paths, and capability-separated credentials.
- Produces: canonical P0/P1/P2/P3/P4 receipts and immutable evidence objects.

- [ ] **Step 1: Write harness REDs**

  Require explicit phase, exact binary/source identities, separate construction/router/evaluator roles, no page client in P0/P1, terminal no-clobber, interruption classification, instance identity, RSS/PSI/swap/wall stops, and cleanup of only named scratch files. Reject missing/duplicate/unknown/AWS endpoint/D3 flags.

- [ ] **Step 2: Write synthetic-family REDs**

  Pin `D={384,768,1536,3072}`, intrinsic ranks 32/64/128, rotations, anisotropy, multimodality, heavy tails, hubness, drift, and a full-rank isotropic stress family from registered seeds. Add independent per-query post-route owner/page permutations from `HMAC(master_seed,query_ordinal)`, with the master seed capability-isolated from the router. Convolve each query's exact hypergeometric distribution in ordinal order, count repeated truth rows on one page once, and gate the total upper tail at `alpha=1e-6`. Exact truth is computed in original `D`, never projected space.

- [ ] **Step 3: Run RED**

  Run the exact Rust example filter and `python3 -m unittest scripts.test_run_v35_qualification` serially. Expected failures are only missing harness symbols/files.

- [ ] **Step 4: Implement P0/P1 fail-fast execution**

  Run P0 sub-second contracts first. P1 records owner containment, recall, leaf fraction, CPU samples after 1,024 warmups and at least 10,000 timed routes, memory projection, and remote-read authorization. Report isotropic stress without a forced outcome. After route outputs are immutable, the isolated evaluator applies the registered hidden permutation and checks the exact hypergeometric upper tail at `alpha=1e-6`. Stop the cell at the first failed preregistered gate.

- [ ] **Step 5: Implement P2 sealed development/holdout**

  Register public dataset license/source hashes, split/deduplicate queries, build exact full-D GT@10, burn development once, apply the spec's global lexicographic arm rule, commit the chosen format, then open holdout exactly once. Add a literal fixture where maximizing minimum recall, maximizing total hits, minimizing maximum bytes, minimizing maximum p99, and minimizing memory choose different arms; require the rule's stated direction and final canonical-arm-ID tie.

- [ ] **Step 6: Verify the complete repository once**

  Run focused Rust/Python gates during repairs. When stable, run `cargo fmt --all -- --check`, strict workspace/all-targets Clippy, locked workspace/all-targets tests, dependency-complete Python discovery, research-doc validation, and `git diff --check` exactly once. Repair only failing layers, then repeat the full gate once.

- [ ] **Step 7: Commit the harness and registration**

  Commit/push the verified harness and immutable P1/P2 registration before creating fresh query artifacts or launching compute.

### Task 8: Execute the Evidence Ladder and Promote Only a Survivor

**Files:**
- Modify: `docs/research/publication-v3-attempt-ledger.md`
- Create: `docs/research/v35-high-dimensional-qualification.md`

**Interfaces:**
- Consumes: committed Task 7 harness/registration and immutable terminal receipts.
- Produces: a rejection record or one frozen release-candidate format.

- [ ] **Step 1: Run P1 locally or on an existing no-spend worker**

  Execute every registered synthetic dimension/family/arm. A positive-family candidate fails on authority mismatch, owner containment below 999,000 ppm, router p95 above 5 ms, leaf fraction above 25%, or 100M projection at/above 3 GiB; another preregistered candidate may survive. Report full-rank isotropic stress without forcing failure. Separately require the capability-isolated post-route owner/page permutation to remain below its exact hypergeometric upper-tail bound at alpha `1e-6`, or invalidate the harness for leakage.

- [ ] **Step 2: Run P2 public-1M development and holdout**

  Use no paid scale resource until P1 passes. Require one public corpus in every 384/768-or-960/1536/3072D band, 600 development and 600 sealed holdout queries per corpus, and exact original-dimensional GT@10. Preserve every terminal receipt, apply the automatic arm selection across the complete mandatory matrix, open holdout once, and require the complete recall/resource/selectivity/paired-baseline gates.

- [ ] **Step 3: Run P3 10M on one `causality` Spot worker**

  Preregister the real corpus/license/source URI/hash/dimension/actual row count, 600 queries, blocked f64 full-scan GT@10, region, instance candidates, maximum cost, wall/pressure stops, interruption handling, and exact S3 prefixes. An approximately-8.8M MS MARCO corpus is labeled 8.8M, never 10M. Terminate the instance immediately after terminal sync.

- [ ] **Step 4: Run one P4 100M qualification**

  Promote only the unchanged P3 survivor. If no licensed real 100M source at dimension at least 1,536 is registered, generate exactly 100M rows at 1,536D with the committed counter-based P1 generator and compute GT@10 for 128 sealed queries by one f64 blocked full scan; label the result synthetic scale/resource evidence, not real-data quality. Measure three terminal repetitions of the recall frontier, warm/cold latency, QPS, GETs/bytes, RSS, and the fixed 70/20/10 insert/replace/delete workload through three compactions, choosing replacement/delete IDs uniformly without replacement from the live set. Require every repetition to pass identity/recall/memory/access gates. Apply the absolute warm p99 at-most-15-ms gate only to a preregistered query subset repeated at least 10,000 times after 1,024 warmups whose union of selected objects is at most 159,549,376 B; apply only paired relative gates to the complete 10,000-distinct-query workload. Require median p95/p99 no more than 105% of baseline with no repetition above 110%; QPS at least 95%; and at least 10% fewer cold GETs or bytes. Require writer rows/s at least 125% in all repetitions, visibility p95 at most one second, total S3 write amplification at most 3.0 using every final/scratch/retry/compaction byte over logical uncompressed accepted bytes, and concurrent-read p99 at most 110% of no-write V35.

- [ ] **Step 5: Record the decision**

  Validate research docs and commit immutable result identities. On failure, record the causal boundary and stop. On pass, replace the experimental reader/writer defaults with the frozen V35 format in a separately reviewed breaking-change commit; retain no predecessor compatibility layer.
