# V33 shape-aware storage-group routing implementation plan

> **For implementers:** Follow strict TDD and stop at every scientific gate.

**Goal:** Qualify one query-independent fine-routing representation that preserves
perfect truth-owner containment while reducing selected rows, code objects, and
bytes before building the production selective S3 reader.

**Architecture:** Rank fine leaves with authenticated moments/prototypes, fetch
only the bounded groups containing selected leaves, then reuse the existing PQ
candidate, page, and exact-rerank stages.

**Tech stack:** Rust, Arrow IPC, Parquet, half/f16, serde JSON, SHA-256, existing
V32 PQ/page kernels, Python controller, causality Spot.

**Spec:** `docs/superpowers/specs/2026-09-05-v33-shape-aware-group-routing-design.md`

---

### Task 0: Freeze authority and a sub-second structural gate

**Files:**
- Create: `crates/borsuk/src/v33_group_shape.rs`
- Modify: `crates/borsuk/src/lib.rs`

- [ ] Add RED tests for group ordinals, row bounds, parent coverage, logical-row
  coverage, finite vector accumulation, deterministic ties, and exact Arrow
  schemas for centroid, sphere, prototype-2/4, diagonal, and low-rank-4.
- [ ] Add checked 100M/1B memory and scoring-work projections using actual group
  counts; reject a complete process projection at or above 3 GiB.
- [ ] Implement only authority, formulas, and synthetic reducers. Run the narrow
  `v33_group_shape_` selector; no dataset or AWS access.
- [ ] Commit and push the verified authority slice.

### Task 1: Burned metadata/PQ shape proxy

**Files:**
- Create: `crates/borsuk/examples/v33_group_proxy_diagnostic.rs`

- [ ] RED-test leaf residual moment and diagonal-moment arithmetic, exact
  population-weighted group farthest-first initialization, ten Lloyd iterations,
  f16 quantization, ordinal ties, and the freeze-before-query capability boundary.
- [ ] Authenticate routing metadata, PQ codebooks/codes, the existing 178 groups,
  128 exposed queries, and 1,280 truth-owner identities. Read no exact corpus or
  page body.
- [ ] Compare centroid, fixed analytic moment, diagonal moment, three-prototype
  group minimum, and a same-byte smaller-plain-leaf control. Admit the longest
  complete-group prefix within 131,072 rows, capped at 64. Require 1,280/1,280
  owners and 128/128 perfect queries within 131,072 cumulative rows, with
  p50/p95/max frontier dominance. One miss rejects that arm with no rerun.
- [ ] Replay 480/240/120/60 logical-row pages from identical candidate order and
  report fixed page-prefix containment and bytes independently of routing.
- [ ] Record and commit the terminal. Proceed only on a pass.

### Task 2: Query-independent streaming summary builder

**Files:**
- Create: `crates/borsuk/examples/v33_group_shape_build.rs`
- Create: `scripts/run_v33_group_shape_spot.py`
- Create: `scripts/test_run_v33_group_shape_spot.py`

- [ ] RED-test one-pass streaming, bounded queues/RSS, exact logical ordering,
  deterministic farthest-first seeds and fixed updates, and zero query/truth
  capability in the builder process.
- [ ] Emit one authenticated Arrow shard set per arm plus canonical manifests.
  Never persist a local corpus copy.
- [ ] Add a 10k-row reduced-shape differential fixture and require byte-identical
  output across worker counts before any 1M stream.
- [ ] Build once on causality Spot, stream the frozen 1M corpus once, upload
  summaries, record vectors/s, CPU, bytes, RSS, and terminate the instance.

### Task 3: Fresh cohort and oracle-gap fail-fast

**Files:**
- Create: `crates/borsuk/examples/v33_group_shape_diagnostic.rs`
- Modify: `scripts/run_v33_group_shape_spot.py`

- [ ] Authenticate about 2,000 fresh queries and exact GT@10 with no ordinal,
  content, or near-duplicate source-group overlap against every prior V32 cohort;
  freeze TRAIN 800, development 600, and sealed holdout 600.
- [ ] RED-test the truth-only diagnostic that computes minimum containing group
  count and population without exposing truth to any scorer/builder API.
- [ ] Burn the development split once. Stop if the layout oracle cannot contain
  every GT@10 within 64 groups and 262,144 rows.
- [ ] Preserve the oracle receipt; do not open an arm ladder after a layout-level
  failure.

### Task 4: Development shape ladder

- [ ] RED-test exact centroid, moment, diagonal-moment, sphere lower-bound,
  prototype-min, low-rank and fixed 16-axis projected-interval scores, including f32/f16
  differential behavior and adversarial
  anisotropic/multimodal/tie fixtures.
- [ ] Run all arms on development for row budgets 262144/131072/65536. Report truth-owner
  containment, selected rows/bytes/objects, score CPU, and complete memory.
- [ ] Select at most one non-control arm and one budget by the committed ordering:
  perfect minimum containment, then fewer objects, bytes, rows, CPU, memory.
- [ ] Reject the program if no arm achieves perfect containment within 64 groups,
  131,072 rows, and 8 MiB encoded code payload. Do not tune new seeds or
  thresholds.
- [ ] Commit the chosen format/scorer and sealed-holdout registration before
  revealing any holdout result.

### Task 5: Single sealed holdout and resident replay

- [ ] Run the chosen arm once on the sealed split. Require 6,000/6,000 exact
  neighbors and 600/600 perfect queries; stop on one miss or resource violation.
- [ ] If routing passes, run selected-object PQ replay with 12,288 candidates and
  report fixed page prefixes 16/32/64. Do not choose a page budget from holdout.
- [ ] Require exact Recall@10 aggregate and minimum of 1,000,000 ppm and an actual
  request/byte improvement over the V32 64-page baseline before promotion.
- [ ] Record all results, including rejections, in the evidence ledger and send
  the operator the promised Telegram result summary.

### Task 6: Production selective S3 path only after qualification

- [ ] Replace the experimental V32 directory/layout with the winning V33 format;
  retain no compatibility reader.
- [ ] Implement bounded async group-object waves, authentication before decode,
  selected-range scoring, candidate reduction, page fetch, and exact rerank.
- [ ] Add same-host resident/selective parity, request/byte simulator, S3 latency
  injection, and fail-fast network/request-count gates before one real S3 run.
- [ ] Measure real cold/warm S3 distributions, QPS/concurrency, retries, bytes,
  CPU and RSS on 1M. Only then authorize 100M.

### Task 7: Write throughput and scalable segments

- [ ] Implement immutable delta segments with per-segment summaries, bounded
  segment fan-out, atomic manifest publication, tombstones/version resolution,
  and background compaction.
- [ ] Measure build vectors/s, append vectors/s, visibility delay, write
  amplification, compaction debt, and simultaneous read/write tail latency.
- [ ] Freeze production defaults only after 100M recall, read, write, memory and
  failure-recovery gates pass unchanged.
