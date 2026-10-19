# Three bounded cold-serving research paths — 2026-10-19

This is a docs-only research packet, not an implementation or experiment approval.
The three options are (A) an exact, smaller SOURCE lookup representation,
(B) explicitly budgeted semantic coverage beyond the current ceiling, and
(C) size-aware dispatch of the existing authenticated SOURCE ranges. They target
different constraints; no preferred winner or native performance gain is asserted.

## Authority and observed workload

Source inspected with bounded `git show` ranges at
`afab20a8c2f58d326c4502065a80e61f2a9be680`
(`fp32-concurrent-root-delivery-a0001`), crates tree
`ab0730bfc49fe9587705275a464cb54f3813c1d2`.
The completed native evidence is
`/mnt/borsuk-workspace/evidence/fp32-concurrent-full1000-root-a0001/`:
`result.json`, `performance-audit.json`, `preregistration.json`, and
`qualified-native-authority.json`. Their hashes and the small offline geometry
result are in [the companion JSON](cold-tail-research-paths-20261019.json).
The authority binds native source `0cddd79569db2f92bf2079f1d9bbc3b90759a895`
to the same main crates tree, runner SHA-256
`afcbbfbd616d7c35aa29beb71968435f4b0e6142c526f11051796ccb21076648`.
The preregistration's prospective HOLD label is historical; the separate
completed result says VALID_MATCHED_FULL1000_PAIR.

**Workload metadata, not library constants:** Cohere 1M, D1024, cosine, k10,
fixed 1,000 development queries, ABBA, fresh scratch/processes, cache0,
multi_thread2, fetch64, Offload2, semantic64, FP32 shortlist16. The same
source/ELF differs only in one versus two active queries and fresh scratch.

| Completed measure | Q1 | Q2 |
|---|---:|---:|
| Aggregate cohort queries/second | 5.213088 | 6.332382 |
| Per-cell query p95 | 257–282 ms | 411–415 ms |
| Native process peak RSS, decimal MB | 158–172 | 232–235 |
| Modeled bytes, decimal MB | 615.933 | 871.591 |
| Mean recall@10 | 99.02% | 99.02% |

All arms preserve exact answer bits, plans and charges. Each 1,000-query cell
charges 169,024 logical GETs and 51,077,441,536 verified bytes:
SOURCE 122,948 / 39,255,017,472; SQ8 30,076 / 11,756,760,064;
canonical 16,000 / 65,664,000. These are not wire or billed bytes.
Q1 phase means are discovery 1.57–1.59 ms, planning 8.81–8.98,
SOURCE 74.29–75.17, SQ8 50.69–50.73, canonical 43.60–55.91.
Those are **stage wall intervals, not isolated CPU**. Q1 cohort CPU is
137.8–139.4 seconds against 185.3–198.3 seconds wall; overlapping Q2 query
CPU/transport counters must not be summed.

The hit histogram is 925 queries with ten hits, 61 with nine, nine with eight,
four with seven, one with three: 98 missing hits across 75 queries.
Provider cache eviction is unproven. This is a fixed-cohort Pareto observation,
not a population tail, service-capacity, production-readiness or vendor-win claim.

## Verified source seams

All line numbers below refer to that exact base; paths start at
`crates/borsuk/src/`. Public generic APIs and the dataset-specific native
runner must remain distinct.

| Call-path segment | Functions and exact inspected lines | Consequence |
|---|---|---|
| Native entry and concurrent timing | `bin/check_cohere_native_baseline.rs:817–849` `owned_query`; `query_and_seal_with` dispatch/drain at 2894–2939 | Dispatch starts the query wall timer; task siblings drain before the next batch. Changing this is a separate experiment. |
| FP32 serving | `two_bit_generation.rs:4087–4096` `search_fp32_with_store`, diagnostic wrapper 4101–4117; `search_fp32_inner` at 4162–4255 | Admission, normalization and the query lease precede the ordinary SOURCE/SQ8 shortlist; FP32 reads cannot recover an unnominated row. |
| SOURCE planning | `search_store_shortlist_unadmitted` 4613–4640 → `search_store_measured` 4654–4738 → `plan_paged_measured` 3551–3589 and 3718–3824 | Prepares the lookup, nominates, covers, fetches/authenticates, replays scores, then reaches SQ8. |
| Semantic selection | `two_bit_generation.rs:3267–3280` `source_walks` → `semantic_unit_router.rs:1653–1665` `select_leaves_with_coverage` → 1700–1721 `nominate_selected_with_coverage` | Membership nomination is metadata-driven. Serving uses the existing squared-Euclidean prototype order; changing the metric is not part of option B. |
| Coverage admission | `two_bit_generation.rs:1390–1394` `SemanticLeafLimit::new`, 2221–2243 `with_semantic_leaf_limit`, 1898–1933 `admit_source_walks_coverage` | Current explicit limit is Scale1m/1..=64; increasing it requires checked coverage/scratch/closure modeling, not an unchecked configuration edit. |
| Exact SOURCE score | `rotated_two_bit.rs:354–405` `prepare_query`, 417–441 `PreparedTwoBit::score`, 122–159 `TwoBitScoreBlock::score`; `two_bit_generation.rs:1624–1645` `SourceScores::score_response` | Already batches 32 rows, preserves per-row f64 byte accumulation, and scores authenticated responses on the existing workers. |
| SOURCE range admission and dispatch | `sq8_s3_range.rs:1752–1901` `fetch_verified_ranges_with_authentication`; 1905–1982 `fetch_source_pages_offloaded`; 2367–2377 `drain_ordered` | Validates original increasing ranges, then uses completion-driven `buffer_unordered`, restores original index order, and chooses the first original-order error after full drain. Download permits are released before authentication; byte leases survive into it. |
| SQ8 rank | `sq8_s3_range.rs:1484–1558` `rank_verified_sq8_pages_traced_with_split`; `returned_sq8.rs:202–213,232–334` cosine wrapper/`rank_ranges_impl` | Fetches and validates the full returned population including bridges; duplicate detection and final score/ID order must survive any future optimization. |
| Canonical refinement | `fp32_rerank.rs:80–134` `cosine`/`authenticate`, 140–186 `plan_ranges`, 189–218 `authenticate_range` | Sequential-f64 cosine and row SHA/ID binding; coalescing authenticates bridge rows but only selected nominees become hits. |

The current SOURCE overlap is already real code:
`two_bit_generation.rs:3746–3771` installs `SourceScores::score_response`;
`sq8_s3_range.rs:1963–1973` runs it after payload verification. Neither more
async wrappers nor a repeat of that intervention is a new option here.

## Primary material actually read

Five primary source links; the two papers' mechanisms are not Borsuk evidence.

1. [RaBitQ, arXiv:2405.12497v1](https://arxiv.org/html/2405.12497v1),
   sections 3.2, 3.3.2 and 4. It separates compressed estimation, batched
   computation and exact reranking. Its randomized estimator has a
   probabilistic error bound; that theorem is not a deterministic pruning
   certificate for Borsuk's existing two-bit reconstruction. Its IVF guarantee
   concerns probed clusters, not arbitrary missed clusters.
2. [SPANN, arXiv:2111.08566](https://arxiv.org/abs/2111.08566),
   full paper read via arXiv's ar5iv rendering, especially section 3.2.3.
   It combines memory-resident routing, disk postings, closure augmentation
   and query-aware distance-based pruning. This motivates separating coverage
   from scoring, not transferring SSD timings to authenticated S3. Its
   centroid-distance rule is not a recall certificate for this corpus.
3. [Faiss ScalarQuantizer.cpp at d4a4e3e](https://github.com/facebookresearch/faiss/blob/d4a4e3ee300dcd26e6e74116de2118a4a10a684f/faiss/impl/ScalarQuantizer.cpp#L645),
   lines 645–710. The actual dispatch code retains dimension-compatible
   SIMD fallbacks and explicit metric dispatch. This supports keeping geometry
   and fallback contracts explicit; it proves neither Borsuk score-bit parity
   nor any speed advantage for a replacement kernel.
4. [AWS S3 performance guidelines](https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance-guidelines.html),
   connection parallelism, byte ranges, measurement and retry sections.
   AWS recommends measuring resource constraints and parallel range access.
   It does not promise that larger fetch counts, reordering, or extra retries
   improve this exact operating point. No retry-policy change is proposed.
5. [Karpathy autoresearch at 228791fb](https://github.com/karpathy/autoresearch/tree/228791fb499afffb54b46200aca536f79142f117),
   README lines 7–17, 63–65 and full `program.md` (especially 25–39, 64–110).
   This is a methodology reference, not ANN performance evidence.

Public files were fetched on EC2 with 20-second/2-MiB bounds. GitHub files were
refetched at the listed immutable commits and matched their initially fetched
bytes. No repository code was installed or executed. One nonexistent RaBitQ
v3 URL returned 404; the cited v1 succeeded. SPANN PDF downloaded, but a missing
PDF text utility prevented extraction; its successful HTML body was read instead.

## Prior decisions that constrain these proposals

Read the committed decisions at the same base, rather than treating old
screening results as current measurements:

- [FP32 depth32](fp32-depth32-20261019/decision.md): the old 133-loss-query
  screen gained zero hits for twice the canonical reads. Its missing hits
  were upstream of fetched SQ8. Do not repeat shortlist32 as the quality fix.
- [Routing64](routing64-fp32-20261019/decision.md): 48→64 increased full-cohort
  hits 9852→9902, with 43 improved queries and one regression (ordinal113).
  All 98 remaining missed hits were outside semantic nomination/SOURCE
  evaluation. This is a historical boundary diagnosis, not new truth access
  in this packet. [Leaf48](leaf48-fp32-20261019/decision.md) remains a lower-I/O
  quality/cost point.
- [Progressive high-bit bound](progressive-high-bit-bound-20261019/decision.md):
  eliminated zero of 35,640 full units even with the final exact cutoff.
  Do not build the same split layout or relabel RaBitQ's probability bound
  as justification for it.
- [Canonical coalescing](fp32-coalescing-screen-20261019/decision.md):
  35.94% fewer canonical GETs cost 3.3164× canonical bytes, without consistent
  latency improvement. Preserve that request/byte Pareto point.
  [SOURCE96](source-range96/decision.md) similarly saved requests while
  adding bytes and worsening both paired p95/p99 values.
- [Chunk preservation](source-chunk-preservation-20261019/decision.md) and
  [real ownership witness](s3-stream-ownership-20261019/decision.md):
  unique-owned synthetic chunks did not describe the observed S3 response;
  zero prefix bytes met immediate zero-movement handoff. Retain the prototype
  for suitable ownership backends, not as an S3 copy win.
- [Four-row primitive closure](cold-two-bit-four-row/primitive-a0001/closed/next-step-decision.md)
  remains a valid rejection against its then-frozen gate.
  [Packed block32 full1000](source-block32-packed/full1000/decision.md)
  is a distinct adopted increment, about 5.28% CPU and 4.17% wall improvement.
  [SOURCE overlap full1000](cold-source-overlap-full1000-20261019/decision.md)
  is also already represented in the inspected source. Option A changes
  lookup representation, not the previously tested lane width.
- [Upstream profile](upstream-sq8-profile-20261019/decision.md) measured
  SQ8 rank thread CPU at 2.644 ms on a different trace/configuration;
  it cannot turn the present 50.7-ms SQ8 wall into a scoring-CPU estimate.
  [Runtime-thread ABBA](runtime-task-parallelism.pending/runtime-result-decision.md)
  and [older Q1/Q2](concurrent-query-abba-closed-a0001/decision.md) are separate
  Pareto evidence, not fresh controls for the current FP32 cohort.

A requested historical sampled-boundary decision blob was absent from the
sparse object store (tree entry existed). No archived tree was hydrated.
No fresh overlap-replication or centroid-metric arm is proposed from that
unread evidence; either would first require its exact historical decision blob.

## Simulated perspectives

The following are deliberately **simulated arguments, predictions and objections**,
not actual experts, consultations, consensus or empirical evidence.

| Perspective | Argument and prediction | Objection / discriminating observation |
|---|---|---|
| Retrieval researcher | Misses upstream of nomination make extra eligible coverage more promising for recall than deeper final reranking. Some missing IDs should enter a wider nominated set. | More leaves can change completion/shortlisting and lose previous hits; the observed ordinal113 regression already demonstrates non-monotonicity. Count gained and lost IDs separately. |
| Systems engineer | Heterogeneous range sizes plus finite permits can leave large responses late. Earlier admission of long transfers might reduce the final SOURCE tail without extra I/O. | Stochastic request latency, shared bandwidth and two-worker authentication can dominate size. The offline transfer-only model below gives little predicted headroom. |
| Rust performance engineer | Reusing a much smaller exact lookup representation may reduce preparation and cache pressure while keeping the existing 32-row lane structure. | More indexing and dependent adds may exceed saved cache misses; a full block already shares one table slice across 32 rows. Exact signed-zero and operation-order tests precede timing. |
| Measurement reviewer | Each mechanism has a cheap failure condition before a full cold experiment. Preserve both lower-latency Q1 and higher-throughput Q2. | Repeated development queries are not holdout evidence; phase wall is not CPU, failed admission is not an algorithm negative, and an unpaired slow control is not proof of a win. |

## A. Smaller exact SOURCE lookup representation

**Hypothesis/prediction:** replace each 256-entry byte lookup with a 16-entry
prefix table for the first two two-bit levels plus two four-entry term tables.
Compute each lookup as the original left-associated four-coordinate expression:
the prefix contains its original initial positive-zero addition; append term2,
then term3. Keep each row's original sequential byte sum starting at negative
zero, its final scaling, validation, and packed block32 loop. Do not quantize
the query, add FMA, use horizontal sums, or change reconstruction.

For P packed bytes, current table storage is 256×P×8 bytes; the proposed
table values need 24×P×8. At the observed D1024/P256 this is
524,288→49,152 bytes, a static arithmetic prediction, not measured allocation,
RSS or latency. Extra representation fields, alignment, construction peak,
tail handling and overlap-held Arcs must be modeled. Skipped padded coordinates
must preserve the original operation sequence, not be replaced blindly by
zero additions.

**Cheapest valid native falsifier, for the root:** first a bounded release
primitive comparing the original full scorer with the candidate, including
query preparation separately and together. Before timing, compare every
expanded byte-table entry and complete score bits, full32 and partial blocks,
at generic dimensions including 1,3,257,768,1024,1025, with malformed records,
nonfinite values, signed zero and cancellation witnesses. Preserve callback
invocation/error prefixes. Any semantic mismatch rejects the implementation.
Then alternate fixed work in both orders with identical release settings;
no improvement in total prepare+score CPU, or a material generic-tail
regression, falsifies the claimed CPU mechanism before S3 work. A small table
may remain a memory Pareto candidate even if slower; it is not a speed win.

Only a surviving primitive warrants actual-input admission, a separate canary,
then a frozen first32 native ABBA at Q1 with exact answers/plans/charges.
The final cold test must include existing SOURCE overlap; an isolated hot
lookup win alone cannot establish a query win. SQ8 and canonical scoring
remain controls, not extra interventions.

## B. Explicitly budgeted extra semantic coverage

**Hypothesis/prediction:** increase eligible coverage beyond 64 under checked
resource bounds so at least some of the 98 currently unnominated missing hits
can enter SOURCE evaluation. Start with one prospective explicit cap, for
example 80, keeping the current prototype order. The number is an experimental
configuration, never a dataset constant in generic scoring code.

This is not an immediately available knob: `SemanticLeafLimit::new` currently
refuses >64. A root-owned diagnostic/generalized coverage contract must charge
leaf IDs, membership union, closure, SOURCE-score storage, planner scratch and
per-active-query peaks before I/O. Retain existing byte/GET ceilings unless the
root prospectively authorizes a different comparison envelope. Do not silently
convert resource refusal into a lower-recall result.

**Cheapest valid native falsifier:** a frozen failure-enriched screen containing
all 75 current loss queries, with exact current64 reproduction first, then
the one wider cap. Record whether missing IDs newly enter nomination,
SOURCE survivors, fetched SQ8 and final FP32 results; record all gained/lost
hits and every charge. Zero new nomination coverage rejects this cap's
coverage mechanism; new coverage but no net final gain rejects its proposed
quality benefit at this operating point. A positive screen only nominates a
full1000 quality/cost comparison; it cannot establish mean recall, tail latency
or no regression outside the selected subset. Include the full cohort before
promotion, preserving the known regression and lower-cost 48/64 options.

SPANN suggests a later, separately falsifiable query-aware allocation policy
using routing ambiguity and resource costs. It does not justify tuning a
threshold on these 75 queries and calling it validated. First establish that
extra coverage has recoverable headroom. If it does, freeze an allocation
rule without truth-dependent dispatch and evaluate on untouched queries.
No learned-query special case, metric change or tie manipulation is allowed.

**Exact additional input seam:** the parent supplies its already authenticated
current-loss ordinal roster and native diagnostic binding at
`source_walks → select_leaves_with_coverage → nominate_selected_with_coverage`.
The worker did not open request, truth or vector payload files. Absent
unselected leaf scores/membership in the completed plan, metadata alone cannot
predict recall of cap80.

## C. Size-aware dispatch with unchanged authenticated ranges

**Hypothesis/prediction:** validate and retain the original physical-order
SOURCE plan, then dispatch indexed requests in descending expected byte
length, ties by original plan index. Keep fetch64, Offload2, cache0, every
range/ETag/digest, byte lease, scored unit and downstream plan unchanged.
Use the existing ordered outcome restoration to preserve error selection and
all result ordering. This changes admission order, not range cover or request
count. Do not sort the input before its increasing-range validation.

This would add only a bounded index/permutation buffer and its memory charge.
Its risk is queue interaction: large transfers can monopolize byte capacity or
workers, harm cross-query fairness, or overlap less favorably with scoring.
The current scheduler already refills on completion; there is no batch-wave
barrier to remove. Do not relax the FIFO fairness contract of `InFlightBytes`.

**Offline counterfactual, not native speed:** the completed A1 stream was SHA
matched to the recorded result. From semantic units plus seed additions,
256-row SOURCE cover and smallest-gap merging were reconstructed following
`budgeted_page_rank.rs:30–98`; all 1,000 per-query SOURCE GET/byte charges
matched exactly. Range count min/median/p95/max is 53/128/128/128;
994 queries exceed 64 ranges. Across 122,948 ranges, length
min/median/p95/max is 67,584/202,752/1,081,344/6,893,568 bytes.
A toy 64-server model using only byte length as service time gave descending
size/original-order pooled makespan ratio **0.993623**: 132 queries improved,
868 tied, zero worsened. This 0.64% modeled change is weak transfer-only
headroom, neither an upper bound on real improvement nor a native speed claim.
It omits request startup, authentication, score work, 66 pending futures,
network contention, query concurrency and retries. Do not use it as a reason
to launch a full1000 performance arm.

**Cheapest valid native falsifier:** first qualify the permutation with a small
controlled native range fixture: shuffled completions, two failures whose
completion order differs from plan order, cancellation and permit recovery.
Require unchanged selected error, full drain, charge semantics and no SQ8
after SOURCE failure. Then actual-input admission and a separate canary,
followed by one preregistered first-eight ABBA mechanism screen, same-source
control/candidate selector and unchanged Q1 limits. Capture SOURCE completion,
query wall, cohort CPU, request/byte parity and exact outputs. If the paired
SOURCE tail does not improve in both orders, stop this particular dispatch
hypothesis before full1000; a single favorable noisy query is insufficient.
An eight-query result never qualifies population p95. Only a surviving,
independently reproduced mechanism warrants a broader Q1/Q2 tradeoff study.

## Autoresearch method transfer and shared experiment contract

Transfer the small reviewable edit boundary, immutable evaluation, baseline
comparison, fixed resource envelope, commit/result ledger, and one explicit
hypothesis per iteration. For Rust ANN the ledger must include exact source
and ELF, original job ID, inputs, admission/canary, recall and per-query gains/
losses, wall/CPU, RSS/model, logical/transport charges and terminal outcome.
Retain useful Pareto candidates and all negative/INVALID evidence.

Do not transfer the GPU/Python training setup, `val_bpb`, five-minute training
window as a universal ANN time budget, soft VRAM constraint, automatic reset
of inferior experiments, kill-after-ten-minutes rule, or indefinite-loop
instruction. Those are the referenced project's protocol, not authority here.
Its README's exclusions of startup/compilation also do not erase our lifecycle
cost accounting. This worker has one bounded packet and must complete/archive;
only the root owns continued research, native jobs, scientific gates and spend.

For any later candidate, freeze the unchanged metric, denominator, tie rules,
generation/input pins, semantic and memory admission, transport policy and
original job handle. Distinguish compile/config/timeout/resource INVALID from
a valid scientific negative. Never restart a live job because a receipt is
quiet. Require generic dimensions, authenticated boundaries, error ordering,
full drain, cancellation and slot/buffer recovery. Cover tombstones/immutable
overlays, maintenance, reopen/compaction and generation isolation when the
touched representation or admission path reaches them.

Measure both paired orders. Mechanism-only instrumentation has its own admitted
memory and cannot be compared casually with an uninstrumented baseline.
Cohort throughput is completed queries divided by cohort wall; phase intervals
and concurrent per-query CPU are not substitutes. A later full1000 comparison
remains development-cohort evidence. Preserve Q1 latency and Q2 throughput
as separate Pareto choices; unchanged evidence does not need to be rerun.

## Packet validation and limits

All repository/evidence reads and document writes were on EC2 instance
`i-09f8571d0edf296e9` through the assigned SSM profile/region.
Commands used AllowedCPUs8, CPUQuota100%, MemoryMax512MiB, swap0, TasksMax128,
RuntimeMax120s; the root's CPU0–7 gate was untouched. Static diff/path/parent/
crates-tree and bundle checks accompany the handoff. No Rust changed.
No compiler, test, native ANN, S3 payload request, cloud control, child or
consultation was run. The only completed-JSONL analysis was metadata geometry;
no requests, truth, SOURCE, SQ8 or canonical vector payload files were opened.

Native falsifiers above are proposed work, not executed results. The missing
historical boundary blob and lack of untouched-query evidence remain explicit
limits; neither prevents delivery of this bounded packet.
