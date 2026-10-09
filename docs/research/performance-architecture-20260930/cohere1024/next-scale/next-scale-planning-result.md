Recommend one bounded tooling change: **extend the existing Rust preparer and runner for an authenticated 1M fixture, then run one 32-query local native recall/resource falsifier before any full cold-S3 measurement.** Keep baseline A, its source-order recipe, and native scoring unchanged.

This plan uses committed main `e64e01e…` and candidate `6de2799d…`. The candidate’s closed qualification failed; its replacement must pass the existing library gates before this plan proceeds. The ongoing repair and critique remain outside this scope.

**1. Freeze the larger fixture without query leakage**

Use the existing revision, English shard ordering, D1024, cosine, k=10, and original unnormalized f32 bits:

- Queries remain source ordinals **`[100000,101000)`**, permanently excluded from corpus selection.
- The 1M corpus is **`[0,100000)` followed by `[101000,1001000)`**.
- Preserve historical corpus IDs `0..99999`; assign subsequent corpus ordinals consecutively. Record publisher IDs and exact shard/row locators separately.
- Authenticate and reject duplicate document IDs, including collisions with all 1,000 reserved queries. Reject invalid/missing rows; never substitute another row.
- Preserve the historical query binary exactly: SHA256 `8460a81ff2f979deff7d82bede874a1301f47dfd3e4589305c9f53e020920d5e`. Verify the larger corpus’s first 100,000 rows against the retained corpus hash.
- Compute new exhaustive truth against all 1M corpus rows. Historical 100k truth is invalid for this corpus.

The committed preparation receipt establishes **two shards of 100,000 rows each**, totaling 433,359,090 compressed bytes. Those inputs cannot supply 1M. If subsequent shards also contain 100,000 rows, selection requires eleven whole shards, through `en/0010.parquet`; **their availability, lengths, hashes and row counts remain unverified**. Freeze the actual ordered roster before acquisition/execution—do not infer those properties from filenames.

This selection follows [single-dataset-focus.md](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/single-dataset-focus.md:21) and the [retained preparation receipt](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/cohere1024/native-preflight-spot/a0002/preflight/prepared/complete.json).

**2. Smallest owned implementation**

| Owned file | Concrete change |
|---|---|
| `crates/borsuk/src/bin/prepare_cohere_native_cohort.rs` | Replace fixed geometry/two-shard assumptions with authenticated geometry, ordered shard descriptors and explicit corpus/query intervals. Separate extraction from truth generation. Stream original binary output; compute truth by scanning sealed binary corpus blocks, retaining queries and bounded top-k heaps instead of the full corpus. |
| `crates/borsuk/src/bin/check_cohere_native_baseline.rs` | Derive shape from validated config; require explicit profile and resource limits. Replace both `Native100k` checks with exact configured-profile agreement. Bind the cohort/derivation receipts, preserve request authentication and seal-before-truth, and emit all effective limits. |
| `crates/borsuk/examples/compare_native_replay.rs` | Support the new result schema for single-run reduction, including authenticated geometry and diagnostic scope. Validate seals, exact query count, source/config identities and charge totals. Produce population percentiles only for the full 1,000-query run. |

Bump preparer config/receipt and runner config/result format markers. Reject incompatible formats clearly; historical results retain their original binaries, source archives and dispositions.

Keep D1024/k10 restrictions in these **experiment tools**. No Cohere-specific logic belongs in the library.

Reuse unchanged:

- `build_sq8_source normalize`, then the existing `fit`, then SQ8 construction.
- `build_two_bit_generation` with `discovery:"semantic"` and `semantic_profile:"scale1m"`.
- `TwoBitGenerationBuilder::build_with_semantic_profile`.
- Existing publication and native query APIs.

The generation builder already accepts explicit profiles. A new router-building adapter is unnecessary. Bind builder payload caps supplied through CLI arguments into the hashed stage protocol and exact invocation receipt.

Retain original f32 files permanently. Normalized vectors, order, SQ8 and generation artifacts are separately hashed derivatives; truth always uses the originals.

**3. Prospective envelope—arithmetic, not measured admission**

For `N=1,000,000`, `D=1024`, `Q=1000`, the source gives these quantities:

| Component | Checked formula / prospective amount |
|---|---|
| Original corpus | `4ND` = **4,096,000,000 B**; normalized derivative costs another identical amount |
| Queries / full truth | `4QD` = **4,096,000 B**; `8Qk` = **80,000 B** |
| Order / canonical / SQ8 | `8N` = **8,000,000 B**; `N(4D+8)` = **4,104,000,000 B**; `N(D+12)` = **1,036,000,000 B** |
| Two-bit records | `N(ceil(padded_D/4)+8)` = **264,000,000 B** |
| Router geometry | `U=ceil(N/32)=31,250`; centres `489`; maximum leaves `977`; root bound **4,064,832 B** |
| Router construction | Candidate model **506,729,928 B**, under its **536,870,912 B payload cap** |
| Ordered generation build | Existing model **515,513,384 B**; this is neither total RSS nor the whole maintenance lifecycle |
| Flat source fit | Source formula gives **1,134,200,704 B**, assuming the target’s 24-byte `RankedRow`; confirm native layout |
| Exact truth work | 32 queries: **32,768,000,000** coordinate products; 1,000 queries: **1,024,000,000,000** |
| Flat-fit work | With `C=3907`, `S=250048`: `12SCD + NCD + C²D ≈ 1.6021×10¹³` coordinate terms |

The flat fit is the main build-cost risk: it grows approximately quadratically here. Its historical 100k stage took about 4.20 seconds; a roughly 100× work extrapolation is **not** a measured 1M deadline. Keep the recipe unchanged for this gate; a timeout does not authorize switching to `hier-fit`.

For streaming preparation, budget:

`identity set + retained shard metadata + admitted Parquet decoder/batch peak + query/block buffers + output buffers`.

At the existing maximum ID length, the conservative identity-set term alone is  
`(N+1000) × (1024+256) = 1,281,280,000 B`. Streaming eliminates the 4.096GB corpus vector allocation, but does not eliminate ID or decoder costs.

A prospective **remote pilot** envelope is:

- Extraction/build: **4 CPUs, 8 GiB process memory, no swap**, serial stages; 7 GiB general builder payload allowance, while the router’s own 512 MiB cap remains enforced.
- Truth: one scalar worker, **1 GiB process memory**, bounded blocks; preserve sequential f64 coordinate accumulation and corpus-ordinal tie breaking.
- Serving falsifier: **1 CPU, concurrency 1, 512 MiB modeled payload, 1 GiB process ceiling**, no swap.
- **32 GiB data scratch**, separately from compiler targets; extraction/build deadline 2,400 seconds, truth deadline 1,200 seconds, 32-query replay deadline 120 seconds.

These are proposed new pilot ceilings requiring root admission, **not an extension of the historical 512MiB process qualification**.

Scratch must be admitted from the actual coexistence schedule. Original/normalized vectors and principal derived files total approximately **13.742GB before shard bodies, ID text, publication copies and temporary files**. The current worst-case ID-text formula adds **7.175GB**. Include every local publication copy and pending output before accepting 32 GiB.

For baseline serving, initially retain explicit limits of **128 SOURCE GETs / 64 MiB** and **32 SQ8 GETs / 16,773,120 B** per query. Current membership discovery performs no leaf-payload GETs. Thus the configured query ceiling is **160 logical GETs and 83,881,984 verified bytes**, excluding startup, HEADs, retries and transport overhead. This is a trial envelope, not a measured cost or universal scale gate.

Before freezing performance limits, measure stage RSS/cgroup peaks, scratch high-water marks, build/truth time, actual metadata residency, caller pins, query scratch, physical request attempts and bytes. Maintenance also needs its own coexistence accounting: the candidate’s whole-build model is `523,775,528 + P + H + S` bytes, and its conservative disk model is **11,052,387,168 B**. Fresh-index success does not qualify maintenance or multiple live generations.

**4. One cheapest real 1M falsifier**

Use **the first 32 historical query ordinals, selected before outcomes**. Preserve the full 1,000-query binary and exclusion interval, but derive an authenticated 32-query prefix:

- Requests: **131,072 B**.
- Newly exhaustive truth: **2,560 B**, against every 1M original corpus row.
- One real native generation build and one local object-store replay.
- Baseline A, `scale1m`, source cache off, fetch parallelism 32, concurrency 1.
- Existing diagnostic-panel execution with tracing off; seal returned results before opening truth.

Proposed falsification rule: **fewer than 304/320 hits, any underfill, invalid IDs/scores, or an evidenced resource-envelope violation stops progression**. The 0.95 threshold is a prospective gross-regression screen; it does not establish population recall or preserve the historical 0.9723 result.

Configuration, authentication, transport or enforcement failures are **INVALID**. Classify a resource failure only from evidence that the declared envelope was actually enforced. Preserve the attempt; no automatic cap increase, parameter sweep or retry.

Local timings are diagnostic only. This gate establishes no cold-S3 latency, QPS or competitor result.

**5. Admission and cold-measurement sequence**

1. Wait for the repaired library revision to pass its original gates and root integration. Qualify the tooling revision with affected Rust tests, release builds, workspace Clippy and `scripts/check_rust_test_build.sh`, recording exact revision and exits.
2. Test interval overlap, reserved-query exclusion, short/reordered shards, duplicate IDs, original f32 bit preservation, streaming truth/ties, arithmetic overflow, profile mismatch and seal-before-truth. Keep fixtures tiny and generic.
3. Run actual-input validation and the single 32-query falsifier above on authenticated 1M records, requests and truth.
4. If it passes, generate full 1,000-query truth without tuning the algorithm. Validate the exact full-run artifacts/config through the runtime validator.
5. Pass a separate disposable canary covering packaging/imports, CLI, real SDK credentials, authenticated S3 reads/publication as required, original exit status and cleanup. Declare all mocked operations; canary output carries no performance claim.
6. Only then freeze the exact source, binary, configuration, measured resource envelope and cold protocol for the full 1,000-query S3 run. Record startup separately, recall, p90/p95/p99, serial QPS, CPU/RSS, physical GET/bytes and lifecycle costs. Application caches stay off; uncontrolled S3 backend cache state remains disclosed.

Any later paid execution needs its own bounded Spot protocol, interruption disposition, terminal artifact sync and immediate compute termination. **This consultation supplies no launch authority.**

The historical A/B decision remains unchanged: A retained; B opt-in. Do not repeat the completed 100k S3 Vectors measurement. A 1M BORSUK result cannot establish parity with that 100k reference or Turbopuffer’s published 10M numbers. Turbopuffer remains a dataset-family cold target with unmatched IDs, split, region and load. The current profile also stops at 1M; progression to 10M requires separate generic library qualification.

Additional evidence: [qualification protocol](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/cohere1024/scale-maintenance-qualification-a0001/protocol.json), [candidate resource contract](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/cohere1024/scale-maintenance-qualification-a0001/candidate-contract.json), [closed failure disposition](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/cohere1024/scale-maintenance-qualification-a0001/closed/failure-disposition.json), and [pending measurement prerequisites](/data/target/borsuk-cold-membership-native/scale-maintenance-qualification-a0001/next-measurement-prerequisites.pending.md).

No files changed, native execution performed, data opened, network accessed, or workers launched. Additional shard availability and every proposed 1M runtime envelope remain unverified.
