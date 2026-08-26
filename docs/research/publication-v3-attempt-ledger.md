# Publication V3 attempt ledger — 2026-08-25

This ledger records terminal Publication V3 attempts, including attempts that
cannot contribute to a publication claim. It prevents outcome-based selection
and preserves the exact boundary that must be repaired before another paid
attempt. Terminal marker and receipt objects are authoritative; a locally
observed process exit is not a substitute for those objects.

No artifact from a failed runtime attempt is merged into another attempt. A
completed cell remains bound to its source, manifest, protocol, build receipt,
arm, and attempt identity. A partial campaign does not authorize a comparative
or aggregate publication claim.

## Standard ANN read campaign

The first scheduled BORSUK cell of `standard-ann-read` used cell identity
`r01-f4b9090272773676be960bbd` and one immutable build:

- source archive SHA-256:
  `7fdcac7f3564265048e366cf4ca76fa4268aacb910d0bf53b3869515e82b718a`;
- manifest SHA-256:
  `7cba62c8cb04a04e5b42c4a324dc14d7cd65800a7482beadb8fe3375d61db3cb`;
- build protocol SHA-256:
  `1337e9a2d0eaa99c5c42c2f1bc8a591c5323e1f6cc32f6a93fb07690d6cf6cba`;
- binary SHA-256:
  `2e2e9ee5e006ccce4f6d487780ebd3d1aa0020913746f923ff23f00c81db985f`;
- index URI:
  `s3://borsuk-bench-453182569524-euc1/publication/v3/20260812/indexes/build-attempts/0001/index-22ed55815e0f56c4daed6911`.

All attempts used EC2 Spot in AWS account `453182569524`. The controller
terminated each instance after its terminal marker; no campaign instance was
left running.

| Role / arm | Attempt | Instance | Terminal state | Terminal-marker SHA-256 | Boundary |
|---|---:|---|---|---|---|
| build | 1 | `i-003d0d872898399c6` | complete | `4992ce9f1f92bacacd00c88d5e2f8c935e899aea79db72f2d0720e32ae197e7f` | Immutable BORSUK index build completed and published its receipt. |
| recall arm 0 (`cold`) | 1 | `i-0879428218d03f90c` | complete | `f103648ae3858bf6ccce3cf2868dbad625c13fc8005914127e0074c10cf21d3e` | The exact cell passed result validation and published `RESULT_COMPLETE.json`. |
| recall arm 1 (`warm`) | 1 | `i-045d20cea081877e1` | failed | `0bee75e35f70ff35cb207f78f1d7eebf2d270a18f8f63ccad10f2b59de4d351b` | The disk-cached guard rejected query 20 after two backing GET/HEAD operations. The runtime failed closed before a result could be published. |

The terminal marker prefixes are:

- build:
  `s3://borsuk-bench-453182569524-euc1/publication/v3/20260812/results/r01-f4b9090272773676be960bbd/build/attempts/0001/`;
- cold runtime:
  `s3://borsuk-bench-453182569524-euc1/publication/v3/20260812/results/r01-f4b9090272773676be960bbd/runtime-recall/arms/0000/attempts/0001/`;
- warm runtime:
  `s3://borsuk-bench-453182569524-euc1/publication/v3/20260812/results/r01-f4b9090272773676be960bbd/runtime-recall/arms/0001/attempts/0001/`.

### Completed cold-cell evidence

The cold cell measured 1,000 queries and is valid evidence for that exact cell,
but not for a completed campaign or third-party comparison:

| Metric | Result |
|---|---:|
| recall@10 | 97.41% |
| latency p50 | 143.564 ms |
| latency p95 | 238.364 ms |
| latency p99 | 332.367 ms |
| throughput | 6.475 queries/s |
| peak RSS | 1,189,474,304 bytes |
| backing bytes read | 27,098,790,672 bytes |
| backing GETs | 54,625 |
| global leaf code requests | 42,887 |
| global leaf exact requests | 11,738 |

The cell used a 2 GiB RAM budget, no disk cache, S3 GET concurrency 64,
leaf-read width 32, 48 maximum in-flight leaf reads, four active searches, and
exact-read physical amplification 2. Its result object is bound to index
receipt SHA-256
`54c01a9131905f36001f517e3011cd9691f0447e99b2bb7c4ec0ac65fc8ee08a`.

### Warm failure decision

The failure is a benchmark-methodology defect, not a warm performance result.
The disk directory was cleared between bounded query cohorts while the same
open serving handle retained decoded and code-plane RAM state. Priming a later
cohort could therefore be satisfied by RAM without recreating its disk-cache
objects; subsequent RAM eviction allowed the measured query to reach S3. This
explains why the first cohort passed and the next cohort failed at query 20.

The failed attempt authorizes no latency, throughput, cost, or comparison
claim. A replacement attempt must use a frozen repair that makes the disk cache
the only state shared between priming and measurement, preserves the immutable
attempt numbering, and rebuilds under the repaired source authority.

## Repaired cache-isolation rerun

The replacement cell used source commit
`23aa21506dd81c8ca0fddaeaf439ed2342741db9` and cell identity
`r01-ab690b34e46e4c84ad4d130e`. Its immutable authority is:

- source archive SHA-256:
  `c290aec5c4a0fa85dc7e4ec46dad5f29f927970652e7b4fe2b29ec052677d509`;
- manifest SHA-256:
  `bc61b3512ce9f35e57763fa787745956743bb6129a9796a9069ccff6b9608978`;
- build/runtime protocol SHA-256:
  `a8331947b6eebe89f0655e45cac55bc669af8d8b43b573d233268077a1844dbb`;
- binary SHA-256:
  `9f799dd127e970563ffc75639d5a719ae36813103ff4dde72492002bda8769b0`;
- index URI:
  `s3://borsuk-bench-453182569524-euc1/publication/v3/20260812/indexes/build-attempts/0001/index-19cb5ca5b93d5b21193ceb96`.

Every completed job used EC2 Spot. The first warm-concurrency controller launch
exhausted its three SDK `RunInstances` retries with
`InsufficientInstanceCapacity`, all under one client token and before instance
creation. A second controller launch reused the same immutable attempt and
idempotent client token, then acquired Spot capacity. No On-Demand exception,
replacement attempt, or partial artifact was used.

| Role / arm | Attempt | Instance | Terminal state | Terminal-marker SHA-256 |
|---|---:|---|---|---|
| build | 1 | `i-07bd9f94687a34281` | complete | `cda89a6ca8eb3a34b4bc26e8d1f972cc171deda68071e678a3a408de17cacbb1` |
| recall arm 0 (`cold`) | 1 | `i-068df0b467258ef26` | complete | `b4a0937710b2b32118d151fa6d3fe406624f09d3840d20694a1e9a75f169709f` |
| recall arm 1 (`warm`) | 1 | `i-0f8abb768c2778fe3` | complete | `2ebb41488c0270d38c5ba256a67b60e03b1bdf6565e5c9ba8a6175ccf65c081b` |
| concurrency arm 0 (`cold`) | 1 | `i-021fc150346bbc82f` | complete | `64f286f8e2ffa4c51948a5b94bc65e9107f9ba531dacd0908195bf88ec06af8b` |
| concurrency arm 1 (`warm`) | 1 | `i-047917fe606d34827` | complete | `f91f34c0db951f7fc79d7ab71d96e86b4432506a1949322b059a1c3760bb11f6` |

The controller terminated every instance after its terminal marker. The
replacement read artifacts are `publishable:true`, share the exact build
receipt SHA-256
`2667d679bf8d31d205b587659475238a17d17692348ec7b5d5f4df45394d5e63`,
and report the following result boundary:

| Cache state | Recall@10 | p50 | p95 | p99 | Single-query throughput | Peak RSS | Measured backing I/O |
|---|---:|---:|---:|---:|---:|---:|---:|
| cold | 99.04% | 54.316 ms | 99.679 ms | 118.535 ms | 17.002 qps | 208,474,112 bytes | 9,345 GETs / 612,487,936 bytes |
| warm | 99.04% | 8.060 ms | 10.349 ms | 11.381 ms | 121.830 qps | 219,058,176 bytes | zero GETs / zero bytes |

The concurrency sweep preserved 99.04% recall for every row:

| Cache state | Workers | Throughput | p50 | p95 | p99 |
|---|---:|---:|---:|---:|---:|
| cold | 1 | 25.599 qps | 37.556 ms | 54.982 ms | 68.582 ms |
| cold | 2 | 49.283 qps | 38.218 ms | 56.226 ms | 70.575 ms |
| cold | 4 | 94.675 qps | 39.752 ms | 59.110 ms | 71.949 ms |
| cold | 8 | 154.049 qps | 49.000 ms | 69.141 ms | 90.751 ms |
| cold | 16 | 161.415 qps | 96.190 ms | 117.074 ms | 130.448 ms |
| warm | 1 | 119.494 qps | 8.224 ms | 10.484 ms | 11.522 ms |
| warm | 2 | 153.394 qps | 12.822 ms | 16.180 ms | 18.468 ms |
| warm | 4 | 170.896 qps | 21.593 ms | 34.431 ms | 40.284 ms |
| warm | 8 | 174.750 qps | 40.765 ms | 58.539 ms | 66.668 ms |
| warm | 16 | 174.243 qps | 62.977 ms | 91.303 ms | 101.462 ms |

Cold concurrency peaked at 262,488,064 bytes RSS; warm concurrency peaked at
252,170,240 bytes RSS. Both runtime attestations report zero swap, zero OOM
events, and zero OOM-kill events. Every warm measurement row reports zero
backing GETs and bytes; its measured bytes are explicitly separated into
decoded-RAM and local-disk tiers.

## Final-source claim-free lifecycle scaling slice

The preregistered `cohere-medium-1m-768` claim-free, 1,024-record-batch
scaling slice used source commit
`c59eee820dc7eb9a84b638a8fbbc7483947c3c62` and cell identity
`r01-0a2d474957bd3e8c3c8cbdf1`. The exact immutable authority was:

- source archive SHA-256:
  `15569c307c12758cd3532a0ede3a3389d61dfb3267e9092c587a3e7cd020ab8e`;
- manifest SHA-256:
  `40efedb07d423d75574817c9f12d14f161ab0f3eda7da9a6921d1fd295d3ec89`;
- build/runtime protocol SHA-256:
  `5b170defb3b3df03345ce9006ed741028ee6a4720ecacd7d98ef06c60710437f`;
- benchmark binary SHA-256:
  `1f33973c5e21fea274a9e92b2b764feffc5a8a970489d7e94ddba578c0c76f43`;
- REST benchmark binary SHA-256:
  `67960ff6f29b95316468bf9b2c0b39c228a99fc71e4f1a7800a0da63ec8026fe`;
- index receipt SHA-256:
  `dcd29eb344f4e89f7407b8b9c4a217e35f16364312837d60b9d408c6809d18e1`;
- index URI:
  `s3://borsuk-bench-453182569524-euc1/publication/v3/20260812/indexes/build-attempts/0001/index-debd4f8777d4a6fb2219200e`.

Every job used EC2 Spot. The controller confirmed termination of every
instance after its terminal marker. At the end of the slice, no BORSUK
benchmark instance remained pending, running, stopping, or shutting down.
The runtime was a `c7g.xlarge` with four vCPUs and 8 GiB of memory. It used
three benchmark CPU threads, a 2 GiB BORSUK RAM budget, no disk cache, S3 GET
concurrency 64, leaf-read width 32, and at most 48 in-flight leaf reads.

| Role / arm | Attempt | Instance | Terminal state | Terminal-marker SHA-256 | Result/receipt SHA-256 |
|---|---:|---|---|---|---|
| build | 1 | `i-0fe025a3c8800440c` | complete | `e553e0427e6678efcbf10e37c2d0e031dfbdbc849dc831ee83f6357f3be88ec5` | `58e89279e7918efaea0a3975b26a34931dda2c5ba85d52f7ace3679cf9595e7f` |
| lifecycle arm 11 (1 writer) | 1 | `i-05070f8c6a3fc01e5` | complete | `2eeed1bbd70f25f4379bfcebf263dc1d198cf31cb423df8c1626a2281103bba1` | `e901e67075ec0db24da3377a486fcd3deb1b2bf484cf8aa13f266816c7fd93fd` |
| lifecycle arm 14 (4 writers) | 1 | `i-055ca9644d9733dc9` | failed | `a6e185a588c3efecfab9edc9cad4e2231c531360d91b4e9dea30a225f55ecacf` | not published |
| lifecycle arm 14 (4 writers) | 2 | `i-041c5204609953251` | complete | `77ecb5d0cb652beb3c0a57c505106580cb5eb5790eb7b45eb0d7a8a60a736dbe` | `872cdad02f2d18e94269f50df8466fb14114005ed84f4eac87029508cf8024f1` |
| lifecycle arm 17 (16 writers) | 1 | `i-01cc1b49a65c3b12a` | complete | `7eb00256621400474182dd304015235a5a6a32539cf3b190cf1d6398dd5de69f` | `07e5a0059d3235a53b2b6d714190ad40d207940d79dc4d9a295cf634dce6f538` |

The mutable clone receipt SHA-256 values were
`ad89981b4be8cc2e490498d3487e6d1cc3314dbe1168fac9e570f26d53c8cab9`
(arm 11 attempt 1),
`24d282c799a4203e7866bc5dc0350b3c6cb7f3a59fc3192896e8c52d8f4f263f`
(arm 14 attempt 1),
`766c2db3ef99063357bb8e4058a38a4f63cbbb4653739249c6c98cf457839c89`
(arm 14 attempt 2), and
`9f1316125503da8ea3148e201627fda3c00025d90e0ca42730472e3cc88084eb`
(arm 17 attempt 1). Terminal objects are below the common prefix
`s3://borsuk-bench-453182569524-euc1/publication/v3/20260812/results/r01-0a2d474957bd3e8c3c8cbdf1/` at
`build/attempts/0001/` and
`runtime-lifecycle/arms/{0011,0014,0017}/attempts/NNNN/`.

Arm 14 attempt 1 reached `publish-receipts` and then published only an
80-byte terminal failure document plus its bounded failure log. It published
no `RESULT_COMPLETE.json`; the log contained no BORSUK product traceback and
EC2 reported neither an interruption nor an impairment. The attempt therefore
contributes no performance observation. The controller advanced explicitly to
attempt 2 under the same immutable build and source authority, which completed.
The exact cause remains unknown. Before the remaining lifecycle campaign, the
receipt publisher and failure reporter therefore required a focused robustness
repair. Source commit `5fcec05dc59cc952d23e4077a9874dfd9b7ebe7f` adds
three bounded upload attempts, S3-computed-checksum reconciliation for an
ambiguous conditional PUT, a shared ten-minute publication-stage deadline,
bounded aggregate error preservation, and a durable reconciliation count in
versioned terminal receipts. Foreign-checksum conflicts still fail closed.

Each completed arm inserted 19,859 new rows, flushed and consolidated that
inserted delta, then performed 1,986 upserts and 1,986 deletes before compaction
and purge. The insert count is the exact dimension-dependent maintenance-free
envelope after reserving the subsequent upserts. `Mutation throughput` below is
the published 23,831 insert + upsert + delete operations divided by the sum of
those three mutation-phase wall times. It excludes flush, consolidation,
compaction, purge, refresh, and verification time.

| Writers | Mutation throughput | Speedup vs 1 writer | Batch p50 | Batch p95 | Batch p99 | Insert searchable | Insert fully indexed | Insert consolidated | Correctness gate | Peak process RSS |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 3,127.093 ops/s | 1.000x | 221.862 ms | 855.643 ms | 938.739 ms | 9.348 s | 22.597 s | 301.624 s | pass (100%) | 2,153,451,520 bytes |
| 4 | 7,373.362 ops/s | 2.358x | 378.668 ms | 465.864 ms | 704.620 ms | 7.003 s | 20.188 s | 298.395 s | pass (100%) | 2,449,399,808 bytes |
| 16 | 8,800.992 ops/s | 2.814x | 308.603 ms | 1,350.634 ms | 1,377.983 ms | 6.904 s | 20.071 s | 306.986 s | pass (100%) | 3,028,852,736 bytes |

The three insert milestone columns are reported phase-duration sums rather
than end-to-end wall-clock measurements: searchable is insert publication plus
refresh, fully indexed adds delta flush, and consolidated adds consolidation.
They exclude the interleaved verification and query stages and occur before
the later upsert/delete/compact/purge phases.

The correctness value is a publication gate, not a statistically graded
accuracy estimate: any value below 100% would fail the arm. The benchmark
sampled 16 inserted, updated, and deleted IDs at one and four writers, and 32
at sixteen writers, then repeated the survival/deletion checks after compaction
and purge. All sampled identities passed. The query stages additionally
completed and were bound into the terminal receipt as separate artifacts.

The configured 1,024 records is a maximum batch size. Insert used 20 batches;
at 16 writers its final wave contained only four participating writers.
Upserts and deletes used two batches at one writer, four balanced batches at
four writers, and sixteen balanced 124--125-record batches at sixteen writers.
The pooled batch percentiles therefore are useful within an arm but are not a
like-for-like cross-arm latency comparison.

The four-writer point delivered 2.358x the one-writer mutation-phase throughput
at 58.9% parallel efficiency. Sixteen writers delivered the highest measured
mutation-phase throughput, but only 1.194x the four-writer result and 17.6%
parallel efficiency. The reported insert-consolidation milestones were
301.624, 298.395, and 306.986 seconds: consolidation dominated the observed
slice, four writers improved that milestone by only 1.1% over one writer, and
sixteen writers was slowest. Each arm is a single measurement without a
dispersion estimate. These data establish diminishing mutation-phase scaling
on this four-vCPU host, but do not yet justify freezing an operating writer
count.

Published write-amplification lower bounds remained stable at 2.425--2.427x.
They include WAL publication plus indexed-delta bytes but exclude the later
consolidation bytes. The completed arms reported zero swap bytes, zero OOM
events, and zero OOM-kill events. Their runtime cgroup memory peaks were
7,508,709,376, 7,506,055,168, and 7,695,392,768 bytes respectively under the
8 GiB instance limit. The separate 2 GiB BORSUK RAM budget bounds index state,
not whole-process RSS or clone/setup activity. These are publishable results
for the exact three-arm BORSUK lifecycle slice only. They do not complete the
manifest's other lifecycle factors or authorize a cross-system product
comparison.
