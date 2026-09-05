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

## Repaired-source paired lifecycle insert-mode slice

After the receipt-publication repair, one preregistered four-writer,
1,024-record-batch cell was run for each lifecycle insert mode on the same
immutable `cohere-medium-1m-768` build. The source revision was
`ba4d55b9037a6f04e24be56b272143d68e29e69f` and the cell identity was
`r01-feaba6b65dfaab744e5c8b6a`. Its authority was:

- source archive SHA-256:
  `ff730871cebe7b482b9efcca3d0bac95be86b13cb6603bdc459847251f7a624a`;
- manifest SHA-256:
  `c2e8a6b7b31f31bcafcf4f7159260323dba36a37428eb7627c7aebe85226905f`;
- build/runtime protocol SHA-256:
  `2e23f164080b9775698a3968afe72ed6c6e68e5ada0e2346d5677469fe748809`;
- benchmark binary SHA-256:
  `1f33973c5e21fea274a9e92b2b764feffc5a8a970489d7e94ddba578c0c76f43`;
- REST benchmark binary SHA-256:
  `67960ff6f29b95316468bf9b2c0b39c228a99fc71e4f1a7800a0da63ec8026fe`;
- index receipt SHA-256:
  `4c6a1b679bdb203c6999770dd9e1285c986cd98b2ec9b9a9d1abbeaf95afe0bd`;
- index URI:
  `s3://borsuk-bench-453182569524-euc1/publication/v3/20260812/indexes/build-attempts/0001/index-48db0648b72ffef43ca13ee9`.

All three jobs used EC2 Spot and completed under the repaired persistent
terminal schemas: build schema 2 and runtime schema 5. Every terminal receipt
reported `artifact_upload_reconciliations:0`. The controller confirmed that
each instance terminated after its terminal marker, and no campaign instance
remained pending, running, stopping, or shutting down.

Both runtime cells used a `c7g.xlarge` with four vCPUs and 8 GiB of memory,
three benchmark CPU threads, a 2 GiB BORSUK RAM budget, no disk cache, S3 GET
concurrency 64, leaf-read width 32, at most 48 in-flight leaf reads, and
exact-read physical amplification 2. The shared build in this section is not
the different immutable build used by the preceding scaling slice.

| Role / arm | Attempt | Instance | Terminal-marker SHA-256 | Result/receipt SHA-256 |
|---|---:|---|---|---|
| build | 1 | `i-0aba82ea16cdc125f` | `d8d1be85bf72ae694e44cc5c7bfc55d197294113771f52a7f6a58e80a7dcdd27` | `4c6a1b679bdb203c6999770dd9e1285c986cd98b2ec9b9a9d1abbeaf95afe0bd` |
| arm 5 (`general-upsert`, 4 writers) | 1 | `i-005de724a39565e87` | `19a79679c543dd6c1acf564dd066f471a048a55cb5202bfec24852e83cec2971` | `ebcfb82f0134e718a814248c05eff4c5e47b681060228a77461d77bd6962b5e5` |
| arm 14 (`claim-free-put`, 4 writers) | 1 | `i-09d3c24e19308b44e` | `2266d95fe6a93cdd18f2e0da65bc1a103a905aa84b52ccf9b2f9e68852b705eb` | `76f2f12bd14e6b7f0e6aa3fce4ec52c4f98862ea2e581420ac6888ff4c9ae6e2` |

The terminal objects are below
`s3://borsuk-bench-453182569524-euc1/publication/v3/20260812/results/r01-feaba6b65dfaab744e5c8b6a/` at
`build/attempts/0001/` and
`runtime-lifecycle/arms/{0005,0014}/attempts/0001/`. The mutable clone receipt
SHA-256 values were
`e32baa50c9dd70c51ab1681e049f70990a975a0eb6c157d99ca52d350fa3a57a`
and
`f75043388979e91518291cddf934216b57bf2ca3b9f9e84e48f9c468b8a5023a`
respectively.

Both cells inserted 19,859 rows, then performed 1,986 upserts and 1,986
deletes, and passed the exact 100% sampled lifecycle correctness gate. The
throughput definition is the same 23,831 insert + upsert + delete operations
divided by the three mutation-phase wall times; it excludes maintenance,
verification, and query time.

| Insert mode | Mutation throughput | Batch p50 | Batch p95 | Batch p99 | Insert searchable | Insert fully indexed | Insert consolidated | Correctness | Peak process RSS |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `general-upsert` | 8,080.313 ops/s | 403.211 ms | 455.257 ms | 459.668 ms | 6.988 s | 19.964 s | 296.998 s | pass (100%) | 2,470,227,968 bytes |
| `claim-free-put` | 7,826.255 ops/s | 386.281 ms | 455.491 ms | 477.178 ms | 6.923 s | 19.886 s | 299.891 s | pass (100%) | 2,415,534,080 bytes |

Relative to `general-upsert`, the claim-free cell was 3.144% slower in
mutation throughput. It improved batch p50 by 4.199%, was effectively tied at
p95 (+0.051%), worsened p99 by 3.809%, reduced peak process RSS by 2.214%, and
took 0.974% longer to reach the reported consolidated milestone. Storage
traffic was effectively identical: claim-free issued 13 fewer GETs and three
more PUTs, and wrote only 0.000219% more total traced storage bytes. That total
campaign-storage scope is broader than the published write-amplification
numerator, which includes WAL plus indexed-delta bytes but excludes later
consolidation bytes. The write-amplification lower bounds were 2.425604x and
2.426313x.

The runtime cgroup memory peaks were 7,514,591,232 and 7,495,815,168 bytes.
Both attestations report zero swap bytes, zero OOM events, and zero OOM-kill
events. This section has one observation per insert mode under its exact
authority, with no dispersion estimate. The earlier claim-free scaling slice
reported 7,373.362 ops/s at four writers, 6.142% below this section's
7,826.255 ops/s, but it used another source archive, protocol, index build, and
receipt schema. Pooling those points would be invalid; their spread also shows
why the 3.144% paired gap cannot yet be treated as a stable effect size.

The result does not establish a universal ordering or justify a product-level
performance claim. In this exact paired cell, however, claim-free coordination
did not deliver a mutation-throughput advantage. The evidence therefore does
not justify freezing `claim-free-put` as the production default; the existing
`general-upsert` path remains the conservative default until repeated and
broader lifecycle evidence says otherwise.

## Standard read campaign restart on 2026-08-26

The first `deep-image-96` cell of the restarted standard read campaign used
cell `r01-ff1ce5a82f2e8965c5251cf1`, source archive SHA-256
`7949727eaac8229b2cdcc9210432953e9b8adaa43fe050db136fb3fd38d79e75`,
manifest SHA-256
`1566f0a23fe3d2497be44ee6d845b92d10741128f7e1dff9d89adca6a0676117`,
and protocol SHA-256
`a92273cccf108d6b52c009f86be876c31ffc4d34041fa33988d47e707e42ca4b`.
The immutable build completed on Spot instance `i-029bff19e92295841` at
`index-79e971700cc1deab01e500df`; its terminal-marker SHA-256 was
`8fe3c818955c452750a5fb787bc39c0541d7cc55fefa69e0c49ed502724f5c87`.

Cold arm 0 completed on Spot instance `i-0987913bfa9bc9e5f`; terminal-marker
SHA-256 was
`e7b8ad02777cfdda3f785de0123e222829b431376037c741d1ab9c5594a92503`.
It measured 1,000 queries at 97.41% recall@10, with p50/p95/p99 latency
238.845/309.538/345.944 ms, 4.155 queries/s, peak RSS 793,575,424 bytes,
58,642 backing GETs, and 28,481,498,592 backing bytes. This is valid evidence
for that exact source-bound cold cell only; it is not pooled with a replacement
source or presented as a completed campaign.

Warm arm 1 failed closed on Spot instance `i-0b8ce6ea81d59d9cb`. Its terminal
marker SHA-256 was
`0bee75e35f70ff35cb207f78f1d7eebf2d270a18f8f63ccad10f2b59de4d351b`;
no result was published. After priming a 20-query cohort, measured query 88
issued one backing GET. The fixed 32 MiB-per-query estimate had not proved
that all 20 data-dependent working sets would remain in the 1 GiB disk cache.
The runtime therefore correctly rejected the arm rather than mislabeling it
warm.

The replacement methodology resets the disk cache, prepares one handle, clears
the disk-resident product of excluded startup while deliberately retaining
RAM-resident serving metadata, and primes all 1,000 registered queries once.
Recall clears query-populated decoded state before each measured query;
concurrency clears it before each steady worker profile. This avoids setup
pollution, cross-query eviction, and repeated expensive index opens. The BORSUK
runtime now
uses a dedicated 96 GiB volume and a 64 GiB cache; only 75% is admissible at a
conservative 48 MiB per query, giving authority for 1,024 queries and leaving
16 GiB explicit headroom. Because this changes the frozen source, storage, and
cache-cohort protocol, the completed cold cell above is historical evidence and
the replacement campaign must rebuild and rerun both arms under the new
authority.

The replacement concurrency run primes once and measures one 1,000-query wave
per worker profile. Its QPS is not comparable to any earlier worker-sized,
multi-wave diagnostic, whose repeated spawn/barrier overhead dominated the
measurement; only the replacement campaign is publication-eligible.

## Full-query-set cache restart on 2026-08-26

The next standard-read restart used source commit
`7334c5299f0649c6a74262b0e7f92fc645f29505`, cell
`r01-1a02288fe99c832240fe6037`, and the following immutable authority:

- source archive SHA-256:
  `9c9b11832c7525dcaf0c2f3dc5d014afa65bee6968e458b7ec90f558194804ad`;
- manifest SHA-256:
  `c74df8d4d392164e02680b9142cc57123c66cf07c511e53d6082e34c70f8fd9e`;
- build/runtime protocol SHA-256:
  `9cfe4716f828f7aa18c6eed0df765b2716614a58664c214956b8b396257fb8bc`;
- binary SHA-256:
  `c56a29b6aed7e7a9ef40426ef210da8dd0c35af57e86ad25f590db6701a679bb`;
- index URI:
  `s3://borsuk-bench-453182569524-euc1/publication/v3/20260812/indexes/build-attempts/0001/index-6e3e9756f5c66f82a2fb1c24`.

Every attempt used Spot and the controller terminated every instance. The build
completed on `i-028912a14239bc11f`; its terminal-marker SHA-256 was
`e4cc7665faeef269e954255812da80786b958751f4837885ccd012f7a5154f47`.
Cold arm 0 completed on `i-0f30af4df4b15f8f5`; its terminal-marker SHA-256
was `9fd63199c6a3c5efaa72f0e4f02198ff42ea02e33008bfc0c9215bf7ca2ddbdc`
and its result SHA-256 was
`23a187ad75225b98dd0639b5a5e77683052efc5a137b6e3ffe9913f15164d414`.
It measured 1,000 queries at 97.41% recall@10, p50/p95/p99 latency
227.895/292.266/331.762 ms, 4.322 queries/s, peak RSS 793,972,736 bytes,
58,642 backing GETs, and 28,481,498,592 backing bytes. This is valid evidence
only for that exact cold cell.

Warm arm 1 failed closed on `i-0fb7e55eaeee6301c`; its terminal-marker
SHA-256 was
`0bee75e35f70ff35cb207f78f1d7eebf2d270a18f8f63ccad10f2b59de4d351b`.
No result was published. Opening and preparing the handle first admitted
65,146,500,000 bytes of excluded setup traffic into the 64 GiB read-through
cache. Priming the complete query set then evicted early query objects, and
measured query 3 correctly failed the zero-backing-I/O guard after three GETs.

The repaired methodology keeps RAM-resident prepared metadata but clears the
disk-resident product of open immediately before query priming. This makes the
complete primed query set, rather than excluded setup objects, the sole
disk-resident measurement cohort. Because the repair changes source authority,
the completed cold cell above remains immutable historical evidence; both arms
must rebuild and rerun under the repaired source.

## Per-query primer-state restart on 2026-08-26

The next standard-read restart used source commit
`483a9aba7428206ca941f266ac87b3a5b6d39100`, cell
`r01-1d33e3e3d8cf2b48184f8f90`, and the following immutable authority:

- source archive SHA-256:
  `ac6ba200169225ed63748a49a9caf220918d9ed1ce6c02486b549abeef7f1379`;
- manifest SHA-256:
  `bb3bce356acff9617c9738982e2468883ae155d3cad78f4dd33d9ff700b818db`;
- build/runtime protocol SHA-256:
  `76fa6bcfb0cbb0ca628cc08906a12fa7d68d6305ecda2c4244ac06bfbb86ffbb`;
- benchmark binary SHA-256:
  `118a6c890d9ae52dba944d96029358a6e09cf1f1a8c78224468a2390d885ef26`;
- REST benchmark binary SHA-256:
  `286bae811ff008ca5157db985fb7147984ce78b916160f4ea4a6633f7853860d`;
- index URI:
  `s3://borsuk-bench-453182569524-euc1/publication/v3/20260812/indexes/build-attempts/0001/index-2091599d38cd90eff99eeb8a`.

The build completed on Spot instance `i-0f99cb9114a95bc81`. Its terminal-marker
SHA-256 was
`9c6476b6a226eb38b15d1d2830bf8409e11a173ff04189813412890d6fee398c`.
The first two runtime launch calls in `eu-central-1a` failed before an instance
existed with EC2 `InsufficientInstanceCapacity`. Retrying the same immutable
cell in the campaign VPC's `eu-central-1b` subnet acquired Spot capacity; no
On-Demand exception was used.

Cold arm 0 completed on `i-03cc0519b0df2dccc`. Its terminal-marker SHA-256
was `61458a0d4f3067fc9158a8f37755a0303ec4b21be67dadbe50985ed32c51a8c5`
and its result SHA-256 was
`609d340fa71a1fa96bc90f7cac5f1229a6ad3814f0cd29949afc4c6c88799182`.
It measured 1,000 queries at 97.41% recall@10, p50/p95/p99 latency
236.047/316.108/361.812 ms, 4.172 queries/s, peak RSS 781,037,568 bytes,
58,642 backing GETs, and 28,481,498,592 backing bytes. This remains valid
evidence only for that exact cold cell.

Warm arm 1 failed closed on `i-01a37c21cde1f0753`; no result was published.
Its terminal-marker SHA-256 was
`0bee75e35f70ff35cb207f78f1d7eebf2d270a18f8f63ccad10f2b59de4d351b`
and the failure-log SHA-256 was
`dd9f5384af18ac28a653e0f0846dc9830fa8642fafa44ddc179f7c24dbee0d75`.
Measured query 3 issued three backing GETs even though the read-through cache
had been cleared immediately after open and the complete query set had been
primed.

The second failure isolated a different state boundary. Primers ran in one
sequence while decoded query state accumulated in RAM. A later query could
therefore be satisfied by a prior primer's retained code plane without writing
every disk key that the later measured query needed after its mandatory RAM
clear. The replacement lifecycle clears query-retained RAM before every
primer, while preserving the disk tier across the complete cohort, and clears
it again before every measurement. A regression reproduces the former failure:
without the pre-primer clear, query 1 inherits retained RAM, omits its disk
authority, and then performs a backing GET after RAM is cleared. The replacement
source must rebuild and rerun both arms; the cold result above is not pooled
with it.

Before another paid launch, the repaired lifecycle was exercised through the
real `production_bench` binary against a deterministic local
`synthetic-clustered-v1` index (20,000 rows, 96 dimensions, 100 queries, 64
logical cells, nprobe 32, candidate budget 512, 128 MiB RAM authority, and an
8 GiB disk-cache authority). The disk-cached recall run emitted 100 measured
samples with zero backing GETs/bytes and at least four disk-cache reads per
sample. A separate disk-cached concurrency run emitted 300 samples across the
registered 1/2/4-worker profiles with the same zero-backing and positive-disk
invariants. This is non-publication diagnostic evidence: it proves the repaired
primer/measurement state transition on the production path, but it is not a
performance result and does not replace the required paid immutable rerun.

## V22 Deep Image 10M Stage-L layout census on 2026-08-28

The claim-ineligible V22 census used a freshly rebuilt, unreleased-format
Deep Image index rather than adding a V12 compatibility reader. The build ran
on Spot instance `i-005ae9b51afe36680` and published
`index-bcda7bb66812e162d45077e6`. Its terminal-marker SHA-256 was
`b55959a6cb3f2478557606050fe22b52059b4254c9dbdbf430ae9a45a55217e1`;
the index receipt SHA-256 was
`19dd0f5788625bb4c87adf5cd86a942be7addd797cf2ceb0651ea80476ca5189`.

The final diagnostic ran from source commit
`14464c8a36e3cf4551735e26ee1320cbd88f4bb6` on Spot instance
`i-085e81b0fc9885225`, under diagnostic cell
`r01-0754b82e49c729e5bd946ca0`. The controller terminated the instance after
completion. The immutable artifacts are under
`s3://borsuk-bench-453182569524-euc1/publication/v3/20260812/results/r01-0754b82e49c729e5bd946ca0/runtime-v22-stage-l/arms/0000/attempts/0001/`.
Their authenticated SHA-256 values are:

- terminal marker:
  `ad08b71b5f0c30e9b8d2d123d9d6eddac9153417d6efb49144a3bc37f6fb8ef0`;
- result receipt:
  `fa76d31a784edd53c0afdd1119959ea90b263538494688a677c87006cf8529bc`;
- complete layout report:
  `bc42a4f510d9341ea22cc7707e4fe246228644dbe14ba44028800a9953f1ab6b`;
- derived summary:
  `a5df4a1e0b6eedd253ea12096ac63826a4a6d1912729f3153f0b5fba1d2641c0`;
- execution contract:
  `fbf24381e4eba4696eb0e5cd4d5347baf67b54d82bc2d7b22448e1d5c7397cc4`.

The census covered all 9,990,000 indexed rows, all 4,096 routing cells, 32
frozen queries, seven layouts, and six exact-prefix sizes (42 arms). Routing
covered all ten ground-truth rows for every query. No layout satisfied the
registered combination of at most four primary exact GETs, at most 1 MiB of
physical exact bytes, at most 2x physical amplification, and at most 512,000
routed rows for every query.

For the best 10-row candidate (`semantic-within-cell`, 64-row
microclusters), six of 32 queries were eligible. Twenty-two queries were
request-limited and four were amplification-limited. The request distribution
ranged from two to ten (median seven; p95 and maximum ten), while physical
bytes ranged from 136,768 to 341,696 (median 202,688; p95 308,736). Thus the
small-prefix failure is request fan-out, not the 1 MiB byte ceiling. At 256
and 512 exact rows, 30 of 32 queries were byte-limited in every tested layout;
the worst physical payloads were about 7.4--8.5 MiB and 14--16 MiB. Repacking
and the tested semantic orders changed those bounds only marginally.

The evidence rejects V20 block scattering plus query-independent linear
repacking as the cold production architecture. It does not establish latency
or recall for a replacement format. The next qualified design must route a
query directly to a bounded number of semantically coherent exact-vector
pages, then prove the four-GET/1-MiB envelope before another publication build.

## V23 Deep Image 10M page-routing diagnosis on 2026-08-29

The first V23 diagnostic used source commit
`946386c3d914121b5bdb7fb2f2a016311bdb759c`, the immutable V22-built index
whose terminal-marker SHA-256 is
`b55959a6cb3f2478557606050fe22b52059b4254c9dbdbf430ae9a45a55217e1`, and
Standard S3 only. D1 completed on Spot instance `i-0f851a76d0d2369b1` and D2
completed on Spot instance `i-00421096ae5938591`; both instances were
terminated after their terminal marker. The D1 result SHA-256 was
`5266ef3cb60d4bed2db8c04777897084d867da7eb280f3c082d87743b7a2612e` and
the D2 result SHA-256 was
`311dca58e568404195647fb88770b27715fd248cd91884aedcc289855b7f4121`.

D1 established that the 192-byte f16-flat codec preserves routed ranking at
99.6875% recall with 0.587 ms CPU p99. It also rejected the original 16-byte
SRHT-PQ selector codec, whose routed recall was only 28.4375%. D2 then failed
closed before any D3 latency launch. Across the 384/512/640-row layout arms,
actual recall was 21.5625%, 25.625%, and 20.9375%; the corresponding
four-page coverage oracles were 81.5625%, 84.0625%, and 85.3125%. CPU p99 was
5.95--6.30 ms and projected 100M-row serving RAM was 648--724 MiB, so the
failure was page containment and selector quality rather than scoring cost or
memory.

An exact recomputation over the authenticated D2 truth assignments showed
that an eight-page oracle reaches 99.375% aggregate recall with 90% minimum
query recall for the 384- and 640-row layouts; the 512-row layout reaches
98.4375% aggregate and 80% minimum. The replacement unreleased format
therefore uses an eight-page, 1,966,080-byte maximum cold wave, 192-byte
f16-flat page representatives, and nearest-representative page ranking rather
than reciprocal-rank voting. Its selector, D1, D2, and D3 schemas are replaced
rather than retaining readers for the failed experimental artifacts. A fresh
D1 and D2 are required; D3 remains forbidden until D2 satisfies every frozen
recall, selector-regret, amplification, RAM, and CPU gate.

The replacement frontier excludes the 512-row layout because its authenticated
eight-page oracle is below both frozen coverage gates. It also excludes the
640-row layout: although its oracle matches 384, its observed worst eight-page
payload reaches the registered 1.875-MiB ceiling versus about 1.19 MiB for 384.
The registered 384-row layout projects about 1.45 GiB of serving RAM at 100M
rows, including the 192-byte selector representatives and two maximum waves.
These are design projections, not latency or throughput results. Fresh D2 must
measure selector regret and CPU p99 for that one latency-first layout before any
Standard-S3 D3 launch.

## V23 Revision-4 eight-page diagnosis on 2026-08-29

Revision 4 ran from source commit
`c59128ee68eb28beaa7f5eef7e0570dc7c787b88`, the same immutable base index,
and Standard S3 only. D1 completed and passed on Spot instance
`i-0ba5583f9b60ad3b6`; the controller terminated it immediately afterward.
Its terminal-marker SHA-256 was
`6d7e79e398a53eebf080309e9434f40f3021913e8137bbf733458cdfc0ece210`,
result SHA-256 was
`a32e9269fef037510ef843d8f8bb25d50f62d1c4c608fb270528fe7ed588556c`,
report SHA-256 was
`128a5d95c8f0e11ed6d58d6319de35196d8ab91dd253be518216657b57201c7c`,
and summary SHA-256 was
`5ea62511be3e5ea644ba68cdd9f2f78f8c605a8406ba60df6ae935209a35598e`.
Peak memory was 6,467,158,016 bytes with no swap.

D2 evaluated only the registered 384-row arm and completed on Spot instance
`i-0ce89225cf96cad55`; the controller terminated it after terminal
publication. Its immutable artifacts are under
`s3://borsuk-bench-453182569524-euc1/publication/v3/20260812/results/r01-f7a6e06a6a40c1165b6cb889/runtime-v23-d2/arms/0000/attempts/0001/`.
Their authenticated SHA-256 values are:

- terminal marker:
  `db12dd670ae5121fa4d90147fba7816d6a20878764a28d089be45be1138579ef`;
- result receipt:
  `41ec2b4eb9e0506f4732c2e0ff34d92e1493b24953669c486fc5714a38002a00`;
- complete D2 report:
  `665dc206d04073b8cbc0b8bab9e5645760440d2336ddf4bfebea81d176b4779d`;
- materialized page manifest:
  `dfa5759c06663655b4a963a7687b40c8bd8020bebf805d7c825a88c6d0df53e1`;
- derived summary:
  `f8b9341126e6bc70359d3dfceb46b405f97d5cff54c1ade1eefe1a0d1e823d1b`.

The eight-page 384-row layout passed its structural bounds and reproduced the
authenticated coverage oracle: 99.375% aggregate and 90% minimum-query
recall@10. Projected 100M-row serving RAM was 1,558,626,504 bytes, below the
3-GiB budget. The content selector nevertheless achieved only 26.5625%
aggregate recall with a zero minimum-query result, for 267,295 ppm selector
regret against the 995,000-ppm floor. CPU p99 was 15.307763 ms, narrowly above
the 15-ms gate. Peak build-worker memory was 17,743,450,112 bytes with no swap
or OOM.

This terminal result rejects sampled nearest-representative page routing. It
does not reject the 384-row eight-page layout: its oracle and RAM bounds passed.
The next design must route using the same geometry that constructs primary and
replica page membership, and must reduce rather than increase selector work.
D3 was not launched, so Revision 4 establishes no cold-latency or throughput
claim.

Two outcome-blind offline counterfactuals then isolated the selector boundary
without opening D3 or changing the immutable pages. A checksum-authenticated
scan of all 28,282 pages ranked their centroids against the same 32 frozen
queries. Primary-only centroids reached 68.75% aggregate recall, 30% minimum
query recall, and 691,823 ppm oracle attainment. Centroids over primary plus
replica contents reached 69.6875%, 20%, and 701,257 ppm respectively. The
counterfactual output SHA-256 was
`d003104bd60d3fa2192282ef6741f33c5fd5ab446e31e2a2811aeb27ed2d52e7`.
This rejects another page-summary selector: adding replica information to one
smooth summary per page improves the result but cannot distinguish sibling
leaves well enough.

The decisive row-granularity ceiling ran on Spot instance
`i-03fad085c482d6718` and authenticated all 18,620,111 primary and replica
assignments before scoring the production f16-flat distance for every query.
For each query it retained the exact f16 top ten rows, looked up both immutable
page assignments, and computed the best deterministic eight-page cover. The
result matched every oracle-reachable row: 99.375% aggregate recall, 90%
minimum-query recall, and 1,000,000 ppm selector-regret attainment (318 of 318
oracle hits). F16 top-ten row-identity recall was 99.6875%. The terminal marker
SHA-256 was
`6bf0e3cc2d160a49c7e3b42c19855578b7a256da33a56409a7847818636cbf05`;
the result SHA-256 was
`6c2cdbe6cc251ed950e7c0238c5a7bc5c20884d2b0a6db28f2ea844e1fe3d171`.
Artifacts are under
`s3://borsuk-bench-453182569524-euc1/research/v23-row-vote-f0/20260829T125701Z-v23-row-vote-f0/`.
The controller observed the instance terminating after terminal publication.

One preceding Spot attempt on `i-07f3db728368d9151` authenticated and scored
the complete assignment corpus but exited before aggregation because its
Python 3.9 runtime rejected a nonessential `zip(strict=True)` call. It
published no scientific result; its failure terminal SHA-256 was
`d1864d18dde02550b11478e1ae9cffdf1f505b343c92124171bfd962ea4bfed0`.
The replacement removed only that syntax and reran the identical algorithm.

The evidence therefore accepts the page layout and rejects page-level
selection. The next unreleased selector must identify rows with a compact
resident code, preserve both primary and replica page labels, and choose the
eight-page cover from those row candidates. A width ladder must now establish
the smallest code at or below the 12-byte-per-row 100M RAM boundary before the
format is frozen. These counterfactuals are architectural evidence, not cold
latency or throughput measurements; D3 remains forbidden.

## V23 BVS3 compact-selector diagnosis on 2026-08-30

The BVS3 diagnostic used source commit
`c339a546f8f9370cb2e6e9fb3b0fd4bdefa3cb05`, source-archive SHA-256
`77917b0f5621d2580fef444ee362669a39d01c8453bee1c10ca1823631117f6d`,
and manifest SHA-256
`9a4055750f62d8460a1f2b1e58ff318d7190e5ef07a46390de59a37342aad4b1`.
The immutable V22 base terminal remained
`s3://borsuk-bench-453182569524-euc1/publication/v3/20260812/results/r01-46d286fd1e2290c1cb8b8645/build/attempts/0001/BUILD_TERMINAL_COMPLETE.json`
with SHA-256
`b55959a6cb3f2478557606050fe22b52059b4254c9dbdbf430ae9a45a55217e1`.

D1 completed on Spot instance `i-014619e4787fa99c1`, and the controller
terminated the instance after publication. Its artifacts are under
`s3://borsuk-bench-453182569524-euc1/publication/v3/20260812/results/r01-6846520de9e7ffcfb93d5efd/runtime-v23-d1/arms/0000/attempts/0001/`.
The terminal-marker, result, report, and summary SHA-256 values were,
respectively,
`d547bf797507680f8946fd120592805fb32ce17b4d54c6847401d93b8ec22035`,
`217e2ffb057008940f4b185064b4169fb9e69a21740e3295b578cb1d6a784235`,
`91717a4077c8a7d6b909f1f8d14f59d6a6d422a29e06b3d665a02c29743cbc39`,
and
`4140bc045364a1e6ae660e80fe9323de532b52c84f532128c44947d67e7bbb48`.
The 8-byte PQ arm reached 103,125 ppm routed recall against a 125,000-ppm
oracle and 0.264186-ms CPU p99. The 12-byte PQ arm reached 243,750 ppm against
a 246,875-ppm oracle and 0.336145-ms CPU p99. Only the 192-byte f16-flat arm
passed, at 996,875 ppm routed and oracle recall with 1.290391-ms CPU p99. Peak
cgroup memory was 6,465,392,640 bytes with no swap.

D2 completed on Spot instance `i-0b2270ed88f29e80b`, published an authenticated
scientific failure, and shut down without swap or OOM. Its artifacts are under
`s3://borsuk-bench-453182569524-euc1/publication/v3/20260812/results/r01-6846520de9e7ffcfb93d5efd/runtime-v23-d2/arms/0000/attempts/0001/`.
Their authenticated SHA-256 values are:

- terminal marker:
  `c130cdc81e46f636573583e295515c8ce9a16503eb3bb6c9b5494459932729ca`;
- result receipt:
  `8d6caeac559e32fe86f58e059693166e7f133d6153b9c389d8680f428024459d`;
- complete D2 report:
  `bb8f97360827abd0f18964982c9729c083888ad02ad4cc08d1ba6779100f409a`;
- materialized page roster:
  `276dfa1914fc1cfa980a0d5037fd8f3d53f7a3e35d4ae64c863956b9095c4303`;
- derived summary:
  `5a524a337dcf3a1a554e073c48a5ff41c1e779b5e13c534578d94485d72fbbad`.

The shared geometry contained 9,990,000 unique rows, 18,620,111 primary and
replica assignments, at most two assignments per row, and 28,282 pages. Each
query selected at most eight pages with a primary target of 384 rows. This
geometry passed: aggregate oracle recall was 993,750 ppm, minimum-query oracle
recall was 900,000 ppm, and storage amplification was 1,863,874 ppm. Projected
build peak was 12,131,787,914 bytes.

The 8-byte selector encoded 161,429,348 bytes and ranked at most 4,096 rows.
It produced 468,978 / 967,419 / 1,619,705 candidate rows per query at the
minimum / median / maximum. Projected 100M-row serving RAM was 2,234,575,048
bytes and passed. Aggregate recall was 556,250 ppm, minimum-query recall was
zero, selector-regret attainment was 559,748 ppm, and CPU p99 was 49.283974
ms; all four scientific gates failed. Query CPU minimum / median / maximum was
17.041054 / 28.1606115 / 49.283974 ms.

The 12-byte selector encoded 201,389,348 bytes and projected 2,634,575,048
bytes of 100M-row serving RAM, which also passed. Aggregate recall was 671,875
ppm, minimum-query recall was 100,000 ppm, selector-regret attainment was
676,100 ppm, and CPU p99 was 50.146044 ms; the same four gates failed. Query
CPU minimum / median / maximum was 18.586229 / 32.578573 / 50.146044 ms.

Scientific elapsed time was 3,351,099,168,480 ns and measured CPU time was
2,553,685,737,000 ns. The result recorded 5,885,296,640 bytes written and peak
RSS of 12,459,761,664 bytes; terminal cgroup attestation recorded a
18,372,472,832-byte peak and zero swap/OOM events.

This run accepts the immutable page geometry, oracle coverage, storage
amplification, and serving-RAM projection. It rejects both per-row PQ selector
widths: each examines roughly half a million to 1.6 million candidate rows per
query, misses too many oracle-reachable rows even after ranking 4,096, and
exceeds the 15-ms CPU gate by more than threefold. Increasing selector width or
rank cap would worsen the already-failed RAM/CPU tradeoff. D3 was not launched,
so this run establishes no cold-latency or throughput claim. The next paid run
remains forbidden until an outcome-blind offline counterfactual validates a
replacement selector architecture against the same immutable D2 evidence.

A bounded no-spend replay then removed the remaining ambiguity in the
Revision-4 representative result. It authenticated the historical
`c59128ee68eb28beaa7f5eef7e0570dc7c787b88` BVS2 selector at
`s3://borsuk-bench-453182569524-euc1/publication/v3/20260812/results/r01-f7a6e06a6a40c1165b6cb889/runtime-v23-d2/arms/0000/attempts/0001/pages/selectors/a1c126ecf129c12a9f5c3e7d4830a1a3a7707305f48f806c2691594482e6005e`.
The 93,407,096-byte object's BLAKE3 checksum is the terminal path component.
It contains 450,087 deterministic 192-byte f16 farthest-point representatives,
at most 16 per each of 28,282 pages. The replay normalized the same 32 frozen
queries, scored every representative without the historical 320-cell filter,
reduced scores to the exact minimum per page, and selected exactly eight pages
by `(distance, page ordinal)`. Ground-truth page assignments and the 318-hit
oracle came from the authenticated D2 report whose SHA-256 is
`665dc206d04073b8cbc0b8bab9e5645760440d2336ddf4bfebea81d176b4779d`.

Removing routing changed no scientific result: the exact global scan recovered
85 of 320 ground-truth rows, or 265,625 ppm aggregate recall, zero minimum-query
recall, and 267,295 ppm of the oracle. The canonical counterfactual JSON
SHA-256 was
`dedb7ef24a08ec90639082f79a20be5678de4b849ecbedd0bb92c5c29c685e1e`.
This rejects the farthest-point representative plane itself rather than merely
its router. It does not qualify another paid run. Because one true mean per
page previously reached 696,875 ppm aggregate recall, the only remaining
page-prototype hypothesis worth falsifying is a query-independent ladder of
multiple clustered means. That test must stream and authenticate immutable
page objects with bounded memory; it must not materialize the multi-gigabyte
page corpus locally or sample away competing pages.

## V23 Revision-4 clustered page-prototype falsifier on 2026-08-30

The bounded K=32 falsifier at source commit
`f3a57436f4e1df79b37a8272739510cf71c78c37` streamed the complete immutable
Revision-4 BVS2 page corpus from Standard S3 in `eu-central-1`. It authenticated
the historical source commit `c59128ee68eb28beaa7f5eef7e0570dc7c787b88`,
all 28,282 pages, 9,990,000 primary rows, and 8,630,111 replica rows. It read
exactly 3,780,639,674 page bytes without persisting a page or prototype corpus.
The canonical falsifier result SHA-256 is
`4c45453034324700a16d533d63234d7ac8736d5291765469c6c6403e5477bc7a`.

Each page was represented by at most 32 deterministic spherical-k-means
centers after eight Lloyd iterations, and every one of the 32 registered
queries selected exactly eight pages. The complete result failed every quality
gate:

- aggregate recall was 725,000 ppm, below the 975,000-ppm gate;
- minimum-query recall was 100,000 ppm, below the 800,000-ppm gate;
- oracle attainment was 729,559 ppm, below the 995,000-ppm gate.

The resource envelope passed. Projected 100M-row serving memory was
2,686,433,028 bytes, below the 3-GiB ceiling. Peak process RSS was 234,872,832
bytes, peak cgroup memory PSI full avg10 was zero, and swap growth was zero.
Scientific elapsed time was 484,689,365,034 ns and measured CPU time was
391,646,813,344 ns. The process completed normally, emitted one canonical
result, removed its five prerequisite files and explicit temporary directory,
and left no worker process or local page corpus.

This evidence rejects fixed per-page prototypes as the Revision-4 replacement
selector architecture: even 32 clustered representatives per page remain far
below the registered recall and oracle-attainment requirements. Passing the
memory envelope does not rescue the failed quality relation. This is an
offline architectural falsification, not a cold-latency or throughput result.
D3 remains fenced, and no additional paid run is authorized by this result.

## V23 BVS3 exact-global ADC diagnostic on 2026-08-30

The no-spend exact-global ADC diagnostic ran from source commit
`007af90de373cc10c373f8d02e0215b3c1e316a4`. Its optimized local executable was
9,937,792 bytes with SHA-256
`292f791e1e39c67d267270dee3f7df30cb69719e9be29e7d1fdd74666b963f8d`.
The historical scientific source remained
`c339a546f8f9370cb2e6e9fb3b0fd4bdefa3cb05`, with source-archive SHA-256
`77917b0f5621d2580fef444ee362669a39d01c8453bee1c10ca1823631117f6d`
and index identity `index-bcda7bb66812e162d45077e6`.

The run authenticated these seven immutable inputs before semantic use:

- D1 report, 3,749,135 bytes, SHA-256
  `91717a4077c8a7d6b909f1f8d14f59d6a6d422a29e06b3d665a02c29743cbc39`,
  at
  `s3://borsuk-bench-453182569524-euc1/publication/v3/20260812/results/r01-6846520de9e7ffcfb93d5efd/runtime-v23-d1/arms/0000/attempts/0001/bench_v23_d1_report.json`;
- D2 terminal, 2,893 bytes, SHA-256
  `c130cdc81e46f636573583e295515c8ce9a16503eb3bb6c9b5494459932729ca`,
  at
  `s3://borsuk-bench-453182569524-euc1/publication/v3/20260812/results/r01-6846520de9e7ffcfb93d5efd/runtime-v23-d2/arms/0000/attempts/0001/RUNTIME_TERMINAL_COMPLETE.json`;
- D2 result, 1,782 bytes, SHA-256
  `8d6caeac559e32fe86f58e059693166e7f133d6153b9c389d8680f428024459d`,
  at
  `s3://borsuk-bench-453182569524-euc1/publication/v3/20260812/results/r01-6846520de9e7ffcfb93d5efd/runtime-v23-d2/arms/0000/attempts/0001/RESULT_COMPLETE.json`;
- D2 report, 25,725,198 bytes, SHA-256
  `bb8f97360827abd0f18964982c9729c083888ad02ad4cc08d1ba6779100f409a`,
  at
  `s3://borsuk-bench-453182569524-euc1/publication/v3/20260812/results/r01-6846520de9e7ffcfb93d5efd/runtime-v23-d2/arms/0000/attempts/0001/bench_v23_d2_report.json`;
- page roster, 12,825,166 bytes, SHA-256
  `276dfa1914fc1cfa980a0d5037fd8f3d53f7a3e35d4ae64c863956b9095c4303`,
  at
  `s3://borsuk-bench-453182569524-euc1/publication/v3/20260812/results/r01-6846520de9e7ffcfb93d5efd/runtime-v23-d2/arms/0000/attempts/0001/bench_v23_pages.json`;
- query Parquet, 3,843,448 bytes, SHA-256
  `296d45828020c1c0b88c6a1d5c822f6283280513b8c58d01cfa961f3a139a5d4`,
  at
  `s3://borsuk-bench-453182569524-euc1/publication/v3/20260812/datasets/deep-image-96/attempts/0001/materialized/test.parquet`;
- width-12 selector, 201,389,348 bytes, BLAKE3
  `89ca5a9a1661c84cf91540bfdf0bbf371879697adc224ae1d27aa068bed850b2`,
  at
  `s3://borsuk-bench-453182569524-euc1/publication/v3/20260812/results/r01-6846520de9e7ffcfb93d5efd/runtime-v23-d2/arms/0000/attempts/0001/pages/selectors/89ca5a9a1661c84cf91540bfdf0bbf371879697adc224ae1d27aa068bed850b2`.

The diagnostic scanned all 4,096 selector cells and all 9,990,000 width-12
rows for each of the 32 frozen queries, selected exactly eight pages, and read
no page bodies. Scalar and SIMD page selections were identical, with zero ppm
maximum distance delta. Its literal gates were 975,000 ppm aggregate recall,
800,000 ppm minimum-query recall, and 995,000 ppm oracle attainment. The
faithful global-top-4,096 reciprocal-rank max-cover reducer reached 671,875 /
100,000 / 676,100 ppm and failed. The per-page-minimum-ADC reducer reached
568,750 / 0 / 572,327 ppm and also failed. The canonical causal classification
is `tested-reducers-rejected`, and `claim_eligible` is false.

The canonical local result is
`/tmp/v23-global-adc-007af90de373cc10c373f8d02e0215b3c1e316a4.json`,
15,061 bytes with SHA-256
`640506d7e2ca33b046d609abe6578cdecd07a5ec5fc4b41c3eb0c4856794798a`.
The whole wrapper completed in approximately 17.90 seconds, including the
seven downloads, authentication, loading, and science. The result schema did
not persist separate scientific elapsed time, CPU time, or exact peak
process-group RSS; every five-second RSS sample stayed under the 1-GiB stop
cap. Memory PSI full avg10 was 0.00 at the start and terminal snapshots. Swap
used fell from 467,724 KiB to 467,524 KiB, a -200-KiB delta. No pressure stop
fired. The process exited normally, removed all seven prerequisite files and
its named scratch directory, and left no AWS, timeout, or diagnostic process.

The faithful exact-global result exactly matches the prior routed width-12 D2
aggregate recall, minimum-query recall, and oracle-attainment values. The
historical 320-cell router is therefore not causal for that failure. The
per-page-minimum alternative is worse. This result rejects only these two
registered reducers; it does not reject every possible PQ reducer or every PQ
representation. It authorizes no paid run, and D3 remains fenced.

## V23 leaf-page incidence Spot bootstrap failure on 2026-08-31

The first post-hardening tree-training attempt used source commit
`354e25778184fde91f98c6db0c38180f9fdb4ff9`, source-archive SHA-256
`80f851d8fc1bad29809c897f934c87e575d7aa3216875e283a929faea836262e`,
and run ID `v23-incidence-tree-20260831T084254Z`. One `c7g.8xlarge` Spot
instance, `i-0a7426c3bbd9009c8`, launched in `eu-central-1` at a recorded
price of 0.525300 USD/hour. Its immutable evidence prefix is
`s3://borsuk-bench-453182569524-euc1/research/v23-leaf-page-incidence/80f851d8fc1bad29809c897f934c87e575d7aa3216875e283a929faea836262e/v23-incidence-tree-20260831T084254Z/`.

The worker built the release example successfully in 97 seconds and recorded
a 10,633,936-byte executable with SHA-256
`366ca766f81816ea7fafc38afaea35fc2400cbbafa2b6687ccd984e956ae35a8`.
It then installed the pinned Python environment but failed before the namespace
probe, input staging, preflight, or scientific execution. Direct execution of
`scripts/launch_v23_incidence_spot.py --namespace-probe` could not resolve the
sibling `scripts.run_v23_leaf_page_incidence_falsifier` package and raised
`ModuleNotFoundError: No module named 'scripts'`. No training shard, corpus
body, progress record, scientific receipt, or incidence tree was produced.

The canonical `ATTEMPT_FAILED.json` is 330 bytes with SHA-256
`bf06d6c05698a55eab34e405f1bf954b0b701bf24e2f7e61b95fa2f426cfb2bb`;
it records `claim_eligible=false`, `worker_exit=1`, and the exact source and
archive identities above. `binary.json` has SHA-256
`12619701973a5507879c61900a41bcdb266d6e58dba5a152529ba53448912b86`,
and the 110,549-byte `worker.log` has SHA-256
`9f07a2f949457fb56868ac4eb82eef81f539295a391bf061e6353fb3cc5065ba`.
The controller authenticated the failed terminal, terminated the instance, and
the local bounded evidence inspection removed its three named scratch files
and temporary directory.

The direct-script import boundary was repaired test-first and published as
`d89a0fd7498b4a5dfa769099e34c60d6a9450721`. The repair uses the repository's
single direct-execution import pattern rather than a compatibility reader. Its
affected 41-test gate and dependency-complete 921-test Python discovery passed,
with one expected skip; Ruff, pycompile, generated Bash syntax, ShellCheck, and
diff checks also passed. This failed attempt establishes no performance or
quality evidence, authorizes no downstream phase, and leaves D3 fenced.

## V23 leaf-page incidence second Spot bootstrap failure on 2026-08-31

The second tree-training bootstrap attempt used source commit
`68d9da84d8e127cc9549ba59de1cee0e748a325d`, source-archive SHA-256
`4054ff21c7c8adbf216d5873f3b973326c2e651057a63cb04395fb278e2b0788`,
and run ID `v23-incidence-tree-20260831T085730Z`. The controller recorded one
`c7g.8xlarge` Spot instance, `i-0d4e4e3f172ea8cc2`, in `eu-central-1`; the
terminated instance has since aged out of the current EC2 query surface. Its
immutable evidence prefix is
`s3://borsuk-bench-453182569524-euc1/research/v23-leaf-page-incidence/4054ff21c7c8adbf216d5873f3b973326c2e651057a63cb04395fb278e2b0788/v23-incidence-tree-20260831T085730Z/`.

The worker again built the release executable and installed its pinned Python
environment. The 10,633,936-byte executable had SHA-256
`366ca766f81816ea7fafc38afaea35fc2400cbbafa2b6687ccd984e956ae35a8`.
The systemd scientific unit then ran for 6.633 seconds and consumed 1.057
CPU-seconds before failing in the old private-root bootstrap. The failure path
was `enter_sandbox` to `authenticate_policy_files` to `_authenticate_file`,
which raised `ValueError: sandbox authenticated file authority differs` while
authenticating a dynamic-loader runtime mount represented by a symlink. This
occurred before the Rust preflight or any scientific training work.

The canonical 330-byte `ATTEMPT_FAILED.json` has SHA-256
`a91ad4d385ab1fc319fd9f5b986feb535de19ad47646b13803bffbb93371fb05`
and records `claim_eligible=false`, phase `tree-training`, `worker_exit=1`, and
the exact source identities above. The remaining authenticated evidence is:

- 210-byte `binary.json`, SHA-256
  `76587e8a1ec6261a64fd063f8a42a9bc2809a2d0eed8ad058e95c684608e7e`;
- 151-byte `phase-failure.json`, SHA-256
  `c09b8757a50675f4bb6a892781b354eb726fab1342dc84e2aae8b0772cabf764`;
- 1,206-byte `phase-journal.txt`, SHA-256
  `8993ca82f99c678dfb5a078e4f7828ca1a9ba29a8aff12785b5414a3c4072770`;
- 271-byte `phase-traceback.txt`, SHA-256
  `786384cdc299e61fbeb979503b0b813e22515e1656c2a91767ba72a84fb1bc15`;
- 1,505-byte `phase.log`, SHA-256
  `52459ce36fddd08b3ca111a234c622243f22c30fa740930950aaec0f27028249`;
- 619-byte `preflight-staging-receipt.json`, SHA-256
  `7c086d071440ebe9f9e052e4c189fc8b98a8bad1eb0f38c2b5b92bafa03a4845`;
- 110,181-byte `worker.log`, SHA-256
  `d9a079fa549995ba3c8be6abfed53d070bd3435dc0f46838067c3fb231a8eb7f`.

No preflight scientific receipt, progress record, trained tree, performance
measurement, or quality result exists. The bounded evidence inspection removed
all eight named local files and its explicit temporary directory after process
clearance.

The failed private-root architecture has since been deleted rather than made
compatible. Source commit `093be8cf40c86bb0b7335eb41d50f162b173c986`
runs the dynamically linked executable against the normal operating-system
runtime and removes `ldd`, `pivot_root`, runtime-library mounts, and loader
discovery. It retains exact byte authentication for every declared input,
phase-specific role allowlists, a network/PID namespace with child reaping,
canonical receipts, and post-run input reauthentication. Receipt schema v3 is
an intentional pre-release break, not a legacy adapter. The replacement passed
54 affected Python tests, 75 grouped Rust library tests, three Rust example
tests, strict workspace Clippy, dependency-complete 934-test Python discovery,
and the full locked workspace/all-targets Rust gate with 1,747 tests passed and
23 ignored. This second failed attempt remains bootstrap-only evidence,
authorizes no quality claim or D3 phase, and does not count as a scientific
repetition.

## V23 leaf-page incidence tree construction on 2026-08-31

The first successful tree-training attempt after deleting the private-root
runtime used source commit `717c845bc895dc7cb5ffef45fa10bb45e09dbb0d`,
source-archive SHA-256
`a321c473cb38a3b38c4757a50acf14e144b0441b0ca4bbbe7a8c7f3baaef78cc`,
and run ID `v23-incidence-tree-20260831T120514Z`. One `c7g.8xlarge` Spot
instance, `i-000e178f6f00087b4`, ran in `eu-central-1` at 0.524900 USD/hour.
Both EC2 health checks reached `ok`; the original controller authenticated the
terminal and the instance then reached `terminated`. Its immutable evidence
prefix is
`s3://borsuk-bench-453182569524-euc1/research/v23-leaf-page-incidence/a321c473cb38a3b38c4757a50acf14e144b0441b0ca4bbbe7a8c7f3baaef78cc/v23-incidence-tree-20260831T120514Z/`.

The 10,633,816-byte release executable has SHA-256
`eba95fc1f83443843e6c69bd62e332ca0d825b73d92c8e251d6cdb1758554a64`.
The preflight used the `aarch64-neon-fma` backend, authenticated the construction
manifest and one 67,160,858-byte Parquet sample shard, and measured 395,213,929
distance dimensions/second and 319,222,756 input bytes/second. It projected
127,103,895,168 ns for the full 3,839,183,629-byte input, below the registered
5,400,000,000,000-ns limit. All five capability probes passed: declared inputs
opened, forbidden roles were absent, the network canary was denied, the network
namespace changed, and the output directory was writable. The 1,855-byte
preflight receipt has SHA-256
`f2cd86f078b164fb4b9f7e51fc6fa16f42ffb2dd46438d94743c075be9d40c45`.

Execution authenticated 63 ordered inputs, including all 59 frozen Parquet
training shards, and completed all 115 progress units. The 30,226-byte progress
chain has SHA-256
`b08cf082e7488ba7293a20cacb1982c7d0675cbe3e50a0a8c5b725b73683af12`.
The scientific unit ran from 12:08:31 to 12:12:35 UTC and consumed 2 minutes
57.360 seconds of CPU time. It emitted a 40,369,836-byte incidence tree with
BLAKE3 `aa72bf926c6fcbd17890188d8b3bd3b35393d9c392bffc032e75328ea47fae64`
at the evidence prefix above. The 26,106-byte tree receipt has SHA-256
`c1af5ab84ef20797ffe52fa0a93872008df817c142957f009895c8b7fc853a99`,
binds the preflight receipt, progress chain, executable, inputs, and output, and
records `stop=null`. Its execution probes repeat the same five passing
capability checks.

The canonical 722-byte `ATTEMPT_COMPLETE.json` has SHA-256
`a8905b94b7f89aab80465d2d466056e36d08dfaf7aff31c16c236dfed2612630`.
It records the exact source, executable, tree, receipt, Spot price, and run
identities with `status=complete` and `claim_eligible=false`. The bounded local
receipt inspection explicitly removed its five named files and temporary
directory after process clearance. This result establishes only deterministic,
bounded, offline construction of the preregistered incidence tree. It contains
no query-quality, recall, or serving-latency measurement, does not authorize a
product claim, and leaves posting construction, evaluation, and D3 fenced until
their own committed immutable phase boundaries exist.

## V23 leaf-page incidence posting-construction bootstrap failures on 2026-08-31

Three `c7g.8xlarge` Spot workers attempted the first posting-construction phase.
Every worker terminated after its canonical failed marker; none produced a
posting artifact, performance measurement, quality result, or downstream phase
authority.

The first attempt used source commit
`293294719d9c33014c4a0a772ddab31e463e86b5`, source-archive SHA-256
`001f14cbb9c1ec4b68503cc2e443ba881d33d28e48358542e2938966aa4266d9`,
run ID `v23-incidence-posting-20260831T135801Z`, and instance
`i-0783c5d4f53055b36`. Its evidence prefix is
`s3://borsuk-bench-453182569524-euc1/research/v23-leaf-page-incidence/001f14cbb9c1ec4b68503cc2e443ba881d33d28e48358542e2938966aa4266d9/v23-incidence-posting-20260831T135801Z/`.
The 340-byte `ATTEMPT_FAILED.json` has SHA-256
`c4fdd3b103aeef1302d8c910738aeddfce42b51860b4e4d0236831a20c7da436`.
Staging stopped before Rust preflight because the frozen tree receipt object did
not expose an optional S3-computed checksum even though its downloaded bytes
matched the registered SHA-256. The 153-byte `phase-failure.json` has SHA-256
`eb521f04a0363da7b7b8931b3a2607a728d1a5a0dd173b767d6cf267c21bfbeb`
and records `object S3 checksum differs` at `preflight-staging`.

The second attempt used source commit
`d6d81da316e667b29cbc98448b7d54e3dc8a8588`, source-archive SHA-256
`deb23894217c8771432c864ec9ef1cb908ea460523f65739437b61621ff32fc7`,
run ID `v23-incidence-posting-20260831T140733Z`, and instance
`i-01c709f291edfe1c7`. Its evidence prefix is
`s3://borsuk-bench-453182569524-euc1/research/v23-leaf-page-incidence/deb23894217c8771432c864ec9ef1cb908ea460523f65739437b61621ff32fc7/v23-incidence-posting-20260831T140733Z/`.
The 340-byte `ATTEMPT_FAILED.json` has SHA-256
`87488e086e8cd4df5f772734fc27a4619d78133684a7a4b3dbb25593d7884cc8`.
Staging stopped because the exact current incidence-tree identity uses the
content-addressed generation `content-aa72bf926c6fcbd17890188d8b3bd3b35393d9c392bffc032e75328ea47fae64`,
which the stager had not accepted. The 162-byte `phase-failure.json` has
SHA-256
`965bcd7bcec981ceac28f95ca4945f4f29d92582e01250311d08386ce3afe4b7`
and records `object generation authority differs` at `preflight-staging`.

The third attempt used source commit
`45b85249eec7590fb0687063d617f36915f6a477`, source-archive SHA-256
`42edd3822b99cbff40d728d1e84d71169f674574db554e73cde93aceab363bb7`,
run ID `v23-incidence-posting-20260831T141438Z`, and instance
`i-030a3e69114c8963c`. Its evidence prefix is
`s3://borsuk-bench-453182569524-euc1/research/v23-leaf-page-incidence/42edd3822b99cbff40d728d1e84d71169f674574db554e73cde93aceab363bb7/v23-incidence-posting-20260831T141438Z/`.
The 340-byte `ATTEMPT_FAILED.json` has SHA-256
`ebdc33cc2fa7289e20ef19af24f1901ca204e4d28a8ddf9c50be64ea51cd8143`.
This attempt authenticated and staged all 259 bounded preflight objects. Its
131,628-byte `preflight-staging-receipt.json` has SHA-256
`a580268cc3e7c5097ee078d469058bf6bef5ca31f2c0d4cb9de4c173ad663d09`.
Rust then rejected the exact immutable parent receipt before page decoding with
`V23 incidence parent receipt authority differs`. The 161-byte
`phase-failure.json` has SHA-256
`2bb108e9047622b62adfbf57bd0abfc99e05665ed759e2220bf4b3bc9e40c245`
and records `posting preflight failed with exit 1` at `preflight-run`.

All three workers used the same 10,638,112-byte posting executable with SHA-256
`a000b6068ba8bd1ffc05295fecb6ee7665c52abc0b43330048635b88f60432d2`.
The authenticated tree receipt instead correctly binds its own prior-phase
executable SHA-256
`eba95fc1f83443843e6c69bd62e332ca0d825b73d92c8e251d6cdb1758554a64`.
The Rust validator had incorrectly required those two phase-specific binaries
to be identical. Source commit
`4c853f6f03475ab2b778dccba2480034c2a0d83c` removes only that cross-phase
equality: the parent remains canonical, exact-byte SHA-256 rooted, predecessor
phase/run/stop validated, and output-bound, while same-phase preflight and
execution still require the same executable. Two live-path tests now cross
from a distinct predecessor executable to the current phase executable. An
explicit mutation restoring the old equality reproduced the production error.

The correction passed 77 grouped incidence tests, strict locked
workspace/all-targets Clippy, and the full locked workspace/all-targets gate
with 1,749 tests passed and 23 ignored. An independent Claude authority review
found no Critical issue and confirmed that the parent binary remains bound by
the exact receipt digest; its end-to-end coverage finding was incorporated
before the final gates. These bootstrap failures are operational evidence only.
They authorize no performance or quality claim, and development evaluation,
holdout evaluation, paid follow-on work, and D3 remain fenced until a posting
construction attempt produces its own authenticated terminal receipt.

## V23 leaf-page incidence posting construction on 2026-08-31

The first posting-construction attempt after the complete phase-authority audit
used source commit `f32fa92c89fbd30aeeb2555aa0f3edcfd0b840e8`,
source-archive SHA-256
`7f9d1350948112ecef393dc5c6994cef642ce965639c7f682d47aabfb87976a2`,
and run ID `v23-incidence-posting-20260831T152007Z`. One `c7g.8xlarge`
Spot instance, `i-0cbf035208a9d4e29`, ran in `eu-central-1` at 0.524900
USD/hour. Both EC2 health checks reached `ok`; the worker published its
terminal marker, initiated shutdown at 15:59:45 UTC, and reached `terminated`.
Its immutable evidence prefix is
`s3://borsuk-bench-453182569524-euc1/research/v23-leaf-page-incidence/7f9d1350948112ecef393dc5c6994cef642ce965639c7f682d47aabfb87976a2/v23-incidence-posting-20260831T152007Z/`.

The 10,638,192-byte release executable has SHA-256
`597ad6d9d3d76a8fd7283cead4259216ad2411affaecae2ef5dad67a6d2ab008`.
Preflight authenticated 260 ordered inputs, decoded 256 page bodies once, and
processed 1,048,576 records. It measured 290,655,343 distance dimensions,
648,197,787 input bytes, and 6,912,793 records per second. The projected full
wall was 740,147,631,792 ns, below the registered 5,400,000,000,000-ns gate,
with `resource_stop=false`. The 124,249-byte preflight receipt has SHA-256
`315ac5c5d5233c83a97a022bb54b5426f37319059808488f1e80c1b286171be4`.

Execution authenticated 28,289 ordered inputs, including all 28,282 frozen
page bodies, and completed with `stop=null` on the `aarch64-neon-fma` backend.
The 119,868-byte final progress chain has SHA-256
`379d29677e036807f7e8a738d203905b311a1acc8275018db9c204eca3f1c28d`.
The scientific service ran from 15:23:11 to 15:59:35 UTC and consumed 10
minutes 30.302 seconds of CPU time. It emitted:

- a 51,502,404-byte one-leaf posting artifact with BLAKE3
  `b5f6b1009e67d8286f012d80d4eea0f52d2516db70ddbad88e1e4477e3ae7c61`
  and uploaded-object SHA-256
  `67f4368abd274c446776cb4ac1ecd12a2d61696da6ad9191aed74c914b81aaf7`;
- a 59,186,088-byte two-leaf posting artifact with BLAKE3
  `ad75479318297d9c95e0f8f71220e7a5f2d283440be762238ea0bb8959f6897d`
  and uploaded-object SHA-256
  `2bb075ac78d086c8c4b2dc0ea8680cf83cb3763dba08b686e7e2087a8289c084`.

The 13,407,759-byte posting receipt has SHA-256
`cca5b1f895fd633ad5e6fab0288f6838d3efa9087f83809fc0c2032736ff6aca`
and binds the preflight receipt, current executable, progress chain, 28,289
inputs, and both content-addressed outputs. The canonical 871-byte
`ATTEMPT_COMPLETE.json` has SHA-256
`a891743cf34c8f758a89459de39411703ec8e124247c1cfa4775f0e11aadfe89`
and records `status=complete`, `purchase_option=spot`, and
`claim_eligible=false`. The bounded local inspection removed its six named
files and explicit temporary directory after process clearance.

This phase establishes deterministic, bounded construction of the two
preregistered posting representations. It contains no query recall or serving
latency result and authorizes no product claim. Development evaluation may use
only the already-burned query ordinals 0--31 to select one preregistered ladder
cell. Holdout evaluation and D3 remain fenced until their own sealed phase
boundaries and terminal receipts exist.

## V23 leaf-page incidence development preflight rejection on 2026-08-31

The first development-evaluation attempt used source commit
`7e267587ae169ddf62bcf74eabcc96fcd9b2ad1c`, source-archive SHA-256
`9f86243caac3836cda3e22533d7db87947db99fefa84ce901669f1496994d8af`,
and run ID `v23-incidence-development-20260831T171643Z`. One
`c7g.8xlarge` Spot instance, `i-067274d328dcf28b8`, ran in
`eu-central-1` at the controller-recorded 0.524900 USD/hour. Both EC2 health
checks reached `ok`. The controller authenticated the failed terminal,
terminated the instance, and the instance reached `terminated`. The immutable
evidence prefix is
`s3://borsuk-bench-453182569524-euc1/research/v23-leaf-page-incidence/9f86243caac3836cda3e22533d7db87947db99fefa84ce901669f1496994d8af/v23-incidence-development-20260831T171643Z/`.

The 10,644,400-byte release executable has SHA-256
`ca3aa923601a79424a642d2826b79edb1e6f864fa8d5030be011684aac7b186b`.
Preflight authenticated the sealed posting receipt, incidence tree, and both
posting planes without opening the burned D2 report or query Parquet. It used
the `aarch64-neon-fma` backend and measured:

| Preflight metric | Result |
|---|---:|
| distance dimensions | 62,914,560,000 |
| distance elapsed | 186,810,141,755 ns |
| distance dimensions/second | 336,783,428 |
| posting records/second | 75,531,651 |
| input bytes/second | 885,802,267 |
| projected full distance dimensions | 1,252,050,075,648 |
| projected full posting records | 52,168,753,152 |
| projected full input bytes | 194,038,337 |
| projected wall | 5,510,722,051,549 ns |
| registered wall limit | 5,400,000,000,000 ns |

The 3,447-byte preflight receipt has SHA-256
`4beb5a9d429c559f54a04984f74b44741508a4c1703b15b013aea370a59fd8ec`,
records `resource_stop=true`, and binds `stop=resource-stop`. Execution then
failed closed before reading the burned query cohort because a stopped
preflight cannot authorize execution. The 153-byte `phase-failure.json` has
SHA-256
`fa2d79926b70dc72a3b60af9e70ab41dd8349779898e90b577d17ae4391d5515`
and records `development execution failed` at `execute-run`. The canonical
346-byte `ATTEMPT_FAILED.json` has SHA-256
`760987e25a1e9758f36104642bf74297c55d377ba4d661b091e949cb7074373a`
and records `status=failed` and `claim_eligible=false`.

This is a preregistered performance rejection, not a quality result. The
distance-only preflight averaged about 18.681 ms for each exhaustive
65,536-leaf query scan before posting accumulation, already above the 15 ms
serving p99 gate. The registered 1.25 safety projection also exceeded the
90-minute scientific wall gate by about 2.05%. The gate will not be weakened
and this exact architecture will not be rerun. No development result or
latency artifact was produced, no ladder cell was selected, and no holdout or
D3 work is authorized. The next candidate must avoid exhaustive 65,536-leaf
scoring on the serving path rather than disguise the same scan behind a larger
timeout.

## V23 leaf-page incidence local causal screen on 2026-08-31

The no-spend development screen ran once from source commit
`245ef61938c269b0813a5726668e824641b469de`, with source-archive SHA-256
`71f257185f398c783b3676fde2de5225a60d26f41061a1d877bf9958786d4aec`.
Its 9,828,704-byte optimized local executable had SHA-256
`463cb628694f5d9e62392644d7b4b5c97ceeadfe1194e31b23c1e890ee158ff2`.
The run authenticated the exact incidence tree, one-leaf and two-leaf posting
planes, their two receipts, the frozen D2 report, and query Parquet recorded by
the preceding construction sections. It used only the already-burned query
ordinals 0--31, read zero page bodies and zero sealed holdout rows, and emitted
`claim_eligible=false`.

The registered quality gates were 975,000 ppm aggregate recall, 800,000 ppm
minimum-query recall, and 995,000 ppm oracle attainment. The best tree-beam
cell was one-leaf, beam width 128, cap 512. It scored 2,558 centroids per query,
visited at most 18,366 posting records, touched at most 4,952 pages, projected
1,172,979,332 serving bytes, and reached 581,250 / 100,000 / 584,905 ppm. The
best exhaustive-leaf control was one-leaf, cap 512. It scored all 65,536
centroids per query, visited at most 5,190 posting records, touched at most
2,265 pages, projected the same 1,172,979,332 serving bytes, and reached the
same 581,250 / 100,000 / 584,905 ppm. Both were deterministic. None of the 18
tree-beam cells or 18 exhaustive controls passed, and no ladder cell was
selected.

The canonical local result is
`/tmp/v23-incidence-development-screen-245ef61938c269b0813a5726668e824641b469de.json`,
124,665 bytes with SHA-256
`26a1550d4b53a55ecec395c990f60934aeb3b922d69c66ff53ab9ecaa1bad586`.
The result schema did not persist separate scientific elapsed time, CPU time,
or exact peak RSS; the observed process remained below 1 GiB, memory PSI was
0.00 at terminal, the process exited, and no staging directory or worker
remained.

The canonical classification is `leaf-incidence-quality-rejected`. Exhaustive
leaf scoring does not improve the best tree-beam result, so tree traversal is
not the causal quality bottleneck. The fixed leaf-to-page incidence
representation loses too much neighbor-page evidence even before the 15 ms
serving constraint is applied. Increasing beam width, posting cap, timeout, or
paid compute cannot repair this representation. The architecture is rejected;
no sealed holdout, paid evaluation, or D3 work is authorized. The next
falsifier must change the page-routing representation itself and must retain
the exact eight-page and 15 ms serving constraints.

## V23 residual RaBitQ construction and preregistration stop on 2026-09-01

The residual RaBitQ construction ran from source commit
`d63a8e87aafc2c605bae8465201ecdc4342b7016`, source-archive SHA-256
`6acc91bba5afacd67f4deeb5205ff79e117b0d0c65f23c54aa88fc1c024f59d7`,
and run ID `v23-rabitq-construction-20260901T032447Z`. The native Amazon
Linux 2023 ARM64 constructor executable had SHA-256
`60081aea16dbba7ea7a507675e8222a638a4fb432ce04d02afbaadd2f70c3254`
and required no glibc version newer than 2.34. One `c7g.16xlarge` Spot
instance completed and terminated.

The 5,132-byte construction receipt has SHA-256
`6f9e68b7fca4c6b907aff5cdad9d6fe11fa66ead20f20bf5211707d9f1cb5d4d`.
It authenticates 9,990,000 unique rows, 18,620,111 source occurrences, all
28,282 pages, and these Arrow outputs:

- row codes: 286,074,354 bytes, SHA-256
  `8fd855f27dff97e2e57c8972369726b1a79e59b391ef9e252510ebcedd18a744`;
- leaf offsets: 532,970 bytes, SHA-256
  `9212e3913639aeaf239e4c6235d131f4ef4428c264bc1de84e3306d65ab01edf`;
- centroids: 13,378,138 bytes, SHA-256
  `3aa2ad9623708a439a86c6097962966c1b91e062b407b8e72459b8841690f5ec`;
- rotation: 38,682 bytes, SHA-256
  `ac8bd4db7eef2aeef3d66b5166f6b561368cde24bf59448548e2eec7af5484aa`;
- development-only f16 control: 2,039,270,138 bytes, SHA-256
  `e6d647c4b1207b1ca3992012af2f54a79d9f910a2aa6a9fe6c8295a7465c92fb`.

The 408-byte terminal `COMPLETE.json` has SHA-256
`9f5b45363852644d00336eaa721e47d28db8df2f2c32c24c5c5cc846780df484`.
Its worker-log SHA-256 is
`e4c335f71073063ddec34e8b28281e30702ad965cc1ef139c56ce6aee78aeaba`.

The first development attempt used run ID
`v23-rabitq-development-20260901T042731Z`, native falsifier SHA-256
`21c43cc689292297a127abf941970eb94f02478415aa7fb70d6c1f96db25ca73`,
and development-manifest SHA-256
`831cbacc1ec813a822aa716918a746c9b1dbd3d231ca58d4ca62b25f01ae25cc`.
Spot instance `i-06e6561d8e9391dad` reached both EC2 health checks `ok`, then
failed closed and terminated. The 409-byte `FAILED.json` has SHA-256
`ada2fa60f7d2eba4b4c0e876cc8217b530cd0808d5417553c1e09e7172b54f22`;
the 9,925-byte worker log has SHA-256
`e671fb689fa2d5fcf4ddb9aa690a96517ba1680ea6abb84ed62debb04c4e9697`
and records `V23 RaBitQ control cannot select exactly eight pages`.

No RaBitQ recall, timing, or causal cell was measured. The abort exposed a
preregistration dimensional error: the development code scaled the fixed
4,096-row production heap down to 410 rows while leaving the physical
384-row page geometry unchanged. The retained rows saturated the greedy cover
before eight evidence-bearing pages existed, so the exact-f16 authority
control could not reproduce the registered top-4,096 ceiling. This is not a
RaBitQ quality result and does not authorize rejection or acceptance.

Before any rerun, the frozen design is amended outcome-blindly: the scan cap
remains corpus-scaled, the heap and assignment caps remain the production
4,096/8,192 constants, and a saturated cover records its natural strictly
ordered width between one and eight rather than padding with an unearned page
or aborting the whole screen. Recall, memory, timing, scalar-differential, and
causal gates do not change. The immutable construction artifacts may be reused
after the amended source and native executable are independently authenticated.
The amended recall evidence is specific to the 9,990,000-row development
index: its scan cap is corpus-scaled while its heap cap is the absolute
production constant. Only the serving-memory projection extends to 100M rows;
recall does not. No holdout or D3 work is authorized.

## V23 residual RaBitQ development rejection on 2026-09-01

The amended development screen ran once from source commit
`e0695cc95893d3dc3abfc1efb88344ff988f4517`, source-archive SHA-256
`8e71a996ea3ae354451b8a9981091ffcaf90a683bdf4e15b7aa0b8be43777a83`,
and run ID `v23-rabitq-development-20260901T051300Z`. The native Amazon
Linux 2023 ARM64 falsifier had SHA-256
`bebbe12ee82e5b9772c7dade24463185a75c96708794c6dae7bb3140c5bba99c`
and 10,063,056 bytes. The 3,771-byte development manifest has SHA-256
`ed893378b078df8cb18a2d737af55e9cecf8c495ca9815960acf196ae183064c`.
Spot instance `i-03b4868961eb70b78` reached both EC2 health checks `ok`,
completed, and terminated.

The canonical `borsuk-v23-rabitq-screen-v3` result is 117,392 bytes with
SHA-256
`ef1b47c1ea1cb62941b5087cad314b4056e13a760bbf11b4b86267a91669fb32`
at
`s3://borsuk-bench-453182569524-euc1/research/v23-residual-rabitq/8e71a996ea3ae354451b8a9981091ffcaf90a683bdf4e15b7aa0b8be43777a83/v23-rabitq-development-20260901T051300Z/terminal/screen-result.json`.
The 408-byte `COMPLETE.json` has SHA-256
`e16b72c92f7cc995f5d5c1af69f38ec2d442ae0b4d87d6ac4f7ff874ef6709ec`;
its authenticated 10,053-byte worker log has SHA-256
`0201536ef981aa1107cbef7a78ec0f336949a5c3db68fead1fde9d694c91046b`.
The result is evidence-only with `claim_eligible=false`.

Every cell evaluated the same 32 frozen queries, retained exactly 4,096 rows,
and selected exactly eight strictly ordered pages per query. The registered
pass boundary is 318/320 hits: 993,750 ppm aggregate recall, at least 900,000
ppm minimum per-query recall, and 1,000,000 ppm oracle attainment. The cells
were:

| Control | Leaves | Aggregate ppm | Minimum ppm | Oracle ppm | Hits | Kernel total |
|---|---:|---:|---:|---:|---:|---:|
| exact exhaustive | 65,536 | 921,875 | 600,000 | 927,672 | 295/320 | 104.934 s |
| exact tree | 32 | 746,875 | 200,000 | 751,572 | 239/320 | 0.057 s |
| exact tree | 64 | 787,500 | 300,000 | 792,452 | 252/320 | 0.114 s |
| exact tree | 128 | 818,750 | 400,000 | 823,899 | 262/320 | 0.219 s |
| RaBitQ exhaustive | 65,536 | 753,125 | 100,000 | 757,861 | 241/320 | 40.028 s |
| RaBitQ tree | 32 | 684,375 | 200,000 | 688,679 | 219/320 | 0.038 s |
| RaBitQ tree | 64 | 734,375 | 300,000 | 738,993 | 235/320 | 0.064 s |
| RaBitQ tree | 128 | 762,500 | 300,000 | 767,295 | 244/320 | 0.108 s |

The exact exhaustive cell scanned all 65,536 leaves and all 9,990,000 rows
for every query. Its SIMD and scalar selected pages were identical, with zero
reported scalar/SIMD distance error and zero exact fused ULP drift. The RaBitQ
cells also reproduced scalar page choices exactly within the registered
one-ppm differential limit, but their quality was materially below the exact
controls. The projected serving representation for this 9,990,000-row index
was 400,342,772 bytes. The outer Spot lifecycle took about 390 seconds,
including artifact download and pinned-environment setup; the result does not
persist whole-process CPU time or peak RSS, so neither is claimed.

The canonical classification is `authority-stop`. This is a decisive quality
rejection of the tested residual-RaBitQ page-evidence architecture: even exact
exhaustive row scoring cannot reach the frozen recall/oracle boundary, so tree
pruning and estimator approximation are not the causal blocker. Raising beam
width, improving the RaBitQ kernel, or spending more compute cannot repair the
missing page evidence. The result does not reject all possible page-routing
representations. It requires the next falsifier to change the routing evidence
itself while retaining the exact eight-page and 15 ms serving constraints.
No holdout, paid follow-on, or D3 work is authorized by this result.

## V23 residual RaBitQ fixed-reducer amendment on 2026-09-01

Before any successor development execution, the page reducer is frozen to the
quality metric itself: use only the first ten ranked rows and choose the
lexicographically smallest set of at most eight pages that maximizes coverage
of those rows. Ranks 11--4,096 may remain in the bounded scoring heap for
diagnostic evidence but cannot vote for pages. A query with fewer than ten
scored rows covers only the available prefix and receives no padding or free
recall. This is a prerelease result-format break to
`borsuk-v23-rabitq-screen-v4`; v3 is rejected rather than migrated or aliased.

The amendment is evidence-driven but parameter-free. The immutable row-vote
F0 result at
`s3://borsuk-bench-453182569524-euc1/research/v23-row-vote-f0/20260829T125701Z-v23-row-vote-f0/`
already established that exact top-ten rows plus deterministic best eight-page
coverage recover all 318 oracle-reachable hits. Its result SHA-256 is
`6c2cdbe6cc251ed950e7c0238c5a7bc5c20884d2b0a6db28f2ea844e1fe3d171`.
The first RaBitQ screen used reciprocal-rank voting across 4,096 rows and the
exact exhaustive control retained only 295 of those 318 hits. Therefore the
next single development run tests one causal question: did out-of-metric
midrank page mass displace top-ten evidence? There is no cutoff ladder and no
tuning on the burned 32 queries.

The paired exact/RaBitQ exhaustive and 32/64/128 tree cells, immutable inputs,
quality gates (318/320 hits, 993,750 ppm aggregate, at least 900,000 ppm
minimum, and 1,000,000 ppm oracle attainment), memory projection, determinism
checks, and timing gates remain unchanged. If exact exhaustive still fails,
the reducer hypothesis is rejected. If exact exhaustive passes but an exact
tree cell fails, tree/layout containment remains the blocker. If an exact tree
cell passes but its RaBitQ pair fails, the estimator is rejected. Only a
passing paired serving cell can authorize a separately preregistered holdout;
D3 and paid follow-on work remain fenced meanwhile.

## V23 residual RaBitQ fixed-reducer result on 2026-09-01

The single preregistered fixed-reducer screen ran from source commit
`c14f9305c07b2b8b602361275356a3530dfb1f8b`, source-archive SHA-256
`bb5d45ef80d4dc80481a665b65b0f196b57dc6a60b065048bbe9be20086fb477`,
and run ID `v23-rabitq-development-20260901T072128Z-recall-k`. The 7,345,264-
byte stripped ARM64 falsifier has SHA-256
`120f3b8c4913e3a9ba3e454bfe0894c884cb40a9ab853a826d4edd6b59ca3f01`.
The canonical 3,780-byte manifest has SHA-256
`f6bfc6d0932fd0ffe2fac557a05ece56c3f94db5a40c9a92fe0034eaca2c0241`
and reuses the nine immutable construction, tree, Arrow, D2, and query inputs
from the first screen. Spot instance `i-00798cf43c404552d` completed with both
EC2 system checks healthy and terminated.

The 116,061-byte canonical `borsuk-v23-rabitq-screen-v4` result has SHA-256
`6576ad212c36a40a7e1ff962aeda3c3ad3271fb6c2a369fa45a286e460c6e335`
at
`s3://borsuk-bench-453182569524-euc1/research/v23-residual-rabitq/bb5d45ef80d4dc80481a665b65b0f196b57dc6a60b065048bbe9be20086fb477/v23-rabitq-development-20260901T072128Z-recall-k/screen-result.json`.
The 407-byte `COMPLETE.json` has SHA-256
`9e2fe61c470c866ef5742841a85c689d3728985dfbc4f2f95b77107ca94c3dc6`;
its authenticated 9,282-byte worker log has SHA-256
`ab99f06c893d33a2425fd4943e4dab05278bec0f3331c330e9d09f80a70db0d6`.
The result is evidence-only with `claim_eligible=false`.

| Control | Leaves | Aggregate ppm | Minimum ppm | Oracle ppm | Hits | p99 kernel |
|---|---:|---:|---:|---:|---:|---:|
| exact exhaustive | 65,536 | 993,750 | 900,000 | 1,000,000 | 318/320 | 3.263 s |
| exact tree | 32 | 728,125 | 100,000 | 732,704 | 233/320 | 1.991 ms |
| exact tree | 64 | 756,250 | 300,000 | 761,006 | 242/320 | 3.783 ms |
| exact tree | 128 | 809,375 | 400,000 | 814,465 | 259/320 | 7.139 ms |
| RaBitQ exhaustive | 65,536 | 625,000 | 100,000 | 628,930 | 200/320 | 1.290 s |
| RaBitQ tree | 32 | 640,625 | 100,000 | 644,654 | 205/320 | 12.046 ms |
| RaBitQ tree | 64 | 653,125 | 0 | 657,232 | 209/320 | 13.379 ms |
| RaBitQ tree | 128 | 681,250 | 100,000 | 685,534 | 218/320 | 20.359 ms |

The exact exhaustive control now reproduces every one of the 318
oracle-reachable hits. This validates the fixed recall-at-ten reducer and
proves that the prior 295-hit ceiling was caused by ranks 11--4,096 displacing
top-ten page evidence. It does not validate the serving architecture: all
three exact tree cells remain far below the quality boundary, so current tree
containment and page layout are the first causal blocker. RaBitQ loses another
41 hits versus the paired exact 128-leaf cell, so its estimator is also
insufficient in the tested form. The recorded classification is
`tree-pruning-rejected`.

All scalar and fused/LUT paths selected identical pages. Exact controls had
zero fused ULP drift; RaBitQ controls stayed within one ppm scalar/LUT
differential. The production RaBitQ tree timing includes row ranking and the
fixed reducer: 32 and 64 leaves remain below the 15-ms p99 boundary, while 128
leaves exceeds it. Exact and exhaustive timings remain diagnostic. The serving
projection remains 400,342,772 bytes for the 9,990,000-row development index.

The next falsifier must change the query-independent routing/layout so the
exact tree control can preserve the authenticated exhaustive top-ten evidence
within eight pages. It must not tune on these burned 32 queries. The current
RaBitQ estimator may only be reconsidered after the exact tree/layout control
passes. No holdout, D3, or paid campaign is authorized by this result.

## V23 balanced-page router pseudoquery rejection on 2026-09-01

The single preregistered balanced-page screen ran from source commit
`594ec92f81fb972bd03bbd5e6feab1e02042dbf1`, source-archive SHA-256
`55bad7799890e5794abc36cd8454433add39bb1a36144e46a9479985859df237`,
and run ID `v23-balanced-pages-20260901T170154Z`. The 12,537,472-byte
statically linked ARM64 executable had SHA-256
`e640cf82a887ecff50d55cebeae89c54388bb7c1c7ce9cfa3bd39c32f4f99ccd`.
The 2,538-byte canonical manifest had SHA-256
`757f47992ebb0294ab52dd62468e79aecccd1bb558e12b1c188f1f5594060fed`.
Spot instance `i-077916d30e16cfd16` was an `r8g.2xlarge` in
`eu-central-1c`; both EC2 health checks remained `ok`, the run emitted its
terminal evidence in about 20 minutes, and the instance terminated.

The 8,471-byte canonical `borsuk-v23-balanced-page-receipt-v4` receipt has
SHA-256
`3c251879a52f660b61c904777c5513d1c3aec6fa62e77d4e8a8ef31d23a9bf75`.
The 80-byte `QUALITY.json` has SHA-256
`a0e54c2f4d8e3d778310eca9849f9c9a1fbd1d3a53fde7553b89123f3e4fcc0c`
and records `claim_eligible=false`, the exact instance ID, and terminal status
`quality`. The receipt authenticates the balanced tree, supercell table, and
all four page and row-page planes. Their SHA-256 identities are:

- balanced tree: `1357741b28110a0bf2d6c615e8f7fbbafe48b24e97cf5bab3685d34f56ff9284`;
- supercells: `2269787cae4e13dc2f157d7536453c67127b4d5de744b9909cc45ba43352f9c3`;
- primary pages / row pages:
  `0ce94a4d39a938bb5dd4d0be2dc878f92f3eb0a86ae1c868b9357931b2b1e65a` /
  `312e4b5490fa3ace1dca0fd65917daf237471bb2169fbee3c63c1c763dc511d0`;
- 1.125x pages / row pages:
  `c1cd10d3e52a8d0d498ab653fc1e8db7dae0b312ee9d7e5e1f45ada791f82568` /
  `4144e4e9cffe8b76e47baee1bb7db753f0dab538bb8fca90f668bf8810a131fa`;
- 1.25x pages / row pages:
  `9798528a685aec493e2922e8c7c1d1a93c8e1bf4b1d6d7a0fb267c4e67199efa` /
  `10ffda9a255e8e6530976a9a16ccc641b75f8d3d1b82760d9596854d7d3ae996`;
- 1.5x pages / row pages:
  `8e0d738ec31fd100a0f2a8b7619da89e7a6a4c68cf1007fa62764beea4fa230f` /
  `cb5e986fd8f66297ad9ef7adf92663dba5a81155b341d395adea59b34ce836ca`.

All twelve amplification/page-budget pairs passed their registered structural,
page-byte, and amplification caps. None approached the quality gates of
993,750 ppm aggregate recall, 900,000 ppm minimum-query recall, and 995,000
ppm oracle attainment:

| Amplification | Pages | Aggregate ppm | Minimum ppm | Oracle ppm | Projected page bytes |
|---:|---:|---:|---:|---:|---:|
| 1.125x | 8 | 1,464 | 0 | 1,540 | 983,040 |
| 1.25x | 8 | 1,855 | 0 | 1,933 | 983,040 |
| 1.5x | 8 | 1,855 | 0 | 1,909 | 983,040 |
| 1.125x | 16 | 5,273 | 0 | 5,273 | 1,966,080 |
| 1.25x | 16 | 5,371 | 0 | 5,371 | 1,966,080 |
| 1.5x | 16 | 5,566 | 0 | 5,566 | 1,966,080 |
| 1.125x | 32 | 13,671 | 0 | 13,671 | 3,932,160 |
| 1.25x | 32 | 14,160 | 0 | 14,160 | 3,932,160 |
| 1.5x | 32 | 14,746 | 0 | 14,746 | 3,932,160 |
| 1.125x | 64 | 29,882 | 0 | 29,882 | 7,864,320 |
| 1.25x | 64 | 31,347 | 0 | 31,347 | 7,864,320 |
| 1.5x | 64 | 31,738 | 0 | 31,738 | 7,864,320 |

No pair was selected, so the official development-query and holdout phases
were never opened. The classification is a decisive query-independent
`balanced-page-quality-rejected`: even the largest allowed page and replica
budgets recover only 3.1738% aggregate pseudoquery recall and some queries
recover no neighbor. Amplification improves recall monotonically but is far
too weak to be causal. Increasing the same page ladder, replica cap, timeout,
or construction memory cannot credibly close the gap while retaining the
15-ms and bounded-I/O product. The coupled centroid/radius page geometry is
rejected. The next architecture must use query-dependent row or witness
evidence rather than another fixed page summary. No holdout, D3, or further
paid balanced-page campaign is authorized.

## V24 witness-router reduced determinism and CPU preflight on 2026-09-01

The claim-ineligible reduced preflight ran once from source commit
`efe3a60c7995f33da254d8cc3789eb62852c36cf` on Spot instance
`i-00800236e2d22a2f6`, an `m7g.4xlarge` in `eu-central-1c`. The source archive
was 5,843,804 bytes with SHA-256
`8309e9c8b2e52fc87dc122c0cfb7309aa4f13e0b4454d7a97b2d2096a9e4d0f2`.
The 10,487,936-byte statically linked ARM64 executable had SHA-256
`959f614a48caa4695fe8860f61430a152c3bf55c32f12dee676db376954407d1`.
The instance remained at zero memory PSI and zero swap and terminated after
publishing its terminal.

The 143,561-byte canonical preflight receipt has SHA-256
`e41eb656f53ce0273bb64a0d8c9295b75002e9e74665be7af660839b34d5d6d1`.
The 467-byte complete terminal has SHA-256
`62b37259bb1d512a6e6bf89d56b4411fc2e310d170c94d3e9bb800b80ffb7b11`.
One-worker and four-worker processes produced identical construction rows,
queries, neighbors, page rows, witnesses, graph, postings, training receipt,
posting receipt, and normalized evaluation evidence. The normalized evidence
SHA-256 is
`b4ea837f1c18da27c3894dea8ac556d3fdcc1f7cd66fb7a094e6f282e7e6ef90`.
Raw timing-bearing development results intentionally differ.

Both processes selected the same first passing reduced cell: 64 pages,
`ef_search=128`, 8 selected witnesses, and posting cap 16. Each achieved
1,000,000 ppm aggregate recall, 1,000,000 ppm minimum-query recall, and
1,000,000 ppm oracle attainment with exact scalar/SIMD page equality. After
1,024 untimed warmups, their independently retained 10,000-sample selector
p99 values were 491,329 ns and 489,170 ns. Both are more than thirty times
below the 15,000,000-ns selector gate. The exact serving projection is
1,644,167,168 bytes, below 3 GiB.

This closes reduced determinism, projected serving memory, and native selector
CPU methodology. It does not qualify the architecture or authorize a release:
the fixture has only 65,536 corpus rows, 4,096 witnesses, 64 pages, and 32
synthetic queries. Full-scale load RSS, unbiased pseudoqueries, burned
development, sealed holdout, and page-body end-to-end latency remain fenced.
D3 remains unauthorized.

## V24 full-scale witness-router pseudoquery rejection on 2026-09-02

The one preregistered corpus-uniform pseudoquery screen ran from source commit
`c8ddcdf323eaf336fb418709f20c489afb7c4ab4` and source-archive SHA-256
`04f7aabbd551ba7f99a04d79bad678a2949b001bb0cf3ccc384a4794cbdce2c5`.
The 12,566,200-byte statically linked ARM64 worker had SHA-256
`b190d6ded31328500662362682322eb6c6741c0fde37e7e1ace427b1eb4ee003`.
The canonical 3,141-byte manifest had SHA-256
`0a8c0a668d5afb3523bc77c4a89c7d448a0cb6beccce8986a70b09fb83b7c3ff`.
Spot instance `i-0aae54e541083223a` was an `m7g.8xlarge` in
`eu-central-1c`; both EC2 health checks were `ok`, it emitted its terminal in
about eight minutes, and it terminated.

The screen authenticated the full 9,990,000-row construction stream,
18,620,111 physical page assignments, 28,282 pages, 1,048,576 witnesses, the
319,049,002-byte witness graph, and the 225,619,098-byte posting plane. It
selected exactly 1,024 non-witness source rows with split seed
`1311768467463790320`; their ordered source-list SHA-256 is
`206eb6cb019059688f4802cd70e958256841fa6d233b52a4eeab524530a317da`.
The worker completed all 38,601,351 registered progress units, read zero
benchmark queries and zero page bodies, and evaluated all 108 cells.

The 33,382-byte canonical `borsuk-v24-pseudoquery-result-v1` result has
SHA-256
`8eebdb1449e0bb6d59471ab39451c24bb23bf359619e18162b4c9faafd5b1271`.
Its 10,312,464-byte Parquet evidence has SHA-256
`9188c714a3330975331e7046bb6c4f295704c547b991b17de89c725a2365ec61`.
The 1,210-byte complete terminal has SHA-256
`e26db217ba56259e4625ab235ec94f8d19186a844eddfc51e90a83dc7ac984aa`.
All are under the immutable prefix
`s3://borsuk-bench-453182569524-euc1/research/v24-witness-router/full/04f7aabbd551ba7f99a04d79bad678a2949b001bb0cf3ccc384a4794cbdce2c5/v24-full-20260902T090100Z-a5a5db3/pseudoquery-a0001/`.

No cell reached the catastrophe-screen gates of 975,000 ppm aggregate recall
and 995,000 ppm oracle attainment:

| Page budget | Best aggregate ppm | Minimum ppm | Oracle attainment ppm | Cell (`ef`, witnesses, cap) |
|---:|---:|---:|---:|---|
| 8 | 652,636 | 0 | 659,463 | 512, 32, 64 |
| 16 | 763,183 | 0 | 763,183 | 512, 32, 64 |
| 32 | 857,226 | 0 | 857,226 | 512, 32, 64 |
| 64 | 913,281 | 300,000 | 913,281 | 512, 32, 64 |

The exact eight-page layout oracle itself recovered 10,134 of 10,240 hits:
936 queries fit all ten neighbors in eight pages, 70 fit nine, and 18 fit
eight. Perfect recall is therefore impossible with the current layout and
exact-eight-page contract, although the 98.9648% layout ceiling remains above
the registered aggregate gate. The best eight-page router matched that oracle
on only 184 queries and fell below it on 840. It selected the pseudoquery's own
page for 938 queries; the registered own-page-removal sensitivity drops its
aggregate recall from 652,636 to 452,929 ppm. At 64 pages the corresponding
own-page-removed recall is 789,550 ppm. Because production queries do not have
an indexed self row, this sensitivity makes the already-failing corpus-row
screen optimistic rather than pessimistic.

The result records `passed=false`, `selected_cell=null`, and
`claim_eligible=false`; consequently no pseudoquery pass receipt exists. The
failure is scientific rather than operational: increasing every registered
search/posting knob and the page budget from 8 to 64 improves recall
monotonically but still misses the gate by 61,719 ppm. At the product's exact
eight-page budget the gap is 322,364 ppm. The tested witness-HNSW plus capped
witness-to-page reducer is therefore rejected at full scale, despite its
perfect reduced fixture. Burned development, sealed holdout, page-body
integration, D3, and release claims remain fenced. The next falsifier must
change the query-dependent routing/layout representation rather than tune this
terminal ladder or loosen the registered quality boundary.

## V25 open rank-sharp containment rejection on 2026-09-02

The fail-fast V25 screen reused the immutable V24 construction and page-row
artifacts but converted them into a clean V25-only Parquet cohort. The final
converter source was commit `e4edd5095c4e1fc6778e6a007e4f402b0551a5f5`
with source-archive SHA-256
`abe86bce936ba443efbe240c42aaf04c70419823b40d50d079faa1fd7d2e8da1`.
Spot instance `i-0c3be3090d152c3fc` converted exactly 262,144 SplitMix-ranked
rows and 512 independently ranked leave-self-out pseudoqueries in 162.440
seconds. Peak RSS was 1,379,975,168 bytes with zero memory PSI and zero swap.
The canonical 3,074-byte conversion receipt has SHA-256
`eba6b07ee0273828e46cc54c00102cb88e53255f2608e8b7cf237b104ae472af`.
Its selected dataset-ordinal digest is
`cf23fddc00723642c703d04076a8aec1122bac4cb552644cca3ad0ae37bb0e4c`.

The authenticated open-screen manifest has SHA-256
`01d801038731787eb26a9f581537669ad941bdfd8a47fd7235ea671c74d5cdb6`.
The 11,062,640-byte ARM64 binary has SHA-256
`3b66c6fb1ea627bc1f3c1468d64eb9951875bef5a25fd80cbca2224e0c630bc0`.
Spot instance `i-0357a0dcfafed2c2d` authenticated every input, scanned all
262,144 construction rows for all 512 queries, retained at most 4,096 ranked
rows per query, and read zero page bodies. Scientific execution took 14.782
seconds with peak RSS 179,372,032 bytes, zero memory PSI, and zero swap. The
856-byte result has SHA-256
`c4b8a71e3c4dd5280fb5a71d07adc80cb0d56d248eba7b74d707e0f1a0d5e06a`;
the 43,667-byte Parquet evidence has SHA-256
`c623ae401047f3cfe6b1475f170f7f8d27bee2bb4d47531cc65e466a99d115e7`;
the 668-byte terminal has SHA-256
`a44abd614755ec08f421b361c2994d3610594e5595ca38fe4ce3fdf12d3e45a3`.
The instance terminated after publishing the terminal.

The exact eight-page layout oracle recovered 4,880 of 5,120 ground-truth
hits: 335 queries recovered ten, 114 recovered nine, and 63 recovered eight.
Its aggregate ceiling was therefore 953,125 ppm and its minimum-query recall
was 800,000 ppm, below the registered 975,000 ppm layout gate. Exact global
f32 row scoring followed by the rank-sharp best-row-per-page reducer achieved
3,302 of 5,120 hits: 644,921 ppm aggregate recall, 400,000 ppm minimum-query
recall, and 676,639 ppm oracle attainment. The result was identical at every
registered ranked-row limit from 10 through 4,096; candidate inventories ranged
from 262,091 to 262,138 after self and own-page exclusion.

This is a scientific `layout-rejected` result. The inherited V24 physical page
layout cannot reach the V25 exact-eight-page gate even under an exact oracle,
and the tested rank-sharp reducer loses substantial additional attainable
recall. No hierarchy, residual codebook, bounded router, sealed sentry, D3, or
release claim is authorized. The next falsifier must create a query-independent
neighborhood-preserving page layout and test its exact eight-page oracle before
any serving router is trained. Lowering the quality gate would conceal the
causal defect and is not an accepted repair.

## V26 exact-global reducer rejection and cohort invalidation on 2026-09-02

The V26 dual-tree layout selected the first passing registered capacity,
2,816 rows per page. Its layout-only oracle reported 998,828 ppm aggregate
recall, 900,000 ppm minimum-query recall, 188 pages, 2.16 seconds build time,
and 0.071 seconds evaluation time. This established that the new physical
packing can hold the pseudoquery truth neighborhoods, but it did not establish
that the serving reducer can find them.

Source commit `a7379228ff239cc1c963403df157a69c05f3f5f7` then ran one
authenticated exact-global screen on Spot instance `i-03bfff5a0efe79cfd`
(`c7g.4xlarge`, `eu-central-1c`). The 11,154,592-byte executable had SHA-256
`eacd3966406c6c8bb03ff1e7e250bea14a94d897bcead7c05fd0ea96c1bbb50f`.
It exact-scanned all 262,144 rows for all 512 queries, retained at most 4,096
rows per query, read zero page bodies, and remained claim-ineligible. Scientific
wall time was 1.940685863 seconds, CPU was 21.18 seconds, and peak RSS was
153,567,232 bytes with zero memory PSI and zero swap.

The 3,418,327-byte result has SHA-256
`3503cd93c4d874ad51a3a4393279242073cd6ec54d7e58e8cee513571ac67d5a`;
the 1,315-byte terminal has SHA-256
`ace22dc7b8a36d183d6b435e3bf65009956459aefb8758863537c70943256462`.
Both are under
`s3://borsuk-bench-453182569524-euc1/research/v26-dual-tree/open/a7379228ff239cc1c963403df157a69c05f3f5f7/v26-exact-global-a737922/exact-a0001/`.
Rank 10 achieved 680,468 ppm aggregate recall and 681,267 ppm oracle
attainment. Every registered limit from 32 through 4,096 converged at 681,054
ppm aggregate, zero minimum-query recall, and 681,853 ppm oracle attainment.
The terminal disposition is `rank-reducer-rejected`; no router, D3, or release
claim is authorized.

A subsequent no-spend authority replay exposed a stronger protocol defect.
The exact-global scorer inherited V25's corpus-pseudoquery rule: exclude the
query's own row and every row sharing either of its pages. The V26 layout oracle
that produced 998,828 ppm did not apply that exclusion. At capacity 2,816,
443 of 512 queries have a ground-truth neighbor on a forbidden own page, so the
maximum leakage-safe exact-eight-page oracle is only 951,562 ppm with a
zero-recall worst query. Replaying every preserved capacity assignment gives
aggregate ceilings of 958,984 (704), 955,468 (768), 955,664 (896), 964,453
(1,024), 958,007 (1,408), 967,578 (2,048), and 951,562 ppm (2,816). Every
ceiling is below the registered 975,000 ppm gate; capacity tuning cannot repair
the contradiction.

Accordingly, the corpus-row cohort is invalid for V26 promotion rather than a
reason to lower the quality threshold. V26 will replace it, without a legacy
reader, with 512 immutable external test queries and a separately authenticated
exact top-ten truth Parquet against the frozen construction. External queries
have no construction source or own page. The fast gate remains the iteration
boundary; full workspace assurance remains deferred to a coherent milestone.

### External-query truth and exact-global reducer result

Source commit `5bfede30588b2f7ebc884e008000db6f1caee7c8` removed the invalid
corpus-pseudoquery protocol and consumed physical rows 0 through 511 of the
immutable Deep Image test artifact as an external development cohort. The
3,843,448-byte query Parquet has SHA-256
`296d45828020c1c0b88c6a1d5c822f6283280513b8c58d01cfa961f3a139a5d4`.
Spot instance `i-0edef53e333f4e5b9` generated exact top-ten truth against all
262,144 frozen construction rows. Scientific execution took 1.297258385
seconds and 8.49 CPU-seconds, peaked at 115,331,072 bytes RSS, and observed
zero memory PSI, swap, and page-body reads. The 31,875-byte truth Parquet has
SHA-256
`6789ab82f1014bb7f3d1476045b0f95af5bd18e33e6cf9befbf4270a2a297548`;
the 881-byte terminal has SHA-256
`fdfdd31b7ada44b6f09deea964457e7d0cc988a055ac7fb91ba8e5fbee0d8005`.

The preregistered exact-global rank-reducer screen then ran once on Spot
instance `i-0febaad9826a946ff` (`c7g.4xlarge`, `eu-central-1c`). The
11,163,640-byte executable had SHA-256
`a9a1f28516e037f4eaa88a104160f0fdb97fc10930dd5f9e7d45f8def5ebc24f`.
It scanned all 262,144 rows for every external query, read zero page bodies,
and completed in 1.669034887 seconds and 11.79 CPU-seconds with peak RSS
143,323,136 bytes, zero PSI, and zero swap. Every registered prefix length
from 10 through 4,096 produced the same 831,054 ppm aggregate recall, 400,000
ppm minimum-query recall, and 832,355 ppm oracle attainment. All cells failed
the literal 975,000 aggregate and 995,000 oracle-attainment gates. The
3,420,057-byte canonical result has SHA-256
`bcaaa20cd1460d86e9e7e27bd45e37067fc8cb61017b2365adb81aa52af0578f`;
the 1,331-byte terminal has SHA-256
`59446d4cc435152b12de475f2c8ebc0312839ec488953bb55e10363b4ec13ab7`.
Artifacts are rooted at
`s3://borsuk-bench-453182569524-euc1/research/v26-dual-tree/open/5bfede30588b2f7ebc884e008000db6f1caee7c8/v26-external-exact-global-5bfede3/exact-a0001/`.

This is a valid `rank-reducer-rejected` result, not a layout rejection. The
rank-sharp reducer represents each page only by its nearest ranked row, so
additional corroborating rows cannot strengthen a page after its minimum is
set. The flat prefix ladder is evidence against that objective, not evidence
against exact global row ranking or the already-passing physical layout. Router
construction, D3, and release claims remain fenced. The next bounded falsifier
must freeze and test a genuinely different cumulative page-evidence reducer
before any router is built.

Source commit `36f8bf0aacc6725c8450f38f21e4695fc8ed3c94` tested exactly one
such successor: fixed integer reciprocal-rank accumulation with no learned
weight or distance-scale parameter. Spot instance `i-00aaf603ab76e5381`
completed in 2.212211318 seconds and 22.83 CPU-seconds with 157,556,736 bytes
peak RSS, zero PSI, zero swap, and zero page reads. Rank 32 was best at 884,765
ppm aggregate recall, 500,000 ppm minimum recall, and 886,150 ppm oracle
attainment; deeper prefixes degraded monotonically after rank 32. The
3,421,449-byte canonical result has SHA-256
`c92b87efe7b56ab07afbe790af2795e626dbb1c8eddd7ba76d342cd9e6482554`;
the 1,331-byte terminal has SHA-256
`c8112296485f892f3ee51ef1aba3ed09c1164435a15eab3d83409ff05866428e`.
They are rooted at
`s3://borsuk-bench-453182569524-euc1/research/v26-dual-tree/open/36f8bf0aacc6725c8450f38f21e4695fc8ed3c94/v26-cumulative-exact-global-36f8bf0/exact-a0001/`.

A bounded post-terminal identity check then compared the first-ten row
ordinals retained by exact-global scoring with the independently authenticated
external-truth Parquet. All 512 ordered lists matched exactly. Consequently,
applying the already-registered exact maximum-cover rule to those ten rows is
algebraically the external-query layout oracle: 998,437 ppm aggregate,
800,000 ppm minimum,
and at most eight pages. This is not a third fitted reducer and requires no new
scientific run. It proves that exact row retrieval plus exact maximum cover can
map the external queries to the passing layout, while both lossy weighted page
summaries are rejected. The bounded router may now be falsified against this
fixed top-ten/maximum-cover target. D3 and release claims remain fenced.

### Fixed dual-tree router rejection

Source commit `40aa4717d62ae04085f435bbc825ac140a614e03` tested the
preregistered best-first dual-tree router once on Spot instance
`i-0beb8218e9c376c6c` (`c7g.4xlarge`, `eu-central-1c`). The
11,257,096-byte executable had SHA-256
`b5efe3ebcf939e16f853dea7486e61ee8352fceb3a846168521009d33a7e36ed`;
the source archive had SHA-256
`58882a13ddb379fe751e10f6bad525da9194f244fe1a04b6a6e53338512fe75a`.
The router selected exactly eight pages for each of 512 external queries and
read zero page bodies. It completed normally in 0.117644136 seconds and 0.14
CPU-seconds, peaked at 11,780,096 bytes RSS, and observed zero memory PSI and
zero swap growth. The instance terminated after publishing its terminal.

The result was 641,406 ppm aggregate recall, zero minimum-query recall, and
642,410 ppm oracle attainment. It therefore failed all three registered quality
gates: 975,000 aggregate, 800,000 minimum-query, and 995,000 oracle attainment.
The 74,633-byte canonical result has SHA-256
`22911c3c6b65df13dbc17308c7582677433e18a511653aed2a2dbf872ea554f0`;
the 685-byte terminal has SHA-256
`9c2912d1aaa40624b4fad861083af6ac28acbb168284ae4f1190fb1ef07a9b55`.
Artifacts are rooted at
`s3://borsuk-bench-453182569524-euc1/research/v26-dual-tree/open/40aa4717d62ae04085f435bbc825ac140a614e03/v26-tree-router-40aa471/router-a0001/`.

This is a scientific `tree-router-rejected` result. Latency and memory are not
the failure: the fixed threshold-margin traversal does not recover the leaves
containing the exact top-ten rows that the authenticated external-query
maximum-cover control maps to 998,437 ppm aggregate and 800,000 ppm minimum
recall. The quality
thresholds are unchanged. Milestone assurance, the sealed sentry, D3, and
release progression remain fenced for this router. The next work is a bounded
local causal diagnostic of traversal versus representation; no further paid
run is authorized by this terminal.

### Fixed-router candidate-width diagnosis

Source commit `ccd267d1780a980886ea66849542fbcec856a8d3` ran one
claim-ineligible, page-free candidate-width diagnosis on Spot instance
`i-017f002ee0e81cfbe` (`c7g.4xlarge`, `eu-central-1c`). The
11,346,288-byte executable had SHA-256
`e07f2bf6d3a7c925ed7709df570a5c19041387d45d957caf83746f1254be34dd`;
the source archive had SHA-256
`70a011f822dde9e121c2069a226187955dfb19765b2f21003a73548f26f8b0ca`.
The diagnostic ranked all 188 leaves under the frozen tree-margin rule, then
used truth only to compute the exact best eight-page cover inside fixed prefixes
of 8, 16, 32, 64, 128, and 188 candidate pages. It did not read page bodies or
alter serving behavior.

The aggregate/minimum/oracle-attainment ppm triples were respectively
641,406/0/642,410; 800,976/0/802,230; 915,039/300,000/916,471;
979,882/500,000/981,416; 997,265/800,000/998,826; and
998,437/800,000/1,000,000. Width 128 is the smallest registered prefix that
passes the 975,000 aggregate, 800,000 minimum-query, and 995,000 attainment
gates. The diagnostic completed in 0.105580768 seconds and 0.19 CPU-seconds,
peaked at 14,479,360 bytes RSS, and observed zero PSI and zero swap growth. The
instance terminated after its terminal.

The 493,988-byte canonical result has SHA-256
`e3cb66c69bdd98bb235af9ea0eb9519b9f13fbd2e488dd24858f46e741b2fb54`;
the 698-byte terminal has SHA-256
`42eba63ea8819958a12269718f2086f877e0404482297e3abece92389a790878`.
Artifacts are rooted at
`s3://borsuk-bench-453182569524-euc1/research/v26-dual-tree/open/ccd267d1780a980886ea66849542fbcec856a8d3/v26-tree-router-diagnostic-ccd267d/diagnostic-a0001/`.

This isolates the failure to eight-page selection from a bounded tree
frontier, rather than absence of the relevant pages from every bounded prefix.
The next falsifier is therefore a query-independent page-summary reranker over
exactly the first 128 tree candidates. It must still choose exactly eight pages
without truth or page-body access. D3 and release claims remain fenced.

### Single-centroid bounded-frontier rejection

Source commit `95b6b8cfbe93d34fcccb2ebfd109e0ad8734793b` tested the first
query-independent page-summary reranker once on Spot instance
`i-047c0df919a85ae72` (`c7g.4xlarge`, `eu-central-1c`). The
11,364,888-byte executable had SHA-256
`a3002b5bbfccb3aec30adafbf85d8fe50dd65e91fabb2ac49adb1847aed18c94`;
the 8,438,358-byte source archive had SHA-256
`ee19ec9f5b182e132f7f782cd7784a9e654ba6db058e5fb31ebf548dcef69da1`.
Construction computed one normalized centroid per physical page using every
primary and replica assignment. Serving ranked only the already-frozen first
128 tree candidates by centroid distance and selected exactly eight pages. It
used no truth during selection and read zero page bodies.

The reranker recovered 3,561 of 5,120 ground-truth hits: 695,507 ppm aggregate
recall, 100,000 ppm minimum-query recall, and 696,596 ppm oracle attainment.
The hit histogram was 83 queries with ten hits, 61 with nine, 79 with eight,
77 with seven, 75 with six, 66 with five, 37 with four, 18 with three, 13 with
two, and three with one. It failed all three literal gates: 975,000 aggregate,
800,000 minimum-query, and 995,000 oracle attainment. Scientific execution
took 0.883661622 seconds and 0.85 CPU-seconds, peaked at 124,514,304 bytes RSS,
and observed zero memory PSI and zero swap growth. The instance terminated
after publishing its terminal.

The 74,970-byte canonical result has SHA-256
`4993824fb009d7fd1158c9195ab6f6eb87a29d276feb7961f74e66f52ba2f342`;
the 683-byte terminal has SHA-256
`92c51bbe7e9c19156f430ac6d54a957841f5bc7cba91e5c20cedb8e132ef4e6b`.
Artifacts are rooted at
`s3://borsuk-bench-453182569524-euc1/research/v26-dual-tree/open/95b6b8cfbe93d34fcccb2ebfd109e0ad8734793b/v26-centroid-router-95b6b8c/centroid-a0001/`.

This is a scientific `tree-router-rejected` result for a single page centroid,
not evidence against the 128-page frontier. The authenticated width diagnosis
showed that the same frontier contains a passing exact eight-page cover, while
the centroid collapses each page's multimodal neighborhoods to one mean. No
weight tuning, quality-gate relaxation, sealed sentry, D3, or release claim is
authorized. The next falsifier must preserve multiple query-independent modes
per page under a fixed memory/CPU ladder, or reject summaries and advance to a
bounded compact row-code scan within the same frontier.

### Fixed page-mode ladder rejection

Source commit `409e8c044237efd9f330224ca2166b6f67d4a79e` evaluated the
fixed query-independent K=2/4/8/16 page-mode ladder once on Spot instance
`i-0a2c74b7977f26667` (`c7g.4xlarge`, `eu-central-1a`). The
11,550,976-byte executable had SHA-256
`2134c765925f9fbbe435659d239800602153ff581391a8a6760a341b27009f60`;
the 8,430,569-byte source archive had SHA-256
`8f42c0f017734e52cb312bee8b565582c1bf0f60803566d16fefa7391509a31a`.
Each arm used deterministic nested balanced page splits, the same first 128
tree candidates, closest-mode page scoring, and exactly eight selected pages.
Truth was joined only after selection. Bulk per-query evidence was emitted as
Parquet, and no page bodies were read.

The aggregate/minimum/oracle-attainment ppm triples for K=2, 4, 8, and 16 were
respectively 705,078/100,000/706,181; 698,632/100,000/699,726;
713,671/0/714,788; and 716,210/0/717,331. Every arm failed every literal
quality gate. Scientific execution took 1.490091527 seconds and 1.67
CPU-seconds, peaked at 124,518,400 bytes RSS, and observed zero memory PSI and
zero swap growth. The instance terminated after publishing its terminal.

The 1,135-byte canonical result has SHA-256
`0cd65782887508eda9ee0bb9babb732c40bfecebe2908a92e573c135848943d3`;
the 25,308-byte Parquet evidence has SHA-256
`2adafdaa6da29df915e0d7a11a02c4932a1b5d8530d3ff980d0a1546a37734f1`.
Artifacts are rooted at
`s3://borsuk-bench-453182569524-euc1/research/v26-dual-tree/open/409e8c044237efd9f330224ca2166b6f67d4a79e/v26-page-mode-409e8c0/modes-a0001/`.

This rejects the fixed page-score family, not the 128-page frontier. A
no-spend replay against the same external truth also confirmed that the
188-page diagnostic and external-query layout oracle are identical at 998,437
ppm aggregate and 800,000 ppm minimum recall; the earlier 998,828/900,000
figures belong to the superseded corpus-pseudoquery cohort. The next falsifier
must preserve row identity inside the fixed frontier and feed the registered
maximum-cover reducer. Page-mode tuning, D3, and release claims remain fenced.

### Exact candidate-row ceiling passes

Source commit `99c57bcada25a9bcc32a15d52e7d13889d17a4ef` evaluated the
fixed first-128-page tree frontier with exact float32 construction-row scoring,
a bounded top-10 row heap, and the exact eight-page maximum-cover reducer once
on Spot instance `i-0de7e3efba5c1bf21` (`c7g.4xlarge`,
`eu-central-1c`). The 11,564,872-byte executable had SHA-256
`79db57b451c73738294f8205a0f530d823af829e9125d0ca43d65d013c5cdf11`;
the 7,001,673-byte source archive had SHA-256
`15a488452259d4680c124c3b7219b94ea4cff25fe43b04525ddd092b83f9a610`.
Truth was joined only after the eight pages were selected. Bulk per-query
evidence was emitted as Parquet, and the diagnostic read zero page bodies.

The fixed arm recovered 5,106 of 5,120 ground-truth hits: 997,265 ppm aggregate
recall, 800,000 ppm minimum-query recall, and 998,826 ppm oracle attainment.
It passed all three literal gates: 975,000 aggregate, 800,000 minimum-query,
and 995,000 oracle attainment. Scientific execution took 2.576466685 seconds
and 28.77 CPU-seconds, peaked at 125,435,904 bytes RSS, and observed zero
memory PSI and zero swap growth. The instance terminated after publishing its
terminal. A first bootstrap-only Spot cell, `i-0a0ed4a89fd21f0ca`, had no
instance profile, executed no authenticated science, and was replaced rather
than treated as a scientific repetition.

The 813-byte canonical result has SHA-256
`79e60f7db9ed4bccd84a3ff245c32d99d4784c93e5140fec4e4d774b5bfe9ac4`;
the 7,948-byte Parquet evidence has SHA-256
`d4f49709bacba222748b6875d30b2054be6b33077e6239f4ad9bca6a2df7d79c`;
the 1,057-byte terminal has SHA-256
`df49e543e6db692370e4d39d8f91baa2360d6d2fec5495c39143cbb1fe3c5a0c`.
Artifacts are rooted at
`s3://borsuk-bench-453182569524-euc1/research/v26-dual-tree/open/99c57bcada25a9bcc32a15d52e7d13889d17a4ef/v26-candidate-cover-99c57bc/cover-a0001/`.

This is a causal pass for row identity plus maximum cover inside the existing
128-page tree frontier. It rejects additional page-score tuning as the next
step: the frontier is sufficient, while single-centroid and fixed page-mode
reducers discarded the decisive row-level overlap evidence. The next serving
falsifier must replace exact float32 rows with a preregistered compact row-code
ladder under the 3 GiB and 15 ms gates. D3 and release claims remain fenced.

### Eight-byte page-major PQ rejection

Source commit `89899eef2c5ff60414b93269f4591a87b58270e1` evaluated a
fixed eight-byte product-quantized row code once on Spot instance
`i-0a16eb6cea7661962` (`c7g.4xlarge`, `eu-central-1a`). Every source row
was represented by two page-major occurrences containing only its eight-byte
code and four-byte partner page. When both assigned pages were in the fixed
first-128-page frontier, the query scan scored only the lower-page occurrence,
so each row contributed once without a stored row identifier. The complete
100M-row projection, including offsets, codebook, and a 512 MiB runtime
reserve, was 2,937,537,416 bytes. Selection retained a bounded top ten and used
the same exact eight-page maximum-cover reducer as the passing float32 control.

The arm reached 720,898 ppm aggregate recall, 0 ppm minimum-query recall, and
722,026 ppm oracle attainment. It failed all three literal quality gates while
passing the 3 GiB resident-memory projection. Scientific execution took
5.628570197 seconds and 11.09 CPU-seconds, peaked at 225,763,328 bytes RSS,
and observed zero memory PSI and zero swap growth. The instance published its
terminal and shut down.

The 828-byte canonical result has SHA-256
`3573c189081df5994fe8f15e099349c78893395a1330465e7e638249a2713b6d`;
the 9,988-byte Parquet evidence has SHA-256
`0dca2c6e5492c9faf48d9d2c1f702ed7eeba8b9d59f5bf1fe42b834ed6f0cede`;
the 1,057-byte terminal has SHA-256
`932c7daec7127b1ac55eeb71e51ef6fed18e8e98b6276cfa42a3931cd3f03a8d`.
Artifacts are rooted at
`s3://borsuk-bench-453182569524-euc1/research/v26-dual-tree/open/89899eef2c5ff60414b93269f4591a87b58270e1/v26-pq8-cover-89899ee/pq8-a0001/`.

This rejects the tested eight-byte identity-PQ representation, not row-identity
maximum cover: the exact float32 control on the identical frontier passed at
997,265/800,000/998,826 ppm. Recall gates must not be relaxed. The next
diagnostic is a single preregistered 8/16/24/32-byte fidelity curve; it will
identify the minimum code width that recovers the control before any serving
RAM gate is revised. D3 and release claims remain fenced.

### Fixed PQ fidelity ladder rejects naive widening

Source commit `c7097101dacddad9cc2f8b5cc9e9714b72c952a9` evaluated the
fixed 8/16/24/32-byte PQ ladder once on Spot instance
`i-0aeaca990bb5c9098` (`c7g.4xlarge`, `eu-central-1a`). All four arms used
the same immutable construction, trees, assignments, 512 external queries,
truth, first-128-page frontier, bounded top-ten heap, and exact eight-page
maximum-cover reducer. Bulk evidence was emitted as Parquet. Width, training
inventory, frontier, and rank depth were not caller tunable, truth was joined
only after selection, and the run read zero page bodies.

The aggregate/minimum/oracle-attainment ppm triples for widths 8, 16, 24, and
32 were respectively 720,898/0/722,026; 815,039/300,000/816,314;
867,382/400,000/868,740; and 907,226/600,000/908,646. Every arm failed the
literal 975,000 aggregate, 800,000 minimum-query, and 995,000 oracle-attainment
gates. Their complete 100M-row resident projections were respectively
2,937,537,416; 4,537,537,416; 6,137,537,416; and 7,737,537,416 bytes, so only
the already-rejected eight-byte arm also fit the 3 GiB gate.

The 11,841,456-byte executable had SHA-256
`3280ad884625b7386483ab8e7dbab0d8ea79bcefa37f23e22422c27df9842fbc`;
the 8,442,802-byte source archive had SHA-256
`acfe7e00745bea54588560953d86910eb5f4187f4240dfd3e468baa4b76df3ef`.
Scientific execution took 30.717411154 seconds and 69.85 CPU-seconds, peaked
at 265,318,400 bytes RSS, and observed zero memory PSI and zero swap growth.
The 1,838-byte canonical result has SHA-256
`2f1719848eab742931ff4201fa339fa71b96552353531cdfde7c20e080ff53f1`;
the 28,700-byte Parquet evidence has SHA-256
`02f130b549fcac090a112ab05f3d5958e4c0295e783b41cd4fd1f0e918bd3c65`;
the 2,089-byte terminal has SHA-256
`b1e72e6d63739f72e4f1d7050bb2015aff982d96ab06408b55f39012a515fd9e`.
Artifacts are rooted at
`s3://borsuk-bench-453182569524-euc1/research/v26-dual-tree/open/c7097101dacddad9cc2f8b5cc9e9714b72c952a9/v26-pq-width-c709710/width-a0001/`.
The instance published its terminal, shut down, and is terminated.

This rejects naive identity-PQ widening as the complete serving answer. The
monotonic quality gain shows quantization fidelity is causal, but 32 bytes per
row still misses the quality gates while exceeding the memory gate by more
than 4.5 GiB. The next bounded falsifier deduplicates one 16-byte code per row
plus two page identifiers within the 3 GiB resident gate, retrieves a fixed
top-L ladder, and exact-reranks only that bounded set from local cold vector
storage. Recall gates, D3, and release claims remain fenced.

### PQ16 plus bounded exact rerank passes at depth 512

Source commit `066075cdb8adce33ffe94e2b6ff739123d56f346` evaluated one
fixed PQ16 top-L plus exact-rerank ladder on Spot instance
`i-019929d5991b73373` (`c7g.4xlarge`, `eu-central-1a`). A single 16-byte
code per row provided approximate membership inside the same immutable
first-128-page frontier. The diagnostic retained bounded depths
10/32/128/512/2,048, exact-reranked only each retained set against the
authenticated float32 construction vectors, and then applied the same exact
eight-page maximum-cover reducer. Truth was joined only after page selection.
All arms share the 2,937,537,416-byte 100M-row serving projection: one code per
row, two four-byte page-posting row identifiers, page offsets, codebook, and a
512 MiB runtime reserve. Cold exact vectors are excluded from resident memory
and require a separately qualified local-storage representation.

The aggregate/minimum/oracle-attainment ppm triples for depths 10, 32, 128,
512, and 2,048 were respectively 815,039/300,000/816,314;
910,937/400,000/912,363; 979,687/500,000/981,220;
995,507/800,000/997,065; and 997,851/800,000/999,413. Depth 512 is the
smallest arm passing all literal quality gates. Depth 128 passes aggregate
recall but fails worst-query recall and oracle attainment, so it is not a
qualifying shortcut.

The 11,873,832-byte executable had SHA-256
`dbe1b872a6b87bb102101e665d694855b2f08c29316200b83df0334e7f9fe740`;
the 8,446,323-byte source archive had SHA-256
`94082c101fe6358ba8936b6060a16988ce32927287f515e534bf7350c551c888`.
Scientific execution took 7.039013601 seconds and 13.64 CPU-seconds, peaked at
231,956,480 bytes RSS, and observed zero memory PSI and zero swap growth. The
2,154-byte canonical result has SHA-256
`3bd1909a914b2ff450e1969511f2f2c83c19cc7dc48b7eb5644624cade68921e`;
the 24,493-byte Parquet evidence has SHA-256
`aa62454c3c12bde9da85d5e4c996e656a26f2d9b107a54aa61c1635a4c5f10ec`;
the 2,424-byte terminal has SHA-256
`298ddae61b9a3d16a947e9b46d29565bcf2a8283cbabffce3a681da13f7b4d6d`.
Artifacts are rooted at
`s3://borsuk-bench-453182569524-euc1/research/v26-dual-tree/open/066075cdb8adce33ffe94e2b6ff739123d56f346/v26-pq16-rerank-066075c/rerank-a0001/`.
The instance published its terminal and is terminated.

This accepts PQ16 depth 512 as the serving architecture candidate, not as a
latency or release claim. The next implementation must use packed resident
codes and page postings, a bounded deduplicating candidate merge, and an
authenticated Arrow cold-vector file on local SSD. It must reproduce the
quality result and independently pass the 15 ms p99 gate before promotion.
D3 and release claims remain fenced.

### Packed PQ16 serving preflight passes at 262,144 rows

Source commit `a0a896382f456de9b3e187b2d2418e9033222124` replaced the
corpus-sized merge heap with an exact bounded occurrence scan and opened the
authenticated Arrow cold-vector file through a read-only memory map. Arrow IPC
remains the cross-language storage authority; the memory map is only the local
access path after SHA-256 authentication. The final preflight ran once on Spot
instance `i-08edb3f9f153caa89` (`c7gd.4xlarge`, `eu-central-1a`) and the
instance is terminated.

The benchmark executed 1,024 untimed warmups and retained 10,000 raw latency
samples across 512 immutable external queries. It used the fixed first-128-page
frontier, retained and exact-reranked 512 rows, selected exactly eight pages,
and read zero page bodies. Latency was 8,260,309 ns p50, 8,446,408 ns p95,
8,553,453 ns p99, and 9,313,985 ns maximum, so the preregistered 15,000,000-ns
p99 gate passed. The benchmark process completed in 92 seconds, peaked at
113,688,576 bytes RSS, and observed zero memory PSI and zero swap.

The build process completed in eight seconds, peaked at 221,503,488 bytes RSS,
and also observed zero PSI and zero swap. Its 262,144-row artifacts contain
524,288 page occurrences across 188 pages. The authenticated cold-vector Arrow
file is 108,136,626 bytes with SHA-256
`a28affeb26f80f57c4c83a2dc2df992e91dbaff86b98e1b2509e226f026dc64b`.
The complete resident representation projects to 2,937,537,416 bytes at 100
million rows, below the 3-GiB gate.

The 1,514-byte canonical benchmark result has SHA-256
`bb94525f98d6def92770ae1b11b713ec7395d00b1be228e09a7d1b6f60f68753`;
the 26,836-byte latency Parquet has SHA-256
`33053b2618c6a5e4520a3de1fd6b16c9f3da25e1d1b6bf10e689db4acb15a098`;
the 4,264-byte serving manifest has SHA-256
`83aab3ec2582da49a3c41503ccd6e1a70850694b615784239bf873d7097b2230`;
and the terminal has SHA-256
`1ce38af17c8346fb68f182acd82616c6735768cfebedcfc4e501e9b132973b0b`.
The build and benchmark executables have SHA-256
`87fda90fd13bdb7e90a8932ec43c339286f36237736c2d0b94b10fee356ceb3c`
and `2ffd3719762a2438b5ff926083a94082a008c890d249a0a9fd0a3984056da456`.
All evidence is rooted at
`s3://borsuk-bench-453182569524-euc1/research/v26-dual-tree/open/a0a896382f456de9b3e187b2d2418e9033222124/v26-pq16-serving-262144-a0a8963/preflight-a0001/`.

This closes the reduced serving latency, memory, deterministic-result, and
local-artifact access gates. It does not establish native 9,990,000-row or
100-million-row latency, and it does not turn the burned development quality
result into a release claim. The next gate is an authenticated native Deep
Image scale build and serving measurement. D3 and competitor claims remain
fenced until that scale result passes.

### Native-layout 1,048,576-row structural preflight passes

Source commit `e567f58a6a3bed09329f59e71e3a5fb163cde34f` built the
registered leading 1,048,576 source rows from the authenticated 9,990,000-row
construction Parquet once on Spot instance `i-0fcd548db2b85c112`
(`m7g.4xlarge`, `eu-central-1c`). The builder used 16 workers, capacity 2,816,
and the two frozen projection-tree seeds. It produced 373 leaves per tree,
746 disjoint physical pages, 1,048,576 two-copy assignments, and
27,788,574,720 registered projection steps. The phase opened no query role
and read no page body.

Scientific construction completed in 41.545130431 seconds and 95.6 CPU-seconds,
peaked at 883,109,888 bytes RSS, and observed zero memory PSI and zero swap.
The 3,422-byte sealed receipt has SHA-256
`b4b7660acf642b869e5055285432afdef6e047c805ed2399cf54c500d361f0d8`.
Output SHA-256 values are
`f08efe18e27a4496118757e350584bee1454fd73197c7274fe0ffe7131bc623e`
for page assignments,
`3d5b5d0c220ffaeabb974b152a7da7f3814d7879ab32828bc734ed6297b3c091`
for the primary tree, and
`a6154b36377eaceb69f926bcdca0333b9c44676e5edc9393c1b9e3b2a27dfcc0`
for the replica tree. The complete terminal is rooted at
`s3://borsuk-bench-453182569524-euc1/research/v26-dual-tree/native-layout/v26-native-layout-1048576-c60c9a0-preflight-a0001/`.
The instance published its terminal and terminated.

This closes the native-layout structural scale and resource preflight. It is
not a recall or serving-latency claim. The exact 9,990,000-row manifest is now
frozen without changing capacity, seeds, binary, or source data. D3 and
competitor claims remain fenced until native layout, truth, quality, and serving
measurements pass.

### Native 9,990,000-row layout ladder freezes ten pages

The frozen full-scale capacity ladder built three layouts from the same
authenticated 9,990,000-row construction Parquet, binary, projection seeds, and
16-worker method on `causality` Spot `m7g.4xlarge` instances in
`eu-central-1c`. Every instance published its terminal and terminated. Capacity
2,816 produced 7,096 pages in 144.758136441 seconds, used 1,095.37 CPU-seconds,
and peaked at 8,419,000,320 bytes RSS. Its receipt SHA-256 is
`2ae33960bc54eb06dc4f83210ca3646f250549614a1214078115135773b9144f`.
Capacity 4,096 produced 4,878 pages in 126.331000369 seconds, used 914.93
CPU-seconds, and peaked at 8,437,325,824 bytes RSS. Its receipt SHA-256 is
`382407fee465c74b9d2a95ce56eba3baa28d4893bd74884acb5a41f6b8092dab`.
Capacity 8,192 produced 2,440 pages in 119.665546686 seconds, used 843.69
CPU-seconds, and peaked at 8,432,369,664 bytes RSS. Its receipt SHA-256 is
`53b15fcec13d11ea65a3e3069ea86feb6b4c0c964acec2c13385d507960ece30`.
All three observed zero memory PSI, zero swap, zero query-role opens, and zero
page-body reads.

A bounded exact cover check streamed each authenticated assignment Parquet and
the immutable official Deep Image neighbor Parquet (SHA-256
`d305fcea7387988941defd2942cca1673693271329f977ba073da888cac3de8d`).
It retained only the first ten official neighbors for query ordinals 0 through
511 and their two page assignments. At an eight-page budget, capacities 2,816,
4,096, and 8,192 reached respectively 954,101, 959,765, and 979,492 ppm
aggregate oracle recall; all failed the 995,000 gate. On the 8,192-row layout,
nine pages reached 995,312 ppm aggregate and 900,000 ppm minimum-query recall.
Ten pages reached exactly 1,000,000 ppm aggregate and minimum-query recall: all
512 queries recovered all ten official neighbors. Twelve pages added no quality.

The native serving candidate therefore freezes capacity 8,192 and ten selected
pages. Quality gates are unchanged; the extra two pages are an explicit resource
trade rather than a relaxed recall claim. The next gate must show that the fixed
128-page PQ16 frontier, bounded top-512 exact rerank, ten-page selection, and
local Arrow cold-vector reads remain below 15 milliseconds p99 at native scale.
D3 and competitor claims remain fenced.

### Dual PQ-key router reaches the recall boundary but fails oracle and latency

Source commit `ba509c88e4ce83a0d67f74756694093cee02f6de` evaluated the
fixed two-plane PQ16 key router once on `causality` Spot instance
`i-0aa96cac1548b0449` (`c7gd.4xlarge`, `eu-central-1a`). The two key planes
were fixed at code-byte pairs `(0,8)` and `(4,12)`, the per-plane arm ladder was
128/512/1,536, each arm retained 2,048 rows for exact Arrow reranking, and every
query selected exactly ten pages. The run authenticated the complete
9,990,000-row serving bundle, 512-query Parquet, and 512-row truth Parquet before
evaluating the fixed first 32 queries. It read zero page bodies and emitted 96
samples as Parquet. Truth was joined only after page selection.

The aggregate/minimum/oracle-attainment ppm triples for 128, 512, and 1,536
keys per plane were respectively 809,375/300,000/809,375;
925,000/400,000/925,000; and 975,000/800,000/975,000. Maximum query latency was
22,383,494; 30,742,644; and 47,115,175 ns. Thus the widest arm exactly reached
the 975,000 aggregate and 800,000 minimum-query gates, but failed the 995,000
oracle-attainment gate and exceeded the 15,000,000-ns latency gate by 3.14x.
Its per-query unique-row inventory ranged from 510,939 to 825,730, showing that
the current two-plane key union is both too broad and still eight total hits
short across the 320 ground-truth opportunities.

The complete 100M-row resident projection is 2,938,017,816 bytes, 283,207,656
bytes below 3 GiB. Scientific execution took 21 seconds, peaked at 500,703,232
bytes RSS, and observed zero memory PSI and zero swap growth. The 11,688,328-byte
binary has SHA-256
`2e1c26d700fa832db58ba3d91a3a3d48858073b2716b09b4e3ee905f6318947c`.
The 3,493-byte canonical result has SHA-256
`4988ffd398f00f04e3f20ec6ef79ec97bc0773186c04ef0826c9807c3bf8c32b`;
the 5,421-byte Parquet evidence has SHA-256
`4741dfbf59f15dff373a354b4cdc4d936dfdf5baa8d0c65481f0532520828b14`.
The two cross-language Arrow planes are 2,819,458 and 267,318,994 bytes with
SHA-256 `a9121720c6cd7c860b0c62d1d5049b11903538ffe10a32ac7d82326df38b8b99`
and `b553bb3f2de1a80ac17088e78f889f4056a060af2c758ab81c40f8a9785e578d`.
All artifacts are rooted at
`s3://borsuk-bench-453182569524-euc1/research/v26-dual-pq-key/ba509c88e4ce83a0d67f74756694093cee02f6de/v26-dual-pq-key-preflight-20260903T070022Z-a0002/`.
The instance published its terminal and is terminated.

This rejects the tested two-plane key ranking and top-2,048 reducer, not PQ16
itself: prior global PQ16 evidence retains a perfect-recall frontier at wider
rank depth. Memory is no longer causal. The next no-spend work must improve
candidate concentration and replace full 65,536-key sorting with a bounded
selection kernel under the fast synthetic gate. No further Spot run is allowed
until that gate predicts both fewer candidate rows and a credible path below
15 ms. Recall gates remain unchanged; D3 and release claims remain fenced.

### Optimized dual PQ-key ladder rejects the two-plane family

Source commit `b45a13ed75346001ab638766abf2c87c4a99aa03` replaced full
65,536-key sorting with deterministic `(distance,key)` partitioning and replaced
candidate sorting/deduplication with a source-ordinal bitset. It fixed exact
reranking at 512 rows and evaluated 1,536/4,096/8,192 keys per plane. The
sub-minute local gate passed all ten steps in 48.822 seconds before the one
scientific run. The 11,689,992-byte release binary has SHA-256
`d5ce19ee7b909abcda2a34d97af177959e91b8ec2ae08e3ccea05bdb3956dbd4`.

The sole run used `causality` Spot instance `i-0d74a19b1a04f8855`
(`c7gd.4xlarge`, `eu-central-1a`) and SSM command
`743ee2de-34fe-4637-83f5-2d86f03d0ac2`. It authenticated and reused the same
9,990,000-row serving, 512-query, and 512-truth artifacts as the first run,
evaluated the fixed first 32 queries, selected exactly ten pages, and read zero
page bodies. The instance published a complete terminal and was immediately
terminated.

The aggregate/minimum/oracle-attainment ppm triples for the 1,536, 4,096, and
8,192 key arms were respectively 956,250/700,000/956,250;
971,875/800,000/971,875; and 975,000/800,000/975,000. Maximum query latency was
61,717,111; 129,692,964; and 226,264,900 ns. Unique candidates ranged from
510,939 to 825,730; 1,283,115 to 1,786,441; and 2,385,413 to 3,142,023.
Independent PyArrow recomputation over all 96 non-nullable evidence rows exactly
matched every stored aggregate, minimum, oracle-attainment, latency, and
candidate bound. No arm passed: the widest arm remained eight total hits short
of the 995,000-ppm oracle gate and exceeded 15 ms by 15.08x.

The scientific process completed in 31 seconds, peaked at 504,205,312 bytes
RSS, and observed zero memory PSI and zero swap growth. The 3,570-byte canonical
result has SHA-256
`4c0d235e736a2dd0e11f4fcf25b2ac1be0c338c305145c5059fba52656fb8add`;
the 5,422-byte Parquet evidence has SHA-256
`9e1270d57156ad65296541b2f2313fd2ff1f7fa84b8237f1fb1b0f45ed01f739`.
The regenerated Arrow offsets and ordinals retained SHA-256
`a9121720c6cd7c860b0c62d1d5049b11903538ffe10a32ac7d82326df38b8b99`
and `b553bb3f2de1a80ac17088e78f889f4056a060af2c758ab81c40f8a9785e578d`.
All evidence is rooted at
`s3://borsuk-bench-453182569524-euc1/research/v26-dual-pq-key/b45a13ed75346001ab638766abf2c87c4a99aa03/v26-dual-pq-key-preflight-20260903T072054Z-a0001/`.

This closes the fixed two-plane PQ-key family: widening increases work by
millions of rows without recovering the missing oracle hits, while depth 512 is
already independently sufficient under global PQ16 ranking. Memory remains
within bounds and is not causal. Quality and latency gates are not relaxed; D3,
competitor, and release claims remain fenced pending a fundamentally different
candidate-concentration design.

### Native tree frontier isolates the remaining concentration defect

Source commit `367442ba5526e0c9c2a896610eca79bec141fcf3` ran the existing
truth-only tree-frontier diagnostic against the frozen 9,990,000-row,
2,440-page native layout. It authenticated the 29,812,214-byte assignment
Parquet, both tree Parquets, all 512 external queries, and all 512 truth rows.
The diagnostic chose no production pages, opened no page body, and used truth
only to compute the best ten-page cover within each already-ranked frontier.

The width ladder's aggregate/minimum/oracle-attainment ppm triples were:
422,460/0/422,460 at 8 pages; 543,750/0/543,750 at 16; 661,132/0/661,132
at 32; 782,812/100,000/782,812 at 64; 881,054/300,000/881,054 at 128;
949,414/300,000/949,414 at 256; 986,523/800,000/986,523 at 512;
999,218/900,000/999,218 at 1,024; and 1,000,000/1,000,000/1,000,000 at
2,048 and at the exhaustive 2,440-page control. Width 1,024 is therefore the
smallest tested frontier passing the unchanged 975,000/800,000/995,000 quality
gates, while width 2,048 is perfect.

The sole run used `causality` Spot instance `i-04af8bc2de81934dc`
(`c7gd.4xlarge`, `eu-central-1a`) and SSM command
`6d001771-507c-4ee2-90a6-5abb416e9e6c`. The scientific process completed in
one second, observed zero memory PSI and zero swap growth, and published an
867,983-byte canonical result with SHA-256
`f32b1c32c607739ab23014e9c674aecb62d3ac663cf4629f744cace082e71da2`.
The 11,966,336-byte executable has SHA-256
`29c8408f2b784d3403c6bb0c213c457606ecc9d79ebc6f6f7138d8bda8567f91`.
Evidence is rooted at
`s3://borsuk-bench-453182569524-euc1/research/v26-native-tree-diagnostic/367442ba5526e0c9c2a896610eca79bec141fcf3/v26-native-tree-20260903T072941Z-a0001/`.
The instance published its terminal and is terminated.

This accepts the native page geometry and rejects the current random-projection
tree as a sufficiently concentrated serving frontier. Scanning 1,024 pages at
8,192 rows per page is not a credible 15-ms path. The next falsifier must rank
the same leaves with a query-independent geometric representation and show a
passing frontier at no more than 256 pages before any row-code latency run.
Recall gates remain unchanged; D3 and release claims remain fenced.

### Global page centroids do not concentrate the native frontier

Source commit `edebedba80421260bd70e9868d56210fdf2e85b2` evaluated one
query-independent normalized centroid per physical page against the same
authenticated 9,990,000-row construction data, 2,440-page layout, 512 external
queries, and 512 truth rows. For each query it ranked all page centroids before
truth joined the diagnostic, then measured the exact best ten-page cover inside
each fixed frontier. It read no page body and emitted claim-ineligible canonical
JSON. The preceding attempt at source `b96085ca8e5231e07266e3cd97e3cd687f70107d`
closed before loading construction data because the ten-page diagnostic reused
an eight-page layout gate. A 5.76-second local regression now locks the correct
ten-page authority before any further remote execution.

The aggregate/minimum/oracle-attainment ppm triples were
440,625/0/440,625 at 8 pages; 570,898/0/570,898 at 16;
708,984/100,000/708,984 at 32; 823,632/200,000/823,632 at 64;
911,523/300,000/911,523 at 128; 966,210/400,000/966,210 at 256;
991,015/800,000/991,015 at 512; 999,609/900,000/999,609 at 1,024;
and 1,000,000/1,000,000/1,000,000 at 2,048 and exhaustive 2,440.
Width 1,024 is again the smallest passing frontier. At width 512, the minimum
query gate passes but aggregate recall remains below 975,000 and oracle
attainment remains below 995,000. Width 256 is materially farther away.

The sole scientific run used `causality` Spot instance
`i-0b6a98cd15d15894a` (`m7gd.4xlarge`, `eu-central-1c`) and SSM command
`54d7efce-5616-405f-aa19-98c7639f08d4`. The diagnostic took 29 seconds,
peaked at 4,090,126,336 bytes RSS, and observed zero memory PSI and zero swap
growth. The 11,983,768-byte executable has SHA-256
`cafd2ab0162c89e59d7c72a1c39ab085b33d0307f8f1f637a7e8a4b1cdac9f1a`.
The 871,555-byte result has SHA-256
`6505e0d5d58806248cb07bb5ab001d22ace2d5680fb5dc71503bcb4697961be2`.
Evidence is rooted at
`s3://borsuk-bench-453182569524-euc1/research/v26-global-centroid-frontier/edebedba80421260bd70e9868d56210fdf2e85b2/v26-global-centroid-20260903T075719Z-a0002/`.
The instance published its terminal and was terminated.

This rejects a single mean centroid per page as the missing concentration
mechanism. It also shows that the random-projection tree is not uniquely causal:
both representations require a 1,024-page frontier on the frozen geometry. The
next no-spend gate must represent multimodal within-page structure and pass at
no more than 256 candidate pages before any serving-latency or full-suite run.
Recall, oracle-attainment, D3, competitor, and release gates remain unchanged.

### Sixteen page modes halve but do not close the native frontier

Source commit `f3b43e4350839d96999e9bdaefd4cb1f6ad23870` replaced each
single page mean with a deterministic query-independent 2/4/8/16-mode ladder.
It ranked every one of the 2,440 pages by nearest mode, then joined truth only
to measure the exact best ten-page cover inside each fixed frontier. The run
authenticated the same 9,990,000 construction rows, assignments, trees, 512
queries, and 512 truth rows as the preceding diagnostics. It opened no page
body and stored all 20,480 samples in typed Parquet rather than bulk JSON.

At 256 candidate pages, the aggregate/minimum/oracle-attainment ppm triples for
2, 4, 8, and 16 modes were respectively 966,601/500,000/966,601;
972,851/500,000/972,851; 976,953/400,000/976,953; and
979,101/500,000/979,101. None passed. At 512 pages, the corresponding triples
were 993,359/800,000/993,359; 992,578/800,000/992,578;
994,531/800,000/994,531; and 996,093/800,000/996,093. Only the 16-mode arm
passed all unchanged gates. Every arm passed by width 1,024. Multimodal page
summaries therefore move the passing frontier from 1,024 to 512 pages, but do
not yet reach the preregistered at-most-256-page concentration boundary.

The sole run used `causality` Spot instance `i-0b971d9f0e4923da7`
(`m7gd.4xlarge`, `eu-central-1c`) and SSM command
`e398d309-9426-4756-9ff9-b32d0aac0737`. Scientific execution took 60 seconds,
peaked at 4,259,348,480 bytes RSS, and observed zero memory PSI and zero swap
growth. The 12,004,088-byte executable has SHA-256
`9b983f07abff6c4979c2ed1eea32c1ccbcbac03852f4d3ca703cff15962167f1`.
The 6,690-byte result has SHA-256
`0a24f13d0b7459cbbf6cc721deb0c51a033bc2d83b9133b0e5a40e07a1bfe320`;
the 169,714-byte Parquet evidence has SHA-256
`e975bd3d6d99fb7455f437d7c48545383de2ec51b2d5c90010a6a7cbfc9d52e3`.
An independent PyArrow pass authenticated all 20,480 rows, 40 arm groups, and
17 physical fields. Evidence is rooted at
`s3://borsuk-bench-453182569524-euc1/research/v26-global-page-mode-frontier/f3b43e4350839d96999e9bdaefd4cb1f6ad23870/v26-global-page-modes-20260903T081207Z-a0001/`.
The instance published its terminal and was terminated.

Source commit `f160d32df8e66344c635d652eb082aed521ac54d` then extended the
same fixed ladder to 32 and 64 modes. At 256 pages, K32 reached
982,226/500,000/982,226 and K64 reached 986,523/700,000/986,523 ppm. Both
failed. At 512 pages, K32 passed with 996,679/800,000/996,679, while K64's
996,875/700,000/996,875 failed the minimum-query gate. This non-monotone tail
rules out further blind K widening. The run used `causality` Spot instance
`i-04c85a09351fe2add` (`m7gd.4xlarge`, `eu-central-1c`) and SSM command
`d3bf5812-f37f-4a83-a62d-75f43251f845`; it took 73 seconds, peaked at
4,259,344,384 bytes RSS, and observed zero memory PSI and zero swap growth.
The executable SHA-256 is
`5145fd58036fe27ae4db44baacaaa61a09e93789ee821f6c6deb1db29e2f597e`;
the result SHA-256 is
`b6afa58dc95317d22512b46ae96d2bac575a57b21d60e20f8d2fdc23ff3dc6bd`;
and the 30,720-row Parquet evidence SHA-256 is
`5e0ab67da6765902b178a0c814e9acd440638e13ecfaf1e5013fb9987d08e3f3`.
Independent PyArrow recomputation authenticated all 60 arm groups. Evidence is
rooted at
`s3://borsuk-bench-453182569524-euc1/research/v26-global-page-mode-frontier/f160d32df8e66344c635d652eb082aed521ac54d/v26-global-page-modes-20260903T082004Z-a0002/`.
The instance published its terminal and was terminated.

This establishes within-page multimodality as causal but rejects the complete
2/4/8/16/32/64 ladder as sufficient concentration at the at-most-256-page
boundary. The next fast path must bypass page-frontier concentration and test
the already implemented global packed-PQ16 scan plus bounded exact rerank at
native scale. D3 and release claims remain fenced.

### Native global PQ16 is near the quality gate but misses the latency budget

Source `19ac7c377a5fd065d520ff785c0406dd2d9520db` built the native packed-PQ16
serving representation from the authenticated 9,990,000-row construction data
and 2,440-page assignment. The first preserved attempt on `causality` Spot
instance `i-011b4031bd0a66dbf` (`m7gd.4xlarge`) failed before construction
because its 884.8-GiB instance-store device was not mounted. The corrected
build took 330 seconds, peaked at
8,166,158,336 bytes RSS, and observed zero memory PSI and swap growth. Its
6,100-byte manifest has SHA-256
`b09813c92e6522ffec1698b26f6bd87939ec52994e6b20522ac697a4fa1fc2ac`;
the 100-million-row resident projection is 2,937,537,416 bytes. Evidence is
rooted at
`s3://borsuk-bench-453182569524-euc1/research/v26-pq16-native-serving/19ac7c377a5fd065d520ff785c0406dd2d9520db/v26-pq16-native-build-20260903T083000Z-a0002/`.
The instance was terminated after its terminal.

The first truth-bound 32-query screen at source
`016426118bd1eb99ac438ae531e7f54ea8a3a0b5` selected exactly ten pages after a
global scan and exact rerank of 2,048 rows. It recovered 316 of 320 truth
neighbors: aggregate recall 987,500 ppm, minimum-query recall 900,000 ppm, and
oracle attainment 987,500 ppm. Aggregate and minimum-query recall passed, but
oracle attainment missed the unchanged 995,000-ppm gate by three required
hits. Its p50/p95/maximum latencies were 18,317,209/19,248,000/19,282,390 ns,
so it also failed the 15-ms gate. It opened no page body and remained
claim-ineligible.

A bit-exact four-row interleaving experiment did not improve the result and was
removed. Source `46501ee996bae527128c82f603451601aef98335` added authenticated
stage timings and reran the same screen on `causality` Spot instance
`i-0b9c34077eb861a82` (`m7gd.4xlarge`, `eu-central-1b`) with SSM command
`0eb5f8b0-bafa-443f-b98c-f411103e99f5`. Global ADC alone measured
15,843,732/16,640,799/17,412,229 ns p50/p95/maximum; exact Arrow rerank measured
3,130,375/3,229,063/3,238,975 ns. Total p50/p95/maximum was
19,041,016/19,726,433/20,580,732 ns, with the same 316/320 quality result.
Peak process-group RSS was 258,887,680 bytes, memory PSI was zero, and swap did
not grow. The 2,302-byte canonical result has SHA-256
`bf440eb592f6ccc945d7ea11b2c4e12776e3663948c3742cc0a50b6841ff764e`;
the 4,234-byte Parquet evidence has SHA-256
`d796b38cb073eb99aeb1002f54cf1be1b6380cec1e8e964401cfc4aba9c95d4b`.
Evidence is rooted at
`s3://borsuk-bench-453182569524-euc1/research/v26-pq16-native-quality/46501ee996bae527128c82f603451601aef98335/v26-pq16-native-quality-20260903T092519Z-a0001/`.
The instance was terminated after its terminal.

This rejects scalar 8-bit PQ16 global ADC as the final serving path: its scan
alone exceeds the total latency budget, while increasing rerank depth to recover
quality would add work. The next bounded falsifier is a 128-bit, 4-bit
fast-scan representation with 32 three-dimensional subquantizers, retaining the
16-byte row-code budget while replacing random 256-entry scalar lookup tables
with SIMD-resident 16-entry tables. Recall, 15-ms latency, 3-GiB projection,
page budget, D3, competitor, and release gates remain unchanged.

### Native PQ4 fast scan passes the sealed V26 holdout

Source `82d2f6411cb5e58635da838c4cfdb31bd01f8fb7` built the fixed
32-subquantizer, 16-centroid, 16-byte-per-row PQ4 representation over all
9,990,000 rows. The manifest projects 2,336,975,744 resident bytes at 100
million rows, below the 3-GiB serving bound. Its 7,170-byte Arrow codebook and
162,417,514-byte Arrow code plane have SHA-256
`68baeace6e8c24b39009274c1c774f740bd1c88f47b3999652ea3913063b6e3f`
and `1bc301160860a8151d53373c8dcadcb43fcb4f4d95ef5338ecfafd3533a811c7`.
The 2,426-byte manifest has SHA-256
`169005e8978dc4a1a5865dd59968014a4e91a52190a4e0ae6cca6d6a7b7d43e3`.
The `causality` Spot build instance `i-073590a493de4fb12` completed and was
terminated; the build monitor peaked at 8,210,640,896 bytes RSS with zero
memory PSI and zero swap.

The 32-query development frontier at source
`15faf2351a7de825ba31f3ac13f7b90ac4f03e6c` selected the smallest passing
depth of 2,048 rows. Its depth-512/1,024/2,048/4,096 aggregate recall was
953,125/984,375/996,875/1,000,000 ppm; minimum recall was
600,000/800,000/900,000/1,000,000; and oracle attainment matched aggregate
recall. Depths 2,048 and 4,096 passed. The 3,088-byte canonical result has
SHA-256 `90b89f9da7afa61307e769a6eeb3576e76265e7d272be1c53904dcd2eaa25871`;
the 6,347-byte Parquet evidence has SHA-256
`bd7b71c434c1af21273c4424c8a9f678bddb390bd2f6f7043d34876e788da57f`.
The development serving screen at source
`7f799f32404f946c572cd120840aac9d9d3e920a` measured p50/p95/maximum of
12,938,087/13,008,570/13,028,312 ns and passed the 15-ms gate. Its result and
Parquet evidence SHA-256 values are
`08fa0cab8e18a1e84a288a81d3c5e0acb9e730eb6caf930b5d91986c3e579f20`
and `fc9ccb1b23807e5810b7c0065b52ed9a2fac11e9543c3d7dc62a720b89e256a7`.
Both Spot instances published complete terminals and were terminated.

The first sealed-launch attempt, instance `i-02c735cfa197ce2c5`, failed before
any input download or scientific process because it lacked the required IAM
profile; it was terminated and contributes no scientific result. The next run
at source `4b6eedc85ce087a8782b960a5a71d774cf623df2` on Spot instance
`i-0bd77383d60361410` authenticated and measured all sealed queries 32 through
511. It produced 996,458 ppm aggregate recall, 800,000 ppm minimum recall, and
996,458 ppm oracle attainment, but schema v1 incorrectly applied the 15-ms
release target to the single maximum observation. One 16,546,121-ns outlier
made `passed=false` even though independently recomputed p99 was 13,313,830 ns
and only one of 480 samples exceeded 15 ms. The run is preserved with result
SHA-256 `249bec8512ca7f72a0246c4825786107b6f3d706c4bce069ac449e8fca093d9c`
and Parquet SHA-256
`f96acd6170804df6e4d3e3f4ac037164ed4c2c2cbfe1b568c6788f4afda70152`;
the instance was terminated.

Source `836aa9b1332e55fd4fca9219a410282f09ddc1d1` corrected only that contract:
schema v2 retains maximum latency as evidence and gates the preregistered p99.
The final sole sealed run used `causality` Spot instance
`i-0a42fad21023b07e4` (`r7gd.4xlarge`, `eu-central-1b`). Across all 480 sealed
queries it achieved 996,458 ppm aggregate recall, 800,000 ppm minimum recall,
996,458 ppm oracle attainment, and 13,863,178-ns p99 against literal gates of
975,000/800,000/995,000 ppm and 15,000,000 ns. Maximum latency was 15,360,104
ns. The arm used depth 2,048, selected exactly ten pages, read zero page bodies,
and remained claim-ineligible. Peak observed RSS was 191,504,384 bytes, memory
PSI was zero, and swap did not grow. The 2,910-byte canonical result has
SHA-256 `c2a13d6f11877e94ba027a282f6499a0b6fe89a23d27aafbfbb0fe94483cd8b6`;
the 15,953-byte Parquet evidence has SHA-256
`193804fe47a1a422a7359c8321e818337c812a13b5399fe2913a4aa7597d512f`.
Evidence is rooted at
`s3://borsuk-bench-453182569524-euc1/research/v26-pq4-fast-scan/836aa9b1332e55fd4fca9219a410282f09ddc1d1/v26-pq4-holdout-20260903T112336Z-a0003/`.
The terminal is complete and the instance is terminated.

### Direct-row PQ4 diagnosis rejects page decoding and qualifies depth 3,072 for fresh validation

Source `8ce45d12a69417a252f67a76c011f0ffef90911e` added independent
direct-row Recall@10 measurement to the existing page-containment holdout. The
sole `causality` Spot run used instance `i-08c41f390a1b66c50`
(`r7gd.4xlarge`, `eu-central-1a`), authenticated the unchanged PQ4, query,
truth, and exact-vector artifacts, and evaluated all 480 queries. At depth
2,048 it produced 993,541 ppm aggregate direct-row recall and 700,000 ppm
minimum recall, while page containment remained 996,458/800,000/996,458 ppm
aggregate/minimum/oracle-attainment. Direct-row p99 was 13,231,400 ns and
maximum was 14,233,360 ns. The run opened zero page bodies, peaked at
191,590,400 bytes process-group RSS, and observed zero memory PSI and swap.
The 3,031-byte canonical result has SHA-256
`b86d1c37ed38cc4095917068c0bb9322a0345ea090e5fafff785a0785c78db37`;
the 16,662-byte Parquet evidence has SHA-256
`e2a7d8dfcee6fd9136781fe13c4df3d44029725fab2e150247fec33f52c2a269`.
Evidence is rooted at
`s3://borsuk-bench-453182569524-euc1/research/v26-pq4-fast-scan/8ce45d12a69417a252f67a76c011f0ffef90911e/v26-pq4-row-recall-20260903T133158Z-a0001/`.

Two subsequent instances produced no scientific result and were terminated.
Instance `i-0152e08f0ea84856e` at source
`e794ffd54e0280a4b815842516f88a4b152ba642` stopped at the ranker's stale
depth allowlist; instance `i-08acf979e0fd7be85` at source
`61f7d305a5e5ac513dfe2b0999135aac8d1021ba` stopped at the sample validator's
duplicate 2,048-row guard. Focused RED/GREEN tests locked both repairs before
the next attempt.

The completed depth-3,072 run used source
`e538eb51ffb67cb9a7d5fd07f279f2d92cf16cc9`, binary SHA-256
`78369970fc1a2213811871f564f34c196dc5a3c67b8cfe295e8b144b84c5a0eb`,
and `causality` Spot instance `i-0ba8a5eeb758f2a45` (`r7gd.4xlarge`,
`eu-central-1a`). Direct-row aggregate recall increased to 997,291 ppm; 479
of 480 queries reached at least 800,000 ppm, with the same single query 493 at
700,000 ppm. P99 was 14,863,808 ns and maximum was 15,134,075 ns. Page
containment was 998,541/800,000/998,541 ppm, but remains non-serving evidence.
The run opened zero page bodies, peaked at 193,851,392 bytes process-group RSS,
and observed zero memory PSI and swap. The 3,034-byte canonical result has
SHA-256 `324eed01edb363223056c49505bdf365f8071c972683b935f3c0dac33b07624c`;
the 16,330-byte Parquet evidence has SHA-256
`4b595478e43d6db4331d8b41ff36d7627e9c8f2779ad5d4f93a5bb5a71ab04a1`.
Evidence is rooted at
`s3://borsuk-bench-453182569524-euc1/research/v26-pq4-fast-scan/e538eb51ffb67cb9a7d5fd07f279f2d92cf16cc9/v26-pq4-row-depth3072-20260903T135002Z-a0001/`.
The terminal is complete and the instance is terminated.

This evidence rejects page-body decoding and fixes direct exact-row return as
the production boundary. Queries 0..511 are burned development evidence. The
next sealed cohort uses unused query ordinals and gates 995,000-ppm aggregate
Recall@10 plus 997,500-ppm compliance with an 800,000-ppm per-query floor,
retaining the 15-ms p99 and 3-GiB process limits. Absolute minimum recall stays
visible but no longer lets one observed query drive architectural overfitting.
D3, distributed 100-million-row latency, and competitor claims remain fenced.

Commit `d10f3106314dc7a157341b7702a3e3979722003b` repaired four stale pre-release
test contracts exposed by the final workspace gate and added them to one
fail-fast release-contract selector. The smoke gate passes in 3.346 seconds;
the four release contracts pass together in 23.84 seconds; strict locked
workspace/all-targets Clippy is clean; and the final locked workspace/all-targets
test run passed every executed target, including 1,209 core tests (four
ignored) and all 87 `borsuk-v26` library tests. Development now uses the smoke
gate per edit, focused/release-contract gates at boundaries, and the exhaustive
workspace gate only once per release candidate.

### Production PQ4 shard passes the sealed external-query holdout

Source `f8f23ad4f4cca3a8173b4420034b70f25488149c` moved the bounded 3,072-row
exact rerank into the existing 16-thread Rayon pool without changing final
distance/source ordering. On the burned queries 0 through 511, `causality`
Spot instance `i-0653e9592306dee89` (`c7i.4xlarge`, `eu-central-1c`) measured
997,265-ppm aggregate Recall@10, 998,046-ppm compliance with the 800,000-ppm
per-query floor, 15,617,910-ns p99, and 16,365,560-ns maximum latency. This was
a 15.8-percent p99 reduction from the same-host 18,559,407-ns baseline, but it
remained 617,910 ns above the unchanged release gate. The 1,060-byte result has
SHA-256 `7910ebf2254e379444f7fc165df94ce4aa621e2ccfdfa9503ea002118b111725`;
the 59,371-byte Parquet samples have SHA-256
`7604d7e46d24990c7975d5fc658eb9066cffee28224fbb9eaf1722e5d8f26b24`.

Source `163996525bb35cb24647ede934c5ca67b1d5e577` then removed a redundant
serial pass over all 9,990,000 query scores by merging and reusing the
histograms already produced by the parallel scan. Burned development instance
`i-02a24e12549edbf44` (`c7i.4xlarge`, `eu-central-1c`) preserved the identical
997,265/998,046-ppm quality values while reducing p99 to 12,886,594 ns and
maximum latency to 13,511,357 ns. Peak process RSS was 342,228,992 bytes;
monitor RSS peaked at 222,609,408 bytes; memory PSI and swap were zero. Its
1,059-byte result has SHA-256
`ff7f6cafe1741e2e996a7f784e70c515607df5244e01cdd9bcc82d3299b222bb`;
the 59,371-byte Parquet samples have SHA-256
`79dcda3a8330e3cf2af0d36bcc22c0c27a0b0e32e652dc11e3dca27d16e18272`.
Both instances published complete terminals and terminated. An intervening
instance `i-02a474440f9fefcde` omitted the benchmark IAM/network launch fields,
failed before science, published a claim-ineligible failure receipt, and
terminated; it contributes no measurement.

Source `d0632a8c14cdf942aacbeed172e37aad0ce3dc21` froze the independently selected
16-thread configuration and evaluated the untouched sealed query ordinals 512
through 991 once. Spot instance `i-038db8a7f462f525d` (`c7i.4xlarge`,
`eu-central-1c`) achieved 997,708-ppm aggregate Recall@10, 1,000,000-ppm floor
compliance, 800,000-ppm minimum recall, 11,465,765-ns p99, and 11,863,230-ns
maximum latency. These pass the literal 995,000/997,500/800,000-ppm quality
gates and the 15,000,000-ns p99 gate. Peak process RSS was 342,233,088 bytes,
well below 3 GiB; monitor RSS peaked at 202,362,880 bytes; memory PSI and swap
were zero. The 1,065-byte canonical result has SHA-256
`ce6fac4609987a158fc3a99c112ac496df541ecbdd65572f246da54a60b39ada`;
the 55,732-byte Parquet samples have SHA-256
`a9c28208e7d8894f2674a0780663ae05206d049968b331c7a8cbae00ec923345`.
Evidence is rooted at
`s3://borsuk-bench-453182569524-euc1/research/v26-pq4-production/d0632a8c14cdf942aacbeed172e37aad0ce3dc21/v26-pq4-sealed-20260903T171227Z-a0015-x86/`.
The terminal is complete and the instance terminated.

This promotes the immutable 9.99-million-row PQ4 shard boundary: exact-row
quality, memory, and latency pass on sealed external queries without page-body
reads. The result remains claim-ineligible until the same frozen code is
qualified as a roughly ten-shard 100-million-row deployment, including bounded
parallel fan-out and deterministic global top-k merge. D3 and competitor claims
remain fenced.

Final repository assurance is rooted at
`b797352fb673007441c1723e8f87e5c217434845`. The only change after the sealed
measurement was a test-only replacement of wall-clock sleeps in the maintenance
lease-renewal test with an observed-renewal handshake; production search and
storage code are byte-identical to the measured candidate. On `causality` Spot
instance `i-04c02c30d37b99037` (`r7g.4xlarge`, `eu-central-1c`), strict locked
workspace/all-targets Clippy exited zero, followed by the complete locked
workspace/all-targets test gate with 2,053 passed, zero failed, and 23 explicitly
ignored tests across 102 result suites. The test process peaked at 3,161,592 KiB
RSS; memory PSI and swap remained zero. The 195-byte canonical assurance
terminal has SHA-256
`22d65dae7fb8448f27c5ea210974c7ae1fe80104dfa658cbd7fea39a5cb8c08a`;
the 4,519-byte compressed Clippy log has SHA-256
`120c23bf43e10add9db7aeff190588da08ec34a2d9e0b4817f8a8673e18ca1c2`;
and the 47,124-byte compressed test log has SHA-256
`6a9a44f558d3eaa9f75bc5983928d28ff1c8af29298f8222102109bba9dbd685`.
They are preserved under
`s3://borsuk-bench-453182569524-euc1/research/v26-pq4-production/b797352fb673007441c1723e8f87e5c217434845/assurance/`.
The builder terminated immediately after evidence publication. This assurance
closes the repository-quality checkpoint only; it does not relax the explicit
100-million-row fan-out, package, object-store, D3, or competitor-claim fences.

### V28 secondary-leaf control isolates compressed ranking loss; V29 page graph is rejected

Source `dbd878b2f77a8702585cc52b0aa59a9b237367c2` evaluated the V28
hierarchical S3 layout on the burned 100,000-row Deep Image development
fixture (32 queries, 512 rows per page, 323 materialized pages, and 46,761,076
construction bytes streamed). The variable-rate 24-byte PQ8 arm reached
996,875-ppm aggregate recall, 900,000-ppm minimum recall, and 31/32 perfect
queries with a five-percent refinement fraction. Its projected 100-million-row
resident footprint was 2,625,266,208 bytes. The 1,295-byte claim-ineligible
result has SHA-256
`4689f66fab78b74b91e3c96ae91516f8647dcd8248a27a8bb7c077c12660f663`
and is preserved at
`s3://borsuk-bench-453182569524-euc1/research/v28-s3/dbd878b2f77a8702585cc52b0aa59a9b237367c2/v28-leaf-variable-rate-pq8-20260904T000800Z/result.json`.

A query-independent secondary-leaf candidate route did not change compressed
quality: its best arm also reached 996,875/900,000 ppm and 31/32 perfect
queries, while increasing the projected resident footprint to 3,025,528,356
bytes. The sole missing neighbor was query 30, source row 75,809: primary leaf
209, secondary leaf 52, physical page 266. Under the secondary route the row
was present at candidate and first-page-evidence rank 82, proving that the
hierarchy and page layout contained it before the ten-page cutoff. The
1,367-byte result has SHA-256
`30d7821d714865ceba51a7c5b8416319f00fd144f5f1e7570f6629a56a22a3b0`
and is preserved at
`s3://borsuk-bench-453182569524-euc1/research/v28-s3/dbd878b2f77a8702585cc52b0aa59a9b237367c2/v28-secondary-leaf-pq8-diagnostic-20260904T005000Z/result.json`.

The exact-distance control over the identical secondary-leaf candidates
reached 1,000,000-ppm aggregate and minimum recall with 32/32 perfect queries,
at the same ten-page serving boundary. Its 1,317-byte result has SHA-256
`61c08c078b0113b25afd1475a194168ec1ea00db70bb07366781b16bc71d09e0`
and is preserved at
`s3://borsuk-bench-453182569524-euc1/research/v28-s3/dbd878b2f77a8702585cc52b0aa59a9b237367c2/v28-secondary-leaf-exact-control-20260904T003500Z/result.json`.
This is a causal diagnosis: the remaining miss is caused by compressed
candidate ordering, not by failure to explore the relevant region or by the
fixed page budget. It does not authorize query-trained routing or a quality
claim on the burned cohort.

V29 tested a query-independent degree-16 page graph derived from the secondary
leaf incidence, using eight routed seeds plus two frontier pages in one fetch
wave. It regressed to 987,500-ppm aggregate recall, 800,000-ppm minimum recall,
and 29/32 perfect queries. The 1,299-byte claim-ineligible result has SHA-256
`a7c729004f385d6060360dfd6c77d0a84b1897e11fede415568b52707b008bf6`
and is preserved at
`s3://borsuk-bench-453182569524-euc1/research/v28-s3/dbd878b2f77a8702585cc52b0aa59a9b237367c2/v29-boundary-page-graph-20260904T013000Z/result.json`.
V29 is rejected and removed from the current pre-release production tree. Its
immutable result remains negative evidence. No 100-million-row scale run,
sealed-query run, D3 campaign, or competitor claim is authorized by these
burned 100,000-row experiments.

The next candidate must preserve the successful query-independent hierarchy
and exact S3 page boundary while improving compressed ordering. It must be
screened first on small immutable fixtures, keep the resident projection below
3 GiB at 100 million rows, select exactly a bounded page set, and read only
those Arrow/Parquet page objects from S3 rather than materializing the corpus
locally. Cold Standard S3 page latency is evaluated separately from CPU search;
the 15-ms target is a resident-index/hot-cache target unless a lower-latency
object tier is explicitly qualified.

### Sparse secondary placement and high-error f16 refinement are rejected

Two bounded query-independent V30 spikes tested whether the remaining V28
compressed-ordering loss could be repaired without widening the ten-page S3
read boundary. Both used the same burned 100,000-row Deep Image fixture and 32
queries, streamed 46,761,076 authenticated page bytes, emitted
`claim_eligible=false`, and left the exact vector corpus only in immutable S3
page objects. They are diagnostic evidence, not a new persistent format.

The sparse-secondary-placement spike duplicated the rows nearest an alternate
leaf boundary into code-sorted pages owned by that alternate leaf. At 0/5/10/15
percent replication it reached 981,250/984,375/987,500/981,250 ppm aggregate
recall, with 700,000/800,000/900,000/700,000 ppm minimum recall. The best
ten-percent arm projected 2,732,766,208 resident bytes at 100 million rows but
recovered only 316/320 hits. The 1,411-byte result has SHA-256
`7ae18479df657d61ab08e99165827996228783b5998a9bce9ddb5ac0e8c6e098`
and is preserved at
`s3://borsuk-bench-453182569524-euc1/research/v30-s3/c0ac66590c834d48d998f51c0af1f8c27d36cb4f/v30-sparse-secondary-replica-20260904T012438Z/result.json`.
The `causality` Spot worker `i-01803c5a52da68dd7` completed and terminated.

The high-error-refinement spike selected rows solely by their PQ
reconstruction error and retained normalized f16 vectors for the selected
0.5/1/2 percent. Every arm remained exactly at the 981,250-ppm baseline with
700,000-ppm minimum recall and 28/32 perfect queries; the two-percent arm
projected 2,897,266,208 resident bytes. This proves that the relevant page
mistakes are not concentrated in the small query-independent tail of largest
PQ reconstruction errors. The 1,324-byte result has SHA-256
`e0050a76ded6656e8fd0962e25b99f833630f6e44f9ae978b8e8ba563efbc91b`
and is preserved at
`s3://borsuk-bench-453182569524-euc1/research/v30-s3/c0ac66590c834d48d998f51c0af1f8c27d36cb4f/v30-high-error-f16-20260904T013000Z/result.json`.
The `causality` Spot worker `i-0151a8668ae0ed8b3` completed and terminated.

Neither representation proceeds to production. Together with the earlier V28
ladder, these results close sparse physical replication, small high-error exact
sidecars, page-count expansion, page graphs, the previously tested global
additive-PQ arm, OPQ, multiview PQ, radius routing, and alternate page-score
aggregation on this burned fixture. The best historical variable-rate
observation remains 996,875-ppm aggregate recall, 900,000-ppm minimum recall,
31/32 perfect queries, ten pages, and a reported 2,625,266,208-byte
100-million-row projection. It is not yet a production candidate.

Post-run reproducibility review found that the variable-rate result and
terminal did not preserve the evaluator, its input manifest, or the exact code
composition. The surrounding ladders establish that the variable-rate
zero-percent arm exactly matches the separate 24-byte PQ8 residual arm, while
the separately tested additive-PQ arm regressed. The `PQ8` label, exact
25.2-byte average, and quality progression therefore identify a 24-byte/48-byte
PQ8 replacement as the credible interpretation, but it still requires exact
reproduction because its evaluator was not preserved. The committed V28 codec
implements PQ4 rather than that measured interpretation. The
2,625,266,208-byte projection also increases by exactly the refinement payload
and does not expose bitmap, rank, sparse-range, or framing components. The arm
is therefore a promising historical observation, not a production candidate.
The failed f16 sidecar does not settle this question: that sidecar used a
different 981,250-ppm page-selection baseline and did not alter approximate
candidate/page ordering, whereas a residual refinement would alter that
ordering before page selection.
V30 must first reproduce the fixed PQ8 replacement interpretation on the same
burned fixture with committed code and complete authority, then freeze its
smallest passing arm before any untouched-cohort run.

### V30 reproduces the archived variable-rate PQ8 mechanism exactly

The committed V30 reproduction evaluator at source commit
`9e1ed2e13b8b6c04cf2084f345983269a2dd6be8` authenticated the frozen V27
100,000-row page manifest, leaf postings, leaf centroids, and 10,000-row query
Parquet object, streamed the 46,761,076 bytes of registered Arrow page bodies
only into one disposable `causality` Spot worker, and independently recomputed
truth for the first 32 queries. It used the fixed 24-by-4D and 48-by-2D,
256-centroid PQ8 replacement interpretation with one base-code page layout,
leaf beam 64, candidate depth 12,288, and exactly ten selected pages.

| refined fraction | hits / 320 | aggregate ppm | minimum ppm | perfect queries |
|---:|---:|---:|---:|---:|
| 0% | 318 | 993,750 | 900,000 | 30 / 32 |
| 5% | 319 | 996,875 | 900,000 | 31 / 32 |
| 10% | 319 | 996,875 | 900,000 | 31 / 32 |
| 20% | 319 | 996,875 | 900,000 | 31 / 32 |

These four observations exactly match the archived V28 result. Five percent
is therefore frozen as the smallest passing reproduction arm; the 24/48-byte
replacement mechanism, not the earlier committed PQ4 interpretation or the
separately rejected additive-PQ arm, proceeds to production TDD. The burned
result remains claim-ineligible and does not authorize a 100-million-row build.

The reproduction's maximum observed selected-page payload was 1,986,668 bytes
and its maximum selected-leaf scan was 39,612 codes. Whole-worker elapsed time,
including dependency installation, was 121.394 seconds; peak process RSS was
281,704 KiB, memory PSI full avg10 remained 0.0, and swap delta was zero. The
1,765-byte per-query Parquet evidence has SHA-256
`ad6b92b515efb748ad2627e45a3b57c87f613367b472546575ed2ceac101909e`.
The 2,415-byte canonical result has SHA-256
`01e33cf71f571e28c799c2d4ac260b2b5f348c0221204fcc4a84226200510695`
and is preserved at
`s3://borsuk-bench-453182569524-euc1/research/v30-s3/9e1ed2e13b8b6c04cf2084f345983269a2dd6be8/v30-pq8-reproduction-20260904T022040Z/result.json`.
Its canonical terminal is preserved beside it. Spot worker
`i-09f05e6479860ee03` completed and terminated. A preceding attempt at commit
`f7f7f941fe4dd66d1082c2e44e37d8a2fef367cd` failed before any page or
scientific work because the direct CLI passed its filename into the closed
parser; its failed terminal is preserved, the regression is covered, and its
worker also terminated.

### V30 flat leaf-to-page-centroid routing is rejected at 100K

Source `0b796e97224d89b2c47bb15b3495770d556d2658` replaced the production
root/PQ candidate router with an exact scan of all resident leaf centroids,
followed by a bounded scan of geometric page centroids owned by the best 192
leaves. The query path selected exactly 16 pages and performed exact reranking
only inside the downloaded Arrow pages. It did not download or materialize the
corpus. The query-independent construction produced 907 pages from the frozen
100,000-row Deep Image development fixture; its 2,752-byte manifest has
SHA-256 `36f410fdaf6cab3a107b7851ea8a1e74c436154e20698c49518cafa2551a56cc`.

The sealed 32-query evaluation reached 273/320 hits: 853,125-ppm aggregate
Recall@10, 300,000-ppm minimum recall, 843,750-ppm floor compliance, and 15/32
perfect queries. It therefore failed the 995,000-ppm aggregate,
800,000-ppm minimum, 997,500-ppm floor-compliance, and 31/32 perfect-query
quality gates. This rejects a single-centroid-per-page ranking signal for the
geometric layout; no 100-million-row build is authorized.

Routing metadata work itself was small: maximum routing elapsed time was
255,522 ns, maximum routing CPU time was 10,002,430 ns, no row PQ codes were
scanned, and no row candidates were retained. Every query issued 16 Standard
S3 GETs. Maximum encoded page bytes were 794,728; page-read elapsed time reached
235,442,397 ns; measured cold p99 was 238,803,273 ns; and measured process CPU
p99 was 51,523,314 ns. Peak process RSS was 28,139,520 bytes, memory PSI was
zero, and swap was zero. Thus resident routing memory and S3 byte volume pass,
but both page discrimination and the 16-request physical layout fail the
quality/latency objective.

The 16,286-byte canonical terminal is preserved at
`s3://borsuk-bench-453182569524-euc1/research/v30-hierarchical-pq/0b796e97224d89b2c47bb15b3495770d556d2658/attempts/v30-deep-100k-page-centroid-eval-20260904T110707Z-a0001/TERMINAL.json`.
The construction and evaluation Spot instances
`i-0cdaad818c8d04746` and `i-028a02199fe1eae4a` published terminal evidence
and terminated. The next bounded experiment must add a distribution-sensitive
page signal rather than merely increasing the page-centroid beam. D3, a
100-million-row build, and competitor claims remain fenced.

### The retained 16-page PQ route is quality-perfect; residual correction is stopped

Read-only recovery of the earlier bounded-candidate terminal at source
`2bce312c1bc7759efc1e540e2787750775ff85e8` establishes the shortest current
quality path. The original leaf/PQ route with 16 parallel page reads reached
320/320 hits, 1,000,000-ppm aggregate and minimum recall, 1,000,000-ppm floor
compliance, and 32/32 perfect queries on the frozen 100,000-row, 32-query Deep
Image development cell. It scanned at most 33,001 codes and fetched at most
2,928,808 encoded page bytes. Its 16,889-byte terminal has SHA-256
`f7ca28d37e1fe1d2cc08790d7155980bdeede8b6ce8fd78faf8635373ca2641f`
and is preserved at
`s3://borsuk-bench-453182569524-euc1/research/v30-hierarchical-pq/2bce312c1bc7759efc1e540e2787750775ff85e8/attempts/v30-deep-100k-pages16-bounded-candidates-20260904T084718Z-a0002/TERMINAL.json`.

That cell failed performance, not quality: process CPU p99 was 74,808,007 ns
and Standard S3 cold p99 was 144,065,141 ns. Maximum routing, page-read, and
exact-rerank elapsed times were respectively 8,185,812, 127,003,602, and
11,159,727 ns. Peak RSS was 28,602,368 bytes. The next production work must
therefore restore this quality-perfect candidate route and reduce object-store
and decode overhead; it must not replace the routing signal with the rejected
single-page-centroid route.

V31 source `a72ffc0089b09d5d6d2e2302d28493dfeb74603a` then tested residual
corrections over the primary-leaf-only frontier. The uncorrected and exact
cross-term controls both reached 319/320, while u8 error norm, sign8, sign16,
and exact error norm reached respectively 316/320, 267/320, 294/320, and
316/320. The 2,771-byte claim-ineligible result has SHA-256
`450a46dd274af89b2d5a17e04addbc8f3650083eb67accf49f9a62108b1d9b37`
and is preserved under
`s3://borsuk-bench-453182569524-euc1/research/v31-residual-correction/a72ffc0089b09d5d6d2e2302d28493dfeb74603a/attempts/v31-deep-100k-residual-20260904T114126Z-a0001/`.
Because the exact control did not reproduce the historical secondary-route
ceiling, this is negative evidence for that primary-only frontier rather than
a valid rejection of all residual correction.

A follow-up at source `9cf2dfbd472725b571d84dc8cc82cd5f77bab340`
added a reconstructed nearest-secondary membership, but the committed
scientific-control check rejected it before publishing arm evidence because
the old exact-control evaluator and membership authority were not preserved.
Its failed terminal is under
`s3://borsuk-bench-453182569524-euc1/research/v31-residual-correction/9cf2dfbd472725b571d84dc8cc82cd5f77bab340/attempts/v31-deep-100k-residual-20260904T114757Z-a0002/`.
Both Spot workers, `i-0248cd98b37fabd3e` and `i-05515c0c17f2d246c`, are
terminated. No further reconstruction or residual-correction run is warranted:
the directly preserved 16-page route already closes quality, and the release
path is now latency/throughput engineering. D3 and 100-million-row work remain
fenced until that path passes fast performance gates.

### Global V32 routing contains every one-million-row truth candidate; page reduction fails

Source `af05a46b75212c894fc5208aa768910552ed083d` ran the preregistered
root-independent diagnostic over the frozen one-million-row Deep Image index
and query ordinals 64 through 95. It ranked every routing microleaf, admitted at
most the first 768 subject to the unchanged 262,144-code ceiling, retained the
best 12,288 row-PQ candidates, and selected exactly 16 distinct pages without
reading any page body. The maximum observed truth-microleaf rank was 625 and
the maximum scanned population was 230,856 codes. Every missed truth row was
therefore inside both the admitted microleaf frontier and retained candidate
population.

The current first-distinct page reducer recovered 308/320 truth rows:
962,500-ppm aggregate containment, a 7/10 per-query minimum, and 23/32 perfect
queries. Reciprocal-rank page aggregation regressed to 298/320. All twelve
first-distinct misses were classified at the page-reducer boundary. The truth
sets occupied eight to ten current physical pages per query, so the current
layout is provably incapable of perfect containment with exactly eight page
reads for queries occupying nine or ten pages. Sixteen selected pages occupied
at most 3,117,216 authenticated encoded bytes. Page reads remained exactly zero.

The 262,537-byte canonical terminal has SHA-256
`88226dcc0bc3a6b7034349d95698c0946d500a40b7ba1133bdd418fc5eefb74e`
and is preserved at
`s3://borsuk-bench-453182569524-euc1/research/v32-quality-perfect-s3-serving/af05a46b75212c894fc5208aa768910552ed083d/attempts/v32-deep-1m-global-containment-l768-20260905T020228Z-a0001/TERMINAL.json`.
The disposable `causality` Spot worker `i-069be3f1306237791` published this
scientific-failure terminal and terminated. A preceding attempt failed before
science because a locally linked executable required a newer glibc than the
frozen Amazon Linux 2023 worker. A native build on terminated Spot worker
`i-066b0c09f5993287b` produced the 21,642,672-byte executable with SHA-256
`371d9a4184f9a3de7283b163e1890434de4ea0605410acabc02d7e93fe2fa624`;
the controller now probes executable startup before downloading scientific
inputs.

This result rejects further root-beam, leaf-beam, candidate-depth, and page-score
tuning on the burned cohort: global routing and candidate retention are not the
observed cause, and reciprocal-rank aggregation is worse. The next bounded
experiment is a page-free, query-independent geometric repacking replay that
keeps candidate order fixed and changes only page membership. It must first
reproduce this exact control and then reach 320/320 before any layout is
materialized. D3, a larger build, sealed-query evaluation, and competitor claims
remain fenced.

### V32 within-microleaf eight-page repacking fails the occupancy bound

Read-only recomputation from the authenticated 262,537-byte global V32 terminal
with SHA-256 `88226dcc0bc3a6b7034349d95698c0946d500a40b7ba1133bdd418fc5eefb74e`
counts distinct `leaf_ordinal` values among each query's ten truth targets.
For query ordinals 64 through 95, the counts are, in order:
`5,5,10,8,6,9,6,3,6,5,7,4,8,3,7,3,4,1,5,8,8,4,4,2,5,6,5,8,8,8,5,8`.
Query 66 requires ten separate microleaves and query 69 requires nine.

Because the proposed repacker forbids a page from crossing a microleaf boundary,
these two queries cannot recover all ten neighbors in eight page reads under
that design, regardless of its geometric splitter or page ranking. This closes
the within-microleaf experiment before reconstruction or replay. It does not
reject cross-microleaf packing, replication, or every unique-owner layout.
The cohort remains burned development evidence; no new recall or latency
measurement is claimed. No page or corpus object was fetched for this check.

The already-started native qualifier build at source
`a0e232c0d36d319962bd14d1d77941a12ab17db4` completed on Causality Spot instance
`i-01bc6dc8fd7f206af`, which is confirmed terminated. Its 21,834,408-byte binary
has SHA-256 `0cb59e347411ca151c7e46109ce49200ec0f96aab2c294f9a5a3acc4e32854bc`.
The 715-byte build receipt is at
`s3://borsuk-bench-453182569524-euc1/research/v32-quality-perfect-s3-serving/a0e232c0d36d319962bd14d1d77941a12ab17db4/authority/qualifier-build.json`.
This is build evidence only. The next design review must address pages spanning
microleaves and account for the strict serving-memory projection separately.

### V32 global balanced ownership: frozen eight-page cell rejected

Source `572b344a346adceb8d29b1d1d35c9f000eaa8370` produced one bounded
global480-row unique-ownership replay on1,000,000 Deep Image rows, burned
development queries64..95. Candidate replay was unchanged and complete control
bytes matched the governing terminal before treatment. The old16-page control
reproduced308/320 hits (962500ppm). The global8-page treatment obtained275/320
(859375ppm), minimum500000ppm,13/32 perfect queries. Failed gates were
`perfect-containment`, `minimum-containment`, and
`virtual-eight-page-obstruction`: six queries' truth spans more than eight new
pages, maximum ten. This rejects this frozen eight-page cell, not all global
layouts or a separately preregistered larger page budget. No tuning or100M
escalation follows this result. Astra failure analysis was requested.

Read-only explanatory aggregation of the stored16-page selections gives307/320,
minimum8/10,24/32 perfect, versus original308/320,minimum7/10,23/32 perfect:
neither dominates. A truth-only within-layout oracle summing the largest eight
truth-page occupancies gives312/320. Thus eight misses are structurally forced
at8 pages;37 additional misses separate the observed275 from that oracle.
This oracle is not a deployable selection rule and does not authorize tuning.

The317,286-byte canonical terminal is at
`s3://borsuk-bench-453182569524-euc1/research/v32-quality-perfect-s3-serving/572b344a346adceb8d29b1d1d35c9f000eaa8370/attempts/v32-deep-1m-global-ownership-572b344a-a0001/TERMINAL.json`,
SHA-256 `f621b215b4821d3bd8e957c2ffff358fea9d545ff6be9767af5ffcb4b89ccbaf`.
It preserves all input hashes, replay/map evidence and page counts.
`claim_eligible=false`, zero page-body reads: this is neither measured S3
latency nor throughput. Control phase wall/CPU were2,815,498,124/2,819,911,452ns;
treatment22,073,560,291/22,075,685,903ns, excluding resident loading.
Control VmHWM93,548,544B, treatment500,297,728B, controller228,950,016B;
the conservative sum729,247,744B passes the2GiB resource gate, not a simultaneous
peak measurement. Qualifier build finished in2m09s on Spot
`i-067eafe42e3b28b45`; science ran on Spot `i-0133330710f10393b`.
Both are confirmed terminated. Original controller exited1 on the scientific
failed terminal; it was not restarted.

Native binary21,857,168B SHA-256
`ce9fb4fd2922d4fc17733ae9f123b0c3b399ccc024f51ed7105605c32774b2e1`;
source archive8,198,015B SHA-256
`4604db60e06793b762f6d892850cfa30c961fa7fff5fa53bb13eec05652acf23`.
Both and the715-byte build receipt are under the same source's `authority/`
prefix (`v30_s3_qualify`, `source.tar.zst`, `qualifier-build.json`).

### Current latency objective clarification

On2026-09-05 the operator withdrew the hard15ms cold-S3 requirement in favor of
very low measured latency, high write/read throughput, high recall and scalable
S3-first storage. Historical failed thresholds and measurements remain unchanged
as historical evidence. Future qualification reports cold/warm distributions,
concurrency/QPS, write vectors/s and visibility latency, requests/bytes and write
amplification; no resident-only replay is relabeled an S3 measurement. The next
global-layout exact8 diagnostic retains its preregistered cell budget rather
than changing that budget after observing quality.

### Exact-truth arithmetic correction before physical global serving

On2026-09-05 Astra's review identified that prefix truth's NumPy axis sum did
not guarantee the sequential f64 dimension order used by Rust exact reranking.
A bounded synthetic reproduction confirmed the difference: query e0; source0
is e1 plus coordinates2..95 equal2^-27; source1 is e1. After the registered
normalizations, sequential distances are exactly2 for both, while the previous
axis sum returns `0x1.0000000000009p+1` for source0. With nine zero-distance rows,
this changes the tenth neighbor from source0 to source1. The regression now
requires exact distance bits and the source-ordinal tie-break.

The builder now uses separate f64 multiply/add in increasing dimension order;
new prefix truth receipts use `borsuk-v32-prefix-truth-v3`. Current containment
execution requires that version. No historical truth object, terminal or
reported recall is overwritten. This is proof of an arithmetic-contract bug,
not proof that any frozen Deep Image truth ID differs. Original replay/page
parity remains a valid historical regression; new quality qualification must
use newly authenticated v3 truth, with any v2/v3 differences explicitly reported.
No additional corpus stream or paid run was performed for this diagnosis.

### Global physical S3 regression registration (941f770f)

Source `941f770f1eaba37103086051c70ca752259f9128` contains the reviewed global
serving path and strict replay/receipt reductions. Native ARM binary21,941,968B
SHA256 `8e3f5851c5936241a7650ec1f05355fa5430f630fd2ec8307bc2bafab9af170f`
was built on Causality Spot `i-0f4c051fa1241b9df`, now confirmed terminated.
The8,211,075B source archive SHA256 is
`0016b89acb83b59180ae30e9ab6d07a6bc0d09a4f98f920e2b4666d599ff23f7`.
Build receipt715B SHA256
`e98c0f9067d28799766c0ba43479bba4ef8d0e37d77fa972327914e8e69de90e`.
These are under this source's `authority/` prefix in
`s3://borsuk-bench-453182569524-euc1/research/v32-quality-perfect-s3-serving/`.
An earlier controller failed SDK parameter validation before upload/launch;
there was no instance and no scientific execution in that failure.

The single registered attempt is
`v32-deep-1m-global-serving-standard-q64-941f770f-a0001`, Spot
`i-026812520cbd42922`, r7g.2xlarge, eu-central-1. The immutable REGISTRATION.json
under this source's `attempts/` path binds worker SHA256
`f153aa254febd56457efb319f7039d5b92098d5b9ef5fea68397976ff3f33285` (5615B)
and userdata SHA256
`7f94ca8e1684f306061ce71eecf8fdd464b009d8c521ebea46b943d736e8e4fa`.
Original query64..95, global768, scan262144, candidates12288, sixteen original
pages, k10. This is historical-v2-reference regression, **not new quality
qualification**. Preserve the arithmetic caveat above, exact replay/page parity,
raw batch and terminal. No corpus download or index rebuild; compact resident
PQ/routing artifacts are explicitly part of1M setup, not a1B memory proof.

Maximum512 logical page reads and100,663,296 page payload bytes. One sequential
batch, shared client/connection reuse, no application page cache; S3 internal
cache uncontrolled. Report n32 empirical timing, no stable p99/QPS claim,
transport attempts unmeasured. Worker process-group limit3GiB, PSI full avg10
stop above0.5, swap-growth cap256MiB, scientific wall1200s, whole wall1800s.
One original only, no automatic restart; retain terminal/stop evidence and
terminate compute on every outcome. No result was inspected at registration.

Terminal execution completed; exact quality target did not pass. Terminal
136,166B SHA256
`8b4b81ef4a9ed48ac47df08f2d29965704aa1463243553dafb60f69dfafe732f` and raw
`BATCH.json`127,474B SHA256
`f09024c379fd8be7e9e670d9b1307fe8e147fe4148ab478739b43abf47c96b43` are preserved
under the registered attempt prefix. Independently downloaded/authenticated
raw batch, page-location Parquet and frozen truth (208,787B total); all32 replay
hashes/ordered physical page selections validate, and independent intersections
reproduce308/320=962500ppm, minimum700000ppm,23 perfect queries. These exactly
match the prior resident control, not a new quality improvement. The reciprocal
rank historical alternative was worse at298/320 and is not a new candidate.

Real S3 Standard end-to-end elapsed ns: empirical p50=152,514,062,
p95=187,821,992, maximum225,491,858; total5,040,539,415 across32 sequential
queries. Routing p50=84,530,137ns (total2,706,343,174); page reads
p50=65,013,999ns (total2,230,584,420); exact rerank p50=3,214,833ns
(total103,535,075). Process CPU total3,518,316,346ns. Whole qualifier
5,313,156,706ns includes resident setup; bootstrap/input downloads are outside
that interval. These are n32 empirical quantiles, not a stable tail estimate,
sustainable concurrency/QPS result, or controlled cold-server-cache result.

512 logical page reads transferred99,750,912 encoded bytes,3,117,216/query.
Setup fetched15 authenticated reference/resident objects totaling32,213,566B,
excluding binary/source/bootstrap dependencies. No vector corpus was downloaded
for serving. Peak reported **qualifier** RSS87,220,224B is not a complete cgroup
or controller-plus-child peak. The30s sampled watchdog reported startup
RSS9,445,376B, PSI0 and swap growth0; the short measured phase does not establish
an exact whole-group high-water mark or prove the3GiB release memory bound.
Controller observed terminal at76s and confirmed instance termination. It exited0;
no restart occurred. At observation, eu-central-1a Spot rate was$0.2922/hour
for r7g.2xlarge (not a final billed-cost claim).

Disposition: physical global serving parity is established for this frozen
cohort. Routing CPU and S3 page reads are both material latency contributors;
remaining recall loss is still page selection, not missing physical execution.
Ask Astra for the smallest bounded routing-hot-path improvement, retain v3
truth qualification requirement, and do not launch100M or claim release readiness.

### V32 lazy-PQ CPU comparison: build isolation failure and separate attempt

Source `407a8f74bde00b9cd71a39e0aeb5170110b0f1e4` implements on-demand PQ
table entries and independently validated work counters. Baseline is
`941f770f1eaba37103086051c70ca752259f9128`. Both preserve the frozen candidate
replay and routing semantics; CPU benefit is not implied by correctness tests.

Under `s3://borsuk-bench-453182569524-euc1/research/v32-quality-perfect-s3-serving/407a8f74bde00b9cd71a39e0aeb5170110b0f1e4/attempts/`,
`v32-deep-1m-pq-abba-407a8f74-a0001/` failed before measured ABBA. The second
Cargo build reused the shared target's first executable: both binaries were
21,944,048B with SHA256
`7e3a3ee6f41511afb62bf26d8e0c9cadf96572af0a6227329f5525a74c0b42de`.
Warm-B emitted baseline schema8 rather than current schema10. This is a build
provenance failure, **not a speed or recall result**. Original `FAILED.json`
57B SHA256 `30c3ac39b0663134f45c8de79846eaaa84de928c129eece758d4137bbbad88d2`
and `BUILD.json`823B SHA256
`ff3a47d0cedc42df3c927d3ec25076b2af7fc8d03b8b41a3908aede102f3464e`
remain immutable. Original instance `i-06d7e81d039c61b23` is terminated;
original controller exited1. Neither warm-up is admissible measurement evidence.

Separately registered `v32-deep-1m-pq-abba-407a8f74-a0002/` isolates targets
`/opt/pq-build-eager` and `/opt/pq-build-lazy`, checks distinct binary hashes,
and retains schema8/schema10, all32 replay hashes, semantic-current equality
and PQ-counter validation during the two prescribed warm-ups. A local shell
generation regression failed on shared target paths and passed with isolated
paths; it does not substitute for the real remote build. Astra reviewed the
repair. Source archive remains 8,220,109B SHA256
`490961d017f6d21f29c1c34b442180aa982825f8ba6e08e1201531a720519d68`.

The new original is Spot c7g.4xlarge in eu-central-1b, instance
`i-047d35462c0419e30`. Rust1.98.0, identical Cargo.lock and release flags;
CPU0 affinity; warm-A/warm-B then measured A1/B1/B2/A2. Preregistered promotion
requires B1<A1, B2<A2 and at least5% aggregate CPU reduction. Timing scope is
the whole control-diagnostic phase, not routing-only or physical S3 serving.
Inputs are the same historical-v2-reference 1M cohort64..95; no new quality
qualification and no page-body GET. Scientific cap900s, whole cap1800s,
process-group RSS3GiB, full PSI avg10 above0.5 or swap growth256MiB stops the
original. Preserve terminal/stop evidence and terminate owned compute; do not
restart in this attempt. No result was inspected at registration.

Attempt a0002 also terminated before measurement, at warm-B. Isolated builds
produced distinct binaries: eager21,944,048B SHA256
`7e3a3ee6f41511afb62bf26d8e0c9cadf96572af0a6227329f5525a74c0b42de`,
lazy21,946,424B SHA256
`0d92518bd8c0866586967f93209169761ddcf777ace80fb208dac02ea1c2fff7`.
`BUILD.json`1,204B SHA256
`0436f9a01e13c4db6f140f3e8f7c4e1e5a7535224d985aabeba709cbf56a6900`
proves the build-isolation repair. The lazy qualifier exited1 with
`V32 global resource schema differs`: its producer emitted schema10, but the
outer resource serializer still allowed only8/9. `FAILED.json` is57B with
SHA256 `30c3ac39b0663134f45c8de79846eaaa84de928c129eece758d4137bbbad88d2`;
the original controller exited1 and confirmed `i-047d35462c0419e30` terminated.
No measured ABBA or speedup claim exists for this attempt.

The focused producer-to-resource-wrapper regression reproduced this exact
failure locally (one test,0.08s execution). The minimal repair accepts current
schemas9/10 and rejects retired8; no compatibility reader was added. The test
checks preservation of the complete query/replay/counter payload, addition of
the resource object, and rejection of duplicate resources. Five focused tests
passed in0.22s; the complete23-test example passed in0.23s. Astra independently
confirmed the missed composition boundary. This is a diagnostic-output repair,
not a change to PQ scoring, index layout, recall or serving defaults.

### V32 lazy-PQ paired CPU result: 49.5573% reduction with exact replay parity

Frozen measured source `76cb7382a28fce8838b3969b39c2cf27bda3b4ab`, archive
8,223,206B SHA256
`266292a5c096a6680d0740900b8874c59136d33b0e2c726aad0d92859f349df0`,
ran one separately registered attempt at
`s3://borsuk-bench-453182569524-euc1/research/v32-quality-perfect-s3-serving/76cb7382a28fce8838b3969b39c2cf27bda3b4ab/attempts/v32-deep-1m-pq-abba-76cb7382-a0001/`.
Source, worker and userdata identities are registered before launch. Both
sources built in isolated targets on the same c7g.4xlarge Spot instance
`i-0dad04de919f94b4e`, eu-central-1b, with Rust1.98.0 and identical locked
dependencies/release flags. Eager binary remains21,944,048B SHA256
`7e3a3ee6f41511afb62bf26d8e0c9cadf96572af0a6227329f5525a74c0b42de`;
lazy21,946,368B SHA256
`5c4319e1fb68811ad4a8cd9364cae388a9fbe7f671fad7d8cfecd98d4c8190b9`.

The original controller exited0, preserved `TERMINAL.json`9,322B SHA256
`c1f3937d602100f65f312a7ba5ead3639e789e855ab2099de5415e80959d221e`, and
confirmed instance termination. All six raw warm/measured outputs were separately
downloaded after terminal, authenticated against the terminal's hashes/lengths,
and checked against all32 frozen replay hashes. Semantic controls are identical
across both implementations. Current Python validation independently accepts
the lazy work counters. No restart occurred.

Measured whole control-diagnostic CPU ns,32 queries per repetition:
A1=2,819,973,317; B1=1,419,942,402; B2=1,419,945,848; A2=2,809,961,762.
Both adjacent comparisons improve. Independently recomputed aggregate reduction
is495,573ppm (49.5573%), exceeding the preregistered50,000ppm minimum. Mean
CPU/query is87,967,735.609375ns eager versus44,373,253.90625ns lazy.
Corresponding phase wall ns: A1=2,815,023,366; B1=1,427,692,119;
B2=1,427,802,218; A2=2,817,025,769. Warm-ups are excluded from these calculations.

Each lazy repetition reports base65,645,049 computed entries/81,078,375 cache
hits; high16,165,098 computed entries/3,596,358 cache hits; zero eager fallbacks.
Across32 queries,24,196 parent-table pairs imply445,980,672 entries for the
full eager table geometry, versus81,810,147 actually evaluated. This geometry
comparison is derived, whereas lazy work counters and CPU are measured.
The optimization preserves arithmetic, selected candidates/pages and exact
replay identity; it does not improve recall by itself.

Inputs are13 named authenticated metadata/reference objects,32,209,070B total;
no vector corpus or page bodies were downloaded for this CPU diagnostic.
Peak qualifier RSS across the six invocations93,814,784B. Nine2s watchdog
samples peaked at232,017,920B for the process group, full PSI avg10=0 and swap
growth0. `monitor.jsonl`793B SHA256
`b5c9b17e0d64fbd72a34d1ccfc086db52da68e76642d923a7497a2a95af317eb`.
Sampled group RSS is not an exact high-water mark. Bootstrap and compilation
are outside scientific phase timing. This is whole control-diagnostic CPU
(query reading, capture, hashing, diagnosis and serialization), **not a measured
49.5573% improvement in end-to-end S3 latency**, routing-only timing, sustainable
QPS, a cold-S3 result, new quality qualification or100M/1B scalability proof.

Disposition: retain the lazy scorer. Next, evaluate the preregistered16/32/64
first-distinct-page ladder on a new cohort with corrected-v3 truth, keeping
candidate generation fixed and reporting exact selected bytes before physical
serving. Do not tune intermediate budgets on that result or promote64pages as
a default without measuring its S3 cost/latency/throughput tradeoff.

Separately, commit `c9695615aa6935383970d19623bec796caef498f` validates immutable
base/high PQ codebooks once per serial preparation pass rather than per row.
This was **not** part of the measured CPU binary. It adds two borrowed handles,
no corpus cache or parallel buffers, and retains residual/error validation and
record order. TDD missing-interface RED preceded implementation; three focused
tests passed, including complete prepared-record byte equality with the frozen
encoder. All21 PQ and22 layout tests, targeted strict library/test Clippy, fmt
and diff-check passed. Astra reviewed it READY. Write throughput benefit is
unmeasured and requires a separate encoding/preparation benchmark.

### V32 preregistration: fixed page-budget replication cohort

Registered before obtaining new-cohort truth or diagnostic output. Diagnostic
source is `b21169715f36315395622352d0a6cd7b6f5f7f08`. Use the unchanged Deep
Image 1M index built by `1dd90354c3eeb0c24c8839284b9b95dd07a32546`, manifest
3,642B SHA256 `17c5429da4887b4e2c266326861a6645d53463fe3face91deadcf5cf5dbecb29`.
Use the same 10,000-row query Parquet, 3,843,448B SHA256
`296d45828020c1c0b88c6a1d5c822f6283280513b8c58d01cfa961f3a139a5d4`, but select
exactly ordinals 1024–1055. This is a preregistered evaluation cohort outside
the documented recent tuning cohort (0–991), **not a globally untouched
holdout**: earlier whole-dataset evaluations may include these queries.

Freeze root beam8, global leaf limit768, scan budget262,144, candidate depth
12,288, and k10. Capture each query's candidates once. Evaluate exactly the
nested first-distinct physical-page prefixes16/32/64 from that same capture;
record replay SHA, requested and actual counts, complete registered identities,
exact encoded bytes, per-query truth-page hits, aggregate and minimum
containment. No adaptive intermediate budgets, altered routing, layout, or
truth-dependent selection. Exhausted prefixes must be consistent with all
known target ranks. This is **page containment**, not exact-rerank recall,
S3 latency, throughput, or a publication-quality sample.

Generate corrected-v3 exact truth for these32 queries by the existing bounded
query-independent corpus-shard stream and fixed-dimension-order float64
distance contract. Bind the query/corpus/source-row identities in its receipt;
translate source IDs through the manifest-authenticated logical-source Arrow.
The diagnostic reads only authenticated resident metadata/codes, query and
truth artifacts; it has no page-body capability. Streaming corpus vectors to
construct reference truth is benchmark preparation, not the serving design.
Do not persist a local corpus or expand serving downloads to the full dataset.

Decision rule: report all three cells, including failures. If any budget has
320/320 containment, take the smallest such budget into a separately registered
real-S3 comparison against16 pages. Do not promote it as a production default
on containment alone. If64 misses, stop page-budget widening and classify the
remaining candidate-retention/routing/reducer evidence before changing the
architecture. No causal guarantee extends beyond this cohort and tested
candidate population. Release qualification still needs broader independent
quality, sustainable read/write throughput, memory and scaling evidence.

Execute on causality EC2 Spot with one original attempt, monitored under3GiB
process-group RSS, full memory PSI avg10<=0.5, swap growth<=256MiB, and a30-minute
whole-worker cap. Preserve terminal/stop artifacts and exact source/binary/input
identities; terminate owned compute immediately at terminal. Inspect incomplete
work only through phase/liveness/health markers, not partial quality output.
No automatic restart after scientific failure. The former15ms cold-S3 target
is not a release gate; measured end-to-end latency, tail latency, throughput,
recall and request/byte cost must be disclosed together.

### V32 fixed page-budget replication: terminal containment result

The original `v32-deep-1m-page-ladder-b2116971-a0001` completed on Spot
`i-0d2f8fa53df3419f7`, c7g.4xlarge, eu-central-1b. The controller exited0 and
confirmed termination; no restart. Source and preregistration remain those
above. Source archive8,243,286B SHA256
`4b89decce6f0c9c813ae8d81ddc5775d90149f165853ca975d5bfd1a88d57538`;
Rust1.98.0 arm64 release qualifier21,947,896B SHA256
`0178830f70d6bda69372508581eac1b7c4bd725143d36e85ca8aaab6c91e3367`.
The attempt prefix is
`s3://borsuk-bench-453182569524-euc1/research/v32-quality-perfect-s3-serving/b21169715f36315395622352d0a6cd7b6f5f7f08/attempts/v32-deep-1m-page-ladder-b2116971-a0001/`.
`TERMINAL.json`815,871B SHA256
`b5f5434073b46d40693929621579ae5034a1f89aa6c76298f1d5f1b757ac52c0`
preserves the32-query raw diagnostic, input identities, corrected-v3 truth
bindings, binary provenance and independently validated summary.

| Requested pages | Contained truth /320 | Aggregate containment | Minimum query | Perfect queries /32 | Logical GETs if served | Mean encoded bytes/query |
| --- | --- | --- | --- | --- | --- | --- |
| 16 | 302 | 94.375% | 6/10 | 22 | 512 | 3,117,216 |
| 32 | 319 | 99.6875% | 9/10 | 31 | 1,024 | 6,234,432 |
| 64 | 320 | 100% | 10/10 | 32 | 2,048 | 12,468,864 |

All actual prefix counts equal their requested caps. Exact aggregate bytes are
99,750,912 /199,501,824 /399,003,648. After terminal, the controller-side check
recomputed every sample's hits from target-page membership, each byte sum from
physical identities, all totals/minima/perfect-query counts, and unique counts.
No vector-page GET was executed; the table's GET counts describe the physical
prefixes **if served**, not measured S3 operations. This is not reranked recall.

Corrected reference truth required six registered corpus-shard reads totaling
402,965,152B, streamed without saving corpus shards. All20 input reads totaled
442,984,572B including metadata, query and logical-source/reference authority.
Reference truth wall time21,159,661,468ns; whole worker25,603,885,975ns. Bootstrap
and compilation are outside that worker time. One32-query diagnostic phase
reports CPU1,459,893,883ns and wall1,461,320,381ns, including input/capture/hash/
diagnosis/serialization work: neither routing-only nor end-to-end S3 latency.
Qualifier peak RSS100,827,136B. Fourteen2s process-group samples peak at
583,516,160B, full PSI avg10=0, swap growth0. `monitor.jsonl`1,240B SHA256
`44ef3e14d37513b754de5e0da8335db41a1a9a8c9cd3a70e86fe7f080dda0444`;
sampled RSS is not an exact group high-water mark.

Decision under the preregistered rule:64 is the smallest tested budget with
perfect containment on this cohort. Implement an explicit bounded64-page
serving arm and separately register its real-S3 comparison against16 pages.
Do not retune candidate generation or test intermediate budgets on this cohort,
and do not declare64 a default or claim100M/1B scale, sustainable throughput,
perfect general recall, or release qualification. The fourfold page-byte cost
must be measured against the recovered truth before promotion. The cohort's
prior-exposure caveat and claim-ineligible status remain unchanged.

### V32 selective S3 page-budget comparison: preregistered protocol

Serving implementation source is
`a15ba5d772a8420df062d8530b28ede2ad47d736`. The explicit global serving arm
accepts16 or64 pages without changing routing, scan budget or candidate depth;
schema4 receipts distinguish the16-page reference capture from actual reads.
The affected gates passed:40 library search tests,27 qualifier tests,14 Python
reader tests, targeted strict Clippy, Ruff, formatting and diff checks. Astra's
read-only delta review is READY after candidate-count and per-page-byte bounds
were mutation-tested and repaired. These are implementation checks, not serving
quality or throughput qualification.

Freeze Deep Image1M queries1024..1055, corrected-v3 truth and candidate replay
from the preceding ladder terminal, exactly815,871B SHA256
`b5f5434073b46d40693929621579ae5034a1f89aa6c76298f1d5f1b757ac52c0`.
Authenticate its manifest, query, truth, truth receipt and physical page
identities before execution. Reuse truth output; no reference corpus GETs or
truth regeneration in this experiment. The authenticated ladder owns expected
page order and capture hashes; serving output must match them exactly. Do not
use current result output to construct its own expected routing authority.

One causality c7g.4xlarge Spot instance, same-region S3 Standard, one release
binary, CPU0 affinity, fixed single-thread query execution and sequential32-query batches.
Execute four separate original qualifier processes in fixed order
A1(16), B1(64), B2(64), A2(16), with no unreported warmup or retry after a failed
cell. Each process starts a fresh SDK client, reuses connections within its
batch, has no application page cache and reads only its selected pages.
S3 service cache state is uncontrolled: do not label this strict cold latency.
No inspection of partial quality or latency samples while the attempt runs.

Expected logical page GETs:1,024 across A1/A2 and4,096 across B1/B2, total5,120.
Expected encoded page bytes:997,509,120 across all four batches, excluding
resident setup inputs and SDK transport/retry overhead. Record those overheads
separately where observable; logical GET counts are not a claim of wire-level
request counts. Preserve each raw batch, command, source/archive/binary/input
identities, per-query phase timings, memory and terminal classification.

Report exact-reranked hits, minimum recall and perfect-query counts for every
cell, plus empirical median/p95/maximum end-to-end and phase latency, CPU,
bytes and GETs. No32-sample p99 or sustainable-QPS claim. Confirm exact returned
identities/distances are stable across same-arm repetitions. B's quality target
is320/320 with minimum10/10 in both repetitions; any loss relative to frozen
containment is a reranking/serving correctness investigation, not permission to
retune on this cohort. Report A unchanged even if it fails. A quality pass for
B permits broader independent query and concurrency evaluation, not default
promotion or a100M/1B claim. The extra fourfold page cost remains explicit.

One original controller/instance only, no automatic scientific restart. Stop
at process-group RSS>3GiB, full memory PSI avg10>0.5, swap growth>256MiB,
900s scientific wall or1800s whole-attempt wall. Preserve a stop/failure receipt
and completed cell artifacts; terminate the owned instance at terminal. Setup
and build time are separate from measured query latency. No15ms cold-S3 gate;
recall, low practical latency, sustained throughput and bounded scaling all
remain required for eventual release.

### V32 paired selective S3 serving: terminal exact-recall result

The original `v32-deep-1m-serving-pair-28e60e45-a0001` completed on causality
Spot `i-0e0c12c894daa5d8a`, c7g.4xlarge, eu-central-1b. Controller exited0 and
confirmed instance termination, without restart. Exact execution source
`28e60e45d61f1e4e1953366f5581820df8a5954e` adds only the Python historical
ladder projection/tests after preregistration; its Rust tree is byte-identical
to registered serving source `a15ba5d772a8420df062d8530b28ede2ad47d736`.
The projection passed15 Python tests, actual frozen-terminal preflight, Ruff,
pycompile/diff-check and Astra read-only review before launch. The exact
corrected-v3 truth files also passed a bounded preflight before compute launch.

Attempt prefix:
`s3://borsuk-bench-453182569524-euc1/research/v32-quality-perfect-s3-serving/28e60e45d61f1e4e1953366f5581820df8a5954e/attempts/v32-deep-1m-serving-pair-28e60e45-a0001/`.
`TERMINAL.json`996,135B SHA256
`10534cdb6faeaa8763ae48118574189d1be767d9f6e84953044aa83dd8cbed76`.
Source archive8,253,283B SHA256
`183f168e9896ccb8b377b9ea9ca725e777a9a3d7b0e872a1686b122550e87f2d`;
Rust1.98.0 aarch64 release qualifier21,947,992B SHA256
`5c32d89ea0bbf3d5e99c8fc8c4275918a979b50b5c05fe87fabfd4bb38dd15ef`.
Exact source, worker, bootstrap and protocol identities are in REGISTRATION;
BUILD preserves compiler identity. No source/binary changed between cells.

| Cell | Pages | Reranked hits /320 | Minimum hits /10 | Perfect queries /32 | E2E median ms | Empirical p95 ms | Maximum ms |
| --- | --- | --- | --- | --- | --- | --- | --- |
| A1 | 16 | 302 | 6 | 22 | 131.851682 | 176.167393 | 183.247700 |
| B1 | 64 | 320 | 10 | 32 | 179.975310 | 224.100390 | 331.154534 |
| B2 | 64 | 320 | 10 | 32 | 139.570339 | 173.875713 | 276.309966 |
| A2 | 16 | 302 | 6 | 22 | 84.715535 | 110.782293 | 134.023896 |

These are actual S3 Standard page fetches and exact reranking, not simulated
latency or page-containment recall. A recall94.375% and B recall100% exactly
match their frozen containment on this cohort. All same-arm returned source
IDs and squared distances match exactly across repetitions. Post-terminal
controller-side validation fetched all four exact batch payloads, checked
their registered lengths/hashes, reran the independent serving validator
against the pinned ladder projection and exact v3 truth, and recomputed every
sample, aggregate and timing statistic. All fields matched the terminal.

| Artifact | Bytes | SHA256 |
| --- | --- | --- |
| A1-BATCH.json | 128,214 | `9ad407c9e6c4f631a28812a9ee8496aef8a08637049b85b6b098be0cb005be3f` |
| B1-BATCH.json | 360,958 | `c123b4925a59323c25dacc6c6e81c6e6869e0a0403aec6740437598f943916f5` |
| B2-BATCH.json | 360,953 | `3ac1bc3562a3d685f83fbbc13ac3af8599595da11bd07fe66edf960f08be03b2` |
| A2-BATCH.json | 128,179 | `da45783e8b56daae17443619efcff4e7532994c1bcc84aaeec87d296e1dd9cfb` |

No application page cache; each batch has a fresh client with connections
reused within the batch. S3 internal state is uncontrolled. The large A1/A2
and B1/B2 read-latency differences prohibit claiming independent strict-cold
latency, a stable tail distribution, or attributing all wall-time differences
to page budget. Both repeats are disclosed, not cherry-picked. CPU0 affinity
and sequential queries also mean these are not sustainable-QPS measurements.

Median routing wall times A1/B1/B2/A2 are41.359088 /41.786363 /42.842519
/41.475046ms; page-read medians77.999791 /89.239960 /48.421537 /31.694004ms;
exact-rerank medians11.890316 /47.035633 /47.253932 /11.919908ms. Phase medians
do not sum to end-to-end medians. The wider arm visibly quadruples reranking
work and bytes; this remains an optimization target, not a free recall gain.

Each A batch reads512 logical pages /99,750,912 encoded bytes; each B reads
2,048 /399,003,648. Per-query bytes are3,117,216 versus12,468,864. Total5,120
logical page GETs /997,509,120B; wire attempts/retry overhead are not measured.
Resident setup inputs total32,712,150B. No reference-corpus GETs occurred and
no full vector corpus was downloaded for serving. Whole worker21,076,439,430ns
excludes bootstrap/build; qualifier wrapper walls A1/B1/B2/A2 are4,631,048,592
/6,184,814,382 /5,016,854,025 /3,089,491,744ns and include process setup.
Maximum reported qualifier RSS98,115,584B. Twelve2s group-monitor samples peak
at216,580,096B, full PSI avg10=0, swap growth0. `monitor.jsonl`1,060B SHA256
`94ec4c5901ff7a9f6605d85cf85d5a6b40c7f099bb4c823d478104c7c6181304`;
sampled group RSS is not an exact high-water mark.

Decision: the explicitly tested64-page arm passes this small-cohort physical
serving quality check. Preserve claim_eligible=false and no default promotion.
Next qualify broader independently selected queries and bounded concurrency;
investigate exact-rerank CPU without changing results; independently measure
the existing write-encoder optimization. Do not extrapolate perfect general
recall, sustained throughput or100M/1B memory/scale from this result. The64-page
arm's fourfold request/byte cost and the cohort's previous ladder exposure
remain explicit limitations. No additional experiment was started in this
attempt, and owned compute is terminated.

### V32 broader fixed64 quality falsifier: preregistered stopping rule

Before additional physical serving or throughput harness work, challenge the
assumption that64 pages suffice beyond the32 examined queries. Freeze source
`28e60e45d61f1e4e1953366f5581820df8a5954e`, the same1M index/query artifact,
root beam8/global768/scan262144/candidate12288, and64 as the decision budget.
Use existing corrected-v3 truth and no-page ladder tooling, not a new router.

Choose four literal nonoverlapping windows in fixed order:4096..4127,
5120..5151,6144..6175,7168..7199. These starts are fixed before collecting
their outcomes, not selected from recall results. They do not overlap the
recent0..1055 cohorts, but historical whole-dataset exposure still prevents
calling them globally untouched holdouts. No candidate/page-budget tuning,
window replacement or output-dependent extension is allowed.

On one causality Spot instance, execute the windows serially, at most128
queries. Authenticate corpus/query/manifest inputs and generate v3 truth for
each window with the existing bounded streaming builder. At most24 reference
shard reads /1,611,860,608 corpus bytes across four windows; do not persist
corpus shards and do not call this serving traffic. Reuse resident setup once.
No vector-page body GETs. Existing tooling emits16/32/64 diagnostic cells;
preserve all of them, but only the fixed64 result controls this falsifier.

Stop after the first completed32-query window with any64-page miss. Preserve
all completed windows, denominator, exact sample evidence and stop reason;
do not run the remaining windows or quietly restart. Distinguish a target not
retained as a candidate from one whose true page ranks beyond64, using the
recorded per-target evidence; disclose any other loss boundary separately.
All128 passing permits broader physical/concurrency qualification, not a
universal-recall claim. Any miss is evidence against perfect recall for this
fixed configuration and requires causal diagnosis before tuning anything.

Same one-original/no-restart lifecycle:3GiB process-group RSS, full PSI
avg10<=0.5, swap growth<=256MiB,900s scientific wall,1800s whole-attempt cap;
record source/archive/binary/inputs, terminal or stop receipt and monitor;
terminate compute on terminal. Inspect only liveness/phase/health while active.
No fourth-core latency or write benchmark is bundled into this quality attempt.
Separately, a later multi-core configuration test must explicitly set
BORSUK_CPU_THREADS and affinity: RAYON_NUM_THREADS does not control BORSUK's
private pool. Existing parallel reranking must not be reimplemented merely
because the preceding single-core comparison reported47ms rerank time.

### V32 broader fixed64 falsifier: terminal frontier miss

The original `v32-deep-1m-broader-fixed64-28e60e45-a0001` completed its stopping
rule on Spot `i-065b696ac07fd16b0`, c7g.4xlarge eu-central-1b. Controller exited0
and confirmed termination; scientific quality is **failed**, not an execution
error. The exact preregistered source/binary were reused, with no compilation
or restart. Windows4096 and5120 passed64; window6144 failed, so7168 was not run.

| Window start | Completed queries |16-page hits /320 |32-page hits /320 |64-page hits /320 |64-page minimum |
| --- | --- | --- | --- | --- | --- |
|4096|32|308|318|320|10/10|
|5120|32|318|320|320|10/10|
|6144|32|311|319|319|9/10|

Total64-page containment959/960 across96 completed queries, or99.895833...%;
95/96 queries are perfect. The denominator stops at96 as registered; neither
128 nor the earlier examined cohort is pooled into it. This is containment,
not new measured serving recall. Failed-window64-page bytes398,224,344 differ
from399,003,648 in each earlier window because physical page sizes differ.

The sole missed target belongs to query6160: logical411202, page856, routing
leaf1711. Its one-based global leaf rank is1500, owner root52 ranks60, and it
has no retained-candidate or first-distinct-page rank. Its page is absent from
both scanned and retained populations. The query stopped at `leaf-limit`:
768/4141 leaves,186,745/262,144 codes,12,288 retained candidates. Thus more
physical pages alone cannot recover this miss. Rank1500 requires at least1500
leaves; the cumulative code cost to reach it is not present in this terminal.
Do not infer that raising the leaf limit alone suffices under the same budget.

Attempt prefix:
`s3://borsuk-bench-453182569524-euc1/research/v32-quality-perfect-s3-serving/28e60e45d61f1e4e1953366f5581820df8a5954e/attempts/v32-deep-1m-broader-fixed64-28e60e45-a0001/`.
`TERMINAL.json`2,438,226B SHA256
`5313f81f18aaf08fae1dadb80a25207fa497f05435b2e99d7b03a2be0ebc6daa`
preserves all three complete diagnostic/truth bindings and summaries. Source
archive SHA256 `183f168e9896ccb8b377b9ea9ca725e777a9a3d7b0e872a1686b122550e87f2d`;
reused21,947,992B qualifier SHA256
`5c32d89ea0bbf3d5e99c8fc8c4275918a979b50b5c05fe87fabfd4bb38dd15ef`.
No source or binary changed between windows. Each `q<start>-RESULT.json`,
DIAGNOSTIC.json, diagnostic.arrow, truth.parquet and truth-receipt.json is
preserved under that prefix; terminal fields bind the scientific input/output
digests. No partial measurements were examined while active.

After terminal, independent local validation authenticated the raw diagnostics,
query, manifest, logical-source mapping, page registry and v3 truth/receipts,
reran the existing reader for each window without executing the qualifier,
and matched every terminal field. The bounded temporary directory was removed
and absence confirmed. No vector-page GETs were used for this check.

Reference truth read18 frozen shards /1,208,895,456B, streamed without corpus
persistence; all setup/reference reads totaled1,248,914,876B. No serving page
reads occurred. Whole worker74,859,647,399ns; individual truth walls21,162,490,765
/21,162,390,092 /21,060,926,169ns. Diagnostic CPU1,439,939,467 /1,419,913,011
/1,439,930,301ns for the respective32-query windows. Maximum qualifier RSS
100,876,288B. Thirty-eight2s monitor samples peak at712,478,720B group RSS,
full PSI avg10=0, swap growth0; sampled memory is not an exact high-water mark.
`monitor.jsonl`3,375B SHA256
`368374b22d5d13527572b0b4e853088d06d521fa598119954677b7b6ee4f8453`.

Decision after Astra failure review: first measure metadata-only cumulative
ranked-leaf row costs for the completed96 queries. A larger bounded frontier
may justify an explanatory no-page replay, but not a default change. Do not
alter page budget, claim perfect general recall, invent a new score without
evidence, or launch scale/throughput qualification on the failed configuration.
Recovery on these burned queries would explain the failure, not independently
qualify it; the unexecuted7168 window remains a prospective check after a new
configuration is frozen. The separate resident-code memory limitation at1B
also remains unresolved. Claim eligibility remains false.

### V32 metadata-only frontier-cost explanation

A bounded local cross-language diagnostic reused exact query Parquet and
manifest-owned routing-ranges Arrow metadata only:4,826,010B, no corpus,
PQ-code scoring, page bodies or EC2 compute. It reproduced the production
reader-plus-router normalization (two ordered-f64 passes with f32 casts),
f16-centroid conversion through f32, ordered96-dimension f64 distance and
leaf-ordinal tie-breaking. Before deriving new costs it exactly matched all
96 original scanned-leaf/code counts and all960 recorded target leaf ranks.

Two preflight failures were preserved: an incorrect scratch-script field name
(`leaf_ordinal` instead of authenticated `routing_leaf_ordinal`) and the rank
base assumption. Source inspection confirms diagnostic root/leaf ranks are
one-based; candidate and first-unique-page ranks are zero-based. The preceding
ledger wording was corrected accordingly. No cost result was accepted before
the full reproduction gate passed; no production or index data changed.

For query6160 the missed leaf is index1499 / reported rank1500 and needs
353,435 cumulative codes. A1536-leaf/262144-code intersection stops after1083
whole leaves /262,080 codes and still excludes it. A1536/524288 intersection
visits1536 leaves /360,482 codes and reaches it. Across the96 queries, the
262144-budget cell reaches959/960 target leaves and scans261,213..262,141 codes;
the524288 cell reaches960/960 and scans315,293..421,521. This is **frontier
reachability only**, not PQ candidate retention, page containment or recall.
Whole-leaf stopping neither skips an over-budget leaf nor partially scans it.

Result134,107B SHA256
`964df481d44b590cc8d8b1497d6cac048b13d54c85fec24337bac7c5d684fc5f`
is preserved with the exact diagnostic script at
`s3://borsuk-bench-453182569524-euc1/research/v32-quality-perfect-s3-serving/63df86a22ff44f99aeaf387665a062cd708c6a18/attempts/v32-frontier-cost-63df86a2-a0001/RESULT.json`.
It binds the exact broader terminal and input hashes, all per-target inclusive
prefix costs and both fixed-cell counts. Wall362,869,301ns including bounded
downloads and parsing; process peak RSS142,028,800B. Runtime checks enforced
RSS<1GiB and full PSI avg10<=0.5. This is not Rust routing latency.

Astra confirmed the next bounded explanatory cell:1536 leaves,524288 codes,
12288 retained candidates and first-distinct64 pages, unchanged truth/ranking
arithmetic for all96 burned queries. No further ladder or page-budget tuning.
Measure scan CPU and candidate/page-loss stages; changed routing bounds must
produce newly bound captures rather than claiming old hashes remain equal.
Stop on failed recovery and diagnose, with no physical page reads. A successful
replay would explain this failure, not qualify general recall; freeze it before
evaluating the still-unexecuted7168 window. Implementation and execution of
that explanatory cell remain pending at this checkpoint.

### V32 expanded-frontier explanatory replay preregistration

The next cell fixes global leaf limit1536, whole-leaf code ceiling524288,
candidate depth12288, reference capture16 pages and first-distinct physical
page projections16/32/64. Only64 is the recovery decision; the smaller
projections remain explanatory, not a tuning ladder. Serving defaults remain
unchanged. The explicit `--expanded-frontier-replay` diagnostic has schema12;
the independent Python runner authenticates the original construction plan
before requesting these overrides and rejects old-scope evidence.

Replay all three completed windows4096,5120,6144 (32 queries each), using the
exact v3 truth Parquet and receipts bound by the broader terminal SHA256
`5313f81f18aaf08fae1dadb80a25207fa497f05435b2e99d7b03a2be0ebc6daa`.
Use the same query, index, ordered distance arithmetic and candidate/page
tie rules. Do not inspect or run the7168 window in this explanatory attempt.
Do not regenerate truth or fetch corpus shards or vector-page bodies. The
resident routing/PQ artifacts and small frozen references are sufficient.

Record each query's retained candidate rank, first-distinct page rank, all
loss stages, exact scanned leaves/codes, selected page identities/bytes and
new capture digest, plus phase CPU/wall and process RSS. The changed bounds
must not be represented as capture parity with the old768-leaf experiment.
The primary recovery check is960/960 truth-page containment, including the
known query6160/logical411202 counterexample. Even full recovery is burned-set
explanation, not measured reranked recall or independent quality qualification.
If a neighbor remains missing, preserve its precise stage and stop this path
for diagnosis; do not widen candidates/pages/frontier or launch a ladder.

Run one causality Spot attempt with an authenticated release binary built
from the verified source, CPU affinity and worker count recorded. Bound the
scientific process group to3GiB RSS, full memory PSI avg10<=0.5, swap growth
<=256MiB and300s wall; bound setup/build/whole attempt to1800s. Preserve the
original terminal or registered stop, upload immutable evidence and terminate
the owned instance immediately. No restart after a scientific terminal or
pressure stop. A Spot interruption is separately recorded and any replacement
requires a new attempt identity, never a partial measurement merge. No serving
benchmark,100M run or production-default change is authorized by a positive
result alone: first freeze the explanatory result and run the prospectively
held window, retaining the prior-dataset-exposure caveat.

### V32 expanded-frontier explanatory result: recovery with higher CPU

Source and preregistration `1ee31efb081d9b5bb432b93a87aea442341243bb`
implemented the diagnostic-only boundary. TDD missing-API REDs preceded the
implementation; affected search42/42, qualifier29/29 and Python34/34 tests
passed, as did targeted Clippy, Ruff, formatting, syntax and docs checks.
Astra independently reviewed the code and the one-shot controller before
execution. Normal serving limits were not changed.

One causality Spot c7g.4xlarge in eu-central-1b, instance
`i-0d8d7fc51e1784c3e`, completed and was confirmed terminated. Original
controller session23730 exited0; no restart. Evidence prefix:
`s3://borsuk-bench-453182569524-euc1/research/v32-quality-perfect-s3-serving/1ee31efb081d9b5bb432b93a87aea442341243bb/attempts/v32-deep-1m-expanded-1ee31efb-a0001/`.

`TERMINAL.json`2,436,745B SHA256
`f78754e0453d939a2c44a7dfeb332bf08e274264f12a48c706994171c2d00950`
binds all three complete windows, exact inputs, source and build identity.
Source archive8,260,523B SHA256
`3c770a90cc60749356a51abd292307a60b4f8af5f98e5e4314eef12d0c099a7e`;
release qualifier21,953,280B SHA256
`65da0ce4652b112d033ae50c7162e4d1bb08904daa366eaf1cabeef62f13ce01`.
Build used Rust1.98.0/aarch64 release; diagnostic affinityCPU0 and
`BORSUK_CPU_THREADS=1` were explicit. The terminal appeared after the
controller's227-second liveness sample; build/setup time is not query latency.

| Query start | 16-page hits/320 | 32-page hits/320 | 64-page hits/320 | Minimum64 containment |
|---|---:|---:|---:|---:|
|4096|308|318|320|10/10|
|5120|318|320|320|10/10|
|6144|311|319|320|10/10|

All960/960 truth pages were contained at64, with96/96 perfect queries.
For the previous query6160/logical411202 miss, leaf1711 remains global rank1500
(one-based). It now enters the scanned pool at360,482 codes, survives at
candidate rank56 (zero-based), and its page856 is first-distinct rank39
(the40th page). The nested reference16 diagnostic still calls it a
page-reducer miss; the independently checked64-page projection recovers it.
The frontier was the old obstruction, not candidate retention. Across all96
queries, scanned codes315,293..421,521 exactly match the metadata prediction.

Whole diagnostic CPU totals for the three32-query windows were
2,599,840,384 /2,579,883,146 /2,579,825,514ns:80.828635875ms/query overall,
versus about44.79ms in the earlier768-leaf diagnostic (+80.46%). This includes
diagnostic work/reductions and serialization; it is not routing-only CPU,
S3 request latency, paired throughput or an uncertainty-qualified speed claim.
Worker wall12,722,205,962ns included reference downloads and validation.
Qualifier peak RSS100,798,464B. Seven2-second monitor samples showed process
group peak277,028,864B, full PSI0 and swap growth0. `monitor.jsonl`616B SHA256
`dbb32315aaa3131129450c5883ed00ca8a3dd1b0229479f6f82bfdf9640de5d0`.

Input downloads42,479,861B comprised routing metadata/PQ and frozen references;
there were zero corpus-shard reads and zero vector-page reads. The query/truth
identities were unchanged. After terminal, a separate bounded read-only check
authenticated the exact query, manifest, logical-source and page-range files,
all three prior truth/receipt/Arrow batches, and revalidated every result field
against the terminal without invoking the qualifier. All three matched;
explicit scratch cleanup and path absence were confirmed.

Disposition: burned-set recovery only, claim_eligible=false. Do not call it
measured reranked recall, general perfect quality, or100M qualification. Astra
confirmed the next fixed cell is7168..7199 with the same configuration and
v3 truth; no candidate/page/frontier tuning after its outcome. Thereafter,
perform metadata-only code-block access tracing before a provider rewrite or
100M launch. A read-only scaling audit identifies resident all-row PQ and
global all-leaf scoring as separate unresolved limits: at5% high fidelity,
PQ resident bytes alone project to2,535,625,000 at100M and25,356,250,000 at1B,
and the current borrowed-artifact decoder also retains encoded inputs while
allocating decoded planes. Fixed scan/page limits do not bound that loading
cost or the number of centroids scored. These are code-derived projections,
not measured large-scale results.

### V32 fixed prospective7168 window:320/320 contained, no serving claim

Preregistration `9724f9c750de97f57697bfbc20d1edd1821f5947` froze the expanded
configuration before running7168..7199. Source remained
`1ee31efb081d9b5bb432b93a87aea442341243bb`; the exact21,953,280B qualifier
SHA256 `65da0ce4652b112d033ae50c7162e4d1bb08904daa366eaf1cabeef62f13ce01`
was reused without rebuilding. Astra reviewed the one-shot runner. No
frontier, candidate or page parameter was tuned after the preceding result.

Original controller session48450 exited0. One causality Spot c7g.4xlarge in
eu-central-1b, `i-08b8434d54288b547`, completed and was confirmed terminated.
Prefix:
`s3://borsuk-bench-453182569524-euc1/research/v32-quality-perfect-s3-serving/9724f9c750de97f57697bfbc20d1edd1821f5947/attempts/v32-deep-1m-prospective-9724f9c7-a0001/`.
`TERMINAL.json`816,052B SHA256
`c54255e18102a425d740acb7b204bc5215a0325fed5632dae3546571a5cff8cb`.

At16/32/64 pages, contained truth counts were311/316/320 out of320, with
minimum per-query containment6/10,7/10,10/10. All32 queries were perfect at64.
Selected encoded bytes totaled99,621,552 /199,372,464 /398,355,276, but no
vector-page bodies were fetched. Scanned codes328,323..422,404. Whole
diagnostic CPU2,589,891,495ns, or80.93410921875ms/query; phase wall
2,588,159,358ns. This is neither S3 latency nor routing-only CPU.

Fresh v3 reference truth streamed exactly six authenticated shards totaling
402,965,152B, without persisting corpus bodies. Reference construction wall
21,111,126,250ns; whole worker26,819,777,384ns; all input downloads442,984,572B.
Truth Parquet SHA256
`85d7bad04a861e52b85eafae013edd0c8c3b291c0bc757e15a95389ba45ee573`;
truth receipt SHA256
`b68204db5b5b660a06f32f60374b1475df2f2eb3f486bf415c56b26e8b31633e`;
diagnostic Arrow SHA256
`aee174e27796d8f97212f39577ea892099e39ccd9a4ed96fc742870a2f631d3f`;
raw diagnostic SHA256
`c741a1ec7a71d0cfe890126608fb9445545cba4b0b9331800f1a0a20884ac96b`.
The exact query/index identities remain those of the preceding experiment.

Qualifier peak RSS100,646,912B. Fourteen2-second process-group samples show
peak594,055,168B, full PSI0 and swap growth0. `monitor.jsonl`1,240B SHA256
`d34cc15a59c5da9ef2528a3b15e012b73112d99d09bf3f7602923adb2ef2892c`.
After terminal, independent bounded revalidation authenticated the query,
manifest, logical-source/page-range artifacts and new truth/receipt/Arrow;
every result field matched without executing the qualifier. Explicit
verification scratch deletion and absence were confirmed.

Disposition: this previously unexecuted campaign window passes the frozen
containment check. Historical dataset exposure still prevents a globally
untouched holdout claim, and32 queries do not establish general perfect
recall. Do not pool it with the96 burned queries as an independent128-query
qualification. No measured expanded-arm S3 serving latency, write-throughput
or100M result is claimed. The next cheap scaling falsifier is a fixed
hypothetical row-aligned code-block trace, not an immediate100M launch.

### V32 metadata-only code-block trace: naive object-per-block layout rejected

From source `0f72238a40feff412db39dae2ce078dd9d2eef03`, one bounded local
metadata trace completed as original session79040, exit0. The pure block/rank
helper had an intended missing-module RED followed by five literal boundary
tests GREEN; Astra reviewed the driver/helper read-only before execution.
No production provider or defaults changed. The fixed hypothetical layout
uses2730 base24-byte rows or1365 high48-byte rows per block:65,520 payload
bytes per full block, one object per block, no cache or coalescing. Arrow
envelopes are excluded. Final partial blocks are clipped to actual population.

The trace authenticated the preceding expanded/prospective terminals and
manifest, downloaded only query/routing/fidelity metadata (three GETs,
5,057,500B), and matched128 scan counts plus1280 observable target-leaf ranks
before producing costs. This matches observable evidence, not every unrecorded
frontier element. Burned and prospective windows are combined only for access
work accounting, not independent quality qualification. Fidelity rank maps
each disjoint logical interval into separate base/high row planes; block IDs
are deduplicated within each query.

| Hypothetical code-plane work/query | Minimum | Maximum | Mean |
|---|---:|---:|---:|
| Object GETs |255|323|291.5859375|
| Useful code payload bytes |8,429,760|10,752,216|9,787,786.6875|
| Fetched block payload bytes |16,682,400|21,137,760|19,079,878.125|
| Request rounds at32 in flight |8|11|9.640625|

These are additional to vector-page requests. Request rounds are not latency,
and this layout is not a universal lower bound for other S3 organizations.
The whole1M code plane comprises385 such blocks; broad leaf selection touches
255..323 of them. Moving the current code planes to this naive block layout
does not solve selective serving: it creates hundreds of GETs and substantial
read amplification. Do not implement that provider blindly or extrapolate
these1M costs as measured100M behavior.

Result177,982B SHA256
`102b970f913086a7ff3890515bd65a6e4b837ac501307a0e61f586c74f6196ce`;
driver9,358B SHA256
`5a553692577ee2530206c4cdf402b919f6bde705efcbf94fef5fdde7b4407cbc`;
helper3,224B SHA256
`a4efb1dacac1c21ec5b00032f53f10d0c7d0302c2cd024506cfd4fdf8408fee9`;
tests3,517B SHA256
`478c37e0def239ee056ce1c938621cfa15362ed332e64a21cd8ec942dc1483d1`.
All four are preserved under
`s3://borsuk-bench-453182569524-euc1/research/v32-quality-perfect-s3-serving/0f72238a40feff412db39dae2ce078dd9d2eef03/evidence/code-block-trace-102b970f/`.
Result basename `borsuk-code-block-trace-0f72238a.json`; driver has the same
stem with `.py`; helper/test basenames are `borsuk_code_block_trace.py` and
`test_borsuk_code_block_trace.py`. Trace wall1,177,525,985ns, peak RSS141,971,456B;
registered RSS/PSI/swap-growth/wall guards did not fire. No code payload,
vector-page or corpus-body reads occurred and no EC2 instance was launched.
Disposition: claim_eligible=false, reject this naive layout and resolve
routing selectivity/code locality before a100M campaign.

### V32 fixed root64 metadata falsifier: inclusion and row cost pass

Following the naive code-block rejection, Astra proposed selecting exactly
the nearest64 coarse roots and scoring all their descendant codes, with no
microleaf pruning. Before implementation, the fixed hypothesis required all
recorded truth owners within64 roots and at most524288 rows per query;
no truncation, root skipping or root-count tuning was permitted.

An exact-SHA read of the preceding two terminals found all1280 observed
target owners within64 (maximum rank60). One bounded metadata-only driver
then authenticated query, roots, code-parent leaves and routing ranges:
four GETs totaling5,698,334B. It followed the actual ownership chain
`routing_range.code_parent_leaf_ordinal -> hierarchy.leaves.root_ordinal`,
not routing-leaf ordinal indexing. Source-matched double query normalization,
ordered f64 distances to f16 centroids and root-ordinal ties reproduced
all1280 owner identities and one-based root ranks before cost reporting.

All128 queries fit the frozen row budget:425,822..503,541 codes,
mean467,969.8125, zero failures. Every selected root contributes all its
routing ranges. Mixed-width byte cost, candidate retention, final page
containment, S3 latency and throughput were not measured by this driver.
Combining the burned/prospective windows here is access-cost accounting,
not independent128-query quality qualification. Astra checked the saved
result digest and driver after execution and confirmed this interpretation.

Source checkpoint `288e865cb689f5ce1c1427d3ab95053a24744d2c`.
Result38,875B SHA256
`b52247dae73a0ea641f3507de83f7eb2b0b8c94c4958fcd684b2f788bbf44e3c`;
driver8,180B SHA256
`24257961b246740d13a0ba87909f5e6cace580707ead528fcf6930d8730b4ee7`.
Both are preserved under
`s3://borsuk-bench-453182569524-euc1/research/v32-quality-perfect-s3-serving/288e865cb689f5ce1c1427d3ab95053a24744d2c/evidence/root64-cost-b52247da/`
with basenames `borsuk-root64-cost-288e865c.json` and `.py` respectively.
Original command exited0; wall421,578,262ns, peak RSS144,310,272B.
RSS/PSI/swap-growth/wall guards did not fire. Zero PQ code, vector-page or
corpus reads; no EC2 instance was launched.

Next fixed diagnostic: root64/all descendants,524288 hard pre-score row cap,
12288 retained candidates and64-page projection, preserving each code's
existing code-parent residual centroid. Use a distinct diagnostic identity;
do not change serving defaults or call it global1536. Verify selected-root
membership, deterministic ties, exact cap boundaries, code-score equality,
reference16/64 projection and no-page capability before one no-page replay
on the128 now-exposed queries. Inclusion alone does not prove retention.

This is not100M qualification. Keeping128 roots at100M would average781250
rows per root, already above the whole scan budget. A deployable follow-up
requires construction-time bounded root populations and renewed quality
evidence; the1M metadata result does not establish that future hierarchy.

### V32 fixed root64 replay: containment preserved, CPU increases

The preregistered whole-root diagnostic ran once on causality Spot
`i-03f9545b737b2f6d3` (c7g.4xlarge,eu-central-1b), source
`f8f5519554080ce2f17b43b8fc106a41de07ccfa`. Original controller exited0
after confirming the instance terminated; no scientific restart occurred.
All four windows4096/5120/6144/7168 were already exposed. This is a
containment/layout falsifier, not independent recall qualification.

| Query start |16-page hits /320|32-page hits /320|64-page hits /320|
|---|---:|---:|---:|
|4096|308|318|320|
|5120|318|320|320|
|6144|311|319|320|
|7168|311|316|320|

All128 queries retain all10 target pages at64; these aggregates exactly
match the preceding expanded-frontier results. The implementation selects
the nearest64 roots and every descendant, retaining the original code-parent
residual centroids and12288 candidates. No microleaf pruning, partial-root
truncation, vector-page reads or reference-corpus GETs occurred. The saved
schema13 output was independently revalidated against authenticated roots,
code-parent leaves, routing ranges, query Parquet, logical mapping and frozen
truth/receipt/Arrow inputs without executing the PQ diagnostic again.
Every regenerated root-scope/coverage/authority receipt matched; verifier
exited0 and explicitly cleared its temporary files and directory.

Codes scanned425,822..503,541 (mean467,969.8125); routing ranges
scanned2048..2073; code-parent query-table pairs built2024..2038, with
peak live pairs1. The128-query prior expanded diagnostic averaged
80.8550042109375ms CPU/query; this replay is19.654465% higher.

Diagnostic phase CPU by window was3,095,244,684 /3,077,093,650 /
3,111,273,985 /3,099,955,407ns, totaling12,383,567,726ns or
96.746622859375ms/query. These include diagnostic reductions/serialization,
not just routing, and exclude S3 serving. Whole worker wall19,200,259,674ns;
qualifier maximum reported RSS100,786,176B. The11 two-second monitor samples
peaked at310,075,392B process-group RSS, PSI full avg10=0 and swap growth0.
The300-second science/1800-second attempt and memory guards did not fire.
Resident/reference input downloads totaled43,303,945B; this experimental
resident-PQ replay does not implement selective root-object downloads yet.

Terminal3,279,782B SHA256
`1a4bdc15e9775625f39fc8a30e2ff3f30c70217c181ad675eaf74be1aeed697a`;
monitor972B SHA256
`532fd34ae7b5deeaa05aebb855ae3e708cbea8f6110465af789676600dae0a1f`.
Source archive8,272,869B SHA256
`765a710e48924bd40ffb898d8b7d17664ec2e4b292c7ca43d812933737e0937c`;
release qualifier21,962,200B SHA256
`acbbcc6d6f8d9c28dcfa44033c8f05d20ef2963048f8fd0740711f041ea3c090`.
Rust1.98.0 aarch64 release, CPU affinity0, `BORSUK_CPU_THREADS=1`.
Registration, controller, build template, worker, source, binary, per-window
results and terminal are preserved under
`s3://borsuk-bench-453182569524-euc1/research/v32-quality-perfect-s3-serving/f8f5519554080ce2f17b43b8fc106a41de07ccfa/attempts/v32-deep-1m-root64-f8f55195-a0001/`.

Disposition: whole-root grouping preserves the observed containment but
increases diagnostic CPU. It supports evaluating a less fragmented code
layout, not claiming lower measured S3 latency, QPS, write throughput or
100M scalability. Bounded root populations, actual selective code fetches,
CPU profiling and independent quality qualification remain required.

### Preregistered bounded-group metadata spike after root64

Before any new query outcome, freeze one query-independent partition of
the authenticated4096 code parents within their existing128 roots. Parent
weights are the sum of their routing-range rows; verify the1M total and
ownership, exclude zero-weight parents, and reject any parent above8192.
Code parents remain indivisible. For each oversized group, compute a
weighted f64 mean followed by weighted squared deviations, each accumulated
in increasing parent ordinal from exact f16-to-f64 centroids. Select the
maximum-variance coordinate, ties smallest dimension. Sort by that
coordinate then parent ordinal; choose the nonempty cut minimizing integer
`abs(2*left_rows-total_rows)`, ties smallest cut, then recurse left/right
until every group has at most8192 rows. Assign terminal group IDs sorted by
`(root_ordinal,minimum_parent_ordinal)` and recompute weighted means in
parent order. No normalization or f16 rounding of group representatives.
Require each occupied parent exactly once and reject nonfinite metadata.

Freeze the directory and its digest before loading query outcomes. Use the
same source-matched double-normalized query vectors and ordered f64 squared
distance; select nearest64 groups, ties group ordinal. The fixed128 exposed
queries4096/5120/6144/7168 must include every recorded target's owner group.
Any exclusion rejects this fixed hypothesis before PQ replay; no beam sweep,
representative adjustment or page fetch follows. Selected rows are bounded
by524288 by construction. This changes representatives even for unsplit
roots and is a new routing representation, not a pure splitting comparison.
Its output is throwaway feasibility evidence; it does not qualify unseen
recall,100M scaling, S3 latency or write throughput. Only a metadata pass
would justify another fixed PQ replay using unchanged residual code parents.

### Bounded-group metadata result: fixed nearest64 hypothesis rejected

Preregistration/source `b3c43de347513a6ae9fb9c475ff2a66efa1f8ba3`.
The directory was frozen before query outcomes:178 groups,384,840B,
SHA256 `1cd77b268304bc4d36acf9f4beb402ccabc3ec0b1ebde316d2dd7f3a2cdcc995`.
The original one-shot exited0 with scientific `passed=false`; no retry.
All1280 original owner/root-rank checks matched. Nearest64 bounded groups
excluded one target in one of128 queries: query6160, logical411202,
group75. Thus1279/1280 target inclusions and127/128 fully included queries
do not satisfy the frozen all-target gate. No PQ replay or beam adjustment
followed. This is exposed-query inclusion evidence, not measured ANN recall.

Selected populations342,859..377,891 (mean359,519.7578125) all passed the
524288-row cap. Whole driver wall855,571,483ns, maximum RSS168,869,888B;
registered pressure/swap/wall checks did not fire. Four authenticated
metadata GETs totaled5,698,334B, with zero PQ-code, page or corpus reads.
No EC2 instance was launched. Input bytes were not persisted; the frozen
directory, result, driver and literal test/helper remain evidence artifacts.

Result105,427B SHA256
`f0f0b93567448b665d5e8269eb6e670fe325135a140dfe6daef37deb7502e022`;
driver10,086B SHA256
`3546b22516f650cd68ab9cff8d347fdb958626c325423afd9be4f95417fc417e`;
helper3,791B SHA256
`c20535fb4b6142f59257e30b788bfd750c118dc2f53037ab4c710ff472412fd5`;
six-test file2,594B SHA256
`2c5e4475882c960b54e2257f00ae15b3dd9b0d6ac2861243fab8275e300f44c1`.
The helper had missing-module RED then6/6 literal GREEN, and Astra reviewed
the fixed algorithm/helper and final authenticated driver before execution.
These artifacts are preserved under
`s3://borsuk-bench-453182569524-euc1/research/v32-quality-perfect-s3-serving/b3c43de347513a6ae9fb9c475ff2a66efa1f8ba3/evidence/bounded-groups-f0f0b935/`.

Disposition: reject this fixed weighted-mean bounded-group router under its
preregistered inclusion gate. Reduced scan work does not compensate for
the miss. This does not establish that all bounded-object hierarchies fail,
and no production layout or compatibility behavior was changed.

### Root-preserving bounded storage accounting: exact population retained

Keep the successful root64 router unchanged; use the178 bounded groups
only as storage chunks, fetching every group belonging to each selected
root. Unlike the rejected group-centroid router, no group representative
participates in selection. One metadata-only accounting run authenticated
the root64 terminal and frozen group directory plus routing ranges,
code-parent ownership and PQ fidelity. It verified unique exhaustive parent
membership, each group population and the exact sorted logical-range union
for all128 queries, not merely equal row counts.

| Same root64 logical population |Minimum|Maximum|Mean per query|
|---|---:|---:|---:|
|Bounded group object count|76|89|83.8984375|
|Useful24/48-byte code payload|11,220,720|12,924,480|12,090,384.75|
|Fragmented block object count|203|243|226.6015625|
|Fragmented fetched code payload|13,275,360|15,897,120|14,822,971.875|

The fragmented comparison uses the same root64 rows, not the preceding
global1536 population. Thus the earlier approximately292 requests/19.1MB
is not the paired baseline here. Both accounting models exclude Arrow
envelopes, compression, caches and request retries. Counts are hypothetical
object GETs, not measured latency. Base/high rows average432,173.59375 /
35,796.21875, with425,822..503,541 total rows as in the root64 replay.
No new PQ scoring or quality evaluation was performed.

Source `6baecd7366d7b1904f33ff0796d57a034604357f`;
result199,238B SHA256
`414bebb83694b6a4a34892a5fc7fe9b61d6450f6dfcb55f61c9fcac9e3f5c155`.
The literal accounting helper had missing-module RED then3/3 GREEN;
Astra reviewed the helper and driver before the original one-shot run.
Original exit0; three metadata GETs2,059,646B, wall2,023,352,032ns,
maximum RSS111,939,584B. Pressure/swap/wall guards did not fire; zero
code/page/corpus reads and no EC2 launch. Driver9,121B SHA256
`14f044a425013abd2900a6b54a47c83cc0966b585b7fe95bc6eb5d5138f4ee88`;
accounting helper2,286B SHA256
`11ac995f9e352838e811f5bd8d10e2cb15ada1eeefe15e1dacd1256562f58ff4`;
test1,812B SHA256
`24a63d073143e13716d7e7a982be6d146329d6127d0a7bfc153e4aeef46d5bd5`.
The result and driver/helpers/tests are preserved under
`s3://borsuk-bench-453182569524-euc1/research/v32-quality-perfect-s3-serving/6baecd7366d7b1904f33ff0796d57a034604357f/evidence/group-storage-414bebb8/`.
Disposition: the request reduction merits a bounded streaming-code reader
prototype, provided actual object envelopes and deterministic candidate
equivalence are verified. This does not solve the separately unqualified
100M routing geometry and does not alter serving defaults.

### V33 exposed-query group-shape proxy: prototypes improve but do not qualify

Source `a2a301ae148944337f89514e33a34f75f17d30e9` implemented the
preregistered metadata-only V33 proxy. Two pre-scoring attempts terminated at
direct-script import and sparse populated-parent authority defects. Both were
reproduced with focused tests, repaired without changing the frozen scorer or
budgets, and produced no scientific result. The historical bounded-group
writer explicitly excludes zero-population parents; the repaired adapter
preserves the remaining original parent ordinals rather than inventing empty
representatives. The focused proxy gate is7/7 GREEN, with scoped Ruff,
`py_compile`, and diff checks GREEN before the scientific attempt.

The sole scoring attempt authenticated six frozen objects totaling9,309,241B:
the178-group directory, expanded and prospective V32 terminals,4096-parent
leaf centroids,4141 routing ranges, and10,000-row query Parquet. It combined
the same128 already-exposed queries and1,280 recorded truth-owner identities;
therefore it is a burned-set explanatory proxy, not held-out evidence. It read
zero PQ codes, corpus rows, or page bodies and launched no EC2 instance.

Both arms ranked the178 complete storage groups and admitted the longest
prefix bounded by64 groups and131,072 logical rows. The control used each
group's frozen population-weighted centroid. The alternative used up to three
population-weighted parent prototypes, initialized by deterministic
farthest-first choice, refined for exactly ten Lloyd iterations, and stored as
f16. Query outcomes did not influence construction.

| Fixed arm | Owners included | Perfect queries | Missed owners | Selected groups min/median/max | Selected rows min/median/p95/max | Owner rank p50/p95/max |
|---|---:|---:|---:|---:|---:|---:|
|Weighted mean|1,255/1,280|115/128|25|20/23/25|123,237/128,646/130,712/131,018|1/11/68|
|Three parent prototypes|1,265/1,280|115/128|15|20/23/25|123,237/127,651.5/130,749/131,023|1/11/67|

The prototype arm reduced mean selected population from128,288.09375 to
127,774.7578125 rows and mean truth-owner rank from3.35546875 to3.18359375,
but still failed13 queries. Thus multimodal representation helps this coarse
grouping, yet neither arm satisfies the frozen1,280/1,280 owner and128/128
query gate at the target row budget. No post-outcome prototype-count or budget
sweep follows.

Canonical result51,033B SHA256
`d52137c6b9746a95fa4784d4b3e2d2f556c9120c94b8633e1b9ad18c7b1480b6`
is preserved at
`s3://borsuk-bench-453182569524-euc1/research/v33-shape-aware-group-routing/a2a301ae148944337f89514e33a34f75f17d30e9/evidence/group-proxy-d52137c6/result.json`.
It is `claim_eligible=false`; scratch inputs were explicitly removed, the
process cleared, local memory PSI full avg10 remained0.00 after terminal, and
swap remained250MiB. Disposition: reject fixed three-parent groups as a
qualifying router at131,072 rows. Continue only with preregistered analytic
moment/diagonal shape arms and a same-byte finer-cell control before any fresh
cohort, production layout,100M campaign, or paid D3 work.
