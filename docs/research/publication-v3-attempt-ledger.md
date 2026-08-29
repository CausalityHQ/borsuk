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
