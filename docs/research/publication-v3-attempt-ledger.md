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
