# Group-commit scalability attempt ledger

Historical campaigns qualified the production `GroupCommitWriter` at 2,000
and 16,000 logical cells with 1, 8, and 32 concurrent producers over five
repetitions using a 96-dimensional control. The current realistic architecture
qualification uses checksum-pinned 768D Cohere vectors, independent 1/2/4
worker-lane factors, and three repetitions; it remains ineligible for
publication claims, which require a separately frozen five-repetition run.
While an attempt is incomplete, inspect only terminal markers, service/process
health, non-measurement progress, and resource availability. Never inspect a
partial summary or samples CSV.

| Attempt | Revision | Source SHA-256 | Manifest SHA-256 | Result prefix | Index prefix | Status |
|---|---|---|---|---|---|---|
| v1 | `3ea0335` | `1fb3282fe8b3ea8327c60c121e394f50dd2bb36ff866c7c1e4102af015dc891a` | `c9a3914d39ded8b119f19f61f6faf8c58068c9d8f99b53d5f0f4deadb2e727bf` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260803-v1/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260803-v1/index` | terminal failure at `c2000/r01/w8`: resource exit 1 after 9:47.29; journal reported the combined post-reopen visibility/recall gate. No campaign CSV was inspected. Investigation found that writer-count cells reused identical vectors under different IDs, making exact top-1 tie-sensitive, while 20 exhaustive S3 scans per cell made validation dominate the ingest experiment. |
| v2 | `a49d10b` | `caca6cdf273c125712cf2bc0e5218cc045b1a951817ff145222951c8a3fe2598` | `cadc28cc51d96ab0f4a26bed037836ca61d9950c13be8f3a75499150d4336a84` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260803-v2/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260803-v2/index` | terminal timeout at `c16000/r01/w8` after every 2K cell and `c16000/r01/w1` advanced: service and resource exit 124 at the frozen 30:00.04 bound, peak RSS 719,696 KiB, explicit failure marker. No incomplete campaign CSV was inspected. Point validation exposed repeated generation resolution and allocation of the complete live WAL tail for every ID lookup. |
| v3 | `565b6d1` | `ea77af985a0ea5daf3a93ece3aa57ebb80f18ebdcb8e4d52e81831808c8a21f2` | `ed84a8aa6cf2254d972f9dd99b488cbd7d23d60725f27e6e2f99ae250ce2acdf` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260803-v3/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260803-v3/index` | terminal timeout at `c16000/r01/w8`: all fifteen 2K cells and `c16000/r01/w1` advanced, then resource/service exit 124 at the frozen 30-minute bound with an explicit failure marker. No incomplete CSV was inspected. Generation-indexed point lookup reduced live process memory versus v2, but the remaining exhaustive 16K-cell S3 recall scan still dominated validation. |
| v4 | `ee3049c` | not launched | not launched | not launched | not launched | superseded before launch: fail-fast write gates were added, but the protocol retained the exhaustive S3 recall scan exposed by v3 |
| v5 | `bb45436` | `8f0cb3eb0cfac456fc447dfff4460bce87590fed189feba3b2de0e4bff2c0aea` | `c2055d1b95d4272ab39870f244dc1141643d447a5ff54522ae75efb11258e500` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260803-v5/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260803-v5/index` | terminal production-gate failure at the first `c2000/r01/w1` cell, with service exit 1 and explicit root/cell failure markers. No incomplete CSV was inspected. The v5 marker contract identified only the aggregate performance gate, so the failing sub-gate is not defensibly known. |
| v6 | `eeb6d5b` | `c5be30676b1fe13ec75135e454fb7b666828dc634702f18a55a16e3ae5afd859` | `24ccf43afdaa0bbc83c888d8ca137e4d87b1a8f947d93667576f1d599013a916` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260803-v6/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260803-v6/index` | terminal at the first `c2000/r01/w1` cell: write p95 and recall gates passed, while explicit markers identify write throughput below 5 records/s and production read p95 at least 200 ms. Service exit 1 and root failure marker present; no incomplete CSV inspected. |
| v7 | `507c74d` | `013a53c59be73f6858b6072748e51a2839a8a9c11bb5a6ca379237b05cdd5726` | `5cdd9251575e8af08ed1365fa3b731e815debc641a57a71ac12ec9b4bbb13a49` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260803-v7/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260803-v7/index` | terminal at the first `c2000/r01/w1` cell: the explicit write-p95 marker proves durable operation p95 was at least 200 ms; no throughput, read-p95, or recall failure marker was emitted. Service exit 1 and root failure marker present with 58 GiB memory available; no incomplete CSV inspected. Depth eight crossed the latency bound, while v6 depth one had passed write p95 but failed throughput, motivating a preregistered midpoint without claiming an unmeasured value. |
| v8 | `e7472a4` | `0a036513bfea2f49f3bd81f4ceb4da41c4059b8a82444563c8b8593fc1a5e795` | `f111744dcf2ab51da9005d7ea9a9f8b4b1bd946c516c030bb61792a9464f7972` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260803-v8/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260803-v8/index` | terminal at `c2000/r01/w8`: the preceding `w1` cell passed every production gate, then the explicit write-p95 marker proves eight writers had durable operation p95 at least 200 ms. No throughput, read-p95, or recall failure marker was emitted; service exit 1 and root failure marker present, no incomplete CSV inspected. This isolates serialization through the single commit worker as the next production bottleneck. |
| v9 | `68ab091` | `bb2fc97d23528db54f754790707a10743c710b97b631a41c49916a1ba3369938` | `cf584e7da95600dd4b93490c550d4720c31155f10e1d20d895b732cfe59ae549` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260803-v9/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260803-v9/index` | terminal at the first `c2000/r01/w1` cell: only the explicit write-p95 failure marker was emitted, with service exit 1, root failure marker, and 58 GiB available memory; no incomplete CSV inspected. Per-append round-robin dispatch scattered one producer's four-ticket pipeline across four competing one-record transactions instead of forming one group. |
| v10 | `fb98977` | `bfd5f868d0a9f4041fdfb8c45cf755b467a6454627327c1ea1063012afcb5de7` | `a2ee0ac900b3e326dfe176bcf8ca36f15c41fb69513ddd4e6f8e0e63bbb91796` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260803-v10/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260803-v10/index` | terminal at the first `c2000/r01/w1` cell: only the explicit write-p95 failure marker was emitted, with service exit 1, root failure marker, and 58 GiB available memory; no incomplete CSV inspected. Affinity was retained, leaving the unnecessarily wrapped clone used for lane zero as the production-path difference from v8. |
| v11 | `75787b9` | `085e78073650d8bda60f1abe7a98820af5f69378f3204d4b939dc4215435190f` | `712978fe8645f20bf08d1a62cd0b6c305e9391822dacc71082359fc9a5435db5` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260803-v11/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260803-v11/index` | terminal at the first `c2000/r01/w1` cell: only the explicit write-p95 failure marker was emitted, with service exit 1, root failure marker, and 58 GiB available memory; no incomplete CSV inspected. Restoring the original lane-zero handle did not recover the v8 pass, disproving the wrapper hypothesis and showing that fixed foreground S3 publication latency remains too close to the 200 ms bound. |
| v12 | `d90a774` | `4fe32db84943f41adc7a1a528b05e2ced833d8d7c686870a0d5c0be7bcfacc5a` | `90f02139221a051aa375d410af7f9117d522327e766a404ce1256b9619f8bb1d` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260803-v12/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260803-v12/index` | terminal at `c2000/r01/w8`: the preceding `w1` cell passed every production gate, then explicit markers identified write p95 at least 200 ms, throughput below 40 records/s, and read p95 at least 200 ms; no recall failure marker was emitted. Service exit 1 with 58 GiB available; no incomplete CSV inspected. The concurrent lanes still share the per-group `id-directory/last-write-wins/NEXT` CAS allocator, a serialized critical path now under root-cause investigation. |
| v13 | `4c8f76c` | `2a9a7b0f3c078ac7ac57d7c80fdbd4ecb08d3d0fdb83531a0e0e63f36060caa9` | `cb73ab58366570446889624c4a7000a4adf773cd0f0f83c3c73d2f41748f6f0b` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260804-v13/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260804-v13/index` | terminal at `c2000/r01/w8`: the preceding `w1` cell passed every production gate, then explicit markers identified write p95 at least 200 ms, throughput below 40 records/s, and read p95 at least 200 ms; no recall failure marker was emitted. Service exit 1 with 58 GiB available and 121 GiB free disk; no incomplete CSV inspected. Amortizing the last-write-wins counter CAS therefore did not remove the production bottleneck. Source inspection independently confirms every acknowledgement still serializes root reservation GET+CAS, immutable WAL/descriptor upload, and final frontier CAS. |
| v14 | `b1ec3b1` | `b50004ab2ffd13272afb246f9525324ab18494de2837f7fdaf064a5b3eb0cace` | `acac024bcb0112e855f2000803a2499f1c4de579122a4cec95fbe3a8962d1e57` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260804-v14/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260804-v14/index` | terminal timeout at `c2000/r01/w8`: the preceding `w1` cell passed every production gate, then the eight-writer cell reached the exact 1,800-second bound and the service/root exited 124 with `GROUP_COMMIT_SCALABILITY_FAILED`. No cell sub-gate marker was emitted, so write latency, throughput, read latency, and recall are not defensibly separable; no incomplete CSV was inspected. Memory and disk remained healthy at 58 GiB available and 120 GiB free. Carrying four commits per shard removed steady-state admission requests but clustered frontier transactions; source review predicts earlier per-shard soft-pressure maintenance, now the leading hypothesis rather than a measured claim. |
| v15 | `2e6f6ff` | `2984bc4c9a0bf5d965fe8269e0cf3b3cca6e35005635d27d75849d43e413aed0` | `e90dbfde01a8fdc2c1676ec5f22a111d1eed60399f5be3ad7da700268bfec93f` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260804-v15/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260804-v15/index` | terminal production-gate failure at `c2000/r01/w8`: the preceding `w1` cell passed all gates at 156.040 ms write p95, 36.467 records/s, 1.90 requests/record, 2.268 ms read p95, and recall@1 1.0. The eight-writer cell completed 800 visible records with recall@1 1.0 but failed at 350.925 ms write p95, 3.513 records/s total, 93.766 requests/record, and 60,941.567 ms read p95. It issued 75,013 requests, including 7,476 HEADs; a small set of groups amplified from the ordinary 7--9 requests to 5,001--30,792 requests. Service exit 1 and explicit root/sub-gate markers were present with 58 GiB memory available and 118 GiB free disk. The fail-closed campaign validator rejected the terminal artifacts because the success marker and full matrix were absent. Source tracing ties the amplification to synchronous global WAL materialization after a reused root shard reaches eight commits; a new writer starts a random shard phase without accounting for the growing index's occupancy. No v15 result is publication eligible. |
| v16 | `a602578` | `f2c22e7709aa7b5edf5d7e5de8a1f499c63564ba12b0c7f03fcbb675e7e6222a` | `ffe6865b7a48771bcd6d42b82f6373202091ddc25a0dc8a8ce26c3d50a0dceb6` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260804-v16/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260804-v16/index` | terminal production-gate failure at `c2000/r01/w1`: immutable pending publication passed write p95 at 184.526 ms, throughput at 23.775 records/s, and recall@1 at 1.0 with zero HEADs and 2.51 requests/record. The separately measured drain took 177.555 s. Read p95 failed at 7,221.377 ms because the preregistered `SrhtPqScan` had no segment budget after materializing 100 small segments. Service exit 1 and explicit root/read-p95 failure markers were present with 58 GiB available memory and 117 GiB free disk. The fail-closed validator rejected the incomplete campaign. |
| v17 | `1e0e1bb` | `64274aa0e109eac1c7e685f3d70febbb612a77f13a1038f0e53ef86a2691daa4` | `2d0d98ee12f04764d79372ccaf53461efef5fe7ebd204014beefac77cf36fbb9` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260804-v17/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260804-v17/index` | terminal production-gate failure at `c2000/r01/w1`: `max_read_segments=8` reduced read p95 from v16's 7,221.377 ms to 294.469 ms while preserving recall@1 1.0, but remained above 200 ms. Write p95 passed narrowly at 194.222 ms, throughput passed at 23.456 records/s, and drain took 176.425 s. Service exit 1 and explicit root/read-p95 markers were present with 58 GiB available memory and 116 GiB free disk; the validator rejected the incomplete campaign. |
| v18 | `2d1f3ee` | `77d4cac1356c991df569f2af29f42be146bde15670b81a81acbdefdfad746771` | `2d8212dc03001bcbd2b79ec4bc20f1a9c5f1e6fcfdd318aaa1d03917ce7bafa9` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260804-v18/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260804-v18/index` | terminal production-gate failure at `c2000/r01/w1`: moving transaction-state finalization off ACK improved write p95 to 124.385 ms, throughput to 35.664 records/s, and foreground cost to 2.01 requests/record. Recall@1 remained 1.0, but preregistered `max_read_segments=4` reached 273.241 ms read p95 and failed the 200 ms gate; drain took 211.151 s. Exact root/read-p95 failure markers were present, the service exited, resources remained healthy, and the fail-closed validator rejected the incomplete campaign. |
| v19 | `1820a5f` | `fa42f0cd99ea1122c14d4586dc8442bec63eb73baa4c4fe771454178a44c57ea` | `9fa0af4a907797d10bb92b7568456776a7cb907222e0005082280cd837cdec88` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260804-v19/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260804-v19/index` | terminal diagnostic failure at `c2000/r01/w1`: write p95 144.819 ms, 35.818 records/s, 2.01 requests/record, and recall@1 1.0 passed; read p95 213.986 ms failed. The 20 terminal queries issued 421 requests (341 GET, 80 HEAD), read 932,837 bytes, and searched 160 segments: exactly 21.05 requests and 8 segments/query despite `max_read_segments=4`. Source tracing reproduced that the resident global base and materialized delta each consumed the full budget. Drain took 181.944 s, resources remained healthy, and the fail-closed validator rejected the incomplete campaign. |
| v20 | `f5b809d` | `59c8932cf49ec8cc68cadd4ad4ab9cca72495647a88b079db96dba96ff2e4256` | `70e0b1837b80e61fd905ebfa6f1d039c96ce36620aacea1ebfd309d100cd2053` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260804-v20/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260804-v20/index` | terminal read-p95 failure at `c2000/r01/w1`: unified drain reduced the measured search from 8 to exactly 4 segments/query, 21.05 to 8.55 requests/query, and 45.5 to 22.9 KiB/query. Read p50 improved to 110.064 ms and p95 to 203.846 ms, narrowly above the 200 ms gate; recall@1 remained 1.0. Write p95 134.647 ms, 35.950 records/s, and 2.01 requests/record passed. Rebuilding the global artifact increased drain to 474.054 s. Exact root/read failure markers were present, resources remained healthy, and the fail-closed validator rejected the incomplete campaign. |
| v21 | `7e27e2d` | `37402caae468d47bb4415d15a773f964e6549874bcda18e19399536c8fb84d50` | `12f6e7a5f398482c204e2a0437bfab0136da6fd5df80dbbffbddbc50d0de4c93` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260804-v21/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260804-v21/index` | terminal production-gate failure at `c2000/r01/w8`. The validated preceding `w1` cell passed at 141.044 ms write p95, 34.706 records/s, 144.551 ms read p95, and recall@1 1.0. The terminal `w8` cell failed only write p95 at 372.224 ms; throughput was 117.518 records/s, read p95 147.548 ms, and recall@1 1.0. Drain took 650.805 s. The fail-closed validator rejected the root failure marker before measurement inspection. Diagnostic inspection then found a telemetry defect: lanes 1--7 each reported exactly 8 requests per four-record group (one lane paid a one-request lease initialization), while lane zero falsely averaged 63.56 because child independent counters wrap and increment the retained original counting store. Therefore the reported 2,990-request aggregate and 3.7375 requests/record are invalid and must not be used as physical-request claims; write latency, throughput, read telemetry, and recall remain valid terminal measurements. Root and exact write-p95 failure markers were present, the service exited, and resources remained healthy. |
| v22 | `3547acb` | `ee2d9201ac3b3620f4fc2a1a546baaab2ddfee77742052fefb3c13e3daa458a4` | `ce80720be3f7f0f727acc74ccd19c025d3f3bd6c8b204d445c37b801d627bc0c` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260805-v22/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260805-v22/index` | terminal timeout at `c2000/r01/w32`. Validated `w1` passed at 130.080 ms write p95, 34.924 records/s, 2.01 requests/record, 150.622 ms read p95, and recall@1 1.0. Validated `w8` confirmed the shared-lane architecture: every group contained 32 records, write p95 was 117.818 ms, throughput 299.456 records/s, requests/record 0.25125, read p95 179.645 ms, and recall@1 1.0. The `w32` process then reached the exact 1,800-second bound with resource exit 124 and a root failure marker but no cell or sub-gate marker; it emitted no cell artifacts, so no `w32` CSV was inspected and its gates are not defensibly separable. Peak RSS was 290,552 KiB and host resources remained healthy. Source tracing found that drain's global-PQ refresh performs two strictly sequential full-segment object-store passes; the timeout process used only 2% CPU, making bounded parallel rebuild I/O the next causal experiment rather than changing the successful foreground grouping. |
| v23 | `1896ada` | `e61f705c87414ff8141b9b3735a41e302dc9a675f47c6a3d48e5989ecc38bcec` | `3e9c4b638b6e249f31a8d7260f5d60926a4752f776414a45d552c283ee376813` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260805-v23/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260805-v23/index` | terminal read-p95 failure at the first `c2000/r01/w1` cell. All four cell phase markers were present before inspection. The fail-closed validator first rejected the root failure marker; terminal diagnostic inspection then found write p95 159.341 ms, 35.669 records/s, 2.01 requests/record, and recall@1 1.0 passed, while read p95 205.398 ms narrowly failed. Deterministic bounded rebuild reads reduced drain from v22's 481.669 s to 225.023 s (53.3%) without changing the four-segment query shape. The 20 terminal reads issued 171 GETs and no other request class; the slowest queries required 9--10 GETs. Exact source tracing found a second object-store stage after lossless-vector reranking that rereads physical segment sidecars only to recover ID and generation. The service and benchmark exited with exact root/read-p95 failure markers and healthy resources; no v23 result is publication eligible. |
| v24 | `46ac7a3` | `114868c789a875bce9fb153d3bf594c62b9c0c845fe87af5209d6720ae6de425` | `15ba755d6e7ef8306730f60bb6f036247dd654d7f48dd47f39f365918d960569` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260805-v24/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260805-v24/index` | terminal write-p95 failure at `c2000/r01/w32`. The preceding `w1` cell passed at 117.887 ms write p95, 36.172 records/s, 2.01 requests/record, 119.527 ms read p95, and recall@1 1.0. The preceding `w8` cell passed at 130.252 ms write p95, 274.882 records/s, 0.25125 requests/record, 108.870 ms read p95, and recall@1 1.0. The terminal `w32` cell completed all phases and preserved recall@1 1.0, 100.886 ms read p95, 614.013 records/s, and 0.1253125 requests/record, but write p95 was 240.997 ms. Every `w32` group contained the configured maximum 64 records, so 128 concurrently outstanding tickets were necessarily split across two sequential durable groups. The `identity-v1` path reduced the comparable `w1` read total from v23's 171 GETs to 131 and read p95 from 205.398 ms to 119.527 ms, with a disclosed byte increase from 485,176 to 559,152. Exact root/write-p95 failure markers were present, the service exited 1, memory and disk remained healthy, and the fail-closed validator rejected the root failure before terminal CSV inspection. No v24 result is publication eligible. |
| v25 | `5b02651` | `c6051fd0666d5e4f6cad209654eda06d378632235d1ed3d27b0e2199b5553712` | `18fc26edff80bb50e538518ac2fd8e86839f028cec2ea5bac8ca9ae1e53594bd` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260805-v25/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260805-v25/index` | terminal timeout at `c2000/r05/w32`. Fourteen preceding 2K cells completed every phase and production gate. In the four completed 32-writer repetitions, one 128-record durable group kept write p95 at 144.270--164.839 ms, sustained 954.295--1,018.314 records/s, and reduced foreground traffic to 0.0628125 requests/record, confirming that v24's 64-record cap caused its two-group latency failure. The terminal cell completed ingest at 07:22:14 UTC and drain at 07:36:49, but did not complete point visibility before the exact 1,800-second bound at 07:52:05. Resource exit was 124, CPU utilization was 4%, peak RSS was 934,204 KiB, and the host retained about 58 GiB available memory and 106 GiB free disk. The fail-closed root validator rejected the missing completion marker, and terminal-cell validation rejected the root failure marker. The timeout marker and raw artifacts are preserved; the incomplete terminal-cell CSV was not inspected. Source tracing shows that the runner verifies all 3,200 inserted IDs with sequential `get_record` calls after drain, repeatedly traversing immutable object-store routing and segment state. V25 is not publication eligible and motivates a public batched point-read primitive whose request complexity follows unique immutable objects rather than requested IDs. The 96-dimensional control does not qualify production embedding read performance. |
| v26-realistic | `6b27c28` | `b85bbe9ac6707592d8574a7677aa0a912afdb999647f9d107a9a1120c88d18c0` | `0df32bfdb06d7c085c5e07a61635c6b3c41957743cceb3f9f0461e7bcd72da17` | not allocated | not allocated | preregistered, not launched. The 768D Cohere architecture qualification isolates 1/2/4 worker lanes, 1/8/32 writers, and 2K/16K logical cells over three cyclically ordered repetitions. Every cell receives a pristine clone of its immutable cosine base and terminal validation before aggregation. Write/read p95 and inserted-ID visibility gates apply to every cell; the 10,000 vectors/s burst bulk target applies only to 32-writer cells and is not a sustained-throughput claim. Local structural smoke and all repository gates passed. Fable/Opus review and per-factor safety closure remain launch prerequisites. |
| v27-sustained | `8cfd04f` | `93421fd25da5cfd38892f681f9fb4dcf55f810cd7942f473132a3e5d0a5a655b` | `83cc17397f93a36769f00c6d8e960153a11bf69278118de728e864d0fa88b2ec` | not allocated | not allocated | preregistered, not launched. GPT-5.6 Sol review rejected v26 as insufficient for production claims. The replacement uses treatment-independent IDs, 1/2/4/8 worker lanes, 1/8/32 writers, 2K/16K logical cells, five paired repetitions, and 1,000 operations per writer to cross repeated background-materialization boundaries. The 32-writer cells must pass both 10,000 durable acknowledgements/s and 10,000 records/s through final drain; all summaries are recomputed from raw evidence and resource telemetry must bracket ingest plus drain. Post-drain inserted-ID retrieval on the synthetic logical-cell base remains visibility evidence only, not corpus recall or production read qualification. Local structural smoke, 443 script tests, strict Clippy, policy, formatting, and 25 group-commit integration tests passed. Per-factor safety closure remains a launch prerequisite. |
| v28-sustained | `4bf608f` | `7986bf797ff126a0eda70685b7b2376a202dfb9f93377c81a4def5c8bd380ce2` | `be4698ff6cb8defd8008457763c7b0ec00e1101069e0c6c3a62f1856458b9d9c` | not allocated | not allocated | preregistered, not launched; supersedes v27 before any paid prefix was allocated. The sustained protocol is unchanged, and a factor-spanning integration gate now proves acknowledged reopen visibility, sequential last-write-wins, and drain/reopen correctness at every preregistered 1/2/4/8 worker-lane factor. Local structural smoke, 443 script tests, strict Clippy, policy, formatting, and 26 group-commit integration tests passed. Production corpus recall/read-latency qualification remains separate and unresolved. |
| v29-sustained | `c8d3b34` | `e1f319c9b69e4e48ad19740043fa09932843d1e456a7aebd4cabec1f180a24ce` | `be4698ff6cb8defd8008457763c7b0ec00e1101069e0c6c3a62f1856458b9d9c` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260805-v29/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260805-v29/index/` | terminal preflight failure before output creation or measurement. The archive identity check passed, then the read-only Cohere dataset validator exited because system Python lacked checksum-pinned `pyarrow`; no result/index object or measurement CSV was created. Service exit was 1, and the host retained 59 GiB available memory and 87 GiB free disk. Replaced by an isolated `uv` validation environment from the pinned format requirements. |
| v30-sustained | `a595717` | `1e931b1a5eb466b58de16adad705e5cea959dcbfc9e1b7b52370bdaf868085ff` | `be4698ff6cb8defd8008457763c7b0ec00e1101069e0c6c3a62f1856458b9d9c` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260805-v30/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260805-v30/index/` | terminal at the first `c2000/r01/l1/w1` cell. Every phase marker was present, then the exact write-p95 failure marker and root/cell failure markers were emitted; no throughput, end-to-end throughput, read-p95, or inserted-ID visibility failure marker was present. The fail-closed terminal validator rejected the root failure marker before reading CSVs, so the only numeric conclusion is write p95 at least 200 ms. Service exit was 1; the host retained 59 GiB available memory and 86 GiB free disk. Direct source tracing confirms each acknowledgement still serializes an immutable block PUT and a conditional growing-HEAD PUT. GPT-5.6 Sol review selected a one-CAS inline-HEAD with asynchronous spill as the smallest architecture preserving immediate fencing and fixed-fanout reopen. |
| v31-inline-head | `a5a5573` | `b2e6d04acf0fe3bf7315e79543a8e4cf3b6a56c3decb3ae079e6cb2343f74bc0` | `cd9f8b49a7e624ea948acee1dfe27c68e3036b45b0fa82035ece40d1a15d596f` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260805-v31/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260805-v31/index/` | terminal failure in first cell `c2000/r01/l1/w1`; root/cell fail-closed validators rejected the campaign before measurement inspection. All phase markers were present, with write-p95 and active-tail-read-p95 failure markers and no other phase failure. The complete cell recorded 1,000 records, 836 groups, mean group 1.196, 2.210 acknowledged records/s, 1.849 drain-inclusive records/s, write p50/p95 241.252/603.825 ms, active-tail read p50/p95 812.839/951.618 ms, post-drain read p50/p95 59.284/73.770 ms, visibility 1,000, and inserted-ID recall@10 1.0. The complete physical trace recorded 920,705,087 write bytes and 80,124 write requests for 3,072,000 input vector bytes (~299.71x). Causal inspection identified synchronous four-block spill in the owning append worker and a corpus-wide global-PQ rebuild during drain; the campaign remains immutable failed evidence. |
| v32-search-budget | `6d0215a` | `8ae704d2d73b58df12df570e5c669becf3b3372558322b960adf1eaf55333da9` | `e05d6718eafd72723ef4b72990132abc5ec35b30d08f3e261f728cc02c15158f` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260806T202000Z/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260806T202000Z/index/` | terminal failure before measurement inspection in `c2000/r01/l1/w1`. Root and cell failure markers were present and the process exited; the pane reported `InvalidSearchOptions("max_segments 4 leaves no stable-base probe after reserving 8 exact-fringe segments and 0 delta probes")`. No measurement CSV was read. The next revision permits an exact-fringe-only merge when the configured segment budget is smaller than the fringe. |
| v33-search-budget-aws | `f0249a6` | `ace58ebf9c4d130a5c1b1d659761c2ed6caf1ea068bca16b357ec0f4fca03405` | `e05d6718eafd72723ef4b72990132abc5ec35b30d08f3e261f728cc02c15158f` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260806T210000Z/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260806T210000Z/index/` | terminal failure in `c2000/r01/l1/w1`; the worker exited with status 1 and emitted root/cell failure plus `PRODUCTION_WRITE_P95_FAILED`, `PRODUCTION_ACTIVE_TAIL_READ_P95_FAILED`, and `PRODUCTION_READ_P95_FAILED`. No measurement CSV was opened; only terminal marker names and worker health were inspected. Local bulk structural smoke passed on this revision. |
| v34-wal-refresh-aws | `6ba7b1d` | `ec094fde22e8cbe0462d78ab355af68371ee3b09b9e646f8c58aec8b3ed2fb2e` | `e05d6718eafd72723ef4b72990132abc5ec35b30d08f3e261f728cc02c15158f` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260806T220000Z/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260806T220000Z/index/` | terminal failure in `c2000/r01/l1/w1`; only `PRODUCTION_ACTIVE_TAIL_READ_P95_FAILED` and `PRODUCTION_READ_P95_FAILED` were present (write-p95 passed). No measurement CSV was opened; only terminal marker names and worker health were inspected. |
| v35-coalesce-aws | `ff8f5ec` | `1053d31bead88207b5ebbc6ec77876d04434cbe9337d6ab7c0da2c82e0d6b3c9` | `e05d6718eafd72723ef4b72990132abc5ec35b30d08f3e261f728cc02c15158f` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260806T230000Z/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260806T230000Z/index/` | terminal failure in `c2000/r01/l1/w1`; only `PRODUCTION_READ_P95_FAILED` remained (write and active-tail read gates passed). No measurement CSV was opened; only terminal marker names and worker health were inspected. The remaining cause was traced to the routing fixture's one-vector base segment setting being reused for WAL materialization, creating one tiny immutable segment per record. |
| v36-coalesce-aws | `da66c03` | `baaf67d58d594a5564ec3348ed27a48c5209065af378b719ffd253c0c477ccdf` | `e05d6718eafd72723ef4b72990132abc5ec35b30d08f3e261f728cc02c15158f` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260807T000000Z/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260807T000000Z/index/` | terminal failure in `c2000/r01/l1/w1`; only `PRODUCTION_READ_P95_FAILED` remained. No measurement CSV was opened; only terminal marker names and worker health were inspected. The coalescing target was active but did not clear post-drain read p95, so the next revision bounds global-PQ reranking to 16 candidates per segment for k=10. |
| v37-rerank-aws | `66ba247` | `856671706e58e4a5955bc6a1f3f7a352f99d3eb5e53e75b67a51771208c0f3a6` | `e05d6718eafd72723ef4b72990132abc5ec35b30d08f3e261f728cc02c15158f` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260807T010000Z/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260807T010000Z/index/` | terminal failure in `c2000/r01/l1/w1`; only `PRODUCTION_READ_P95_FAILED` remained. No measurement CSV was opened; only terminal marker names and worker health were inspected. The next revision adds a bounded in-process cache for immutable global-PQ code chunks so repeated post-drain queries do not re-read the same S3 ranges. |
| v38-global-code-cache-aws | `7ade6b0` | `d92481e1d3f1099a5a007bafed1dcf04c80abaf3f468640fda95030eaa434f7d` | `e05d6718eafd72723ef4b72990132abc5ec35b30d08f3e261f728cc02c15158f` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260807T020000Z/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260807T020000Z/index/` | terminal failure in `c2000/r01/l1/w1`; only `PRODUCTION_READ_P95_FAILED` remained. No measurement CSV was opened; only terminal marker names and worker health were inspected. The next revision enables a bounded 64 MiB decoded segment cache in the benchmark's disclosed production read profile. |
| v39-segment-cache-aws | `122e6d3` | `9e068669b2183be0eabaf1421b42c4987ca6feac2ff858b5948ae13753df5f4e` | `e05d6718eafd72723ef4b72990132abc5ec35b30d08f3e261f728cc02c15158f` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260807T030000Z/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260807T030000Z/index/` | terminal failure in `c2000/r01/l1/w1`; only `PRODUCTION_READ_P95_FAILED` remained. No measurement CSV was opened; only terminal marker names and worker health were inspected. The decoded segment cache alone did not move the global-PQ post-drain path; the next revision adds a bounded disk read-through cache for immutable S3 ranges. |
| v40-range-coalesce-aws | `257e9e4` | `07fc4ee89a014d3bd1df0a0235fdae1a3d920b3ecb1b2e3513d6157f7dbcd58e` | `e05d6718eafd72723ef4b72990132abc5ec35b30d08f3e261f728cc02c15158f` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260807T040000Z/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260807T040000Z/index/` | terminal failure in `c2000/r01/l1/w1`; only `PRODUCTION_READ_P95_FAILED` remained. No measurement CSV was opened; only terminal marker names and worker health were inspected. The next revision widens bounded sidecar physical ranges from 4 MiB to 16 MiB to reduce exact-rerank GET fan-out. |
| v41-range-coalesce-aws | `807881e` | `ed3c0ef8982c2710e5358fcb48c81d37f23e75d4298e7734764d7dcc10072c53` | `e05d6718eafd72723ef4b72990132abc5ec35b30d08f3e261f728cc02c15158f` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260807T050000Z/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260807T050000Z/index/` | terminal failure in `c2000/r01/l1/w1`; only `PRODUCTION_READ_P95_FAILED` remained. No measurement CSV was opened; only terminal marker names and worker health were inspected. The 16 MiB range cap did not clear post-drain read p95; no further paid retry was started. |
| v42-range-gap-aws | `807881e` | `ed3c0ef8982c2710e5358fcb48c81d37f23e75d4298e7734764d7dcc10072c53` | `e05d6718eafd72723ef4b72990132abc5ec35b30d08f3e261f728cc02c15158f` | not launched | not launched | prepared locally only: the next candidate widens the sidecar range coalescing gap to the same 16 MiB bound. No additional AWS run was started in this turn. |
| v43-bundle-range-aws | `f845a05` | `5321d6f917f060ae8a77225b8ad39924eefad36e616453b5dad4726cfc73a18c` | `e05d6718eafd72723ef4b72990132abc5ec35b30d08f3e261f728cc02c15158f` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260807T060000Z/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260807T060000Z/index/` | terminal failure in `c2000/r01/l1/w1`; only `PRODUCTION_READ_P95_FAILED` remained. No measurement CSV was opened; only terminal marker names and worker health were inspected. The next candidate widens the sidecar coalescing gap and physical cap to 64 MiB, bounded to one bundle-sized request. |
| v44-exact-fringe-aws | `9e37382` | `ca0154e83f308d89279603ffbf637a025524de47472e36f06c6fa0d3e38ee3b7` | `e05d6718eafd72723ef4b72990132abc5ec35b30d08f3e261f728cc02c15158f` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260807T070000Z/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260807T070000Z/index/` | terminal failure in `c2000/r01/l1/w1`; terminal markers identify `PRODUCTION_READ_P95_FAILED` after ingest, drain, point visibility, active-tail qualification, and read qualification completed. No measurement CSV was opened; only terminal marker names and worker health were inspected. The exact-fringe persistence change was present in the next pushed revision. |
| v45-direct-get-aws | `3c3501c` | `027a170da9dd9fc77d3812c2c8fdab636abc0fb66315aa9564d78013d13c44d0` | `e05d6718eafd72723ef4b72990132abc5ec35b30d08f3e261f728cc02c15158f` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260806T230500Z/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260806T230500Z/index/` | terminal failure in `c2000/r01/l1/w1`; `CELL_FAILED` and `GROUP_COMMIT_SCALABILITY_FAILED` were present after ingest, drain, point visibility, active-tail, read qualification, and aggregate performance markers. The fail-closed terminal validator rejected the cell because `CELL_COMPLETE` was absent and `CELL_FAILED` was present. No measurement CSV was opened. |
| v46-routing-parallel-aws | `c468566` | `92f60d0291bf03a1f3004e9b807ff82486bc1cd2cbbb772649fb16ff633628f1` | `e05d6718eafd72723ef4b72990132abc5ec35b30d08f3e261f728cc02c15158f` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260806T234000Z/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260806T234000Z/index/` | terminal failure in `c2000/r01/l1/w1`; `CELL_FAILED` and `GROUP_COMMIT_SCALABILITY_FAILED` were present after ingest, drain, point visibility, active-tail, read qualification, and aggregate performance markers. The fail-closed terminal validator rejected the cell because `CELL_COMPLETE` was absent and `CELL_FAILED` was present. No measurement CSV was opened. |
| v47-io-pool-aws | `d023f21` | `81236bcc10bd6bfb99a75d80259277fb98218319769a3abbe59093f3efa16253` | `e05d6718eafd72723ef4b72990132abc5ec35b30d08f3e261f728cc02c15158f` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260806T235200Z/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260806T235200Z/index/` | terminal failure in `c2000/r01/l1/w1`; phase and aggregate performance completion markers were present, but the fail-closed validator emitted `physical write amplification exceeded` and the runner preserved `CELL_VALIDATION_FAILED` plus `validation-error.txt`. No measurement CSV was opened. |
| v48-deferred-global-pq-aws | `0ceac78` | `116ca04dcff5ffd14c0cde16bf6c9ecc92438487011633db28235b889ad4d3c6` | `e05d6718eafd72723ef4b72990132abc5ec35b30d08f3e261f728cc02c15158f` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260807T004500Z/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260807T004500Z/index/` | terminal failure in `c2000/r01/l1/w1`; phase and aggregate performance completion markers were present, but the fail-closed validator again emitted `physical write amplification exceeded` and preserved `CELL_VALIDATION_FAILED` plus `validation-error.txt`. No measurement CSV was opened. |

| v49-routing-prefix-reuse-aws | `0e9e018` | `0d0033de83640241a9d8f8f515370012cb9b5e100c83cef007e39c4f9e3d57da` | `e05d6718eafd72723ef4b72990132abc5ec35b30d08f3e261f728cc02c15158f` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260807T120000Z/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260807T120000Z/index/` | terminal failure in `c2000/r01/l1/w1`; all phase and production-performance markers were present, but fail-closed validation again emitted `physical write amplification exceeded` and preserved `CELL_VALIDATION_FAILED` plus `validation-error.txt`. No measurement CSV was opened. The next revision keeps paged manifests metadata-only and passes resolved summaries only to page construction. |
| v50-paged-metadata-aws | `75d3a02` | `8e673d179976b6abbe718cfdfe89577cf546014372d065dc2b2ef3edebed4a38` | `e05d6718eafd72723ef4b72990132abc5ec35b30d08f3e261f728cc02c15158f` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260807T020500Z/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260807T020500Z/index/` | terminal failure in `c2000/r01/l1/w1`; ingest, drain, point visibility, active-tail, and read qualification markers were present; the physical-write validator did not fail, but production performance failed with `PRODUCTION_READ_P95_FAILED`. No measurement CSV was opened. This revision cleared the prior physical write-amplification failure; remaining target is read request fan-out/latency. |
| v51-routing-summary-cache-aws | `4b3921d` | `d2e4fca9726d9835ab8ea987b620a3b8dc4037d80054ea4349ec1a3f8afe7aec` | `e05d6718eafd72723ef4b72990132abc5ec35b30d08f3e261f728cc02c15158f` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260807T040500Z/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260807T040500Z/index/` | terminal failure in `c2000/r01/l1/w8`; ingest, drain, point visibility, active-tail, and read qualification markers were present; no physical-write validation failure was emitted, but production performance failed with `PRODUCTION_READ_P95_FAILED` (the active-tail-specific marker was absent). No measurement CSV was opened. The search-only routing-summary memoization slice did not close the post-drain read-p95 gate; segment/rerank cost remains causal. |
| v52-projected-read-aws | `82324db` | `02c01ff0b4b38bda7680efd89e0c431505d195cc57825797f7699ef4fd49e193` | `e05d6718eafd72723ef4b72990132abc5ec35b30d08f3e261f728cc02c15158f` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260807T060500Z/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260807T060500Z/index/` | terminal failure in `c2000/r01/l1/w8`; all phase markers and post-drain read qualification completed, and post-drain read p95 was 194.620 ms with inserted-ID recall 1.0, but `PRODUCTION_ACTIVE_TAIL_READ_P95_FAILED` remained at 203.957 ms. The projected-read branch reduced post-drain payload work enough to clear that gate; active-tail WAL scoring remains the next bottleneck. Terminal cell summary was inspected only after `GROUP_COMMIT_SCALABILITY_FAILED`; no incomplete CSV was opened. |
| v53-wal-query-norm-aws | `785386c` | `7c2ce34dcb64301b942751d908a4bb871658276eb4ba252a080564cbdef73a95` | `e05d6718eafd72723ef4b72990132abc5ec35b30d08f3e261f728cc02c15158f` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260807T021500Z/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260807T021500Z/index/` | terminal failure in `c2000/r01/l1/w8`; write p95 69.179 ms, active-tail read p95 182.903 ms, and inserted-ID recall 1.0 passed, but post-drain read p95 was 209.749 ms and `PRODUCTION_READ_P95_FAILED` remained. WAL cosine/angular query-norm reuse cleared the active-tail gate; post-drain variability remains on repeated immutable projected-segment reads. Terminal cell summary was inspected only after `GROUP_COMMIT_SCALABILITY_FAILED`; no incomplete CSV was opened. |
| v54-projected-cache-aws | `6cde7c0` | `beefaa37f1d051895155126b4d4104691c0a62b019ecdd99eedbc167d15b981c` | `e05d6718eafd72723ef4b72990132abc5ec35b30d08f3e261f728cc02c15158f` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260807T024500Z/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260807T024500Z/index/` | terminal failure in `c2000/r01/l1/w1`; write p95 54.431 ms, post-drain read p95 93.710 ms, and inserted-ID recall 1.0 passed, but `PRODUCTION_ACTIVE_TAIL_READ_P95_FAILED` remained at 213.199 ms. The projected metadata cache did not affect the active WAL-tail path; stored-vector norm reuse is the next targeted optimization. Terminal cell summary was inspected only after `GROUP_COMMIT_SCALABILITY_FAILED`; no incomplete CSV was opened. |
| v55-wal-snapshot-lock-aws | `033afb1` | `fae2ad8221d0716410b69751ac3aa3798060422b8110c323d3b7a24577c9dd97` | `e05d6718eafd72723ef4b72990132abc5ec35b30d08f3e261f728cc02c15158f` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260807T024300Z/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260807T024300Z/index/` | terminal failure in `c2000/r01/l1/w8`; write p95 73.829 ms and inserted-ID recall 1.0 passed, but active-tail read p95 was 166.352 ms and post-drain read p95 was 200.920 ms, emitting `PRODUCTION_READ_P95_FAILED`. The fail-closed validator rejected the terminal campaign because the root failure marker is present. Completed terminal artifacts were inspected only after terminalization; no incomplete CSV was opened. |
| v56-wal-snapshot-unlocked-aws | `5e196a5` | `5f578ee3cb72249d82f1215bdb122c05c717691d2e76288597af1f35c49da26b` | `e05d6718eafd72723ef4b72990132abc5ec35b30d08f3e261f728cc02c15158f` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260807T025527Z/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260807T025527Z/index/` | terminal failure in `c2000/r01/l1/w8`; write p95 63.758 ms and inserted-ID recall 1.0 passed, but active-tail read p95 was 207.876 ms and post-drain read p95 was 201.151 ms, emitting both `PRODUCTION_ACTIVE_TAIL_READ_P95_FAILED` and `PRODUCTION_READ_P95_FAILED`. The non-blocking WAL snapshot build change did not close the read gates; completed terminal artifacts were inspected only after terminalization, and no incomplete CSV was opened. |
| v57-sidecar-cache-aws | `2b82550` | `316a2ed9ea42e7b36b9fe5820157a477b393461a6da1d8d1214e95ddec8e7391` | `e05d6718eafd72723ef4b72990132abc5ec35b30d08f3e261f728cc02c15158f` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260807T030642Z/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260807T030642Z/index/` | terminal process failure at `c2000/r01/l1/w32` before a summary or process-exit artifact was produced; `w1` and `w8` completed, while only `CELL_FAILED` and resource telemetry were preserved for `w32`. No performance value is claimed and no incomplete measurement CSV was opened. |
| v58-single-flight-aws | `dbde0cc` | `8fe5b9201c566228671dbf683141df55c04b71872058402796f1fc6d0b8c3f48` | `e05d6718eafd72723ef4b72990132abc5ec35b30d08f3e261f728cc02c15158f` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260807T031439Z/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260807T031439Z/index/` | terminal process failure at `c2000/r01/l1/w32` before a summary or process-exit artifact was produced; `w1` and `w8` completed, while only `CELL_FAILED` and resource telemetry were preserved for `w32`. Worker disk was at 92% with approximately 45 GiB free; no performance value is claimed and no incomplete measurement CSV was opened. |
| v59-disk-recovered-aws | `1dab969` | `20af36c549057b2dd6859eb1aa2c4cead8d4bda625ffd0422584878c4d72ebe9` | `e05d6718eafd72723ef4b72990132abc5ec35b30d08f3e261f728cc02c15158f` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260807T032534Z/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260807T032534Z/index/` | terminal process failure at `c2000/r01/l1/w32` before a summary or process-exit artifact was produced; `w1` and `w8` completed, while only `CELL_FAILED` and resource telemetry were preserved for `w32`. Worker disk was healthy at approximately 196 GiB free; the runner’s missing storage-trace path prevented process-exit preservation. No performance value is claimed and no incomplete measurement CSV was opened. |
| v60-global-sidecar-cache-aws | `47deb3e` | `e521c4795fc698ee066194b2b8f7ed66860bdd87e7d1cf05fe565868cec2952f` | `e05d6718eafd72723ef4b72990132abc5ec35b30d08f3e261f728cc02c15158f` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260807T033448Z/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260807T033448Z/index/` | terminal failure at `c2000/r01/l1/w8`; write p95 67.241 ms, active-tail p95 164.338 ms, and inserted-ID recall 1.0 passed, but post-drain read p95 was 204.137 ms and `PRODUCTION_READ_P95_FAILED` remained. The terminal cell’s completed summary, reads, active-tail reads, and storage trace were inspected only after the root failure marker; no incomplete measurement CSV was opened. |
| v61-pane-exit-no-marker-aws | `738d52b` | `1c5fef23bc9fad0280d5f3b3ea7b1d49bf97184a94920f3bfccf141b499195be` | `e05d6718eafd72723ef4b72990132abc5ec35b30d08f3e261f728cc02c15158f` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260807T035226Z/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260807T035226Z/index/` | fail-closed infrastructure outcome: the benchmark tmux pane exited, but neither root nor cell terminal markers appeared. No performance value is claimed and no measurement CSV was opened. A fresh run was started from the same pushed revision after confirming the worker was idle. |

## Bounded local 768D causal checks after v28

These are single-cell, claim-ineligible diagnostics over 16,384 checksum-pinned
Cohere 768D vectors with 32 writers, 16 records/operation, pipeline depth four,
and eight worker lanes. Every numeric row below was inspected only after all
five phase markers existed and the process exited successfully.

- `8e2f00d` preserved recall 1.0, 62.722 ms write p95, and 47,530 acknowledged
  records/s. Parallel exact tail ranking did not resolve the whole-query path:
  active-tail p95 remained 353.006 ms. Deferring stable generation-page folding
  and foreground base promotion reduced drain only to 13.473 s, yielding 1,186
  drain-inclusive records/s; post-drain p95 was 273.571 ms.
- `6f03074` changed only lane-materialized segment-local codes to scalar bounds.
  Its terminal result was causally neutral: 13.329 s drain, 1,198
  drain-inclusive records/s, 346.245 ms active-tail p95, and 277.992 ms
  post-drain p95. The global delta build, not the segment-local quantizer, was
  therefore the dominant drain stage.
- `8c0f0a8` reused the stable base codebooks for delta bootstrap and terminated
  after drain because the delta artifact was absent and four exact-fringe
  segments exhausted the four-segment query budget. No incomplete CSV was
  inspected. `569535b` then made the previously discarded refresh error
  fail-closed and reproduced the exact cause: the one-row logical-cell base
  location layout could not encode 5,461-row delta segments.
- `faf0d18` retained the frozen codebooks but derived an independent packed
  delta location layout. The terminal run preserved recall 1.0, 60.182 ms write
  p95, and 46,132 acknowledged records/s while reducing drain from 13.329 s to
  5.685 s and raising drain-inclusive throughput from 1,198 to 2,713 records/s.
  Active-tail p95 (345.349 ms) and post-drain p95 (273.564 ms) still fail. This
  proves codebook reuse is material but insufficient; writer-side searchable
  extent codes, sparse generation fences, and a lower-request read merge remain
  required before AWS qualification.
- `cf5003f` replaced v28 with v29 and persisted whether each acknowledged ID
  was a first insert or a replacement. Fresh inserts now create no generation
  fence, while repeated-upsert tests preserve last-write-wins. The terminal
  cell remained effectively unchanged at recall 1.0, 59.849 ms write p95,
  47,998 acknowledged records/s, 5.766 s drain, 2,683 drain-inclusive
  records/s, 347.326 ms active-tail p95, and 271.876 ms post-drain p95. This
  rules out dense generation-frontier reads as the dominant latency source in
  this local shape; the fixed base/delta object-request merge and unindexed raw
  active tail remain causal targets.
- `0b7ceff` tested one bounded exact-vector envelope GET per selected Arrow
  bundle. The terminal 16,384-vector cell preserved recall 1.0 and 61.751 ms
  write p95, but post-drain read p95 only moved from `cf5003f`'s 271.876 ms to
  265.776 ms. Requests remained 113--114 per query, while mean post-drain bytes
  rose from 3.31 MiB/query to 5.67 MiB/query. The change is therefore dominated
  and was reverted. The invariant 54 HEADs per ordinary query trace instead to
  candidate-by-candidate generation visibility resolution; batch resolution is
  the next causal experiment.
- `611d652` added required stable tombstone-page blooms and rebuilt a fresh v26
  logical-cell base. Its terminal cell preserved recall 1.0, 64.508 ms write
  p95, 46,540 acknowledged records/s, 5.665 s drain, and 2,723 drain-inclusive
  records/s, but post-drain p95 remained 261.752 ms with 54 HEADs per ordinary
  query. A second terminal run with storage tracing attributed the invariant
  fan-out to a full paged-routing walk used only to rediscover that the current
  global base plus delta still covered the active segment set. The Bloom is
  still required for mutation-heavy scale, but it is causally neutral here.

The campaign is claim-ineligible until the root completion marker exists, no
failure marker exists, the service exits successfully, and the fail-closed
validator reconciles every matrix cell, raw sample, group receipt, request
total, resource exit, visibility result, exact-recall result, and correctness
gate.

## Bulk receipt-evidence harness correction

The local bulk smoke exposed a validator defect: a single append can touch
multiple ownership lanes, while the original `samples.csv` exposed only the
first lane identity and aggregate acknowledgement bytes. The validator could
therefore conflate distinct lane groups and reject otherwise coherent output.
The benchmark now records every lane receipt in a delimiter-safe field, and
the validator reconciles those per-lane records before reading any production
campaign measurements. The corrected local 64-cell, one-writer, 16-record
structural smoke completed all phase markers and passed fail-closed validation;
it remains claim-ineligible.

## WAL-tail refresh hardening after v33

The v33 AWS failure showed that active-tail probes were paying for a full
collection snapshot and immutable-manifest reload before checking whether a
lane head had advanced. The reader now exposes `refresh_wal_tail()`, which
polls only authoritative lane heads and decodes changed extents; full
`refresh()` remains the required path for published manifest or collection
changes. The production harness uses the explicit tail path for its
refresh-plus-search probes. The new regression and all 30 group-commit
integration tests passed, as did all 498 library tests (six ignored).

Background materialization now uses the same WAL-only refresh before building
its immutable delta. This removes a redundant collection/manifest reload from
the acknowledgement-critical maintenance pass while retaining the existing
manifest compare-and-swap publication fence.

## v62 global identity-range cache qualification (2026-08-07)

Run `20260807T040700Z` used source commit `738d52b` (archive SHA-256
`7a3dccf2c93e4f78c44e9de370b442cc3f8783c24a66dc7b25b176a773b9149f`) and the
frozen campaign manifest SHA-256
`e05d6718eafd72723ef4b72990132abc5ec35b30d08f3e261f728cc02c15158f`.
The root `GROUP_COMMIT_SCALABILITY_FAILED` marker is present. The c2000/r01/l1
w1 and w8 cells reached `CELL_COMPLETE`; c2000/r01/l1/w32 reached
`CELL_FAILED`, exited with status 1, and emitted `STORAGE_TRACE_MISSING`.
The repository validator passed for the completed w1 and w8 cells when invoked
against a reconstructed run root; the incomplete w32 cell was rejected
fail-closed. The cell prefixes intentionally contain no standalone
`manifest.json`; validation must use the run root. No incomplete measurement
CSV was read; only terminal markers, process exit, and the storage-trace
failure marker were inspected. The run is not publication-eligible. The
runner now preserves per-cell stdout/stderr logs so an early process failure
cannot be reduced to an unexplained missing trace.

## v63 diagnostic qualification launched (2026-08-07)

Run `20260807T043006Z` launched from source commit `7575eda` with source
archive SHA-256
`358d48b7727d552c89604120a7157cee27ae8837444d509fd5e7b78fc6298517`, frozen
manifest SHA-256
`e05d6718eafd72723ef4b72990132abc5ec35b30d08f3e261f728cc02c15158f`, and
fresh result/index prefixes under
`s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260807T043006Z/`.
The c7g.8xlarge worker was running and the launcher found no competing BORSUK
benchmark process. A detached health/terminal-marker watcher polls every 15
minutes. Until a root terminal marker appears, no measurement CSV is opened.

The run terminalized immediately at c2000/r01/l1/w1 with process exit 1. Its
preserved stderr reported `refusing to replace output` for the EC2-local result
directory: a stale-directory collision, not a library performance measurement.
The root failure is claim-ineligible and no measurement CSV was opened. The
launcher now refuses an existing remote result directory before creating a
tmux session. Group-commit dispatch also uses sparse lane buckets, avoiding an
empty allocation for every persisted lane on scalar appends; the focused
group-commit suite remains green.

## v64 sparse-dispatch qualification launched (2026-08-07)

Run `20260807T043548Z` launched from source commit `cb0ab50`, archive SHA-256
`b81faed0b67b65eb0f123ec23d9c2d57cd699e24e0de713527cd3ec78f2ddc81`, and the
same frozen manifest SHA-256
`e05d6718eafd72723ef4b72990132abc5ec35b30d08f3e261f728cc02c15158f`. It uses
fresh S3 result/index prefixes and the launcher verified the c7g.8xlarge worker
had no competing BORSUK workload. The watcher is polling terminal markers and
EC2 health every 15 minutes; no incomplete measurement CSV is being inspected.

Run `20260807T043548Z` terminalized immediately at c2000/r01/l1/w1 with
process exit 1. Its preserved stderr again reported `refusing to replace
output /home/ec2-user/borsuk-group-commit-results/20260807T043548Z/cells/...`;
the benchmark did not execute and produced no performance evidence. This is a
claim-ineligible harness collision, not a library result; no measurement CSV
was opened.

## v66 corrected-diagnostics qualification launched (2026-08-07)

Run `20260807T044600Z-r4` launched from source commit `3e7be41`, archive
SHA-256
`7e0f3bee30d197e34945a5f7e7ac3f9f15879eb6d140e0916247f17304302dfd`, and
manifest SHA-256
`e05d6718eafd72723ef4b72990132abc5ec35b30d08f3e261f728cc02c15158f`. The
worker preflight confirmed an absent EC2-local result directory and no
competing benchmark process. A detached watcher polls only terminal markers
and EC2 health every 15 minutes; no incomplete measurement CSV is being
inspected.

## v65 unique-prefix qualification launched (2026-08-07)

Run `20260807T044500Z-r3` launched from source commit `bf54837`, archive
SHA-256
`30fd30358e2389b7c03396d42374e402f50ee78c6f556b26aadaf9cb03237bbb`, and
manifest SHA-256
`e05d6718eafd72723ef4b72990132abc5ec35b30d08f3e261f728cc02c15158f`. A
read-only SSM preflight confirmed the unique EC2-local result directory was
absent and no competing benchmark process was running. The watcher polls only
terminal markers and instance health every 15 minutes; no incomplete
measurement CSV is being inspected.

Run `20260807T044500Z-r3` terminalized at c2000/r01/l1/w1 before executing the
benchmark. The newly added diagnostics initially created the cell directory
before process start, and the benchmark correctly rejected that pre-existing
output path. This was a runner bug, not library evidence. The runner now
captures stdout/stderr beside the cell and moves them into the cell only after
process exit. Scalar and 16-record bulk local structural smokes both pass the
fail-closed validator after the correction; no AWS measurement CSV was opened.

Run `20260807T044600Z-r4` terminalized with `GROUP_COMMIT_SCALABILITY_FAILED`.
The first two cells (c2000/r01/l1/w1 and w8) completed their terminal gates, but
c2000/r01/l1/w32 failed before executing the workload. Its preserved stderr
reported that 128 outstanding records could not express the preregistered
10,000 records/s at the 200 ms p95 gate; this is the mathematically invalid
one-record workload default, not a library performance result. No incomplete
measurement CSV was inspected. The campaign manifest now explicitly pins
`records_per_operation=16`, yielding 2,048 outstanding records for the 32
writer gate; its changed manifest requires a fresh unique-prefix qualification.

## v32 records-per-operation qualification launched (2026-08-07)

Run `20260807T045700Z-v32` launched from source commit
`4ec996ac9cfbac09eec45be483a590dab20b00c983fd087b0a9e4212f9583acc` with
manifest SHA-256
`2c2c7d219289f16779170f8b786a3de9de47a356b1b44c57672e56497af44bd5`.
Preflight found a unique result directory and no competing workload.

The root `GROUP_COMMIT_SCALABILITY_FAILED` marker is present. The first cell
(`c2000/r01/l1/w1`) completed, while `c2000/r01/l1/w8` emitted
`PRODUCTION_READ_P95_FAILED`, `PRODUCTION_PERFORMANCE_GATE_FAILED`, and
`CELL_FAILED` after its phase markers. The run is not publication-eligible;
the terminal diagnostic reports only a production performance-gate failure,
and no measurement CSV was opened. The remaining matrix was not treated as
evidence. The marker watcher was stopped after terminalization.

## cache-reuse qualification launched (2026-08-07)

Run `20260807T061500Z-cache-reuse` launched from the verified `abc83c0`
revision with source archive SHA-256
`6e1b9b1531b9111c1dbc7d24c1976d967e049bb6c984d9227e333b0f29f04625` and
manifest SHA-256
`2c2c7d219289f16779170f8b786a3de9de47a356b1b44c57672e56497af44bd5`.
Launcher preflight confirmed the c7g.8xlarge worker was running, idle, and had
unique source/result paths. The detached watcher polls only terminal markers
and EC2 health every 15 minutes; no measurement CSV is being inspected while
the campaign is incomplete.

Run `20260807T061500Z-cache-reuse` terminalized with
`GROUP_COMMIT_SCALABILITY_FAILED`. The `c2000/r01/l1/w1` cell completed, while
`c2000/r01/l1/w8` emitted `PRODUCTION_READ_P95_FAILED`,
`PRODUCTION_PERFORMANCE_GATE_FAILED`, and `CELL_FAILED` with process exit 1.
The terminal diagnostic reports a production performance-gate failure; the
decoded-segment cache reuse change did not clear the eight-writer read gate.
The run is claim-ineligible and no measurement CSV was inspected.

## sidecar-cache qualification launched (2026-08-07)

Run `20260807T053500Z-sidecar-cache` launched from verified commit `25c436a`
with source archive SHA-256
`d5d3b79f56e0aa183e08f94f2d5972a13f22de4bab75835261329151874b3529` and
manifest SHA-256
`2c2c7d219289f16779170f8b786a3de9de47a356b1b44c57672e56497af44bd5`.
Launcher preflight confirmed the dedicated c7g.8xlarge worker was running and
idle, with unique source, result, and index paths. A persistent watcher polls
only terminal markers and EC2 health every 15 minutes; no measurement CSV is
being inspected while the campaign is incomplete.

Run `20260807T053500Z-sidecar-cache` terminalized with
`GROUP_COMMIT_SCALABILITY_FAILED` at `c2000/r01/l1/w1`. The cell emitted
`PRODUCTION_READ_P95_FAILED`, `PRODUCTION_PERFORMANCE_GATE_FAILED`, and
`CELL_FAILED` with process exit 1. The terminal diagnostic reports a
production performance-gate failure; no measurement CSV was inspected and the
run is claim-ineligible.

## routing-cache qualification launched (2026-08-07)

Run `20260807T083000Z-routing-cache` launched from commit `caa21f4` with source
archive SHA-256
`7e928af51f9ef204de4039f5955c43f3c2a3b6682d333ef720ec7b55c5f9b32b` and
manifest SHA-256
`2c2c7d219289f16779170f8b786a3de9de47a356b1b44c57672e56497af44bd5`.
Launcher preflight confirmed the dedicated c7g.8xlarge worker was healthy,
idle, and had unique source/result/index paths. During execution, monitoring
used only phase/terminal markers, process liveness, and instance health.

The run terminalized with `GROUP_COMMIT_SCALABILITY_FAILED` at
`c2000/r01/l1/w1`. The cell emitted `READ_QUALIFICATION_COMPLETE`, then
`PRODUCTION_READ_P95_FAILED`, `PRODUCTION_PERFORMANCE_GATE_FAILED`, and
`CELL_FAILED`; the terminal stderr contains only `production performance gate
failed`. The process-shared routing-page cache did not clear the one-writer
read gate. The run is claim-ineligible; no measurement CSV was inspected.

## parallel immutable qualification (2026-08-07)

Run `20260807T160000Z-parallel-immutable` used commit `d1f31e4`, source archive
SHA-256 `2d9ec11b19e47174d2f5382429a824bd4d2d46113554e79e328dab7f0612d99d`,
and frozen manifest SHA-256
`2c2c7d219289f16779170f8b786a3de9de47a356b1b44c57672e56497af44bd5`.
The root `GROUP_COMMIT_SCALABILITY_FAILED` marker is present. Measurement CSVs
were opened only after that terminal marker.

The `c2000/r01/l1/w1` cell completed every phase and production gate: write
p95 was 61.070 ms, acknowledgement throughput was 1,570.363 records/s,
inserted-ID recall@10 was 1.0, active-tail read p95 was 173.119 ms, and
post-drain read p95 was 198.564 ms. The throughput threshold is preregistered
only for 32 writers.

The `c2000/r01/l1/w8` cell preserved 1.0 inserted-ID recall, 10,516.000
acknowledged records/s, 71.485 ms write p95, and 137.010 ms active-tail read
p95, but failed the post-drain read gate at 6,666.159 ms p95. Each post-drain
query searched 24 immutable segments and accounted for 479,421,516 bytes;
the 20 probes accounted for 9,588,430,320 bytes. The cell exited 1 with
`PRODUCTION_PERFORMANCE_GATE_FAILED`, and the remaining matrix did not run.

Source tracing identified two independent causes rather than a cache miss:
lane-log drain published materialized segments without invoking the existing
bounded global-PQ delta refresh, leaving the complete 128,000-row append as an
exact per-segment fringe; and the projected sidecar planner compared candidate
row count with batch count instead of testing actual batch coverage, turning a
sparse rerank into a full-sidecar decode. The next qualification must measure
the drain-to-delta hook and corrected sparse range decision from one frozen
revision; neither improvement is claimed from this failed run.

## global-delta plus ranged-sidecar qualification launched (2026-08-07)

Run `20260807T081436Z-global-delta-ranged` launched from commit `30d5110` with
source archive SHA-256
`0bb9f73617e2bf5d8fdf26386e7572a8a1682169a319061aad645a805a389ac1` and the
same frozen manifest SHA-256
`2c2c7d219289f16779170f8b786a3de9de47a356b1b44c57672e56497af44bd5`.
Launcher preflight verified the dedicated `c7g.8xlarge` worker was running,
the source/result/index paths were unique, and no competing BORSUK workload
was active. Until a root terminal marker appears, monitoring is limited to
markers and infrastructure/process health; measurement CSVs remain unopened.

The root later terminalized with `CAMPAIGN_FAILED` after the eight-writer cell;
the worker process exited and no benchmark workload remained.  The repository
root validator then failed closed with `campaign is incomplete`, as required
for a stopped matrix.  Terminal cell artifacts show the one-writer cell passed
with recall@10 1.0, 1,498.282 acknowledged records/s, 67.003 ms write p95,
147.720 ms active-tail read p95, and 72.099 ms post-drain read p95.  The
eight-writer cell preserved recall@10 1.0, 10,483.263 acknowledged records/s,
73.742 ms write p95, and 127.649 ms active-tail read p95, but failed only the
post-drain read gate at 375.449 ms p95.

The prior 479.4 MB/query exact-fringe failure fell to 1.02 MB/query and the
6,666.159 ms p95 fell to 375.449 ms, validating the global-delta and sparse
range fixes without qualifying the matrix.  The terminal eight-writer samples
average 17.7 GETs and 4.4 searched segments per query.  Source tracing confirms
that bounded range GETs are already parallel; the remaining cold path executes
the stable base's code/rerank phases and the independent immutable delta's
code/rerank phases serially.  The next causal factor must overlap those two ANN
layers while preserving exact merged ranking, recall, and shared explicit
budgets.  Cache warming is not an acceptable substitute for this core-path
change.

The next local causal slice now starts the independent immutable-delta ANN
search on the bounded process-wide I/O pool while the stable-base ANN search is
in flight.  It applies only when the caller has no explicit byte or latency
budget; budgeted searches retain serial remaining-budget accounting.  A real
delayed object-store regression captures base object paths before delta
publication and delta paths afterward, preserves the exact delta hit, and
requires simultaneous GETs across those two path sets.  It failed on the
serial implementation and passes with overlap.  This is local structural
evidence only; no AWS latency result is assigned yet.

Before requalification, the full library gate's five baseline blockers were
resolved without suppressions.  Routing tests now assert the two selected
parent/leaf reads and retain corrupt-sibling sentinels; the range test exercises
the current 4 MiB cap with merge-eligible 64 KiB gaps; and lane-drain coverage
requires a current searchable delta.  Optional ANN maintenance failure after
durable segment publication is reported through the post-commit maintenance
warning rather than converting a completed drain into a retry-unsafe error.
The resulting full library gate passes 510 tests with 6 explicitly ignored.

## parallel base/delta qualification launched (2026-08-07)

Run `20260807T090031Z-parallel-base-delta` launched from commit `c51f8d5` with
source archive SHA-256
`19b54a72691d36e8ce9806d87fd20b9b5c30783adc326920920948e70a948e32` and
the unchanged frozen manifest SHA-256
`2c2c7d219289f16779170f8b786a3de9de47a356b1b44c57672e56497af44bd5`.
Preflight verified Causality account `453182569524`, the dedicated
`c7g.8xlarge` worker was online with 158 GiB free, and no competing BORSUK
benchmark process was active.  Until a root terminal marker appears, observe
only markers, process/tmux state, instance health, and resource telemetry; do
not inspect measurement CSVs.

The run terminalized at the eight-writer cell with root
`GROUP_COMMIT_SCALABILITY_FAILED`; the pane exited 1 and no benchmark process
remained.  The root validator failed closed with `campaign is incomplete`, as
required for the stopped matrix.  Terminal one-writer artifacts passed with
recall@10 1.0, 1,490.580 acknowledged records/s, 68.013 ms write p95,
164.003 ms active-tail read p95, and 64.044 ms post-drain read p95.  The
eight-writer artifacts preserved recall@10 1.0, 9,885.707 acknowledged
records/s, 77.373 ms write p95, and 119.741 ms active-tail read p95, but failed
only post-drain latency at 363.535 ms p95.  The overlap factor is therefore a
small improvement from 375.449 ms, not a closed gate.

The terminal trace accounts for 355 post-drain GETs and 21,112,548 bytes over
20 queries.  Individual global exact-rerank calls issued as many as 19 physical
GETs for only 49-107 KiB, while the generic range reader admitted ten requests
per wave.  The next causal factor keeps the 64 KiB merge gap and 4 MiB physical
span cap unchanged, but gives global exact reranks a bounded 32-request wave so
the production shortlist does not serialize internally.  No result is assigned
until a fresh terminal arm measures that revision.

## 32-request global rerank wave qualification launched (2026-08-07)

Run `20260807T092504Z-rerank-wave32` launched from commit `a0402e6` with source
archive SHA-256
`e209494085b5ea03ded6b107d38d31c147d8186ce51ab7c3ed0942aafb14e80b` and
the unchanged frozen manifest SHA-256
`2c2c7d219289f16779170f8b786a3de9de47a356b1b44c57672e56497af44bd5`.
The launcher again passed its dedicated-worker and no-contention preflight.
Until terminalization, inspect markers and infrastructure/process health only.

The run terminalized at eight writers with root failure and an exited pane.
The root validator correctly rejected the incomplete matrix.  One writer
passed with recall@10 1.0, 1,518.347 records/s, 71.798 ms write p95,
118.284 ms active-tail p95, and 59.767 ms post-drain p95. Eight writers kept
recall@10 1.0, reached 10,320.644 acknowledged records/s, 77.767 ms write p95,
and 151.455 ms active-tail p95, but post-drain p95 was 245.432 ms. The
32-request wave materially improved p50/p95 from 181.328/363.535 ms to
135.663/245.432 ms, but did not pass.

The remaining trace still contains 19-GET exact-rerank calls for only 49 KiB.
The next factor prices one remote request against up to a 1 MiB parent-local
gap while retaining the 4 MiB physical-range cap and 32-request wave. This is a
request-count/latency trade, not caching or reduced recall, and needs a fresh
terminal arm before any claim.

## 1 MiB global rerank coalescing qualification (2026-08-07)

Run `20260807T094639Z-rerank-coalesce1m` used commit `2ebd3f4`, source archive
SHA-256
`66533fc46681a3e46959b94e9d4a010dee825bf4feff408faf17b990e9253533`, and
the unchanged frozen manifest SHA-256
`2c2c7d219289f16779170f8b786a3de9de47a356b1b44c57672e56497af44bd5`.
Preflight found no competing workload. Monitoring used one observed 15-minute
sleep and inspected only markers and host/process health until the root failure
marker appeared and the benchmark pane had exited.

The repository validator then failed closed with `campaign is incomplete`, as
required for a matrix stopped at the eight-writer gate. Terminal one-writer
artifacts passed with recall@10 1.0, 1,594.968 acknowledged records/s,
62.448 ms write p95, 121.419 ms active-tail p95, and 64.419 ms post-drain p95.
Eight writers retained recall@10 1.0, 10,249.414 acknowledged records/s,
78.701 ms write p95, and 114.108 ms active-tail p95, but post-drain p50/p95
were 138.972/346.277 ms. Drain-inclusive throughput was 2,759.083 records/s.

The one-megabyte merge gap is rejected. It reduced the 20 post-drain queries
from 355 to 189 GETs relative to the 32-request-wave arm, but increased bytes
from 21,109,924 to 84,919,460 and worsened p95 from 245.432 to 346.277 ms.
The packed bundle's unselected gaps cost more than the avoided same-region S3
round trips for this scattered shortlist. The source therefore restores the
64 KiB gap while retaining the independently useful 32-request wave. A real
range regression requires a 512 KiB unselected gap to remain two four-byte
physical reads.

## Global base/delta phase telemetry qualification launched (2026-08-07)

Run `20260807T103224Z-phase-telemetry` launched from commit `0da625d` with
source archive SHA-256
`56624bc5f4c88a1f0ff5f954c5a5221b1d0003e83366fe0b1be65240c9d1049c`
and the unchanged frozen manifest SHA-256
`2c2c7d219289f16779170f8b786a3de9de47a356b1b44c57672e56497af44bd5`.
Preflight found no competing benchmark process on the dedicated
`c7g.8xlarge` worker; instance checks, memory, disk, and load were healthy.
The only source change relative to the rejected coalescing arm restores the
64 KiB rerank merge gap and adds stable-base approximate/rerank,
immutable-delta approximate/rerank, and post-base delta-wait telemetry. It
does not change candidate count, cache policy, recall requirements, or the
frozen workload. Until the root terminal marker appears and the benchmark
process exits, inspect markers and infrastructure/process health only; do not
open measurement CSVs.

The run terminalized at `c2000/r01/l1/w8` with root
`GROUP_COMMIT_SCALABILITY_FAILED`, an exited pane, and no benchmark process.
The repository validator failed closed with `campaign is incomplete`. The
one-writer cell passed at recall@10 1.0, 1,523.113 acknowledged records/s,
73.377 ms write p95, 175.292 ms active-tail p95, and 65.634 ms post-drain
p95. Eight writers preserved recall@10 1.0, 10,361.668 acknowledged records/s,
71.290 ms write p95, and 115.823 ms active-tail p95, but post-drain p50/p95
were 157.994/268.989 ms. Drain-inclusive throughput remained only
2,914.856 records/s.

The new terminal phase evidence identifies the immutable delta as the read
critical path. At eight writers the stable base's approximate and exact-rerank
p95 intervals were 30.386 and 31.985 ms. The delta's corresponding p95
intervals were 195.575 and 108.566 ms, and the completed base then waited as
long as 216.891 ms at p95 for the overlapped delta. The 20 queries issued 353
GETs and fetched 19,902,948 physical bytes while retaining exact inserted-ID
recall. Wider base/delta overlap is therefore rejected as the next factor:
delta code scanning/locality and exact-row fetch work must shrink without
reducing the candidate or recall contract.

The same terminal storage trace and source audit expose an independent drain
defect: the eight-writer cell read 740,039,296 exact-vector bytes in 216
requests and 218,803,438 normal-segment bytes in 271 requests over the full
cell, while `materialize_lane_log_tail` wrote decoded records and then invoked
the segment-reading delta refresh. The next drain slice must encode the delta
from those owned records and atomically publish segment plus delta coverage in
one manifest. It needs causal AWS measurement before any throughput claim.

## Fused lane-drain delta publication qualification launched (2026-08-07)

Run `20260807T110628Z-fused-drain` launched from commit `1f92d6b` with source
archive SHA-256
`324a8538dbb3def16fe2c6d3da24fb8c0d19d77afb478cc87c55de55a913e04c`
and unchanged manifest SHA-256
`2c2c7d219289f16779170f8b786a3de9de47a356b1b44c57672e56497af44bd5`.
The matched source removes only the drain's redundant segment reread and
second manifest publication: an exact operation-log regression requires zero
GETs of newly written segments and one atomic segment-plus-delta manifest PUT.
The complete 512-test library gate, 32 group-commit tests, 12 fault-injection
tests, strict all-target/all-feature Clippy, and the structurally validated
bulk runner smoke passed before launch. The dedicated-worker no-contention
preflight passed. Until root terminalization and process exit, inspect only
markers and infrastructure/process health; do not open measurement CSVs.

The run terminalized at `c2000/r01/l1/w8` with root
`GROUP_COMMIT_SCALABILITY_FAILED`, an exited pane, and no benchmark process;
the worker remained healthy and idle. The fail-closed validator rejected the
partial matrix as `campaign is incomplete`. Terminal artifact inspection shows
that the fused publication removed half of the redundant materialization I/O
in the eight-writer cell: exact-vector reads fell from 216 requests / 740.0 MB
to 108 / 370.0 MB, normal-segment reads from 271 / 218.8 MB to 121 / 109.4 MB,
and catalog writes from six / 23.9 MB to three / 12.0 MB. Drain time improved
from 31.560 s to 29.996 s and drain-inclusive throughput from 2,914.856 to
3,029.758 records/s. This is a causal improvement, but it is only 3.9% and is
not production parity.

Recall remained 1.0 and acknowledged throughput was 10,447.260 records/s at
71.669 ms p95, but post-drain read p50/p95 were 128.448/291.102 ms. The delta
remained the critical path: approximate, exact-rerank, and base-wait p95 were
216.807, 135.400, and 259.356 ms while the stable base phases were 34.596 and
33.476 ms. The terminal descriptor contained 128,000 delta vectors but only
103 of 256 occupied cells; the hottest cell held 16,053 rows and 49.3 MB of
lossless vectors. The implementation reused scan and coarse codebooks trained
on the 2,000-vector stable base, so a distributionally broader tail inherited
bad physical routing. The next isolated factor is bounded tail-specific scan
and coarse training at delta bootstrap; cache changes and reduced recall or
candidate gates are explicitly excluded.

## Tail-trained delta routing qualification launched (2026-08-07)

Run `20260807T121141Z-tail-routing` launched from commit `a64814e` with source
archive SHA-256
`0ad840cc3c6a98583f9b9f81ce578c7a7c74510ddac72b3e58866ca9ce7cd4ff`
and unchanged manifest SHA-256
`2c2c7d219289f16779170f8b786a3de9de47a356b1b44c57672e56497af44bd5`.
The isolated factor trains a new immutable delta's scan and coarse quantizers
from its own deterministic, dimension-aware reservoir bounded to 64 MiB,
using records already owned by the drain when available. Later appends reuse
that delta codebook; stable-base promotion is unchanged. The same slice also
corrects the fixed-budget parallel segment reader so it cannot schedule more
physical segment payloads than `max_segments`.

The 513-test library gate, 32 group-commit integration tests, the exact
segment-budget regression, strict all-target/all-feature workspace Clippy,
validator tests, repository policy, formatting, and a structurally validated
bulk runner smoke passed. A broader all-target integration invocation exposed
seven pre-existing routing/cache-accounting assertions unrelated to this diff;
they remain explicit production-hardening work and are not counted as passing
evidence. The dedicated AWS worker passed the launcher's idle/no-contention
preflight. Until root terminalization and process exit, inspect only markers,
process state, and infrastructure health; do not open measurement CSVs.

The run terminalized at `c2000/r01/l1/w8` with root failure, an exited process,
and a healthy idle host. The fail-closed validator correctly rejected the
partial matrix. Tail training fixed the intended physical skew: the 128,000-row
delta occupied all 256 cells instead of 103; median rows/cell were 482.5, p95
1,106 instead of 7,101, and the maximum fell from 16,053 to 1,871. Post-drain
GETs fell from 353 to 308 and bytes from 20.51 MB to 10.71 MB. Delta approximate
p95 improved from 216.807 to 141.812 ms, delta exact-rerank p95 from 135.400 to
102.266 ms, and overall read p95 from 291.102 to 232.256 ms while recall stayed
1.0. The factor is therefore directionally valid but still fails the 200 ms
production gate.

The cost is also material and disqualifies promotion as-is: eight-writer drain
time rose from 29.996 to 40.453 s and drain-inclusive throughput fell from
3,029.758 to 2,394.973 records/s. Acknowledged throughput remained 9,852.023
records/s at 80.619 ms p95. The balanced delta selects 28 small code regions;
the code path permits a 32-read wave but incorrectly caps actual concurrency by
the general segment `prefetch_depth` of eight, producing two object-store RTT
waves before ADC. The next isolated read factor may decouple compact global-code
range concurrency from full-segment prefetch depth, but must keep the existing
32 MiB code-wave memory bound. Separately, training CPU must be reduced or
amortized before the balanced router can be a production default.

## Independent global-code read width qualification launched (2026-08-07)

Run `20260807T123450Z-code-read-width` launched from commit `a47b01c` with
source archive SHA-256
`383d7bbd4e4db2d10361a49758dd4789db43e6bfee05e95e6aefb5b9b64ab8e1`
and unchanged manifest SHA-256
`2c2c7d219289f16779170f8b786a3de9de47a356b1b44c57672e56497af44bd5`.
This single-factor arm decouples compact global-PQ code-range read concurrency
from the full-segment prefetch-depth setting. It permits one bounded wave for
the 28 balanced delta code regions while retaining the existing maximum of 32
concurrent reads and 32 MiB of code payload; exact reranking, candidate count,
routing, cache policy, and recall contract are unchanged.

The 513-test library gate, strict all-target/all-feature workspace Clippy,
formatting, repository policy, and diff checks passed before launch. The
dedicated worker passed the launcher's account, instance, pinned-dataset, and
idle/no-contention checks. Until root terminalization and process exit, inspect
only markers, process state, and infrastructure health; do not open measurement
CSVs.

The run terminalized at `c2000/r01/l1/w8` with root failure, an exited pane,
no benchmark process, and healthy instance/system checks. The fail-closed
validator rejected the partial matrix as `campaign is incomplete` before the
terminal cell CSVs were inspected. Widening the code-read wave did not qualify:
eight-writer post-drain read p95 was 234.213 ms versus 232.256 ms in the
predecessor, with exact inserted-ID recall still 1.0. Delta approximate p95
improved modestly from 141.812 to 123.081 ms, but delta exact-rerank p95 was
111.562 ms and the completed base waited 171.997 ms p95 for the delta. The
cell issued the same 308 post-drain GETs and fetched the same 10,707,428 bytes,
so the extra concurrency is rejected as insufficient rather than promoted.

Write behavior was likewise unchanged within run noise: acknowledged
throughput was 10,056.115 records/s at 79.176 ms p95, drain took 39.560 s, and
drain-inclusive throughput was 2,447.957 records/s. Maximum process RSS was
3,680.9 MiB and maximum sampled CPU was 2,016.0%; the host remained healthy.
Source inspection after this result found that a coalesced code-range GET can
already contain identity and exact-vector bytes between selected cell code
slices, but the query copies only the codes and later fetches those covered
rerank ranges again. The next causal read factor should reuse only bytes already
transferred in the same query, with a strict transient-memory bound; it must not
introduce a persistent cache or relax recall/candidate gates.
