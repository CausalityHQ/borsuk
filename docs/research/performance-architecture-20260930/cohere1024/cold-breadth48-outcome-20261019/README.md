# Cold breadth 48: closed fixed-68 outcome

**Decision: VALID_TRADEOFF_OR_NEGATIVE_ON_FIXED68. Retain breadth 64 as the quality incumbent.** Breadth 48 lowers work and aggregate wall time but loses seven hits per repetition and fails the preregistered tail gates. This is a closed diagnostic-panel result, not a new full-quality measurement or a production/vendor/generalization claim.

Base: `056d3fa3ddc4d26a30d4d7979e3a751452a1d1fc`, whose parent is `9902a281e4c3e33d30d249244ff764e828dcb8de`. Only this outcome directory is added. [Evidence manifest](evidence.json) pins the inputs; [stage summary](stage-summary.json) derives wall intervals from the closed rows; [next falsifier](next-falsifier.md) is a proposal requiring root review.

## Matched result

Source: `evidence/cold-breadth48-pareto-a0002/report.json`, SHA-256 `405448fcff3a08724b25f13af7d89c6a8657f9b957d93b1dd8c93193222f14be`. Order A1/B1/B2/A2; A=64, B=48; 68 fixed original ordinals per cell, nearest-rank percentiles. All valid cells are retained.

| Measure | B/A or A versus B | Frozen gate | Result |
|---|---:|---:|---|
| Paired p95, repetition 1 | 0.9877792663481367 | <=0.90 | Fail |
| Paired p95, repetition 2 | 1.007355057012229 | <=0.90 | Fail |
| Pooled query wall | 0.9056116651706831 | <=0.90 | Fail |
| Pooled p90 | 1.0140390427829589 | <=1.00 | Fail |
| Query CPU | 0.805494063770874 | <=1.00 | Pass |
| External process wall | 0.9085238585472507 | <=0.95 | Pass |
| External children CPU | 0.8093117271218667 | <=1.05 | Pass |
| Logical GETs | 0.9290974481931837 | <=1.00 | Pass |
| Verified bytes | 0.7713136651645733 | <=1.00 | Pass |
| Panel hits, each repetition | A 674/680; B 667/680 | Own-arm exact reference | Exact; cross-arm quality loss |

A1/A2 p95 are 437.274893/434.983168 ms; B1/B2 are 431.931073/438.182494 ms. Query wall improves about 9.44%, query CPU 19.45%, verified bytes 22.87%, and logical GETs 7.09%. These savings do not satisfy the complete screen. Panel recall is 99.1176% versus 98.0882%, a 1.0294 percentage-point loss. Historical full references are A 9902/10000 and B 9852/10000 (99.02% and 98.52%); those are historical evidence, not measurements repeated here.

Each A panel cell has 11,678 logical GETs and 3,481,321,088 verified bytes; each B cell has 10,850 and 2,685,190,528. Independent proof counts matching query physical GET attempts, zero retry GET attempts, and zero transport/stream failures in all eight cells. Transport counters are not a billing/wire-byte measurement; the raw summary's `physical_s3_measured=false` must not be reinterpreted as a measured bill.

## Validity, authority, and closure

The experiment ran on Spot instance `i-031447f122c124b4e`. The sole causal parameter change was `semantic_leaf_limit: 64 -> 48`. Both arms use the same qualified source tree `ab73789e2b2ee642f893123d8d2e96a90c26e77c`, runner `67055f6d7d1f4911f7a024768d6c87d2ee93fa59e629173e097c4566ce6c9d0b`, and reader `fa16dccadc44f974a019f8613122fc5aa88f6f2ac2d5b469d4200fe9f922073e`. Cache bytes=0, Q1, Offload2, fetch parallelism=128, FP32 shortlist=16, no coalescing. Source/runtime binaries were unchanged. Root preflight reports eleven source files and both ELF pins verified, and a clean qualified source. Prior qualified gate results are reused; unclassified historical suite failures remain in the original qualification. This document does not claim a newly clean whole suite.

All eight native exits and eight reader exits are 0. Root verification SHA-256 `0d77d2d0ecc2f8f5843dfb68998a089038a9c013f3ceefc97bd228cd8b883d9b` records exact own-arm reference agreement across all 292 observations: 272 panel queries, 16 admission queries, and four canary queries. The comparison covers returned IDs, score bits, physical ordinals, plans, traces, shortlists, and charges. Own-arm equality does not mean A equals B. Root proof SSM receipt prefix `277bf54c` terminated 0 and records checksum-HEAD verification of eight cell archives, the evidence archive, and terminal marker. This worker rehashed the closed report, protocol, proof, raw four panel files, and root closure; it did not repeat root's cloud verification or rerun native measurements.

Terminal epoch `1792410915.4150102` is **2026-10-19 11:55:15.415010 UTC**; `controller.finished` is **11:55:16 UTC**. Both precede Spot termination **11:55:28 UTC** (termination time supplied by the authenticated root/opening brief). The second reclamation occurred after every cell and terminal closure; it does not turn this completed attempt into an interrupted experiment. The first host, `i-015199f89c1cf89ea`, was reclaimed before native work. Root used the stated on-demand-host exception after repeated capacity losses for verification/documentation on `i-0bb6acc848d4d11ee`; no experiment was rerun on that host.

Durable evidence is in bucket `borsuk-bench-453182569524-euc1`, prefix `research/semantic-router/20261019/cold-breadth48-pareto-a0002/`:

- `evidence.tar.gz`: 7,924,487 bytes, SHA-256 `c92fb4cf6014ff173235e22eae0be77c143376833e24e96006ae83b50fc42532`.
- `root-verification-closure.tar.gz`: 47,974 bytes, SHA-256 `042af85b388a09b76eb760b9d19531378655199af987b7f478e2388cfe64c3f5`; root publication records conditional PUT and checksum HEAD, SSM receipt prefix `a39a25d1`.

## Limits and handoff

The fixed 68 were diagnostic selections, not an independent population sample. Two repetitions do not establish population p95/p99, quality parity, generalization, vendor superiority, or readiness. A fresh process with application SOURCE cache disabled does not prove provider-cache eviction. Initialization is not isolated. Stage intervals measure wall time and contain no stage CPU attribution. The next hypothesis is untested.

This documentation was prepared only on the designated EC2 host under CPU 1, memory 512 MiB, swap 0, tasks 128, and 120 seconds per static command. Missing inherited Git blobs were preserved by sparse-before-checkout and audited `write-tree --missing-ok`/`commit-tree`; no inherited source was reconstructed or removed. The final handoff records the exact head, bundle/doc hashes, complete inherited-tree comparison, and static checks.

**HOLD for root review.** Root owns integration, push, future experiment authorization, and shutdown at 12:21:41 UTC (fixed by root; no extension). No Cargo/native run, new data, cloud resource, child, or consultation was launched.
