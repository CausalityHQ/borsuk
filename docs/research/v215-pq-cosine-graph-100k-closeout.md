# V215 cosine PQ graph 100k development closeout

**Decision: pass the frozen development gate and freeze
ef_search=2048 with a 2048-row FP16 shortlist.** Test that exact arm
on method-held-out ReLAION-100k queries 256–999 before any 1M
promotion. These queries appeared in older campaigns, so they are
held out from this method's arm selection, not globally pristine.

The sole Causality c7i.4xlarge Spot attempt `a0001` ran on
`i-031eadd3afdb752da` and is terminated. Source commit
`d861520e8559d28dd10def977c9e941c550d7099`, archive SHA-256
`c244a7fd60bec49f963d83824e4509e148cf997f930fb3ab4ebbfa82405885ad`.
Terminal SHA-256
`bd0297af0c3d24fd336dbf6e3e6adb44e52636a6c59e6c654e1f0acf20fca97b`
is at `s3://borsuk-bench-453182569524-euc1/research/v215-pq-cosine-graph-100k/d861520e8559d28dd10def977c9e941c550d7099/runs/a0001/terminal.json`.
It reports complete, exit 0; all seven artifact byte counts and
SHA-256 hashes passed readback. Raw returned IDs were sealed before
truth and paired baseline downloads. All graph/plane/PQ/map inputs
were reused from the independently verified V214 terminal.

Dataset/split: **ReLAION-100k D768, already-used development queries
0–255**. The paired V193 full-rank SQ8 baseline returns
**25,440/25,600 GT100 hits**. V215 navigates the same source-built
cosine graph as V214, scoring each PQ code by the cosine of its
reconstructed source vector, then reranks by generation-bound FP16
cosine. No codebook, graph degree, or query threshold was fitted to
truth.

| ef / FP16 shortlist | GT100 hits /25,600 | p05 hits/query | p50 / p95 / p99 (ms) | p95 base visits | sequential QPS | Gate |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| 1024 / 1024 | 25,442 | 97 | 3.340 / 4.193 / 4.290 | 14,472 | 295.01 | p05 fail |
| 2048 / 1024 | 25,454 | 97 | 4.233 / 5.341 / 5.666 | 22,532 | 234.65 | p05 fail |
| **2048 / 2048** | **25,473** | **98** | **6.047 / 7.130 / 7.437** | **22,532** | **165.48** | **pass, selected** |
| 4096 / 1024 | 25,459 | 98 | 5.955 / 7.521 / 7.807 | 34,840 | 170.17 | pass |
| 4096 / 2048 | 25,477 | 98 | 7.720 / 9.303 / 9.605 | 34,840 | 130.78 | pass |

The frozen gate required hits≥25,440, p05≥98, whole in-process
p95≤10 ms, peak serving RSS≤320,000,000 bytes, and zero vector-body
GETs. The selected arm passes every condition. Versus the unchanged
V214 squared-L2 PQ navigation at 2048/2048, cosine PQ gains **43
GT100 hits**, improves p05 from 97 to 98, and changes p95 from
7.224 to 7.130 ms. This supports the metric-mismatch hypothesis on
this panel; it does not prove the same effect on other data.

The fresh serving process peaked at **202,469,376 bytes RSS**,
including **30,889,640 bytes** loaded graph heap,
**154,400,000 bytes** FP16 plane, **6,400,000 bytes** PQ codes and
**400,000 bytes** reconstructed-norm sidecar. Cold hydration was
**0.330 s**; zero vector-body GETs. The five-arm 1,280-search loop
took **6.987 s**. Times are full in-process Rust requests but exclude
network front end, concurrent admission, mutation, generation swap
and S3 transport, so they are not external-product latency.

The Spot instance launched 2026-09-25 09:34:01 UTC; terminal landed
09:38:18 UTC, 257 s later. EC2 Spot history for eu-central-1c gave
**$0.3644/hour** effective at launch; launch-to-terminal compute-only
cost is **estimated $0.0260**, excluding termination tail, EBS, S3
and taxes. This is not a billing measurement.

The next gate freezes the selected arm on query ordinals 256–999,
compares paired V193 aggregate hits, requires p05≥98 and the same
p95/RSS/zero-GET limits. Only then should a full 1M end-to-end
serving comparison and matched Turbopuffer/S3 Vectors campaign be
considered.
