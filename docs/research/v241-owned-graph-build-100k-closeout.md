# V241 owned F32 graph build, ReLAION-100k closeout

**Decision: pass the frozen memory and exactness gate.** The production
builder now consumes and normalizes its F32 source rows in place. This
removes one full D768 F32 copy during construction. The graph algorithm,
authenticated graph bytes and returned IDs are unchanged. This result
does not address serial HNSW insertion time or establish a vendor win.

The sole complete `causality` c7i.4xlarge Spot cell was `a0002` on
`i-00d11dd438c21e097` in `eu-central-1c`; it is **terminated**. Source
commit `0e705d5aed7998f58526447d00fd5b19fe01ae54`, archive SHA-256
`30b94f352a7cbdafdd557021088efeb7ddc4a236e1e47079da28b6c9df7edbd4`,
terminal SHA-256
`44af28b81d876e864431880a00a7f9263d0a1316a7ecd5f46a00716677252fa5`.
The terminal exited zero; the original launcher replayed the size and
SHA-256 of all ten artifacts and confirmed termination. The sealed raw
SHA-256 is
`13abdde658e2023d73fd2643ee063e637b5e727f6965d345fe8039ae84f0f14e`.
Evidence is under
`s3://borsuk-bench-453182569524-euc1/research/v241-owned-graph-build-100k/0e705d5aed7998f58526447d00fd5b19fe01ae54/runs/a0002/`.
The first attempt `a0001` on `i-04d705de40a781fdc` failed before
measurement: the new owned API moved a four-row test fixture that its
test later reused. Its closed build log showed Rust E0382. The test
caller was fixed at the `a0002` source revision. `a0001` is terminated
and contributes no build or quality result.

The source, plane, PQ books/codes and 1,000 queries are V218's
authenticated ReLAION-100k D768 physical-order inputs. Queries 0–255
are prior-used development; 256–999 are prior-used method held out.
Cosine k=100, M=32, M0=64, ef_construction=128 and PQ/FP16
ef/shortlist=2048/2048 were frozen. The paired V218 graph artifact was
`d8b70919243a7cd6ecb9448ce23f776374738476c1882cbc6a651fb34753af2f`;
V241 produced **exactly the same SHA-256 and 26,571,646 B**. The base
layer is 100,000/100,000 reachable with minimum in-degree four, zero
rows below four in-edges and maximum degree 99. The narrow release
Rust graph regression passed 1/1.

| Verified build | Peak process RSS, B | Rust build time, s | Graph SHA-256 |
| --- | ---: | ---: | --- |
| V218 borrowed F32 + normalized copy | 842,149,888 | 177.702 | `d8b70919…af2f` |
| V241 owned in-place F32 | **530,915,328** | **177.361** | `d8b70919…af2f` |

V241 saved **311,234,560 B (37.0%)** of peak build RSS. The 0.341 s
time difference is descriptive across separate Spot runs, not a speed
claim. V241 met the preregistered 600,000,000 B and 210 s ceilings.
Its `/usr/bin/time` wall build was 2m57.69s with no swaps.

Both V218 and V241 terminals and raw artifacts were independently
authenticated against their SHA-256 receipts. Replaying all 1,000
queries showed **1,000/1,000 identical ordered PQ result lists** and
**1,000/1,000 identical exact-FP16 diagnostic lists**. V241's direct
GT100 replay gave 25,537/25,600 development hits and 74,234/74,400
held-out hits, 99,771/100,000 combined and p05=99, identical to V218.
Its eight-worker loaded in-process p50/p90/p95/p99 was
5.745/6.669/6.941/7.333 ms, 1,371.9 QPS, peak serving RSS
211,558,400 B and zero vector-body GETs. These serving numbers exclude
network admission and are not a new product latency comparison.

`a0002` ran from 20:58:49 to terminal upload 21:09:17 UTC (628 s);
`a0001` ran 303 s to its failed terminal. The eu-central-1c Spot quote
was $0.3676/hour, implying about **$0.0641** compute for the valid cell
and **$0.0309** for the failed compile cell, **$0.0951** together.
These are estimates excluding EBS, S3, termination tail and billing
rounding.

**Next gate:** retain the owned builder and test one material way to
remove serial insertion time at 100k. Do not infer a 100M build time
from the unchanged 100k runtime. A parallel insertion design must
preserve authenticated generation, deterministic output, full
reachability and paired returned quality before any 1M promotion.
Build RAM still includes one F32 source plane alongside the serving
FP16 plane and graph; direct FP16 construction is a separate quality
decision, not established by this result. Set memory budgets from
explicit rows, dimensions and recall requirements, with no fixed row
count cutoff.
