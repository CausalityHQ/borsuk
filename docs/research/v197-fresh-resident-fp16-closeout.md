# V197 fresh resident FP16 1M source gate closeout

## Decision and authority

**Advance the frozen optional-risk + dynamic-floor + resident FP16
method to a distinct real-query dataset and paired live S3 gate.** The
source-query screen passes its preregistered quality and planned
transport limits, and the Rust serving primitive matches all fresh
panel FP16 results. This does not freeze production defaults or establish
end-to-end latency, throughput, cost or 100M behavior.

The Causality Spot `c7i.12xlarge` cell `a0002` used source commit
`e3f6ee9b35923397f3ca037be4640bd831b363ea` and instance
`i-0e3d5da7030ae3bd4`. The source archive SHA-256 is
`46941744c455486f28b83de6c7ceb0e59062b386da0ed9dfbd4ac67934e1cfc7`.
The terminal is complete, SHA-256
`a54ca2acd78bdb2c6b5b689a1719d18efb28e102163661919fabb98582f3fef6`,
at `s3://borsuk-bench-453182569524-euc1/research/v197-fresh-resident-fp16-source/e3f6ee9b35923397f3ca037be4640bd831b363ea/runs/a0002/terminal.json`.
The launcher streamed back and hashed every terminal artifact, checked
the four GT-blind S3 seals were in place before the raw result, and
terminated the instance. The checked-in
`scripts/check_v197_fresh_resident_fp16_source.py` independently replayed
the 512 IDs, disjointness from V194, mandatory floors, physical interval
charges, returned-ID/GT100 intersections, aggregates, Rust parity and
decision. It passed on the closed artifacts. Attempt `a0001` under
commit `1c3a92da` reserved source but EC2 rejected oversized user data;
no instance or measurement started. The bootstrap was compressed before
the `a0002` source archive was frozen.

## Fresh source-query evidence

Dataset/split: **ReLAION-1M D768, source pseudoquery SHA ranks
3457–3968 (512 queries)**, identity-disjoint from the V194–V196 ranks
2945–3456. The V115 router and quantizer, V164 order, V189 fit,
V192 prices, V194 radius 32 and optional-risk model were frozen.
Every query applied the same `max(672, mandatory-floor)` unit cap.
There were zero infeasible plans. Exact GT100 was opened only after
features and plans were sealed.

| Same-panel arm | Returned GT100 / 51,200 | p05 / 100 | Below 98 / 512 | Planned SQ8 bytes | Planned SQ8 GETs |
| --- | ---: | ---: | ---: | ---: | ---: |
| Optional-risk + FP16 | **51,028** | **98** | 17 | **3,735,912,960** | **5,110** |
| Optional-risk + SQ8 | 50,802 | 98 | 22 | 3,735,912,960 | 5,110 |
| Full-rank + FP16 | 51,034 | 98 | 21 | 4,905,114,240 | 8,763 |

The optional FP16 arm clears the frozen ≥50,979 returned-hit,
p05≥98, zero-infeasible, ≤5,700,611,604-byte and ≤11,328-GET gates.
Its minimum query was 90/100. The candidate set contained 51,121
GT100 positions; the optional physical ranges contained 51,028 and
FP16 returned all of those. The remaining 172 positions decompose to
79 candidate omissions and 93 physical-plan omissions on this panel.
Compared on the **same queries and same ranges**, FP16 recovered 226
returned GT100 positions over SQ8. Full-rank recovered six more than
optional, at 1,169,201,280 extra planned bytes and 3,653 extra GETs.
No per-query switch to full-rank is licensed by this result.

The V195/V196 **used, unpaired** ReLAION-1M source panel returned
51,022/51,200 GT100 with p05 98 and planned 3,883,676,160 SQ8 bytes/
5,179 GETs. The V155 **used, unpaired** ReLAION-1M validation-1000
real-query baseline returned 99,567/100,000 GT100 with planned
11,134,007,040 bytes/22,126 GETs. Neither is a paired V197 comparison;
the table above is the paired evidence. No external-product matched
comparison has been measured for this architecture.

## Serving primitive and limits

The Rust resident tier authenticated the same 1,544,000,064-byte V196
plane and exactly matched all 5,120 fresh-panel Python FP16 top-100 ID
lists over 10 timed repetitions. Its worst repetition p95 local rerank
was **0.270111 ms**, p99 **0.278788 ms**; peak serving-process RSS was
**1,550,270,464 bytes**, with **2.181093192 s** cold full-plane
authentication. The local p95≤2 ms, p99≤5 ms and RSS≤2 GiB guards
passed. Offline preparation took 2:18.13 wall/9,370,744 KiB peak RSS;
planning 1:19.01 wall/212,320 KiB; evaluation 0:59.32 wall/
10,979,276 KiB. Those Python peaks are build/evaluation costs, not
serving RSS. Local rerank timing excludes S3 fetch, routing, SQ8 first
wave, concurrency, compilation and source download.

The resident file has linear modeled payload, 154,400,000,064 bytes
per 100M D768 generation and 308,800,000,128 bytes for two before
router, deltas and workspace. `formal/ResidentFp16Resources.lean`
proves only that arithmetic and a conditional RAM-admission implication;
100M resources and latency remain unmeasured. Higher RAM can be chosen
by recall tier and corpus size without a fixed vector-count knee.

## Next qualification

Freeze this method and test a **distinct real-query dataset** with
authenticated source/GT, paired against the strongest current BORSUK
baseline on identical vectors, queries, metric, quality target and
resource envelope. Measure live S3 p50/p95/p99, throughput, full
charged RAM, end-to-end planned versus observed bytes/GETs, and cost
under disclosed equivalent conditions. Only a passing real-query and
live-S3 result licenses a 10M scaling gate, then a 100M profile with
memory sized from the target recall and generation count. Mutation,
compaction and failure-recovery integration remain product gates.
