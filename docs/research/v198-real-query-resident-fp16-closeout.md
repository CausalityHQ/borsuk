# V198 used real-query resident FP16 gate closeout

## Decision and authority

**Advance this frozen optional-risk + dynamic-floor + resident FP16 method to
paired live S3 transport.** The preregistered offline real-query quality,
planned SQ8 traffic and local Rust rerank gates passed. This does not yet
qualify end-to-end latency, throughput, observed S3 traffic, cost, a distinct
real-query dataset, 10M/100M scale, or the production planner and lifecycle.

The one Causality Spot `c7i.12xlarge` cell used source commit
`fdc51a358be350678d9f6f279a2be95a57709c02`, archive SHA-256
`19afd543e0685a425c7c7b3591c0f1d71059f224c0a0317c86a57c257eaa09af`,
and instance `i-03e1b116ca4d427b7`. It closed with terminal SHA-256
`b3ae47adc77e155b12ddd23e2a061f90ca06415d828586bc4df36e51dd833994`
at `s3://borsuk-bench-453182569524-euc1/research/v198-real-query-resident-fp16/fdc51a358be350678d9f6f279a2be95a57709c02/runs/a0001/terminal.json`.
The launcher streamed back and hashed all 15 terminal artifacts, checked all
four GT-blind S3 seals predated the raw result, and terminated the instance.
`scripts/check_v198_real_query_resident_fp16.py` independently replayed the
closed artifact hashes, query pairing, mandatory floors, interval witnesses,
returned-ID/GT100 intersections, aggregates, Rust parity and decision. It
passed. The source truth parquet and V155 baseline evidence hashes were also
rechecked; the independent checker uses the per-query sealed result's GT100
lists for its arithmetic and does not parse the parquet itself.

## Measured used-split result

Dataset/split: **ReLAION-1M D768, validation-1000 real queries**. This split
was already used by V116/V155 and is a paired falsification screen, not a
fresh generalization result. The V116 real-query route produced the same
512-nominee sets before GT opening. V198 used the frozen V115 router and
quantizer, V164 physical SQ8 order, V189 optional fit, V192 prices, V194
radius 32, V197 `max(672, mandatory-floor)` cap and V196 FP16 plane. There
were zero infeasible plans.

| Arm on the same 1,000 queries | Returned GT100 / 100,000 | p05 / 100 | Below 98 | Planned SQ8 bytes | Planned SQ8 GETs |
| --- | ---: | ---: | ---: | ---: | ---: |
| V198 optional-risk + resident FP16 | **99,605** | **98** | **31** | **7,388,559,360** | **10,047** |
| V198 same ranges + SQ8 top100 | 99,261 | 98 | 41 | 7,388,559,360 | 10,047 |
| V155 cached sparse + exact source | 99,567 | 98 | 42 | 11,134,007,040 | 22,126 |

V198 FP16 gained 38 hits against V155, with 91 per-query wins, 820 ties and
89 losses. Against its *same-range* SQ8 arm, FP16 gained 344 hits. Planned
bytes were 33.64% lower and planned GETs 54.59% lower than V155. The V155
exact-source baseline uses a different historical storage/local-read profile;
its bytes and GETs are the closed planned SQ8 transport charge, not a claim
of equal total resources. The V198 minimum was 88/100; V155's was 87/100.
The 100,000 GT100 positions split into 127 absent from the candidate set,
173 candidate positions omitted by physical ranges, and 95 fetched positions
absent from the returned top100. This is an aggregate decomposition, not a
promise about any unseen query. The frozen quality/traffic pass limits were
99,567 hits, p05 98, zero infeasible, 11,134,007,040 bytes and 22,126 GETs.

The Rust tier authenticated the 1,544,000,064-byte resident plane and matched
all 10,000 Python FP16 top100 ID lists across 10 repetitions. Its worst
repetition local p95 was **0.270989 ms**, p99 **0.278222 ms**; peak serving
process RSS was **1,553,944,576 bytes** and cold full-plane authentication
**2.077118498 s**. These measurements exclude routing, planning, S3 fetch,
SQ8 first-wave scoring, concurrency and end-to-end request latency. The
offline Python planner took **1:14.55 wall for 1,000 queries** (about 74.55
ms/query averaged), peak 250,972 KiB; it is a production latency blocker.
Offline preparation took 2:36.66 wall/9,361,920 KiB peak and evaluation
0:14.33 wall/9,341,020 KiB peak, neither a serving measurement.

## Next qualification

Measure a paired live S3 cell for the same frozen requests and plan with
observed conditional range GETs/bytes, page authentication, errors/retries,
end-to-end and phase p50/p95/p99, concurrency throughput, and charged memory.
The planner must be implemented as a low-latency Rust refinement with exact
interval/decision parity against the frozen reference before it can be counted
in production latency. Then run an authenticated **different real-query
embedding dataset** and matched external-product comparison before freezing
defaults or promoting to 10M. Size 100M RAM from the chosen recall tier and
generation count rather than imposing a fixed vector-count knee. Generation
hydration, mutation, compaction and recovery remain production gates.
