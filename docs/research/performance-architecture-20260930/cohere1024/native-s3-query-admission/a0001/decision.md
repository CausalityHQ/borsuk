# Real S3 admission and remaining product gates

Frozen completed run: 100,000 Cohere Wikipedia vectors, D1024, cosine k10, 1,000 queries. Source and query bytes remain pinned in the adjacent receipts. This workload is an evaluation fixture; the library must remain generic.

| Requirement | Current evidence | Decision / next gate |
|---|---|---|
| Actual S3 query correctness | All 1,000 queries sealed before truth; recall 9,723/10,000; IDs, score bits and logical charges match completed local reference exactly | Admission passed; preserve original output |
| Descriptive runtime | Query wall total 185.679262298 s; one serial query stream; cgroup peak 47,333,376 B; zero swap/OOM | Mean 185.679 ms; concurrent throughput and formal performance panel unproven |
| Object traffic | 67,943 logical submitted GETs; 27,259,427,868 authenticated payload bytes | Separate from cumulative process transport including credentials; billed/wire totals unknown |
| Tail latency and causal stages | Sealed per-query native timings exist | Qualify the existing Rust reducer extension and reduce this completed input; do not rerun queries to obtain these statistics |
| Matched vendors | No completed matched S3 Vectors or Turbopuffer runs | No measured vendor win; exact corpus/query/metric/k/region/concurrency/cache and lifecycle-cost comparison still required |
| Incremental mutation | `two_bit_mutations::apply_two_bit_mutations` persists bounded logical-ID upserts/deletes over authenticated recovered state | Existing API is evidence of implementation, not real-S3 lifecycle qualification |
| Callable compaction | `two_bit_compaction::compact_two_bit_index` uses durable local staging and publication | Concrete gap at inspected source: semantic compaction rejects D>768, while Native100k query admission supports D<=1024; repair and qualify an actual D1024 mutation/compaction/reopen fixture |
| Reader-safe GC/recovery | `two_bit_gc::collect_two_bit_garbage` has bounded scans/deletes and durable fencing for cooperating same-host/same-directory participants | Real S3 recovery, active-reader retention, failure and lifecycle-cost gates remain; cross-host safety is not established |
| Scale | Native100k profile explicitly bounds rows <=100,000; Fresh1m maintenance explicitly unsupported | 10M generic scale and maintenance qualification remain; no extrapolated production claim |

Source inspected: 841167a0af043c1bc57ca6d89446c8eb03c93507. The next causal runtime change is selected only after stage reduction. No new arm, corpus preparation, vendor upload or paid query run is authorized by this table. Environment/config failures remain INVALID; they do not reject the algorithm.
