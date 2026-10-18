# Runtime-thread ABBA: retain a measured Pareto point

Workload: Cohere1M D1024 cosine k10, full1000 fixed queries, fixed40, Q2, Offload2, downloads64/query, same ELF and identical independently spawned task dispatch. CPU0-3/400%, external8GiB/no swap. Source efef1d988359076da2f552a90e41765f7665e217. Order C2/M2/M2/C2.

| Cell | Runtime | Recall@10 | Cohort QPS | p90 ms | p95 ms | p99 ms | Cohort CPU s | Max RSS MiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| A1 | current_thread | 97.37% | 8.44325 | 327.711 | 344.064 | 383.734 | 102.511 | 133.85 |
| B1 | multi_thread2 | 97.37% | 9.30601 | 320.119 | 338.862 | 373.180 | 112.629 | 168.18 |
| B2 | multi_thread2 | 97.37% | 9.17646 | 318.170 | 339.563 | 381.821 | 112.440 | 170.05 |
| A2 | current_thread | 97.37% | 8.17600 | 333.149 | 350.769 | 404.314 | 102.184 | 130.32 |

All four native/strict-reader/checkpoint stages passed; canonical per-ordinal ordered IDs, score bits, physical rows, plans, trace, charges and hits match exactly for all1000. Each cell charges127811 logical GETs and34229466368 verified bytes. These are not wire/billed totals.

Temporal pairs: QPS +10.218/+12.237%; p95 -1.512/-3.195%; cohort CPU +9.870/+10.037%; GET and logical-byte ratios1.0. Both candidate cells exceed both control cells in QPS and have lower p95, descriptively. Retain runtime2 as an opt-in throughput/latency versus CPU/RAM point. Q1/current_thread remains a separate lower-latency setting; no default promotion or significance claim.

ENA inbound allowance-exceeded counters increased by2860864/3961938/3570130/3192060 in A1/B1/B2/A2; other selected allowance counters remainzero. AWS defines this counter as packets queued or dropped when aggregate inbound instance bandwidth exceeds its allowance: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/monitoring-network-performance-ena.html . These are host-wide counters, not per-flow evidence or an isolated cause attribution. The result motivates a network-headroom falsifier; it does not reject the valid gain.

Setup included a real-input admission panel and a disposable full-cohort candidate smoke before measurement. All cells are fresh processes, application source cache OFF; backend/cache state UNKNOWN. Do not call this universally cold. Dispatch-to-completion latency includes initial runtime queue. Overlapping outstanding task lifetimes do not prove physical I/O or compute parallelism. Two repetitions per arm are not2000 independent queries and establish no service capacity, lifecycle$ or competitor win. Earlier joined/cooperative Q2 is historical, not this control.

Failed gate attempta0001 used an inherited90s smoke timeout; that incomplete execution remains INVALID. Correcteda0002 used the prospectively declared450s native full-cohort bound. Same algorithm/config/source/ELF and scientific limits; no timed-out data pooled.

Immutable evidence will bind results archive, strict report seals, config/header/source identities and all original attempt/controller receipts. No follow-up data run is authorized by this decision alone; root reconciles one contextless next-plan result.
