# Verified paged cold offered-load decision

Attempt a0002: all 53 artifacts, source archive, frozen binary/source authority, inputs, schedules, ground truth and resource/termination evidence independently authenticated. Both datasets are FIRST1M D768 cosine k10, consumed development ordinals 0–63: ReLAION fresh rank16 development0–63; CoHere fresh development0–63. Six worker slots; namespace/process cold on each admission, application SQ8 cache absent, S3 service cache uncontrolled, Frankfurt eu-central-1a, loopback HTTP. These are development results, not disjoint publication panels.

| Dataset | Offered QPS | Success/64 | Drops | Success R10 % | Cold p90 ms | Cold p95 ms | Successful full-span QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| ReLAION | 8 | 32 | 32 | 99.6875 | 1530.102 | 1634.393 | 3.468585 |
| CoHere | 8 | 29 | 35 | 95.8621 | 1953.944 | 2036.538 | 3.170019 |
| ReLAION | 0.25 | 64 | 0 | 99.3750 | 1487.110 | 2105.740 | 0.252654 |
| CoHere | 0.25 | 64 | 0 | 96.8750 | 1444.918 | 1778.862 | 0.252621 |
| ReLAION | 0.5 | 64 | 0 | 99.3750 | 1378.653 | 1415.677 | 0.502780 |
| CoHere | 0.5 | 64 | 0 | 96.8750 | 1398.991 | 1435.251 | 0.502735 |
| ReLAION | 1 | 64 | 0 | 99.3750 | 1452.244 | 1511.781 | 0.985325 |
| CoHere | 1 | 64 | 0 | 96.8750 | 1441.691 | 1594.477 | 0.990513 |
| ReLAION | 2 | 64 | 0 | 99.3750 | 1505.265 | 1695.063 | 1.952615 |
| CoHere | 2 | 64 | 0 | 96.8750 | 1401.952 | 1470.716 | 1.953546 |
| ReLAION | 4 | 59 | 5 | 99.3220 | 1517.317 | 1522.612 | 3.415152 |
| CoHere | 4 | 61 | 3 | 96.7213 | 1454.902 | 1538.817 | 3.567634 |

All numbers in the table are verified measurements. Latency and recall are conditioned on successful offers; drops remain in all-offer denominators and cannot pass the gate. Errors and aborts are zero. Full-span throughput includes cleanup. At 0.25–2 offered QPS, both datasets retain 99.375%/96.875% recall@10 and exact frozen resident reference results: 0 percentage-point quality delta. The reference is a quality control, not a matched cold latency/throughput control.

**Decision: fixed 8-QPS FAIL; 444-ms published-context FAIL at every rate.** Largest tested rate passing all-offer quality is 2 QPS on both datasets. Four and eight QPS drop offers; neither is attained. Retain the valid quality candidate. No matched vendor win or latency speedup is established. Serial a0004 p90 1404.494/1397.579 ms and 0.743629/0.755502 completion QPS remain verified historical measurements under a different protocol.

Campaign cgroup peak 1,120,149,504 bytes; largest native RSS 173,998,080 bytes. No swap/OOM; source/SQ8 successful-call failures zero and charged caps verified. Resource population is the closed campaign. Compute estimate: 1110 controller seconds × observed Spot quote $0.204200/hour = $0.06296167. This is an estimate, not a bill, and excludes EBS/IP/S3/storage, publication/setup and failed attempts. Total cost remains unmeasured.

Next decisive test: one causal startup decode intervention with unchanged persisted graph, scorer, result order and memory admission, then fresh paired control/candidate cold measurement on the same worker/toolchain. Prior serial marginal staging/decode medians (~449/~352 ms ReLAION, ~438/~352 ms CoHere) identify a startup gap; they are not additive tail decomposition. Read-only code inspection found repeated graph preflight scans and per-small-adjacency duplicate-check allocations; their causal CPU contribution is not yet measured. Preserve identity/duplicate/edge/level/budget validation. Do not introduce another router architecture based on this load failure.

Both-vendor matched latency/QPS/total-cost, fresh publication panels, warm/R100 and 10M/100M lifecycle/scalability qualification remain open.
