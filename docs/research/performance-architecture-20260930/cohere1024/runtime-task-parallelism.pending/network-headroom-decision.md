# Network-headroom host-class screen
## Result
Valid descriptive Pareto point; no default promotion or competitor-win claim.

Same qualified Rust source efef1d988359076da2f552a90e41765f7665e217, same ELF and frozen Cohere 1M D1024 cosine k10/full1000 inputs, multi_thread2/Q2, Offload2, fetch64/query, fixed40; 4 CPU, 8GiB, swap0 on both hosts.

| Host | Cell | QPS | p90 ms | p95 ms | p99 ms |
|---|---|---:|---:|---:|---:|
| m6in.4xlarge | B1 | 9.493 | 256.287 | 349.540 | 507.169 |
| m6in.4xlarge | B2 | 9.905 | 241.238 | 319.992 | 489.142 |
| m6i.4xlarge | A1 | 8.078 | 343.556 | 383.472 | 533.908 |
| m6i.4xlarge | A2 | 7.984 | 338.054 | 374.692 | 524.849 |

All four cells exactly match canonical ordered returns/scorebits/physical rows/plans/traces/charges/hits: recall 9737/10000 (97.37%), 127811 logical GETs and 34229466368 verified payload bytes each. These are not wire or billed counts.

Ordinal descriptive comparisons: QPS +17.52%/+24.05%, p90 -25.40%/-28.64%, p95 -8.85%/-14.60%; query CPU -2.88%/-2.74%. SOURCE intervals lower by 24.76%/31.17%; discovery/planning/SQ8 largely similar. Stage intervals are not additive to cohort time.

ENA inbound-allowance deltas B1/B2=0/0; A1/A2=3363097/3429580. Other selected allowance counters zero. Counters are host-wide packets queued/dropped due allowance, not packet-loss or elapsed-time fractions or proof of isolated NIC causality.

## Protocol and limits
Preregistered chronological B1,B2 then A1,A2; NOT ABBA. One fresh Spot instance per class in eu-central-1b; physical host, CPU/Nitro/IRQ/EBS/neighbors, time block, unknown credits/backend cache are confounds. Both hosts passed actual-input admission and a separate full1000 smoke (excluded), fresh native process/application cache OFF/backend UNKNOWN. First query included. Same runtime and conservative reserve143261696 bytes.

Observed whole-instance Spot quotes m6i4 $0.4331/h, m6in4 $0.5459/h. Compute cost/completed query ratio 1.0726/1.0160 (candidate +7.26%/+1.60%); no prorated four-core or total lifecycle claim. Retain the faster point despite this tradeoff. No significance, isolated network mechanism, service capacity, universal cold, production readiness or vendor parity claim.

## Decision and next step
Retain m6in4 as a faster research execution option. Current source still transfers ~34.23GB verified payload per1000; reducing SOURCE demand while preserving the demonstrated quality is the next algorithmic lever. Use one bounded contextless complete experiment packet based on measured SOURCE-stage evidence; reuse qualified runner, canonical inputs and truth split. Do not rebuild unchanged code or expand to a broad campaign. Separate any recall/latency/byte tradeoff and retain valid Pareto gains.

Evidence: immutable per-cell S3 checkpoints under research/semantic-router/20261018/network-headroom-{A,B}-a0001/cells; full closure network-headroom-closure-a0001. Persistent EBS retained; terminal compute cleanup is recorded separately.
