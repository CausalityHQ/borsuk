# Logical-cell routing bounded-diagnostic attempt ledger

These attempts diagnose remote write-path progress only. They are deliberately
smaller than the frozen paired matrix and are never eligible for routing
performance or product claims. Incomplete attempts are monitored through their
diagnostic terminal markers, non-measurement progress log, process health, and
resource telemetry; partial measurement CSV files are not inspected.

| Attempt | Source SHA-256 | Manifest SHA-256 | Result prefix | Index prefix | Status |
|---|---|---|---|---|---|
| v1 | `b6c4ab1f57afd9872e840e3922247a0f9ef8b3406194c1bb272155f30fcb2976` | `c41b1f16bb0871fc6c2ca943220f701520f9b689834574079582f6b3f2452d47` | `s3://borsuk-bench-453182569524-euc1/research/logical-cell-routing-diagnostic/20260802-v1/results/` | `s3://borsuk-bench-453182569524-euc1/research/logical-cell-routing-diagnostic/20260802-v1/indexes/flat` | complete and structurally valid from clean revision `ff07610` on isolated `c7g.8xlarge`; all stage counters balanced, the process exited 0, the success marker is present, the failure marker is absent, and all 40 unique samples match the immutable identities. Claim-ineligible diagnostic output records 21.553s measured wall time, 3.17 CPU-seconds, p50 2.718s, p95 6.549s, 1.856 appends/s, and 13,362 storage requests |

The v1 result is diagnostic evidence of remote request amplification, not a
routing comparison: 40 successful single-record appends issued 13,362 storage
requests, about 334 per append, while measured wall time exceeded process CPU
time by 6.8x. The resource envelope independently recorded 28.49s elapsed at
19% CPU and 241,961 voluntary context switches. Before another paired matrix,
reduce or amortize remote write-path request amplification and repeat this
bounded diagnostic from fresh immutable prefixes.
