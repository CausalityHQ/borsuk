# Logical-cell routing bounded-diagnostic attempt ledger

These attempts diagnose remote write-path progress only. They are deliberately
smaller than the frozen paired matrix and are never eligible for routing
performance or product claims. Incomplete attempts are monitored through their
diagnostic terminal markers, non-measurement progress log, process health, and
resource telemetry; partial measurement CSV files are not inspected.

| Attempt | Source SHA-256 | Manifest SHA-256 | Result prefix | Index prefix | Status |
|---|---|---|---|---|---|
| v1 | `b6c4ab1f57afd9872e840e3922247a0f9ef8b3406194c1bb272155f30fcb2976` | `c41b1f16bb0871fc6c2ca943220f701520f9b689834574079582f6b3f2452d47` | `s3://borsuk-bench-453182569524-euc1/research/logical-cell-routing-diagnostic/20260802-v1/results/` | `s3://borsuk-bench-453182569524-euc1/research/logical-cell-routing-diagnostic/20260802-v1/indexes/flat` | complete and structurally valid from clean revision `ff07610` on isolated `c7g.8xlarge`; all stage counters balanced, the process exited 0, the success marker is present, the failure marker is absent, and all 40 unique samples match the immutable identities. Claim-ineligible diagnostic output records 21.553s measured wall time, 3.17 CPU-seconds, p50 2.718s, p95 6.549s, 1.856 appends/s, and 13,362 storage requests |
| v2 | `b25e0c1667bd7740d0195fdecfc732b8b694c49dc87515f82076baaa1da99e81` | `c41b1f16bb0871fc6c2ca943220f701520f9b689834574079582f6b3f2452d47` | `s3://borsuk-bench-453182569524-euc1/research/logical-cell-routing-diagnostic/20260802-v2/results/` | `s3://borsuk-bench-453182569524-euc1/research/logical-cell-routing-diagnostic/20260802-v2/indexes/flat` | complete and structurally valid from clean format-v19 revision `a6766b9`; all stage counters balanced, the process exited 0, the success marker is present, the failure marker is absent, and all 40 unique samples match the immutable identities. Claim-ineligible output records 27.841s measured wall time, 3.41 CPU-seconds, p50 2.845s, p95 7.278s, 1.437 appends/s, and 14,656 storage requests; it fails the production-viability gate |
| v3 | `7e63acf53e15f75861cdfd671ca044d68f5a11706a4b7e7b9df826b37ed839b2` | `c41b1f16bb0871fc6c2ca943220f701520f9b689834574079582f6b3f2452d47` | `s3://borsuk-bench-453182569524-euc1/research/logical-cell-routing-diagnostic/20260802-v3/results/` | `s3://borsuk-bench-453182569524-euc1/research/logical-cell-routing-diagnostic/20260802-v3/indexes/flat` | terminal infrastructure failure before index creation or measurement: the service environment omitted the AWS region, so the S3 client defaulted to `us-east-1` and rejected the bucket redirect; only manifest, environment, and failure-marker artifacts exist |
| v4 | `7e63acf53e15f75861cdfd671ca044d68f5a11706a4b7e7b9df826b37ed839b2` | `c41b1f16bb0871fc6c2ca943220f701520f9b689834574079582f6b3f2452d47` | `s3://borsuk-bench-453182569524-euc1/research/logical-cell-routing-diagnostic/20260802-v4/results/` | `s3://borsuk-bench-453182569524-euc1/research/logical-cell-routing-diagnostic/20260802-v4/indexes/flat` | running from clean format-v20 revision `a311450` on the isolated `c7g.8xlarge`, with `AWS_REGION` and `AWS_DEFAULT_REGION` pinned to `eu-central-1`; source upload was verified by round-trip SHA-256 before launch |

The v1 result is diagnostic evidence of remote request amplification, not a
routing comparison: 40 successful single-record appends issued 13,362 storage
requests, about 334 per append, while measured wall time exceeded process CPU
time by 6.8x. The resource envelope independently recorded 28.49s elapsed at
19% CPU and 241,961 voluntary context switches. Before another paired matrix,
reduce or amortize remote write-path request amplification and repeat this
bounded diagnostic from fresh immutable prefixes.

The v2 result does not establish an improvement over v1: request count rose
9.7%, throughput fell 22.6%, and p95 rose 11.1%. With one claim-ineligible
repetition per revision these deltas are directional diagnostic evidence, not
stable performance estimates. The unchanged order of magnitude shows that
16-way claim-shard collisions still invalidate most writer checkpoints and
trigger collection-wide refreshes. Do not launch the full paired matrix.
