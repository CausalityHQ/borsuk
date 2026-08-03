# Group-commit bounded-diagnostic attempt ledger

This diagnostic qualifies durable process-local group commit before any full
publication matrix. It is claim-ineligible. While an attempt is incomplete,
inspect only terminal markers, service/process health, logs that do not contain
measurements, and resource telemetry; never inspect partial measurement CSVs.

| Attempt | Revision | Source SHA-256 | Manifest SHA-256 | Result prefix | Index prefix | Status |
|---|---|---|---|---|---|---|
| v1 | `1c5309c` | `afdffb282d9c8b8cf1f3e65a5acdeead9f6ae9b71c8a22d45c662d1f042a3297` | `293c573172c5a210e6e759e03fa3ca1625501fbd79ad1fdc404d6cfb002c56eb` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-diagnostic/20260802-v1/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-diagnostic/20260802-v1/index` | terminal infrastructure failure before index creation or measurement: systemd did not inherit Cargo on `PATH`; explicit failure marker present |
| v2 | `1c5309c` | `afdffb282d9c8b8cf1f3e65a5acdeead9f6ae9b71c8a22d45c662d1f042a3297` | `293c573172c5a210e6e759e03fa3ca1625501fbd79ad1fdc404d6cfb002c56eb` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-diagnostic/20260802-v2/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-diagnostic/20260802-v2/index` | terminal infrastructure failure before source execution: the service user could not execute the root-owned mode-700 wrapper; explicit failure marker present |
| v3 | `1c5309c` | `afdffb282d9c8b8cf1f3e65a5acdeead9f6ae9b71c8a22d45c662d1f042a3297` | `293c573172c5a210e6e759e03fa3ca1625501fbd79ad1fdc404d6cfb002c56eb` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-diagnostic/20260802-v3/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-diagnostic/20260802-v3/index` | terminal infrastructure failure before source download: the service user could not create the work root under `/data`; explicit failure marker present |
| v4 | `1c5309c` | `afdffb282d9c8b8cf1f3e65a5acdeead9f6ae9b71c8a22d45c662d1f042a3297` | `293c573172c5a210e6e759e03fa3ca1625501fbd79ad1fdc404d6cfb002c56eb` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-diagnostic/20260802-v4/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-diagnostic/20260802-v4/index` | terminal infrastructure failure before source download: the wrapper tried to delete its pre-created work root without parent permission; explicit failure marker present |
| v5 | `1c5309c` | `afdffb282d9c8b8cf1f3e65a5acdeead9f6ae9b71c8a22d45c662d1f042a3297` | `293c573172c5a210e6e759e03fa3ca1625501fbd79ad1fdc404d6cfb002c56eb` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-diagnostic/20260802-v5/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-diagnostic/20260802-v5/index` | terminal infrastructure failure before index creation or measurement: the service user could not write the shared `/data/target`; explicit failure marker present |
| v6 | `1c5309c` | `afdffb282d9c8b8cf1f3e65a5acdeead9f6ae9b71c8a22d45c662d1f042a3297` | `293c573172c5a210e6e759e03fa3ca1625501fbd79ad1fdc404d6cfb002c56eb` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-diagnostic/20260802-v6/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-diagnostic/20260802-v6/index` | terminal and structurally valid under the fail-closed validator: 160/160 records visible, exact recall@1 1.0, 20 groups of eight, 7.732 records/s, 1.173-second p95, and 1,150 reconciled requests (7.188/record). The correctness gate passes but production viability fails |
| v7 | `16b4ac4` | `187ffc4b895bf043c7a51c0f0b581cd3319c94eb099d43fb7c79f7fd389b653e` | `293c573172c5a210e6e759e03fa3ca1625501fbd79ad1fdc404d6cfb002c56eb` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-diagnostic/20260803-v7/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-diagnostic/20260803-v7/index` | terminal and structurally valid under the fail-closed validator: 160/160 records visible, exact recall@1 1.0, 20 groups of eight, 27.356 records/s, 307.23 ms p95, and 320 reconciled requests (2.000/record). The correctness and request-amplification gates pass, but the sub-200 ms production latency gate fails |
| v8 | `d51db4d` | `0ce5491eff78a74031715f6e208ac255634a8911aaec3abe43290b8a177f3671` | `293c573172c5a210e6e759e03fa3ca1625501fbd79ad1fdc404d6cfb002c56eb` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-diagnostic/20260803-v8/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-diagnostic/20260803-v8/index` | terminal and structurally valid under the fail-closed validator: 160/160 records visible, exact recall@1 1.0, 20 groups of eight, 36.820 records/s, 387.43 ms p95, and 260 reconciled requests (1.625/record). Request amplification improves, but the sub-200 ms production latency gate fails |
| v9 | `e8aa7bd` | `9a2de843be30a87e074bbadccb4c14888f56a37932c6c09fbcab5d67beb85f76` | `293c573172c5a210e6e759e03fa3ca1625501fbd79ad1fdc404d6cfb002c56eb` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-diagnostic/20260803-v9/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-diagnostic/20260803-v9/index` | terminal and structurally valid under the fail-closed validator: 160/160 records visible, exact recall@1 1.0, 20 groups of eight, 51.582 records/s, 198.41 ms p95, and 200 reconciled requests (1.250/record). This first bounded run passes the sub-200 ms checkpoint by 1.59 ms; scalable repeated qualification remains required |

The frozen cell uses 2,000 logical cells, eight writers, 20 one-record calls
per writer, a five-millisecond maximum group delay, and a 64-record maximum
group. Success requires all 160 records after reopen and exact recall@1 of 1.0
for 20 deterministic inserted-vector probes.

Terminal v7 is correctness evidence for durable group commit and materially
lower request amplification, but it is not acceptable production performance.
Its 5.849-second measured append window, 307.23 ms p95, and 6% aggregate CPU
show a remote-wait-bound path; they do not identify one individual request as
causal. The full routing/publication matrix remains blocked.

Terminal v8 removes another 60 requests versus v7 but does not establish a
latency improvement: p95 is 387.43 ms versus v7's 307.23 ms. The bounded runs
are single repetitions and cannot separate code effect from remote variance.
The remaining happy-path final publication still rereads the pinned collection
snapshot and already-reserved root head, so the next exact-revision diagnostic
must test elimination of those serial reads before any matrix promotion.

Terminal v9 removes another 60 requests versus v8 and is the first bounded AWS
run below the latency checkpoint: 198.41 ms p95, 139.74 ms p50, and 51.582
records/s with complete visibility and exact recall. The 1.59 ms margin is too
narrow and the attempt is only one repetition, so it authorizes scalable
repeated qualification but is not by itself a production-readiness claim.
