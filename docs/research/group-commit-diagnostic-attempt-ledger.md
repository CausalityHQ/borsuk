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
| v6 | `1c5309c` | `afdffb282d9c8b8cf1f3e65a5acdeead9f6ae9b71c8a22d45c662d1f042a3297` | `293c573172c5a210e6e759e03fa3ca1625501fbd79ad1fdc404d6cfb002c56eb` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-diagnostic/20260802-v6/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-diagnostic/20260802-v6/index` | running on the isolated `c7g.8xlarge`; region, toolchain path, owned work root, and attempt-local target directory are pinned |

The frozen cell uses 2,000 logical cells, eight writers, 20 one-record calls
per writer, a five-millisecond maximum group delay, and a 64-record maximum
group. Success requires all 160 records after reopen and exact recall@1 of 1.0
for 20 deterministic inserted-vector probes.
