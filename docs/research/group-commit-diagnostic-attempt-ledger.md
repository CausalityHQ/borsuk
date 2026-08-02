# Group-commit bounded-diagnostic attempt ledger

This diagnostic qualifies durable process-local group commit before any full
publication matrix. It is claim-ineligible. While an attempt is incomplete,
inspect only terminal markers, service/process health, logs that do not contain
measurements, and resource telemetry; never inspect partial measurement CSVs.

| Attempt | Revision | Source SHA-256 | Manifest SHA-256 | Result prefix | Index prefix | Status |
|---|---|---|---|---|---|---|
| v1 | `1c5309c` | `afdffb282d9c8b8cf1f3e65a5acdeead9f6ae9b71c8a22d45c662d1f042a3297` | `293c573172c5a210e6e759e03fa3ca1625501fbd79ad1fdc404d6cfb002c56eb` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-diagnostic/20260802-v1/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-diagnostic/20260802-v1/index` | running on the isolated `c7g.8xlarge` worker with region pinned to `eu-central-1`; both result and index prefixes were verified empty before launch, and the source archive is SSE-S3 protected |

The frozen cell uses 2,000 logical cells, eight writers, 20 one-record calls
per writer, a five-millisecond maximum group delay, and a 64-record maximum
group. Success requires all 160 records after reopen and exact recall@1 of 1.0
for 20 deterministic inserted-vector probes.
