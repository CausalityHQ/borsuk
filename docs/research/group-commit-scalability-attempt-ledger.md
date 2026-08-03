# Group-commit scalability attempt ledger

This campaign qualifies the production `GroupCommitWriter` at 2,000 and 16,000
logical cells with 1, 8, and 32 concurrent producers over five repetitions.
While an attempt is incomplete, inspect only terminal markers, service/process
health, non-measurement progress, and resource availability. Never inspect a
partial summary or samples CSV.

| Attempt | Revision | Source SHA-256 | Manifest SHA-256 | Result prefix | Index prefix | Status |
|---|---|---|---|---|---|---|
| v1 | `3ea0335` | `1fb3282fe8b3ea8327c60c121e394f50dd2bb36ff866c7c1e4102af015dc891a` | `c9a3914d39ded8b119f19f61f6faf8c58068c9d8f99b53d5f0f4deadb2e727bf` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260803-v1/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260803-v1/index` | running on the isolated `c7g.8xlarge` from fresh immutable prefixes |

The campaign is claim-ineligible until the root completion marker exists, no
failure marker exists, the service exits successfully, and the fail-closed
validator reconciles every matrix cell, raw sample, group receipt, request
total, resource exit, visibility result, exact-recall result, and correctness
gate.
