# Logical-cell write-routing attempt ledger

The production matrix is eligible only when its immutable result tree contains
`LOGICAL_CELL_ROUTING_COMPLETE`, contains no
`LOGICAL_CELL_ROUTING_FAILED`, and the fail-closed validator accepts every
paired summary, raw append sample, resource file, and correctness gate. Partial
attempts never authorize performance claims.

| Attempt | Source SHA-256 | Manifest SHA-256 | Result prefix | Index prefix | Status |
|---|---|---|---|---|---|
| v1 | `ea7322911393bec64f3153328bc412806546047a593f02ea8498dd3ba2564de8` | `b07a617061245b3f60fe0f40948746fa0c2790c3e042b012a5c0c902e22644d1` | `s3://borsuk-bench-453182569524-euc1/research/logical-cell-routing/20260731-v1/results/` | `s3://borsuk-bench-453182569524-euc1/research/logical-cell-routing/20260731-v1/indexes/` | running; launched 2026-07-31 on the dedicated `c7g.8xlarge` after confirming no live benchmark process and 163.7 GB free; two detached-shell startup attempts exited before runner initialization because the first output path was not writable and the second lacked the ec2-user Rustup context; both result and index prefixes remained empty with no terminal marker, and execution then started under ec2-user without changing the frozen source, manifest, or prefixes |

The v1 source, result, and index prefixes were verified empty before upload or
execution. The source archive and manifest objects use SSE-S3. Until terminal
validation, inspect only marker and infrastructure health; do not read partial
CSV files.
