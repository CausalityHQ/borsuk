# Positioned V12 Logical-Cell Routing Attempt Ledger

This ledger is for the positioned V12 architecture only. Earlier Cell-WAL,
lane-log, 96-dimensional, and format-version campaigns remain immutable
historical evidence and are not comparable to this campaign.

## Claim boundary

The frozen production matrix is a paired synthetic write-routing architecture
qualification: 768-dimensional cosine vectors, 2,000 and 16,000 logical cells,
1, 8, and 32 separately opened writer instances, flat and quantizer routing,
and five paired repetitions. It does not establish corpus ANN recall,
production read latency, 100M-vector scalability, or parity with another
product. Those require separate representative-dataset and paired-product
campaigns from the selected frozen revision.

## Attempts

| Attempt | Manifest SHA-256 | Source SHA-256 | Execution | Result |
|---|---|---|---|---|
| local-smoke-v1 | `9789067349810f85a8119bf7151554c8ec973ba275d2f95253c40c8e0f5e4537` | `4243561b1745b55b7f6fd33491c57a84521821d7402fef98f8618687ed21b65a` | Local filesystem, 2,000 cells, one writer, two operations per paired arm, 768D cosine, default production in-memory caches and no external disk cache | Complete and structurally valid after the source-identity and copied-manifest gates were enabled. All eight positioned correctness gates actually ran and passed; both arm processes exited 0; every terminal marker, raw sample, storage trace, stdout/stderr log, and sampled resource file was present. Claim-ineligible smoke observations were flat p95 2.889 ms at 340.818 operations/s and quantizer p95 3.097 ms at 303.512 operations/s, with 39 reported storage operations per two measured appends in both arms. These two-operation local figures are diagnostic only. |
| local-smoke-v2 | `1446bfddb64b34b8ee70111e89238e0a76834ba245d5315271c7a77b423a363a` | `1ab9fdf4fed2a0214044605f4ff1946f99e11d3e5d0094ac918d79d6b8b22eae` | Local filesystem, 2,000 cells, one writer, two operations per paired arm, centered 768D cosine, default production in-memory caches and no external disk cache | Complete and structurally valid after routing-path, vector-cohort, lifecycle-deadline, and terminal-marker publication gates were enabled. All eight positioned correctness gates ran and passed; the terminal validator passed; both arms reported two distinct cells and the same actual-vector cohort BLAKE3 `70ef5c303f241a678278b633b883a79ad7e049cb447422d5d05877c412ccfd95`. Claim-ineligible observations were flat p95 3.787 ms at 275.708 operations/s and quantizer p95 4.089 ms at 235.140 operations/s, with 39 reported storage operations per two measured appends in both arms. These two-operation local figures are diagnostic only. |
| local-smoke-v3 | `0ab241dc7a0f36a5ad982c06d20daea241581c5e0c42d4d8251266286d093451` | `8a407623d2c769c47af7d6998ef213eea28f9849d3fdff96f67fdbf470fe4fe9` | Local filesystem, planner-driven two-arm execution, 2,000 cells, one writer, two operations per arm, centered 768D cosine, default production in-memory caches and no external disk cache | Complete and structurally valid after the runner began consuming the authoritative planner and the worst-case lifecycle budget became fail-closed. The execution plan contained exactly two arms and a bounded 1,800-second worst case; all eight positioned correctness gates ran and passed; the terminal validator passed; both arms reported two distinct cells and the same actual-vector cohort BLAKE3 `70ef5c303f241a678278b633b883a79ad7e049cb447422d5d05877c412ccfd95`. Claim-ineligible observations were flat p95 3.208 ms at 303.711 operations/s and quantizer p95 3.394 ms at 280.340 operations/s, with 39 reported storage operations per two measured appends in both arms. These two-operation local figures are diagnostic only. |
| local-smoke-v4 | `0ab241dc7a0f36a5ad982c06d20daea241581c5e0c42d4d8251266286d093451` | `9b32778868bd2fb4d5d066c8cbf07fcaca62f12171ab8c584ddb325cd44b9bf6` | Local filesystem, planner-driven two-arm execution, 2,000 cells, one writer, two operations per arm, centered 768D cosine, default production in-memory caches and no external disk cache | Complete and structurally valid after the local-smoke execution plan was explicitly labeled claim-ineligible. The plan contained exactly two arms; all eight positioned correctness gates ran and passed; the fail-closed terminal validator passed; both arms reported two distinct cells and the same actual-vector cohort BLAKE3 `70ef5c303f241a678278b633b883a79ad7e049cb447422d5d05877c412ccfd95`. Claim-ineligible observations were flat p95 3.284 ms at 302.769 operations/s and quantizer p95 4.837 ms at 207.266 operations/s, with 39 reported storage operations per two measured appends in both arms. These two-operation local figures are diagnostic only and make no performance claim. |
| aws-v1 | `75c5f53b0a61d29a4b7d53e4724e25928847965cbb033415b311876b90502a80` | not launched | EC2 Spot `c7g.8xlarge`, eu-central-1, S3-only | Superseded before launch by the fail-closed routing-path and lifecycle revision; no measurements exist. |
| aws-v2 | `b65b97ddce55195bb2656e2b1aa25d28725a26099703932eed3b371b93664f44` | not launched | EC2 Spot `c7g.8xlarge`, eu-central-1, S3-only | Superseded before launch because its 900-second publication timeout exceeded its 60-second systemd stop grace; no measurements exist. |
| aws-v3 | `c0fd3524f55d03f894e85c1ff7f6e36ad8ddbea874fbe4062bc1ff42afde94a2` | `cfc55a96bac049426c467d9da5236c1011def009c8068f1e54b024a6f5ce6176` | EC2 Spot `c7g.8xlarge`, eu-central-1, S3-only; run `20260811T092233Z-v12`, instance `i-0ddc5ce165796bc7a`, launch price `$0.542900/hour` | Terminal startup failure before any measurement cell. `LOGICAL_CELL_ROUTING_FAILED` and runner exit 1 were preserved; zero cell completion or failure markers were published; the instance self-terminated. The terminal artifact boundary (`execution-plan.json` and `arms.tsv` present, `manifest.json` and the first correctness log absent) locates failure before the first Cargo gate. The stock Amazon Linux 2023 AMI had no launcher provisioning for the Rust toolchain expected at that boundary. No measurement CSV exists and no performance claim is made. |

The AWS launcher records the exact instance ID, availability zone, observed
Spot price, source and manifest hashes, process/resource/storage telemetry, and
terminal markers. Every terminal arm is synced immediately. A Spot-interrupted
nonterminal arm is discarded; samples are never combined across attempts. The
worker uses instance-initiated termination, a six-hour independent scheduled
shutdown, a systemd runtime deadline, and a delete-on-termination root volume,
so terminal or orphaned compute is not retained idle.
