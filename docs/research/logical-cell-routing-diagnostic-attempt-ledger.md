# Logical-cell routing bounded-diagnostic attempt ledger

These attempts diagnose remote write-path progress only. They are deliberately
smaller than the frozen paired matrix and are never eligible for routing
performance or product claims. Incomplete attempts are monitored through their
diagnostic terminal markers, non-measurement progress log, process health, and
resource telemetry; partial measurement CSV files are not inspected.

| Attempt | Source SHA-256 | Manifest SHA-256 | Result prefix | Index prefix | Status |
|---|---|---|---|---|---|
| v1 | `b6c4ab1f57afd9872e840e3922247a0f9ef8b3406194c1bb272155f30fcb2976` | `c41b1f16bb0871fc6c2ca943220f701520f9b689834574079582f6b3f2452d47` | `s3://borsuk-bench-453182569524-euc1/research/logical-cell-routing-diagnostic/20260802-v1/results/` | `s3://borsuk-bench-453182569524-euc1/research/logical-cell-routing-diagnostic/20260802-v1/indexes/flat` | running from clean revision `ff07610` on `c7g.8xlarge` in `eu-central-1`; both prefixes and the worker directory were fresh, the worker had no competing workload and 147 GB free, source and manifest hashes were verified after SSE-S3 upload, and SSM command `ed52ac52-7ee8-4d9d-abd2-490eb7c86f31` launched the isolated tmux job |
