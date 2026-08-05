# Object-store acknowledgement-floor attempt ledger

This diagnostic measures the physical latency floor for one immutable payload
PUT followed by one conditional mutable-head PUT on the Causality benchmark
host. Incomplete attempts may be inspected only through markers, process/host
health, and error logs; measurement CSV files remain ineligible until the
fail-closed validator accepts a terminal campaign.

| Attempt | Revision | Source SHA-256 | Result prefix | Object prefix | Status |
|---|---|---|---|---|---|
| v1 | `6663162` | `fa12468a5b8e52e22aba8b07ab4f361ee09eca4007e31f9edc4c418f82eda361` | `s3://borsuk-bench-453182569524-euc1/research/object-store-floor/20260805-v1/results/` | `s3://borsuk-bench-453182569524-euc1/research/object-store-floor/20260805-v1/objects/` | terminal pre-measurement control failure. The conditional-create control passed, but the stale-update control rewrote identical bytes, whose unchanged S3 content ETag remained a valid update token; the runner failed closed before creating a measurement directory and uploaded only the root failure marker and resource trace. No CSV was created or inspected. v2 versions every head payload so each successful CAS changes the ETag input, matching a real advancing committed sequence. Host memory and disk remained healthy. |
| v2 | `1cede24` | `6f1e2869f90a581946968f78e2b95d811eb7177dc65d27266ae6b10f4acc2991` | `s3://borsuk-bench-453182569524-euc1/research/object-store-floor/20260805-v2/results/` | `s3://borsuk-bench-453182569524-euc1/research/object-store-floor/20260805-v2/objects/` | terminal and independently validated. All 4,000 raw samples, protocol factors, conditional-create/stale-update negative controls, summaries, source/manifest identities, and resource exit reconciled. Plain 3,072-byte PUT p50/p95 was 23.155/27.436 ms; conditional create 23.681/37.820 ms; changing 4,096-byte conditional head update 30.982/38.640 ms; the sequential payload PUT then conditional head update was 53.975 ms p50, 64.781 ms p95, 87.274 ms p99, and 97.037 ms maximum over 1,000 samples. The preregistered decision is `proceed`: a two-dependent-write lane-log acknowledgement boundary has ample physical headroom below 200 ms on the isolated Causality c7g.8xlarge/S3 eu-central-1 setup. Wall time was 2m20.56s, maximum RSS 9,528 KiB, and exit status 0. |
