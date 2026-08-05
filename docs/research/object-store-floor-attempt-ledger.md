# Object-store acknowledgement-floor attempt ledger

This diagnostic measures the physical latency floor for one immutable payload
PUT followed by one conditional mutable-head PUT on the Causality benchmark
host. Incomplete attempts may be inspected only through markers, process/host
health, and error logs; measurement CSV files remain ineligible until the
fail-closed validator accepts a terminal campaign.

| Attempt | Revision | Source SHA-256 | Result prefix | Object prefix | Status |
|---|---|---|---|---|---|
| v1 | `6663162` | `fa12468a5b8e52e22aba8b07ab4f361ee09eca4007e31f9edc4c418f82eda361` | `s3://borsuk-bench-453182569524-euc1/research/object-store-floor/20260805-v1/results/` | `s3://borsuk-bench-453182569524-euc1/research/object-store-floor/20260805-v1/objects/` | terminal pre-measurement control failure. The conditional-create control passed, but the stale-update control rewrote identical bytes, whose unchanged S3 content ETag remained a valid update token; the runner failed closed before creating a measurement directory and uploaded only the root failure marker and resource trace. No CSV was created or inspected. v2 versions every head payload so each successful CAS changes the ETag input, matching a real advancing committed sequence. Host memory and disk remained healthy. |
