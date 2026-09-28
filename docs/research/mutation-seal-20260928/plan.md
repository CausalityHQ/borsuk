# Mutation compaction fence

Hypothesis: one irreversible CAS of the existing per-generation mutation head fences stale and initial writers while preserving a readable delta. Generation replacement must require that fence before staging objects. No new service or dependency.

Use mutation head v2 with required sealed flag, reuse bounded snapshot serialization, and seal an absent head with an empty revision-1 snapshot. Observed closed snapshots reject writes before staging; stale tokens fail CAS. Seal is idempotent only after authenticated readback; lost acknowledgements require sealed readback, not just equal snapshot SHA.

Gate: focused application-ID and generation tests on existing Spark, then affected HTTP tests. Cover absent/nonempty heads, stale writers, lost seal acknowledgement, prior schema rejection, readable pinned snapshots and unsealed replacement rejection. No paid runs or quality measurements. This fences publication but does not certify replacement contents or implement merge/GC; full compactor remains required.
