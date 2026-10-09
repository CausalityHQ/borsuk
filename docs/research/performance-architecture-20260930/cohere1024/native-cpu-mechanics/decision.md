# Native CPU mechanics qualification

| Attempt | Fault checks | Canary | Native fixture calls | Disposition |
|---|---|---|---|---|
| a0001 | 61 passed, 1 failed | Unrun | 0 | INVALID: fallback-write fault injection did not establish intended directory |
| a0002 | 62 passed, 0 failed | Failed before dummy release | 0 | INVALID: payload cpuset interface unreadable |

Both original Spot instances were terminated before collection. Closed archive sizes, hashes, exact asset bodies and original exits were independently verified. The driver remains unqualified. These runs make no numerical CPU accuracy, observer-overhead, ANN recall or performance claim.

The a0001 harness repair preserves the driver and all refusal assertions; a0002 proves those function-level checks pass. The a0002 archive records controller delegation but lacks raw controller/subtree-control snapshots. Missing child controller enablement is an inferred setup cause, not a reconstructed observation. The next repair must record and check actual controller state and payload CPU 0 before any release, preserving resource limits.

Historical S3 Vectors and Turbopuffer performance comparisons are unchanged. No new corpus or ANN measurement was run.
