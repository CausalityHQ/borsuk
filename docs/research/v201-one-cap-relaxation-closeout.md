# V201 one-cap relaxation closeout

## Frozen result

Source commit `6fa72aae30df9f7d5b5ae2e29d32ddad702f1ab7`, attempt
`a0003`, Causality Spot `i-0b75e975897cc7df5`, closed terminal SHA-256
`17703b3e38eb1c42f25d4e09e0be543a2803d1bcbee9bf3dfcf6031c64b42978`.
The launcher read back all six terminal artifacts and verified the instance
terminated. Independent `scripts/check_v201_relaxed_cover.py` passed its
closed-artifact replay. The targeted Rust test filter passed 2/2 tests.

The cohort is the **already-used** ReLAION-1M D768 validation-1000 real-query
split, with frozen V198 GT-blind weights/plans and the same fixed prices and
physical caps. The worker read no ground truth. V201 made no new quality or
recall measurement.

| Exact tier | Queries | Admission |
| --- | ---: | --- |
| Linear uncapped | 861 | Both hard caps met |
| Unit-cap relaxation | 85 | 32-GET cap also met |
| GET-cap relaxation | 3 | Query unit cap also met |
| Needs full 2D DP | 51 | Neither one-cap winner admitted |

Every one of the 949 admitted plans reproduced V198's exact physical
intervals, modeled mass, unit count and GET count. The 51 unresolved rows
emitted no serving witness.

| Measured phase on one c7i.12xlarge | p50 | p95 | p99 |
| --- | ---: | ---: | ---: |
| Linear planner core, all 1,000 | 46.604 µs | 56.817 µs | 60.656 µs |
| Unit-cap phase, 139 fallbacks | 6.118 ms | 6.487 ms | 6.697 ms |
| GET-cap phase, 54 fallbacks | 0.431 ms | 0.501 ms | 0.526 ms |
| Complete hierarchy, all 1,000 | 46.768 µs | 6.465 ms | 6.947 ms |

Peak Rust process RSS was 20,914,176 bytes. Times exclude feature and weight
construction, 2D fallback, S3, reranking, concurrency and throughput. The
V200 Python weight construction previously averaged 2.56 ms/query on this
cohort; it is still a separate measured phase, not part of this Rust result.
The V199 live authenticated S3 sequential transport+SQ8+FP16 result remains
46.57/92.23/137.87 ms p50/p95/p99 and 99,605/100,000 returned GT100 on the
same already-used split; it was not rerun with V201, so do not sum percentiles
or claim end-to-end latency from these separate cells.

## Decision

The generic conditional one-cap hierarchy is accepted as a fast exact
partial solver. Implement the 2D hard-cap recurrence for the remaining 51
queries and check all 1,000 against V198 before integrating it into serving.
The hard cap remains a query workload/quality policy, not an arbitrary
100M-vector memory knee. Lean proves the admission implication only when the
relaxed solver is exact and the physical witness satisfies both caps; the
remote parity validates this frozen cohort, not executable refinement for all
inputs or actual recall/latency at other scales.

`a0001` was canceled before measurement after finding a traceback bug;
`a0002` compiled but stopped in a flawed test fixture. Both failed attempts
and their terminated instance identities are recorded in the preregistration.
