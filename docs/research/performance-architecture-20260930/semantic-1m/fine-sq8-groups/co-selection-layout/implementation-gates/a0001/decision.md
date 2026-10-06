# Co-selection native qualification: PASS

Exact native candidate `510bde198af4f28952db2fc85a42018472b23fdc`; integrated four files byte-for-byte as `5da993c7`. Original exec68555 completed exit0. Instance `i-0138fe480f4178e19` terminated before collection. Authenticated controller replay passed.

All21 serial stage groups passed:11 new library tests and1 new binary test, affected regressions, release binary, workspace all-target Clippy correctness/suspicious, and actual shim-unset workspace test compilation. Source407 before/after identical. This does not claim execution of every workspace test. Frozen commands and exact named pass counts are in workspace-receipt.json.gz and test.log.gz.

Cgroup memory peak8589934592 bytes (8GiB cap), memory.max events5172, zero swap peak and zero OOM/kills. CPU quota200%; compiler jobs1 and test threads1. Reclaim at the limit is recorded, not hidden.

| Gate | Decision | Evidence scope |
|---|---|---|
| Native correctness/compilation | PASS |21 actual serial gates; exact source |
| Real co-selection locality | NOT RUN | Next:128 authenticated plans,32 total reads/16MiB, no truth |
| Recall/cold latency/QPS | NOT RUN | Locality survival required before actual SQ8 population scoring |
| Production maintenance/100M/vendor win | UNQUALIFIED | Full product goal remains active |

Next use the qualified binary for the frozen source-only locality falsifier. Preserve graph/PQ nomination and SQ8 bytes. Complete valid locality failure rejects the arm; authentication, execution or resource failure is INVALID and must be repaired. No retuning from consumed queries or truth.
