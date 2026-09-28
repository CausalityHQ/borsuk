# Resumed GC checkpoint decision

Full product objective restored verbatim using native goal tool; remains active.
No existing job/review duplicated. All original and new GC check workers closed.

| Gate | Evidence | Decision |
|---|---|---|
| Pre-I/O mutation admission | Regression red, then green operation log empty | PASS |
| Validation error before deletion | Pending body exceeds cap; current head remains readable | PASS |
| Claim-only capped sweep and recovery | Old SQ8 retained/claim deleted; ready rebuilt under fresh claim | PASS |
| Focused functional regression | 3 application-ID/fence/GC,1 generation,4 HTTP | 8PASS |
| Real authenticated S3 | Stale conditional PUT rejected,4 deletes, IDs[1,2,99], owned-prefix cleanup | PASS |
| Full workspace all-target gate | Native S3 then workspace: library1630PASS,57FAIL,6ignored; later targets unexecuted | FAIL; no push yet |
| Late ambiguous DELETE/reused key | Causal trace in reconciliation; falsifier not yet run | OPEN; no production crash-safe GC claim |
| Matched quality/tails/QPS/dollars versus BOTH vendors and100M | No new relevant measurement | OPEN |

Existing dual review and all failed launcher/compile/prefix attempts retained.
New worker compute estimates sum$0.106, excluding EBS/S3 and not invoice; based
on the carried observed0.3618USD/hour Spot quote. These are correctness checks,
not product lifecycle/throughput-dollar evidence.
