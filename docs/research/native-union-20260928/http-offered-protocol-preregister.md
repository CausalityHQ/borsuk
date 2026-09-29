# Bounded offered HTTP driver protocol authority

2026-09-29, base 83c72d1f. This is Python-only protocol qualification, not
corpus recall, ANN performance, fresh panel qualification or vendor comparison.
The current Rust library 2696-pass authority and HTTP k10/k100 affected example
2-pass authority remain unchanged. No native build or full workspace gate.

Reuse the existing absolute-offset scheduler and native HTTP/percentile helpers.
A finite declared panel (1..10000) schedules each identity exactly once at the
specified rate. Pre-encode requests before timing. One bounded thread pool,
nonqueued client admission, no retries. Record scheduled, dispatched, started,
wire-completed timestamps; preserve dropped, 503, timeout, transport and invalid
response outcomes. Server admission timestamps are unavailable and remain null.
The successful-QPS denominator includes the full offered window and final drain.
Report successful HTTP and scheduled-completion tails separately, all offered
terminal tails, dispatch and client queue delays. Percentiles use the previous
linear interpolation method. Conditional successful recall and all-offered
recall (unsuccessful offers contribute zero) are distinct. Actual native k10
references are mandatory; k100 prefix references cannot satisfy k10 geometry.
Caller authenticates artifacts/source/reference provenance before invocation.
RSS, publication/open time and infrastructure/lifecycle dollars belong to the
campaign caller and remain unknown in this protocol report. Failed/timeout
physical counters remain incomplete; never turn known-counter sums into full
GET/cost totals. The unchanged current boundary rejects 503 before ANN/data GET.

## Single AWS synthetic check

Causality eu-central-1 c7g.2xlarge Spot, quote <=$0.30/hour; encrypted disposable
80GiB volume. Wall cap600s; check180s; systemd210s,512MiB,zero swap,CPU0-1.
Compute ceiling$0.05 plus$0.10 EBS/S3 allowance, estimates not invoiced totals.
Shared campaign lock, global live BORSUK guard and exact-tag stopped guard;
immutable attempt reservation/source archive/terminal. No overlapping checks.
Interruption invalidates the attempt; no automatic retry. Terminal artifact
sync then immediate termination, independently verified terminated state and
all artifact/source hashes. Active observation is terminal/infrastructure only.

Runnable real-socket self-check uses synthetic identifiers and five cases:
1. Actual8QPS full ten-offer schedule,k10 identity/counter/timing/denominators.
2. Actualk100 request/reference geometry.
3. 503, wrong head and timeout retained with conditional/all-offer denominators.
4. One-worker overload produces explicit client drops, drain-adjustedQPS.
5. Invalid rates and k100-as-k10 reference rejected before dispatch.
PASS requires all five, zero swap/OOM, unchanged native395-file authority and
complete authenticated terminal/cleanup. Any check failure is engineering
INVALID, never a scientific product KILL. No dataset performance number follows
from synthetic socket timing. Next campaign must independently freeze actual
native references, exact GT and audited novel identities before corpus work.
