# Observed raw S3 Vectors ENHANCED baseline

The original frozen run completed all phases successfully and its native cleanup
was independently confirmed: both owned resources are absent. Same1M compact
corpus IDs and same1000 query/truth pins;1000000 raw vectors were read back
bit-exact before one-hour inactivity and query execution. First query remains
included. ENHANCED is the observed provider default, not STANDARD.

| Configuration | Recall@10 | p95 ms | Query-time serial QPS |
|---|---:|---:|---:|
| S3 Vectors ENHANCED |96.17%|205.998|7.483|
| BORSUK48 A1 / A2 |97.83%|218.011 /212.188|6.350 /6.411|
| BORSUK44 B1 / B2 |97.63%|207.753 /215.558|6.660 /6.155|

SDK whole-pass throughput is7.224QPS. Its query-sum reciprocal and whole-pass
clock boundaries differ, and the table does not establish equal lifecycle cost
or concurrent service capacity. Actual dollar/billed-byte values remain null.
SDK process query RSS was not independently recorded; enforced external envelope
was8GiB, not the legacy512MiB emitted field. Preserve all first-failed receipts.

BORSUK used the authenticated normalized corpus; SDK used original raw f32.
The native normalization reexecution matched the retained digest; the original
producer ELF is not recovered. Finite precision/ties, startup pool/credential
work, retry policies, uncontrollable backend cache, and single SDK pass versus
Rust ABBA prevent a strict matched vendor-win claim. One-hour inactivity does
not prove provider eviction. Turbopuffer remains unmeasured.

BORSUK currently offers higher measured recall with slightly slower tails.
New evidence reprioritizes the next cheapest latency falsifier: retain48 as
high-quality incumbent and test existing40-leaf Pareto point with its qualified
binary rather than add a wider-profile implementation first. The40-leaf point
already measured97.37% recall and13.76% fewer bytes than48 on this fixed cohort;
its end-to-end timing is not established. Preregister order/cache/resources and
reuse admission/smoke; no fresh experiment was launched by this decision.
Wider coverage remains a separate high-quality research candidate, not rejected.
