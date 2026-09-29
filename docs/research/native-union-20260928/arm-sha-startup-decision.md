# ARM SHA startup intervention decision, 2026-09-29

GO for the preregistered matched startup diagnostic. Not a query, quality,
throughput or vendor-comparison GO. Frozen source d32d4722, archive
`a37b33402a82bf9653ed6542b7a523fd46d64307d449ff3d575d4c572e751ace`,
config`326e708dc97839c9782b194730e69893661212401e548ebb92cad0e92ec8e75a`.
Original controller74411 CLOSED0; independent verifier6311 CLOSED0, no reader fix
or rerun. Spot`i-079f60c802ee62c7b` independently terminated after924s.

One change: ARM-only sha2 hardware feature and its locked optional assembly
package. Same native source/scorer, roots, transfer schedule and identity checks.
Control is the authenticated software executable from startup-profile/a0001,
reused on the same c7g.2xlarge host. Both rustc release/commit match1.98.0/88d9e12ae;
full verbose toolchain/Cargo parity is not proven and not claimed. Four ordered
ABBA blocks, each[ReLAION,CoHere]*3; six starts per arm per dataset. FIRST1M D768
cosine metadata, no query split,24starts, zero ANN queries. No application cache;
S3 cache uncontrolled. Small-panel medians are exploratory, not population p90.

| Dataset | Control ready ms | Candidate ready ms | Candidate minus control ms | Control load/decode ms | Candidate load/decode ms | Candidate minus control ms |
|---|---:|---:|---:|---:|---:|---:|
| ReLAION |4715.098704|3512.8186385|-1202.2800655|1801.7981715|656.3444995|-1145.453672|
| CoHere |4715.3827985|3512.6526265|-1202.730172|1801.1508505|656.1656525|-1144.985198|

All table values are measured and independently verified. Both fixed gates
require lower readiness AND lower load/decode medians; both pass. Streaming
medians remain2480.816ms/2484.301ms(candidate), against2494.140/2485.593ms(control).
Staging medians2690.172/2692.515ms. Physical metadata per start stays nine objects,
256,694,403/256,694,456B. These are metadata reads, not full startup physical
request accounting. Decode still includes authentication/read/codec/graph work;
its remaining656ms is not finer phase attribution.

Locked ARM release and nine focused tests pass: staging2, generation2, HTTP2,
source/KAT/tamper3. All395 Rust/Cargo identities match control except the exact
three declared feature/lock/KAT files. Build peak6,977,839,104B<10GiB, profiling
639,840,256B<8GiB; zero swap/OOM, four CPUs, profiling4GiB address-space cap.
Historical2696-test assurance is prior evidence; changed authentication dependency
has this focused qualification, not a fresh full-workspace pass claim.
Compute$0.045791 estimated from924s and observed Spot quote, excludes EBS/S3,
not invoiced or per-query total cost. Both executables/compiled source, all138
closed artifacts, resource and shutdown receipts independently authenticated.

Retain the hardware-backed candidate. It removes about1.2s of namespace
readiness but startup remains3.51s, above subsecond/444ms published contexts.
No new recall, incoming-query p90/p95, QPS, saturation or matched vendor result.
Historical peer quality/results remain tied to their earlier source epoch.

Next decisive test is a runnable true namespace-cold first-query boundary with
fixed quality/identity parity and a separately stated protocol. Record the full
startup-to-first-response interval and component timings; do not subtract startup
or merge it into historical metadata-resident tails. Streaming is the next
measured bottleneck; any subsequent transfer intervention gets one declared
change and a matched control.10M/100M memory/scale, saturation/totalcost,
generation swap and incremental lifecycle measurements, and BOTH-vendor matched
comparisons remain open. No universal architecture KILL from a tiny historical
internal regression. [Verification](arm-sha-startup/a0001/verification.json).
