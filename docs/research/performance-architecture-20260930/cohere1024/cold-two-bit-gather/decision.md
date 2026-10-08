# Two-bit lookup gather candidate

Decision: implement one bounded source-only SIMD candidate in the generic
PreparedTwoBit scorer, then falsify it on an exact release executable. No
measured gain is claimed. The closed cold CPU profile attributes13.65% self
CPU to this scorer. SQ8 already batches8/4/2/1 rows. The alternate SHA backend
was validly rejected at5.2563% saving; its source remains unintegrated.

Gather four successive table values within ONE record, then reduce in the
exact original byte order through the original f64 sum semantics. Retain
record/scalar validation, final arithmetic, public APIs, scalar fallback,
all caller/row/error order and scratch/layout/authentication/GET accounting.
Runtime AVX2 detection only on x86/x86_64; other machines remain scalar.
Tile-relative indices slot*256+word prevent dimension-dependent signed
index overflow. No horizontal reduction, chunk sums, FMA, reassociation,
new allocations/dependencies, prepared-query fields or dataset restrictions.
Bounds and target-feature safety require explicit audit before remote use.

Across-row batching has better latency-hiding potential but risks fetching
later FnMut callback rows before an earlier error. This candidate avoids
that contract change. Existing scalar expression is an independent test
control, not a production backend/configuration framework.

OWN2: rotated_two_bit.rs production/helper/inline tests/one ignored release
primitive; two_bit_generation.rs TEST ONLY stateful callback parity fixture.
Prior test bodies preserved. Exact Result/score-bit parity in debug+release
for forced scalar, available AVX2 and public dispatch; generic dimensions
1,3,5,255,256,257,768,1023,1024,1025,4097; cancellation/signedzero/all byte
values and scalar/length/nonfinite failures. Real codec-generated fixtures
plus independent literal table/reduction witnesses; no tolerance-only oracle.
Planner fixture asserts exact invocation prefix and error for missing/bad
rows, and successful plan/trace parity. No callers change in production.

Root reconciliation of specialist recommendation: retain historical safety
limits for the primitive CPU1/256MiB/noSwap/pids128/30CPU-sec/120wall-sec;
remote build CPU2/8GiB/jobs1. This synthetic warmed primitive is not an ANN
measurement and reads no corpus/query/GT. Actual input admission and separate
staging canary remain mandatory before any future cold ANN experiment.
Full release/Clippy/real shim-unset test compilation required before source
integration. Local SOURCE ONLY CPU1/256MiB/noSwap/120s formatting/hash checks.

Preregistered primitive design: complete frozen scalar scorer vs actual
public candidate dispatch;32-row and17-row deterministic panels at D257,
768,1024,1025; five alternating paired blocks (80 ordered records), fixed
4MiB encoded-record work target per cell. Query preparation excluded; all
Result/score bits independently compared before timing. Report all geometry,
order, fixed/requested/completed calls/bytes, processCPU/wall, identities and
score-bit seals; use black_box so both kernels execute. Linux release only,
AVX2 required by root host admission. Native missing ISA/control failure,
resource/deadline incomplete run INVALID; candidate semantic mismatch REJECT.
Accept only >=20% median scoringCPU saving separately for D1024 full/tail
AND both alternating-order subsets; <=5% median regression on D257/768/1025
full/tail. No timing assertions in ordinary tests. Root independently checks
all80 records and live unit/cgroup limits, selected release ISA/codegen,
ordered additions without FMA/reassociation and exact source pins.

Only a primitive winner advances to full native qualification and separately
frozen uninstrumented cold ABBA (CPU1/fetch32/application caches OFF).
Cold p90/p95 improve>=5% in both pairs, QPS nonregression and exact1000-query
IDs/scorebits/recall/traces/charges parity required. No saved S3 rerun, no
Turbopuffer service run, warm caches or additional CPU cores in this arm.
