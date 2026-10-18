# SQ8 cosine: first32 quality gate
Status: retain a small-panel research/Pareto candidate; full1000 confirmation next.
This is not production readiness, a serving latency result, or vendor parity.

## Change and result
The opt-in generic Rust scorer ranks reconstructed SQ8 vectors by cosine distance,
using existing codes, calibration and reconstructed squared norms. Existing L2 APIs,
operation order, serving defaults, routing and storage layouts remain unchanged.
The authenticated offline falsifier includes all fetched rows and bridge rows.
It replays original native IDs, physical ordinals and score bits before candidate
evaluation, and independently verifies decoded stored-norm bits and ID order.

On all first32 development queries (Cohere1M/D1024/cosine/k10), exact frozen inputs:
- Native SQ8:314/320; reconstructed-SQ8 cosine:315/320.
- Gains: q8 ID382916 and q20 ID397315.
- Displacement: q7 ID608770.
- Independent literal f64 reconstructed-cosine oracle:315/320, no query ranking disagreements.
- Repaired and original query objects are identical except allowed workspace-charge metadata.
No extra S3 fetch is introduced by the candidate arithmetic over the existing fetched
population; actual serving integration and transport/performance remain unmeasured.
The earlier canonical-f32 reference319/320 is a different reference, not this result.

## Correctness and memory evidence
Qualified source92d586fa79ed3a0bbfad95ecb50e05d748a4571b preserves original71005d5.
Original affected native counts:9 scorer,7 returned ranges,2 sealed replay,5 prior
f32 controls; release/workspace correctness+suspicious Clippy/actual shim-unset
workspace testbuild passed. Narrow actual-capacity repair:2 affected tests, release,
Clippy and actual shim-unset testbuild passed. Original compile and fixture failures
are preserved. Exact resource/command/exit receipts are in the immutable archive.
Both independent reviews were read in full. One requested-capacity versus allocated-
capacity finding was fixed and tested for refusal/exact-bound acceptance. Existing
L2/scorer arithmetic did not change in that repair. Model maximum115367936 bytes
under512MiB workspace; model and cgroup/RSS evidence are distinct.

## Next gate and limits
Proceed to the predeclared offline full1000 check of the same frozen scorer,
reporting every recovered and displaced neighbor without retuning. Existing1000
queries are development/regression data, not an untouched holdout.
A positive offline result still requires explicit serving integration, source-bound
admission, disposable canary, matched paired runtime/quality confirmation and
mutation-score consistency before production default consideration.
Lifecycle/recovery/concurrency and matched S3 Vectors/Turbopuffer gates remain open.
Spot interruption of the first source host is an infrastructure event, not a negative
algorithm result; retained EBS recovered the same worker/source. Failed attempts and
the provisional first run remain immutable.
