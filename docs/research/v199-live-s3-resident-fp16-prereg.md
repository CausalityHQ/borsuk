# V199 paired live S3 transport preregistration

## Decision

Can the V198 frozen ReLAION-1M D768 validation-1000 real-query plan be
executed against the immutable V164 SQ8 object through BORSUK's conditional,
page-authenticated S3 range reader, with exact SQ8 shortlist and resident
FP16 returned-ID parity, while observed response bytes and submitted GETs
equal the sealed plan? This is a transport and serving-execution gate on an
already-used split. It does not qualify the Python planner as production
serving code or establish cross-dataset generality.

Use V198's terminal-closed `plans.jsonl` and `cases.jsonl` from source commit
`fdc51a358be350678d9f6f279a2be95a57709c02`, attempt `a0001`, hashes
`0a61974457030d2e2ce828e7bbbbaf3d8acd9849e70a7aaafd0cb44b0c5dda00`
and `e1ba95fbd0cb58e28f1601740edf8c8775230e2ab5c8954e7f11c522dd94cd39`.
The V164 SQ8 object SHA-256 is
`aecf0f2704f44906f411a74ab81b36e5e05f81bab35f4c70558e88acbc4d05c9`.
Use its current `HeadObject` version identity and ETag, pinned in the attempt
reservation before any measurements. Do not rewrite the object. Authenticate
the full object once on Spot and derive a 32-row SHA-256 page sidecar. Bind
its manifest and digest hash to the same object, 1M rows, D768 and generation
196. Verify each conditional 206 range by ETag, object size, exact range and
all page hashes before scoring. Disable hidden transport retries; any error
fails the cell and records the submitted physical charge.

Use the same 1,544,000,064-byte V196 plane SHA-256
`1bce4288b38d88384503d8cfeae21667f45dbfb62303ce510f676fc0d66d4c47`
and source SHA-256
`2796b579f37afe99ca4aff57e282335a6a79ad30596645957d26326a0560cf86`.
Check each query's Rust SQ8 top-128 ordered IDs and physical ordinals
against V198's sealed cases, then rerank with the authenticated resident
FP16 tier and require the exact V198 ordered top-100 ID list. Stop on the
first mismatch, preserving the closed failing ordinal and cause. No GT is
needed for live transport; V198's per-query GT100 intersections remain the
paired quality authority.

## Frozen pass and measurements

Pass only if all 1,000 cases match SQ8 shortlist and FP16 top-100 exactly,
all requests are authenticated, and observed successful GET count and
response payload bytes equal V198's **10,047 GETs** and **7,388,559,360
bytes**, with no failed/hidden retries. Record each query's submitted GETs,
observed bytes, S3 phase, SQ8 score phase, FP16 phase and total wall time;
report p50/p95/p99 plus run wall, peak process RSS, cold resident hydration,
page-authority construction and object ETag/version. Compare the same-panel
V198 planned charge and V155 historical planned baseline of
11,134,007,040 bytes/22,126 GETs. This gate measures sequential query
execution with parallel GETs *within* each query. It does not claim
multi-query throughput, planner/router latency, or external-product parity.

Use one Causality Spot cell and immutable source archive. If interrupted,
discard the measurement cell and restart with a new attempt. Sync terminal
artifacts to S3, verify their hashes after terminal, and terminate the
instance immediately. A failure must be diagnosed from closed artifacts
before any revised attempt. A pass advances to a production Rust planner
and concurrent end-to-end gate, followed by a distinct real-query dataset.

## Amendment after closed attempt a0001

Attempt `a0001` on source commit `2e27d18e` ended at live query ordinal 84:
the first 84 SQ8 shortlists and FP16 returned lists matched, then ordered
SQ8 top-128 parity failed. Its terminal SHA-256 is
`e7e573b8fd1cea93a3d3423d47afc5e38de712c4c9bb98d8575a900d1d6f9cf3`;
Spot instance `i-05136be4b13e5f861` was terminated. The full V164 SQ8
object SHA and 31,250 page digests authenticated, and 84 live query records
closed, but this is a **failed gate**, not a partial pass. The failure may be
an order-only floating-point difference or a shortlist set difference;
neither is established from the first terminal log.

Attempt `a0002` keeps the identical inputs, plan and strict acceptance rule.
It adds a closed `live-mismatch.json` with the first differing rank, actual
and expected top-128 identities, actual score bits, intersection size and a
same-set flag. This is a bounded diagnostic repeat to decide whether the
production SQ8 arithmetic must be corrected or the parity contract should
use the set that resident FP16 actually consumes. Do not silently relax the
frozen gate or treat `a0002` as a pass if ordered parity fails.

## Amendment after closed diagnostic a0002

Attempt `a0002` on source commit `8b4b28b0` again stopped at ordinal 84.
The closed mismatch artifact SHA-256 is
`a62402169370a6f7fc981343975f88575a80a508b2a841a561e47a2e7c56335a`;
Spot instance `i-045137da83eaf7452` was terminated. Its actual and expected
top-128 `(physical ordinal, stable ID)` **sets were identical** (128/128).
Only ranks 62 and 63 swapped: Rust's two scores differed by four `f32`
ULPs. The Rust scalar and NumPy matrix-product arithmetic can order nearly
equal scores differently. The FP16 reranker consumes all 128 identities and
orders them afresh, so this swap cannot change its input or returned list.
This is a verified representation/precision ordering issue, not an S3
authentication or physical admission failure.

For attempt `a0003`, retain every frozen source artifact and physical plan.
Change the SQ8 parity gate to exact **set parity of all 128 `(ordinal, ID)`
pairs** on every query; retain exact ordered FP16 top-100 parity. Record the
number of SQ8 order-only mismatches and the first full mismatch artifact.
Any shortlist set difference, FP16 return difference, authentication error,
unexpected GET or byte count still fails the whole cell. This rule follows
the production result dependency: FP16 ranking is permutation invariant in
its candidate input. It is method-generic and does not inspect GT or tune an
individual query. The earlier ordered-parity failure remains negative
evidence and must not be retroactively called a pass.

## Amendment after closed a0003 boundary failure

Attempt `a0003` on source commit `61ee6f32` closed after 617 complete live
queries, then found a top-128 **set** difference at query 617, rank 127:
127/128 pairs matched. Its mismatch artifact SHA-256 is
`b3caf14730ad268213182cf65f985913726b9767ffa2c14120a0360c5600868d`;
Spot instance `i-084ed275bfc7e04f3` was terminated. Thus the V198 NumPy
SQ8 shortlist is not an exact oracle for the Rust production scorer at every
boundary. A claim that V198's 99,605 returned hits transfer unchanged to
Rust is unproven. The closed 617-query prefix is diagnostic only.

For attempt `a0004`, keep the same frozen requests, physical plans, object,
ETag, resident FP16 artifact and Rust SQ8 arithmetic. Execute all 1,000
queries and record **actual Rust top-100 returned IDs** in every authenticated
live raw row, alongside ordered/set shortlist and returned parity indicators.
Do not feed GT into the worker. After the terminal closes, an independent
checker intersects those IDs with V198's authenticated per-query GT100 lists
and compares the production result to the paired V155 baseline. Advance only
if actual Rust output meets the original ≥99,567/100,000 returned-hit,
p05≥98, zero-failed-request, ≤11,134,007,040-byte and ≤22,126-GET limits;
observed charge must still equal the V198 frozen plan's 7,388,559,360 bytes
and 10,047 GETs. Report every numeric parity mismatch without treating the
Python result as measured Rust quality. This restores the correct dependency:
production recall is measured from production returned IDs, independent of
irrelevant upstream score-order differences. The previous strict parity gates
remain recorded as failed evidence.
