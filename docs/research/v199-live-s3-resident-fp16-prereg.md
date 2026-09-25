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
