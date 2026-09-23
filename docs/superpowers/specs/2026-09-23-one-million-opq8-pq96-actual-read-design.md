# ReLAION-1M real PQ96 code-read and page-containment gate

Status: conditional preregistration draft. First close the cheaper
source-vector final-page feasibility gate described below. Freeze
implementation constants and source revision before any source
construction or attempt. The terminal-closed 1M OPQ8 group-containment
result selected this investigation; it did not measure final-page quality.

## Prerequisite: source-only final-page feasibility

On the same sealed OPQ8 and page-centroid group plans, authenticate the
physical truth-owner page for all 100,000 development GT positions. For
each query, sum the largest 32 truth-owner page counts as an upper bound
that ignores the byte cap. If that upper bound fails the fixed GT100,
p05 or sub-90 gate, stop: no 32-page nomination rule can pass. Then
compute bounded float64 source-vector distances only within each arm's
selected groups and run the unchanged top-100-row page nomination under
32 pages and 16,777,216 bytes. If the source-vector candidate fails the
fixed absolute page gate, revise the page architecture before building
PQ96. Preserve the oracle, source-score, route and truth identities in
one terminal-closed source-only result. This is a new data-dependent
gate, not a Lean consequence of group containment.

## Decision and immutable inputs

Test whether the selected OPQ8 row route preserves its paired advantage
after a real 96-byte code wave and bounded page nomination. Freeze the
seven ReLAION source/development identities, original 7,278-page order,
910 groups, 1,000 queries and top-100 truth IDs from the prior 1M gate.
Authenticate and replay its `source-seal.json`, `plan-seal.json`, and
`plans.json` by their terminal-listed hashes; reject any mismatch in
ranked groups, selected groups, merged intervals, projected GETs or bytes.
The candidate and control are those sealed OPQ8 and page-centroid plans.
Truth is unavailable to construction and plan-replay phases.

## One real code representation

Use one source-only standard PQ96 candidate: 96 contiguous subspaces of
eight float32 coordinates, 256 centroids per subspace, one uint8 code per
subspace and exactly 96 bytes per physical row. Use deterministic source
training with a fixed seed, 100,000 source rows and ten Lloyd iterations;
pin the source-row sampler, NumPy version, arithmetic order and tie rule
in the committed implementation. No extra per-row scale, norm, ID or
padding may be hidden outside the 96 bytes. The initial codebook may be
large enough for the gate, but the production memory budget must include
it. This candidate is a width-compatible baseline, not a selected
quality winner.

Each physical group uses `u32_le page_count`, then one `u32_le row_count`
per page, then concatenated 96-byte records in sealed physical row order.
Its exact length is `4 + 4*page_count + 96*row_count`. Include group
SHA-256, offset, length, object role, page bounds and row count in a new
incompatible generation manifest. Bind source, membership, physical
order, PQ books, code object, OPQ8 route model and format marker by
digest. Reject incompatible or torn objects before query evaluation.
Construct in bounded source batches and seal source artifacts before
exposing queries or truth.

## Real reads and paired evaluation

Both arms use the same PQ96 object, same ADC scorer, same tie rule, same
page nomination, same 32-page/16,777,216-byte data-wave cap and same
queries. Only sealed code-group selection differs. A selected plan may
contain more than 32 groups; issue one S3 Range GET per sealed contiguous
interval, at most 32 intervals and 16,777,216 bytes. A broker with the
only network/credential access checks the plan coordinates. Require HTTP
206, exact Content-Range and byte count, then verify each constituent
group's framing and SHA-256 before delivering codes to the evaluator.
No eager full-object download, cached source-code substitution, or
group-by-group reads that exceed the interval budget count as the
actual-read gate. Record logical planned GETs and physical HTTP attempts
separately, including failures and SDK retries.

For each arm and query, record three ordered top-100 truth-owner hit masks:
selected-group containment; final pages nominated from bounded,
independently computed source-vector squared L2 over the selected rows;
and final pages nominated from actual PQ96 scores. The source-vector
diagnostic uses float64 difference-and-square in bounded batches, with
an explicit finite-precision error allowance if called exact. It never
promotes a failing production scorer. Use the fixed page-count and data
byte caps for both nomination arms. The historical source-only control
is a route-control authority, not a final-page control; compute a new
contemporaneous final-page control on this same code object.

After the prerequisite passes, the candidate advances only if its real-code final-page outcome meets
all four absolute gates: at least 98,151 GT100 hits of 100,000, p05 at
least 90 GT100 hits per query, at least 9,928 GT10 hits of 10,000, and
at most 49 queries below 90 GT100. It must also match or beat the
contemporaneous control on total GT100, p05 and GT10, and have fewer
sub-90 queries. Every code plan must stay within 32 logical GETs and
16,777,216 actual bytes; every data-page plan must stay within 32 pages
and 16,777,216 bytes. This is a deliberately strict development-cohort
gate. Report effect sizes, paired better/worse/tied counts, and all
resources even if it fails. If selected-group quality passes but the
source-vector page diagnostic fails, assign loss to page nomination or
data budget. If source-vector pages pass but PQ96 pages fail, assign
loss to representation or scoring. Do not tune PQ96 on these queries
after a valid quality failure.

Measure routing CPU, code fetch/authentication, PQ96 scoring and page
nomination separately per query. Fix concurrency, arm order or balanced
randomization, cold/warm cache policy, retry policy and failure handling
before launch; report observed p50/p95/p99 plus request/byte counts.
Sequential campaign wall time is not serving latency. Without actual
data-page reads and reranking, label timing code-read-to-page-nomination
latency, not end-to-end ANN latency.

## Execution and closeout

Run one create-only/readback-verified attempt on Causality Spot from a
pushed source archive. Record instance identity. A Spot interruption
invalidates its measurement cell: sync immutable terminal artifacts,
discard incomplete results without inspecting measurement CSVs, and
restart at the next attempt ordinal from the same source revision if
code is unchanged. Stop compute immediately after terminal publication.
Keep evaluator plus broker and descendants below 3 GiB with a 64-MiB
margin and zero swap. Independently recheck all 1,000 hit masks,
selected intervals, bytes, GET counts, group hashes, result aggregates
and source/plan identities after terminal closure. An authority or
resource failure invalidates the attempt; a valid quality failure
rejects this fixed OPQ8/PQ96/page-nomination combination.

Promotion also requires one preregistered validation-split run of the
frozen winner with no retuning; the repeatedly used development cohort
alone is insufficient. The sealed holdout remains unopened.

The next scale gate after a pass must use an untouched cohort, actual
data-page reads, a bounded-memory region or hierarchical route and measured 10M/100M throughput,
latency and memory. Lean bounds apply only to their modeled planner and
conditional certificate premises until implementation refinement is
proved.
