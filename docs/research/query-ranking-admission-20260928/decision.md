# GO: charge complete concurrent query workspace and release result scratch

2026-09-28 UTC, baseefead3ad. Concrete library correction in both object-native
loaders and their shared SQ8 ranker. No query algorithm, score arithmetic,
ordering, duplicate-ID validation, GET/byte cap or persistent format change.
No local build, full suite, cloud job or duplicate execution.

## Root causes / changes

1. Both loaders charged response buffers but not row-count-dependent global/
   local score Vecs, ordinal rosters, membership/duplicate HashSets and sorting.
   In narrow dimensions these dominate response bytes. New shared checked
   query_payload_bytes admits3*wire + planner/scratch +256*maximum returned
   rows +4D weights, multiplied as a whole by active query slots. Returned rows
   are bounded by min(source rows,wire/(D+12)).256B/row is conservative payload
   modeling, not allocator/transport/RSS proof; their headroom remains separate.
2. The older PQ object-native loader added planner allowance once outside the
   concurrency multiplier. The same estimator now charges every active planner.
   Its256-row D64/two-query fixture rejects the previously admitted1MB cap and
   successfully opens/reloads at2MB. These are model/admission bytes, not RSS.
3. rank_returned_ranges truncated its fetched-score Vec to top-k but retained
   fetched-row capacity after releasing the query slot. Return the top-k slice
   in a fresh Vec and drop scratch before return. Existing score/ordinal/ID/tie
   results are unchanged. Caller-held results remain caller-owned memory.

## Original evidence / scope

- Cargo regression red exit101: old response-only model misses even one score
  vector for1M D1 rows. Two existing scoring tests still passed.
- Cargo green exit0:3 returned-range tests including narrow geometry, row cap,
  concurrent planner fees, invalid/overflow rejection and existing score/error
  cases. The helper changed to own the whole concurrency multiplier.
- Original sequential verification job exit0:1 PQ-object-native fixture plus1
  two-bit integration; authenticated open, remote reload, corruption, budget,
  namespace/conditional publication and search paths exercised. No full suite.
- Result-buffer red exit101: two returned hits retained capacity4 instead of2.
  Green exit0:6 tests compile the EXACT std-only production modules with rustc
  (ranking-memory-harness.rs), covering result capacity, ranking/ties, sparse
  ranges, budgets, duplicate IDs, malformed data and file/RAM score parity.
  This output-allocation correction followed Cargo loader checks; those loader
  admission paths are unchanged. The final exact core source was tested directly,
  rather than rebuilding the entire crate solely to duplicate its small tests.
- Final source hashes match remote tested modules. Formatting/diff checks pass.
  Original exits and SHA256 of uncompressed logs retained in verification.json;
  raw logs compressed losslessly with deterministic gzip. Admission-stage and
  final source patches distinguish the two verification stages.

No latency, throughput, empirical RSS, recall or vendor measurement occurred.
Earlier architecture KILLs remain unchanged; this fixes a required memory
boundary without reopening them. Both-vendor/fresh quality/cold HTTP/100M and
ID/mutation/compaction/recovery release gates remain OPEN. No background job is
left running. Next independent product gap: separate logical application IDs
from source ordinals in authenticated source/generation construction before
incremental overlays; reuse existing mutation/publication machinery.
