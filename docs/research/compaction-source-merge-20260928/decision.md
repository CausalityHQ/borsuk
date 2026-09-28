# Bounded compaction input — GO for preparation and functional handoff

2026-09-28 UTC; parent `9e1f2aeb`. Beating BOTH S3 Vectors and Turbopuffer remains required; this is correctness evidence, not a matched quality/performance win.

## Concrete increment

`prepare_two_bit_compaction` requires an opaque sealed delta tied to the same base root, dimensions and namespace. Authenticated canonical recovery supplies descriptor/hash/geometry. A dedicated in-process blocking worker streams surviving FP32 base rows and IDs, masks replacements/deletes before writing, then appends sorted sealed puts. It never hydrates the base vector or ID plane into RAM. Local source bytes are rehashed during consumption and finite/unit vector validation fails closed. Existing SQ8 rebuilding revalidates output ID uniqueness; original authorized generation construction established base uniqueness.

Completion receipt v1 binds base/mutation revision+SHA, raw/ID SHA, geometry and observed maintenance SDK submissions/bytes. New raw/ID files install before manifest. Payload model charges twice the retained delta plus262144 fixed bytes, delivered chunk cap and three row widths. Disk model bounds canonical input + maximum merged raw/ID output +64KiB metadata before source GET. Transport/runtime/allocator and other tasks require separate caller admission. Dropping an async future cannot cancel an already-running blocking worker; retain/await maintenance admission through completion. This primitive does not mutate an index head.

## Original terminal and verification

| Evidence | Verified result |
|---|---|
| Provider/account | AWS profile causality, eu-central-1; one c7i.4xlarge Spot worker |
| Source | Parent9e1f2aeb + frozen dirty archive586072700a590b6703d89daf299bb10a931dea59019fced6dfd06f0d668d9ba5; all3 touched source/test files match immutable S3 archive |
| Worker | i-08e2583fcc9aed45b; original terminal exit0/complete; EC2 readback terminated |
| Focused fixtures | Signed-ID/source/publication test1 + generation test1 pass |
| Affected real local HTTP |4 tests pass; no live vendor/query performance measurement |
| Duration/resources |237s observed whole worker; timed compile/test154.12s, reported maxRSS5206908 KiB for compiler/test command, not serving RSS |
| Compute estimate |$0.0238 from observed Spot quote$0.3618/h × elapsed; excludes EBS/S3, not invoice/lifecycle cost |

Exact source/base/instance identities, terminal SHA and decompressed artifact hashes are retained alongside original reservation/user-data/launcher. No automatic replacement or duplicate paid job. Native review cooldown remained unresolved; no duplicate consultation/override. Early missing API red and the first misplaced by-value source test compile failure are retained in the conversation; Spark's full logs remain on that retired host and were not fetched after operator prohibition. No claim of complete local archive for those preliminary failures.

The functional512-row D2 test verifies all survivor bytes/IDs, pending replacement, absent deletion, no duplicate IDs, output digests, unsealed/wrong-namespace/RAM/disk/output/corrupt-source rejection with call charges, and all-deleted zero-row preparation. It then rebuilds SQ8 + generation using only existing native Rust APIs, publishes against the sealed original head, checks exact ID roster and fresh mutation head absence, and confirms the old sealed generation remains recoverable. Query quality and cloud serving latency are not measured here.

## Decision and next single gate

GO for bounded preparation and native nonempty handoff. Next implement one callable crash-resumable compaction orchestration using these inputs and native builders. Generation publication must incorporate every sealed state. Empty preparation succeeds but current generation builder rejects zero rows: empty serving semantics are explicitly OPEN, with no sentinel or silent mutation loss. Reader-safe in-process GC follows a verified handoff.

Both vendor matched recall/tails, e2e p90/p95/QPS/$,100M bounded-memory scale and total lifecycle cost remain OPEN. Current ANN route remains disqualified by its prior paired recall deficit; this increment does not reopen that decision. Execution policy is AWS causality only: no further DGX Spark compile/test/benchmark work.
