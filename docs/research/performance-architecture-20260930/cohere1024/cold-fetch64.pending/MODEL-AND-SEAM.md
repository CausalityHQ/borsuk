# Qualified source derivation and remaining seams

All paths below are relative to sourcecd8cac6 (cratesab73789) at
/mnt/borsuk-workspace/cold-overlap-reader-repair-a0001; no source changes.

runner check_cohere_native_baseline.rs:670-698 returns runtime reserve0 for
this Q1 config and couples max_blocking_threads to fetch_parallelism.
1675-1779 derives scheduler/auth/diagnostic and applies width to SOURCE and SQ8.
4385-4409 pins leaf64 trace scratch314752 and diagnostic caller delta524288+8*n.
Codec532480 + trace314752 = queryscratch847232, unchanged by fetch width.
Full caller7010842 stays unchanged: fetch_parallelism is scalar, execution
ordinal storage adds8*n, Q1 runtime reserve0; query geometry remains leaf64.

sq8_s3_range.rs:3198-3211 scheduler formula:
min(ranges,width)*(2048+outcome+8)+3*ranges*outcome.
Closed source-qualified startup312320 at width128 implies outcome96 exactly;
closed fetch128-full1000 reports confirm174592 at64. The RangeOutcome/IndexedRangeOutcome typedefs, VerifiedRange/RangeFetchError
definitions and Cargo.lock checked between7c3de4f andcd8 were unchanged.
Root independently confirmed qualified A startup in SSMb23ceacd, message
1792412517882167414-2287866. Runtime strict reader recomputes the compiled ABI;
these prospective arithmetic pins are not a new native qualification.
SOURCE128 dominates SQ832: A312320, B174592, delta-137728.

sq8_s3_range.rs:56-62, SourceAuthentication::additional_memory_bytes:
min(128,width+2)*4096 +2*(1048576+4096)+4096 +256.
The unchanged flat Offload2 score-overlap reservation adds2231766. Thus
A4865750/B4611798, delta-253952. Both changes are additive to the full FP32
model; total B-A=-391680. Full A471434981/B471043301; both diagnostic arms
add524288+8*n. Model-pins.json carries every expected startup value.

The actual SOURCE flat/cache0/Offload2 path passes max_parallel_source_gets
from two_bit_generation.rs:3747-3771 through sq8_s3_range.rs:1720-1748.
1804-1817 creates min(width,range_count) download permits, and admits
min(width+2,range_count) fetch/auth futures. 1944-1959 acquires a download
permit immediately before fetch_exact_range_core and drops it after the body,
before authentication/scoring. The shared compute pool still has two workers.
Actual eight-query admission validates the exact width headers and fresh
startup model, plus >64 SOURCE GETs, proving this configured seam is exercised.
The report emits per-query cap calculations; it does not claim observed full
permit occupancy or remove deeper SDK/provider limits.

Strict reader examples/compare_native_replay/current_v10.rs:402-442 recomputes
auth plus score-overlap;1600-1614 recomputes scheduler;1750-1815 checks
startup scheduler/codec/trace/query scratch/library cap. fp32_serving.rs:188-220
calls modeled_remote_memory with authenticated generation metadata and bound
width/limits, and checks complete FP32 total. Caller allocation capacities are
not fully serialized, so this packet independently pins caller totals; do not
mistake reader validation for independent proof of absent caller allocations.
Range allocator allowance remains a modeled bound, not measured occupancy/RSS.

# Requested traceON diagnostic: explicit unsupported gap

Root chose [0,14,125,375,454,546,687,859] before new execution (two controls
plus closed A64 tail union), superseding the generic first8 suggestion.
Ordinary8admit and2canary remain unchanged. Intended existing native API:
copy generated admit-A config, set execution.ordinals to that fixed roster and
execution.trace=true, bind the new config hash and BOTH expected headers,
then invoke the same runner CONFIG SHA OUTPUT and strict reader --fp32-serving
REDUCTION_CONFIG SHA REPORT. It would preserve own-reference exactness.
This is an API description, not a runnable/admitted extra experiment.

It cannot run under fixed caps. runner1705-1745 and reader current_v10:1640-1705
require diagnostic_bytes<=1048576. Leaf64 uses linecap262144:
traceON caller diagnostic =2*262144+2*49*16+24+24+131072+1+8192+262144
=927313. QueryProfile::modeled_bytes adds SOURCE scheduler at least128*2048
=262144 before outcomes/probes/retained spans. Total>=1189457>1048576.
Even one ordinal fails; reducing the selected count does not fix per-query cap.
No invented exact trace total, cap waiver, smaller leaf limit or substitute ELF.

If separately enabled in future qualified code, diagnostic.source_range_trace.cpu
exposes source_scoring/authentication/collector_allocation/collector_copy/
owned_allocation/owned_copy as raw thread_cpu_ns, wall_ns and clock_reads.
current_v10 validates CPU residual conservation; source_scoring is optional
when unreached. Observer/first-touch costs remain, scopes may overlap,
acquisition_poll_cpu is unresolved_not_instrumented, and SOURCE wall is not CPU.
This packet's profile entrypoint always exits BLOCKED and never falls through
to measured ABBA. Root must explicitly choose measurement or stop.
