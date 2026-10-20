# Current cold SOURCE CPU diagnostic
## Decision
Keep the original 256-entry scorer and the existing shared two-worker SOURCE pool. Test a runtime-dispatched full-table AVX2 gather kernel next, with exact original arithmetic and a portable fallback. This is a falsifier, not an accepted library change.

The hypothesis that one initialization semaphore serializes entire queries was refuted by source inspection and ten existing SOURCE lifecycle tests. It protects pool creation; queries already share two workers. A four-worker policy needs queue/occupancy evidence, full memory accounting and mixed-policy lifetime safety before implementation.

## Native evidence
One unchanged qualified executable, source 0cddd79569db2f92bf2079f1d9bbc3b90759a895, ran actual runtime admission8, a separate disposable canary2, then A1/B1/B2/A2 with first8 queries. A is external sampling off; B is software user-CPU sampling at199Hz with bounded DWARF callchains. Current main edad92fb5 has the same relevant production code; its three additional Rust fixture repairs are test-only. Source identities, configs and executables are bound in the archive.

Every cell passed the strict reader and the independent canonical f64 oracle. Every measured cell returned80/80 truth hits, identical ordered IDs, raw score bits, physical ordinals, plans, traces, logical charges and canonical shortlist against the sealed full1000 gold. No population-quality claim follows from selected8.

Each measured cell charged SOURCE986GET/310142976verified bytes, SQ8256GET/52645376bytes and canonical128GET/525312bytes. SDK cohort counter deltas were identical:1370GET attempts,363313664consumed payload bytes, no failures. These exclude startup and are process transport observations, not wire or billed bytes.

B1 sampled167 events and B2 sampled157, both with zero recorded lost samples. Block scoring accounted for21.56%/21.02% self user-CPU samples across SOURCE workers and Tokio. SOURCE-worker SHA256 accounted for17.37%/19.75%; hashing also appears on other threads. These samples identify expensive current functions, not duplicate authentication obligations.

Measured cohort CPU totals A1/B1/B2/A2 were1066093749/1101942796/1096887632/1127626939ns. Pooled sampled/unsampled CPU ratio was1.0023292573. This small observation is not an overhead guarantee or speedup. Application cache was0; provider eviction was not established.

## Limits and reconciliation
The earlier eight-query residual used different selected ordinals, fetch128 and a different executable. Its408ms remainder cannot be treated as an exclusive current SOURCE cost. The new samples include process startup, have small counts, and cannot partition exclusive SOURCE residual or prove a critical-path bottleneck. The proposed50% residual explanation criterion is therefore unproven, not passed.

The research consultation used simulated retrieval, async-systems and skeptical performance perspectives. Their arguments are hypotheses, not consulted experts or evidence. Its claim that chunk preservation already exists must be narrowed: a separate candidate exists; the current range implementation still collects contiguous bodies. No repeat or promotion of that candidate is authorized here without its own closed result.

## Smallest next falsifier
Use the existing256-entry table and row validation unchanged. Compare a runtime-checked AVX2 full32 gather path against the original emitted portable/vector kernel in the same release test executable. Preserve per-row byte-add order, signed-zero identity, scalar finalization, generic dimensions, invalid-record behavior, scalar-control and non-AVX2 fallback. Charge bounded stack and alignment before I/O.

Run the fixed-work, eight alternating repetition, single-prepared-query/256-distinct-unit primitive only after native correctness and required assurance pass. Preserve1/7-row tails as controls. Inspect emitted instructions. A win must then survive separate actual-input admission/canary and an exact cold paired query falsifier before default integration. A primitive loss stops this arm without another ANN run. No new table-compression sweep or worker-count change is selected.

## Primary measurement references
- [Linux process/thread clocks](https://man7.org/linux/man-pages/man3/clock_gettime.3.html): process CPU includes all threads; thread CPU must stay on the executing thread.
- [Async tracing scope](https://docs.rs/tracing/latest/tracing/struct.Span.html#in-asynchronous-code): entered spans across awaits can misattribute work.
- [perf record](https://man7.org/linux/man-pages/man1/perf-record.1.html): software sampling/callgraph semantics.
- [Rust AVX2 gather](https://doc.rust-lang.org/core/arch/x86_64/fn._mm256_i32gather_pd.html) and [runtime feature detection](https://doc.rust-lang.org/std/macro.is_x86_feature_detected.html): mechanism only, no predicted speedup.

No performance/vendor win or production readiness is established. The generic library goal remains active.
