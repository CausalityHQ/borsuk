# Diagnostic-only cold SOURCE profile v6 (pending root qualification)

Base source: 72a51e9cf2a9c675e675299331c9a5d421f2ebe9. Historical cold-fetch64 outcome and its dispositions remain immutable.

The prior runner both refused FP32 trace execution and reserved only a 1 MiB diagnostic envelope. Raising the cap alone would not produce a SOURCE profile. The hidden TwoBitGeneration::diagnostic_search_fp32_with_store_traced threads the existing QueryProfile through the existing FP32 inner search. It resets the observer before refusal and charges its capacity against fp32_rerank_memory before the outer slot, normalization or query I/O. That same slot remains owned through canonical completion; there is no second search or slot.

The diagnostic marker is borsuk-sq8-range-attribution-diagnostic-v6. Admission records carry diagnostic_schema and diagnostic_transient_bytes; the strict reader independently checks them. Former v5 diagnostics are incompatible with this authority. Config/result schemas, production scoring, layouts, algorithms, defaults, production caps, line caps and cache behavior are unchanged.

On the target 64-bit ABI, each diagnostic query reserves:
- two existing panel-line allowances;
- plan range copy: 16 bytes per admitted physical range plus 32 bytes allocation allowance;
- for FP32, twice [shortlist × (1000-byte nominee Value/map/key allocation bound + 144-byte range/array/copy bound) + 8192-byte proof envelope], covering intermediate and final JSON construction;
- when traced, the actual QueryProfile::modeled_bytes ABI model (retained SOURCE/SQ8 spans, completed probes and one phase's scheduler/outcome peak), host sampler/read/map and 256 KiB failed-diagnostic Value tree.

All arithmetic feeding the prospective 2 MiB envelope is checked. Library scheduler state is not double-pinned by the runner. The library additionally admits retained traces/probes over the combined FP32 model. Returned nomination proofs are borrowed while serializing and already reserved by the existing caller trace scratch model. A test measures actual Value capacities, including BTree node/key allowances; allocator RSS/SDK/TLS still needs the root's independent host gate.

The source-bound leaf64, width64, SOURCE128/SQ8-32, FP32 shortlist16 forecast measured by the native fixture is pinned 980849 + trace peak 284736 = 1265585 bytes, below 2097152. This is a checked capacity model, not measured RSS or a latency result. The actual root input must match its own model/configuration and pass admission.

Existing synchronous authentication and SOURCE scoring CPU counters remain separate. acquisition_poll_cpu remains unresolved_not_instrumented; the process residual is unattributed CPU, not TLS/network CPU. No clock spans an await.

Root alone owns real-eight input admission, a separate two-query disposable canary, the fixed-eight profile [0,14,125,375,454,546,687,859], and interpretation. Test fixtures are synthetic native/loopback evidence, not real dataset admission. Compare IDs, score bits, plans, nomination proofs and charges with the frozen reference. No ANN, native latency, default, or vendor claim follows.

Baseline test evidence is frozen separately from the changed build. The fresh-mtime untouched runner/source copy produced ELF SHA256 1e8e99bf9f5421295005b4c7b30ecaa3cc1d579e501a1ce0cfbf3fa1cac6428f; its complete embedded runner and generation bytes match untouched base. All four pre-existing numeric-oracle tests fail at score bits 0 versus 1031798784. Their cfg(test)-only repair decodes every dyadic SQ8 coordinate and computes sequential scalar cosine independently, retaining all ordered-ID and exact-bit assertions. The earlier Cargo-reused witness is invalid and remains preserved.

Qualification is pending. The old Spot host terminated before final checks. Recovery preserved all four source hashes on the same EBS; a fresh baseline rebuild then exposed a reverse shared-target freshness reuse during the changed build. Neither failed compile nor unrun checks qualify the packet. Final evidence and any limits must accompany the release handoff.


Post-review source repairs are UNVERIFIED. The incumbent is width128, unchanged; the actual native producer fixture, admission fixture and root recipe now use width128. The earlier width64 forecast above is not width128 qualification. The strict reader requires v6 for every traced FP32 configuration. New complete-reducer negatives cover coherent nonexpanded v4 downgrade and startup model zero/minus-one, relying on the existing independent fp32_serving::validate_model equality. HTTP cancellation tests retain the original client, release explicitly cancelled handlers, assert terminal active counts, and attempt recovery through both entrypoints. Only known disconnect errors from explicitly cancelled fixture handlers are tolerated. No native tests or release builds have run for these repairs. Root must qualify the exact final source on a newly authorized host before integration or scientific use.
