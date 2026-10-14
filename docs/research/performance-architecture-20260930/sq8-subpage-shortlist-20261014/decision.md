# SOURCE-scored row shortlists with authenticated SQ8 subpages

Status: **NATIVE CODE QUALIFIED; ACTUAL-INPUT EVALUATION UNRUN**. Reviewed driver source `fde98a39778c5fb76277327cd2b4bb224838cdc7`. This is an offline Rust falsifier; no production routing/default/layout change is selected by this document.

## Causal hypothesis

Keep the frozen production SOURCE128 nomination and baseline control. Capture only rows actually scored by its SOURCE walker, excluding physical gap overfetch. Rank approximate row scores descending with physical ordinal as the tie breaker. Compare fixed shortlists1024/2048/4096/8192 using ephemeral authenticated32-row SQ8 blocks. Rank all returned rows, including bridges, with the same SQ8 kernel. Smaller blocks alone are a byte-parity control, not a claimed improvement.

Baseline/control SQ8 budget is32 GETs /16,773,120B, matching the original producer. Candidate budget is30 GETs /32MiB. A geometry-only over-cap arm has **unknown quality**, never zero loss. The complete1000-query floor is9760/10000 hits, with9780 as a fixed reference. A measured curve, not the cap or an estimated twofold saving, determines the next design.

## Native correctness evidence

The original causality Spot qualification on exactfde passed13 stages: affected compilation, eight exact native fixtures, all206 comparator tests, all8 release shortlist fixtures, release example build, and workspace correctness/suspicious Clippy. The mandatory actual-env-unset jobs1 whole-workspace test build timed out124 after receiving only50s at the shared deadline. This was an execution-budget failure; no OOM, query/recall result or algorithm rejection is inferred. Original source, commands, counts and failed terminal receipts are preserved.

The distinct targeted batch completed the unchangedfde full workspace test build in636s with actual shim unset and jobs1, then compiled the separate test-first checkpoint89ef873773194f8c8c9fd1aacc28a79313681995 and observed the expected one-test missing-admission-implementation failure. The latter does not qualify an admission implementation. Both batches are independently closed with instances terminated, volumes absent and timers inactive. The prior13 plus successful exact-source final build qualify this driver code; they do not establish actual-input admission or scientific performance.

The earlier helper native RED/green and the two complete driver reviews are preserved. The review repair fixes baseline32-vs30 parity, fetch64/windowoff guards, bounded retained traces, independent ID-to-position checks, row-vs-block loss classification, checked bridge accounting and nullable unmeasured quality. Native process RSS is distinct from the payload model; no actual-input RSS claim is made here.

## Required next gates

1. Add shared Rust actual-input admission: authenticate real generation/SOURCE/SQ8/requests/truth through the runtime validator, validate every request and truth row, and recheck immutable identities. No candidate selection, quality or latency measurement in this mode.
2. Pass a separate disposable staging smoke for exact ELF/assets/CLI/refusals/output preservation/cleanup.
3. Freeze source/configuration and run the full1000-query quality/physical-byte curve. Reconcile every baseline plan/trace/logical charge/ordered ID/score bit before truth-based interpretation. Report shortlist containment, block expansion, bridge rescues and all four loss buckets.
4. Only a useful Pareto point proceeds to production-format/library integration and a fresh cold paired experiment. Query compute/download/concurrency limits remain separately bounded. SIMD changes require measured compute evidence.
5. Product acceptance still requires matched competitor end-to-end tail/QPS/lifecycle cost, incremental maintenance, pinning, recovery and bounded generation swaps. A local diagnostic or published unmatched competitor result proves none of these.

## Existing cold context, unchanged by this work

Cohere1M/D1024/cosine/k10,1000 original queries: retained BORSUK SOURCE128 runs R10=97.80%, p95=174.09/176.47ms, sequential QPS=6.913/6.930. SOURCE96 preserved recall but worsened p95 to312.81/317.44ms; retain128. Retained S3 Vectors context R10=96.1%, p95=215.02ms, QPS=7.358 has protocol/cache/timing mismatches. Turbopuffer has no matched reproduction. **No measured competitor win or new shortlist result.**
