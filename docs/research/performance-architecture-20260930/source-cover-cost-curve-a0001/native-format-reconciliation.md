# Native-format research reconciliation

Original d7529eada22243b3 completed once, exit0,426s, Fable5.1 xhigh. Full result retained separately. It proposes experiments and does not establish a measured improvement.

The strongest distinct candidate is 32-row authenticated SQ8 sub-pages plus a top-row shortlist. Current row bytes remain unchanged, but the shortlist, page geometry, planner, publication and admission assumptions change. Treat it as an algorithm/layout arm, not exact-parity scheduling. Keep width64 and current generations as immutable baseline evidence.

Corrections before adoption:

- Recompute missing per-row scores with native Rust on causality EC2 only. No local vector/query/truth processing. An offline test means no timed S3 fetch, not local-devbox computation or zero-cost evaluation.
- Do not turn the researcher's 2x byte-savings threshold into a new universal gate. Preserve positive measured Pareto improvements under the operator's existing hill-climbing policy. Current 30-GET/16MiB SQ8 limits belong to the baseline; changed resource envelopes must be explicitly declared rather than silently universalized.
- Predicted 2–4MB SQ8 reads, 8ms hash savings, SoA18ms-to2ms and BLAKE3 savings are unmeasured estimates. Row clustering, shortlist loss and bridge overfetch decide actual outcomes. Compression entropy was not measured; defer it rather than claiming incompressibility proved.
- Reuse the existing actual public planner/scorer/authentication paths in the falsifier. Preserve SOURCE/router nomination. Separately expose shortlist loss, sub-page closure/bridge costs and SQ8 quantization loss. Old V139/V146/V149/V150 failures stay closed; this arm introduces no new routing hierarchy.
- Audit every 256-row assumption, persistent schema and allocation formula, including import/repackage, publication, compaction and retained generations. Two loader-pin changes and digest emission alone do not prove lifecycle compatibility or bounded memory.
- Current digests are borrowed, not cloned per range; amplification is a future offloading risk. At32rows the SQ8 digest array is32*ceil(N/32)bytes, roughly1MB at1M and100MB at100M. Account both planes, router, concurrent generations, queries and maintenance; the cited3GiB envelope is not new100M qualification.
- Smaller authentication blocks alone do not reduce reads if the same rows remain selected. Separate block-size-only parity from a changed shortlist. A reduced shortlist uses97.60% recall floor against the fixed97.80% anchor, with no cumulative relaxation.

Disposition: candidate1 is selected for a prospective native quality/cost falsifier, not production adoption. Finish the current SOURCEcover cost job and inspect its result first; then choose the smallest informative shortlist/sub-page sweep before committing a new persistent format. Independent request/byte/authentication/scoring/query limits remain necessary product work and can proceed without assuming this candidate wins. No duplicate consultation or paid run is authorized by this report.

## Planner seam checked in selected main0b85

plan_two_bit_source_walks scores rows through a unit-max callback; Scale1m rank_walked_source_with_limit completes all32-row units inside the authenticated256-row candidate-page closure before computing page maxima. A row-shortlist falsifier should record row scores at this actual callback, preserve its exact eligible row roster and original SOURCEcharge, then compare its reconstructed baseline with the recorded baseline. Do not silently score extra corpus rows or promote SOURCEbridge overfetch into the candidate population. SQ8cover can bridge pages that the SOURCE nomination did not score; report baseline hits recovered by that physical expansion separately from shortlist loss. If a candidate requests additional SOURCE rows to recover them, count those new reads and bytes explicitly. A block-size-only arm that retains every baseline reranked row is the control; a shortlist arm is the distinct algorithm intervention.
