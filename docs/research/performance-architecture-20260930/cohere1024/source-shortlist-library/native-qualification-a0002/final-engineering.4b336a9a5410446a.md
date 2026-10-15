**GO for pre-release source integration** of `bf7ada2457d51` over `d667b171551b`. **HOLD for default adoption and product/performance claims.**

I reviewed the changed production and diagnostic code with connected lifecycle context, reconciled the previous reviews, and verified the frozen diff identity and collected native-log checksums. The [qualification receipt](/data/target/borsuk-source-shortlist-library.pending/launch-integration-geometry-a0002/independent-verification.json) records all 15 stages exiting successfully, including Clippy and the shim-unset workspace test build. I found no blocking serving, lifecycle, memory-accounting, or authentication defect.

**One nonblocking diagnostic correctness finding:** report validation admits impossible gain/loss counts.

- Locations: `sq8_count_curve.rs::aggregate`, candidate lines 299–307 ([frozen diff](/data/target/borsuk-source-shortlist-library.pending/final-branch-review.patch:582)), and `sq8_subpage_shortlist.rs::validate_block_admission_report`, lines 3873–3879 ([frozen diff](/data/target/borsuk-source-shortlist-library.pending/final-branch-review.patch:3544)).
- Counterexample: `baseline_hits=10, hits=9, gained=1, lost=2` satisfies the implemented checks, although a perfect baseline cannot gain another truth hit.
- Smallest falsifier: change the existing count-curve fixture’s first arm to `gained=1, lost=2`; assert that `finalize_report` rejects it. Static inspection indicates acceptance.
- Smallest fix: require `gained <= K - baseline_hits` in both validators, with a negative fixture.

This does **not** establish erroneous historical measurements: `assess_arm` derives these counters from actual set membership. It exposes incomplete validation of reconstructed reports.

The diagnostic implementation preserves truth-independent arm selection, authenticated SQ8 reads, bridge charging, and Pareto tradeoffs. Historical summary parity is correctly weaker than exact returned-rank parity.

**Adoption remains gated** on the v9 consumer and SOURCE-preserving fixture conversion; actual-eight exact IDs, score bits, ranges and charges; separate staging smoke; cold ABBA over the same 1,000 queries; actual S3 CRUD/pinning/GC; full canonical-body authentication; and lifecycle, query RSS, concurrency, and matched vendor comparisons. The S3 smoke remains ignored, no actual v9 cold result exists, and the 14 GiB qualification peak includes compilation/cache—it is not query RSS.

Skipped edits, native reruns, scientific payload bodies, network experiments, and delegation. Remaining risk: passing native gates does not establish real-input parity or operational/product qualification.
