**GO for pre-release code integration of `bf7ada2457` against `d667b171`. HOLD default adoption, product readiness and performance/vendor claims.** No blocking source bug established.

I reviewed the necessary changed production and diagnostic/count-curve function bodies, including the older layer, using immutable Git source. The Git diff reproduces patch SHA `2018cb3c…`.

- **Selection is truth-independent.** `sq8_subpage_shortlist.rs::reduce_shortlist_engine` fixes all four plans and rankings before opening truth (`1255`, `1399`). `assess_arm` (`2001`) counts actual returned-ID membership and separately accounts for bridge and block-expansion rescues.
- **Geometry and charges are coherent.** `sq8_count_curve.rs::aggregate` (`237`) reconciles per-query GETs, bytes, bridges, hits, gains/losses and miss buckets. `Control::verify` (`194`) establishes historical summary parity only; ordered historical rank/range parity remains unproved.
- **Production implements the intended policy.** `two_bit_generation.rs::retain_source_shortlist` (`730`) and `plan_two_bit_source_walks` (`760`) retain the best 256 actually scored rows with the specified ordering. `sq8_page_authority.rs::plan_source_shortlist_blocks` (`525`) preserves canonical greedy admission and budget refusals. Actual covered-row admission permits expanded covers to satisfy `k > 256`.
- **Qualification supports integration.** Collected logs corroborate the nonuniform serving, tail, bridge and score-bit checks, affected regressions, workspace Clippy and actual shim-unset test build. The 14 GiB peak includes compilation/cache and supplies no query-RSS qualification.

The measured frontier justifies advancing 256: 9,786 hits with lower planned GETs/bytes than baseline. Preserve 512 as the higher-quality alternative at 9,789 hits. Neither point establishes v9 cold performance or lifecycle savings.

The smallest remaining falsifier is a SOURCE-preserving real v9 fixture plus a v9 consumer comparing eight frozen queries at **30 GETs / 32 MiB**, requiring exact IDs, score bits, independently derived ranges and charges. Separate staging, cold ABBA1000, real S3 CRUD/fencing/GC, generation-swap/concurrency RSS and matched vendor conditions remain adoption gates. No measured v9 regression is established.

The original Claude banner-only failure remains failed history, not approval.

Not checked: fresh native execution, scientific bodies, or full retained-archive/canonical-body hashes; no edits, reruns or delegation occurred.
Risk: this GO admits the source increment only; product performance remains unqualified.
