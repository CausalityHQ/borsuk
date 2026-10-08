**Select the smallest missing evidence: an authenticated source-utilization replay. Do not implement smaller ranges yet.** I inspected committed files at `89f1fad56c0e80d935c578d13025708e944800aa`; the dirty checkout differs, and closed evidence was read through Git.

The geometry gives different answers for source and SQ8:

| Path | Authentication and scoring behavior | Implication |
|---|---|---|
| Source | `PageAuthority::load_two_bit` authenticates **32-row units**. `plan_two_bit_source_cover` covers **256-row closures**. `rank_walked_source_with_limit` scores walked units, then completes units within closure pages up to its limit. | Smaller reads already authenticate, but walked units alone omit required completion rows. Savings remain unproved. |
| SQ8 | Generation opening requires **256-row pages**. `rank_verified_sq8_pages_inner` passes entire verified ranges to `rank_returned_ranges_excluding`, which scores **every returned row, including bridges**. | Smaller authentication pages alone cannot reduce uncompressed bytes while preserving the exact scored population. Omitting bridge rows changes selection semantics. |

`cover_pages` already bridges the cheapest gaps. With unchanged mandatory rows and request allowance, another merge heuristic cannot improve that minimum cover.

1. **Replay the already closed membership traces before changing production code.** Extend the existing bounded, authenticated reader in `crates/borsuk/examples/compare_native_replay.rs`. Process one query at a time; keep corpus, query vectors and truth unopened. Authenticate the archived producer/configuration identities rather than treating the historical run as execution of current HEAD.

   For each query, reconstruct the source closure from `semantic_units ∪ semantic_seed_additions`, and obtain actual scored units from `nomination_evaluated_units`. Reproduce the baseline cover and verify its GET/byte charges.

2. **Calculate the missing statistic:** baseline source bytes minus the minimum **32-row cover of actual scored units under the unchanged 128-GET allowance**. Report total, median, p95 and maximum potential savings, completion-limit frequency, and required GET count. Separately account for completion rows and mandatory gap bridges.

   This is an optimistic physical lower bound. Actual scored units become known after source scoring; using that hindsight directly in serving would be circular. If savings require score-dependent completion reads, a second dependency wave and its cumulative charges must be qualified.

3. **Use one bounded Rust falsifier before any paid measurement.** Extend `two_bit_generation.rs` fixtures around `bounded_completion_recovers_unvisited_rows_without_duplicate_or_extra_work`, `diagnostic_source_cover_charges_bridges_and_partial_tail_before_scoring`, and `fragmented_paged_source_preserves_trace_and_rank_across_get_caps`.

   Include an unwalked unit that changes the winning page, a completion-limit case, tied gaps and a short tail. Then perform a preregistered local replay of the first **64 frozen queries**, using authenticated real source/SQ8 records through the runtime validators. Require identical ordered scored-unit traces, page plans, complete SQ8 scored populations, returned IDs and score bits. Check corrupted payload/identity rejection and retained failure charges. Keep CPU1, 512 MiB, cache off, width32 and all existing caps. Truth stays unopened until outputs are sealed.

   **Stop** if the lower-bound replay shows zero aggregate savings. Any population or score mismatch falsifies exact preservation; authentication/setup failures are INVALID.

The proposed evidence change reaches only the replay reader/report schema and its fixtures, plus the Rust falsifier fixtures. Existing trace fields suffice; no production format change is needed. A later source implementation would reach `plan_paged_measured`, `fetch_source_ranges`, `plan_two_bit_source_cover`, its three non-definition call sites, and diagnostic/search parity fixtures. Source manifest fields remain unchanged: `schema=borsuk-two-bit-plane-v3`, `page_rows=32`, `record_bytes`, `records_sha256`, `page_digest_sha256`.

A smaller SQ8 format would additionally reach the publisher, generation-open `page_rows()==256` check, search offset conversion, planner and transport fixtures; its `schema`, `page_rows`, `page_digest_sha256` and root `page_manifest_sha256` must be reconsidered. That larger change has no demonstrated benefit under exact-population preservation.

Historical co-selection packing remains REJECT: original **50/32** fitting plans versus **48/33** after packing. Page-centroid coverage loss and useful surrounding rows also argue against dropping rows merely to fit budgets. These D768 findings constrain mechanisms; their thresholds do not become D1024 gates. Width32 qualification and the SIMD primitive REJECT remain closed.

No edits, builds, data execution, network activity or consultations were performed. Main risk: apparent source-byte savings may disappear under request limits or cost an additional read wave; no latency or quality gain is established.
