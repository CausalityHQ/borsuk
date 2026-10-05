# Implementation research review

Consultation `da98e08a016c45f1`, actual model `gpt-6.1-sol`. Requested Opus fell back after authentication failure; no duplicate consultation was started. Reviewed frozen source `65183e2223fff58a5bbfb53a6de452e459ed7429`.

**Independent research critic — BLOCK before qualification.**

I inspected frozen commit `65183e2223fff58a5bbfb53a6de452e459ed7429`, not the dirty checkout. These are additional findings; the known startup-admission repair is excluded. No files were edited or native tests executed.

**Blockers needing repair**

1. **P1 — Both real builds and every paired evaluation encounter incompatible reader caps.**

   `hierarchical_semantic_cells.rs:2198,2243` limits `read_source_probe_artifact` to **134,217,728 bytes**, including its supplied `cap`.

   - `semantic_cell_overlap.rs:503` reauthenticates canonical input with `cap = artifact.bytes`. The required 100k/D768 canonical file is **308,000,000 bytes**, so this always rejects after replay and staging.
   - `bin/hierarchical_semantic_cells.rs:165` requires evaluator admission of at least **167,772,160 bytes**, before adding requests and truth. Line **256** passes that aggregate evaluator budget to the same reader. Consequently, every otherwise admitted paired run rejects before opening truth—even when truth is only 25,600 bytes.

   Use bounded streaming authentication for large source artifacts and a per-artifact read cap for truth. Preserve the historical probe cap; lowering evaluator admission cannot resolve this contradiction.

   **Minimal falsifier:** a tiny authenticated file read using the paired caller’s admissible evaluator cap currently fails at the descriptor guard. No large payload is necessary.

2. **P1 — A mandatory test deterministically contradicts the validator.**

   `semantic_cell_overlap.rs:1448` expects this to fail:

   ```rust
   validate_placement(0, 0, false, &[(0, Some(1))])
   ```

   At lines **294–301**, primary validation checks `owner == cell`; both equal zero. This is a valid primary placement whose replica lives elsewhere. Repair the expectation and test an actual owner mismatch.

   **Smallest native confirmation**, on the root’s bounded build host:

   ```bash
   CARGO_BUILD_JOBS=2 cargo test --locked -p borsuk --lib \
     semantic_cell_overlap::tests::overlap_corrupt_frame_mapping_and_body \
     -- --test-threads=1
   ```

3. **P2 — Candidate failure loses completed control-arm read accounting.**

   `bin/hierarchical_semantic_cells.rs:223–241` completes control search before candidate search, but emits both traces only afterward. If candidate search fails, its accounting reaches INVALID while the completed control trace is discarded. The failure context also lacks arm and query ordinal.

   Preserve cumulative attempted-read accounting before fallible candidate work and event emission.

   **Minimal falsifier:** complete one control query, corrupt a candidate frame after authenticated open, then fail candidate search. The INVALID receipt must retain control reads plus candidate attempted/failed reads, with arm and ordinal.

**Missing effective acceptance tests**

- `overlap_full_scanner_matches_independent_sq8` uses the same `score_nominees` kernel as production. Its four-cell fixture fetches every primary, so it does not demonstrate additional unique membership from replicas.
- `overlap_nonunit_sq8_and_duplicate_truncation` also derives expected scores through that kernel. Its existing fixture permits independent assertions: scores **4 and 11**.
- Representative-location assertions call the same mapping helper used by production. Add independently known ordinal/offset expectations where the globally earliest copy is outside the fetched cells.
- `overlap_pair_seal_precedes_truth` exercises helper calls with dummy rosters, not `paired_overlap`. It cannot detect reordered truth access in the actual runner.

These are evidence gaps, not proof that those algorithms are wrong.

**Advisory**

The captured capacity cut, signed-zero handling, unchanged canonical descent, framing, duplicate-byte validation and immutable revision tuple appear consistent with the declared policy.

`representative_location` linearly scans cells for every scored row (`semantic_cell_overlap.rs:822,1206`): up to roughly **6.3 million extent comparisons per query** at 308 cells and 20,480 physical records. Profile this alongside compact-plane copying and repeated sorting before considering SIMD changes.

The supplied prior-art comparison supports trying overlap, but does not establish that this margin proxy targets router mistakes: routing ranks stored prototypes rather than traversing captured split predicates. A successful pair supports this fixed replication policy; it does not establish geometric optimality or vendor superiority.

**Smallest qualification gate**

After repair, run the library and binary `overlap_` tests on the exact revision, including independent scoring, partial-selection membership, representative mapping and paired failure/seal checks. Then complete the already-required Clippy and workspace test-build gates, followed by exact real-input admission and the separate canary. Preserve the fixed policy, existing quality thresholds and original workers.

