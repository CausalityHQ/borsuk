# Implementation engineering review

Consultation `8110737f30604431`, actual model `gpt-6-astra`, source `65183e2223fff58a5bbfb53a6de452e459ed7429`.

**Engineering/release critic: BLOCKED.** Reviewed frozen commit `65183e2223fff58a5bbfb53a6de452e459ed7429`. All five file hashes match the supplied contract; `git diff --check` passed. Findings below exclude the known startup-admission issue.

Source references are relative to `crates/borsuk/src/` at that commit.

**Blockers needing repair**

1. **P1 — Both FIRST100k/D768 builds necessarily fail during final reauthentication.**
   `semantic_cell_overlap.rs:503` calls `read_source_probe_artifact(artifact, artifact.bytes)` for the canonical input. That input is `100000 × (8 + 4×768) = 308,000,000` bytes. The helper explicitly rejects **caps exceeding 128 MiB** (`hierarchical_semantic_cells.rs:2198,2243–2248`).

   This occurs after replay, extent construction and staging fsync, but before publication. Both overlap and control are affected.

   **Smallest repair:** use bounded streaming authentication for large originals; preserve the existing resource limits. A descriptor-only native falsifier can demonstrate the current rejection before opening or allocating any large payload.

2. **P1 — Every admitted paired evaluation necessarily rejects its truth read.**
   `bin/hierarchical_semantic_cells.rs:165–170` requires the evaluator budget to cover at least:
   ```
   128 × 32 × 640 × 64 = 167,772,160 bytes
   ```
   Therefore every admitted `max_evaluator_payload_bytes` exceeds the helper’s 134,217,728-byte maximum. Line 256 nevertheless passes that evaluator budget as the truth-read cap.

   Even the frozen **25,600-byte truth artifact** is rejected, after all 128 rosters have been scored and sealed.

   **Smallest repair:** separate the aggregate evaluator budget from the per-artifact read cap. Native regression: authenticate a tiny truth file with an otherwise admissible evaluator budget, exercising the actual evaluator read path.

3. **P1 — A mandatory test contains an assertion that must fail.**
   `semantic_cell_overlap.rs:1448` asserts:
   ```rust
   validate_placement(0, 0, false, &[(0, Some(1))]).is_err()
   ```
   This is a valid primary in its owner cell. The implementation correctly returns success because `owner == cell` (`:295–300`). Having a replica elsewhere does not invalidate the primary.

   **Smallest repair:** assert success for this case and use a different cell for the invalid-primary case. Do not change valid placement semantics to satisfy this assertion.

4. **P2 — Required independent correctness and seal-order coverage is ineffective.**
   The “independent SQ8” oracle calls the production `score_nominees` and `cosine_vector` (`hierarchical_semantic_cells.rs:4141–4147`). Its representative-location assertion calls the same production lookup used to construct the result (`:4149–4150`). The fixture fetches the entire tiny dataset, so it also misses selective replica gains.

   Separately, `overlap_pair_seal_precedes_truth` manufactures events and calls seal helpers; it never invokes `paired_overlap` (`bin/hierarchical_semantic_cells.rs:1495–1515`). Moving the production truth read before sealing would leave this test passing.

   **Smallest repair:** add hand-computed score expectations, independently decode physical locations and selected replica membership, and exercise the actual paired sequencing with an observed truth-open operation. These are qualification gaps; they do not establish that the current scoring arithmetic is wrong.

**Advisory findings**

- **Failure receipts lose completed work.** If control search succeeds and candidate search fails, lines 223–227 retain only candidate failure accounting. The successful control trace is never emitted because the combined event comes later at line 239. Preserve arm, ordinal and both attempted-read accounts before returning `INVALID`. A candidate-frame corruption after opening is the smallest falsifier.
- **Representative lookup adds avoidable query work.** `representative_location` scans every extent (`semantic_cell_overlap.rs:827`) for every scored unique row (`:1205–1206`), giving `O(unique rows × total cells)` lookup work before truncation. The validated ordered extents permit binary search. This is a source-level cost finding, not a measured latency violation.
- **Panel-offset validation is weaker than the existing frozen-panel guard.** The new evaluator accepts arbitrary `first` values and performs unchecked `×400` truth-offset arithmetic (`bin/hierarchical_semantic_cells.rs:159,257,261`). The existing `probe_panel` requires `first == 0` (`:898–902`). Reusing that restriction would enforce the consumed panel and eliminate this overflow path.

The **smallest existing native falsifier**, for the root’s authorized bounded remote environment, is:

```bash
CARGO_BUILD_JOBS=2 cargo test --locked -p borsuk --lib \
  semantic_cell_overlap::tests::overlap_corrupt_frame_mapping_and_body \
  -- --exact --test-threads=1
```

After repair, require the two cap regressions and independent sequencing/scoring fixtures before the existing overlap suites, Clippy and workspace test-build gates. No algorithm sweep or quality-threshold change is needed.

No files were edited. No Cargo, native tests, corpus reads or experiments ran; the deterministic failures above are established by source inspection.
