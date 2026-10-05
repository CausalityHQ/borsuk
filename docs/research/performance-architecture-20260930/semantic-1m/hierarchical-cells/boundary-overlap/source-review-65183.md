# Startup admission review of 65183e22

Candidate `65183e2223fff58a5bbfb53a6de452e459ed7429` remains unverified.
No compiler, native correctness, recall or performance result is implied.

## Required guard-order repair

`OverlapIndex::open` calls `Prototype::open_for_source_probes` before calculating
and checking its aggregate overlap startup peak. The router therefore preloads
while the parsed overlap root is already live, before aggregate admission.

`paired_overlap` opens both indexes before calculating their combined resident,
preload, evaluator and query coexistence budget. Individually admitted indexes
can allocate their combined startup payload before the pair rejects it.

Authenticate bounded metadata and compute the aggregate allocation requirements
before heavy router/directory/cell bodies are opened or loaded. For the pair,
admit both startup peaks and resident/evaluator/query coexistence before opening
either index. Bind metadata to the same authenticated roots and revalidate it
when materializing the indexes. Account for metadata parsing itself.

Preserve the original candidate and make a separate source repair. Include a
falsifier where each index fits individually but the pair does not: rejection
must precede heavy body reads. Also cover insufficient single-index admission,
metadata tampering and missing bodies. Keep scientific routing, replica quotas,
scoring and quality gates unchanged.

This is source inspection, not a measured RSS violation. Whole-unit remote
cgroup verification remains necessary after compilation.

## Additional test findings independently checked

`semantic_cell_overlap::tests::overlap_corrupt_frame_mapping_and_body` expects
`validate_placement(0, 0, false, &[(0, Some(1))])` to fail. This is a valid primary
placement with an alternate replica destination. Change that expectation to
success and test a genuinely wrong primary owner and replica destination.

`overlap_full_scanner_matches_independent_sq8` computes its expected scores with
the same production `score_nominees` kernel used by the path under test. Retain
that parity check, but add an independent test-only scalar decoder and score
calculation over the fixture's stored norms, codes and coefficient bits. Preserve
the original arithmetic operation order; compare logical IDs and score bits.

The binary's `overlap_pair_seal_precedes_truth` invokes seal helpers and manually
opens absent truth. It never calls the actual paired evaluator. Exercise the
actual paired pipeline with tiny private test geometry, keeping the production
100k/D768/count64 entry and controls unchanged. Verify real selections and full
rosters are sealed before missing/tampered truth refusal, and incomplete or
mismatched selection fails without a success seal. Keep failure, output-cap,
fsync and no-overwrite behavior observable. These are test repairs, not a change
to scientific admission or result thresholds.
