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

## Deterministic real-input cap failures

The final `build_overlap` original-input reauthentication loop calls
`read_source_probe_artifact(artifact, artifact.bytes)`. That helper requires
`cap <= PROBE_CAP` (128 MiB). The authenticated 100k canonical input is
308,000,000 bytes, so this path necessarily rejects the real build. Use secure
streaming exact-length/EOF/SHA authentication within the already admitted build
envelope instead of allocating another full body or lifting the legacy cap.

The paired evaluator passes `max_evaluator_payload_bytes` as the truth-read cap
to the same helper. Its fixed roster allowance alone is 167,772,160 bytes, above
128 MiB, so every admitted evaluator budget causes that call to reject even a
25,600-byte truth artifact. Admit the actual truth descriptor separately and use
its bounded body length as the read cap; retain the aggregate coexistence model
and the truth-after-seal invariant. Cover the actual paired pipeline with a
large evaluator budget and small truth body in the tiny native fixture.

Both failures are source-derived, not native execution results. Their repairs
must be compiled and run before any scientific job.
