# Decision: library assurance repaired; workspace integration gate still fails

AWS causality eu-central-1 Spot i-0895058b8f1d66ba0 terminated; frozen source ed1ca2f6e28ad7a2555bf99930099a561759993e9b8c077f1fd3193cf549da42, base 83cc6cdb. Artifact hashes/lengths independently verified; changed Rust source independently matched the authenticated archive. Terminal exit101 is retained. Worker elapsed776s, estimated compute $.0458 excluding EBS/S3, not invoice.

The two corrected compaction checks passed, then the full cargo test --locked --workspace --all-targets --jobs4 ran once. Library: **1687 passed, 0 failed, 6 ignored**, test runtime229.30s (correctness runtime, not query latency). All fifty-seven original library failures are repaired. The earlier 23-case gate passed21 and failed2; its terminal is preserved in preparation-red.

Twenty-eight setup calls explicitly prepare the existing diagnostic authority after bulk finalization. Three rebuild tests renamed to describe the explicit diagnostic operation. Two paged-compaction checks now assert that production compaction clears the unsupported research authority, then explicitly rebuild it. All original mutation, bounded-delta, memory-pressure, prepared-plane and deadline assertions preserved; production defaults/behavior unchanged.

Workspace then failed at integration test v36_prefix_geometry_resident_projection_is_replayable_and_scalar_exact (crates/borsuk/tests/v36_prefix_dataset.rs:2812): InvalidStorage("V36 prefix source decoder working set differs"). Its target reported62 pass /1fail. Other targets after this point were not reached. This is NOT full workspace success. Next diagnose that shared projection/admission path, rerun the affected integration layer, then one final full assurance gate.

No new performance campaign or vendor comparison. ReLAION returned-quality KILL and CoHere copy-cost KILL remain unchanged. Both-vendor product goal active. New read-only post-spill research consultation497cb16cb6b5447b is running; do not duplicate.
