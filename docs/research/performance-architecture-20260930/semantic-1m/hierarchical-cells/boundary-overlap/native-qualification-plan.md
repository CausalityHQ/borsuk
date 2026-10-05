# Native qualification before the overlap experiment

The original candidate is `65183e2223fff58a5bbfb53a6de452e459ed7429`.
Its startup admission repair is pending. Freeze the repaired source and full
source inventory before launching; do not qualify a moving checkout.

Run the cheapest native correctness stage first: library `overlap_` tests and
the binary paired-seal test. Then run the hierarchical library and returned-SQ8
regressions, release build of `hierarchical_semantic_cells`, workspace Clippy
with correctness and suspicious denied, and the real workspace test compilation
script with `BORSUK_TEST_BUILD_COMMAND` unset. Compile with jobs=1, serial stage
execution and test threads=1. Require every mandatory name exactly once in its
intended stage, with nonzero executed counts and no ignored mandatory tests.

Use a remote worker. The existing On-Demand pilot is pinned to a historical
399-file source and one old test; it is not authority for this candidate.
The existing authenticated minimal-archive qualification lifecycle may be reused
with a fresh source manifest, mode and attempt. No local Cargo. Root must freeze
the selected market, instance, duration, compute/storage caps and source archive
after preflight. No paid launch is authorized by a mock test result alone.

Stop on the first native failure and preserve the original terminal result,
log, source-before/after maps and actual resource/cleanup receipts. A source
repair gets a separate commit and attempt. No automatic replacement or retry.
Terminate and wait for the same owned instance before final collection.

These gates establish native correctness/compilation only. They do not establish
full workspace test execution, recall, cold latency, QPS, 100M feasibility,
incremental lifecycle completeness or competitor parity. Scientific execution
requires exact authenticated real-input admission and a separate infrastructure
canary before the matched pair is frozen and run.
