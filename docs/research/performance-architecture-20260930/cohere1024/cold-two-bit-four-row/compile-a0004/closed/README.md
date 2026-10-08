# Four-row compile attempt a0004: native correctness passed, timing pending

Source `3138bfb0502c986d187e4098718f8b3945fd39c9`, frozen protocol `88bccb23c4f969fbd441ad24c8e68d41321bc19f`, original Spot instance `i-0036b3a365147ea21`. The original watcher collected the terminal, terminated and waited for this exact instance. An independent EC2 describe confirmed `terminated`.

All seven serial stages exited zero: release probe compilation; codec and planner debug; codec and planner release; affected integration tests; and codec scalar-control in a separate target directory. The four codec and eight planner mandatory names each passed once in their owning debug/release stages. The full 415 native and 2454 support file maps are unchanged before/after execution. All 96 evidence artifacts were independently authenticated. Swap and OOM counters are zero, the global scope drained, and the release ELF hash is unchanged before/after tests.

This is compiler and synthetic correctness evidence. Workspace Clippy and the real unshimmed workspace test-build gate were not run in this attempt. Production integration and timing are not authorized by these results alone.

The authenticated release libtest is 88,769,888 bytes, SHA-256 `b0442b3bd294685ec0be3b0615447e998893b3fccb6c35d11a3b5ee3d895c25d`. Independent local static disassembly matches the authenticated remote bytes. The 695-byte `PreparedTwoBit::score_four` kernel has two `addpd` instructions per packed byte: one independent lane pair for rows 0/1 and one for rows 2/3. Both pairs start from literal negative zero, each lane advances through bytes in original order, and scalar finalizers preserve row/error order. No FMA or horizontal arithmetic is present. The standalone successful kernel frame is 80 bytes, with no dynamic stack realignment.

The actual production serving caller/kernel stack, including caller spills and realignment, is still unqualified. A libtest kernel frame alone does not prove the production incremental 512-byte allowance. Retained control production binaries have authenticated source/bytes, but the old gate did not retain its compiler environment. Build a matched-profile control and candidate serving ELF under one explicitly captured toolchain/flag environment before granting the stack gate. The primitive remains unrun; packed instructions are not a speedup.

## Cold runner admission defect and prospective correction

The exact frozen source's `check_cohere_native_baseline::scratch_bytes` admits 532,480 codec bytes plus `TwoBitPlanTrace::scratch_bytes(rows)`. Its diagnostic search subtracts the trace allowance before calling `four_row_scoring_available`, leaving only 532,480. The batch guard requires codec minimum plus 512, so this runner would always select scalar fallback. This is an experiment configuration/admission defect, not an algorithm rejection.

Preserve this attempt and every historical cold arm. A separate source slice will add the fixed 512-byte batch allowance to the benchmark runner, report codec/batch/trace/total separately, and strengthen existing native tests to observe actual paged dispatch at the minimum, minimum+511 and minimum+512. The dispatch observer belongs only in the cfg(test) planner boundary, outside the ignored primitive's timed scorer and outside production builds. No cache, query, truth, population, memory, physical GET or byte threshold changes are authorized. The generic library remains dataset-independent.

After that slice: qualify its actual native runner and both affected modules, retained production release binaries, workspace correctness/suspicious Clippy, real unshimmed test compilation and scalar-control. Audit the actual production stack before primitive timing. A primitive survivor then needs exact real-input runtime admission and staging/canary before paired cold measurement. Reuse the saved matched S3 reference; no new S3 service benchmark or Turbopuffer service run is planned.

## Immutable remote evidence

Bucket `borsuk-bench-453182569524-euc1`, prefix `research/semantic-router/20261008/cold-two-bit-four-row-compile-a0004`:

- `terminal.json`, `artifacts.sha256`, `evidence.tar.gz` (2,509,658 bytes; SHA-256 `e579d047f433786337a1c5e5cbeb56ba14c132745e5d14caeac5dce9d4aadfb4`).
- `supplemental/probe-libtest`, bound to the release ELF pin above.

The accompanying independent verifier and kernel audit are bounded CPU1/256MiB/no-swap static checks, not local Rust execution. No primitive, ANN speed, recall improvement, cold latency improvement or product-readiness claim follows from a0004.
