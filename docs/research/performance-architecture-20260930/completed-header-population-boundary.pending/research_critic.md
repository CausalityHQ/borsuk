# research_critic — gpt-6.1-sol

**Verdict: the one-line cap alignment is justified, but remains unverified.** The private ref adds a useful regression test; it does not contain the production fix.

I reviewed exact Git objects, not the dirty checkout. The supplied source copy matches SHA256 `5a65c2eb…f60e04e`. Citations below use `R = 18f36c212f5590ed628dd5a7299ff467b074adaf`, `P = c3e52c8`, `reducer = crates/borsuk/examples/compare_native_replay.rs`, and `runner = crates/borsuk/src/bin/check_cohere_native_baseline.rs`.

**Source findings**

- **Full and panel `trace:false` share the core records.** Both select the same untraced baseline search call. The query record always includes returned hits, plan, stages, charges, transport and `TwoBitPlanTrace`; `trace:false` disables the additional range/host diagnostics, not that existing plan trace. Panel rows additionally contain `selected_slot`. Thus the source supports comparable per-query payload structure, not byte-identical output. (`P:runner:1995–1999, 2129–2138, 2273–2333`)
- **The size estimate is plausible, not an admission proof.** Panel serialization uses a 64 KiB whole-line buffer; Full streams with a 256 KiB line cap. Panels also have additional admission metadata and memory accounting. The first 32 ordinals do not bound the remaining query records. The supplied [reduced report](/data/target/borsuk-full1000-rust-replay-a0003.pending/s3-completed-reduction-collected/completed-reduction.json:1) authenticates 1,146,181 bytes for a diagnostic panel, with null statistics; it establishes neither full-result size nor population performance. (`P:runner:36–50, 1683–1692, 1985–2031`)
- **`HEADER_RESULT_CAP = FILE_CAP` is the smallest coherent correction.** The producer already caps output at 64 MiB, and the reducer admits files through 64 MiB. The binder’s separate 32 MiB limit rejects artifacts that those surrounding stages can admit. Aligning that constant removes the mismatch without changing ANN execution or record layout. (`P:runner:37, 1637–1647`; `R:reducer:43, 1352–1368, 1940`)

The existing incremental-hash design already separates authentication from semantic validation. Truncating records, suppressing traces, compressing artifacts or introducing another configurable limit would add changes unnecessary for this boundary repair.

**Concrete blockers**

1. **The candidate is still test-only.** At `R:reducer:2458`, the exact 64 MiB fixture must fail because `authenticated_headers` first applies the unchanged 32 MiB pin cap. That is the intended red condition, not evidence that a fix works. The green candidate needs only the constant alias at line 1940.
2. **Native compilation and execution are outstanding.** Source inspection found no apparent compile defect: imports and dependency declarations exist, the size arithmetic is bounded, and `HeaderSnapshot` implements `Debug` for `unwrap_err`. These observations cannot substitute for EC2 compiler/test receipts. (`R:reducer:24–38, 2011–2016, 2433–2469`)

**Bounds and test limitations**

The proposed alias preserves all inspected checks: positive bounded pins, actual-length equality, 8 KiB streaming reads, checked byte totals, whole-body SHA/EOF authentication, two bounded newline-terminated headers, duplicate-key rejection, regular-file enforcement, descriptor/path identity checks and single-link checks. Header parsing occurs only after whole-body authentication. The reducer independently retains row, seal, recall-order and terminal validation. (`R:reducer:1282–1339, 2003–2008, 2056–2107, 1375–1417, 1703–1776`)

The new test correctly writes and hashes the entire opaque 64 MiB artifact incrementally. After the cap fix, its valid case and wrong-SHA case exercise authentication at the boundary. It provides **binder evidence only**: two generic JSON headers and an opaque tail are not valid completed-result evidence. (`R:reducer:2433–2467`)

Its final assertion rejects a **declared pin** of 64 MiB + 1; the physical file remains 64 MiB. That is a coverage limitation, not a demonstrated bounds bug. If claiming physical oversized-file coverage for the binder, add a narrow EC2 case through `authenticated_headers`; an existing physical oversized-file test covers the reducer instead. (`R:reducer:2468–2469, 8047–8049`)

Raising the admitted byte limit also increases worst-case hashing work. Streaming scratch stays bounded, but neither elapsed time nor cgroup peak memory has been measured for this case.

**Feasible next native gate**

Use a separate compiler-enabled, bounded `causality` EC2 gate, with new interruptible capacity on Spot. Leave `i-09badae56dde771e2`, its compiler-false canary, existing ELFs and four workers unchanged.

1. Compile the affected example on the exact test-only revision:
   ```bash
   CARGO_BUILD_JOBS=2 cargo test --locked -p borsuk --example compare_native_replay --no-run
   ```
2. Run the exact boundary test once and retain the expected failure at **“bounded header artifact pin”**:
   ```bash
   CARGO_BUILD_JOBS=2 cargo test --locked -p borsuk --example compare_native_replay completed_header_binding_tests::bind_completed_scale_authenticates_population_sized_result_tail -- --exact --test-threads=1
   ```
3. Freeze the one-line alias candidate and rerun that test for green. Then run the existing header-binding tests and `tests::actual_native_scale_runner_reducer_seal_count_mismatches`, which executes the native fixture and checks reducer rejection boundaries. (`R:reducer:5976–6068`)
4. On that exact candidate, pass the required workspace Clippy command and `bash scripts/check_rust_test_build.sh`, without the script’s test-only command override. Record revision, source SHA, commands, native exits, resource receipts and instance identity; collect terminal evidence and terminate compute.

This qualifies a reducer admission repair. Full1000 completion, actual result size, population quality and performance still require their separate authenticated native evidence. No files, native binaries, workers or infrastructure were changed.
