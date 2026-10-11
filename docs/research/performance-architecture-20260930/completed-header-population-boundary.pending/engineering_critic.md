# engineering_critic — gpt-6-astra

The proposed one-line cap change is appropriate for native qualification. I found no additional correctness or security blocker in it. **The private ref is deliberately red and is not ready for delivery.**

I verified that `18f36c2` adds exactly 38 test lines and that its source SHA matches `5a65c2eb…e04e`. References below use exact Git objects, not the dirty checkout:

- **R:** `18f36c2:crates/borsuk/examples/compare_native_replay.rs`
- **B:** `c3e52c8:crates/borsuk/src/bin/check_cohere_native_baseline.rs`

**Concrete release blocker**

1. **The binder still rejects otherwise admissible results above 32 MiB.** `HEADER_RESULT_CAP` remains 32 MiB, and `authenticated_headers` applies it before reading the file (**R:1940, 2003–2008, 2065–2069**). The reducer admits 64 MiB (**R:43, 1353–1358**); the pinned producer also caps total output at 64 MiB (**B:36–38**). Consequently, the new test should fail at its first authentication unwrap (**R:2458**), with `bounded header artifact pin`.

   The smallest fix is `const HEADER_RESULT_CAP: u64 = FILE_CAP;`. Delivery still requires native red/green evidence on that exact change. The supplied 32-query report does **not** establish that a completed full1000 artifact actually exceeds 32 MiB.

**Source and test assessment**

- **Full and untraced panels retain the same core query payload.** Both select the same untraced baseline search call (**B:1995–1999, 2129–2138**), and both serialize the plan, `TwoBitPlanTrace`, transport, returned hits and charges (**B:2273–2333**). `trace:false` disables optional range/host diagnostics; it does not remove `TwoBitPlanTrace`. Panels additionally emit `selected_slot` and an admission row (**B:2000–2031, 2331–2332**). Thus, the size extrapolation is plausible, but the records are not byte-identical or necessarily equal-sized across queries.

- **The larger cap preserves bounded authentication.** Header buffers remain capped at 65,536 bytes each, excluding newline; the whole artifact is hashed in 8 KiB blocks. Pinned length, growth, whole-body SHA, EOF, descriptor/path identity, single-link and no-symlink checks remain intact (**R:1303–1339, 1995–2008, 2056–2115**). Independent header comparison and downstream schema/seal validation remain separate (**R:2326–2345, 1396–1404, 1702–1776**). No schema change or compatibility layer is warranted.

- **The regression looks type-correct by inspection.** Required imports exist (**R:29–35**), and `HeaderSnapshot` derives `Debug`, satisfying `unwrap_err`’s success-type requirement (**R:2011–2016**). It constructs exactly 64 MiB including both headers, hashes incrementally, closes the writer, verifies authentication, then checks SHA rejection (**R:2438–2466**). There is no whole-file allocation. Compilation remains unverified.

**Limitations**

The final assertion tests a **64 MiB + 1 declared pin**, not a physically oversized file (**R:2468–2469**). That covers the admission guard; physical length equality is enforced separately. The opaque tail proves authentication behavior only: it supplies neither valid completed rows nor quality evidence.

The safe report confirms `result_bytes=1146181`, 32 diagnostic queries and null population statistics. Its extrapolation cannot establish full1000 size, recall, latency, QPS or qualification. The reducer explicitly withholds population statistics outside completed full1000 execution (**R:3510–3520**).

**Next EC2 gate**

Use a separate, bounded, compiler-enabled `causality` EC2 environment; preserve the existing compiler-disabled canary.

1. Compile the private ref’s example test target with `--locked --no-run`.
2. Run exactly one selected test and record the expected admission failure:
   ```bash
   cargo test --locked -p borsuk --example compare_native_replay \
     completed_header_binding_tests::bind_completed_scale_authenticates_population_sized_result_tail \
     -- --exact --test-threads=1
   ```
3. Freeze a candidate containing only the cap assignment plus this test. Repeat that exact test; require one test executed and passed. Then run the example suite.
4. On the frozen candidate, pass workspace Clippy and `bash scripts/check_rust_test_build.sh`; ensure `BORSUK_TEST_BUILD_COMMAND` is unset because that script permits mock overrides at lines 13–20. Record source identities, enforced limits and terminal exit statuses.

**Verdict:** proceed to the native gate; hold delivery pending green evidence. No files, raw result rows, native tests, network resources, workers or canary state were changed.
