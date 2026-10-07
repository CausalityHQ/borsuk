# Native replay reducer research review

Source: db71baed69ec6c5b7e0389fb8e8ab760f0232db3. Group: c368b33868b14096. Consultation: 632683eec0cd4da4. Read-only; native execution unverified.

I don't think this reducer is ready to release yet. The parser, authentication, quantiles and thresholds are correct, but the reducer never checks which binary produced which run. A swapped or same-binary set of four outputs can therefore be reported as a measured pass.

Everything below comes from reading the code and committed artifacts. Nothing was compiled, tested or run, and no job, file, network call or agent was touched. Line numbers refer to `crates/borsuk/examples/compare_native_replay.rs` at `db71baed`.

## Engineering findings

**E1 (High): A and B binaries are never checked.** At `:693-696` the reducer only requires A1 = A2 and B1 = B2.
- **Why it matters:** the B build changes only `Cargo.toml`, `exact_sq8_runtime_probe.rs` and `exact_sq8_nominee.rs` (`exact-sq8-runtime-gates/a0001/workspace-receipt.json`, `candidate_delta_paths`). None of those files are hashed in the native identity row (`check_cohere_native_baseline.rs:941-949`). The five source hashes at the base commit match A's emitted identity exactly (15f06f8b…, 70a1e695…, b9abd271…, 0f51015f…, dbcc4cdb…). So `binary_sha256` is the only field that tells A from B, and nothing checks it.
- **Failure 1:** four outputs from the same binary get perfect parity, and the timing gate can pass on noise.
- **Failure 2:** if B outputs are passed in the A slots, the comparison runs in reverse. A blocked scorer that is at least 5% slower is then reported as `TIMING_GATE_PASSED`.
- **Fix:** add constants for the two binary SHAs the method preregisters (`ce43842c…` for A, `3911839b…` for B). Require runs 0 and 3 to carry the A binary and runs 1 and 2 the B binary, and give the test fixture these binary values.

**E2 (Medium): the query and truth SHAs are not bound.** At `:111-128` the reducer pins dataset, geometry and byte sizes, but not `requests_sha256 == 8460a81f…` or `truth_sha256 == 479064…`. The native `validate_config` (`check_cohere_native_baseline.rs:192-247`) doesn't pin them either.
- **Failure:** four consistent runs over a different 1000-query panel of the same size are reported as `MEASURED`.
- **Fix:** add the two equalities and update the fixture's placeholder values.

**E3 (Low): the tests miss mistakes that would change the result.**
- **p90 vs p95:** no test separates the two. `p95_ns` is never asserted (the golden test at `:945` checks only the `*_ms` values), and every comparison fixture is linear, so p90 and p95 pass or fail together. Writing `p95_ns: percentile(90)` or copying the p90 condition into `p95_pass` would pass every test.
  - Fix: assert `p90_ns`/`p95_ns` in the golden test.
  - Add one case where p95 fails and p90 passes: use B at 900,000 ns per ordinal, set ordinals 949..=999 to 1 s, and adjust the terminal sums.
- **QPS boundary:** an incorrect `b*100 <= a*95` (+5.26%) passes every test, just like the correct `b*105 <= a*100` at `:641-642`.
  - Fix: build A1/A2 at 1,050,000 ns per ordinal and B at 900,000 ns, then raise B's last query to 50,950,000,000 ns.
  - B's wall time × 105 then equals A's × 100 exactly, so the QPS gate passes. One extra nanosecond must fail it, while p90/p95 pass in both cases.
- **Same-arm identity check:** deleting `:693-696` passes every test. Add a case where A2 carries a different binary.
- **Ranking order:** every fixture score is the same 1.0f32, so the score-ordering check at `:495-503` is never exercised with distinct values. Add ascending distinct scores plus one case with a single descending pair.

## Research and methodology findings

**R1 (Medium): range parity is not checked.** Method step 3 requires the same "logical GET/range/byte accounting". The reducer compares GET, byte and failure counts per stage, but skips the per-query `trace`.
- The trace is the plan fingerprint. Its fields are page and unit lists with no timings (`two_bit_generation.rs:491-505`), including `ranked_candidate_pages`.
- `trace` is the last field the runner writes (`check_cohere_native_baseline.rs:744-758`).
- **Fix:** SHA-256 the raw bytes from the first `,"trace":` to the end of each query line and require equality against A1. This needs no trace parsing.

**R2 (Low): pooled per-stage times are missing.** Method step 4 asks for per-stage times in each arm's pooled 2000 queries. `pooled` at `:702-727` reports only latency statistics; stage sums exist per run only. Fix: add the A1+A2 and B1+B2 stage sums to `pooled`.

**R3 (no code change; root's job): some requirements can't be proven by the reducer.** It cannot prove serial A1→B1→B2→A2 order, same instance, the new root/head pin, cgroup memory, swap/OOM, page cache or cost. The closeout must cross-check `runs[i].result_sha256` and `inputs.generation_root_sha256` against the supervisor and publisher receipts. The labels the reducer already writes are correct and sufficient for that: local-file only, `physical_s3: false`, `shared_OS_page_cache_uncontrolled`, external resource/cost gate required, `vendor_or_scientific_win_claim: false`, and `COMPLETED_NO_WIN` versus `TIMING_GATE_PASSED_EXTERNAL_GATES_REQUIRED`.

## What checks out (static evidence only)

- **Real output accepted:** I checked the committed real A output (`native-preflight-spot/a0002/preflight/baseline-result.jsonl`, 2005 rows, binary `ce43842c`) against every check the reducer applies:
  - The prefix of the first 1003 rows is 10215813 bytes with SHA `3f0ea7b6…`, and the sealed 1004 rows are 10216236 bytes with SHA `c211f907…`. Both match the seal and terminal rows.
  - Ordinals 0–999 are in order, every query has 10 results and no underfill, every stage ends within its query's wall time, and per-query charge sums match.
  - Ranking order holds, including 2 tied scores broken by ID.
  - All 1000 recall values equal hits/10, and the wall, CPU, hit (9723), GET (67943) and byte totals equal the terminal row.
  - The longest line is 12,665 bytes, under the 256 KiB cap.
- **Field types:** the native `u128`/`i128` timings fit `u64`, and a missing peak-memory value parses as null. The two stage types have exactly the fields the reducer's strict parsers expect, and `native100k` matches the profile's snake_case name.
- **Float equality:** exact `==` on recall10 and mean recall is safe. Parsing values like `0.9723` is a single correctly rounded division, which reproduces the native's `hits/10000`.
- **Quantiles:** the nearest-rank index is ceil(pN/100) − 1, so p90/p95 are elements 899/949 for 1000 samples and 1799/1899 for 2000. The same rule is used in `native_ann_100k_qualify.rs:289-290`.
- **Thresholds and pairing:** the integer 5% p90/p95/QPS thresholds are right, and both comparisons pair correctly (B1 vs A1 as `runs[0]`,`runs[1]`; B2 vs A2 as `runs[3]`,`runs[2]`).
- **Rejection paths:** the code rejects duplicate fields, extra or missing results, truncation, trailing data, a second terminal row, an `INVALID` terminal, and a `query_failure` row.
- **File handling:**
  - Paths are opened without following symlinks.
  - File identity is re-checked at the end.
  - Duplicate inputs are rejected by SHA and by inode.
  - The report is created exclusively and never overwritten.
  - It is capped at 2 MiB, and both the file and its parent directory are synced.
- **Build wiring:** `rustix` 1.1.4 provides `mkfifoat`, and `tempfile` is a normal dependency. `scripts/check_rust_test_build.sh` uses `--all-targets`, so the example's tests compile there. They only run with an explicit `cargo test --example compare_native_replay`.

Not established: Rust compilation, Clippy, test results, behaviour on a real B binary's output, and any performance or resource result. Those stay with the root.

