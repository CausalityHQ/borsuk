I found no required repairs in `b82e524c`: no blocking compiler, API, ordering, future-RAM or diagnostic-boundary defect. It is ready to go to remote qualification, but it stays **UNVERIFIED** until that passes. Nothing was compiled or run; everything below comes from reading the committed objects with `git show`.

## What I confirmed
- **Identity:** both owned files match the contract's SHA-256 values. The diff touches only those two files. The parent `c31c5162` is `origin/main`, so integration would be a fast-forward.
- **Clock counts, traced by hand through `sq8_s3_range.rs:853-883`:**
  - A successful range reads the clock 4 times before the body, once for the initial mark, twice per chunk, once at EOF, twice for auth and once at completion: **2c+9**.
  - Failure paths match the test table: overlong = 8 (this includes the clock read at `:875`, which already existed and whose value is thrown away), chunk-then-stream-error = 9, empty body = 9, truncated or auth failure = 11.
  - The old code reads **5c+10**, so the test tells the two versions apart (one coalesced chunk gives 15 on the old code, not 11).
- **Untraced path:** it runs the same branches with `None` everywhere.
  - `mark` takes the place of `waited_from` as the only `Option<u64>` held across the `.await`, and `arrived` is dead before the next await.
  - Error and overlong ordering is unchanged, as are bytes, auth, GETs, polling and the copy itself.
  - The previously recorded layout was `production_future=576 body_future=448`, against 1,760 bytes of headroom.
- **Interval accounting:** the intervals now run back to back (mark, arrival 1, copy end 1, …, EOF arrival). So `copy_ns + max_gap ≤ eof − metadata` holds by construction. The downstream ordering checks (`check_cohere_native_baseline.rs:4528-4541`, `two_bit_generation.rs:8912-8925`) use `<=`, so timestamps that are now equal cannot break them.
- **API shape:**
  - `object_store` is pinned at 0.14.1. `ChunkStore` implements exactly the same methods as `SqFaultStore` (`two_bit_generation.rs:8247`), which already compiles in this crate. `ObjectStoreExt` is imported, so `put` resolves, and `Error::Generic` is built the same way as there.
  - `#[tokio::test]` defaults to the current-thread runtime and polls the top-level future on the test thread, so the thread-local counter sees every read. The counter is reset before each measurement, and an undercount fails the equality check rather than passing.
  - `let span = &probe.unwrap().span;` is fine because Rust extends the temporary's lifetime through the field access.
  - Release builds, integration tests and the bin's tests compile without `cfg(test)`, so the counter does not exist there.

## Optional fixes (none block integration)
1. **The "disabled path reads zero clocks" check proves nothing** (`:2012`). `Probe::now_ns` cannot run without a probe, so the check could never fail.
   - Falsifier: add a stray `Instant::now()` or `ns_since` call to the untraced loop and the test still passes.
   - Fix: either describe it as "zero `Probe::now_ns` calls" or move the counter into `ns_since` (`:923`). The second option keeps the same 2c+9 counts and also catches clock reads that go through `ns_since`.
   - Either way, the real zero-clock guarantee comes from the code's structure: every read is gated behind `probe.as_deref()` or `stamp`.
2. **The real-HTTP test probably delivers only one chunk.** It sends a 221-byte body in one write and only asserts `chunks >= 1` (`:2091`).
   - So the "real multi-chunk fixture" claim in `selection.json` overstates what was tested. Multi-chunk coverage comes only from the scripted `ChunkStore` streams.
   - Fix: reword the claim, or use the existing `response_byte_delay` option of `http_fixture_with_copy_response` and assert `chunks > 1`.
3. **One doc line is incomplete.** The doc on `max_body_gap_ns` (`:994-997`) says a gap ends at the next frame or EOF. It can also end at a stream error item (the eof-error case).
4. **Nothing yet tests that a single clock reading is reused.** A cheap check: assert that `max_body_gap_end_ns` equals one of `first_chunk_ns`, `last_chunk_ns` or `eof_ns`. In v2 that equality is exact, so it tests the reuse itself, not just the count.
5. **The negative control from the plan was dropped.** The plan said to run the counting test against the old loop and see it fail; `remote_commands` does not include that. It is an optional mutation check.

Not worth doing: removing the discarded overlong clock read at `:875` (failure path only, already in the contract's count of 8), sharing the `ChunkStore` boilerplate across test modules, or renaming `copy_ns`.

## Does the public trace need its own timing-semantics marker?
No. `Sq8RangeTrace` carries `range_fields` but no schema of its own, and its only serializer is the runner (`check_cohere_native_baseline.rs:1189-1206`), which now writes diagnostic schema v2. `two_bit_generation` only fills and reads the trace. Nothing else in the tree, including any reducer or Python script, refers to the schema string, `copy_ns` or `max_body_gap`. The 22 column names and their order are unchanged. Under the pre-release policy, marking the one artifact boundary is enough. Add a library-level marker only when a second serializer appears.

## Is this a valid distinct causal candidate?
It is a valid, distinct candidate, but nothing in the closed data shows it causes any measured effect.

- **Distinct:** it changes only the traced run's per-chunk work inside the timed window. The procfs sampling (outside the window) and the whole-process noise are untouched. Two reads per chunk is the minimum if gap and copy stay separate fields, so no further reduction is possible without dropping a field.
- **What is verified:** a reduction in clock reads per query, from about 10.1k to 4.2k for A and from about 24.2k to 9.9k for B. This agrees with `operation-counts.json` (about 1,976 and 4,778 chunks per query).
- **Not supported causally**, using the plan's own numbers:
  - The in-window deltas were A +0.83% and B +0.53%, against a standard error of about 0.57 pp (3.2 pp ÷ √32). That is roughly 1.5 and 0.9 standard errors, so neither is distinguishable from zero.
  - Scaling with chunk count goes the wrong way: B has 2.4× the chunks but a smaller delta. The fitted slope is −272 ± 231 ns per chunk, where five reads at 20–25 ns would predict about +100 to +125 ns per chunk.
  - The predicted saving on the recorded `tsc` clock source is about 0.2–0.25 pp for A and 0.36–0.44 pp for B. That is far below the 3·se of about 1.7 pp.

**What cannot be inferred:**
- That any time or CPU was saved; only the read counts are established.
- That clock reads caused the a0001 deltas or its INCONCLUSIVE result. That result was limited by noise: even a zero-cost observer would have an upper bound of 2.05–2.2%, above the 2% gate.
- Anything from rerunning the frozen a0001 design on v2. Under that design a PASS or a FAIL is decided by noise, so neither would tell you whether the clock reuse worked. I'm not proposing a new design; that is deferred to the separate preregistration `selection.json` already calls for.
- Any v1-to-v2 comparison of `copy_ns`, `max_body_gap_ns`, `eof_ns`, or untraced latency. These are different frozen systems, and the loop's compiled code changed in both the traced and untraced runs.
- Anything about the ~200 ms p99 tail or its TCP cause. A sub-millisecond per-query saving cannot produce those events.
- That v2 `copy_ns` measures memcpy. It now includes observer bookkeeping, as its documentation says.

One gain: because gap ends and chunk/EOF timestamps now come from the same reading, you can tell exactly which chunk or EOF a gap ended at.

On prior art: TSC-based timers such as quanta or minstant cut the cost per read, but they add a dependency and calibration semantics. Sampling every Nth chunk or dropping per-chunk clocks would change or remove fields. Reusing readings is the minimal option that needs no dependency.

## Smallest falsifier
- **Source change:** remote `cargo test --locked -p borsuk --lib sq8_s3_range::tests::traced_body_clock_reuse_counts_chunks_and_failure_boundaries -- --exact --test-threads=1`, plus the old-loop negative control from optional fix 5.
- **Causal claim:** none exists in the closed data; it waits for a separately preregistered protocol.

I also saved this verdict to project memory as `sq8-clock-reuse-review-b82e524c`; nothing in the repo was edited.
