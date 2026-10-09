Using the Ponytail skill's shortest-diff rule. I edited nothing in the repo and launched nothing. The only analysis was bounded streaming of the closed a0001 raw data (CPU 1, 256 MiB, no swap, ≤120 s, 64 KiB line cap), using throwaway scripts in `/tmp/obsplan`.

## Recommendation: cut the body-loop clock reads from 5 to 2 per chunk; keep the procfs sampling

### The change (`crates/borsuk/src/sq8_s3_range.rs:852-880`, `fetch_verified_pages_core`)

Today each chunk reads the clock five times: `waited_from`, the `now` after `next()`, `note_chunk`'s stamp, `copied_from`, and the `now` after the copy. Within a few nanoseconds, these are only two distinct moments: when the chunk arrived, and when the copy ended (which is also when the next wait starts). Each per-chunk clock read now covers two fields:

```rust
let mut collected = BytesMut::with_capacity(expected_len);
let mut mark = probe.as_deref().map(Probe::now_ns); // copy end == next wait start
loop {
    let next = stream.next().await;
    let arrived = probe.as_deref().map(Probe::now_ns);
    if let (Some(p), Some(from), Some(to)) = (probe.as_deref_mut(), mark, arrived) {
        p.span.note_wait(from, to);
    }
    let Some(next) = next else {
        if let (Some(p), Some(ns)) = (probe.as_deref_mut(), arrived) { p.span.eof_ns = Some(ns); }
        break;
    };
    let chunk = next.map_err(RangeFetchError::Store)?;
    if let (Some(p), Some(ns)) = (probe.as_deref_mut(), arrived) { p.span.note_chunk(ns, chunk.len()); }
    /* unchanged overlong check */
    collected.extend_from_slice(&chunk);
    mark = probe.as_deref().map(Probe::now_ns);
    if let (Some(p), Some(from), Some(to)) = (probe.as_deref_mut(), arrived, mark) {
        p.span.note_copy(from, to);
    }
}
```

- **Reads per range:** a successful range goes from `5·chunks + 10` to `2·chunks + 9`.
- **Untraced path:** unchanged, still zero clock reads.
- **Schema marker:** bump `DIAGNOSTIC_SCHEMA` in `check_cohere_native_baseline.rs:46` from v1 to v2, because the interval boundaries move by nanoseconds. That keeps a0001 traces from being pooled with new ones.
- **Other files:** `returned_sq8.rs` is unchanged; its rank-phase clocks are about 130 per query, which is negligible.

**What is kept and what is lost:**
- **Kept:** all 22 range fields (including `max_body_gap_ns` / `max_body_gap_end_ns`, `copy_ns` / `copy_count`, and the headers, EOF and auth boundaries), the rank phases, all host counters, and the monotonic and process-CPU anchors.
- **Lost:** nothing.
- **Changed, at nanosecond scale:**
  - The wait interval now includes the loop-back bookkeeping.
  - The copy interval now includes `note_chunk` and the overlong check.
  - `eof_ns` is now the clock read taken when the stream ends, not a separate later read.

### Why not drop the per-query procfs sampling instead

- **It can't move the failing query-CPU or query-wall metrics.** The procfs samples are taken outside the timed window by construction (`runner.rs:1724` and `:1764` sit outside `:1729`–`:1759`).
- **Its cost is real but shows up only in whole-process CPU:**
  - Each before-sample takes 124 µs of wall time at the median (p90 139 µs, p99 208 µs; 8,192 traced queries).
  - Using the precise `process_cpu_ns` from the terminal records, process-CPU overhead exceeds query-CPU overhead by 0.67 pp for baseline A and 0.50 pp for direct B.
  - Traced query lines are 15.9 KB at the median versus 9.1 KB untraced.
- **It holds the only per-query evidence for the tail hypotheses.** Its per-query deltas of TCPTimeouts, RetransSegs and TCPLossProbes test the ~200 ms TCP retransmit-timeout idea. Its thread `sched_wait_ns` separates waiting for the CPU from waiting off-CPU in the wall-minus-CPU gap. Removing it loses that critical attribution.

### Evidence: what the closed data supports and what is conjecture

**Supported by the closed, authenticated data:**
- **Chunk counts** per traced query: about 1,976 in A and 4,778 in B, at about 8.7 KB per chunk and 27.6 / 30.6 ranges.
- **Clock-read reduction:** B drops from about 24.2k to 9.9k reads per query; A from about 10.1k to 4.2k.
- **Clock source** was `tsc`.
- **In-window CPU deltas** (point estimates): A +0.48 ms per query (+0.83%), B +0.43 ms (+0.53%).
- **The canary design could not have passed whole-OS CPU at any observer cost.**
  - The per-quad ratio standard deviation is about 3.2 pp for both query CPU and process CPU, so 3·se is about 1.7 pp at 32 quads.
  - The 0.04 s truncation allowance adds about 0.4–0.5 pp.
  - So even a zero-cost observer would give a whole-OS upper bound of about 2.05–2.2%, which is over the 2% gate.
- **Most of that noise is per-process, not per-query.** Within one state, a single ordinal's CPU varies by about 4.2–4.8% across runs. If those were independent, the per-quad spread would be about 0.5–0.6 pp, not the observed 3.2 pp.

**Conjecture, not established:**
- That clock reads dominate the in-window delta. Regressing per-ordinal CPU delta on chunk count could not resolve it (B slope −272 ± 231 ns per chunk).
- The cost per clock read. Assuming 20–25 ns, the change would save about 0.36–0.44 pp in B and 0.2–0.25 pp in A.
- Why A's delta exceeds what the clock model predicts. It may be the procfs sample polluting caches for the next query, but that is unproven.
- The retransmit-timeout cause of the tails. It stays a hypothesis.

### Smallest native synthetic oracle (local, deterministic, no timing thresholds)

1. Add a `#[cfg(test)] reads: Cell<u32>` counter to `Probe` and increment it in `Probe::now_ns`.
2. Use the existing tiny HTTP fixture to serve one range, with several chunkings.
3. Assert that reads equal `2·span.chunks + 9` and that `copy_count == chunks`.
4. Assert the boundary order: `metadata ≤ first_chunk ≤ last_chunk ≤ eof ≤ auth_start ≤ auth_end ≤ complete`.
5. Assert `copy_ns + max_body_gap_ns ≤ eof_ns − metadata_ns`.
6. Keep the existing trace-on/off parity tests (IDs, score bits, plans, GETs, bytes) and the `RANGE_FUTURE_ALLOWANCE_BYTES` future-size test. `mark` replaces `waited_from` across the `.await`, so the future size should not change.

Run the same oracle at `516ee0fd`; it must show `5c + 10` there.

### Future falsifiable overhead test (preregister only; not authorized here)

1. **Free local stage.** In one process, interleave traced and untraced runs of the fixture loop, at old and new source. Predict that the per-chunk traced-minus-untraced cost falls to at most 0.5× the old value. If it is above 0.6×, clock reads were not the cost: stop and profile. Devbox numbers are indicative only, not c7i.
2. **Paid stage, needing a separate small runner change and authorization.** Within each process, run each ordinal both traced and untraced in a seeded random order, so the per-process noise cancels. The 2% / 1 ms thresholds and the ×3 multiplier stay the same.
   - **Whole-process CPU:** measure per query cycle with `CLOCK_PROCESS_CPUTIME_ID` in nanoseconds, so the centisecond truncation goes away. This is an estimator change and needs review. Keep call-level OS CPU as a disclosed check that does not gate.
   - **Sizing from resampling the closed untraced data:** at 512 pairs, the CPU 3·sd is about 0.8–1.0 pp. The null wall upper bound is 0.42 ms median / 0.64 ms p90 for B, but 1.19 / 1.81 ms for A.
   - **Proposed size:** about 2,560 pairs for A and 512 for B. That is about 6.1k searches in 8 or fewer processes, versus a0001's 16,384 searches in 256 calls.
   - **Carryover check:** compare untraced-after-traced with untraced-after-untraced. If the lower bound is above 0, the result is INCONCLUSIVE, not GO.

### Risks

- The projected saving rests on an unmeasured cost per clock read; stage 1 exists to falsify it.
- Any source change voids the 516ee0fd / e39b0bfd qualification. That means clippy, `scripts/check_rust_test_build.sh`, requalification and real-input admission again before any paid run.
- A per-ordinal mixed process can hide an overhead that carries into the next query; the carryover check is meant to catch it.