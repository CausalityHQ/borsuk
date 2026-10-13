**Verdict: HOLD.** Hold the source freeze and the paid ABBA run. The repairs below are small, and since they change source, run the native GREEN roster once, after they land. This was a source-only read of `2f049daf..905c8f41`. I didn't build or run anything, and I didn't touch the worktree.

## What checks out
- **Memory admission order is correct.** `with_byte_window_limits` (`two_bit_generation.rs:1015-1035`) caps the state budget at `max_memory − modeled` before anything is allocated. `modeled` already includes the caller's pinned bytes (`:1517`). `with_waiter_limit` checks the modeled state before reserving the queue, and checks the actual capacity after (`sq8_s3_range.rs:39-63`). `with_shared_byte_window` then re-checks (`:1040-1060`). Nothing is charged twice. The boundary test (refuse at `charged−1`, accept at `charged`, no requests issued) is correct.
- **No deadlock.** A permit covers exactly one range and is held only while that range downloads (`sq8_s3_range.rs:1205-1226`), so nothing holds a permit while waiting for another. Ranges larger than the window are refused before reserving (`:1059`). The wake-up ordering is correct.
- **The config binding is tight.** The runtime-config field set matches the bin's `Config` struct field for field. Run B's config minus the window must equal run A's (`compare_native_replay.rs:4598-4606`). Each run's `config_sha256` is bound to its config. Identity and inputs must be identical across all 4 runs apart from config SHA and window. The 1M inputs are pinned, and IDs, score bits, trace, physical plan and charges must match across runs.
- **The gate arithmetic is correct.** The pairs are A1/B1 and A2/B2. p90 may be at most 105% of A. For throughput, B's total wall time may be at most A's divided by 0.95.

## Findings (most severe first)

**H1: the new reducer drops the transport-failure guards the older reducers have.** The run loop at `compare_native_replay.rs:4701` never checks for zero transport, stream or dropped-body failures, zero `failed_gets`, matching binding charges, or `max_leaf_peak_inflight ≤ 64`. Those checks exist only in the legacy pair reducer (`:4458-4484`) and the membership reducer (`:4926`); the shared read path doesn't enforce them.
- **How it misleads:** if A1 or A2 hits SDK retries or stream failures, its tail latency inflates and both pairs report "p95 improved".
- **Repair:** copy those three guard blocks into the loop.
- **Fixture:** reseal A2 with `transport.after.stream_failures = 1`, following the existing pattern at `:13534`; the reducer must refuse.

**H2: the window may never actually limit anything, and nothing records whether it did.** That makes a misleading scheduling-only result likely.
- There is nothing for the window to share across queries. `ACTIVE_QUERIES = 1` (`check_cohere_native_baseline.rs:1167`), the query loop is serial (`:2145`), and the SOURCE fetch (`two_bit_generation.rs:1900`) and SQ8 fetch (`:2472`) run one after the other. So the "shared FIFO" is really just a cap on bytes in flight within one fetch call.
- The SQ8 cap is 16,773,120 B (bin `:1260`), which is below 16 MiB (16,777,216). So the 16 MiB fixture value can never bind on SQ8. Any window of 64 MiB or more (the `max_source_bytes` cap, `:1263`) does nothing anywhere, yet both the bin and the reducer accept windows up to 512 MiB.
- Per-range trace timing starts only after the permit is granted, so no waiting is ever recorded.
- **Why that matters:** a window that never binds turns the runs into A/A/B/B noise. A sign-only "both pairs improved" gate then passes about 25% of the time by chance. Your own unchanged width64 repeats spread from 185 to 204 ms at p95 (about 10%), while the fixture accepts a 1% improvement.
- **Repair (reducer only; no new limit, no library change):** compute how many queries could have hit the window at all — SQ8 `plan.planned_bytes > max_bytes`, or per-query SOURCE charge bytes `> max_bytes`. Report that count, and how many of those queries sit in A's top-5% tail. If the count is zero, `performance_screen_passed` must be false. Also report the A1-vs-A2 and B1-vs-B2 p95 differences as the noise reference.
- **Fixture:** B with a 64 MiB window must not pass the screen.
- **Before launch, at zero cost:** run the same calculation on the existing frozen width64 rows. SOURCE per-range sizes aren't recorded (only totals), so take the SOURCE range ceiling from the source-bound admission run and record it in the preregistration. Otherwise a window smaller than some query's largest SOURCE range fails partway through the campaign (`sq8_s3_range.rs:1059`).

**M1: older paired modes can now silently accept windowed runs.** `validate_scale_row` now strips the window fields for every scale reader (`compare_native_replay.rs:719-745`). Before this change, the exact field check refused them. The legacy `reduce_paired_scope` modes (width, scheduler and plain paired; `:4274-4420`) take their expected headers from the config and only check that the two arms are equal. So two windowed runs now pass as a historical width or scheduler comparison. `--bind-completed-scale` is not affected, because its reconstructed headers have no window and identity must match exactly.
- **Repair:** one line in the arm loop: require `expected_identity.get("shared_byte_window").is_none()`.
- **Fixture:** a width64 pair where both arms have a window must be refused.

**M2: a window refusal looks like a data-integrity failure.** Both the too-large and too-many-waiters refusals return `RangeFetchError::UnexpectedMetadata` (`sq8_s3_range.rs:94-101`, `:1212-1219`). In the failure row that is indistinguishable from an S3 ETag or size mismatch. The refusal also happens before I/O only for that one call, not for the whole campaign. The run would end INVALID rather than give a false result, but record the distinction in the preregistration.

**Low:**
- **L1:** The `max_waiters ≥ fetch_parallelism` check (bin `:725-733`) is only sufficient because one query runs at a time and fetch calls don't overlap; going over the limit is a hard refusal, not a wait. Write it as `fetch_parallelism * ACTIVE_QUERIES` so it stays true if that changes.
- **L2:** The first query isn't reported, even though the screen text says "p99/first-query retained". Copy the `first_query` block from `:4487`.
- **L3:** No fixture runs with a window that actually makes requests wait. The library test uses 64 MiB on a 32-row fixture, the bin test uses 16 MiB on 257 rows, and both also test a 1-byte window, which simply refuses. The 4-run parity check on real data would catch any divergence (INVALID, not false), so the risk is wasted compute. Smallest fix: a bin fixture where the window equals the largest single range and the call has at least 2 ranges, checked for parity against unset.

## Does the new mode bind enough?
For config identity, yes. To prevent a misleading result, no: it can't tell a window that never binds from a real effect (H2), and it admits runs with transport failures (H1). Run order and same-host execution can't be bound from the rows at all, since they carry no wall clock or instance ID. That must stay with the external campaign receipts, which the report already discloses as `execution_order_verified: false`.

**Minimal path:** fix H1, H2 and M1, plus the L2 one-liner, with the four fixtures above. Then run the native GREEN roster once and do the zero-cost window-feasibility check before any paid compute.

I saved this review to project memory as `byte-window-abba-review-905c8f41`.
