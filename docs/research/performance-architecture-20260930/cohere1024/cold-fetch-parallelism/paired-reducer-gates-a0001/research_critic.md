## Critique of f0a717f3 (`compare_native_replay.rs`, SHA 823d174a…, parent f4542d84)

Overall: the reducer reads the d8e64dbc producer's output correctly, and the parity, hashing, seal and create-only writer logic is sound. There is one code blocker and one procedural blocker. Everything else is either about how results get interpreted or is optional. I made no edits to the repo and ran no builds, native code or data; the four new tests remain unrun.

The contract file on disk was rewritten while I worked. It now names **e976ceb6** ("admit only the frozen production panel"), and that commit appears to fix B1 below. I only skimmed it. Every other finding applies to e976ceb6 unchanged, because its diff doesn't touch those lines.

### Checked and fine
- **Producer schema matches d8e64dbc:**
  - The `identity` row is the old field list plus `fetch_parallelism`. The producer writes `null` there if config fails, and the reducer rejects `null`.
  - `bound_inputs` adds `fetch_parallelism` plus a hard-coded `"source_cache":"off"`. That string is accurate for this runner: it never calls `with_source_cache`, and the default is `None`.
  - In the query `Record`, the fields end with `…trace, transport`.
  - `QueryStages`, `TwoBitPlanTrace` and `NativeTransportStats` are identical between f4542d84 and d8e64dbc.
- **The trace hash (line 1067) is canonical:**
  - Cargo.lock's serde_json has no `indexmap`, so there is no `preserve_order` and objects are key-sorted.
  - Arrays keep their order, and the validator forces every number to be a u64.
  - Duplicate keys are rejected by `UniqueJson` before hashing.
- **Parity won't fail by design:** the leaf, source and SQ8 fetches all use ordered `.buffered(width.min(n))` (d8e64dbc `two_bit_generation.rs:1703`, `sq8_s3_range.rs:565`). The producer's own Fresh1m test asserts the trace and charges match at width 16 and 32.
- **The zero-failure transport rule is realistic:** the retained `native-s3-sealed-reduction/gates-a0001` run had 67,961 attempts with 0 transport failures, 0 stream failures and 0 dropped error bodies. `validate()` ties `dropped_error_bodies` to the count of all non-2xx responses, so `==0` also rejects runs where a 503 was retried successfully.
- **No panic paths:** `IndexMut` on the expected rows only runs after `["fetch_parallelism"] == 16|32` proves they are objects. `[T;2]` rejects a third arm. `32.0` and `"32"` are rejected.
- **The pooled golden values in the test check out by hand:** p50 667, p90 1600, p95 1800, p99 1960 ms, QPS 1.332001332.

### Blockers
**B1 (code; fixed in e976ceb6): paired mode accepts any matching panel.**
- `validate_v2_inputs` (lines 351–367) only checks rows ≥ 10 and dimensions in 1..=1024. Dataset, revision, profile and input SHAs can be anything.
- `reduce_paired` (1353–1361) only checks the 16/32 selector.
- The positive test `paired_v2_sealed_native_rows_statistics_and_create_only` proves this itself: it returns `MEASURED` for a synthetic 32-row, 16-dimension `synthetic/native-fixture` panel. That breaks the contract's "fixed 100k D1024 / k10 / 1000" requirement.
- Fix: pin the production `Inputs` before opening either file, as e976ceb6 does.

**B2 (procedural; open in both commits): the pins are only checked against each other, so they can be circular.**
- What's not pinned to a fixed value: `binary_sha256`, the five component source SHAs, `generation_root_sha256`, and the backend `bucket`, `region`, `physical_prefix` and `sq8_etag`.
- How they're checked: only that they look like SHAs and that both arms agree.
- The values come from the reduction config. If those rows are copied from the output files, the "exact pin" proves nothing.
- `sq8_s3_range.rs` (the SQ8 fetch-width code and the transport counters) is not one of the five hashed components; only `binary_sha256` covers it.
- Smallest fix, no code: before reducing, the root derives the expected values from independent receipts and records the comparison:
  - Component SHAs at d8e64dbc (recompute if the frozen measured revision is different):
    - runner `de6a9190…b233`
    - generation `bc0bc1b2…6c1c`
    - router `b9abd271…d0c2`
    - codec `eddf88c6…38ec`
    - source_plane `dbcc4cdb…42e0`
  - Binary SHA from the release-build receipt.
  - Generation root and `sq8_etag` from the S3 publication receipt.
- Falsifier: a pair of outputs from any other matching binary is reported `MEASURED` today.

### How to read the results (decision record, no code needed)
- **H1 – the 16-vs-32 difference is confounded with order and host.** There is one run per arm, in a fixed order. Nothing in the rows binds the host, instance, AZ or how close in time the runs were; they carry no wall-clock timestamps. The second arm reads the identical S3 objects (same prefix and etag), so it may benefit from warmth. The historical A1/B1/B2/A2 design existed to prevent exactly this. Run a counterbalanced BA replicate before drawing any conclusion about fetch width.
- **H2 – a built-in A/A control.** `native100k` selects at most 16 leaves (`semantic_unit_router.rs:60`), so router/discovery fetch concurrency is the same in both arms.
  - Any shift in the discovery stage between arms therefore measures run-to-run drift.
  - It also means `leaf_peak_inflight` will be ≤16 in both arms. The reducer has no runtime evidence that width 32 took effect anywhere, because source and SQ8 in-flight counts aren't emitted.
  - The fixture's `leaf_peak_inflight=32` (line 1994) can't occur with this profile.
- **H3 – `pooled_both_arms` mixes two treatments.** Its p95 isn't the p95 of either configuration; never quote it as BORSUK's p95.
- **H4 – "plan parity" is narrower than the contract wording.** It means `TwoBitPlanTrace` plus per-component GETs and bytes. The SQ8 range list and the source page list aren't in the trace, so their parity is inferred. The report's `plan_scope` field says this accurately; the contract's "full ordered plan" overstates it.

### Optional (cheap, not blocking)
1. **Test the other three failure conditions (lines 1400–1406).** Only `stream_failures` is tested (line 2238), so deleting any other condition leaves the suite green. Add cases for:
   - `transport_failures`.
   - `dropped_error_bodies`, with a matching non-2xx status count.
   - The same `failed_gets > 0` in **both** arms. Parity then passes, so only this check rejects it.
2. **Run the transport check before the parity loop (1388–1398).** Then a failed-transport pair reports the real cause instead of "paired query N mismatch".
3. **Relabel the nested arm reports (1416).** They still say `"schema":"borsuk-completed-native-reduction-v1","status":"MEASURED"` but have no `config_*` fields, so a schema-keyed consumer could count them as standalone completed reductions.
4. **Add two outputs that use the pairing.** Charges are identical across arms, so one number per component says whether width 32 could matter at all: how many queries had more than 16 GETs. Per-query wall-time difference quantiles would also use the pairing that comparing each arm's p95 throws away.
5. **Compare startup metadata counters across arms.** Logical GET/HEAD counts, bytes and `reused_root_bytes` would expose a warm local-root difference between arms.
6. **Check `leaf_peak_inflight <= fetch_parallelism` on every query.** It's a one-line consistency check on the selector.

I saved this review to memory (`paired-v2-reducer-review-f0a717f3`).
