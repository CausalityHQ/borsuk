## Closed-trace metadata startup audit

2026-10-01. Original read-only specialist consultation d96f0dbef57a42da completed exit0, gpt-6.1-sol/high, elapsed 273s; collected once. Evidence revision e1c9b445 and CLOSED a0004 only. No new native/cloud measurement. Root accepts this as a subsequent one-causal-change candidate; first repair offered resource sampling and retain a0001 FAIL. Native implementation is not yet authorized to change the frozen measurement source.

**Remove five redundant metadata HEADs in the shared stager; keep metadata reads sequential.** This uses existing authenticated lengths, needs no cache or format change, and preserves admission before payload GET submission.

HEAD is `e1c9b445`; the inspected implementation and checker files have no working-tree diff. Analysis used only the closed a0004 screen artifacts and relevant source/callers. No edits, builds, cloud calls, ANN queries, or other workers were started; live a0001 was untouched.

The closed records contain 128 semantic startups per dataset: two repetitions of the same 64 development queries. Per-object median intervals below are **HEAD / GET headers / stream-and-output / writes**, in milliseconds. Writes are already included in stream time.

| Metadata object | Bytes, ReLAION / CoHere | ReLAION intervals | CoHere intervals |
|---|---:|---:|---:|
| `manifest.json` | 21,329 / 21,426 | 8.895 / 22.333 / 1.042 / 0.043 | 8.841 / 22.804 / 0.978 / 0.039 |
| `page_manifest.json` | 279 | 8.631 / 24.155 / 0.024 / 0.013 | 9.026 / 23.817 / 0.022 / 0.011 |
| `page_digests.bin` | 12,512 | 9.310 / 23.679 / 0.033 / 0.021 | 9.015 / 23.437 / 0.029 / 0.018 |
| `plane/manifest.json` | 650 | 8.699 / 23.855 / 0.020 / 0.009 | 8.946 / 24.289 / 0.019 / 0.009 |
| `plane/mean.bin` | 3,072 | 8.975 / 24.111 / 0.024 / 0.013 | 8.782 / 23.354 / 0.022 / 0.011 |
| `plane/page_digests.bin` | 100,000 | 9.296 / 22.568 / 2.851 / 0.139 | 8.887 / 22.293 / 2.696 / 0.137 |
| `router/manifest.json` | 752,232 / 736,275 | 8.763 / 22.596 / 7.518 / 0.596 | 8.946 / 23.464 / 7.484 / 0.572 |
| `router/membership.bin` | 12,500 | 9.265 / 23.684 / 0.032 / 0.020 | 9.091 / 23.253 / 0.029 / 0.018 |

Observed staging medians are 277.952 / 273.216 ms. Semantic decode is only 3.329 / 3.185 ms. Source and leaf HEADs add separate median intervals of approximately 8.6–9.2 ms each.

The graph control exercises the same stager. Its additional objects have median HEAD/header/stream/write intervals:

| Graph object | ReLAION | CoHere |
|---|---:|---:|
| `centroids.bin`, 4,800,032 bytes | 8.916 / 0 / 46.304 / 1.252 | 8.849 / 0 / 47.322 / 1.219 |
| `graph.bin`, 426,398 bytes | 8.549 / 23.029 / 4.733 / 0.376 | 9.084 / 22.624 / 5.261 / 0.370 |
| `diverse_graph.bin`, 426,398 bytes | 8.718 / 23.200 / 4.560 / 0.373 | 8.884 / 23.260 / 5.092 / 0.360 |

For centroids, ranged GET header waits are inside the stream interval; zero header time does not mean free GETs. None of these object medians should be added to derive a latency percentile.

**The minimal implementation plan is four files:**

1. In [object_native_generation.rs](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/object_native_generation.rs:275), move the existing authenticated `exact` length calculation before `store.head()`. When it produces a length, use that length directly; otherwise retain HEAD admission. Use checked arithmetic and reject overflow rather than treating it as “unknown.” Check nonzero length and the remaining byte/JSON cap before submitting GET. Retain the existing GET metadata, range, streamed-length, hash, write, and cleanup checks. Report zero HEAD count/time for skipped calls.
2. Extend the existing [semantic_object_store_parity test](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/two_bit_generation.rs:2075) with exact request-count and malformed-response assertions.
3. Update [check_native_semantic_router_stats.py](/home/rb/worktrees/borsuk-prod-ready-v9/scripts/check_native_semantic_router_stats.py:66) for the next source revision: require zero HEADs for the exact-length roster and one for the remaining objects; require zero HEAD time when skipped; derive totals by summing actual counts. Preserve strict transport reconciliation.
4. Update the synthetic startup fixtures in [run_native_semantic_router_cold.py](/home/rb/worktrees/borsuk-prod-ready-v9/scripts/run_native_semantic_router_cold.py:1446) to match those counts.

The five semantic lengths already available are:

- `page_digests.bin`: `ceil(rows/256) × 32`
- `plane/mean.bin`: `dimensions × 4`
- `plane/page_digests.bin`: `ceil(rows/32) × 32`
- Router manifest and membership: authenticated `root_bytes` and `membership_bytes`

Graph startup receives the same reduction for the first three. Root, page-manifest, and plane-manifest HEADs remain because this seam lacks their exact authenticated lengths. Source-record and leaf-object HEADs remain because they pin strong ETags and validate whole-object geometry.

**The earliest shared seam is `stage_metadata`, immediately before its unconditional HEAD.** Its complete caller chain is:

- `stage_two_bit_metadata` → `TwoBitGeneration::open_remote` → production callers `TwoBitIndex::open_remote`, `two_bit_plan_demo`, and `examples/two_bit_http`.
- `stage_generation_metadata` → `ObjectNativeGeneration::open_remote`, plus the direct staging tests.
- Additional two-bit remote-open test callers occur in `two_bit_generation.rs`, `sq8_s3_range.rs`, `tests/two_bit_generation.rs`, and `tests/two_bit_application_ids.rs`.

`read_two_bit_head` separately authenticates the authorized head and generation root before staging. Its root GET remains. Neither `two_bit_store.rs` nor `semantic_unit_router.rs` needs an implementation change.

**Trust invariant:** only a fully SHA-authenticated, validated generation root may supply these lengths. GET must still return the exact object size and range; streaming must reject short/long bodies, and local open must verify every child identity and generation/source binding before exposing a generation. Metadata HEADs currently supply no conditional ETag used by their GETs. Source/leaf ETag pinning, conditional range reads, ordered-source identity, SQ8 parity, and downstream failure charges remain intact. Any newly submitted failing GET must remain visible in transport/error accounting.

Bounded concurrent staging is larger: it requires reserving aggregate admission before launch, handling overlapping buffers and cancellation, and changing timing/accounting assumptions. The five-HEAD deletion avoids those changes and retains frozen **4 MiB ranges / eight parallel range GETs**.

**ESTIMATE — removable critical interval:** sum the five HEAD waits within each individual closed record, then summarize those sums:

| Dataset | Median | Largest observed sum |
|---|---:|---:|
| ReLAION | 47.036 ms | 69.610 ms |
| CoHere | 45.169 ms | 73.413 ms |

Thus the largest trace-based saving estimate is **73.413 ms**. This is an observed-sample ceiling under unchanged remaining timings, not a guaranteed future bound or a predicted p90/p95 improvement. GET scheduling, connection behavior, and service variability remain unknown.

**Smallest falsifying test:** extend the existing recorded-store semantic parity test to require eight metadata GETs, exactly three metadata HEADs, and the retained source/leaf HEADs. That assertion fails on current code. In the same bounded fixture, inject wrong GET size/range, short/long payload, and same-length corruption into a headless child; each must fail closed and remove scratch. An insufficient remaining cap must reject before that child GET. Existing parity assertions must retain identical ordered IDs, plans, source/SQ8 charges, and failure charges.

No generation/source-format change is required. Future checker expectations belong to a new qualified source revision; frozen campaign methodology and historical artifacts remain unchanged.

Separately, semantic admission still explicitly stops at **100k rows/D768**. These repeated 64-query development results do not establish fresh 1M quality, admission, or performance. This startup change does not close that gap.
