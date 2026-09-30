Implement the semantic candidate inside `TwoBitGeneration`, then run one bounded cold HTTP comparison. The local scorer passes justify this increment; they establish neither serving performance nor a win.

1. **Own the router mechanism in one library file.** Create `crates/borsuk/src/semantic_unit_router.rs`, register it in `lib.rs`, and reuse its construction/selection code from `bin/build_semantic_unit_router.rs` and `bin/check_semantic_router_scorer.rs`.

   Preserve the existing fit and nomination exactly: 64-unit leaves; unnormalized FP16 unit means; unweighted prototypes rounded to f32; coordinate-order f64 root distances; leaf-ID ties; first eight leaves, expansion within `1.15 × eighth_distance`, maximum sixteen. Retain every admitted unit. Prepare two-bit scoring from the original query, then call the existing cosine normalizer. Use the lowest nominated physical page as seed and add only its missing units.

   Separate publication validation from serving validation: publication checks the complete disjoint partition and original FP16 identities; serving authenticates the root, membership and each selected whole leaf without loading all leaves or centroids.

2. **Add authenticated lazy serving in the existing generation.** Own `two_bit_generation.rs` and the metadata-staging helper in `object_native_generation.rs`.

   Introduce one strict generation format, proposed **v7**, with an explicit graph or semantic discovery descriptor. Preserve graph discovery for the contemporaneous control. The semantic descriptor binds router-root/membership/payload identities, generation, geometry and source identities without a self-referential root hash.

   Authenticate the generation manifest before selecting its startup roster. Semantic startup loads codec metadata, source/SQ8 authorities, router root and membership; it neither fetches nor decodes `centroids.bin`, `graph.bin` or `diverse_graph.bin`. Selected leaves are exact bounded range GETs with length, object-size, strong ETag and per-leaf SHA checks.

   Reuse `TwoBitPlane::open_metadata`, `plan_two_bit_source_cover`, `fetch_verified_ranges_inner`, `plan_two_bit_source_walks` and native SQ8 ranking. Pass the actual semantic page-closure count to the shared planner; retain the control’s existing maximum. Release leaf buffers before source reads and source buffers before SQ8 reads.

   Keep these limits:

   | Payload | GET cap | Byte cap | Concurrent reads |
   |---|---:|---:|---:|
   | Router root | Startup, separately charged | 1 MiB | 1 |
   | Selected whole leaves | 16 | 2 MiB | ≤16 |
   | Source | 128 | 67,108,864 | ≤16 |
   | SQ8 | 32 | 16,773,120 | ≤16 |

   Source plus SQ8 remains capped at **160 GETs / 83,881,984 bytes**. Router and other startup traffic are additional. Keep source/SQ8/leaf caches absent; bound retained root/membership metadata. Charge decoded metadata, transient copies, planner scratch, all concurrent queries and retired pins before allocation; record actual RSS separately.

3. **Carry the same format through build and lifecycle paths.** Own `two_bit_build.rs`, `bin/build_two_bit_generation.rs`, `two_bit_store.rs`, `two_bit_compaction.rs` and `two_bit_gc.rs`.

   Add explicit semantic build entrypoints using the shared constructor; keep graph construction available for control. Replace the fixed upload digest zip with the authenticated format-specific roster. Publication validates all router artifacts before its existing head CAS. GC recognizes router artifacts and retains the current generation’s exact roster.

   Compaction must capture its requested discovery mode in the existing durable job, check it on restart, and rebuild that mode through the same builder. Preserve sealed mutations, logical IDs, canonical-source reconstruction, empty results and lifecycle fences. Reject stale prepared artifacts or unsupported semantic sizes; do not silently switch discovery mode. Exercise these through existing `two_bit_generation.rs` and `two_bit_application_ids.rs` integration tests.

4. **Expose the existing transport seam and accounting.** Own `two_bit_generation.rs`, `sq8_s3_range.rs` and `examples/two_bit_http.rs`.

   Full `ObjectStore` search is currently private; only source planning has a public store entrypoint. Expose an **admitted** full-search entrypoint taking `&dyn ObjectStore`, sharing validation, semaphore and ranking with `OneAttemptS3`. Keep the unadmitted implementation private.

   Extend results/errors and HTTP output with router charges and stage intervals. Instrument the existing native HTTP transport for actual attempts and consumed response-body bytes, including failures. Current `Sq8ReadStats` counts submitted operations and authenticated bytes; it does not establish wire accounting. Retain disabled retries and drain admitted reads on failure.

   Another concrete blocker is the metadata stager’s blanket **64 KiB JSON cap**: permit **1 MiB specifically for the router root**, while retaining the generation-manifest cap.

5. **Qualify locally, then hand the frozen HTTP cell to the root.**

   Leave one new runnable test, `semantic_object_store_parity`, beside the generation tests. Use existing `InMemory`, temporary-directory and recording-store patterns: a small deterministic D2 corpus with more than eight leaves—**16,385 rows permits nine training centers**—scattered units, a partial final unit/page and reversed logical IDs. Compare lazy search with an eager reference using the same shared planner and native SQ8 kernel. Assert selected leaves, every admitted unit, seed additions, plans and ordered IDs match; graph objects are absent; only selected leaf ranges are fetched; reads stay bounded. Within that test, corrupt a selected leaf and lower admission budgets: reject before downstream reads and preserve incurred charges.

   Run sequentially on the exact implementation revision, with two compiler jobs:

   ```bash
   cargo test --locked -p borsuk --lib semantic_object_store_parity
   cargo test --locked -p borsuk --lib --test two_bit_generation --test two_bit_application_ids --bin build_semantic_unit_router --bin check_semantic_router_scorer --bin build_two_bit_generation --example two_bit_http --no-run
   cargo test --locked -p borsuk --test two_bit_generation --test two_bit_application_ids --example two_bit_http
   cargo clippy --locked --workspace --all-targets -- -D clippy::correctness -D clippy::suspicious
   BORSUK_TEST_BUILD_JOBS=2 bash scripts/check_rust_test_build.sh
   cargo build --locked --release -p borsuk --example two_bit_http
   ```

   Record revision, commands, exit statuses and executable SHA. Update format fixtures and affected binary tests before accepting the gates.

**Bounded cold HTTP falsifier:** the root freezes and runs ReLAION first, then CoHere, each FIRST100k/D768/cosine, consumed ordinals 0–63, top100, alternating arm order by ordinal. Use fresh processes/scratch and fresh immutable namespaces for each cold observation; authenticate identical payloads and verify both arms against the closed local plans and ordered IDs before quality evaluation. No retries, sweeps or cap expansion.

Historical inputs are generation **v4 / plane-v2**; current readers require **v6 / plane-v3**. Package unchanged authenticated mean, records, SQ8, graphs and fitted router into the new native envelopes, adding source-unit digests. Record every changed envelope identity. Do not add legacy serving readers or refit the measured router.

Measure client latency, startup-to-first-result latency, startup traffic, leaf/source/SQ8 stage intervals, actual request overlap, attempts, transferred/verified bytes, errors and peak RSS. Leaves add a dependent fetch stage before source nomination; report observed concurrency rather than inferring waves from logical GET counts.

Stop on parity/authentication/budget failure, unsuccessful calls, or candidate mean **R10 <95%**. Report R100 and weak tails unchanged. Preregister that erased physical-I/O savings or worse paired cold p95 blocks promotion; quality survival alone does not demonstrate a serving advantage. Offered-load work follows this screen.

The main scale risk remains the flat root: approximately tenfold growth at 1M would exceed the present 1 MiB admission. Keep that rejection explicit pending a separately measured scale gate. The observed R10 values—98.125% and 95.78125%—and modeled totals of 23.69/23.93 MB remain consumed-panel evidence.

Read-only inspection completed at `36256d9178751a33731d0502c40eb9d418d9497e`; no files changed, builds run, cloud actions or reviews launched.

## Root execution decision

Execute the existing mechanism in ordered verified slices, without another
architecture consultation. First slice owns only semantic_unit_router.rs,
lib.rs, bin/build_semantic_unit_router.rs and bin/check_semantic_router_scorer.rs.
Extract existing construction, root nomination and artifact validation into
one reusable library implementation; keep CLI authentication/file writes in
the binaries. Preserve frozen artifact byte encoding and all floating-point
semantics. Complete-publication validation and selected-leaf validation
must be distinct so serving can authenticate a selected leaf without loading
the full centroid/leaf object. Do not add a v4 serving reader: the current
historical research CLI boundary is not a production compatibility promise.

This slice alone is not cold-HTTP launch authority. Next slice must complete
generation serving, current-format payload repackaging, publication/compaction/
GC and observable HTTP accounting. Keep graph mode as a measured control arm,
not an implicit fallback. The authoritative scientific cold experiment is
semantic-router-cold-http-gate.md (ABBA blocks); the consultation's alternating
order description does not replace that preregistration. Do not refit the
two measured routers or re-evaluate the consumed panels during extraction.

## Shared core delivered (2026-09-30)

Integrated worker `67a0cb06` as `88102baf`: reusable construction, publication
validation, authenticated root/membership open and bounded whole-leaf nomination.
Both research CLIs reuse the module. The deterministic fixture preserves all
three artifact digests; the oversized schema regression failed before its repair
and passes with the 256-byte bound. All 16 affected tests, both release builds,
workspace Clippy and complete workspace test compilation passed on exact source.
See `semantic-router-library-assurance.json` and its authenticated gzip logs.
Full workspace test execution and native serving qualification are not claimed.
The module explicitly remains bounded to 100k rows/D768; no panel was rerun.

Next: execute `semantic-router-native-contract.md` across generation startup,
query admission, publication, compaction/restart, GC and native HTTP accounting.
The preregistered cold HTTP gate remains pending this implementation and authority.
