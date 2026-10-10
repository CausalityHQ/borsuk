What this change does: It extends the accepted 1M/Q32 construction to full1000 truth, then measures a fresh application-cold S3 pass using existing Rust. Preparation, retained publication, and single-run reduction are supported; the required comparison against old Q32 is not.

**Verdict: hold the measured launch.** The main blocker is an unavailable native parity operation, followed by unfinished attempt-specific recipes and execution limits.

I checked Git blobs at `c3e52c8b…` and closed evidence at `96c6e904…`. `crates/borsuk` is unchanged between those revisions. Source references below use Git-blob line numbers, not the stale checkout.

1. **Must fix: no existing native API establishes the demanded Q32 parity.**

   [`specialist-plan`, line 68](/tmp/borsuk-next1m-full1000-preparation/specialist-plan-c7155554762847a3.md:68) requires equal ordered IDs, score bits, and logical charges. Existing modes cannot bind this comparison:

   - `--completed-scale` authenticates **one run**; it does not compare results across runs.
   - `--paired-v2` requires the frozen **100k/count1000/native100k** population, historical hashes, and fetch widths 16 then 32. Apart from those widths and configuration SHA, its identity/input rows must match.
   - `--membership-abba-v2` also requires the frozen 100k protocol and equal bound inputs.
   - Historical four-file comparison requires fixed binaries, count1000, and equal population/root/input bindings.

   See `c3e52c8b:crates/borsuk/examples/compare_native_replay.rs:1919,2471,2616,2876`.

   The old count32/full run and new count1000/diagnostic panel necessarily differ in request/truth hashes, receipt hashes, execution, backend, and root. Matching **314/320** cannot prove ordered-result or charge equality.

   **Smallest fix:** resolve this requirement before execution. Keeping exact parity requires a separately qualified comparison capability. Keeping every existing binary requires an explicitly weaker admission contract based on authenticated component identity and diagnostic results. Neither two successful single-run reductions nor an improvised Python comparison satisfies the current contract.

2. **Must fix: changing the preparation JSON alone does not create a full1000 recipe.**

   The pending template’s null output device/inode cannot pass the preparer. Bind the actual EC2 directory, then hash the finalized configuration.

   Both historical wrappers hardcode Q32 beyond configuration generation: geometry, `[100000,100032)` query ordinals, request/truth sizes and seals, modeled resource accounting, and baseline `count:32`. Preserve those recipes and freeze a small attempt-specific recipe.

   Required bindings include:

   - Full requests: **4,096,000 bytes**, SHA `8460a81f…`.
   - Fresh 1M truth: **80,000 bytes**, freshly measured SHA.
   - Request prefix: **131,072 bytes**, SHA `664f5b26…`.
   - Truth prefix: **2,560 bytes**, SHA `36e83267…`, from accepted **1M** truth.
   - Fresh cohort and derivation receipts; unchanged normalized/order/SQ8 component hashes.

   The existing CLI surface is:

   ```text
   prepare_cohere_native_cohort CONFIG CONFIG_SHA NEW_OUTPUT_DIR
   build_sq8_source --derive CONFIG CONFIG_SHA NEW_OUTPUT_DIR
   build_two_bit_generation CONFIG CONFIG_SHA MAX_MEMORY_BYTES NEW_OUTPUT
   publish_two_bit_generation CONFIG CONFIG_SHA NEW_RECEIPT
   publish_two_bit_generation --retained CONFIG CONFIG_SHA NEW_RECEIPT
   check_cohere_native_baseline CONFIG CONFIG_SHA NEW_OUTPUT
   ```

   The diagnostic panel genuinely authenticates and validates **all1000 requests and truth rows**, although it executes only ordinals 0–31. Truth opens after successful query sealing. This is supported by `check_cohere_native_baseline.rs:1989,2390`.

3. **Must fix: freeze the concrete upload inventory and namespace bindings. The proposed counts are correct.**

   For physical namespace `N`, regenerated local root `R`, and disjoint destination `D`, the source roster is exactly:

   ```text
   semantic/index/head.json
   semantic/index/generations/R/manifest.json
   semantic/index/generations/R/page_manifest.json
   semantic/index/generations/R/page_digests.bin
   semantic/index/generations/R/plane/manifest.json
   semantic/index/generations/R/plane/mean.bin
   semantic/index/generations/R/plane/records.bin
   semantic/index/generations/R/plane/page_digests.bin
   semantic/index/generations/R/router/root.bin
   semantic/index/generations/R/router/membership.bin
   semantic/index/generations/R/router/leaves.bin
   semantic/objects/<canonical-SHA>
   semantic/objects/<SQ8-SHA>
   ```

   The canonical object is **4,104,000,000 bytes**, SHA `72d58c8e…`; SQ8 is **1,036,000,000 bytes**, SHA `cc4395af…`. The closed original store totals **5,472,467,000 bytes**. Construction-only `centroids.bin` is outside this store roster.

   Retained publication adds those ten metadata names under `D/generations/S` and `D/head.json`: **11 new objects**, leaving canonical/SQ8 shared. Successful storage is approximately **5.805 GB**, excluding separately retained fixtures and receipts.

   The existing publisher already authenticates bodies, checks the approved actual SQ8 ETag, preserves coefficient bits, validates staged serving assets, rechecks object identities, and creates the head last. Ambiguous head-create failures preserve metadata. See `two_bit_store.rs:611,896,958`.

   **Smallest fix:** seal key/size/SHA/upload-token inventory and the namespace mapping. Use publisher `backend.kind:"S3"` with `namespace:N`; reader `backend.kind:"s3"` with `physical_prefix:N`. Bind the reader to the destination receipt’s metadata prefix and root. Preserve the uploaded original root; let retained publication perform SQ8 rebinding.

4. **Must fix: publication and interruption budgets remain incomplete.**

   The scratch arithmetic is correct:

   ```text
   135168 + 1306 + 562 + 250048 + 8192
   + 264000000 + 2000000 + 6082944 + 250000 + 64125000
   = 336853220 bytes
   ```

   It matches `two_bit_store.rs:937`: records/leaves count once, other non-root metadata twice, with separate root/head reserves. **512 MiB clears this admission; 64 MiB fails.** This is cumulative admission accounting, not measured RSS or scratch high-water evidence.

   Publication’s **300-second request timeout** does not specify a whole-publication deadline. Upload, retained publication, admission, canary, collection, and shutdown need finite budgets within each host’s overall deadline.

   Freeze fresh compute/request/storage caps. Publication reads large payload bodies, including SQ8 twice when both approved keys coincide; query-only projections omit this cost. Preregister bounded replacement attempts after Spot interruption, discard interrupted measurement cells, retain their receipts, and verify exact instance termination and owned-volume cleanup. Preserve ambiguous publication assets.

5. **Must fix: the full1000 decision rule is unfinished.**

   **9500/10000 with zero underfill** is a reasonable declared diagnostic floor. It does not establish matched quality, production acceptance, or competitive tail latency/QPS.

   The shortest defensible first S3 pass is a preregistered **characterization gate**: distinguish execution validity, resource compliance, quality-floor outcome, and measured performance. If it must produce a performance GO/FAIL, freeze numerical thresholds before seeing results.

   A serial1000 pass measures this selected workload. It does not reproduce the retained Turbopuffer methodology’s 10M population and offered load, nor establish concurrent service QPS. The saved100k S3 Vectors result remains historical evidence; do not rerun it or infer a matched1M win.

6. **Should fix: state exactly what completed-scale reports.**

   This invocation is supported:

   ```text
   compare_native_replay --completed-scale CONFIG CONFIG_SHA256 NEW_OUTPUT_JSON
   ```

   Configuration schema:

   ```json
   {
     "schema": "borsuk-completed-native-reduction-config-v4",
     "input": {"path": "...", "bytes": 0, "sha256": "..."},
     "expected_identity": {},
     "expected_bound_inputs": {}
   }
   ```

   Replace the illustrative placeholders with the sealed length/hash and complete, independently checked identity/input rows. The recorded reducer ELF is `84200c328100f16f612b06c5d7f69db1ddf1516d04abd51099fade3161c97ede`; its source is unchanged from its qualification revision.

   It supports 1M/full1000, validates seals, recall, logical charges and cumulative transport counters, and reports nearest-rank p50/p90/p95/p99 plus `1000e9 / sum(query_wall_ns)`. Diagnostic panels deliberately receive null population statistics.

   Two limitations matter:

   - Terminal transport counters omit detailed status distributions; retain the authenticated JSONL containing full transport snapshots.
   - It emits process time but no separate whole-run QPS field. Native process time ends before final terminal serialization/sync; retain external original-process elapsed time for a precisely defined whole-run measure.

   Query timing encloses the native search and S3 work, while excluding startup, request decoding and result serialization. Label that boundary accurately; do not present it as an offered-load service latency measurement.

The preparation projection is arithmetically sound: `73.06 × 31.25 = 2283.125` seconds wall and `44.43 × 31.25 = 1388.4375` seconds user CPU. Neither guarantees completion. The truth loop is sequential, so CPU4 does not imply four-way truth acceleration. Keep the native **2400-second ceiling**; a timeout is execution INVALID.

The separate canary must exercise real credentials, create-only upload/copy, publisher/reader CLI behavior, occupied-destination and bad-root refusals, durable receipts, original exits, and owned cleanup. Historical synthetic collector coverage cannot qualify this new composition.

“Application-cold first pass” is the justified cache label: payload caches disabled, structural router resident, HTTP connections reused, provider cache uncontrolled. The current envelope is **160 logical GETs / 83,881,984 bytes per query**, not the older universal32-GET/16-MiB goal.

No edits, native/data execution, AWS/network access, children, consultations, or job/control changes were performed. No algorithm redesign or SIMD work is warranted by this review.
