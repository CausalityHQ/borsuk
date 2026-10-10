**Use the existing Rust binaries. Fresh S3 publication does not require a Rust change.** At authoritative `96c6e904…`, `crates/borsuk` is unchanged from qualified native source `c3e52c8b…`. The remaining work is a new frozen configuration, small experiment-glue changes, and runtime qualification.

The ordinary publisher CLI supports LocalFileSystem. Its existing **`--retained` mode supports S3**, authenticates an uploaded retained roster, rebinds SQ8 to its actual S3 ETag, validates the destination, and creates its head last. The [100k retained S3 publication]( /home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/cohere1024/retained-s3-publication/admission-a0002/root-verification.json) already exercised that path successfully.

1. **Prepare full1000 truth once on causality Spot, eu-central-1.**

   Reuse the qualified preparer and shard/ELF pins. In `borsuk-cohere-native-cohort-config-v3`, set `geometry.queries=1000`; retain:

   - Corpus intervals `[0,100000)` and `[101000,1001000)`.
   - Reserved queries `[100000,101000)`, D1024, cosine, k10.
   - Dataset revision `ade45fb52bd549f5e8c065636fe4160a43c2af36`.
   - Existing decode, memory, CPU, swap and scratch limits; bind the new output parent’s actual device/inode.

   Run `prepare_cohere_native_cohort CONFIG CONFIG_SHA NEW_OUTPUT_DIR`. Expect the five prepared files plus `complete.json`: corpus **4,096,000,000 B**, requests **4,096,000 B**, truth **80,000 B**, and both ID files.

   Require the unchanged corpus SHA `1a491c06…`; full requests must match existing SHA `8460a81f…`. Authenticate the first32 truth prefix against the closed 1M Q32 truth—not the 100k truth. Seal the fresh full1000 receipt.

   **Glue must change:** the current admission wrapper and `run_native_scale_build_gate.sh` hardcode Q32 geometry, seals and `count:32`. Updating only their JSON templates will fail. Preserve the historical scripts/receipts and make one attempt-specific full1000 recipe.

2. **Recreate the same native index and retain its actual published roster.**

   The prior attempt explicitly discarded large artifacts at termination. Therefore regenerate them using unchanged `build_sq8_source --derive`, `build_two_bit_generation`, and local publication.

   Keep derivation query-free and require the existing normalized/order/SQ8 seals:

   | Artifact | Bytes | SHA prefix |
   |---|---:|---|
   | `normalized.f32` | 4,096,000,000 | `31f28fa2…` |
   | `order.u64` | 8,000,000 | `1060c919…` |
   | `sq8.bin` | 1,036,000,000 | `cc4395af…` |

   Builder fields remain `discovery:"semantic"`, `semantic_profile:"scale1m"`, `rows:1000000`, `dimensions:1024`, `generation:1`, `base_epoch:0`, authenticated `raw`, `sq8`, `order`, and receipt-derived `low`/`step`.

   **Use the newly emitted local root SHA.** Recreated LocalFileSystem inode/mtime ETags can change it; do not demand historical root `151e996a…`.

   Upload the exact local **store** roster create-only, preserving logical keys beneath a new physical S3 namespace:

   - `semantic/index/head.json`.
   - Ten generation metadata objects: `manifest.json`, `page_manifest.json`, `page_digests.bin`, three `plane` sidecars plus its manifest, and three router objects.
   - Shared canonical object **4,104,000,000 B** and SQ8 object **1,036,000,000 B** under `semantic/objects/<sha>`.

   That is **13 source-store objects**, approximately **5.472 GB**. Retain full1000 requests/truth, both provenance receipts and the upload inventory separately. Authenticate uploaded bodies; an ETag alone is not a content SHA.

3. **Publish through the retained CLI, then pass admission and a separate canary.**

   Use:

   ```text
   publish_two_bit_generation --retained CONFIG CONFIG_SHA NEW_RECEIPT
   ```

   Exact retained configuration:

   - `schema:"borsuk-two-bit-retained-publication-config-v2"`.
   - `backend.kind:"S3"`—capitalization differs from the baseline reader.
   - `backend.bucket`, `region:"eu-central-1"`, `namespace`, `request_timeout_seconds:300`.
   - `retained_prefix:"semantic/index"`.
   - `original_root_sha256`, `original_generation:1`, `original_control_epoch:1` from the new local receipt.
   - `sq8_object_key:"semantic/objects/<sq8-sha>"`, actual quoted S3 `sq8_etag`.
   - Fresh, disjoint `destination_prefix`, existing `scratch_parent`, `max_scratch_bytes:536870912`.

   Keep publication limits: memory **512 MiB**, active queries **1**, SQ8 **32 GET / 16,773,120 B**, SOURCE **128 GET / 64 MiB**, both fetch widths **16**, query scratch **594,520 B**, caller pins **0**.

   The retained API’s cumulative scratch calculation is approximately **337 MB** for this roster. The historical 100k **64 MiB** allowance is insufficient. Successful publication adds ten metadata objects and one head: **24 store objects total**, approximately **5.805 GB**, with canonical/SQ8 shared.

   Verify unchanged component hashes and all2048 low/step f32 bits. Accept only SQ8 key/ETag rebinding in the replacement manifest. Preserve ambiguous head-create failures and their objects.

   **Bounded falsifier:** configure the real full1000 fixture with `count:1000`, `execution:{mode:"diagnostic_panel",ordinals:[0,…,31],trace:false}`. Within **300 seconds**, require exact first32 local/S3 ordered ID, score-bit and logical-charge parity; **314/320 hits**, zero underfill, valid seals and original exit0. This validates all1000 requests and the full truth body through the existing runtime path while executing only32 queries.

   Follow with a separate disposable canary covering actual instance-role credentials, S3 create-only/multipart copy, CLI exits, occupied-destination refusal, bad-root refusal, receipt durability and scratch/process cleanup. Keep production assets immutable. Real operations and any mocked shutdown/fault operations must be labelled separately.

4. **Freeze and measure one full1000 S3 pass on fresh Spot compute.**

   Use `borsuk-cohere-native-baseline-config-v7` with:

   - `count:1000`, `profile:"scale1m"`, unchanged corpus/query intervals.
   - Fresh `cohort_receipt`, `derivation_receipt`, existing `producer_authority`, and full request/truth descriptors.
   - `backend:{kind:"s3",bucket,region,physical_prefix,sq8_object_key,sq8_etag}`.
   - `generation_prefix` and `generation_root_sha256` from the retained S3 publication receipt.
   - `max_memory_bytes:536870912`, `fetch_parallelism:16`.
   - `serving:{mode:"baseline"}`, `execution:{mode:"full"}`.

   Run serial queries0–999, without query warmups or payload caches. Reuse the 100k methodology: nearest-rank p50/p90/p95/p99; serial QPS = `1000 / sum(query_wall_seconds)`; disclose binding/startup separately. Preregister **CPU1, 512 MiB, swap0, PID128, 1,200-second measurement deadline**, one attempt, and a proposed quality floor **9500/10000 hits with zero underfill**.

   After terminal closure, use existing `compare_native_replay --completed-scale` with schema `borsuk-completed-native-reduction-config-v4`, sealed input path/bytes/SHA, and exact expected identity/bound-input rows. Its qualified ELF is already recorded as SHA `84200c32…`.

   Report physical native HTTP attempts/statuses/failures and consumed response bytes separately from logical charges. Process counters include IMDS credential traffic; wire and billed quantities remain unknown. Native summary flags deliberately require external qualification—do not rewrite them.

**Envelope and blockers:** retain the proven construction envelope of CPU4/8 GiB/no swap/PID128 and 40 GiB scratch. Q32 preparation scaled by query count projects **2,283 s wall / 1,388 s user CPU** for1000; this is a projection, with only117 seconds below the preparer’s fixed **2,400-second ceiling**. Derivation was789.31 s/1.01 GiB RSS; generation187.91 s/445 MiB; local publication37.61 s/256 MiB. New S3 publication remains unmeasured. Freeze fresh Spot and storage/request cost caps before launch.

Q32 logical reads project roughly **131k reads / 44.27 GB** for1000; enforced query caps allow **160k reads / 83.88 GB**, excluding startup/publication. These are resource projections, not latency or billing evidence.

The result can establish **real S3 performance with application payload caches disabled**. Router metadata remains resident and HTTP connections are reused; provider caches are uncontrolled. It cannot establish guaranteed backend-cold performance for every query. Preserve that disclosure, terminate each host at its terminal marker, discard interrupted measurement cells, and defer warm testing. **Never repeat the completed100k S3 Vectors comparator; neither it nor unmatched published Turbopuffer numbers establishes a measured1M win.**

Read-only planning completed; no files, builds, runtime jobs or cloud resources changed.
