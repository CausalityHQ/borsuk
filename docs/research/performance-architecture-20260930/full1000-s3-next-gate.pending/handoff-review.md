Use one bounded causality EC2 admission that **reuses the closed a0004 local panel and historical Q32 run**. No rebuild or re-derivation is needed. Full1000 cold measurement remains a later gate.

Static inspection found one definite template defect: every `runs[].schema` in the pending parity configs must be **`borsuk-completed-native-reduction-config-v4`**, not `borsuk-completed-native-reduction-v1`. The qualified `afb70da` reducer explicitly requires v4.

1. **Authenticate and replay the closed evidence on EC2.**

   Authenticate the a0004 archive against the supplied 596,125-byte/SHA pin before bounded extraction. Its committed checksum manifest identifies these members:

   - `baseline-result.jsonl`: 1,132,147 bytes, supplied `c427222d…` SHA.
   - `evidence-chain/configs/baseline.json`: SHA `ffa8ce8b844495a3ef09c4379c8205bc32b63b91634f1d28021d02c8e3584086`.
   - `prepared-parent_cohort_complete.json`: 6,238 bytes, supplied `e0503d41…` pin.
   - `prepared-parent_derived_derivation.json`: 23,128 bytes, SHA `00e59ee26745c092f1097ba8892b4f205f7c96b48a757311aedc9072fcc3c96e`.
   - `prepared-parent_generation_manifest.json` and `prepared-parent_publication-receipt.json`.

   `CompletedConfig` has exactly `schema`, `input:{path,bytes,sha256}`, `expected_identity`, and `expected_bound_inputs`. The last two are complete native rows, **including `phase`**, checked for exact equality. Bind them against the authenticated runtime config, original executable, embedded source identities and receipts; copying unchecked result headers is insufficient.

   Replay both historical Q32 and a0004 before publication. The historical archive path exists as a regular **596,854-byte file**; only its filesystem metadata was inspected here. Its authenticated archive/result/receipt pins remain an EC2 prerequisite.

2. **Authenticate and import the retained generation into a fresh namespace.**

   Use all 13 descriptors from a0004 `retained-generation_complete.json`. On EC2, condition reads on recorded object identity and verify exact length, SHA and EOF. Import unchanged logical keys into a fresh attempt namespace `N`, using create-only writes and head-last ordering.

   Bind the existing retained-publication template as follows:

   | Field | Required value |
   |---|---|
   | `schema` | `borsuk-two-bit-retained-publication-config-v2` |
   | `backend` | `{"kind":"S3","bucket":"borsuk-bench-453182569524-euc1","region":"eu-central-1","namespace":N,"request_timeout_seconds":120}` |
   | `retained_prefix` | `semantic/index` |
   | `original_root_sha256` | Supplied `8a9ac0a9…` full root |
   | `original_generation`, `original_control_epoch` | `1`, `1` |
   | `sq8_object_key` | `semantic/objects/cc4395af5fdbdeffcb596460aceaad0e55bdcc4af074f405e78ab2dd2be002e1` |
   | `sq8_etag` | Actual quoted ETag of the **new imported object** |
   | `destination_prefix` | Fresh disjoint logical prefix, e.g. `admitted/index` |
   | Scratch and limits | Existing directory; `max_scratch_bytes=536870912`; unchanged ten library limits |

   Both source and destination resolve under `N`; do not insert `N` into logical object keys. The native operation authenticates retained bodies, changes SQ8 key/ETag bindings, creates ten destination metadata objects, validates them, and publishes the eleventh object—the head—last. Preserve the shared SQ8 and canonical objects while that destination lives.

3. **Bind and run the S3 panel.**

   Clone the authenticated archived baseline config. Remap local artifact/scratch paths and replace:

   - Backend with lowercase `"kind":"s3"` and fields `bucket`, `region`, `physical_prefix:N`, `sq8_object_key`, `sq8_etag`.
   - `generation_prefix` and `generation_root_sha256` with the **new native publication receipt’s** `metadata_prefix` and `root_sha256`.

   Keep the exact cohort and derivation receipt bytes, producer authority, native source hashes, `rows=1000000`, `dimensions=1024`, `count=1000`, `k=10`, `profile="scale1m"`, baseline serving, fetch parallelism 16 and 512 MiB memory limit. Keep full 4,096,000-byte requests and 80,000-byte truth; execution remains ordinals 0–31 with `trace:false`.

   Publication uses `namespace`; the runner uses `physical_prefix`. Neither runner field is the head key. Native S3 credentials come from the EC2 instance role; provisioning uses profile `causality`.

4. **Run the qualified reducers under the existing supervisor.**

   Here `REPLAY` is the qualified `afb70da` example, while `PUBLISH` and `BASELINE` are the original pinned c3e52c8 executables. Each SHA variable binds its finalized config; all output paths must be fresh.

   ```bash
   "$REPLAY" --completed-scale historical.completed.json "$H_SHA" historical.reduction.json
   "$REPLAY" --completed-scale local.completed.json "$L_SHA" local.reduction.json

   "$PUBLISH" --retained retained-publication.json "$P_SHA" retained-publication.receipt.json
   "$BASELINE" s3-panel32.json "$B_SHA" s3-panel32.jsonl

   "$REPLAY" --completed-scale s3.completed.json "$S_SHA" s3.reduction.json
   "$REPLAY" --scale-prefix-parity parity.json "$PARITY_SHA" parity.report.json
   ```

   Keep parity’s outer schema `borsuk-scale-prefix-parity-config-v1`. Order its three v4 runs as historical Q32, a0004 local panel, new S3 panel. Supply historical/full request and truth descriptor pairs. All seven artifact roles require distinct files/inodes; the three logs also require distinct hashes and runtime-config hashes.

   Before accepting parity, independently establish generation-component equivalence: normalized/source-order/SQ8 seals, canonical and plane/page descriptors, semantic router components and construction provenance, profile, generation/base epoch, and exact f32 calibration bits. Root changes from rebinding are expected. The reducer explicitly leaves this provenance gate external.

   Its cross-run invariants retain executable/component hashes, source policy, producer authority and serving settings. Do not relax them to accommodate a different binary.

5. **Use panel32 as the bounded falsifier, then a separate canary.**

   Require successful native closure, **314/320 hits, zero underfill**, and `EXACT_PREFIX_PARITY`: ordered IDs, f32 score bits and per-query logical charges agree. The reducer authenticates full request/truth bodies before comparing their prefixes. It does **not** independently enforce the fixed 314-hit expectation.

   Prospective caps: 120 seconds per reducer, 900 seconds for S3 panel32, sequential execution. Stop at the first failed prerequisite. Provenance/schema/transport failures are execution `INVALID`.

   After admission passes, run a separate disposable real-S3 canary covering asset admission, CLI/SDK, create-only publication, negative refusal, actual manager exit/drain and cleanup. A bounded negative is a copied full request file with one changed suffix byte and its original SHA: admission must refuse despite querying only ordinals 0–31. Freeze full1000 cold measurement only after this canary closes.

The retained roster totals **5,472,467,000 bytes**; its largest body is **4,104,000,000 bytes**. Native retained scratch admission calculates to **336,853,220 bytes**, within 512 MiB. Historical a0004 RSS peaks were **50,712 KiB baseline**, **262,420 KiB publication**, **455,164 KiB generation**, and **1,054,524 KiB derivation**; these are not measurements of S3 republication. Propose sequential CPU1/1 GiB transport/publication/reduction scopes, preserve CPU1/512 MiB panel scope, swap0/PID128 and the existing 40 GiB scratch/host safety ceilings. Preregister transfer costs, deadlines and Spot interruption handling before authorization.

Remaining blockers are the EC2-only historical identity/component checks, remote whole-body authentication, actual imported ETag, receipt-bound configs, and separate canary. No files were edited, payloads opened, builds/tests/native validators run, network used, or paid launch authorized.
