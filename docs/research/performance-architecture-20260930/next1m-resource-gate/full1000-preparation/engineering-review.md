What this change does: it extends the accepted 1M/Q32 correctness screen to full1000 truth and an application-cold S3 measurement using the qualified Rust binaries. **The protocol is not launch-ready: the required cross-run parity check has no existing native invocation.**

I reviewed Git blobs at `c3e52c8b…` and evidence at `96c6e904…`; `crates/borsuk` is unchanged between them. Source line references below refer to those blobs, not the stale checkout.

1. **Must fix — existing Rust cannot enforce the proposed Q32/full1000 parity gate.**

   [`compare_native_replay.rs:1919`](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/examples/compare_native_replay.rs:1919) reads **one** `CompletedConfig`. It validates that result’s identities, seals, returned rows, recall and charge totals; it never compares two scale runs.

   `--paired-v2` explicitly requires the historical **100k**, count1000 inputs and widths16/32, then requires matching bound inputs except width (`:2471–2525`). Membership ABBA likewise requires historical100k and identical bound inputs (`:2643–2676`). Neither accepts changed count, receipt, backend and root. The older parity binaries serve different protocols.

   **Smallest fix:** resolve this protocol conflict before execution. Retaining exact ordered-ID/score-bit/per-query-charge parity requires a narrowly qualified native comparison path. Under the current “existing binaries only” constraint, that requirement is blocked. Two successful single-run reductions, matching aggregate recall, or stripped/resealed JSON do **not** establish it.

2. **Must fix — full1000 needs more recipe changes than geometry and seals.**

   The actual historical preparation wrapper hardcodes Q32 geometry, output lengths **and resource-accounting values** at `native-admission-a0001/run_actual_cohort_admission.sh:286–321`. Its final request check still requires131,072 bytes (`:344`).

   The [build wrapper](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/next1m-resource-gate/derivation/runtime-support.pending/run_native_scale_build_gate.sh:167) also rejects baseline deadlines above **900 seconds**, while this proposal specifies1,200. It emits `count:32`, local backend and full execution at `:263–278`.

   **Smallest fix:** freeze an attempt-specific recipe preserving the originals. Update request/truth/query-ID seals, receipt geometry, query interval endpoint, resource-accounting checks, baseline backend/execution and phase deadline. Bind the actual output-parent device/inode before hashing the final config.

   Static source arithmetic gives these full1000 preparation values on the qualified ABI:

   | Field | Bytes |
   |---|---:|
   | Retained query vectors | 4,096,000 |
   | All query top-k storage | 160,000 |
   | Query norms / heap headers | 8,000 / 24,000 |
   | Resident charge before decoder | 2,897,237,664 |
   | Output cap | 11,275,409,536 |
   | Total preparation scratch admission | 15,335,384,993 |

   Thus the template’s **16GiB preparation allowance still fits statically**; distinguish it from the overall40GiB workspace envelope. Preserve the accepted1M truth-prefix SHA `36e83267…`; the100k truth is inapplicable.

3. **Must fix — deadlines, interruption closure and cost limits remain prose, not an executable frozen gate.**

   The proposal specifies preparation/admission/measurement deadlines but leaves fresh request/storage caps and termination proof open ([protocol:19](/tmp/borsuk-next1m-full1000-preparation/root-protocol-review-input.md:19)). The retained publisher’s300-second timeout is **per request**, not a whole-publication deadline.

   Historical replay itself retains an explicit unresolved seam: cloud-final exit mapping for native exits0/2/3 needs real canary coverage. Its successful replay is not evidence that the new full1000/S3 lifecycle works.

   **Smallest fix:** freeze bounded stage, collection and overall deadlines with cleanup grace. The separate disposable **real** canary must exercise role credentials, conditional PUT/multipart copy, occupied destination, bad root, native status propagation, durable receipts and owned cleanup. Keep fault simulations separately labelled.

   On Spot interruption, preserve the partial attempt, mark the measurement cell invalid, and restart the cell under a new identity. Require exact-instance termination evidence and associated scratch-volume disposition; do not resume partial query measurements. Include upload, publication, admission and startup costs—not just query charges—in the frozen caps.

4. **Must fix — there is no frozen quality/performance acceptance decision yet.**

   `9500/10000` with zero underfill is a proposed diagnostic floor. It cannot establish matched quality, production readiness or a vendor win. The protocol correctly acknowledges this but leaves the actual decision pending ([protocol:17](/tmp/borsuk-next1m-full1000-preparation/root-protocol-review-input.md:17)).

   **Smallest fix:** preregister the decision and applicable quality, tail-latency, throughput and lifecycle-cost thresholds before measurement, or explicitly classify the run as characterization only. Do not select thresholds after seeing full1000 results.

The following parts are supported by the existing source:

- **Actual-input admission:**  
  `check_cohere_native_baseline CONFIG CONFIG_SHA NEW_OUTPUT` with schema `borsuk-cohere-native-baseline-config-v7`, `count:1000` and `execution:{"mode":"diagnostic_panel","ordinals":[0,…,31],"trace":false}` is supported. It validates **all1000 request rows before heavy opens**, then authenticates and validates **all1000 truth rows after sealing the selected results** ([baseline:1988](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/bin/check_cohere_native_baseline.rs:1988), `:2388–2401`). This supports full-input admission; it does not solve finding1.

- **Completed full1000 reduction:**  
  `compare_native_replay --completed-scale CONFIG CONFIG_SHA NEW_REPORT` uses:
  ```json
  {
    "schema": "borsuk-completed-native-reduction-config-v4",
    "input": {"path": "...", "bytes": 123, "sha256": "..."},
    "expected_identity": {},
    "expected_bound_inputs": {}
  }
  ```
  The two expected objects must contain the **complete independently bound native rows**, including receipt hashes, producer authority, backend, root, intervals, count and execution. Source explicitly admits1M/`scale1m`/1000 and computes nearest-rank p50/p90/p95/p99 plus `1000 × 1e9 / sum(query_wall_ns)`.

  It validates physical transport snapshots and retains terminal attempt/method/failure/consumed-byte counters. **Status histograms remain in the sealed phase records; the summary deliberately omits them.** Preserve the JSONL. There is no emitted whole-run-QPS field: `process_wall_ns` is available, but ends before final terminal write/sync. Define any whole-command throughput from the external timing receipt. Diagnostic-panel reports correctly suppress population percentiles.

- **Retained publication and exact roster:**  
  `publish_two_bit_generation --retained CONFIG CONFIG_SHA NEW_RECEIPT`, schema `borsuk-two-bit-retained-publication-config-v2`, supports `backend.kind:"S3"`.

  Under `semantic/index/generations/<local-root>/`, the ten metadata objects are:
  ```text
  manifest.json
  page_manifest.json
  page_digests.bin
  plane/manifest.json
  plane/mean.bin
  plane/records.bin
  plane/page_digests.bin
  router/root.bin
  router/membership.bin
  router/leaves.bin
  ```
  Add `semantic/index/head.json`, canonical4,104,000,000-byte and SQ8 1,036,000,000-byte objects under their manifest-bound `semantic/objects/<sha>` keys: **13 source objects**. The destination adds ten metadata objects plus its head: **24 total**, with both payload objects shared.

  [`two_bit_store.rs:864`](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/two_bit_store.rs:864) confirms the roster. Publication hashes bodies, checks the approved actual SQ8 ETag, preserves coefficient bits, authenticates copied bodies and creates the head last. Use the returned **`metadata_prefix`** as baseline `generation_prefix`, and returned `root_sha256`; keep physical namespace and logical keys separate.

  The scratch formula at `:940–953` agrees with **336,853,220 bytes** from the supplied closed-metadata sizes. **512MiB admits that charge;64MiB fails.** This is scratch admission, not proof of512MiB process RSS. Publication authenticates SQ8 twice even when both keys coincide; account for those reads. Preserve shared payloads and all metadata after an ambiguous head-create failure.

The preparation ceiling is genuinely2,400 seconds. The projections are arithmetically correct: **2,283.125 wall seconds /1,388.4375 user-CPU seconds**. They are not completion guarantees; the truth loop is serial, so CPU4 is not a fourfold speedup. Timeout remains execution INVALID.

The cache wording is appropriate: fresh application process, no payload cache or query warmups, resident router, reused connections, uncontrolled provider caches. That establishes application-cold first-pass behavior only. Keep the saved100k S3 Vectors result untouched; neither it nor unmatched Turbopuffer numbers proves a1M win.

**Verdict: fix1–4 before launching the measured gate.** No edits, execution, cloud/network access, delegation or consultations were performed. Runtime success and the new canary remain unverified.
