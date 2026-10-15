Use a **fresh two-ELF ABBA campaign**, with the retained v8 SOURCE128 control and the published v9 shortlist256/proof32 candidate. The supplied assets support this without changing serving code, but the campaign is **not ready to freeze** until audit `3b1532c09fa642b9` is accepted and the new configuration/reduction path passes admission.

**Compatibility is explicit:**

| Pin | A: historical SOURCE128 | B: v9 shortlist256/proof32 |
|---|---|---|
| Native source | `8910ef50c1cce155eb396d71a2e05dd3b9e906a0` | `ccbb561bec72e97bc18df363ea307616811af264` |
| ELF SHA256 | `59a22eed2e7d3cb8142e35108171cdcd8c27ec83c8968ba307cd9dc092c78083` | `912f23d34dd175f365aab2797896da2c45306bf9dd4faf7d44723c8bdce50995` |
| Config/result/generation versions | v8/v8/v8 | v9/v9/v9 |
| Generation root | `80fb1e8fdea78d72363d28f1406924084227203dd6708d596be3519155b88638` | `460d095ac98f2dba70ff765907ba89d3bd7bbdb03e1cb4781da637ef5d19baf6` |
| SOURCE limits | 128 GETs; 64 MiB | Same |
| SQ8 limits | 32 GETs; 16,773,120 bytes | 30 GETs; 33,554,432 bytes |
| SQ8 proof granularity | 256 rows | 32 rows |

The generation loader requires its exact schema. Using the v9 ELF with `serving.mode="baseline"` does **not** reproduce historical A. Preserve the different SQ8 envelopes as part of the declared treatment; this measures the complete candidate configuration, not shortlist size alone.

I verified the candidate archive SHA256 against its terminal descriptor and matched both runners’ source-file hashes to their expected headers. A metadata check also confirmed the shared workload, backend, native data identities, cohort receipts and request/truth byte/hash pins.

The pinned helper `0845bddd…54fbd`, source `ea3bf2b8c6f81dccf20dbd97023f3fb90bdba342`, supports both versions through `--completed-scale`, including full1000 execution. Its `--completed-v9-panel` command is eight-query-specific. Its `--source-budget-abba-scale` command requires v8 SOURCE128/96, identical normalized identities and exact cross-arm parity, so that command cannot validate this experiment. See [generic reduction](/home/rb/git/borsuk/crates/borsuk/examples/compare_native_replay.rs:3951) and [historical ABBA restrictions](/home/rb/git/borsuk/crates/borsuk/examples/compare_native_replay.rs:4799).

1. **Prepare one new, immutable experiment packet.**

   Proposed directory: `docs/research/performance-architecture-20260930/cohere1024/source-shortlist-library/cold-v9-a0001.pending/`.

   Minimal assets:

   - `protocol.json`: complete arm identities, allowed differences, ordered stages, measurement definitions, decision rules, resource/cost envelope and accepted prerequisite receipts.
   - `transport.tsv`: byte/hash/key/target pins for both serving ELFs, the existing helper, requests, truth and cohort/derivation receipts.
   - Six runtime configs: `admission-{original,candidate}.config.json`, `canary-{original,candidate}.config.json`, `formal-{original,candidate}.config.json`.
   - Six corresponding `*.completed.EXPECTED.json` templates.
   - Frozen support scripts derived from the historical packet: `run-source-shortlist-campaign.sh`, `owned-native-stage.sh`, `supervise-native-stage.sh`, `transport-panel-inputs.sh`, `close-source-shortlist-campaign.sh`. Update arm-specific executable dispatch, stage names, artifact roster and exact hashes; preserve ownership fencing and cleanup checks.
   - `reduce-v9-abba.jq`: a small comparison of **four already validated native summary reports**, with exact roster/config/pin checks and the formulas below. It must never parse result JSONL or recompute population statistics.
   - `packet-inputs.sha256`, covering every static asset above.

   Reuse both published generations. No reconstruction, new data split, serving build or new algorithm is needed.

2. **Freeze the workload and chronology.**

   Both arms use Cohere1M, D1024, cosine, k10, ordinal tie rule, the existing corpus intervals and reserved queries, width64, active queries1, SOURCE cacheoff and absent shared byte window. Preserve these exact input pins:

   - Requests: 4,096,000 bytes; `8460a81ff2f979deff7d82bede874a1301f47dfd3e4589305c9f53e020920d5e`.
   - Truth: 80,000 bytes; `0e38d943adaf62f197b98fe8b01b2edec2008010df760717ababfa5054693b7f`.

   Keep cohort, derivation, producer authority, source/order/SQ8 identities and S3 backend descriptors equal to the retained configs. Bind each arm’s different generation explicitly.

   Formal configs use exactly `execution={"mode":"full"}`. Both expected header sections must agree, and `selected_count=1000`; remove panel ordinals and diagnostic tracing. A1/A2 share one frozen A config; B1/B2 share one frozen B config.

   Exact order:

   `admission-A[527] → admission-B[527] → disposable-canary-A[0] → disposable-canary-B[0] → freeze-check → A1[0..999] → B1[0..999] → B2[0..999] → A2[0..999] → paired-summary → closure`

   Run each native cell in a fresh owned process with a create-only output. Require terminal exit, drained ownership and sealed artifact hash before its completed reduction and the next cell. Record actual starts/ends and instance identity; a declared ABBA array alone does not establish chronology.

   “Cold” means this application-cache protocol. It does not establish S3 server-cache eviction or fresh connections for every query.

3. **Reduce complete cells and apply prospective decisions.**

   After each closed cell, fill only the completed artifact’s path/bytes/SHA into its pre-frozen expected template, then execute:

   `compare_native_replay --completed-scale CONFIG CONFIG_SHA NEW_REPORT`

   Require successful native and manager exits, no forced cleanup/OOM/swap, full1000 coverage, full k10, zero transport errors and valid population statistics. Retain RSS and cgroup evidence separately.

   Use nearest-rank p90/p95 over each cell’s 1,000 query walls, and sequential QPS `1000 × 1e9 / sum(query_wall_ns)`. Compare **B1/A1 and B2/A2 separately**. Report absolute values and percentage changes for p90, p95, QPS, native peak RSS, SOURCE/SQ8/total logical GETs and verified bytes. Retain p99, startup and stage statistics. Keep cumulative SDK transport separate from logical charges and from unknown wire/billed quantities.

   Preregister:

   - A must reproduce **9,780/10,000 hits**; otherwise investigate control reproduction before attributing a candidate win.
   - Each B targets **≥9,780**; **9,760–9,779** passes the prospective floor but misses matched-quality acceptance; **<9,760** fails that floor. Keep execution validity separate.
   - Carry forward the historical modest screen: both paired p95 values improve; neither paired p90 nor QPS regresses more than 5%.
   - Adoption additionally requires both p95 gains to exceed `max(|A2/A1−1|, |B2/B1−1|)` for p95. This is a descriptive repeat reference.
   - Preserve valid quality/resource tradeoffs, including quality gains that miss efficiency targets. Do not require cross-arm returned-ID, SQ8-plan or charge equality for this algorithm change.

   The [historical decision rules](/data/target/borsuk-source-budget96.pending/cold-launch-a0002/decision-preregistration.md) remain immutable. Historical timing values are context; fresh A cells supply the paired denominator.

4. **Resolve the remaining pins before root-owned launch.**

   Outstanding items are acceptance of audit `3b1532c09fa642b9`; final six config/header hashes; support/summary hashes; local serving/helper ELF verification and qualification-receipt authentication; source-archive identities; and the final resource, instance, deadline and funding envelope.

   Keep the library’s 512 MiB admission model. Choose one common external process envelope from accepted v9 RSS/scratch evidence: historical cold used 1 GiB, while actual8 used 2 GiB. Neither automatically establishes the new candidate’s appropriate ceiling. Freeze that choice before measurement.

   Use root-authorized Spot capacity with profile `causality`. On interruption, preserve the failed attempt and discard the incomplete measurement cell; any replacement is a newly recorded attempt, without pooling partial results. Sync terminal repetitions and terminate compute immediately at campaign termination.

**Smallest prelaunch falsifier:** first run a metadata-only packet check under CPU1/256 MiB/swap0/120s. Reject swapped arm ELFs/roots, inconsistent schemas, workload-pin drift, residual panel execution, `selected_count≠1000`, unresolved static placeholders, or dispatch to the historical SOURCE128/96 reducer. This requires no scientific bodies or native execution.

After the pending audit is accepted, the smallest actual-input falsifier is the two one-query admission cells at ordinal527, followed by their generic completed reductions. Then run the separate disposable ordinal0 canaries through the exact launch/SDK/publication/cleanup path. These are real requests, records and truth; they establish execution compatibility, not full1000 recall or performance. Any setup defect is execution INVALID, preserved and repaired before a fresh attempt.

| Simulated perspective—not testimony | Argument, objection and falsifiable prediction |
|---|---|
| Measurement methodology | Fresh paired controls and fixed order reduce drift ambiguity. Two repeats cannot establish significance or remove nonlinear cache effects. Prediction: a robust tail improvement appears in both pairs and exceeds the declared repeat reference. |
| Storage systems | Shortlist256/proof32 may reduce fetched SQ8 payload, while proof handling, bridging or request overhead can offset it. Prediction: completed GET/byte and stage-time reductions explain whether lower payload translates into lower p95/QPS cost. Planned bytes alone cannot establish this. |

No files were edited; no compilation, native/helper execution, AWS/network access, launches or children were used. Runtime compatibility and full1000 performance remain unverified.  
Inspection caveat: one read of `reader.json` inadvertently displayed its per-query array; I excluded those values from this plan. No result JSONL was opened.
