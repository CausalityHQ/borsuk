# Exact-layout nomination staging review

Consultation: `fe19bfed720e4b82`; GPT-6.1 Sol, XHigh; completed read-only. No native or scientific execution. Root keeps authorization and source-freeze authority; the separate coverage reducer is already assigned and must not be duplicated.

Use a **new standalone reconstruction/nomination runner plus an explicit probe mode in the existing launcher**. Leave `prepare_hierarchical_cells_100k.py` unchanged.

The current [hook](/home/rb/worktrees/borsuk-prod-ready-v9/scripts/prepare_hierarchical_cells_100k.py:591) always continues into `diagnose`; raising an exception marks INVALID and deletes recovered directories. Its [proof validator](/home/rb/worktrees/borsuk-prod-ready-v9/scripts/prepare_hierarchical_cells_100k.py:187) also binds original source pins to the current checkout. Neither is a valid successful stop seam.

| Owned Python path | Responsibility |
|---|---|
| New `scripts/run_hierarchical_global_leaf_probe.py` | Exact reconstruction, nomination validation, paired sealing; separate `--reduce` invocation; bounded synthetic self-checks. |
| Modify `scripts/launch_hierarchical_cells_100k_spot.py` | Explicit probe staging/canary/replay branch, two qualification roles, archive/artifact rosters, resource enforcement and shared lifecycle. |

1. **Freeze two distinct binary authorities.** Reconstruction uses the original proof’s source revision `2638caae…`, writer SHA `adde84b8…`, and cells SHA `a2d88fb0…`. Nomination uses separately completed qualification for candidate `15323a1…`, including its new named tests, release binary, workspace Clippy and actual test-build gate. Validate each against its own archive/source inventory; never require their module hashes to match. Bind the runner and launcher source separately. Preserve the existing qualification job.

2. **Restore original paths and config bytes.** Reserve `/mnt/hierarchical-100k` exclusively on the disposable worker. Deploy new orchestration separately, for example under `probe-repo`; reserve original proof/source paths under `repo`. Reject existing or symlinked destinations. Decompress the archived writer/build configs **without JSON reserialization**, authenticate their original sizes/SHA values, and restore their original absolute filenames.

   Execute, sequentially for each dataset:

   ```text
   ORIGINAL_WRITER ORIGINAL_WRITER_CONFIG ORIGINAL_CONFIG_SHA 67108864 ORIGINAL_GENERATION_DIR
   ORIGINAL_CELLS build ORIGINAL_BUILD_CONFIG ORIGINAL_CONFIG_SHA ORIGINAL_CELLS_DIR
   ```

   Preserve original reconstruction scratch environment and arguments. Authenticate raw/order/SQ8 first, then every generated descriptor—including generation, plane, canonical, mean and records—against [recovery admission](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/hierarchical-cells/paired100k/global-leaf-layout-recovery-admission.json). Require both original generation hashes and both cell roots (`fec06fb0…`, `89aee41e…`) before any query. Any discrepancy is INVALID.

3. **Produce nominations without entering the diagnostic pipeline.** The probe staging branch must bypass existing [panel production](/home/rb/worktrees/borsuk-prod-ready-v9/scripts/launch_hierarchical_cells_100k_spot.py:325): it downloads, opens and reduces truth. Restore only authenticated source assets and the exact consumed request panels; historical truth descriptors may remain archival metadata.

   Run the new binary twice, sequentially:

   ```text
   QUALIFIED_NEW_CELLS nominate NEW_CONFIG NEW_CONFIG_SHA NEW_JSONL
   ```

   Each strict `borsuk-hierarchical-cells-nomination-v1` config selects the exact recovered root and original first64 requests. Each output must contain **128 ordered selection events**, hierarchy then global for every ordinal, followed by the verified prefix marker and successful terminal.

   Check source descriptors against the original parsed build config. `source_identity_sha256` hashes Rust serialization; it must not be confused with the original config-file SHA.

4. **Validate and seal before enabling reduction.** Check selected prototypes and source spans against authenticated directories; verify range hashes and source-record ID order from `cells.bin`. Reconstruct primary/covered sets from those IDs, never from `first_row`. Require hierarchy primary/covered parity for all64 original closed traces per dataset. Old traces bind request-panel SHA and ordinal; they contain no per-query hash. Validate the new query hash against original little-endian f32 bytes.

   Require zero per-query directory/whole-cell/refinement reads, 24 authenticated source reads per arm, primary8/union24, and admitted budgets. Authenticate original diagnostic bodies against their closed terminal, parsing only the truth-free trace prefix.

   After both nomination processes exit successfully, validate and hash **both whole files, including markers and terminals**. Create and fsync a new paired seal binding both files, roots, configurations, both binary authorities, request identities and parity receipts. A per-dataset prefix marker alone is insufficient.

5. **Close execution, retain evidence, then reduce independently.** Keep reconstruction within the frozen 2GiB/16GiB/zero-swap safety envelope and one owned native process at a time. Enforce nomination’s separate 512MiB, CPU1, zero-swap envelope; payload accounting is not a host-memory limit. Collect actual exits, process/cgroup closure, peaks and deadline evidence.

   Extend the deployed archive beyond native qualification: include the new runner, transitive controllers, preregistration/recovery evidence, terminal-bound original configs/traces and both qualification authorities. Use a separate probe schema, prefix and artifact roster; do not reuse the old 18-GET contract.

   Retain authenticated copies of each recovered `manifest.json`, `directories.bin` and `cells.bin` **before cleanup**. The current [cleanup](/home/rb/worktrees/borsuk-prod-ready-v9/scripts/launch_hierarchical_cells_100k_spot.py:632) deletes all measurement directories, so the probe branch needs explicit artifact ownership.

   Reuse shared Spot reservation, terminal-last upload, exact-instance termination/wait and collection. Its canary must cover real imports, SDK models, CLI exit handling and cleanup, with clearly labelled mocked versus real transport. It performs no native query or truth-body read and supplies no performance evidence.

   Only a subsequent `--reduce` process receives GT descriptors. It verifies the paired seal and both whole files before opening GT, performs no native calls, and computes coverage, per-query differences and fourth-lowest-of64 p05. Preserve a0001’s disposition and the consumed-panel interpretation.

Five cheap falsifiers:

- **Role/config drift:** mock invocation recording; change one original config byte, path or binary role. Reject before spawning.
- **Accidental GT access:** install failing spies on truth-body opens, `diagnose`, old panel production and reduction during reconstruction/nomination.
- **Source-ID confusion:** synthetic shuffled IDs where `first_row` differs from membership; altered spans, hashes or duplicate IDs must fail validation.
- **Incomplete/tampered pair:** fail dataset two, truncate a terminal, alter one hierarchy ID or change a byte after sealing. Reduction must refuse before its first GT open; tracked processes must close.
- **Root-authorized real noGT admission:** after native gates, use exact recovered roots and real request64 panels for one bounded paired nomination check. Require all256 selections, control parity, source-range validation and resource closure. Missing exact assets is a blocker, never permission to substitute fixtures.

Remaining root decisions: freeze the integrated source and two qualification receipts; approve exact asset transport, phase deadlines/cgroup arrangement, retained-artifact budget and separate GT-reducer configuration.

Read-only inspection only. No files edited, native/corpus/GT/AWS execution performed, or children/consultations started; `exec29072` was untouched. Code and science remain unverified.

