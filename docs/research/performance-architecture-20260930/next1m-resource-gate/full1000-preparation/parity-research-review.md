What this change does: it adds an exact three-run comparison for historical Q32, full1000 local panel, and full1000 S3 panel. It reuses the strict scale reader and authenticates complete request/truth bodies before comparing their prefixes.

**Hold for source fixes.** I reviewed Git blobs at `c9e6f9572148b4e04316146be409ae384442e6ff`, against parent `b4406c98…`. All line references below concern that candidate, not the stale checkout.

**Blocking defects**

1. **Successful parity always exits 2.**
   The new reducer returns `status:"EXACT_PREFIX_PARITY"` at [line 2120](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/examples/compare_native_replay.rs:2120). The shared writer returns success only for `status=="MEASURED"` at line 3366; `main` maps false to exit 2.

   **Smallest fix:** recognize `EXACT_PREFIX_PARITY` specifically for `SCALE_PARITY_REPORT_SCHEMA`, after the existing durability checks. Preserve the distinct status. Otherwise every valid comparison writes a positive report while its command reports failure.

2. **Incompatible arms may claim the same native configuration hash.**
   [The invariant helper](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/examples/compare_native_replay.rs:1932) removes `config_sha256`. The reader validates its syntax and agreement with expected identity, but this path never requires the three hashes to differ.

   Consequently, otherwise valid, independently resealed transcripts can retain one config hash while claiming different count, execution, and backend settings. One authenticated configuration cannot describe those three incompatible executions. Distinct result hashes/inodes do not resolve that contradiction.

   **Smallest fix:** require pairwise-distinct native `config_sha256` values alongside the result-artifact checks. Existing `reduce_paired` already applies this consistency check. Actual configuration-body provenance remains external; this guard does not replace it.

3. **Prefix authentication drops the existing pathname-binding check.**
   [The new helper](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/examples/compare_native_replay.rs:1985) checks only its open descriptors. Unlike `Rows::finish` and `read_config`, it never reopens the pinned paths to confirm their identities.

   Concrete case: after both files are authenticated, rename their containing directory and install a replacement directory at the original path. The open files and their stamps remain unchanged, so comparison can succeed while the frozen paths now identify different bodies. The consumed bytes remain authenticated, but pathname replacement escapes rejection and undermines replay from the retained configuration.

   **Smallest fix:** after comparison, require both paths—opened through the existing safe traversal—to match their authenticated `FileIdentity`. Keep comparison on the original descriptors.

**Additional boundary gap**

4. **“No truth open before seals” assumes disjoint file roles.**
   The ordinary call sequence satisfies the boundary: all three readers finish before either prefix helper runs. However, a configuration can point a run input at a truth file. `read_run_with` then opens and reads that file before validating any seal, although it subsequently rejects the transcript.

   **Smallest fix:** reject role aliases before run reads. Path checks cover direct aliases; descriptor-relative metadata checks are needed for hard links without opening truth contents. This is an early-open defect, not a false-positive parity result.

**Checks supported by the source**

- I found no missing/null coercion bypass. Typed pins reject nulls; removed exemptions must exist; execution geometry is exact. Missing fetch parallelism cannot silently default through this scale path because the field roster and mandatory `source_cache:"off"` checks reject that combination.
- Original ordinals are enforced upstream. Query ordinal, panel slot, and recall ordinal are checked before samples are zipped. `Sample` need not duplicate ordinal storage for this fixed `0..31` panel.
- Ordered IDs and integer score bits are compared exactly. Full-k results, matching recall hits, matching stage charges, and zero failed logical GETs are enforced.
- Non-exempt source, producer, corpus, resource, and fetch-policy fields remain invariant. Generation-root equivalence is correctly left external; matching 32 results cannot prove it.
- Reads and allocations have explicit bounds. I found no new unbounded resource path.
- Reusing the exclusive-create, file-sync, identity-check, and directory-sync writer is appropriate once finding 1 is fixed.

This follows the existing paired-reduction pattern and is preferable to another parser or aggregate-recall comparison. No ANN redesign is needed.

**Narrow positive dispatch fixture**

Place the fixture in the existing `tests` module so it can reuse `v2_fixture`, `encode`, `authenticate`, and the native scale-row patterns.

| Arm | Corpus rows | Input count | Execution | Backend |
|---|---:|---:|---|---|
| Historical | 1,000,000 | 32 | Full | Local |
| Fresh local | 1,000,000 | 1000 | Panel `0..31`, untraced | Local |
| Fresh S3 | 1,000,000 | 1000 | Identical panel | S3 |

Use actual bounded fixture files of the required lengths, with genuine prefix relationships. Give queries distinguishable ordered results, reconcile every seal and terminal total, freeze expected rows independently, and use distinct native config hashes. Fresh local/S3 producer receipts must match.

Exercise the **actual CLI dispatch**, not just `reduce_scale_prefix_parity`. Require exit 0, the exact report schema/status, 32 matches, correct hashes, and false performance/cold/vendor claims. Reusing the occupied output must fail before reduction and preserve its bytes. Synthetic S3 transport records must remain labelled synthetic.

**Negative fixture cases**

Reuse that single fixture and existing mutation helpers:

- At ordinal 31: change an ID, score bit, recall hit, underfill, GET charge, verified-byte charge, or failure count. Reconcile surrounding totals/seals so checksum failures do not mask semantic failures.
- Duplicate/reorder query or recall ordinals; alter panel slots, selected count, execution, or trace mode.
- Change invariant source/producer/corpus/fetch/memory fields; test missing, null, unknown, and duplicate fields. Change only one fresh producer receipt.
- Reuse a native config hash; duplicate result evidence by copy/hard link; corrupt seals, truncate EOF, append a row, or supply an INVALID terminal.
- Test wrong prefixes with freshly correct whole-file hashes, plus stale hashes, suffix tampering, truncation, growth, in-place mutation, and parent-directory replacement.
- Make the third run invalid while observing input opens: requests/truth must remain unopened. Include direct and hard-link role aliases.
- Test occupied/symlink output and failed publication. A positive JSON body alone must never substitute for successful command and durability evidence.

**Known remaining qualification**

The three helper tests are knowingly UNRUN, and dispatch fixtures, remote compilation, and fresh pipeline qualification remain outstanding.

The stored closed Q32 evidence supports the historical arm: 314/320 hits, zero underfill, and recorded logical charges. Existing native fixtures cover real generation/query/seal/recall execution, independent score oracles, original panel ordinals, and local versus stub-backend equality. These are suitable complements to the dispatch fixture; there is no reason to loosen the production geometry for a tiny test.

They do **not** establish the fresh three-run gate. That still needs authenticated fresh inputs, external generation/publication provenance, and successful execution on frozen source. The stored summaries alone are insufficient substitutes for the original sealed transcripts and receipts.

Source inspection found no definite compilation defect: `Seek` is used, comparison traits exist, and test dependencies are present. This is not compilation proof. Run the acknowledged affected tests, locked release build, required Clippy, and actual shim-unset workspace test-build on causality EC2 Spot after the fixes are frozen.

**Verdict: fix 1–4 before qualification.** No files were edited; no native/data/mock/validator execution, network access, delegation, or consultations occurred.
