**Existing evidence suffices to infer that all six original omissions first occur at leaf nomination.** It does not identify why those leaves were excluded or select a replacement algorithm.

At qualified source `240a570d`:

- `semantic_unit_router.rs:1051–1170`: `nominate_selected` retains every unit belonging to selected leaves; seed completion only adds units.
- `compare_native_replay.rs:4640–4746,5167–5178`: control validation binds that nomination to the original producer and requires every unit in the complete physical page closure to be SOURCE-scored.
- `budgeted_page_rank.rs:28–97`: SOURCE cover preserves every closure page, adds bridges, or fails admission.

Therefore, an unscored target lies outside that closure, and its unit cannot belong to a selected leaf. The closed native loss report establishes exactly six such targets: q742×2, q823×3, q991×1. A within-leaf truncation or SOURCE-cover omission would contradict the validated invariants. q537 remains a separate covered ranking loss.

One actionable plan: **add a report-only reduction mode to `compare_native_replay.rs` that makes this attribution explicit from preserved artifacts.** No generic public API change is necessary.

1. On causality EC2, authenticate the preserved selection seal, native loss report and original configuration against their immutable hashes. Reuse `coverage_router` for generation/root/membership binding. Check matching producer, SQ8, physical-order and truth identities, and the exact `CoverageScope::consumed32()` roster before consuming target entries. Keep the historical sealer’s source identity distinct from the new reducer’s identity.

2. Reconstruct only the **original L2 nomination**, using `nominate_selected` on the sealed leaf IDs; compare its units, walk, seed additions and closure exactly with the preserved seal. Recompute SOURCE cover through `plan_two_bit_source_cover` at unchanged 128 GET/64 MiB limits. This requires no query ranking, SOURCE scores, ANN requests or cosine replay. Read target IDs/physical positions from the authenticated, already-reduced loss report; no fresh truth or SQ8-body pass is needed.

3. Emit, for each existing truth slot: `ordinal`, `id`, `physical_row`, `unit_id`, `page_id`, selected-leaf membership, walked membership, closure membership, SOURCE transport coverage, original SOURCE-scored flag, original SQ8 coverage, returned flag and attribution. Selected-leaf membership can use the complete unit union returned by the existing API; an owning-leaf getter is unnecessary. Treat nominated-but-unwalked units and uncovered closure pages as **INVALID**, rather than valid loss categories.

The bounded falsifier is one offline reduction over all 32 preserved queries on fresh causality Spot EC2: CPU0/100%, 512 MiB RAM, zero swap, pids128, ≤60 seconds native runtime, output ≤4 MiB. Require 320 disjoint slots, SOURCE coverage314, returned313, precisely the six nomination exclusions above, zero valid walk/SOURCE-cover/SQ8-admission omissions, and q537’s covered ranking loss. Any contradictory authentic slot falsifies this attribution and stops algorithm selection. Preserve terminal receipts and terminate compute immediately.

Add small Rust fixtures covering complete nomination, page-completion rescue, fetched-but-unscored SOURCE bridges, partial tails, and rehashed inconsistent selections. Before handing off an increment, qualify its exact revision on EC2 with the affected example tests, required workspace Clippy, and `bash scripts/check_rust_test_build.sh` with bounded compiler concurrency.

The main interpretation risk is that coverage-report `l2_fetched` means **SOURCE transport coverage**, while loss-report `sq8_ranked` means actual SQ8 cover; bridges must remain separate. Fable’s historical summary-hierarchy and copy-cost rejections constrain candidate selection, but their numerical results do not transfer to this Cohere workload.

No files were edited or data decoded; no compilation, tests, network, cloud or child jobs ran. The conclusion identifies the first lost layer, not its geometric cause or an ANN/cold-performance win.
