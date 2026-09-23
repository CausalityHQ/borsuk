# ReLAION-1M Page-Representative Selector Plan

**Decision:** Test one fixed source-only page-centroid routing signal on the
frozen 1M development cohort. The eight-page group, 32-GET/16-MiB code
projection and two-bit row representation remain fixed.

**Spec:** `docs/superpowers/specs/2026-09-23-one-million-page-representative-selector-design.md`.

## 1. Source-only artifact

- Write focused tests for physical page grouping, duplicate/missing IDs,
  source-order batch stability and a changed centroid or membership byte.
- Add a new page-centroid builder/seal. Authenticate five source-side inputs,
  parse every V85 page, stream source parquet, sum exact source vectors by
  page in float64, round once to `<f2`, and emit canonical arrays/seal.
- Keep the previous single-centroid source and artifacts immutable. Run only
  focused tests and Ruff while iterating.

## 2. Query selector and independent replay

- Test equal page/group scores, 32-group stable ties, all GT100/GT10
  denominators, p05 nearest rank, byte projection and evidence mutation.
- Rank page centroids in float64, aggregate group minima, select 32 groups,
  and write all ordered samples and fixed aggregate decision in canonical
  JSON. Reuse frozen input identities and query/truth authentication.
- Write independent replay that rebuilds source/page centroids and recomputes
  every query plan without the producer scorer. Compare source arrays, every
  sample, metrics and decision. Run focused tests, Ruff and diff check.

## 3. One immutable Spot screen

- Extend the tested source-only Spot lifecycle for this page selector with a
  fresh output namespace and terminal schema. Seal/upload/readback source
  arrays before downloading query/truth; use create-only artifact writes,
  networkless unprivileged evaluation, exact frozen identities, a 3-GiB
  per-phase cap and zero-swap checks. Test shell syntax/size, phase boundary,
  terminal exit and prefix collision.
- Run the final focused gate once, commit/push the frozen source after
  fast-forward ancestry check, and archive `git archive HEAD` plus source
  commit sentinel. Upload the archive create-only and verify readback.
- Check matching EC2 inventory and output prefix before launch. Start exactly
  one Causality Spot controller session. Observe only terminal markers and
  infrastructure health until it closes; collect original exit and prove
  termination.
- Authenticate every terminal-listed artifact, recompute all 1,000 samples
  and resource bounds, append the fixed decision and next gate to the ledger,
  then commit/push that evidence.
