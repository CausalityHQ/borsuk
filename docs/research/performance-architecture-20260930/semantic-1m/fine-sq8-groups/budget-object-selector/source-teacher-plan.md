# Authenticated source teacher and held evaluation plan

Read-only consultation `5a0580485e2f4569`, completed exit 0. This plan does not establish native implementation, recall, latency, or serving qualification.

Implement one closed Rust source-panel evaluator around the fixed fitter. Evidence is from clean checkout `5151b26e`; its fitter, selector and registration match `69398997`. Native qualification remains pending.

1. **Own exactly three Rust paths.**

   | Path | Change |
   |---|---|
   | `crates/borsuk/src/budget_object_source.rs` | New authenticated admission → training teachers → fixed fit → sealed held evaluation pipeline, with inline tests. |
   | [fine_sq8_groups.rs](/tmp/borsuk-fitter-qualification-a0001/crates/borsuk/src/fine_sq8_groups.rs:146) | Add a crate-private metadata accessor using the existing `Manifest`, `validate_manifest` and `layout_identity`; expose existing `secure_file` crate-wide. |
   | [lib.rs](/tmp/borsuk-fitter-qualification-a0001/crates/borsuk/src/lib.rs:10) | Register the new module. |

   Proposed interfaces:

   ```rust
   pub(crate) fn budget_object_sources(
       root: &Artifact,
   ) -> fine_sq8_groups::Result<BudgetObjectSources>;

   pub fn evaluate_source_panel(
       root: &Artifact,
       budget: Budget,
       keep_going: impl FnMut(SourceProgress) -> bool,
   ) -> Result<SourcePanelReceipt, SourcePanelFailure>;
   ```

   `BudgetObjectSources` contains the existing identity, original `BuildConfig`, primary-root/records/groups/order descriptors and coefficient vectors. The public pipeline fixes production geometry and protocol; smaller geometry exists only behind `cfg(test)`. Keep authenticated inputs, teachers and frozen fitted state private, without `Deserialize` or public constructors.

2. **Authenticate the retained population before producing teachers.**

   Reuse [Artifact and secure small-artifact reads](/tmp/borsuk-fitter-qualification-a0001/crates/borsuk/src/hierarchical_semantic_cells.rs:2273), [fine manifest validation](/tmp/borsuk-fitter-qualification-a0001/crates/borsuk/src/fine_sq8_groups.rs:186), and [secure_file](/tmp/borsuk-fitter-qualification-a0001/crates/borsuk/src/fine_sq8_groups.rs:57). Do not open the PQ graph or nomination machinery.

   Bind the trusted fine-root descriptor and every consumed dependency: original generation/plane/primary metadata, canonical source, original order/SQ8, fine order/records/group hashes, and exact coefficient bits. Use the existing descriptors in the immutable [ReLAION manifest](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/paired100k/a0002/screen/fine/layouts/relaion/manifest.json:1) and [CoHere manifest](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/paired100k/a0002/screen/fine/layouts/cohere/manifest.json:1). Paths alone confer no identity.

   Required layouts at `N=100000, D=768`:

   - Canonical: `LEi64 ID + 768 LEf32`, **308,000,000 bytes**.
   - Original and fine orders: physical-to-logical `LEu64` permutations, **800,000 bytes each**.
   - Original and fine SQ8: `LEi64 ID + LEf32 stored norm + 768 code bytes`, **78,000,000 bytes each**.
   - Fine group hashes: **6,250 × 32 bytes**; each group contains exactly 16 consecutive fine records.

   Pre-admit lengths and allocations before opening. Stream through `NOFOLLOW|NONBLOCK` regular-file handles, checking exact reads, SHA256, explicit EOF and final descriptor length on the **same handle**. Existing fine `authenticated_file` lacks the explicit EOF check; copying that behavior unchanged is insufficient.

   Validate both order bijections and every row ID. Retain authenticated fine records in memory; stream original SQ8 and compare **complete record bytes by logical ID**, using the two inverse maps. Stream canonical once, retaining only the 320 selected source vectors. Validate coefficient bit parity against pinned generation/primary metadata; never recalibrate or reconstruct SQ8.

   Crucial binding: a teacher winner belongs to `fine_physical_ordinal / 16`, **not** canonical ordinal or logical ID divided by 16. The existing builder demonstrates the necessary permutation and complete-byte parity checks at [fine_sq8_groups.rs:440](/tmp/borsuk-fitter-qualification-a0001/crates/borsuk/src/fine_sq8_groups.rs:440).

3. **Generate exact teachers and fit only training observations.**

   Select the first 256 anchors and next 64 from the frozen SHA ordering, with logical-ID ties. Normalize source queries through [cosine_vector](/tmp/borsuk-fitter-qualification-a0001/crates/borsuk/src/sq8_source.rs:22).

   For each training anchor, call [rank_returned_ranges_excluding](/tmp/borsuk-fitter-qualification-a0001/crates/borsuk/src/returned_sq8.rs:139) over **one complete authenticated fine-record range**, excluding its self-ID and requesting 100. This exhaustively invokes the unchanged [score_nominees kernel](/tmp/borsuk-fitter-qualification-a0001/crates/borsuk/src/exact_sq8_nominee.rs:49): sequential f32 squared-distance arithmetic, **score ascending, ID ascending**.

   Retain the ordered top100 IDs and physical ordinals. Aggregate integer counts into ascending `GroupWeight` entries; each positive count must fit its physical group and the sum must equal 100.

   Pass only these 256 observations to unchanged [FitInput/fit](/tmp/borsuk-fitter-qualification-a0001/crates/borsuk/src/budget_object_fitter.rs:24), with `requested_rows=100`, the identical frozen serving budget, and `source=raw32 canonical SHA`. Bind snapshot coefficients to an explicit domain-separated hash of dimension and LE coefficient bits; use an explicit nonzero “no delta” identity.

   Preserve contiguous 56-group initialization, `s=11`, both fixed rounds and every existing optimizer/proposal/acceptance rule. Initialization is already unambiguous at [initialize](/tmp/borsuk-fitter-qualification-a0001/crates/borsuk/src/budget_object_fitter.rs:270): ASCII `BORSUK-budget-fitter-init-v1`, LEu64 seed `20260923`, canonical raw32 SHA, then LEu64 parameter index.

4. **Seal the fit, then evaluate the held population.**

   Require successful completion of both rounds. Seal source/layout, training, model, membership, snapshot and budget identities **before constructing held teachers**.

   Construct the 64 held exhaustive teachers by the same procedure. Rebuild `Model`/`Membership` from the sealed fit and run unchanged [select](/tmp/borsuk-fitter-qualification-a0001/crates/borsuk/src/budget_object_selector.rs:790). Enumerate every physical group owned by selected labels; borrow exactly those complete group slices, sorted by original fine offset, and rescore **all selected rows** through `rank_returned_ranges_excluding`. Exclude self again; fewer than 100 returned rows is underfill.

   Record coverage as a diagnostic, but compute agreement from **returned top100 ID intersection with exhaustive top100**. Coverage alone cannot establish ranking correctness.

   GO requires all 64 completed evaluations, total hits **≥6368/6400**, fourth-smallest hits **≥99**, and no underfill or budget refusal. Preserve maximum-body reservations of **798,784 bytes per selected object**, at most 15 objects, and total **≤32 modeled reads /16 MiB**, including explicit head/root/delta/attempt reservations.

   These are virtual complete-object charges. Resident source slices and fitter diagnostic tokens do not establish actual GETs or authenticated newly published objects.

5. **Keep failures authoritative and accounting separate.**

   Maintain source-authentication, teacher, fitting and held-evaluation ledgers, including failed work and opaque scorer scratch reservations via existing [query_payload_bytes](/tmp/borsuk-fitter-qualification-a0001/crates/borsuk/src/returned_sq8.rs:81). Charge actual retained capacities and simultaneous adapter/fitter storage; borrowed observations must not disappear from aggregate accounting.

   Preserve **512B fitting operations**, **8 GiB aggregate RAM**, **8 GiB scratch**, **4 CPU**, **no swap**, and **1800 seconds for the complete panel**. Poll the shared deadline during streams, between teacher/scoring calls and through the existing fitter callback. Host enforcement remains independently required.

   - Authentication, malformed input, identity/numeric errors, or missing resource enforcement: **INVALID**.
   - Correctly enforced algorithm ceilings, serving refusal, underfill or agreement rejection: **FAIL**.
   - An unfinished stage returns only failure reason, stage, identities and consumed ledgers—no qualified receipt or partially trained output.

6. **Leave a bounded falsifier with four inline checks.**

   - **Independent teacher oracle:** `N=129, D=3`, nonidentity source/fine permutations, nonunit query, unequal coefficients/norms, self exclusion, ties crossing rank100 and a short tail. Independently reproduce scalar arithmetic and sorting; assert score bits, exact top100 IDs and group counts. At least 28 eligible rows are omitted, so the check is nonvacuous.
   - **Split isolation:** independently establish first256/next64 ordering and disjointness. Through a test-only evaluation seam, change held evidence after sealing; held disposition changes while fitted hashes and event sequence remain identical.
   - **Selected-population oracle:** a small multi-object fixture with a one-body budget; independently enumerate selected rows, rescore and check omitted-neighbor loss, post-self underfill and exact read/byte boundaries.
   - **Admission/refusal table:** corrupt digest, truncation/appended bytes, symlink/FIFO, duplicate/mismatched IDs, group hash and one coefficient bit; also force resource exhaustion. Assert INVALID versus FAIL and absence of completed authority.

   Later verification: run the new module’s native tests, required workspace Clippy, and `bash scripts/check_rust_test_build.sh` on the exact implementation revision. The first real-input falsifier is one frozen 256/64 panel; stop that attempt on its terminal result, without a parameter ladder. None of these commands is authorized or executed here.

The main risks are permutation mistakes, counting coverage instead of returned agreement, and exhaustive scoring plus fitting exceeding the panel ceiling. A completed source pass still leaves original-GT recall, lifecycle, 100M scalability and vendor comparisons unqualified.

**One unavoidable root freeze:** [native-spec.md:46](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/budget-object-selector/native-spec.md:46) omits the literal anchor domain and seed placement. Recommend preserving the existing [anchor_key](/tmp/borsuk-fitter-qualification-a0001/crates/borsuk/src/co_selection_layout.rs:347): `borsuk-co-selection-anchor-v1\0 || canonical raw32 SHA || LEu64 logicalID`; seed `20260923` remains in fitter initialization. Record that convention before implementation; do not silently choose another ordering.

No files, data bodies, native commands, network operations or live qualification jobs were touched.

## Root clarification before implementation

Anchor ordering reuses the existing `co_selection_layout::anchor_key` convention: SHA256 of ASCII `borsuk-co-selection-anchor-v1\0`, followed by the canonical source SHA256 decoded to 32 bytes, followed by the logical ID as LEu64. Sort hash ascending, logical ID ascending on ties; first 256 train, next 64 held. Seed 20260923 controls fitter initialization, not an additional anchor-hash input. This fixes the previously unspecified domain and seed placement before any source-panel execution. Existing historical results remain unchanged.

