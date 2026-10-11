Recommend **widest-axis, quota-balanced spatial bisection**, exposed as an experimental builder option. Keep radial chunking as the default pending qualification.

This plan uses `git show 4891e87f74aa6101e701f8e1e9966b36005e25ea`, including the committed observations. Those observations motivate testing fragmentation; they do not establish its geometric cause or a coverage improvement.

**1. Exact partition rule**

In [semantic_unit_router.rs](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/semantic_unit_router.rs), preserve the trainer, `assign_groups`, center-ordinal ties and original group ordinals. Replace only oversized groups’ `.chunks(64)` behavior:

- Empty groups emit nothing. Groups containing at most 64 units retain their existing radial order and output.
- For an oversized group of size `n`, allocate exactly `L = ceil(n/64)` terminal leaves.
- At each node, choose the coordinate with greatest `max − min`, computed in `f64` from the existing decoded FP16 unit means. Equal spans choose the lowest coordinate index. **There are no seeds, random choices, sampled anchors or additional training.**
- Sort the node’s unit references by that coordinate ascending, then original unit ID. Treat signed zeros as numerically equal before the ID tie-break.
- Split by leaf quotas:

  ```
  left_leaves = floor(L/2)
  q = floor(n/L)
  r = n mod L
  left_units = q*left_leaves + min(r, left_leaves)
  ```

  Recurse on the first `left_units` references and the remainder, carrying their respective leaf quotas. Visit left before right.
- When `L == 1`, restore the terminal leaf’s existing `(distance to original trained center, original unit ID)` order before invoking unchanged payload emission and `prototype`.
- If every coordinate span is zero, split by original unit ID using the same quotas. Nonfinite coordinates remain admission errors.

This produces exactly the radial control’s number of leaves per group, each no larger than 64, with terminal sizes differing by at most one. For example, 129 units become `43/43/43`, avoiding ordinary median recursion’s four leaves.

Preserve original FP16 payload bits, unweighted `f64 → f32` prototypes, source physical positions, L2 top48 nomination, scoring and physical page-order rules. Selected pages may change because membership changes; the ordering policy must not.

**2. Causal comparison and resource cost**

Radial chunking orders distance from one center without distinguishing direction. The proposed rule separates coordinates inside that same assigned group. The hypothesis is that the resulting leaf means represent local neighborhoods better at fixed top48.

The intervention also changes occupancy distribution and therefore prototype norms and selected-unit counts. These are consequences of the partition change and must be reported; equal leaf counts do not guarantee equal fetched bytes or better coverage.

For a group of `n` units, dimension `D`, and `H = ceil(log₂ ceil(n/64))`, straightforward in-place sorting costs:

- `O(nD·H + n log(n)·H)` partition work.
- `O(n log 64)` terminal radial reordering.
- `O(D + L + H)` additional builder scratch when reusing the existing group vector.

Publication replay can use one group-sized reference buffer, adding `O(n_max)` scratch. Update checked allocation accounting for actual simultaneous capacities. Preserve the existing total-leaf bound `2*requested_centers − 1`: the number of leaves per group is unchanged.

Measure construction time, publication-validation time, peak RSS, resident router bytes and query scratch separately. No arbitrary 1 GiB scientific rejection. Any changed admission envelope must be explicit and justified; operational CPU, RAM, wall-time and cost ceilings remain finite.

**3. Implementation ownership and authentication**

One bounded Rust implementation owner should handle the partition and its artifact validation together.

| File | Concrete responsibility |
|---|---|
| `crates/borsuk/src/semantic_unit_router.rs` | Partition helper, experimental API, format metadata, accounting, validation and unit tests. |
| `crates/borsuk/examples/compare_native_replay.rs` | A distinct partition comparison mode using the existing authenticated coverage machinery. |
| `crates/borsuk/src/two_bit_build.rs` | Verify existing default construction/import paths against the new format; retain radial default construction. |
| `crates/borsuk/src/bin/build_semantic_unit_router.rs` | Update affected format fixtures/tests. Its historical Native100k adapter should not become the Scale1m experiment runner. |

Proposed public surface:

```rust
pub enum LeafPartition {
    Radial64V1,
    WidestAxisQuotaV1,
}

pub fn build_with_partition(
    blob: &[u8],
    input: &SourceIdentity<'_>,
    allocation_limit: usize,
    partition: LeafPartition,
) -> Result<RouterArtifacts>;
```

Existing `build(...)` delegates to `Radial64V1`. Keep recursion and emission helpers private; do not add tuning parameters.

Use **BORSUSR3**, with an authenticated partition code at header bytes `88..92`: `1` for radial-v1, `2` for spatial-v1; retain zero-reserved validation for `92..128`. The format/version and code determine the complete algorithm contract. Encode must reject inconsistent descriptive `Algorithm` fields rather than silently discard them. Reject BORSUSR2 and unknown codes clearly; historical artifacts remain with their frozen reader/source.

Two reader locations need coordinated changes: `decode_root` and `SemanticUnitRouter::open` currently require a preceding same-group leaf to contain exactly 64 units.

- Radial mode retains that rule.
- Spatial mode checks contiguous group/leaf numbering and the exact quota-derived size sequence from each group’s total units.
- Both retain complete membership, row accounting, hashes, ranges, finite prototypes and source-identity checks.
- `validate_publication` retains original-FP16 and bitwise prototype verification. For spatial artifacts, reconstruct each stored group from membership, replay spatial splitting against authenticated source centroids, and compare ordered leaf memberships. This checks the partition without retraining k-means; it does not independently prove the original center assignment.

Regenerate a radial BORSUSR3 control from identical authenticated inputs and require membership, leaf payload and prototype-bit parity with the frozen radial control. Its new root hash is expected; historical evidence must not be relabelled or overwritten.

**4. Bounded synthetic falsifier**

Add a deterministic test at the **post-assignment seam**, using real BORSUCP1 bytes, production partition/emission code, publication validation, authenticated open and production L2 top48 nomination:

- Scale1m profile, `D=2`, 3,200 units, full 32-row units.
- One fixed oversized group contains 128 units alternating `(1,0)` and `(-1,0)` in physical unit order, with equal radius to center `(0,0)`.
- Forty-eight other fixed groups each contain 64 units at `(0.5,0)`.
- Query is `(1,0)`. Physical pages containing the first group are disjoint from the distractor pages.

Predicted assertions:

- Radial control creates two zero-prototype leaves. All 48 distractor leaves rank ahead of them; the target group’s pages are absent.
- Spatial splitting creates prototypes `(-1,0)` and `(1,0)`. The positive leaf enters top48, and its target pages enter closure.
- Both arms contain 50 leaves, preserve every unit exactly once and retain identical source bytes.
- Assert the displaced distractor leaf and resulting page-set differences too; the test must expose losses as well as recovery.

This deliberately fixes post-assignment groups to isolate chunking. It **does not claim that the frozen trainer produces this grouping**. Add a separate small full-builder round-trip test through the real trainer for integration and determinism.

Required edge tests: sizes `0/1/64/65/127/128/129/193`, identical vectors, coordinate ties, signed zeros, partial final source units, dimensions `1/1024` and rejected `1025`, repeat-build byte identity, malformed mode/version, invalid balance, altered membership, FP16 and prototype bits.

Run this falsifier only on the root’s `causality` EC2 lane: one runtime process, one CPU, 256 MiB, no swap, 120-second deadline after compilation. A wrong predicted result stops progression; passing establishes the mechanism only.

**5. Gate sequence and decision**

1. Compile affected targets and run targeted tests remotely. Before integration, pass the required workspace Clippy command and `bash scripts/check_rust_test_build.sh` on the exact delivered revision.
2. Authenticate exact real inputs through the runtime validator, then pass a separate disposable canary. Seal both constructed partitions before opening requests; durably seal paired selections before opening truth. Preserve original producer/control parity checks.
3. Run one frozen consumed32 comparison: radial L2 top48 versus spatial L2 top48. Count **SOURCE-scored closure**, excluding fetched bridge-only pages.
4. Require control `314/320`, candidate **at least `315/320`**, and **gains > losses**. Preserve existing quality thresholds and historical cosine48/boundary40+8 `312/320` KILLs.
5. Only a surviving candidate proceeds to returned-ID validation, fresh held-out native quality and subsequently cold serving/lifecycle measurements. No adoption follows from the consumed panel alone.

For the first real construction attempt, preregister a finite root-owned Spot envelope—for example, at most 4 vCPU, 8 GiB, zero swap, 30 minutes and $2—subject to the existing host safety limits. Preserve terminal receipts and terminate compute immediately. Setup/protocol failures are INVALID; valid quality failures remain KILL.

The principal risks are axis sensitivity and outliers, unchanged cross-k-means-group fragmentation, prototype-norm changes, reduced selected-unit counts, and increased physical-page dispersion. Source centroids may also conceal useful row-level structure.

No files were edited, geometry bodies read, experiments run, or consultations/children started. This is an unexecuted design, not an adoption recommendation.
