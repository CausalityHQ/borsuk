# V23 Leaf-to-Page Incidence Falsifier Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a local, claim-ineligible 65,536-leaf corpus-trained page-incidence falsifier with capability-separated construction, burned-query development selection, and one sealed untouched holdout.

**Architecture:** A private Rust subsystem authenticates inputs, trains the deterministic balanced spherical tree, externally builds capped leaf-to-page postings, and evaluates the frozen 18-cell ladder. A dedicated local-file example exposes phase-specific commands; a Python launcher creates the allowlisted offline namespace, chains receipts, and refuses to reveal later-phase inputs early. No production search, Storage, object-store, AWS, page-fetch, or D3 API is added.

**Tech Stack:** Rust 2024, Arrow/Parquet 58.3, `half`, architecture-specific `core::arch` fused-f32 intrinsics, `rayon`, `serde`, `sha2`, `blake3`, Python 3.12 standard library, util-linux namespace/mount tools.

**Spec:** `docs/superpowers/specs/2026-08-30-v23-leaf-page-incidence-falsifier-design.md`

## Global Constraints

- Every result is `claim_eligible=false`; no task authorizes paid compute, D3, or a publication claim.
- Training sees only the frozen construction manifest and raw training shards; posting sees only the sealed tree/receipt plus roster/page bodies; development sees only the sealed router plus D2 evidence and query ordinals 0--31; holdout truth/evaluation is unavailable until the chosen development cell is sealed.
- Use exactly 2,097,152 reservoir rows, a depth-16 balanced tree, four Lloyd passes, 65,536 leaves, f16 round-to-nearest-ties-even centroids, and the spec's fixed eight-lane f32 FMA score.
- Build both one-path and width-two beam posting arms, one max-2,048 posting plane per arm, and authenticated 512/1,024/2,048 prefix views via bounded 256-way external partition/sort.
- Evaluate cells in cap 512/1,024/2,048, assignment one/two, probe 32/64/128 lexicographic order; only the first fully passing development cell reaches holdout.
- Enforce exact-eight-page selection, 975,000/800,000/995,000 ppm quality, 3 GiB serving projection, 262,144 visits, 8,192 observed touched pages, and 15,000,000 ns warm native p99.
- Enforce 2 GiB build RSS, 2 GiB scratch free-space, PSI/swap/progress/two-hour stops, exact digest chains, deterministic scalar/SIMD equality, and canonical newline-terminated receipts.
- Use `apply_patch` for edits, preserve configured Git identity, never add AI attribution, and push verified slices explicitly with `git push origin HEAD:main`.

---

## File Structure

- Create `crates/borsuk/src/v23_incidence.rs`: registered constants, phase/object identities, canonical receipts, campaign classification, and high-level phase requests.
- Create `crates/borsuk/src/v23_incidence_tree.rs`: reservoir selection, canonical reductions, tree encoding/decoding, scalar/SIMD split score, and one/two-leaf assignment.
- Create `crates/borsuk/src/v23_incidence_postings.rs`: fixed records, bounded partition/run merge, top-2,048 prefixes, u16 mass quantization, and posting artifact codec.
- Create `crates/borsuk/src/v23_incidence_eval.rs`: query/neighbor authority, scalar/SIMD page scoring, oracle recomputation, development selection, holdout binding, and campaign results.
- Modify `crates/borsuk/src/lib.rs`: declare the private modules and doc-hidden local-file phase boundary only.
- Create `crates/borsuk/examples/v23_leaf_page_incidence_falsifier.rs`: strict local-only phase CLI with no network/storage/page-fetch surface.
- Create `scripts/run_v23_leaf_page_incidence_falsifier.py`: namespace/mount capability boundary, receipt-ordered phase orchestration, resource monitoring, and explicit cleanup.
- Create `scripts/test_run_v23_leaf_page_incidence_falsifier.py`: launcher command, capability, ordering, stop, and cleanup tests.
- Modify `scripts/requirements-format-bench.txt` only if the launcher tests expose a missing already-approved runtime dependency; do not add a dependency preemptively.
- Modify `docs/research/publication-v3-attempt-ledger.md` only after an authorized scientific terminal exists.

## Plan Delivery Checkpoint

Before Task 1, force-add this ignored plan as the only staged path, validate it,
commit it, push it explicitly to `origin/main`, and prove both design documents
are tracked:

```bash
git add -f docs/superpowers/plans/2026-08-30-v23-leaf-page-incidence-falsifier.md
test "$(git diff --cached --name-only)" = \
  "docs/superpowers/plans/2026-08-30-v23-leaf-page-incidence-falsifier.md"
python3 scripts/validate_research_docs.py
git diff --cached --check
git commit -m "Plan V23 leaf-page incidence falsifier"
git fetch origin main
git merge-base --is-ancestor origin/main HEAD
git push origin HEAD:main
git ls-files --error-unmatch \
  docs/superpowers/specs/2026-08-30-v23-leaf-page-incidence-falsifier-design.md \
  docs/superpowers/plans/2026-08-30-v23-leaf-page-incidence-falsifier.md
```

### Task 1: Authenticated phase and receipt boundary

**Files:**
- Create: `crates/borsuk/src/v23_incidence.rs`
- Modify: `crates/borsuk/src/lib.rs`
- Test: `crates/borsuk/src/v23_incidence.rs` (`#[cfg(test)]`)

**Interfaces:**
- Consumes: existing `BorsukError`, `Result`, SHA-256/BLAKE3 helpers, and canonical JSON conventions.
- Produces: `V23IncidenceObjectIdentity`, `V23IncidenceManifest`, `V23IncidencePhase`, `V23FmaBackend`, `V23IncidenceReceipt`, `V23IncidenceStopClass`, `validate_v23_incidence_identity`, and `canonical_v23_incidence_receipt_bytes`.

- [ ] **Step 1: Write the failing authority tests**

```rust
#[test]
fn v23_incidence_authority_rejects_role_digest_length_and_phase_drift() {
    let fixture = authority_fixture();
    assert!(validate_v23_incidence_identity(&fixture, &fixture).is_ok());
    for changed in identity_mutations(&fixture) {
        assert!(validate_v23_incidence_identity(&changed, &fixture).is_err());
    }
}

#[test]
fn v23_incidence_receipt_binds_parent_capability_and_canonical_bytes() {
    let receipt = receipt_fixture(V23IncidencePhase::TreeTraining);
    let bytes = canonical_v23_incidence_receipt_bytes(&receipt).unwrap();
    assert_eq!(bytes.last(), Some(&b'\n'));
    assert_eq!(bytes.iter().filter(|byte| **byte == b'\n').count(), 1);
    assert!(canonical_v23_incidence_receipt_bytes(&receipt_mutations(&receipt)[0]).is_err());
}
```

- [ ] **Step 2: Run the focused RED**

Run: `cargo test -p borsuk --lib v23_incidence_authority_ -- --nocapture`

Expected: compile failure only for the missing identity/receipt types and functions.

- [ ] **Step 3: Implement the strict types and validation**

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum V23IncidencePhase {
    TreeTraining,
    PostingConstruction,
    DevelopmentEvaluation,
    HoldoutBinding,
    HoldoutEvaluation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct V23IncidenceObjectIdentity {
    pub role: String,
    pub uri: String,
    pub digest_algorithm: String,
    pub digest: String,
    pub encoded_bytes: u64,
    pub generation: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum V23FmaBackend {
    Aarch64NeonFma,
    X86AvxFma,
    ScalarControl,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct V23IncidenceReceipt {
    pub schema: String,
    pub claim_eligible: bool,
    pub phase: V23IncidencePhase,
    pub parent_receipt_sha256: Option<String>,
    pub executable_sha256: String,
    pub fma_backend: V23FmaBackend,
    pub network_namespace_inode: u64,
    pub ordered_mounts: Vec<V23IncidenceObjectIdentity>,
    pub probes: V23IncidenceCapabilityProbes,
    pub outputs: Vec<V23IncidenceObjectIdentity>,
    pub stop: Option<V23IncidenceStopClass>,
}
```

Require exact schema strings, concrete JSON types, lowercase digest syntax, role-specific algorithms, unique roles/URIs, exact ordered inputs, `claim_eligible == false`, phase-legal parents, all capability probes passing, and parent/output digest recomputation before canonical serialization.
Scientific preflight/execution receipts accept only `Aarch64NeonFma` or
`X86AvxFma`; `ScalarControl` is legal only in unit/differential evidence and
must be rejected by every production-shape local request.

- [ ] **Step 4: Run the focused GREEN and static check**

Run: `cargo test -p borsuk --lib v23_incidence_authority_ -- --nocapture && cargo fmt --all -- --check && git diff --check`

Expected: authority tests pass; formatting and diff checks exit 0.

- [ ] **Step 5: Commit the authority slice**

```bash
git add crates/borsuk/src/v23_incidence.rs crates/borsuk/src/lib.rs
git commit -m "Add V23 incidence authority boundary"
```

### Task 2: Capability-separated sandbox launcher

**Files:**
- Create: `scripts/run_v23_leaf_page_incidence_falsifier.py`
- Create: `scripts/test_run_v23_leaf_page_incidence_falsifier.py`

**Interfaces:**
- Consumes: `V23IncidencePhase` names and canonical receipt schema from Task 1.
- Produces: `SandboxMount`, `SandboxPolicy`, `build_unshare_command(policy)`, `monitor_process_group(pid, limits)`, `run_phase(policy)`, and an explicit `--execute-<phase>` CLI.

- [ ] **Step 1: Write the sandbox RED tests**

```python
class SandboxPolicyTests(unittest.TestCase):
    def test_training_mounts_only_manifest_shards_binary_runtime_and_output(self):
        policy = training_policy_fixture()
        command = subject.build_unshare_command(policy)
        self.assertEqual(policy.phase, "tree-training")
        self.assertNotIn("query.parquet", " ".join(command))
        self.assertNotIn("neighbors.parquet", " ".join(command))
        self.assertNotIn("pages", " ".join(command))
        self.assertIn("--user", command)
        self.assertIn("--mount", command)
        self.assertIn("--net", command)

    def test_receipt_chain_prevents_later_capability_before_parent_digest(self):
        with self.assertRaises(ValueError):
            subject.validate_phase_inputs(posting_policy_fixture(parent_digest=None))

    def test_pressure_equality_stops_and_cleanup_names_are_explicit(self):
        self.assertEqual(subject.classify_sample(rss=2 << 30), "rss-cap")
        self.assertEqual(subject.classify_sample(psi=0.79), "psi-immediate")
        self.assertEqual(subject.classify_sample(swap_delta=256 * 1024 * 1024 + 1), "swap-delta")
        self.assertNotIn("rm -rf", inspect.getsource(subject))
```

- [ ] **Step 2: Run the launcher RED**

Run: `python3 -m unittest scripts.test_run_v23_leaf_page_incidence_falsifier.SandboxPolicyTests`

Expected: import or missing-interface errors only.

- [ ] **Step 3: Implement the namespace and monitor boundary**

```python
@dataclasses.dataclass(frozen=True)
class SandboxPolicy:
    phase: str
    executable: pathlib.Path
    runtime_mounts: tuple[SandboxMount, ...]
    inputs: tuple[SandboxMount, ...]
    scratch: pathlib.Path
    output: pathlib.Path
    parent_receipt_sha256: str | None

def build_unshare_command(policy: SandboxPolicy) -> list[str]:
    validate_phase_inputs(policy)
    return [
        "unshare", "--user", "--map-root-user", "--mount", "--net",
        "--pid", "--fork", "--mount-proc", sys.executable, __file__,
        "--enter-sandbox", canonical_policy_argument(policy),
    ]
```

The `--enter-sandbox` path mounts a fresh tmpfs root, bind-mounts only declared paths read-only plus scratch/output read-write, invokes `pivot_root`, unmounts the old root, leaves loopback down, performs namespace/canary/open/connect probes, then `execve`s the phase binary. Monitoring uses the exact RSS/PSI/swap/progress/wall equality rules and terminates the original process group once without restart. Cleanup unlinks an enumerated manifest of files, rejects unexpected entries, then `rmdir`s the empty scratch directory.

- [ ] **Step 4: Run launcher tests and static gates**

Run: `python3 -m unittest scripts.test_run_v23_leaf_page_incidence_falsifier && uv run --python 3.12 --with ruff==0.15.20 ruff check scripts/run_v23_leaf_page_incidence_falsifier.py scripts/test_run_v23_leaf_page_incidence_falsifier.py && python3 -m py_compile scripts/run_v23_leaf_page_incidence_falsifier.py scripts/test_run_v23_leaf_page_incidence_falsifier.py && git diff --check`

Expected: all tests and static checks pass.

- [ ] **Step 5: Commit the sandbox slice**

```bash
git add scripts/run_v23_leaf_page_incidence_falsifier.py scripts/test_run_v23_leaf_page_incidence_falsifier.py
git commit -m "Add isolated V23 incidence phase launcher"
```

### Task 3: Deterministic reservoir and balanced spherical tree

**Files:**
- Create: `crates/borsuk/src/v23_incidence_tree.rs`
- Modify: `crates/borsuk/src/lib.rs`
- Test: `crates/borsuk/src/v23_incidence_tree.rs`

**Interfaces:**
- Consumes: authenticated ordered raw f32 rows `(source_ordinal: u64, vector: &[f32])` and the Task 1 manifest.
- Produces: `V23TrainingRow`, `V23IncidenceTrainingShape`, `V23TreeNode`, `V23IncidenceTree`, `reservoir_seed`, `select_reservoir`, `train_incidence_tree`, `encode_incidence_tree`, `decode_incidence_tree`, `assign_one_leaf`, and `assign_two_beam_leaves`.

- [ ] **Step 1: Write scalar determinism and mutation REDs**

```rust
#[test]
fn v23_incidence_tree_is_byte_identical_across_input_batches_and_threads() {
    let rows = training_rows_fixture(1 << 17, 96);
    let left = train_fixture(&rows, 1, 4096);
    let right = train_fixture(&rows, 8, 777);
    assert_eq!(encode_incidence_tree(&left).unwrap(), encode_incidence_tree(&right).unwrap());
}

#[test]
fn v23_incidence_split_uses_exact_fma_lanes_boundary_bits_and_ties() {
    let node = split_node_fixture();
    assert_eq!(split_score_scalar(&node, &TIE_VECTOR).to_bits(), TIE_SCORE_BITS);
    assert_eq!(assign_one_leaf(&tree_fixture(), &TIE_VECTOR, TIE_ORDINAL).unwrap(), 17);
    for mutation in tree_authority_mutations() {
        assert!(decode_incidence_tree(&mutation).is_err());
    }
}
```

- [ ] **Step 2: Run the tree RED**

Run: `cargo test -p borsuk --lib v23_incidence_tree_ -- --nocapture`

Expected: missing tree interfaces only.

- [ ] **Step 3: Implement reservoir and exact scalar tree**

```rust
pub(crate) const V23_INCIDENCE_RESERVOIR_ROWS: usize = 2_097_152;
pub(crate) const V23_INCIDENCE_TREE_DEPTH: usize = 16;
pub(crate) const V23_INCIDENCE_LEAVES: usize = 65_536;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V23IncidenceTrainingShape {
    pub dimensions: usize,
    pub reservoir_rows: usize,
    pub depth: usize,
    pub lloyd_iterations: usize,
}

impl V23IncidenceTrainingShape {
    pub(crate) const PRODUCTION: Self = Self {
        dimensions: 96,
        reservoir_rows: 2_097_152,
        depth: 16,
        lloyd_iterations: 4,
    };
}

fn exact_dot(row: &[f32; 96], centroid: &[f32; 96]) -> f32 {
    let mut lanes = [0.0_f32; 8];
    for lane in 0..8 {
        for step in 0..12 {
            let dim = lane * 12 + step;
            lanes[lane] = row[dim].mul_add(centroid[dim], lanes[lane]);
        }
    }
    lanes.into_iter().fold(0.0_f32, |sum, value| sum + value)
}

pub(crate) fn split_score_scalar(node: &V23TreeNode, row: &[f32; 96]) -> f32 {
    exact_dot(row, &node.child_one) * node.child_one_inverse_norm
        - exact_dot(row, &node.child_zero) * node.child_zero_inverse_norm
}
```

Use the exact SplitMix64 key, smallest-key reservoir, source-ordinal sorting, 4,096-row f64 partials, fixed index-ordered binary reduction with zero right padding, deterministic farthest seed, four Lloyd passes, post-f16 repartition, median `(total_cmp(score), ordinal)` boundary, and fail-closed validation from the spec. Record exact work counters 3,221,225,472, 25,769,803,776, 6,442,450,944, and 35,433,480,192.
The non-test local runner accepts only `V23IncidenceTrainingShape::PRODUCTION`;
unit tests may pass a reduced shape directly to private helpers, and encoded
scientific artifacts always reject a non-production shape.

- [ ] **Step 4: Add optimized score/assignment and differential tests**

Implement `split_score_simd` with architecture-specific fused intrinsics;
ordinary vector multiply followed by add and `wide`'s target-dependent
`mul_add` fallback are forbidden because they are not bit-equivalent to the
scalar `f32::mul_add` authority. On aarch64, use two `float32x4_t` accumulators
and `core::arch::aarch64::vfmaq_f32`. On x86/x86_64, require runtime FMA
detection and use one `__m256` accumulator with `_mm256_fmadd_ps` inside a
`#[target_feature(enable = "avx,fma")]` function. Transpose each 12-dimension
lane step into the eight SIMD lanes, perform exactly twelve fused operations
from positive zero, extract lanes, and reduce lane zero through seven with
scalar f32 addition. The optimized scientific runner must fail closed with
`determinism-stop` when neither verified fused backend is available; it may not
silently substitute a non-fused SIMD path. A private scalar-control entrypoint
remains available only for differential evidence and unsupported-development
diagnosis.
Mutation-lock the exact score bits and selected leaves against the scalar FMA
contract for random finite rows, ties, subnormals, nonfinite input, duplicate
ordinals, wrong dimensions, empty nodes, zero norms, one-path leaves,
two-beam leaves, 1/2/8 threads, the aarch64 fused backend, the x86 FMA backend
where available in CI, unavailable-backend rejection, and the `scalar-control`
feature. The width-two
path returns `BeamSelectedLeaves([u16; 2])` and never names them globally
nearest.

- [ ] **Step 5: Run tree GREEN, formatter, and commit**

Run: `cargo test -p borsuk --lib v23_incidence_tree_ -- --nocapture && cargo fmt --all && cargo fmt --all -- --check && git diff --check`

Expected: all tree tests pass with no warnings.

```bash
git add crates/borsuk/src/v23_incidence_tree.rs crates/borsuk/src/lib.rs
git commit -m "Add deterministic V23 incidence tree"
```

### Task 4: Bounded external posting builder and codec

**Files:**
- Create: `crates/borsuk/src/v23_incidence_postings.rs`
- Modify: `crates/borsuk/src/lib.rs`
- Test: `crates/borsuk/src/v23_incidence_postings.rs`

**Interfaces:**
- Consumes: sealed `V23IncidenceTree`, authenticated `V23PageRef` plus decoded f16-flat page rows, and explicit scratch/output directories.
- Produces: `PostingAssignmentArm`, `V23PostingRecord`, `V23PostingPlane`, `partition_contributions`, `merge_posting_partitions`, `encode_posting_plane`, and `decode_posting_plane`.

- [ ] **Step 1: Write posting boundary REDs**

```rust
#[test]
fn v23_incidence_postings_match_in_memory_reference_with_bounded_runs() {
    let fixture = posting_fixture();
    let actual = build_with_run_limit(&fixture, 4096).unwrap();
    let expected = reference_postings(&fixture);
    assert_eq!(actual, expected);
    assert!(actual.leaves.iter().all(|leaf| leaf.pages.len() <= 2048));
}

#[test]
fn v23_incidence_prefixes_bind_retention_quantization_and_exact_bytes() {
    let plane = posting_plane_fixture();
    for cap in [512, 1024, 2048] {
        validate_posting_prefix(&plane, cap).unwrap();
    }
    for mutation in posting_mutations(&plane) {
        assert!(decode_posting_plane(&mutation).is_err());
    }
}
```

- [ ] **Step 2: Run the posting RED**

Run: `cargo test -p borsuk --lib v23_incidence_postings_ -- --nocapture`

Expected: missing posting interfaces only.

- [ ] **Step 3: Implement fixed records and partition/run lifecycle**

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct V23PostingRecord {
    pub leaf: u16,
    pub page: u32,
    pub reserved: u16,
}

pub(crate) const V23_POSTING_PARTITIONS: usize = 256;
pub(crate) const V23_POSTING_RUN_BYTES: u64 = 64 * 1024 * 1024;
pub(crate) const V23_POSTING_MAX_PAGES: usize = 2048;
```

Encode every record manually as two little-endian leaf bytes, four
little-endian page bytes, and two zero bytes; never serialize the Rust struct's
in-memory layout. Write separate one-/two-arm partitions selected by the leaf
high byte. Authenticate each page before decoding; normalize f16 rows; parse
canonical decimal IDs to source ordinals; emit exactly one or two records per
physical page assignment. Sort runs by `(leaf,page)`, unlink the unsorted
partition only after durable runs, merge with a bounded heap, retain one
leaf's top-2,048 counts, and stream final bytes. Enforce 55,860,333 records,
446,882,664 record bytes, 1,027,983,056 scratch bytes, 553,648,128 posting RSS,
and exact cleanup.

- [ ] **Step 4: Implement mass/prefix codec and eligibility**

Use structure-of-arrays u32 page/u16 mass, full pre-truncation denominators, ties-to-even conversion, `(count desc,page asc)` ordering, zero omission, exact retained-mass and total-variation recomputation for each `(arm, cap)`, and content-addressed canonical headers. Reject duplicates, order drift, count overflow, reserved bytes, length/remainder drift, mass mismatch, prefix mismatch, or any leaf below 995,000 ppm retention / above 5,000 ppm TV.

- [ ] **Step 5: Run posting GREEN and commit**

Run: `cargo test -p borsuk --lib v23_incidence_postings_ -- --nocapture && cargo fmt --all && cargo fmt --all -- --check && git diff --check`

Expected: posting tests pass and only intended files differ.

```bash
git add crates/borsuk/src/v23_incidence_postings.rs crates/borsuk/src/lib.rs
git commit -m "Add bounded V23 incidence postings"
```

### Task 5: Query kernel, truth binding, and exhaustive campaign result

**Files:**
- Create: `crates/borsuk/src/v23_incidence_eval.rs`
- Modify: `crates/borsuk/src/v23_incidence.rs`
- Modify: `crates/borsuk/src/lib.rs`
- Test: `crates/borsuk/src/v23_incidence_eval.rs`

**Interfaces:**
- Consumes: sealed tree/posting planes, authenticated query/neighbor Parquet bytes, D2 burned-query evidence, and independently mapped holdout page truth.
- Produces: `V23IncidenceCell`, `V23IncidenceQueryEvidence`, `V23IncidenceCellResult`, `V23IncidenceCampaignClass`, `V23IncidenceServingProjection`, `project_v23_incidence_serving_bytes`, `score_incidence_query`, `evaluate_development`, `bind_holdout_truth`, `evaluate_holdout`, and `canonical_v23_incidence_result_bytes`.

- [ ] **Step 1: Write scorer and campaign REDs**

```rust
#[test]
fn v23_incidence_scalar_and_simd_select_exact_same_eight_pages() {
    for fixture in scorer_fixtures_with_ties_subnormals_duplicates_and_max_lists() {
        let scalar = score_incidence_query_scalar(&fixture).unwrap();
        let simd = score_incidence_query(&fixture).unwrap();
        assert_eq!(scalar.ranked_leaf_ordinals, simd.ranked_leaf_ordinals);
        assert_eq!(scalar.page_ordinals, simd.page_ordinals);
        assert_eq!(simd.page_ordinals.len(), 8);
    }
}

#[test]
fn v23_incidence_campaign_uses_frozen_18_cell_order_and_exhaustive_precedence() {
    assert_eq!(V23IncidenceCell::registered_ladder().len(), 18);
    assert_eq!(V23IncidenceCell::registered_ladder()[0],
               cell(512, PostingAssignmentArm::OnePath, 32));
    assert_eq!(V23IncidenceCell::registered_ladder()[17],
               cell(2048, PostingAssignmentArm::TwoBeam, 128));
    for fixture in campaign_precedence_fixtures() {
        assert_eq!(classify_campaign(&fixture.results), fixture.expected);
    }
}

#[test]
fn v23_incidence_serving_projection_is_exact_at_the_maximum_cell() {
    let projection = project_v23_incidence_serving_bytes(100_000_000, 2048).unwrap();
    assert_eq!(projection.projected_pages, 283_104);
    assert_eq!(projection.posting_bytes, 805_306_368);
    assert_eq!(projection.touched_workspace_bytes, 1_048_576);
    assert_eq!(projection.total_bytes, 1_723_215_492);
}
```

- [ ] **Step 2: Run the evaluation RED**

Run: `cargo test -p borsuk --lib v23_incidence_eval_ -- --nocapture`

Expected: missing evaluation interfaces only.

- [ ] **Step 3: Implement flat-leaf and posting kernels**

Scan all 65,536 leaf centroids in fixed ordinal blocks using the Task 3
scalar/SIMD contract and retain only a bounded best-128 structure ordered by
`(cosine distance, leaf ordinal)`; a full 65,536-pair allocation or sort is
forbidden. Keep a test-only scalar full-sort oracle and mutation-lock exact
selected leaf order for P=32/64/128 across random finite vectors, exact ties,
subnormals, maximum distances, and reversed input blocks. For P=32/64/128, use
`reciprocal_q32[rank] = round_half_even(2^32/(rank+1))`, u64 score/u32 epoch
planes for 283,104 pages, a 262,144-entry touched list, and top-eight
`(score desc,page ordinal asc)`. Compute quality even when touched pages exceed
the 8,192 gate. Reject nonfinite inputs, overflow, fewer than eight pages,
duplicate pages, or scalar/SIMD page drift.

- [ ] **Step 4: Implement query, neighbor, and page-truth authority**

Validate exact 10,000-row Parquet schemas. Development accepts only ordinals 0--31 and D2's existing ten-neighbor page assignments. Holdout authenticates all 100 neighbors for ordinals 32--159, maps all 12,800 IDs, then binds only the ordered first 1,280 IDs for recall@10. Reject missing/duplicate/OOB IDs, unbound pages, order drift, nonfinite/zero query vectors, fewer than eight oracle pages, or oracle layout below 985,000/900,000 ppm.

- [ ] **Step 5: Implement sealed selection and canonical classification**

Record every eligible cell independently, select the first complete development pass, bind its exact `(cap, arm, probes, tree digest, posting digest, executable digest)` into the development receipt, and permit only that cell in holdout. Implement the spec's authority/resource/determinism/retention/development/holdout precedence verbatim; recompute all aggregate/minimum/attainment/p99 fields before canonical newline JSON.

For each scientific cell, time the complete resident tree plus selected posting
prefix with one pinned worker, a fixed 1,024-query warm-up, and at least 10,000
timed query invocations. Store every raw per-query nanosecond sample in a
content-addressed latency artifact, recompute nearest-rank p99 from those raw
samples during result serialization, and bind its digest/length into the cell
receipt. The Task 6 synthetic preflight projection cannot satisfy this native
15-ms gate.

- [ ] **Step 6: Run evaluation GREEN and commit**

Run: `cargo test -p borsuk --lib v23_incidence_eval_ -- --nocapture && cargo fmt --all && cargo fmt --all -- --check && git diff --check`

Expected: evaluation and mutation tests pass.

```bash
git add crates/borsuk/src/v23_incidence.rs crates/borsuk/src/v23_incidence_eval.rs crates/borsuk/src/lib.rs
git commit -m "Add V23 incidence evaluation campaign"
```

### Task 6: Local-only phase executable and preflight receipts

**Files:**
- Create: `crates/borsuk/examples/v23_leaf_page_incidence_falsifier.rs`
- Modify: `crates/borsuk/src/v23_incidence.rs`
- Modify: `crates/borsuk/src/lib.rs`
- Test: `crates/borsuk/examples/v23_leaf_page_incidence_falsifier.rs`

**Interfaces:**
- Consumes: doc-hidden local phase requests wrapping Tasks 1, 3, 4, and 5.
- Produces: `V23IncidenceRunMode`, `V23IncidenceLocalRolePath`, `V23IncidenceLocalPhaseRequest`, `run_v23_incidence_local_phase`, and example-local `parse_args` for phase-specific preflight or execution of `train-tree`, `build-postings`, `evaluate-development`, `bind-holdout`, and `evaluate-holdout`.

- [ ] **Step 1: Write strict CLI REDs**

```rust
#[test]
fn v23_incidence_example_requires_one_phase_and_exact_local_roles() {
    for phase in PHASES {
        let preflight = parse_args(preflight_arguments_for(phase)).unwrap();
        assert_eq!(preflight.mode, V23IncidenceRunMode::Preflight(phase));
        let execute = parse_args(execute_arguments_for(phase)).unwrap();
        assert_eq!(execute.mode, V23IncidenceRunMode::Execute(phase));
    }
    for changed in missing_duplicate_unknown_and_invalid_arguments() {
        assert!(parse_args(changed).is_err());
    }
}

#[test]
fn v23_incidence_example_refuses_network_storage_query_leak_and_d3_flags() {
    for flag in ["--bucket", "--aws-profile", "--endpoint", "--page-uri",
                 "--storage-uri", "--d3", "--query" , "--neighbors"] {
        assert!(parse_args(arguments_with_forbidden_flag(flag)).is_err());
    }
}
```

The query/neighbor rejection is phase-sensitive: development and holdout phases accept their registered local role flags, while training/posting reject them unconditionally.

- [ ] **Step 2: Run the example RED**

Run: `cargo test -p borsuk --example v23_leaf_page_incidence_falsifier v23_incidence_ -- --nocapture`

Expected: missing parser/high-level runner only.

- [ ] **Step 3: Implement local phase requests and preflight**

Each phase accepts explicit local paths plus exact role URI/digest/length/generation flags, an executable digest, parent receipt digest, scratch/output paths, and exactly one `--preflight-<phase>` or `--execute-<phase>` gate. `V23IncidenceRunMode::Preflight(phase)` accepts only the phase's fixed preflight subset and emits a preflight receipt parented by the prior scientific receipt; `Execute(phase)` requires that exact preflight digest plus the remaining phase inputs. Tree preflight runs 65,536 vectors; posting preflight authenticates and decodes 256 pages plus sorts 1,048,576 records; evaluation preflight runs 10,000 resident synthetic queries only to project feasibility. Each receipt records distance dimensions, bytes, records, throughput, and the 80%-throughput wall projection, and refuses a projection above 5,400 seconds before remaining input acquisition. Evaluation still performs and records the separate complete-representation native timing from Task 5.

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum V23IncidenceRunMode {
    Preflight(V23IncidencePhase),
    Execute(V23IncidencePhase),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V23IncidenceLocalRolePath {
    pub identity: V23IncidenceObjectIdentity,
    pub path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V23IncidenceLocalPhaseRequest {
    pub mode: V23IncidenceRunMode,
    pub manifest_path: PathBuf,
    pub parent_receipt_path: Option<PathBuf>,
    pub preflight_receipt_path: Option<PathBuf>,
    pub input_paths: Vec<V23IncidenceLocalRolePath>,
    pub scratch_path: PathBuf,
    pub output_path: PathBuf,
    pub executable_sha256: String,
}

pub fn run_v23_incidence_local_phase(
    request: V23IncidenceLocalPhaseRequest,
) -> Result<Vec<u8>> {
    request.validate()?;
    let receipt = match request.mode {
        V23IncidenceRunMode::Preflight(phase) => run_phase_preflight(phase, request)?,
        V23IncidenceRunMode::Execute(V23IncidencePhase::TreeTraining) => run_tree_training(request)?,
        V23IncidenceRunMode::Execute(V23IncidencePhase::PostingConstruction) => run_posting_build(request)?,
        V23IncidenceRunMode::Execute(V23IncidencePhase::DevelopmentEvaluation) => run_development(request)?,
        V23IncidenceRunMode::Execute(V23IncidencePhase::HoldoutBinding) => run_holdout_binding(request)?,
        V23IncidenceRunMode::Execute(V23IncidencePhase::HoldoutEvaluation) => run_holdout(request)?,
    };
    canonical_v23_incidence_receipt_bytes(&receipt)
}
```

- [ ] **Step 4: Prove no forbidden call surface**

Use a read-only `rg` callsite test to establish the example reaches only local file readers and the doc-hidden runner. Ensure `object_store`, `Storage`, AWS, HTTP, page fetching, and D3 symbols are absent from the new production paths; their strings may occur only in refusal tests.

- [ ] **Step 5: Run example GREEN and commit**

Run: `cargo test -p borsuk --example v23_leaf_page_incidence_falsifier v23_incidence_ -- --nocapture && cargo fmt --all && cargo fmt --all -- --check && git diff --check`

Expected: parser/run tests pass with zero warnings.

```bash
git add crates/borsuk/examples/v23_leaf_page_incidence_falsifier.rs crates/borsuk/src/v23_incidence.rs crates/borsuk/src/lib.rs
git commit -m "Add local V23 incidence phase executable"
```

### Task 7: End-to-end synthetic campaign and adversarial review

**Files:**
- Modify: `crates/borsuk/src/v23_incidence.rs`
- Modify: `crates/borsuk/src/v23_incidence_tree.rs`
- Modify: `crates/borsuk/src/v23_incidence_postings.rs`
- Modify: `crates/borsuk/src/v23_incidence_eval.rs`
- Modify: `crates/borsuk/examples/v23_leaf_page_incidence_falsifier.rs`
- Modify: `scripts/test_run_v23_leaf_page_incidence_falsifier.py`

**Interfaces:**
- Consumes: all previous task boundaries.
- Produces: one complete synthetic receipt chain proving phase capability separation, deterministic artifacts, development sealing, holdout single-use, and cleanup.

- [ ] **Step 1: Write the end-to-end RED**

Create a 96-dimensional synthetic corpus using a private test shape of depth 5,
32 leaves, 4,096 reservoir rows, four Lloyd iterations, 16 pages,
primary/replica assignments, 160 development/holdout queries, and independently
generated neighbor truth. The local public runner must reject this reduced
shape; the unit-only pipeline accepts it so the same algorithms can be tested.
Add the test-harness child node inside `v23_incidence.rs`'s library unit-test
module, where private reduced-shape phase helpers are visible. When
`BORSUK_V23_INCIDENCE_TEST_CHILD_PHASE` is present under `#[cfg(test)]`, it
reads one test policy path and invokes exactly that private phase. The parent
library test launches `std::env::current_exe()` five times with the exact child-test
filter and a different phase/input directory each time. This exercises real
process and receipt boundaries without adding a production fixture flag. Run
training, posting, development, holdout binding, and holdout evaluation in
those separate processes; assert parent digests,
exact-eight pages, the sealed cell, `claim_eligible=false`, zero page-body reads
during query evaluation, and byte-identical results across 1/2/8 construction
threads.

Run: `cargo test -p borsuk --lib v23_incidence_campaign_end_to_end_ -- --nocapture`

Expected: fail at the first missing cross-phase binding or single-use guard, not at fixture construction.

This library child chain proves phase/request/receipt separation. It does not
claim to prove OS namespace isolation. Add a separate Python integration node
that sends one harmless canary command through `run_phase(policy)` from Task 2
and asserts the changed network-namespace inode, inaccessible host canaries,
down loopback/network failure, allowlisted input success, output success, and
explicit cleanup.

- [ ] **Step 2: Add capability and corruption mutations**

Mutation-lock wrong manifest order, altered generation/digest/length, page before tree seal, query before router seal, neighbors before cell seal, reused holdout receipt, changed chosen cell, partial files, unexpected scratch entries, noncanonical JSON, corrupt tree/posting headers, scalar/SIMD drift, pressure equality, progress timeout, and process-group termination. Each must produce only the registered stop class and no partial scientific result.

- [ ] **Step 3: Run all focused gates**

Run serially:

```bash
cargo test -p borsuk --lib v23_incidence_ -- --nocapture
cargo test -p borsuk --example v23_leaf_page_incidence_falsifier v23_incidence_ -- --nocapture
python3 -m unittest scripts.test_run_v23_leaf_page_incidence_falsifier
cargo fmt --all
cargo fmt --all -- --check
git diff --check
```

Expected: all focused Rust/Python/static gates pass.

- [ ] **Step 4: Request independent read-only review and repair only concrete findings**

Ask the opposite-provider reviewer to inspect the spec-to-diff mapping, capability boundary, exact arithmetic, determinism, external-sort bounds, campaign precedence, and holdout leakage. Do not claim review if credentials fail. For each accepted finding, add a focused RED, implement the minimal fix, rerun the focused GREEN, then rerun Step 3 once after the diff stabilizes.

- [ ] **Step 5: Commit the integrated implementation**

```bash
git add crates/borsuk/src/v23_incidence.rs \
  crates/borsuk/src/v23_incidence_tree.rs \
  crates/borsuk/src/v23_incidence_postings.rs \
  crates/borsuk/src/v23_incidence_eval.rs \
  crates/borsuk/src/lib.rs \
  crates/borsuk/examples/v23_leaf_page_incidence_falsifier.rs \
  scripts/run_v23_leaf_page_incidence_falsifier.py \
  scripts/test_run_v23_leaf_page_incidence_falsifier.py
git commit -m "Add V23 leaf-page incidence falsifier"
```

### Task 8: Repository assurance and execution handoff

**Files:**
- Modify: only files required by concrete verification findings.
- Test: repository-wide.

**Interfaces:**
- Consumes: stable implementation commit from Task 7.
- Produces: verified clean source SHA, release binary identity, and separately fenced commands for preflight, construction/development, then holdout.

- [ ] **Step 1: Run strict static assurance**

Run: `cargo clippy --locked --workspace --all-targets -- -D warnings`

Expected: exit 0 with no diagnostics. Repair findings through focused RED/GREEN tests, rerun Clippy once, and commit the verified repair before proceeding:

```bash
mapfile -t repair_paths < <(git diff --name-only)
test "${#repair_paths[@]}" -gt 0
git add -- "${repair_paths[@]}"
test "$(git diff --cached --name-only)" = "$(printf '%s\n' "${repair_paths[@]}")"
git commit -m "Repair V23 incidence assurance findings"
```

- [ ] **Step 2: Run full repository assurance once**

Run serially:

```bash
cargo test --locked --workspace --all-targets
uv run --python 3.12 \
  --with-requirements scripts/requirements-format-bench.txt \
  python -m unittest discover -s scripts -p 'test_*.py'
```

Expected: both repository-wide layers exit 0. Preserve each original terminal
and pressure evidence; after a failure, repair/rerun only the failing layer,
then run one final copy of both layers. Commit any verified repair as an exact
scoped slice before building the release example.

- [ ] **Step 3: Build and authenticate the release example**

Run: `cargo build --offline --locked --release -p borsuk --example v23_leaf_page_incidence_falsifier`

Record the exact binary path, SHA-256, length, source SHA, and clean-worktree proof. Do not acquire scientific inputs in this step.

- [ ] **Step 4: Prepare separately authorized execution stages**

Prepare three commands, each remaining unstarted until separately authorized:

1. sandbox/preflight plus tree training and tree receipt;
2. posting construction plus burned-query development selection and sealed-cell receipt;
3. holdout truth binding plus the one permitted holdout evaluation.

Each command must assert the source/binary identities, use exact frozen objects, enforce the registered RSS/scratch/PSI/swap/progress/wall stops, retain only canonical complete/stop receipts, explicitly unlink named scratch files after PID clearance, and forbid restart. No command may include D3.

- [ ] **Step 5: Publish verified source before any scientific run**

```bash
git ls-files --error-unmatch \
  docs/superpowers/specs/2026-08-30-v23-leaf-page-incidence-falsifier-design.md \
  docs/superpowers/plans/2026-08-30-v23-leaf-page-incidence-falsifier.md
test -z "$(git status --porcelain)"
git fetch origin main
git merge-base --is-ancestor origin/main HEAD
git push origin HEAD:main
git fetch origin main
test "$(git rev-parse HEAD)" = "$(git rev-parse origin/main)"
test "$(git rev-parse HEAD)" = "$(git ls-remote origin refs/heads/main | awk '{print $1}')"
test -z "$(git status --porcelain)"
```

Expected: local, tracking, and authoritative remote refs match exactly and the worktree is clean.

- [ ] **Step 6: Record evidence only after an authorized terminal**

If a complete scientific result exists, add one evidence-only section to `docs/research/publication-v3-attempt-ledger.md` containing every input/output digest, construction/work/resource counters, all 18 development cells, sealed cell or terminal rejection, holdout authority/result if permitted, claim-ineligible scope, cleanup proof, and D3 fence. Run `python3 scripts/validate_research_docs.py && git diff --check`, commit only the ledger, and push explicitly to `origin/main`. If any phase stops before scientific evidence, record only the authenticated stop boundary and do not invent downstream metrics.
