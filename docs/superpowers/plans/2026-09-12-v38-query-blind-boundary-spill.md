# V38 Query-Blind Boundary Spill Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Determine on the frozen one-million-row ReLAION2B cohort whether one bounded, query-blind alternate coarse owner per selected row can make exact fourteen-posting containment reach 998,000 ppm aggregate and 800,000 ppm minimum.

**Architecture:** A new Rust V38 module reuses the sealed V37 tree and primary ownership, scores one deterministic alternate leaf for every row, and admits at most 250,000 alternates under a 10,240-record posting cap. A separate GT-only phase certifies multi-owner maximum coverage with bounded branch-and-bound; Python orchestration runs source-free preflight, one corpus-only build, and one ceiling on causality Spot with hard phase and resource fences.

**Tech Stack:** Rust, existing `borsuk-fma`, Rayon, Arrow IPC, Parquet, serde JSON, Python 3.12, boto3, causality EC2 Spot.

**Spec:** `docs/superpowers/specs/2026-09-12-v38-query-blind-boundary-spill-design.md`

## Global Constraints

- The first population is frozen ReLAION2B cohort A: 1,000,000 non-null `f32[768]` rows, 1,000 development queries, and exact GT@100.
- Construction can read only authenticated V37/V36 construction artifacts and source; it cannot read queries, GT, prior samples, validation, holdout, or page bodies.
- One arm only: at most one alternate owner per row, 250,000 alternates total, 10,240 records per posting, and 48 bytes projected coarse payload per record.
- Quality gates remain 998,000 ppm aggregate and 800,000 ppm minimum at exactly fourteen coarse postings.
- Use Parquet for cross-language tables, Arrow IPC for dense tree data, and recursively sorted compact JSON plus LF for authority/results.
- A pass is claim-ineligible and opens only later routing/physical qualification. A fail or indeterminate result fences routing, page reads, validation, holdout, 10M, and 100M.
- Heavy gates and scientific runs execute on causality Spot. Local iteration uses narrow tests only; one full repository gate runs after the stable diff.
- No compatibility reader, alias, migration, optional authority field, or alternate post-result arm is added.

## File Map

- Create `crates/borsuk/src/v38_boundary_spill.rs`: V38 authority, deterministic spill construction, Parquet codecs, multi-owner coverage certificates, local phase runner, and canonical results.
- Modify `crates/borsuk/src/v37_relation_router.rs`: expose only the crate-private authenticated V37 tree/ownership and exact fused node-score helpers V38 needs.
- Modify `crates/borsuk/src/lib.rs`: declare the module and doc-hidden high-level local request boundary.
- Create `crates/borsuk/examples/v38_boundary_spill.rs`: strict local-file phase CLI with no network or page API.
- Create `scripts/run_v38_boundary_spill_spot.py`: phase-separated causality Spot orchestration and monitoring.
- Create `scripts/test_run_v38_boundary_spill_spot.py`: launcher, authority, failure, cleanup, and phase-fence tests.
- Modify `docs/research/publication-v3-attempt-ledger.md` only after a terminal 1M result.

### Task 1: Freeze authority, arithmetic, and resource admissions

**Files:**

- Create: `crates/borsuk/src/v38_boundary_spill.rs`
- Modify: `crates/borsuk/src/lib.rs`

**Interfaces:**

- Consumes: authenticated V37 artifact identities and `serde_json`; V38 owns a
  private recursively sorted JSON helper rather than reaching into V37's
  private helper.
- Produces: `V38SpillSpec`, `V38ArtifactIdentity`,
  `V38ConstructionAuthority`, `V38CeilingAuthority`,
  `project_v38_spill_capacity`, canonical parse/serialize functions, and
  warning-clean doc-hidden public entrypoints
  `validate_v38_construction_authority_bytes` and
  `validate_v38_ceiling_authority_bytes` that Tasks 3-4 reuse.

- [ ] **Step 1: Write authority REDs.** Add `v38_boundary_authority_*` unit tests for the exact fixed values, `123 * 10_240 = 1_259_520`, 250,000-alternate admission, 48-byte multiplication, 32,768-byte allowance, checked overflow, exact role/algorithm digest allowlists, input/output disjointness, unknown fields, concrete JSON types, and LF-canonical bytes. Define the public shape exactly:

```rust
pub(crate) struct V38SpillSpec {
    pub corpus_rows: u64,
    pub posting_count: u32,
    pub maximum_alternate_assignments: u64,
    pub maximum_owners_per_row: u8,
    pub maximum_rows_per_posting: u32,
    pub projected_record_bytes: u32,
    pub projected_framing_allowance_bytes: u32,
    pub selected_postings: u32,
}

pub(crate) fn validate_v38_spill_spec(spec: &V38SpillSpec) -> Result<()>;
pub(crate) fn canonical_v38_construction_authority_bytes(
    authority: &V38ConstructionAuthority,
) -> Result<Vec<u8>>;

#[doc(hidden)]
pub fn validate_v38_construction_authority_bytes(bytes: &[u8]) -> Result<Vec<u8>>;

#[doc(hidden)]
pub fn validate_v38_ceiling_authority_bytes(bytes: &[u8]) -> Result<Vec<u8>>;
```

- [ ] **Step 2: Preserve the intended RED.** Run `cargo test -p borsuk --lib v38_boundary_authority_ -- --nocapture`; require only unresolved V38 types/functions and zero warnings.
- [ ] **Step 3: Implement the minimal typed boundary.** Use checked integer arithmetic for every count/byte projection. Reject anything except SHA-256 for JSON/Parquet/Arrow roles and BLAKE3 only where explicitly registered as an additional digest.
- [ ] **Step 4: Prove GREEN.** Rerun the identical selector and require every selected test to pass.
- [ ] **Step 5: Verify and commit the slice.** Make every internal item
  transitively consumed by the doc-hidden validator so no dead-code allowance
  is needed. Run `cargo fmt --all -- --check`, targeted strict Clippy for
  `borsuk`, and `git diff --check`; review only the Task 1 diff; commit the two
  files.

### Task 2: Construct the deterministic alternate-owner relation

**Files:**

- Modify: `crates/borsuk/src/v38_boundary_spill.rs`
- Modify: `crates/borsuk/src/v37_relation_router.rs`

**Interfaces:**

- Consumes: `V37BalancedTree`, `V37OwnershipRecord`, and exact V37 fused node scoring.
- Produces: `V38SpillProposal`, `V38SpillRecord`, `V38PostingSummary`, `V38SpillRelation`, `propose_v38_alternate_owner`, and `build_v38_spill_relation`.

- [ ] **Step 1: Write geometry REDs.** Add `v38_boundary_geometry_*` tests comparing the production scorer against an independent exhaustive scalar reference. Cover left/right/equality margins, approximate-unit persisted normals with no renormalization, f32 rounding, `total_cmp`, primary exclusion, posting ties, reversed row blocks, arbitrary dimensions/tails, nonfinite values, and worker-count equality. The signature is:

```rust
pub(crate) fn propose_v38_alternate_owner(
    tree: &V37BalancedTree,
    projected_row: &[f32],
    primary_posting: u32,
) -> Result<V38SpillProposal>;
```

- [ ] **Step 2: Write admission REDs.** Add `v38_boundary_admission_*` tests for exact local fractional ranks using integer cross-products, violation/source/target ties, target saturation, no third-choice fallback, exactly 250,000 maximum accepted proposals, primary preservation, at most two owners, and deterministic alternates sorted by source ordinal.
- [ ] **Step 3: Run the two RED selectors.** Run
  `cargo test -p borsuk --lib v38_boundary_geometry_ -- --nocapture`, then only
  after its terminal run
  `cargo test -p borsuk --lib v38_boundary_admission_ -- --nocapture`. Accept
  missing production boundaries only.
- [ ] **Step 4: Implement bounded construction.** Score every internal node once per row, propagate path maxima without a row-by-leaf matrix, retain packed proposals within 64 MiB, and spill to bounded external sorted runs when the checked projection exceeds the resident limit.
- [ ] **Step 5: Implement exact Parquet codecs.** Encode/decode the six-column relation and six-column posting summary from the spec. Preserve V37 primary local ordinals; append alternates by source ordinal; validate contiguous posting-local ranges and typed equality of all shared primary fields.
- [ ] **Step 6: Mutation-lock round trips.** Reject column name/order/type/nullability drift, role drift, row ordering, missing/duplicate owners, population mismatch, violation nonfiniteness, digest/length drift, and any payload projection above 491,520 bytes.
- [ ] **Step 7: Prove GREEN and commit.** Run
  `cargo test -p borsuk --lib v38_boundary_geometry_ -- --nocapture`, then
  `cargo test -p borsuk --lib v38_boundary_admission_ -- --nocapture`, then
  `cargo test -p borsuk --lib v38_boundary_codec_ -- --nocapture`, fmt,
  strict targeted Clippy, and diff-check. Commit only this construction slice.

### Task 3: Certify bounded multi-owner K=14 coverage

**Files:**

- Modify: `crates/borsuk/src/v38_boundary_spill.rs`

**Interfaces:**

- Consumes: sealed `V38SpillRelation`, feature-ID GT@100, exact `V38CeilingAuthority`.
- Produces: `V38CoverageCertificate`, `V38LayoutCeiling`,
  `V38TerminalDisposition`,
  `evaluate_v38_multi_owner_ceiling`, `canonical_v38_ceiling_bytes`, and
  `validate_v38_ceiling_bytes(authority, relation, truth, claimed)`.

- [ ] **Step 1: Write solver REDs.** Add `v38_boundary_cover_*` fixtures that exhaustively enumerate every subset on tiny graphs and compare lower/upper/exact results. Include loops, two-owner edges, duplicate feature IDs, fewer than fourteen useful postings, padding to exactly fourteen, ties, a greedy-suboptimal case, and randomized graphs with a fixed test seed.
- [ ] **Step 2: Write bound/stop REDs.** Mutation-lock the cheap top-14
  individual-count upper bound, greedy feasible lower bound, include/exclude
  DFS, `max(incumbent, frontier bounds)` interruption rule, empty-frontier
  exactness, 124-frame/1-MiB limits, 250,000 per-query and 25,000,000 total
  visits, 256-byte certificate cap, and
  `layout-feasible`/`layout-rejected`/`indeterminate` precedence. Add
  complete-population fixtures proving all query cheap bounds reduce before
  search and that a cheap aggregate/minimum rejection or greedy global pass
  performs exactly zero DFS visits.
- [ ] **Step 3: Run the focused RED.** Run `cargo test -p borsuk --lib v38_boundary_cover_ -- --nocapture`; require missing solver/result symbols only.
- [ ] **Step 4: Implement the minimal solver.** Use two u64 words for 100-hit coverage and depth-first mutable state with an undo log; do not retain a heap frontier or apply dominance transformations:

```rust
pub(crate) fn evaluate_v38_multi_owner_ceiling(
    authority: &V38CeilingAuthority,
    relation: &V38SpillRelation,
    truth: &[V37FeatureGroundTruth],
) -> Result<V38LayoutCeiling>;
```

- [ ] **Step 5: Independently validate result bytes.** Implement
  `validate_v38_ceiling_bytes` by replaying the deterministic evaluator from
  the authenticated authority, relation, and GT in increasing query order with
  the same fixed visit limits, then requiring canonical byte equality. This
  recomputes every selection, current/frontier/incumbent bound, visit reduction,
  aggregate/minimum, percentile, exact count, and disposition without an
  unbounded proof trace. Reject a claimed exact ceiling whose replay frontier
  is nonempty.
- [ ] **Step 6: Prove GREEN and commit.** Rerun the focused selector, then fmt, targeted strict Clippy, and diff-check. Commit the solver slice.

### Task 4: Add the strict local phase boundary and source-free preflight

**Files:**

- Modify: `crates/borsuk/src/v38_boundary_spill.rs`
- Modify: `crates/borsuk/src/v37_relation_router.rs`
- Modify: `crates/borsuk/src/lib.rs`
- Create: `crates/borsuk/examples/v38_boundary_spill.rs`

**Interfaces:**

- Produces: doc-hidden `V38LocalRunMode`, `V38LocalArtifact`, `V38LocalOutput`,
  `V38LocalRunRequest`, `run_v38_local_request`, and
  `run_v38_local_request_with_progress` with this exact outer shape:

```rust
pub enum V38LocalRunMode {
    PreflightSpill,
    BuildSpill,
    EvaluateCeiling,
}

pub struct V38LocalRunRequest {
    pub mode: V38LocalRunMode,
    pub workers: u32,
    pub inputs: Vec<V38LocalArtifact>,
    pub outputs: Vec<V38LocalOutput>,
}

pub fn run_v38_local_request(request: V38LocalRunRequest) -> Result<Vec<u8>>;
```

- [ ] **Step 1: Stage CLI REDs.** Add `v38_boundary_cli_*` tests for exactly three modes: `preflight-spill`, `build-spill`, and `evaluate-ceiling`. Require explicit file paths plus URI/SHA-256/BLAKE3/length identities. Reject missing, duplicate, unknown, bucket, endpoint, page, query-in-build, GT-in-build, source-in-ceiling, validation, holdout, 10M, 100M, and D3 flags.
- [ ] **Step 2: Stage capability REDs.** `build-spill` receives exact roles
  `v38-authority`, `v37-authority`, `v37-construction-result`, `v37-tree`,
  `v37-ownership`, and `source`. The V37 authority supplies the projection and
  numeric contents; the V37 result must bind that exact authority plus the
  tree, ownership, source, commit, archive, binary, backend, and worker count.
  `evaluate-ceiling` receives exact roles `v38-ceiling-authority`,
  `v38-construction-result`, `spill-relation`, `spill-postings`, and `gt100`.
  Assert role/path/device/inode disjointness and retained authenticated
  descriptors.
- [ ] **Step 3: Stage preflight REDs.** Freeze a synthetic 65,536-row f32[192] generator, actual fused tree scoring/propagation/proposal ordering, scalar comparison, external-run merge, Parquet encode/decode, solver fixtures, projected work, and full checked memory ledger. Require projected scoring at least 78,080,000 coordinates/s, at most 300 seconds scoring, and at most 450 seconds total construction.
- [ ] **Step 4: Run the example/library REDs.** Run
  `cargo test -p borsuk --lib v38_boundary_local_ -- --nocapture`, then only
  after its terminal run
  `cargo test -p borsuk --example v38_boundary_spill v38_boundary_cli_ -- --nocapture`.
  Accept only missing high-level boundary/parser symbols.
- [ ] **Step 5: Implement the loader and thin CLI.** Parse and authenticate before semantic use, retain handles, replay the fixed V36 SRHT through existing crate-private functions, and write outputs to no-clobber temporary siblings before commit. `main` prints canonical stdout only and exits nonzero on error.
- [ ] **Step 6: Prove phase isolation.** Tests must plant forbidden query/GT/source/page files and verify the corresponding phase cannot open them. Construction and ceiling must run as separate processes; a combined debug path is forbidden.
- [ ] **Step 7: Prove GREEN and commit.** Run local selectors, example tests, fmt, strict workspace/all-targets Clippy, and diff-check. Commit the complete local boundary.

### Task 5: Add causality Spot orchestration and fast terminal evidence

**Files:**

- Create: `scripts/run_v38_boundary_spill_spot.py`
- Create: `scripts/test_run_v38_boundary_spill_spot.py`

**Interfaces:**

- Produces: `V38SpotPlan`, `V38WorkerInvocation`, `V38MonitorSample`, `run_v38_spot_phase`, terminal/result validators, and the exact three-phase command surface.

- [ ] **Step 1: Stage launcher REDs.** Under stdlib unittest, cover profile
  `causality`, Spot-only EC2, region `eu-central-1`, AMI
  `ami-07bcecd13a160173f`, type `c7g.8xlarge`, and the ordered Spot targets
  `eu-central-1c/subnet-0a12dbed0ca6fac25`,
  `eu-central-1b/subnet-00243d923761c047c`, then
  `eu-central-1a/subnet-034528fbd6977848f`. Cover exact source
  commit/archive/binary identities, create-only S3 keys, no prefix listing as
  authority, one process group, bounded failure logs, instance termination,
  and explicit named scratch cleanup.
- [ ] **Step 2: Stage monitor REDs.** Construction stops at 3 GiB `memory.current`, any swap growth, PSI full avg10 above 0.75, 600-second science, 720-second wrapper, or 120-second progress. Ceiling stops at 256 MiB, any swap growth, PSI 0.75, 120-second science, 180-second wrapper, or 30-second progress. Record `memory.peak`, anonymous/file/kernel components, swap, and PSI without calling cgroup memory RSS.
- [ ] **Step 3: Stage predecessor/phase REDs.** A build requires an authenticated successful preflight bound to the same commit, archive, binary, backend, worker count, authority, and exact resource projection. Ceiling requires sealed build terminal/result/relation/posting identities. Failure and indeterminate terminals cannot launch successors.
- [ ] **Step 4: Run the focused RED.** Run
  `uv run --offline --python 3.12 --with-requirements scripts/requirements-format-bench.txt python -m unittest scripts.test_run_v38_boundary_spill_spot`.
  Require only missing V38 controller symbols.
- [ ] **Step 5: Implement by adapting proven V37 mechanisms.** Copy no compatibility dispatch: make a V38-only parser, role matrix, manifest validator, systemd capability sandbox, monitor, terminal schema, bounded diagnostic upload, and termination path.
- [ ] **Step 6: Prove GREEN and commit.** Run the complete V38 controller file, Ruff 0.15.20 on both Python files, py_compile, and `git diff --check`. Commit the orchestration slice.

### Task 6: Run the single 1M fail-fast campaign

**Files:**

- Modify only after terminal evidence: `docs/research/publication-v3-attempt-ledger.md`

**Interfaces:**

- Consumes: one clean committed V38 binary and frozen authenticated inputs.
- Produces: one terminal scientific disposition and immutable evidence identities.

- [ ] **Step 1: Build once on causality Spot.** Preserve full source SHA, archive SHA/length, release binary SHA/length/backend, instance/subnet/region, and build terminal. Terminate the instance.
- [ ] **Step 2: Run source-free preflight.** Stop on any authority, numeric, throughput, memory, capability, progress, or cleanup failure. Do not reinterpret infrastructure failure as science.
- [ ] **Step 3: Run one corpus-only build if preflight passes.** Stream the frozen 1M Parquet source in-region. Preserve proposal/admission/capacity histograms, work counts, times, complete cgroup memory telemetry, relation/posting identities, terminal, and cleanup. No queries or GT are staged.
- [ ] **Step 4: Run one GT-only ceiling if build passes.** Preserve lower/upper aggregates, minimum, percentiles, exact-solved count, solver visits, per-query certificates, disposition, artifact identities, resource telemetry, and cleanup.
- [ ] **Step 5: Enforce the stop.** A certified fail or indeterminate result ends V38. A feasible result opens only a new written routing/physical-page design; it does not launch another arm or population.
- [ ] **Step 6: Record evidence.** Append only authenticated measurements and explicit fences to the attempt ledger. Run `python3 scripts/validate_research_docs.py` and `git diff --check`, commit the one-file evidence slice, and push fast-forward.

### Task 7: Final affected and repository assurance

**Files:** all changed paths from Tasks 1-6.

- [ ] **Step 1: Run affected gates.** Run all `v38_boundary_` library tests, the V38 example tests, and the complete V38 Python controller file serially.
- [ ] **Step 2: Run static gates.** Run `cargo fmt --all -- --check`, Ruff 0.15.20, py_compile, docs validation, and `git diff --check`.
- [ ] **Step 3: Run one strict Clippy.** Run `cargo clippy --locked --workspace --all-targets -- -D warnings` as the sole pressure-monitored process.
- [ ] **Step 4: Run one full Rust gate.** Run `cargo test --locked --workspace --all-targets` once, on remote compute if local PSI/swap is unhealthy.
- [ ] **Step 5: Run one dependency-complete Python discovery.** Run
  `uv run --offline --python 3.12 --with-requirements scripts/requirements-format-bench.txt python -m unittest discover -s scripts -p 'test_*.py'`
  and preserve the sole original terminal.
- [ ] **Step 6: Obtain independent final review.** Start one dual Fable/Astra critique of the stable diff. Convert each accepted Critical/Important finding into a focused RED and minimal repair, then rerun only that layer and one final affected gate.
- [ ] **Step 7: Publish.** Push fast-forward to `origin/main`; require `HEAD == origin/main == ls-remote` and a clean worktree. Do not launch a larger scientific stage from this task.
