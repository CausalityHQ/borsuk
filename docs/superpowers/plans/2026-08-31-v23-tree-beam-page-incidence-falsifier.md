# V23 Tree-Beam Page-Incidence Falsifier Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace rejected exhaustive leaf scoring with a deterministic SIMD tree beam, prove its causal quality ceiling without paid compute, and qualify it for one bounded burned-development Spot run.

**Architecture:** The current depth-16 tree and both leaf-to-page posting planes remain the immutable router data. A bounded width-32/64/128 level beam ranks leaves through internal child centroids, while an isolated exhaustive control exists only inside a claim-ineligible local quality screen. Evaluation/result schemas move to v2, and the new direct runners use explicit authenticated local files with no storage client or namespace/loader machinery.

**Tech Stack:** Rust 2024, `borsuk-fma`, Arrow/Parquet, serde JSON, BLAKE3/SHA-256, Python 3.12 launcher tests, AWS EC2 Spot only after the local screen.

**Spec:** `docs/superpowers/specs/2026-08-31-v23-tree-beam-page-incidence-falsifier-design.md`

## Global Constraints

- No `ldd`, copied loader/runtime, private root, `chroot`, `pivot_root`, mount/PID/network namespace, network canary, or filesystem sandbox.
- No v1 evaluation reader, aliases, fallback scorer, or compatibility path; v1 artifacts remain ledger evidence only.
- Query tables cross languages as exact Parquet/Arrow `emb: FixedSizeList<element: Float32, 96>`; hot tree/posting data remains its authenticated binary codec.
- Production scoring uses only the fused eight-lane-by-twelve-step f32 SIMD kernel and fails closed when it is unavailable.
- The serving path never exhaustively scans leaves and always returns exactly eight unique pages.
- The local screen may open only tree, posting planes, D2 report, and query ordinals 0--31; no page body, neighbors, holdout, storage API, or D3 input.
- Quality gates remain 975,000 aggregate recall ppm, 800,000 minimum-query recall ppm, and 995,000 oracle-attainment ppm.
- Serving gates remain 3 GiB projected RAM, 262,144 posting visits, 8,192 touched pages, and 15,000,000 ns warm p99.
- All scientific outputs are `claim_eligible=false`; D3 remains fenced.

---

### Task 1: Deterministic bounded tree-beam selector

**Files:**
- Modify: `crates/borsuk/src/v23_incidence_tree.rs`

**Interfaces:**
- Produces: `v23_tree_beam_centroid_scores(beam_width: usize) -> Result<u32>`.
- Produces: `rank_v23_incidence_tree_beam(tree: &V23IncidenceTree, query: &[f32; 96], beam_width: usize) -> Result<Vec<u16>>`.
- Produces under `cfg(test)`: `rank_v23_incidence_tree_beam_scalar` with the same signature.

- [ ] **Step 1: Add exact-work and traversal RED tests**

Add tests named `v23_tree_beam_work_is_exact_and_bounded` and
`v23_tree_beam_orders_ties_and_matches_scalar`. Construct a complete synthetic
depth-8 tree with valid child indices and finite f16 centroids. Assert exact
score counts `766`, `1_406`, and `2_558` for the production depth-16 widths,
exact leaf ordering under ties by global child ordinal, identical scalar/SIMD
outputs, and rejection of widths outside `[32, 64, 128]`, malformed child
indices, non-finite norms, and zero queries.

- [ ] **Step 2: Run the narrow RED**

Run: `cargo test -p borsuk --lib v23_tree_beam_ -- --nocapture`

Expected: compilation fails only because the three tree-beam interfaces do not exist.

- [ ] **Step 3: Implement fixed-capacity level traversal**

Define a private candidate containing `distance: f32` and `global_index: u32`.
Normalize once, start from node zero, expand both children at every level,
order by `distance.total_cmp` then `global_index`, and truncate to the beam
width. Reuse `Vec` buffers with capacities 128 and 256; reject a capacity
increase or a final non-leaf candidate. At level 16 convert global indices to
u16 leaf ordinals. The scalar test implementation uses the registered
lane/step `mul_add` order, never a non-fused production fallback.

- [ ] **Step 4: Run the narrow GREEN**

Run: `cargo test -p borsuk --lib v23_tree_beam_ -- --nocapture`

Expected: both tests pass with no warnings.

- [ ] **Step 5: Commit Task 1**

```bash
git add crates/borsuk/src/v23_incidence_tree.rs
git commit -m "Add bounded incidence tree-beam selector"
```

### Task 2: Evaluation v2 scorer and authority

**Files:**
- Modify: `crates/borsuk/src/v23_incidence_eval.rs`
- Modify: `crates/borsuk/src/v23_incidence.rs`

**Interfaces:**
- Replaces: `V23IncidenceCell { cap, arm, probes }` with `V23IncidenceCell { cap, arm, beam_width }`.
- Produces: `query_router: String` fixed to `centroid-tree-beam-v1` in development and campaign authority.
- Produces: per-cell `scored_centroids_per_query: u32` and `distance_dimensions_per_query: u32`.
- Replaces schemas with `borsuk-v23-incidence-development-v2`, `borsuk-v23-incidence-holdout-truth-v2`, and `borsuk-v23-incidence-result-v2`.

- [ ] **Step 1: Add scorer and schema RED tests**

Rename the existing grouped query tests to the `v23_tree_beam_evaluation_`
prefix and make their fixtures contain valid internal nodes. Add mutations for
router string, beam width, score count, dimension count, v1 schema, missing or
extra fields, non-finite distance, tree-beam leaf order, scalar/optimized leaf
inequality, and exact-eight-page output. Assert the registered ladder remains
18 cells ordered by cap, arm, beam width.

- [ ] **Step 2: Run the narrow RED**

Run: `cargo test -p borsuk --lib v23_tree_beam_evaluation_ -- --nocapture`

Expected: failures identify the old exhaustive scorer and missing v2 fields.

- [ ] **Step 3: Replace serving ranking and serializers**

Call `rank_v23_incidence_tree_beam` from both native and scalar page-reducer
paths. Delete serving calls to `rank_incidence_leaves_with_shape`. Keep an
exhaustive helper private and callable only by the Task 4 screen. Rename the
cell field everywhere without serde aliases. Make canonical serializers
recompute `v23_tree_beam_centroid_scores(beam_width)` and multiply by 96 with
checked arithmetic. Reject v1 schemas and any router/work mutation.

- [ ] **Step 4: Run focused and grouped GREEN tests**

Run:

```bash
cargo test -p borsuk --lib v23_tree_beam_evaluation_ -- --nocapture
cargo test -p borsuk --lib v23_incidence_ -- --nocapture
```

Expected: focused tests and the complete incidence group pass.

- [ ] **Step 5: Commit Task 2**

```bash
git add crates/borsuk/src/v23_incidence_eval.rs crates/borsuk/src/v23_incidence.rs
git commit -m "Replace incidence evaluation with tree beam"
```

### Task 3: Exact preflight and serving-memory projection

**Files:**
- Modify: `crates/borsuk/src/v23_incidence_eval.rs`
- Modify: `crates/borsuk/src/v23_incidence.rs`

**Interfaces:**
- Produces: width-128 preflight work of `2_455_680_000` distance dimensions for 10,000 queries.
- Produces: full development work of `30_121_850_880` dimensions.
- Produces: worst holdout work of `2_738_574_336` dimensions.
- Produces: maximum 100M serving projection of `1_776_959_108` bytes.

- [ ] **Step 1: Add projection RED tests**

Mutation-lock the three exact work totals, the 64-MiB complete decoded-tree
bound, 4,096-byte beam workspace, final serving total, 80% throughput divisor,
5,400-second wall gate, posting-visit rate, and overflow behavior. Assert the
preflight actually invokes width 128 and reports its measured score count.

- [ ] **Step 2: Run the narrow RED**

Run: `cargo test -p borsuk --lib v23_tree_beam_preflight_ -- --nocapture`

Expected: old 65,536-leaf work totals fail.

- [ ] **Step 3: Implement exact projections**

Replace the development and holdout `full_distance_dimensions` constants with
checked calculations from the registered invocation counts and tree-beam work.
Measure the same selector used by serving. Replace leaf-only memory accounting
with a 64-MiB decoded-tree bound and the fixed beam workspace, retaining all
posting, page, reserve, and headroom terms.

- [ ] **Step 4: Run focused and grouped GREEN tests**

Run:

```bash
cargo test -p borsuk --lib v23_tree_beam_preflight_ -- --nocapture
cargo test -p borsuk --lib v23_incidence_ -- --nocapture
```

- [ ] **Step 5: Commit Task 3**

```bash
git add crates/borsuk/src/v23_incidence_eval.rs crates/borsuk/src/v23_incidence.rs
git commit -m "Bind tree-beam incidence resource projections"
```

### Task 4: Zero-spend causal quality screen

**Files:**
- Modify: `crates/borsuk/src/v23_incidence_eval.rs`
- Modify: `crates/borsuk/src/v23_incidence.rs`
- Modify: `crates/borsuk/src/lib.rs`
- Create: `crates/borsuk/examples/v23_incidence_development_screen.rs`
- Create: `scripts/run_v23_incidence_development_screen.py`
- Create: `scripts/test_run_v23_incidence_development_screen.py`

**Interfaces:**
- Produces: `V23IncidenceScreenLocalRequest` with seven explicit authenticated local roles: tree receipt, tree, posting receipt, posting one, posting two, D2 report, and query Parquet.
- Produces: `run_v23_incidence_development_screen(request: V23IncidenceScreenLocalRequest) -> Result<Vec<u8>>`.
- Produces schema: `borsuk-v23-incidence-development-screen-v1`.
- Produces classifications: `leaf-incidence-quality-rejected`, `tree-beam-selector-rejected`, or `tree-beam-screen-passed`.
- Produces: a credentialed Python stager with frozen constants for exactly the seven roles and explicit named-file cleanup.

- [ ] **Step 1: Add artifact and causal-classification RED tests**

Use small coherent trees/postings and Parquet query fixtures. Cover all seven
URI/digest/length identities, receipt-to-output cross-bindings, D2 truth,
ordinals 0--31, exact query schema, no page reads, complete 18-cell beam and
18-cell exhaustive control results, first-passing fixed-order selection, and
all three classifications. Mutate exhaustive-only pass, neither pass,
tree-beam pass, query/order/type/digest drift, and coherent-but-unregistered
identity drift.

- [ ] **Step 2: Run the library RED**

Run: `cargo test -p borsuk --lib v23_incidence_screen_ -- --nocapture`

Expected: unresolved screen request/runner/artifact interfaces only.

- [ ] **Step 3: Implement strict loader and screen**

Authenticate bytes before semantic parsing. Decode current tree/posting
formats, validate both receipts against their objects, read exactly query rows
0--31 from the full Parquet artifact, and obtain unchanged development truth
from the authenticated D2 report. Evaluate all 18 beam cells and all 18
exhaustive controls. The exhaustive function remains private to this screen.
Canonical serialization independently recomputes every selection, metric,
cell order, authority binding, selected cell, and classification.

- [ ] **Step 4: Add thin direct CLI RED then GREEN**

The example requires one explicit execute flag, seven local paths with exact
URI/digest/length flags, one output path, and no bucket, endpoint, page,
neighbor, holdout, AWS, or D3 flag. Unknown, duplicate, missing, and malformed
flags fail closed. `main` writes only canonical bytes to the requested output.

Run:

```bash
cargo test -p borsuk --example v23_incidence_development_screen v23_incidence_screen_ -- --nocapture
```

Expected after implementation: parser tests pass and report no warnings.

Add the Python stager with no discovery or generic loader abstraction. It
downloads the seven constant URIs, authenticates registered length plus SHA-256
or BLAKE3, invokes the release example once, moves a successful canonical
result to the requested path, and unlinks the seven exact basenames plus any
partial output before removing its one `mktemp` directory. Its tests mock S3
and subprocess boundaries and mutation-lock every constant, command argument,
terminal class, cleanup path, and absence of page/neighbor/holdout/D3 access.

- [ ] **Step 5: Run grouped Task 4 gates and commit**

```bash
cargo test -p borsuk --lib v23_incidence_screen_ -- --nocapture
cargo test -p borsuk --example v23_incidence_development_screen v23_incidence_screen_ -- --nocapture
python3 -m unittest scripts.test_run_v23_incidence_development_screen
git diff --check
git add crates/borsuk/src/v23_incidence_eval.rs crates/borsuk/src/v23_incidence.rs crates/borsuk/src/lib.rs crates/borsuk/examples/v23_incidence_development_screen.rs scripts/run_v23_incidence_development_screen.py scripts/test_run_v23_incidence_development_screen.py
git commit -m "Add causal incidence development screen"
```

### Task 5: Simple direct development execution

**Files:**
- Modify: `crates/borsuk/src/v23_incidence.rs`
- Modify: `crates/borsuk/examples/v23_leaf_page_incidence_falsifier.rs`
- Modify: `scripts/launch_v23_incidence_spot.py`
- Modify: `scripts/test_launch_v23_incidence_spot.py`

**Interfaces:**
- Replaces: namespace-dependent `V23IncidenceCapabilityProbes` with exact local-input inventory, credential absence, and output-writable evidence.
- Produces phase-specific evaluation receipt `borsuk-v23-incidence-evaluation-receipt-v1` and manifest `borsuk-v23-incidence-evaluation-manifest-v1`.
- Preserves: one process group, progress chain, pressure/swap/wall monitoring, immutable evidence upload, Spot termination, and no automatic restart.

- [ ] **Step 1: Stage simple-boundary RED tests**

Assert the Rust request and receipt contain no namespace inode/canary fields and
reject AWS environment variables, URL-like local paths, forbidden roles, or an
unexpected staged file. Assert launcher user data contains none of `unshare`,
`nsenter`, `ldd`, `chroot`, `pivot_root`, loader copying, or mount commands;
starts the ordinary release binary directly; scrubs AWS variables; monitors
the one process group; and includes only development roles.

- [ ] **Step 2: Run RED tests**

Run:

```bash
cargo test -p borsuk --lib v23_incidence_simple_boundary_ -- --nocapture
python3 -m unittest scripts.test_launch_v23_incidence_spot -k tree_beam
```

Expected: old namespace fields and `unshare` path cause the intended failures.

- [ ] **Step 3: Remove the namespace/loader machinery**

Delete namespace parsing, inode fields, canary fields, and `unshare` execution.
The credentialed Python parent stages exact named files, then starts the release
binary directly with a minimal environment containing no AWS credential or
metadata variables. Rust authenticates only absolute regular local files,
complete exact role inventory, executable digest, preflight receipt, and output
directory. The new evaluation manifest/receipt binds tree and posting object
identities directly; it neither parses nor accepts the earlier general phase
receipt schema. Keep process-group termination, progress, PSI, swap, wall,
evidence, and Spot cleanup unchanged.

- [ ] **Step 4: Run GREEN and complete launcher tests**

```bash
cargo test -p borsuk --lib v23_incidence_simple_boundary_ -- --nocapture
python3 -m unittest scripts.test_launch_v23_incidence_spot -k tree_beam
python3 -m unittest scripts.test_launch_v23_incidence_spot
```

- [ ] **Step 5: Commit Task 5**

```bash
git add crates/borsuk/src/v23_incidence.rs crates/borsuk/examples/v23_leaf_page_incidence_falsifier.rs scripts/launch_v23_incidence_spot.py scripts/test_launch_v23_incidence_spot.py
git commit -m "Simplify incidence evaluation execution boundary"
```

### Task 6: Complete local assurance and delivery

**Files:**
- Modify only if a focused gate identifies a defect in the preceding task's files.

**Interfaces:**
- Produces: one clean fast-forward commit chain on `origin/main`.

- [ ] **Step 1: Run affected gates**

```bash
cargo test -p borsuk --lib v23_incidence_ -- --nocapture
cargo test -p borsuk --example v23_incidence_development_screen v23_incidence_screen_ -- --nocapture
cargo test -p borsuk --example v23_leaf_page_incidence_falsifier v23_incidence_ -- --nocapture
python3 -m unittest scripts.test_launch_v23_incidence_spot
uv run --offline --with ruff==0.15.20 ruff check scripts/launch_v23_incidence_spot.py scripts/test_launch_v23_incidence_spot.py scripts/run_v23_incidence_development_screen.py scripts/test_run_v23_incidence_development_screen.py
python3 -m py_compile scripts/launch_v23_incidence_spot.py scripts/test_launch_v23_incidence_spot.py
cargo fmt --all -- --check
git diff --check
```

- [ ] **Step 2: Run repository assurance once**

Run serially under memory-pressure monitoring:

```bash
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace --all-targets
```

Expected: strict Clippy is clean and the complete workspace test gate passes.

- [ ] **Step 3: Push fast-forward and verify equality**

```bash
git fetch origin main
git merge-base --is-ancestor origin/main HEAD
git push origin HEAD:main
test "$(git rev-parse HEAD)" = "$(git rev-parse origin/main)"
test "$(git rev-parse HEAD)" = "$(git ls-remote origin refs/heads/main | cut -f1)"
test -z "$(git status --porcelain)"
```

### Task 7: Run the zero-spend screen and enforce the fence

**Files:**
- Modify after the terminal only: `docs/research/publication-v3-attempt-ledger.md`

**Interfaces:**
- Consumes: the seven immutable identities from the spec and the verified release screen binary.
- Produces: one canonical screen artifact and one ledger disposition.

- [ ] **Step 1: Prepare one explicit named-file scratch command**

Build the release example offline, record its SHA-256 and size, then invoke the
verified stager exactly once:

```bash
cargo build --release --offline --locked -p borsuk --example v23_incidence_development_screen
python3 scripts/run_v23_incidence_development_screen.py \
  --profile causality \
  --region eu-central-1 \
  --binary "$PWD/target/release/examples/v23_incidence_development_screen" \
  --output "/tmp/v23-incidence-tree-beam-screen-$(git rev-parse HEAD).json"
```

The stager downloads only the seven exact inputs into one `mktemp -d`,
authenticates all identities, and executes the direct screen binary under a
2-GiB RSS, PSI full avg10 0.79, 256-MiB swap-growth, five-minute progress, and
7,200-second wall stop. Its trap unlinks exactly the seven input basenames and
partial result before rmdir.

- [ ] **Step 2: Execute once and classify**

Do not restart after any terminal. Preserve the canonical screen output outside
scratch, verify its SHA-256, confirm PID/scratch clearance, and classify exactly
one of the three registered outcomes. If the tree-beam screen does not pass,
do not launch Spot.

- [ ] **Step 3: Record and commit evidence**

Add source/binary/input/result identities, all 36 cell metrics, causal class,
resource observations, cleanup evidence, and explicit holdout/D3 fence to the
ledger. Run `python3 scripts/validate_research_docs.py` and `git diff --check`,
commit the one-file evidence update, and push fast-forward.

- [ ] **Step 4: Stop or request the separately bounded Spot step**

Only `tree-beam-screen-passed` permits preparation of one Spot development
attempt. The other two outcomes end this architecture without paid work.
