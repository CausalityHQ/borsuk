# Publication, Production Defaults, and Graph Optimization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the shipped defaults, all documentation, and every webpage describe one measured TurboQuant pq-scan production system, then reduce graph-method latency with a recall-matched measured optimization.

**Architecture:** New indexes and implicit approximate searches use the graph-free TurboQuant path; legacy manifests remain graph-enabled and graph construction is an explicit opt-in. Canonical Markdown owns detailed evidence, dated CSV/SVG artifacts own measurements, and static webpages surface only claims that link back to those artifacts. Graph changes follow profiling, a failing regression benchmark/test, and one-variable-at-a-time measurement.

**Tech Stack:** Rust, PyO3, N-API/TypeScript, Clap, Markdown, static HTML/CSS/JavaScript, Python standard-library validators/renderers, AWS S3/EC2 benchmark harnesses.

---

### Task 1: Align production defaults without breaking legacy indexes

**Files:**
- Modify: `crates/borsuk/src/record.rs`
- Modify: `crates/borsuk/src/index.rs`
- Modify: `crates/borsuk/tests/leaf_capability.rs`
- Modify: `crates/borsuk-python/src/lib.rs`
- Modify: `python/src/borsuk/__init__.py`
- Modify: `crates/borsuk-node/src/lib.rs`
- Modify: `crates/borsuk-cli/src/main.rs`
- Modify: binding and CLI tests beside those APIs

- [ ] **Step 1: Write failing default-contract tests**

Assert `LeafMode::default() == PqScan`, ordinary index creation writes no graph objects, implicit approximate binding/CLI search reports `pq-scan`, and explicit graph-enabled creation still writes/serves graph objects. Preserve a fixture/assertion that a manifest with no `leaf_capability` reopens as `GraphEnabled`.

- [ ] **Step 2: Run focused tests and verify the intended failures**

```bash
cargo test --locked -p borsuk --test leaf_capability
cargo test --locked -p borsuk-python
cargo test --locked -p borsuk-node
cargo test --locked -p borsuk-cli
```

Expected: new assertions fail because creation and implicit approximate leaf selection still default to graph.

- [ ] **Step 3: Implement the minimal compatibility-safe default change**

Keep missing legacy manifest capability fields mapped explicitly to `GraphEnabled`; use `PqScanOnly` for new ordinary creation and `PqScan` for implicit approximate leaf selection. Expose graph-enabled creation as an explicit typed option in Rust, Python, TypeScript, and CLI rather than making it unreachable.

- [ ] **Step 4: Verify focused and cross-language behavior**

Run the focused commands above plus the Python and TypeScript API tests. Expected: graph-free default assertions pass; explicit graph tests still pass; legacy reopen remains graph-enabled.

### Task 2: Make documentation completeness executable

**Files:**
- Modify: `scripts/test_validate_research_docs.py`
- Modify: `scripts/validate_research_docs.py`
- Modify: `scripts/test_docs_web.mjs`

- [ ] **Step 1: Add failing validator fixtures**

Require the current defaults (`pq-scan-only`, query cap 4, decode cap 24, dimension-aware cells), precise BUSL grant/change-date language, deployment examples for file/S3/cache, Python LangChain and TypeScript examples, a dated price source/model, SIMD dispatch explanation, novelty/prior-art boundaries, six recall/latency charts, resource/scaling charts, and explicit evidence limitations.

- [ ] **Step 2: Verify failures identify the existing drift**

```bash
PYTHONPATH=scripts python3 -m unittest scripts/test_validate_research_docs.py
node scripts/test_docs_web.mjs
```

Expected: failures cite the stale decode-cap 12 statement, missing public cost/deployment/SIMD material, and obsolete landing-page synthetic claims.

- [ ] **Step 3: Extend validators minimally**

Validate semantic markers and local artifact links without hard-coding prose wholesale. Parse the production-profile CSV to compare documented defaults with data rather than duplicating numeric constants in tests.

### Task 3: Publish a dated, reproducible cost model

**Files:**
- Create: `docs/web/assets/benchmarks/aws-cost-model-2026-07-20.csv`
- Create: `docs/research/cost-and-deployment.md`
- Modify: `docs/research/systems-comparison.md`
- Modify: `docs/research/README.md`

- [ ] **Step 1: Record measured BORSUK cost inputs**

Store the six selected index footprints, measured GETs/query, Frankfurt S3 Standard `$0.0245/GB-month`, and `$0.43/million GETs`. Derive storage/month and uncached GET cost/million queries; state that cached queries have zero measured backing GETs. Treat application/client compute as common to every system. Record the benchmark host's `$1.3192/hour` only as reproduction context: BORSUK executes search inside that client process, while managed products include remote search compute in their API bill, so the host price is not a BORSUK-only surcharge or a normalized product comparison.

- [ ] **Step 2: Add primary-source vendor context**

Date every price snapshot and link official AWS S3 Vectors, turbopuffer, Pinecone, and Chroma pages. Do not normalize incompatible billing dimensions into a winner; show minimums/list prices and one formula-driven BORSUK scenario.

- [ ] **Step 3: State the exact license and deployment boundary**

Explain that the software fee is zero only inside the BUSL Additional Use Grant: production use at or below US `$100,000` gross annual revenue across affiliates; larger organizations need a commercial license; the work changes to MIT on 2030-07-02 or the earlier per-version fourth anniversary. State that durable state needs only local disk or blob storage, while queries still need an application/library process and optional cache disk.

### Task 4: Rewrite the production-facing docs and integration path

**Files:**
- Modify: `README.md`
- Modify: `docs/api.md`
- Modify: `docs/architecture.md`
- Modify: `docs/production-readiness.md`
- Modify: `docs/benchmarks.md`
- Modify: `docs/drop-in.md`
- Create: `docs/deployment-and-integrations.md`
- Modify: runnable examples only where defaults require updates

- [ ] **Step 1: Remove contradicted marketing claims**

Replace “few hundred bytes”, “near-zero RAM”, “perfect recall” without qualification, and unconditional “drop-in replacement” with measured total-RSS envelopes, exact-vs-approx semantics, and compatibility-adapter limits.

- [ ] **Step 2: Document one production architecture**

Describe persisted serving metadata, HNSW-over-cell routing, bounded cell reads, TurboQuant-4b/SRHT shortlist scoring, lossless row-range rerank, query cap 4, decode cap 24, optional read-through disk cache, and graph-free default indexes.

- [ ] **Step 3: Add copy-paste deployment examples**

Include local `file://`, AWS `s3://`, S3-compatible endpoint, local cache, Rust, Python, TypeScript, LangChain/LCEL, and compatibility-adapter examples that point to tested repository examples.

- [ ] **Step 4: Explain SIMD and portability honestly**

Document compile/runtime dispatch, scalar fallback, Arm/x86 paths actually present in the code, padded SRHT behavior, and where exact reranking/scoring benefits. Do not claim an instruction set not proven by source/tests.

### Task 5: Rebuild the public webpages around current evidence

**Files:**
- Modify: `docs/web/index.html`
- Modify: `docs/web/docs.html`
- Modify: `docs/web/research.html`
- Modify: `docs/web/landing.css`
- Modify: `docs/web/styles.css`
- Modify: `docs/web/app.js` only if chart loading requires it

- [ ] **Step 1: Replace obsolete landing proof**

Use the six-corpus production envelope: recall-qualified all six; uncached p95 121–874 ms; disk-cached p95 11.7–79.2 ms; selected peak RSS 230–749 MiB. Label the date, hardware, cache contract, and artifact link.

- [ ] **Step 2: Surface deployment, license, and cost boundaries**

Make “no database service” distinct from “no compute”; show local/object-storage/cache topology and the BUSL grant without calling BORSUK unrestricted open source or universally free.

- [ ] **Step 3: Embed evidence, not decorative charts**

Show the six checked-in recall/latency SVGs plus representative CPU/RSS/disk/cache and scaling plots with captions that state dataset, cache state, and evidence class.

- [ ] **Step 4: Deepen the research navigation**

Expose standard datasets, methods, layout/cap ablations, scale, costs, systems comparison, SIMD/novelty, reproduction, raw data, and explicit missing baseline cells.

### Task 6: Verify the complete documentation surface

**Files:**
- Verify all Markdown, HTML, CSS, JS, CSV, SVG, and example links above.

- [ ] **Step 1: Run documentation gates**

```bash
PYTHONPATH=scripts python3 -m unittest scripts/test_validate_research_docs.py
python3 scripts/validate_research_docs.py
node scripts/test_docs_web.mjs
```

Expected: all requirements and internal links pass.

- [ ] **Step 2: Serve and inspect at desktop and mobile widths**

Run the static site locally and capture/inspect landing, docs, and research pages at representative desktop and mobile viewports. Expected: no overflow, broken charts, missing anchors, or inaccessible controls.

- [ ] **Step 3: Audit every claim against source or artifact**

For each numeric/public claim, identify the CSV/code/license/primary source that proves it; remove or qualify anything without direct evidence.

### Task 7: Profile graph latency before changing the engine

**Files:**
- Inspect: `crates/borsuk/src/index.rs`
- Inspect: graph format/search modules and existing graph benchmarks
- Create: a dated graph profile artifact under `docs/web/assets/benchmarks/raw/`
- Modify: `crates/borsuk/examples/production_bench.rs` only if a missing timing boundary is required

- [ ] **Step 1: Reproduce the recall/latency curve**

Re-run the Fashion graph candidate sweep at the existing graph-enabled index and retain p50/p95, recall, graph bytes, row scores, GETs, CPU, RSS, and disk/cache telemetry.

- [ ] **Step 2: Attribute time by boundary**

Measure cell projection, graph object fetch/decode, entry selection, traversal, candidate materialization, sidecar range reads, and exact rerank. Form one root-cause hypothesis only after the profile identifies the dominant term.

- [ ] **Step 3: Establish a recall-matched gate**

Choose the lowest graph configuration meeting the same recall threshold as pq-scan and record the latency/resource ratio. Do not compare unmatched candidate budgets as the optimization target.

### Task 8: Implement and publish one graph optimization

**Files:**
- Modify: the graph search/layout module identified by Task 7
- Add: focused unit/integration regression tests
- Update: `docs/research/methods.md`
- Update: dated graph CSV/SVG artifacts

- [ ] **Step 1: Write and run the failing regression test**

Encode the measured redundant work or I/O invariant from the confirmed root cause; verify the test fails for the old behavior.

- [ ] **Step 2: Implement the smallest root-cause fix**

Change one mechanism only, preserving exact rerank, deterministic results, graph capability errors, and storage compatibility unless an explicitly versioned layout is justified by the profile.

- [ ] **Step 3: Verify correctness and measure the effect**

Run focused tests, the full workspace, and the identical graph benchmark. Accept only if recall is unchanged/qualified and latency or resource use improves outside run-to-run noise; otherwise retain the negative result and revert the production change.

- [ ] **Step 4: Publish the honest comparison**

Update method curves and resource charts, showing graph vs pq-scan at matched recall and noting whether graph reached, beat, or failed to approach the production path.

### Task 9: Final completion audit

**Files:**
- Verify the entire worktree and active objective.

- [ ] **Step 1: Run full verification**

```bash
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace --all-targets
PYTHONPATH=scripts python3 -m unittest discover scripts 'test_*.py'
node scripts/test_docs_web.mjs
git diff --check
```

Expected: zero failures. Run package-specific Python/TypeScript suites for every changed binding surface.

- [ ] **Step 2: Re-run requirement-by-requirement evidence audit**

Prove defaults, production architecture, comparisons, price, license, storage-only durable state, local/S3/LangChain/TypeScript examples, latency/recall charts, latency/memory scaling, SIMD, novelty, and graph optimization with authoritative current evidence before marking the goal complete.
