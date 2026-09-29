# Bounded source-cell locality implementation plan

> Execute inline with superpowers:executing-plans. No new reviewer or delegation cycle.

**Goal:** Qualify one source-only cell-order change that can retain the fixed
native union's quality while avoiding a quadratic global centroid chain.

**Architecture:** Reuse the source assignment graph already constructed by
fit_hierarchical_source_layout. From its fixed entry, append the nearest
unvisited outgoing base-layer neighbor; when none remains, append the lowest
unvisited cell ID. This changes whole-cell order only. No extra graph build,
query-dependent construction or query work is introduced.

**Tech stack:** Existing Rust source fitter, CentroidHnsw and Candidate distance/
ID ordering; existing Causality Spot correctness/check infrastructure.

**Spec:** fine-source-layout-decision.md and this frozen preregistration.

## Evidence and constraints

The preceding fine256 / fixed-reservoir arm is KILL: ReLAION first100k D768
cosine k100, consumed dev0–63, candidate6360/6400 versus matched flat-union6363
and exhaustive6369. Remaining19 discovery misses are4 walk pool +15 centroid
roster; nomination/physical/physically-present exhaustive-flat ranking loss0.
Only5/19 misses are in boundary micro-units; do not assume padding fixes them.

Prior V61/V63 row-graph BFS packing failed; V63 graph-BFS containment82.762%
versus k-means8192 centroid-chain99.479% on that historical1M oracle. Those
are different layouts and gates, not current matched controls. Preserve
semantic groups and intra-group radius/ordinal order here; do NOT promote
row BFS or replace semantic groups. Current flat source fitter has a closed
1M120-second timeout. Do not rerun/extend that fitter. Its costly global
centroid chain motivates a bounded adjacency-local chain, not proof of gain.

Hold group count ceil(N/256), original global source reservoir
min(N,ceil(N/1024)*64), seed8201, trainer12iterations, HNSW assignment,
1024 extent ceiling, degree32 successor-cycle query graphs, source/scorer,
128seed/1272walk/159pages pergraph/max318union,32GET/16773120bytes and all
scientific gates unchanged. No count sweep, cap increase or fresh seal access.

The source-order primitive visits each cell once. Across the whole order it
inspects each visited cell's outgoing list once: at most32*C distance
computations for the default source graph, O(32*C*D) arithmetic and O(C)
fallback scans. Seen C bytes, output4*C and caller rank8*C bytes plus Vec
headers are covered by existing8192bytes/cell and327680constant conservative
admission. No retained query graph/cache or quadratic matrix is added.
This is a construction bound, NOT actual100M RAM, time or quality evidence.

## Files and ordered gates

- [ ] Add one graph regression with nearest unvisited edge, determinism,
  complete permutation, disconnected fallback and distance/ID tie behavior.
  The local new method is an unqualified numeric-order RED stub; production
  source fitting remains unchanged until closed proof.
- [ ] Run assertion RED on AWS, requiring the exact intended failure rather
  than compilation/I/O/geometry failure.
- [ ] Frozen worker applies three exact declared transforms: implement
  layer0_nearest_order in centroid_hnsw.rs; map original cell IDs to these
  ranks in source_order.rs; source helper recipe v3 declares
  source_cell_order=nearest-unvisited-layer0-entry-ordinal-fallback-v1.
  The existing BFS helper and all old query routes remain unchanged.
- [ ] Focused graph GREEN and existing source-order tests; rustfmt.
- [ ] One full workspace/all-targets gate justified by actual native source
  construction and helper changes:2689 top-levelpasses/0fail/26existingignore,
  same145Cargo targets (144harnesses+onebench12smokes), exact previous parent
  roster plus one regression. Reuse fresh evidence after this gate.
- [ ] Retain four release binaries; public helper on synthetic4096-row/16-mode
  source must emit v3 recipe,16 coherent256-row extents and full permutation.
- [ ] Independently authenticate393native/three exact transforms, RED/GREEN,
  exact full roster, recipe, binaries, cgroup and actualtermination. Adopt
  exact three compiled files only after this engineering GO; push coherent slice.
- [ ] Separately freeze ONE paired ReLAION-first development falsifier using
  retained binaries/current control/source/scorer/requests/truth and cached
  full SQ8 scores; complete128 plans percorpus, unchanged mean98/p0595/
  exhaustivegap<=.5pp/nonregression>=6363R/>=6347C andphysicalcaps.
  CoHere/cold only if preceding qualityGO. ScientificKILL ends exact arm.

## Review focus carried into checks

Disconnected cells still yield a complete permutation. Equal distances use
cell-ID tie ordering; edge-list order cannot change ties. Single identical
source group bypasses the graph and retains rank0. Overflow/source identity/
budget denial remain existing source tests. The construction sees no query/GT;
all corpus score/control parity must pass before quality interpretation.

## Engineering resources and cleanup

One causality eu-central-1c c7g.2xlarge Spot, four jobs/CPU0–3,80GiB encrypted
disposableEBS, worker4200s/check3900s,10GiB cgroup/zeroSwap. Spot quote<=.30/h,
compute cap$.35 plus$.10EBS/S3 allowance. Assert idempotencytoken<=64 before
reservation. Sharedlock/active-tag/prefix guards; frozen source/prereg/worker.
Monitor original session, terminal and infrastructure only; never incomplete
logs. Required finalcheck/cgroup/release/recipe/binaries must be nonempty before
producer completion; independent verifier also rejects missing outputs.
Sync terminal and terminate/wait immediately. No automatic replacement.
Interrupted or invalid cells require preserved proof before explicit correction.
No corpusANN/quality/freshsealed access in this engineering check.

EngineeringGO is not quality convergence or product completion. A survivor
needs actual source/SQ8/native object build and cold incomingHTTP, genuinely
unused cohort,1M/10M/100M,maintenance/recovery/pinRAM and matched lifecycle
wins over BOTH S3Vectors and Turbopuffer. No production defaults freeze.
