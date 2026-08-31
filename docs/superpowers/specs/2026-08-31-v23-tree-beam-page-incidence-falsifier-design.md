# V23 Tree-Beam Page-Incidence Falsifier

**Status:** Approved replacement falsifier. This design replaces the rejected
exhaustive-leaf evaluation path. It authorizes local implementation and one
claim-ineligible burned-development run after verification. It does not
authorize holdout evaluation, D3, a publication claim, or a production-format
freeze.

## Decision and evidence

The sealed 65,536-leaf tree and its one-leaf and two-leaf page-posting planes
were constructed deterministically from query-blind data. Their construction
evidence remains valid. The first development preflight rejected only the
serving algorithm: exhaustive ranking of all 65,536 leaves measured
336,783,428 distance dimensions/second and averaged 18.681 ms/query before
posting accumulation. Its 1,252,050,075,648-dimension development projection
exceeded the 5,400-second wall gate. No query cohort was opened, no quality
result was produced, and no cell was selected.

The replacement retains the authenticated tree, postings, reciprocal-rank page
reducer, exact-eight-page output, and frozen quality gates. It replaces the
exhaustive leaf scan with deterministic beam traversal through the existing
depth-16 tree. This tests whether the query-independent tree can provide the
missing serving acceleration without rebuilding the corpus or changing page
evidence.

## Alternatives

### Deterministic tree beam — selected

The existing tree already stores both child centroids and child indices at
every internal node. A bounded beam uses that structure directly and requires
no new scientific input. It is the smallest experiment that can produce a
quality and latency result from the sealed router.

### Fixed SimHash incidence — deferred

SimHash would be a genuinely different router but requires a new query-blind
corpus/page construction stream and introduces bit-count, table-count, and
multi-probe parameters. It is the next construction candidate only if the
tree beam fails quality rather than resource gates.

### Distributional page sketches — rejected for this step

Page-local moments, envelopes, or residual sketches repeat the lossy
page-summary mechanism already rejected by K32. They are not a smaller or more
decisive next experiment.

## Immutable inputs

The evaluator accepts the existing current-format artifacts, not legacy
adapters:

- tree receipt: 26,106 bytes, SHA-256
  `c1af5ab84ef20797ffe52fa0a93872008df817c142957f009895c8b7fc853a99`;
- incidence tree: 40,369,836 bytes, BLAKE3
  `aa72bf926c6fcbd17890188d8b3bd3b35393d9c392bffc032e75328ea47fae64`;
- posting receipt: 13,407,759 bytes, SHA-256
  `cca5b1f895fd633ad5e6fab0288f6838d3efa9087f83809fc0c2032736ff6aca`;
- one-leaf postings: 51,502,404 bytes, BLAKE3
  `b5f6b1009e67d8286f012d80d4eea0f52d2516db70ddbad88e1e4477e3ae7c61`;
- two-leaf postings: 59,186,088 bytes, BLAKE3
  `ad75479318297d9c95e0f8f71220e7a5f2d283440be762238ea0bb8959f6897d`;
- D2 report: 25,725,198 bytes, SHA-256
  `bb8f97360827abd0f18964982c9729c083888ad02ad4cc08d1ba6779100f409a`;
- query Parquet: 3,843,448 bytes, SHA-256
  `296d45828020c1c0b88c6a1d5c822f6283280513b8c58d01cfa961f3a139a5d4`.

Only query ordinals 0--31 may be opened in development. Query vectors cross
the language boundary as the exact non-null
`emb: FixedSizeList<element: Float32, 96>` Parquet column. JSON is limited to
small typed manifests, reports, receipts, and results. No page body, neighbor
Parquet, holdout truth, or D3 object is an evaluator input.

## Query algorithm

The query is normalized once. The beam starts with internal node ordinal zero.
For each of the 16 levels:

1. expand both children of every retained internal node;
2. score each child centroid by cosine distance using the registered fused
   eight-lane-by-twelve-step f32 kernel and its stored post-f16 inverse norm;
3. order candidates by `(distance.total_cmp, global child ordinal)`;
4. retain the first `min(beam_width, candidate_count)` candidates.

At the final level, every child is a leaf. Convert its global child ordinal to
the exact u16 leaf ordinal and preserve final-level distance order. The
registered beam widths are 32, 64, and 128. No cumulative path score,
backtracking, heuristic early exit, exhaustive fallback, or query-dependent
parameter exists.

For beam width `B`, the exact number of scored centroids is

`T(B) = sum(level=1..16, min(2^level, 2B))`.

| Beam width | Scored centroids | Distance dimensions/query | Reduction vs 65,536 leaves |
|---:|---:|---:|---:|
| 32 | 766 | 73,536 | 85.56x |
| 64 | 1,406 | 134,976 | 46.61x |
| 128 | 2,558 | 245,568 | 25.62x |

The fixed development ladder retains posting caps 512, 1,024, and 2,048,
assignment arms one-leaf then two-leaf, and beam widths 32, 64, then 128. Its
lexicographic order is cap, assignment arm, beam width. The first complete
cell passing every gate is sealed. The field is named `beam_width`; the
rejected `probes` evaluation field and v1 evaluation/result schemas are removed,
not aliased.

For each ranked leaf, the existing posting prefix contributes
`u16_mass * reciprocal_q32[rank]` to its pages. Exactly eight unique pages are
chosen by `(score descending, page ordinal ascending)`. Posting accumulation,
u64 overflow checks, touched-page bounds, and scalar page-reducer equality are
unchanged.

## Determinism and authority

The optimized traversal uses two fixed-capacity buffers: at most 128 retained
candidates and 256 expanded candidates. It never allocates or sorts 65,536
pairs. A scalar fused reference performs the same level-by-level expansion and
must return identical leaf ordinals for widths 32, 64, and 128 across random
finite vectors, exact ties, subnormals, reversed child ordinals, and malformed
trees. Non-finite query, centroid, norm, or distance is an authority error.

Development and campaign artifacts use new exact schemas and carry
`query_router = "centroid-tree-beam-v1"`, the exact beam width, scored-centroid
count, and distance-dimension count. Serializers independently recompute
`T(B)`, all query selections, recall, oracle attainment, budgets, latency
bindings, cell order, seal, and classification. Exhaustive v1 evaluation
artifacts are evidence-ledger records only and are not accepted by current
code.

The construction tree/posting codecs are unchanged because their bytes and
semantics are unchanged. Reusing those current-format immutable objects is
scientific input reuse, not a compatibility reader.

## Resource projection

The preflight measures 10,000 synthetic width-128 queries, so its exact
distance work is 2,455,680,000 dimensions. The full development projection
includes 32 evaluation queries plus 1,024 warm-up and 10,000 timed queries for
each of 18 cells:

`11,056 * 6 * (766 + 1,406 + 2,558) * 96 = 30,121,850,880` dimensions.

The worst-case holdout projection is

`11,152 * 2,558 * 96 = 2,738,574,336` dimensions.

Posting-visit and authenticated-input projections remain separately measured.
The wall projection remains the conservative sum of each complete work count
divided by 80% of its observed preflight throughput. The 5,400-second resource
gate is unchanged.

Serving memory replaces the old leaf-only centroid accounting with a 64-MiB
resident bound for the complete decoded tree and adds 4,096 bytes for both beam
buffers. The registered maximum projection is therefore 1,776,959,108 bytes
(about 1.655 GiB), below the 3-GiB gate. Runtime receipts record actual tree
allocation, workspace capacities, and process peak RSS; exceeding either the
component bounds or 3 GiB is a resource stop.

## Zero-spend quality screen

Before any new Spot worker, one bounded local screen may open only the same
authenticated tree, posting planes, D2 report, and query ordinals 0--31. It
computes quality for two explicitly separated selectors:

1. the production-candidate tree beam over the fixed 18-cell ladder;
2. the rejected exhaustive leaf ranker as a diagnostic ceiling over the same
   posting cells.

The exhaustive control is compiled into the claim-ineligible screen only. It
cannot be selected, serialized as a serving router, reached by the production
query function, or used for latency qualification. The screen emits one
canonical artifact binding every input identity, both complete cell sets, the
fixed gates, and these causal outcomes:

- if no exhaustive control cell passes quality, classify
  `leaf-incidence-quality-rejected` and perform no paid run;
- if an exhaustive control cell passes but no tree-beam cell passes, classify
  `tree-beam-selector-rejected` and perform no paid run;
- if a tree-beam cell passes, record the first passing cell in the fixed
  cap/arm/beam-width order and allow the separately verified Spot development
  phase to measure the complete registered ladder.

This screen may not change a parameter after seeing an outcome. It does not
open holdout rows or make a product claim. A future best-first tree descent may
be separately preregistered only for the second outcome; it is not a hidden
fallback in this experiment.

## Gates and causal outcomes

Every cell still must satisfy:

- aggregate recall at least 975,000 ppm;
- minimum-query recall at least 800,000 ppm;
- exact page-oracle attainment at least 995,000 ppm;
- exactly eight unique pages per query;
- projected 100M serving bytes at most 3 GiB;
- at most 262,144 posting visits and 8,192 touched pages/query;
- native warm p99 at most 15,000,000 ns;
- finite deterministic output and identical scalar/optimized leaves and pages.

If preflight fails, classify `resource-stop`. If no development cell passes
quality, classify `tree-beam-quality-rejected`; this rejects only this tree
beam plus the existing posting reducer, not all incidence representations. If
quality passes but structural or latency gates fail, classify the corresponding
budget or kernel rejection. Only a sealed development cell may authorize a
separate holdout design and run. D3 remains fenced.

## Simple execution boundary

The credentialed controller downloads the exact named immutable inputs, verifies
their registered digests and lengths, and then starts the ordinary release
binary once with explicit local paths and an empty AWS credential environment.
The Rust process has no AWS SDK, URL, bucket, page-prefix, endpoint, or storage
client surface. The controller monitors the one process group and preserves
canonical evidence before deleting only its named scratch files.

No `ldd`, loader copying, private root, `chroot`, `pivot_root`, bind-mounted
runtime, PID/network namespace, network canary, or filesystem sandbox is used.
Safety follows from explicit local inputs, absence of credentials and storage
APIs, process-group RSS/PSI/swap/time limits, a disposable Spot worker, and
immediate termination after the terminal marker.

## Verification and execution fence

Implementation is test-first. Narrow unit tests must prove traversal ordering,
work arithmetic, scalar/SIMD equality, page reduction, schemas, and preflight
projection before grouped incidence tests run. The launcher tests must prove
the exact local role set, credential scrubbing, absence of namespace/loader
machinery, terminal preservation, and no page/holdout/D3 roles. Final local
assurance is formatting, scoped Python checks, strict workspace Clippy, and one
locked workspace/all-targets test.

After a clean fast-forward push, at most one c7g.8xlarge Spot burned-development
attempt may be separately started, and only if the zero-spend screen accepts
the tree beam. It must stop without restart on authority, resource,
determinism, progress, interruption, or terminal failure. No holdout, D3, or
second scientific attempt follows automatically.
