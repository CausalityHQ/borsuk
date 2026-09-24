# V146 production Rust unit-centroid score gate

**Decision:** can one dimension-generic, versioned Rust f16 unit-centroid
builder and scorer replace V140's Python score stage while preserving
β=4 physical page decisions on both frozen used cohorts, and does its
flat-scan CPU leave adequate headroom at 1M?

Use the authenticated V122 deep-image-96-angular random100k train subset,
already-used publication-test ordinals 9000–9999, and authenticated V116
ReLAION-1M validation-1000. Use the same SQ8 objects and coefficient files
as V145. Authenticate V145 complete terminal, score matrices, V140 raw
β=4 plans and the primary rosters at their recorded SHA-256 values before
Rust replay. No GT, returned IDs, fresh quality data, or S3 requests are
read in this cell.

Build f16 centroids in source-row order from the SQ8 stream in Rust using
32 rows/unit and 256 rows/page. Preserve V140's f32 SQ8 restoration,
f64 unit mean and f16 rounding. Encode a new `BORSUCP1` little-endian
format header with explicit row count, dimensions, unit rows and page
rows; reject other versions, malformed geometry, non-finite values and
trailing/truncated data. The generation manifest will authenticate the
resulting blob in serving; this gate records its digest and resident
payload independently. Decode once into f32 resident arrays, then score
all 1,000 frozen queries in Rust. The resident centroid array is twice
the compact f16 payload; record its logical bytes separately from charged
RSS and page cache. This cost is linear in N with no vector-count switch
and belongs in the later recall/resource tradeoff. Record score
differences and the full Rust f32 matrices versus V145's authenticated
reference without assuming NumPy BLAS bit parity. Require all 1,000 Rust
β=4 selected page inventories, physical byte ranges, planned bytes, GETs,
shortfalls and primary retention fields to match frozen V140 exactly.
Stop before ReLAION if D96 decisions differ. A missed scientific gate
is a completed, authenticated rejection with a specific parity or CPU
verdict, not an infrastructure failure.

Measure p50/p95/p99 of isolated Rust centroid scoring and scoring plus
the V145 planner, as well as build wall time, maximum RSS and cgroup
memory. The **CPU headroom screen is p95 ≤10 ms/query** for Rust scoring
plus planner separately on each cohort. This is a deliberately strict
offline screen; if the flat scorer fails, design a bounded hierarchical
candidate route rather than raising the gate or adding a dataset switch.
Passing still cannot establish end-to-end latency: it excludes the
primary router, network, source rerank, startup and concurrency.

Run one frozen-source Causality Spot attempt with a 5,400 s hard stop,
Spot interruption discard/restart semantics, terminal-only monitoring
while incomplete, artifact authentication after terminal, and immediate
compute termination. A Spot interruption is explicitly marked and the
measurement cell is discarded; restart manually under a new `aNNNN`
attempt prefix with the same frozen source and inputs. Reserve 1,200 s
before the hard stop for terminal artifact upload. The strongest current
BORSUK page-plan baseline is
V145/V140 exact parity; V145 Rust planner p95 was 0.474496 ms on D96
and 1.235540 ms on ReLAION, while V141/V142 returned quality remains
historical used-split evidence from the same β=4 plan.
